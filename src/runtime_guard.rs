use rustix::fs::{
    AtFlags, FlockOperation, Mode, OFlags, flock, open, openat, renameat, statat, unlinkat,
};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const LOCK_FILE: &str = "instance.lock";
const MARKER_FILE: &str = "run-state.json";
const MARKER_MAX_BYTES: u64 = 16 * 1024;

#[derive(Debug)]
pub enum GuardError {
    InstanceInUse,
    Io(io::Error),
}

impl std::fmt::Display for GuardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InstanceInUse => f.write_str("instance_in_use"),
            Self::Io(e) => write!(f, "io: {e}"),
        }
    }
}

impl std::error::Error for GuardError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InstanceInUse => None,
            Self::Io(e) => Some(e),
        }
    }
}

impl From<io::Error> for GuardError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Debug)]
pub enum AcquireOutcome {
    Clean(RuntimeGuard),
    Recovery(RuntimeGuard),
}

/// Owns the process-lifetime instance lock. Dropping it never clears recovery state.
#[derive(Debug)]
pub struct RuntimeGuard {
    dir: File,
    _lock: File,
    marker_owner: Mutex<Option<Uuid>>,
}

impl Drop for RuntimeGuard {
    fn drop(&mut self) {
        // CLOEXEC closes a forked child's copy only at exec. Explicit unlock
        // releases this owner's lock even during that brief inherited-FD window.
        // Recovery metadata is intentionally left untouched.
        let _ = flock(&self._lock, FlockOperation::Unlock);
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunMarker {
    instance_id: Uuid,
    phase: String,
    started_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    child_pid: Option<u32>,
}

fn marker_exists(dir: &File) -> io::Result<bool> {
    match statat(dir, MARKER_FILE, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(_) => Ok(true),
        Err(rustix::io::Errno::NOENT) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn io_invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn write_atomic(dir: &File, marker: &RunMarker) -> io::Result<()> {
    let name = format!(".run-state.{}.tmp", Uuid::new_v4());
    let fd = openat(
        dir,
        &name,
        OFlags::CREATE | OFlags::EXCL | OFlags::WRONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::from_bits_truncate(0o600),
    )?;
    let result = (|| {
        let mut file = File::from(fd);
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
        serde_json::to_writer(&mut file, marker)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        renameat(dir, &name, dir, MARKER_FILE)?;
        dir.sync_all()
    })();
    if result.is_err() {
        let _ = unlinkat(dir, &name, AtFlags::empty());
    }
    result
}

fn read_marker(dir: &File) -> io::Result<RunMarker> {
    let file = File::from(openat(
        dir,
        MARKER_FILE,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
        Mode::empty(),
    )?);
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > MARKER_MAX_BYTES {
        return Err(io_invalid("invalid run-state marker"));
    }
    let mut contents = Vec::new();
    file.take(MARKER_MAX_BYTES + 1).read_to_end(&mut contents)?;
    if contents.len() as u64 > MARKER_MAX_BYTES {
        return Err(io_invalid("run-state marker exceeds size limit"));
    }
    serde_json::from_slice(&contents).map_err(|_| io_invalid("invalid run-state marker"))
}

impl RuntimeGuard {
    pub fn acquire(dir: &Path) -> Result<AcquireOutcome, GuardError> {
        if dir.as_os_str().is_empty() {
            return Err(
                io::Error::new(io::ErrorKind::InvalidInput, "empty state directory").into(),
            );
        }
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir)?;
        // Pin all later operations to this directory FD; reject a symlink at the boundary.
        let dir = File::from(
            open(
                dir,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
                Mode::empty(),
            )
            .map_err(io::Error::from)?,
        );
        dir.set_permissions(fs::Permissions::from_mode(0o700))?;
        let lock = File::from(
            openat(
                &dir,
                LOCK_FILE,
                OFlags::CREATE
                    | OFlags::RDWR
                    | OFlags::CLOEXEC
                    | OFlags::NOFOLLOW
                    | OFlags::NONBLOCK,
                Mode::from_bits_truncate(0o600),
            )
            .map_err(io::Error::from)?,
        );
        let metadata = lock.metadata()?;
        if !metadata.is_file() || metadata.nlink() != 1 {
            return Err(io_invalid("instance lock must be a regular file with one link").into());
        }
        flock(&lock, FlockOperation::NonBlockingLockExclusive).map_err(|error| {
            if error == rustix::io::Errno::WOULDBLOCK {
                GuardError::InstanceInUse
            } else {
                GuardError::Io(error.into())
            }
        })?;
        lock.set_permissions(fs::Permissions::from_mode(0o600))?;
        let recovery = marker_exists(&dir)?;
        let guard = Self {
            dir,
            _lock: lock,
            marker_owner: Mutex::new(None),
        };
        Ok(if recovery {
            AcquireOutcome::Recovery(guard)
        } else {
            AcquireOutcome::Clean(guard)
        })
    }

    /// Persist recovery protection before spawning a child or starting any remote work.
    pub fn mark_started(&self, instance_id: Uuid) -> io::Result<()> {
        let mut owner = self
            .marker_owner
            .lock()
            .map_err(|_| io::Error::other("run-state lock poisoned"))?;
        if marker_exists(&self.dir)? {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "recovery_required",
            ));
        }
        write_atomic(
            &self.dir,
            &RunMarker {
                instance_id,
                phase: "starting".into(),
                started_at: wall_now(),
                child_pid: None,
            },
        )?;
        *owner = Some(instance_id);
        Ok(())
    }

    pub fn mark_child_pid(&self, pid: u32) -> io::Result<()> {
        let owner = self
            .marker_owner
            .lock()
            .map_err(|_| io::Error::other("run-state lock poisoned"))?;
        let instance_id =
            owner.ok_or_else(|| io_invalid("no run-state marker owned by this instance"))?;
        if pid == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "child PID must be positive",
            ));
        }
        let mut marker = read_marker(&self.dir)?;
        if marker.instance_id != instance_id
            || !matches!(marker.phase.as_str(), "starting" | "running")
        {
            return Err(io_invalid("run-state marker does not match this instance"));
        }
        marker.phase = "running".into();
        marker.child_pid = Some(pid);
        write_atomic(&self.dir, &marker)
    }

    /// Caller must have confirmed every session closed, child exited, and no unknown cleanup.
    pub fn clear(&self) -> io::Result<()> {
        let mut owner = self
            .marker_owner
            .lock()
            .map_err(|_| io::Error::other("run-state lock poisoned"))?;
        match unlinkat(&self.dir, MARKER_FILE, AtFlags::empty()) {
            Ok(()) | Err(rustix::io::Errno::NOENT) => {}
            Err(error) => return Err(error.into()),
        }
        self.dir.sync_all()?;
        *owner = None;
        Ok(())
    }

    /// Local manual recovery entry point. It never starts or kills any process.
    pub fn acknowledge_cleanup(dir: &Path) -> Result<(), GuardError> {
        let guard = match Self::acquire(dir)? {
            AcquireOutcome::Clean(guard) | AcquireOutcome::Recovery(guard) => guard,
        };
        guard.clear()?;
        Ok(())
    }
}

/// Wall clock for externally visible timestamps. Deadlines must use a monotonic clock.
pub fn wall_now() -> String {
    format_wall(SystemTime::now())
}

pub fn wall_after(duration: Duration) -> String {
    format_wall_millis(wall_millis(SystemTime::now()) + duration.as_millis() as i128)
}

fn wall_millis(time: SystemTime) -> i128 {
    let nanos = match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => {
            duration.as_secs() as i128 * 1_000_000_000 + duration.subsec_nanos() as i128
        }
        Err(error) => {
            -(error.duration().as_secs() as i128 * 1_000_000_000
                + error.duration().subsec_nanos() as i128)
        }
    };
    nanos.div_euclid(1_000_000)
}

fn format_wall(time: SystemTime) -> String {
    format_wall_millis(wall_millis(time))
}

fn format_wall_millis(millis: i128) -> String {
    let days = millis.div_euclid(86_400_000);
    let shifted_days = days + 719_468;
    let era = shifted_days.div_euclid(146_097);
    let day_of_era = shifted_days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    let year = year + i128::from(month <= 2);
    let in_day = millis.rem_euclid(86_400_000);
    let hour = in_day / 3_600_000;
    let minute = in_day / 60_000 % 60;
    let second = in_day / 1000 % 60;
    let fraction = in_day % 1000;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{fraction:03}Z")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::{PermissionsExt, symlink};
    use uuid::Uuid;

    fn clean(dir: &std::path::Path) -> RuntimeGuard {
        match RuntimeGuard::acquire(dir).unwrap() {
            AcquireOutcome::Clean(g) => g,
            AcquireOutcome::Recovery(_) => panic!("expected clean state directory"),
        }
    }

    #[test]
    fn exclusive_lock_is_held_until_guard_drop() {
        let dir = tempfile::tempdir().unwrap();
        let guard = clean(dir.path());
        assert!(matches!(
            RuntimeGuard::acquire(dir.path()),
            Err(GuardError::InstanceInUse)
        ));
        assert!(matches!(
            RuntimeGuard::acknowledge_cleanup(dir.path()),
            Err(GuardError::InstanceInUse)
        ));
        drop(guard);
        assert!(matches!(
            RuntimeGuard::acquire(dir.path()),
            Ok(AcquireOutcome::Clean(_))
        ));
    }

    #[test]
    fn guard_drop_releases_lock_despite_transient_spawn_descriptor() {
        let dir = tempfile::tempdir().unwrap();
        let guard = clean(dir.path());
        // A concurrent fork can retain the same open file description until
        // exec closes CLOEXEC descriptors. Dup models that interval exactly.
        let inherited = guard._lock.try_clone().unwrap();
        drop(guard);
        assert!(matches!(
            RuntimeGuard::acquire(dir.path()),
            Ok(AcquireOutcome::Clean(_))
        ));
        drop(inherited);
    }

    #[test]
    fn abnormal_exit_preserves_marker_until_manual_acknowledgment() {
        let dir = tempfile::tempdir().unwrap();
        let instance = Uuid::new_v4();
        {
            let guard = clean(dir.path());
            guard.mark_started(instance).unwrap();
            guard.mark_child_pid(12345).unwrap();
        }
        let guard = match RuntimeGuard::acquire(dir.path()).unwrap() {
            AcquireOutcome::Recovery(g) => g,
            AcquireOutcome::Clean(_) => panic!("abnormal exit must enter recovery"),
        };
        assert!(guard.mark_started(Uuid::new_v4()).is_err());
        assert!(dir.path().join("run-state.json").exists());
        assert!(matches!(
            RuntimeGuard::acquire(dir.path()),
            Err(GuardError::InstanceInUse)
        ));
        drop(guard);
        RuntimeGuard::acknowledge_cleanup(dir.path()).unwrap();
        assert!(matches!(
            RuntimeGuard::acquire(dir.path()),
            Ok(AcquireOutcome::Clean(_))
        ));
    }

    #[test]
    fn clean_lifecycle_clears_marker_and_preserves_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let guard = clean(dir.path());
        let instance = Uuid::new_v4();
        guard.mark_started(instance).unwrap();
        let started: serde_json::Value =
            serde_json::from_slice(&fs::read(dir.path().join("run-state.json")).unwrap()).unwrap();
        assert_eq!(started["phase"], "starting");
        assert_eq!(started.as_object().unwrap().len(), 3);
        guard.mark_child_pid(42).unwrap();
        let marker: serde_json::Value =
            serde_json::from_slice(&fs::read(dir.path().join("run-state.json")).unwrap()).unwrap();
        assert_eq!(marker["instance_id"], instance.to_string());
        assert_eq!(marker["started_at"], started["started_at"]);
        assert_eq!(marker["phase"], "running");
        assert_eq!(marker["child_pid"], 42);
        assert_eq!(marker.as_object().unwrap().len(), 4);
        guard.clear().unwrap();
        guard.clear().unwrap();
        drop(guard);
        assert!(matches!(
            RuntimeGuard::acquire(dir.path()),
            Ok(AcquireOutcome::Clean(_))
        ));
    }

    #[test]
    fn state_permissions_are_private_and_descriptors_close_on_exec() {
        let dir = tempfile::tempdir().unwrap();
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o755)).unwrap();
        let guard = clean(dir.path());
        guard.mark_started(Uuid::new_v4()).unwrap();
        assert_eq!(
            fs::metadata(dir.path()).unwrap().permissions().mode() & 0o777,
            0o700
        );
        for name in ["instance.lock", "run-state.json"] {
            assert_eq!(
                fs::metadata(dir.path().join(name))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        assert!(
            rustix::io::fcntl_getfd(&guard._lock)
                .unwrap()
                .contains(rustix::io::FdFlags::CLOEXEC)
        );
        assert!(
            rustix::io::fcntl_getfd(&guard.dir)
                .unwrap()
                .contains(rustix::io::FdFlags::CLOEXEC)
        );
    }

    #[test]
    fn symlink_state_directory_and_lock_are_rejected_without_changing_target() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("target");
        fs::create_dir(&target).unwrap();
        fs::set_permissions(&target, fs::Permissions::from_mode(0o755)).unwrap();
        let linked = root.path().join("linked");
        symlink(&target, &linked).unwrap();
        assert!(RuntimeGuard::acquire(&linked).is_err());
        assert_eq!(
            fs::metadata(&target).unwrap().permissions().mode() & 0o777,
            0o755
        );
        let secret = root.path().join("secret");
        fs::write(&secret, "untouched").unwrap();
        symlink(&secret, target.join("instance.lock")).unwrap();
        assert!(RuntimeGuard::acquire(&target).is_err());
        assert_eq!(fs::read_to_string(secret).unwrap(), "untouched");
    }

    #[test]
    fn broken_marker_symlink_requires_recovery_and_ack_does_not_follow_it() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("absent");
        symlink(&target, dir.path().join("run-state.json")).unwrap();
        assert!(matches!(
            RuntimeGuard::acquire(dir.path()),
            Ok(AcquireOutcome::Recovery(_))
        ));
        RuntimeGuard::acknowledge_cleanup(dir.path()).unwrap();
        assert!(!target.exists());
        assert!(matches!(
            RuntimeGuard::acquire(dir.path()),
            Ok(AcquireOutcome::Clean(_))
        ));
    }

    #[test]
    fn pid_update_requires_valid_existing_marker() {
        let dir = tempfile::tempdir().unwrap();
        let guard = clean(dir.path());
        assert!(guard.mark_child_pid(42).is_err());
        guard.mark_started(Uuid::new_v4()).unwrap();
        fs::write(dir.path().join("run-state.json"), "{}").unwrap();
        assert!(guard.mark_child_pid(42).is_err());
        assert_eq!(
            fs::read_to_string(dir.path().join("run-state.json")).unwrap(),
            "{}"
        );
    }

    #[test]
    fn wall_clock_formats_utc_with_calendar_boundaries() {
        use std::time::{Duration, UNIX_EPOCH};
        assert_eq!(format_wall(UNIX_EPOCH), "1970-01-01T00:00:00.000Z");
        assert_eq!(
            format_wall(UNIX_EPOCH + Duration::from_secs(951_827_696)),
            "2000-02-29T12:34:56.000Z"
        );
        assert_eq!(
            format_wall(UNIX_EPOCH - Duration::from_millis(1)),
            "1969-12-31T23:59:59.999Z"
        );
        assert_eq!(wall_now().len(), 24);
        assert!(wall_after(Duration::from_secs(60)) > wall_now());
    }
}

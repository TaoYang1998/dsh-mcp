//! Direct-child lifecycle management. All waiting and stderr buffering are bounded.
use crate::config::limits;
use std::{ffi::OsString, io, process::Stdio, time::Duration};
use tokio::{
    io::AsyncReadExt,
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::watch,
    task::JoinHandle,
    time::Instant,
};

/// Shared absolute deadline, so a later shutdown can shorten cleanup already
/// in progress without waiting for the process owner's mutex.
#[derive(Clone)]
pub(crate) struct TerminationControl(watch::Sender<Option<Instant>>);
impl Default for TerminationControl {
    fn default() -> Self {
        Self(watch::channel(None).0)
    }
}
impl TerminationControl {
    pub(crate) fn tighten(&self, budget: Duration) {
        let requested = Instant::now() + budget;
        self.0.send_if_modified(|deadline| {
            if deadline.is_none_or(|current| requested < current) {
                *deadline = Some(requested);
                true
            } else {
                false
            }
        });
    }
    pub(crate) fn remaining(&self) -> Option<Duration> {
        self.0
            .borrow()
            .map(|d| d.saturating_duration_since(Instant::now()))
    }
    pub(crate) async fn requested(&self) {
        let mut changes = self.0.subscribe();
        while changes.borrow_and_update().is_none() {
            if changes.changed().await.is_err() {
                break;
            }
        }
    }
}

pub struct DshProcess {
    child: Child,
    stderr_drain: JoinHandle<()>,
}

impl DshProcess {
    pub fn spawn(command: &str, args: &[String]) -> io::Result<Self> {
        Self::spawn_with_env(command, args, &[])
    }

    pub fn spawn_with_env(
        command: &str,
        args: &[String],
        env: &[(OsString, OsString)],
    ) -> io::Result<Self> {
        let mut command_builder = Command::new(command);
        command_builder
            .args(args)
            .envs(env.iter().cloned())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command_builder.spawn()?;
        let mut stderr = child.stderr.take().expect("piped stderr");
        let stderr_drain = tokio::spawn(async move {
            // Never use `lines`: a peer may produce an indefinitely long line.
            let mut chunk = [0u8; 4096];
            loop {
                match stderr.read(&mut chunk).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        tracing::debug!(target: "dsh.stderr", "{}", String::from_utf8_lossy(&chunk[..n]))
                    }
                }
            }
        });
        Ok(Self {
            child,
            stderr_drain,
        })
    }

    pub fn take_stdio(&mut self) -> (ChildStdin, ChildStdout) {
        (
            self.child.stdin.take().expect("stdin taken once"),
            self.child.stdout.take().expect("stdout taken once"),
        )
    }
    pub fn pid(&self) -> Option<u32> {
        self.child.id()
    }
    pub fn try_wait(&mut self) -> io::Result<Option<std::process::ExitStatus>> {
        self.child.try_wait()
    }

    /// True means the direct child was observed exited and reaped. Never waits
    /// beyond TERM_GRACE + KILL_GRACE, even if a process cannot be reaped.
    pub async fn terminate(self) -> bool {
        self.terminate_with_budget(limits::TERM_GRACE + limits::KILL_GRACE)
            .await
    }

    pub async fn terminate_with_budget(self, budget: Duration) -> bool {
        let control = TerminationControl::default();
        control.tighten(budget);
        self.terminate_with_control(control).await
    }

    pub(crate) async fn terminate_with_control(mut self, control: TerminationControl) -> bool {
        let result = self.terminate_inner(&control).await;
        self.stderr_drain.abort();
        result
    }

    async fn terminate_inner(&mut self, control: &TerminationControl) -> bool {
        match self.child.try_wait() {
            Ok(Some(_)) => return true,
            Err(error) => {
                tracing::warn!(%error, "child status check failed");
                return false;
            }
            Ok(None) => {}
        }
        if let Some(pid) = self.child.id() {
            #[cfg(unix)]
            // The PID belongs to our unreaped child; it cannot have been reused.
            unsafe {
                libc::kill(pid as libc::pid_t, libc::SIGTERM);
            }
            #[cfg(not(unix))]
            let _ = self.child.start_kill();
        }
        // Spend at most half a short budget on TERM, leaving a bounded explicit
        // KILL + wait/reap phase. A concurrent caller may tighten either phase.
        if let Some(result) = self.wait_phase(control, Some(Instant::now())).await {
            return result;
        }
        let _ = self.child.start_kill();
        let result = self.wait_phase(control, None).await.unwrap_or(false);
        if !result {
            tracing::error!("dsh child could not be reaped within deadline");
        }
        result
    }

    async fn wait_phase(
        &mut self,
        control: &TerminationControl,
        term_started: Option<Instant>,
    ) -> Option<bool> {
        let mut changes = control.0.subscribe();
        let kill_deadline = Instant::now() + limits::KILL_GRACE;
        loop {
            let hard = changes.borrow_and_update().expect("termination requested");
            let deadline = term_started.map_or(hard.min(kill_deadline), |start| {
                let reserve = limits::KILL_GRACE.min(hard.saturating_duration_since(start) / 2);
                (start + limits::TERM_GRACE).min(hard - reserve)
            });
            tokio::select! {
                biased;
                result = self.child.wait() => return Some(match result {
                    Ok(status) => { tracing::debug!(%status, "dsh child reaped"); true }
                    Err(error) => { tracing::warn!(%error, "child wait failed"); false }
                }),
                _ = tokio::time::sleep_until(deadline) => return None,
                _ = changes.changed() => {},
            }
        }
    }
}

impl Drop for DshProcess {
    fn drop(&mut self) {
        self.stderr_drain.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn spawn_and_terminate_reaps_child() {
        let process = DshProcess::spawn("/bin/sleep", &["1000".into()]).unwrap();
        let pid = process.pid().unwrap();
        assert!(process.terminate().await);
        assert_eq!(unsafe { libc::kill(pid as i32, 0) }, -1);
        assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::ESRCH));
    }
    #[tokio::test]
    async fn argv_passed_verbatim() {
        let arg = "a; $(echo injected) with spaces".to_owned();
        let mut process = DshProcess::spawn("/bin/echo", std::slice::from_ref(&arg)).unwrap();
        let (stdin, mut stdout) = process.take_stdio();
        drop(stdin);
        let mut output = String::new();
        stdout.read_to_string(&mut output).await.unwrap();
        assert_eq!(output, format!("{arg}\n"));
        assert!(process.terminate().await);
    }
    #[tokio::test]
    async fn kill_reaps_child_that_ignores_term() {
        let script = "import signal,sys,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); print('ready',flush=True); time.sleep(1000)";
        let mut process = DshProcess::spawn(
            "/usr/bin/python3",
            &["-u".into(), "-c".into(), script.into()],
        )
        .unwrap();
        let pid = process.pid().unwrap();
        let (_stdin, stdout) = process.take_stdio();
        use tokio::io::AsyncBufReadExt;
        let mut stdout = tokio::io::BufReader::new(stdout);
        let mut ready = String::new();
        stdout.read_line(&mut ready).await.unwrap();
        assert_eq!(ready.trim(), "ready");
        let started = std::time::Instant::now();
        assert!(process.terminate().await);
        assert!(
            started.elapsed()
                < limits::TERM_GRACE + limits::KILL_GRACE + std::time::Duration::from_millis(500)
        );
        assert_eq!(unsafe { libc::kill(pid as i32, 0) }, -1);
    }
    #[tokio::test]
    async fn arbitrarily_long_stderr_line_is_drained_in_fixed_chunks() {
        let script = "import sys; sys.stderr.write('x'*(16*1024*1024)); sys.stderr.flush(); print('done',flush=True)";
        let mut process = DshProcess::spawn(
            "/usr/bin/python3",
            &["-u".into(), "-c".into(), script.into()],
        )
        .unwrap();
        let (_stdin, mut stdout) = process.take_stdio();
        let mut output = String::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(3),
            stdout.read_to_string(&mut output),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(output.trim(), "done");
        assert!(process.terminate().await);
    }
}

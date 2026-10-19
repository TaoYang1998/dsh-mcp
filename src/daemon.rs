//! 一个守护进程拥有协调器，每个 Unix socket 连接有独立 MCP 生命周期。
use crate::{
    acp::DshFactory,
    config::Config,
    coordinator::{CoordHandle, spawn_coordinator},
    mcp_transport::BoundedMcpTransport,
    runtime_guard::{AcquireOutcome, RuntimeGuard},
    server::DshMcpServer,
};
use rustix::fs::{FlockOperation, flock};
use std::{
    fs::{self, File, OpenOptions},
    io,
    os::unix::{
        ffi::OsStrExt,
        fs::{DirBuilderExt, FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    },
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::UnixListener,
    signal::unix::{SignalKind, signal},
    sync::Semaphore,
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

struct SocketGuard {
    path: PathBuf,
    dev: u64,
    ino: u64,
    lock: File,
}
impl Drop for SocketGuard {
    /// 只移除本实例创建且 inode 未被替换的 socket，锁文件始终保留。
    fn drop(&mut self) {
        if fs::symlink_metadata(&self.path)
            .is_ok_and(|m| m.file_type().is_socket() && m.dev() == self.dev && m.ino() == self.ino)
        {
            let _ = fs::remove_file(&self.path);
        }
        let _ = flock(&self.lock, FlockOperation::Unlock);
    }
}
/// 路径锁跨状态目录生效，避免不同配置的 daemon 相互删除 socket。
async fn bind_socket(path: &Path) -> io::Result<(UnixListener, SocketGuard)> {
    if !path.is_absolute() || path.as_os_str().as_bytes().len() > 107 {
        return Err(io::Error::other("invalid Unix socket path"));
    }
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::other("socket parent required"))?;
    // 检查现有祖先后才允许创建目录，避免沿符号链接留下文件系统副作用。
    for ancestor in parent.ancestors() {
        match fs::symlink_metadata(ancestor) {
            Ok(m) if m.file_type().is_symlink() => {
                return Err(io::Error::other("socket parent must not contain symlinks"));
            }
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    if !parent.exists() {
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(parent)?;
    }
    if fs::canonicalize(parent)? != parent || fs::symlink_metadata(parent)?.file_type().is_symlink()
    {
        return Err(io::Error::other("socket parent must not contain symlinks"));
    }
    let metadata = fs::metadata(parent)?;
    let uid = unsafe { libc::geteuid() };
    if metadata.uid() != uid || metadata.permissions().mode() & 0o777 != 0o700 {
        return Err(io::Error::other(
            "socket parent must be owned by current UID with mode 0700",
        ));
    }
    let mut name = path
        .file_name()
        .ok_or_else(|| io::Error::other("socket name required"))?
        .to_os_string();
    name.push(".lock");
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(parent.join(name))?;
    let m = lock.metadata()?;
    if !m.is_file() || m.nlink() != 1 || m.uid() != uid || m.permissions().mode() & 0o777 != 0o600 {
        return Err(io::Error::other("invalid socket lock"));
    }
    flock(&lock, FlockOperation::NonBlockingLockExclusive).map_err(io::Error::from)?;
    match fs::symlink_metadata(path) {
        Ok(m) => {
            if !m.file_type().is_socket()
                || m.uid() != uid
                || m.permissions().mode() & 0o777 != 0o600
            {
                return Err(io::Error::other("unsafe existing socket path"));
            }
            match tokio::time::timeout(
                Duration::from_millis(250),
                tokio::net::UnixStream::connect(path),
            )
            .await
            {
                Ok(Err(e)) if e.kind() == io::ErrorKind::ConnectionRefused => {
                    fs::remove_file(path)?
                }
                _ => {
                    return Err(io::Error::other(
                        "socket already in use or cannot confirm stale socket",
                    ));
                }
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let listener = std::os::unix::net::UnixListener::bind(path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    let m = fs::symlink_metadata(path)?;
    let guard = SocketGuard {
        path: path.into(),
        dev: m.dev(),
        ino: m.ino(),
        lock,
    };
    listener.set_nonblocking(true)?;
    Ok((UnixListener::from_std(listener)?, guard))
}
/// 连接结束只注销 owner；关停整个协调器仅由 daemon 主循环负责。
async fn connection(socket: tokio::net::UnixStream, coord: CoordHandle, stop: CancellationToken) {
    let id = uuid::Uuid::new_v4();
    if coord.register_client(id).await.is_err() {
        return;
    }
    let server = DshMcpServer::new(coord.for_client(id));
    let (read, write) = socket.into_split();
    let transport = BoundedMcpTransport::new(read, write);
    let initialized = tokio::select! {_=stop.cancelled()=>None,r=tokio::time::timeout(Duration::from_secs(10),rmcp::serve_server(server,transport))=>r.ok().and_then(Result::ok)};
    if let Some(running) = initialized {
        let cancel = running.cancellation_token();
        tokio::select! {_=stop.cancelled()=>cancel.cancel(),_=running.waiting()=>{}}
    }
    let _ = coord.disconnect_client(id).await;
}
/// 共享状态锁和恢复标记属于 daemon，不属于代理或单个连接。
pub async fn serve(config: Config) -> anyhow::Result<()> {
    let path = config
        .socket_path
        .clone()
        .ok_or_else(|| anyhow::anyhow!("DSH_MCP_SOCKET required for daemon"))?;
    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    let (guard, recovery) = match RuntimeGuard::acquire(&config.state_dir)? {
        AcquireOutcome::Clean(g) => (Arc::new(g), false),
        AcquireOutcome::Recovery(g) => (Arc::new(g), true),
    };
    let (listener, _socket_guard) = bind_socket(&path).await?;
    let capacity = Arc::new(Semaphore::new(config.max_clients));
    let coord = spawn_coordinator(config.clone(), DshFactory::new(config), guard, recovery);
    let stop = CancellationToken::new();
    let mut clients = JoinSet::new();
    let result = loop {
        tokio::select! {
            _=terminate.recv()=>break Ok(()),_=interrupt.recv()=>break Ok(()),
            Some(_)=clients.join_next(),if !clients.is_empty()=>{},
            accepted=listener.accept()=>match accepted{
                Ok((socket,_))=>{
                    if !socket.peer_cred().is_ok_and(|c|c.uid()==unsafe{libc::geteuid()}){continue;}
                    let Ok(permit)=capacity.clone().try_acquire_owned() else{continue};
                    let coord=coord.clone();let stop=stop.clone();
                    clients.spawn(async move{let _permit=permit;connection(socket,coord,stop).await;});
                },
                Err(e)=>break Err(anyhow::Error::from(e)),
            }
        }
    };
    stop.cancel();
    while clients.join_next().await.is_some() {}
    let clean = coord.shutdown().await;
    result?;
    if !clean {
        anyhow::bail!("recovery_required: cleanup unconfirmed");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    /// 非 socket 和符号链接不能被启动清理逻辑覆盖。
    #[tokio::test]
    async fn socket_guard_preserves_regular_files_and_symlinks() {
        let root = tempfile::tempdir().unwrap();
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let path = root.path().join("acp.sock");
        fs::write(&path, "keep").unwrap();
        assert!(bind_socket(&path).await.is_err());
        assert_eq!(fs::read_to_string(&path).unwrap(), "keep");
        fs::remove_file(&path).unwrap();
        let target = root.path().join("target");
        fs::write(&target, "secret").unwrap();
        std::os::unix::fs::symlink(&target, &path).unwrap();
        assert!(bind_socket(&path).await.is_err());
        assert!(
            fs::symlink_metadata(&path)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(fs::read_to_string(target).unwrap(), "secret");
    }
    /// 同 socket 的第二实例既不能监听，也不能删除第一实例的路径。
    #[tokio::test]
    async fn socket_lock_blocks_second_owner_and_only_cleans_its_inode() {
        let root = tempfile::tempdir().unwrap();
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let path = root.path().join("acp.sock");
        let (_listener, guard) = bind_socket(&path).await.unwrap();
        assert!(bind_socket(&path).await.is_err());
        assert!(path.exists());
        fs::remove_file(&path).unwrap();
        fs::write(&path, "replacement").unwrap();
        drop(guard);
        assert_eq!(fs::read_to_string(path).unwrap(), "replacement");
    }
    /// 拒绝已有不安全目录权限，不通过 chmod 掩盖配置问题。
    #[tokio::test]
    async fn socket_guard_rejects_public_parent_and_reclaims_confirmed_stale_socket() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("acp.sock");
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o755)).unwrap();
        assert!(bind_socket(&path).await.is_err());
        assert_eq!(
            fs::metadata(root.path()).unwrap().permissions().mode() & 0o777,
            0o755
        );
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let stale = std::os::unix::net::UnixListener::bind(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        drop(stale);
        let (_listener, guard) = bind_socket(&path).await.unwrap();
        assert!(path.exists());
        drop(guard);
        assert!(!path.exists());
    }
}

#[cfg(test)]
mod parent_validation_tests {
    use super::*;
    /// 校验必须先于目录创建，拒绝符号链接不能留下目标目录副作用。
    #[tokio::test]
    async fn symlink_ancestor_is_rejected_before_creating_descendants() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("target");
        fs::create_dir(&target).unwrap();
        let linked = root.path().join("linked");
        std::os::unix::fs::symlink(&target, &linked).unwrap();
        assert!(
            bind_socket(&linked.join("new").join("acp.sock"))
                .await
                .is_err()
        );
        assert!(!target.join("new").exists());
    }
}

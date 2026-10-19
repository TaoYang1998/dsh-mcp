//! 代理仅转发独立 MCP 字节流，不持有 ACP、状态锁或重放请求。
use std::{io, path::PathBuf, time::Duration};
use tokio::{io::AsyncWriteExt, net::UnixStream};
/// socket 必须显式配置绝对路径，代理不读取模型或运行目录配置。
pub fn socket_from_env() -> io::Result<PathBuf> {
    let path = std::env::var_os("DSH_MCP_SOCKET")
        .map(PathBuf::from)
        .ok_or_else(|| io::Error::other("DSH_MCP_SOCKET is required"))?;
    if !path.is_absolute() {
        return Err(io::Error::other("DSH_MCP_SOCKET must be absolute"));
    }
    Ok(path)
}
/// 任一方向 EOF 即关闭整个连接，确保 Codex 退出会释放归属。
pub async fn run(path: PathBuf) -> io::Result<()> {
    let socket = tokio::time::timeout(Duration::from_secs(3), UnixStream::connect(path))
        .await
        .map_err(|_| io::Error::other("daemon connection timed out"))??;
    if socket.peer_cred()?.uid() != unsafe { libc::geteuid() } {
        return Err(io::Error::other("daemon UID mismatch"));
    }
    let (mut read, mut write) = socket.into_split();
    let mut input = tokio::io::stdin();
    let mut output = tokio::io::stdout();
    let result = tokio::select! {r=tokio::io::copy(&mut input,&mut write)=>r.map(|_|()),r=tokio::io::copy(&mut read,&mut output)=>r.map(|_|())};
    let _ = write.shutdown().await;
    result
}

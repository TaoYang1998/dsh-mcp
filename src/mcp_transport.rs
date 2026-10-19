//! MCP 在解析前限制帧大小，并限制每连接的请求与响应积压。
use crate::config::limits;
use futures::StreamExt;
use rmcp::{
    RoleServer,
    model::{ClientNotification, JsonRpcMessage, RequestId},
    service::{RxJsonRpcMessage, TxJsonRpcMessage},
    transport::{Transport, async_rw::JsonRpcMessageCodec},
};
use std::{
    collections::HashSet,
    io,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::{codec::FramedRead, sync::CancellationToken};

pub const FRAME_MAX: usize = 1024 * 1024;
pub const REQUESTS_MAX: usize = 16;
const RESPONSES_MAX: usize = 16;
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
struct Outbound {
    bytes: Vec<u8>,
    id: Option<RequestId>,
    reply: oneshot::Sender<io::Result<()>>,
}
pub struct BoundedMcpTransport<R: AsyncRead> {
    reader: FramedRead<R, JsonRpcMessageCodec<RxJsonRpcMessage<RoleServer>>>,
    output: mpsc::Sender<Outbound>,
    pending: Arc<Mutex<HashSet<RequestId>>>,
    stop: CancellationToken,
    writer: JoinHandle<()>,
}
impl<R: AsyncRead + Unpin + Send + 'static> BoundedMcpTransport<R> {
    /// 每连接一个有界写队列，慢客户端仅关闭自己的传输。
    pub fn new<W: AsyncWrite + Unpin + Send + 'static>(read: R, mut write: W) -> Self {
        let (tx, mut rx) = mpsc::channel::<Outbound>(RESPONSES_MAX);
        let pending = Arc::new(Mutex::new(HashSet::new()));
        let stop = CancellationToken::new();
        let stopped = stop.clone();
        let inflight = pending.clone();
        let writer = tokio::spawn(async move {
            loop {
                let item = tokio::select! {_=stopped.cancelled()=>break,item=rx.recv()=>match item{Some(i)=>i,None=>break}};
                let result = tokio::select! {
                    _=stopped.cancelled()=>Err(io::Error::new(io::ErrorKind::BrokenPipe,"connection closed")),
                    result=tokio::time::timeout(WRITE_TIMEOUT,async{write.write_all(&item.bytes).await?;write.flush().await})=>result.unwrap_or_else(|_|Err(io::Error::new(io::ErrorKind::TimedOut,"MCP write timed out")))
                };
                if result.is_ok()
                    && let Some(id) = item.id
                {
                    inflight.lock().unwrap().remove(&id);
                }
                let failed = result.is_err();
                let _ = item.reply.send(result);
                if failed {
                    stopped.cancel();
                    break;
                }
            }
        });
        Self {
            reader: FramedRead::new(read, JsonRpcMessageCodec::new_with_max_length(FRAME_MAX)),
            output: tx,
            pending,
            stop,
            writer,
        }
    }
}
impl<R: AsyncRead> Drop for BoundedMcpTransport<R> {
    /// 取消所有写等待，避免断连后留存传输后台任务。
    fn drop(&mut self) {
        self.stop.cancel();
        self.writer.abort();
    }
}
impl<R: AsyncRead + Unpin + Send + 'static> Transport<RoleServer> for BoundedMcpTransport<R> {
    type Error = io::Error;
    /// 编码和队列容量都有限，禁止把积压藏进 SDK 的响应发送任务。
    fn send(
        &mut self,
        item: TxJsonRpcMessage<RoleServer>,
    ) -> impl Future<Output = io::Result<()>> + Send + 'static {
        let id = match &item {
            JsonRpcMessage::Response(r) => Some(r.id.clone()),
            JsonRpcMessage::Error(e) => e.id.clone(),
            _ => None,
        };
        let bytes = serde_json::to_vec(&item).map_err(io::Error::other);
        let tx = self.output.clone();
        let stop = self.stop.clone();
        async move {
            let mut bytes = bytes?;
            if bytes.len() > limits::MCP_RESPONSE_MAX {
                stop.cancel();
                return Err(io::Error::other("MCP response too large"));
            }
            bytes.push(b'\n');
            let (reply, rx) = oneshot::channel();
            if tx.try_send(Outbound { bytes, id, reply }).is_err() {
                stop.cancel();
                return Err(io::Error::other("MCP response queue full"));
            }
            rx.await.unwrap_or_else(|_| {
                Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "MCP writer stopped",
                ))
            })
        }
    }
    /// 超大帧、重复编号或过多未完成请求均只断开本连接。
    async fn receive(&mut self) -> Option<RxJsonRpcMessage<RoleServer>> {
        let message = tokio::select! {_=self.stop.cancelled()=>return None,m=self.reader.next()=>match m{Some(Ok(m))=>m,_=>{self.stop.cancel();return None}}};
        if let JsonRpcMessage::Request(r) = &message {
            let mut pending = self.pending.lock().unwrap();
            if pending.len() >= REQUESTS_MAX || !pending.insert(r.id.clone()) {
                self.stop.cancel();
                return None;
            }
        }
        if let JsonRpcMessage::Notification(r) = &message
            && let ClientNotification::CancelledNotification(n) = &r.notification
            && let Some(id) = &n.params.request_id
        {
            self.pending.lock().unwrap().remove(id);
        }
        Some(message)
    }
    /// 关闭时不再尝试向失联客户端交付响应。
    async fn close(&mut self) -> io::Result<()> {
        self.stop.cancel();
        self.writer.abort();
        Ok(())
    }
}

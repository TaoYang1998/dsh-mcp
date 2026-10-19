//! ACP v1 适配器负责输入限制、有序回调和进程归属。
use crate::history::{BackendCapabilities, SessionPage, SessionSummary};
use crate::{
    config::{Config, limits},
    permission::{PermKind, PermResponder, PermissionOptionView},
    supervisor::{DshProcess, TerminationControl},
};
use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::schema::v1::*;
use agent_client_protocol::{Agent, Client, ConnectionTo, Dispatch, Handled, JsonRpcMessage};
use futures::{AsyncRead, AsyncWrite, task::AtomicWaker};
use serde::Serialize;
use std::{
    collections::{HashMap, VecDeque},
    ffi::OsString,
    future::Future,
    io,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::{Mutex, Notify, mpsc, oneshot};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SessionUpdateView {
    AgentMessageChunk {
        text: String,
        message_id: Option<String>,
    },
    AgentThoughtChunk {
        text: String,
    },
    ToolCall {
        tool_call_id: String,
        title: String,
        name: Option<String>,
        status: String,
    },
    ToolCallUpdate {
        tool_call_id: String,
        status: Option<String>,
        summary: Option<String>,
    },
    ConfigUpdate {
        summary: String,
    },
    Usage {
        summary: String,
    },
    Other {
        kind: String,
    },
}
fn bounded(text: &str, max: usize) -> String {
    let mut end = text.len().min(max);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_owned()
}
fn block_text(block: &ContentBlock) -> String {
    match block {
        ContentBlock::Text(text) => text.text.clone(),
        _ => String::new(),
    }
}
pub fn view_of_update(update: &SessionUpdate) -> SessionUpdateView {
    match update {
        SessionUpdate::AgentMessageChunk(c) => SessionUpdateView::AgentMessageChunk {
            text: block_text(&c.content),
            message_id: c.message_id.as_ref().map(ToString::to_string),
        },
        SessionUpdate::AgentThoughtChunk(c) => SessionUpdateView::AgentThoughtChunk {
            text: block_text(&c.content),
        },
        SessionUpdate::ToolCall(c) => SessionUpdateView::ToolCall {
            tool_call_id: c.tool_call_id.to_string(),
            title: bounded(&c.title, 2048),
            name: c.name.as_ref().map(|s| bounded(s, 1024)),
            status: format!("{:?}", c.status),
        },
        SessionUpdate::ToolCallUpdate(c) => SessionUpdateView::ToolCallUpdate {
            tool_call_id: c.tool_call_id.to_string(),
            status: c.fields.status.map(|s| format!("{s:?}")),
            summary: c
                .fields
                .title
                .as_ref()
                .map(|s| bounded(s, 4096))
                .or_else(|| {
                    c.fields
                        .content
                        .as_ref()
                        .map(|items| format!("{} item(s)", items.len()))
                }),
        },
        SessionUpdate::ConfigOptionUpdate(c) => SessionUpdateView::ConfigUpdate {
            summary: format!("{} option(s)", c.config_options.len()),
        },
        SessionUpdate::UsageUpdate(c) => SessionUpdateView::Usage {
            summary: bounded(
                &serde_json::to_string(c).unwrap_or_else(|_| "usage".into()),
                6144,
            ),
        },
        _ => SessionUpdateView::Other {
            kind: "unsupported_session_update".into(),
        },
    }
}
#[derive(Debug, Clone, Serialize)]
pub struct PermissionRequestView {
    pub tool_call_id: Option<String>,
    pub options: Vec<PermissionOptionView>,
}
pub fn view_of_permission_request(request: &RequestPermissionRequest) -> PermissionRequestView {
    PermissionRequestView {
        tool_call_id: Some(request.tool_call.tool_call_id.to_string()),
        options: request
            .options
            .iter()
            .map(|option| PermissionOptionView {
                option_id: option.option_id.to_string(),
                name: option.name.clone(),
                kind: match option.kind {
                    PermissionOptionKind::AllowOnce => PermKind::AllowOnce,
                    PermissionOptionKind::AllowAlways => PermKind::AllowAlways,
                    PermissionOptionKind::RejectOnce => PermKind::RejectOnce,
                    PermissionOptionKind::RejectAlways => PermKind::RejectAlways,
                    _ => PermKind::Other,
                },
            })
            .collect(),
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StopReasonView {
    EndTurn,
    MaxTokens,
    MaxTurnRequests,
    Refusal,
    Cancelled,
    Other(String),
}
impl From<StopReason> for StopReasonView {
    fn from(reason: StopReason) -> Self {
        match reason {
            StopReason::EndTurn => Self::EndTurn,
            StopReason::MaxTokens => Self::MaxTokens,
            StopReason::MaxTurnRequests => Self::MaxTurnRequests,
            StopReason::Refusal => Self::Refusal,
            StopReason::Cancelled => Self::Cancelled,
            other => Self::Other(format!("{other:?}")),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendErrorKind {
    Timeout,
    Protocol,
    ProcessDied,
    Unavailable,
    Rejected,
    StartupClean,
    CapabilityUnsupported,
    ResponseTooLarge,
}
#[derive(Debug, Clone)]
pub struct BackendError {
    pub kind: BackendErrorKind,
    pub message: String,
}
impl BackendError {
    pub fn new(kind: BackendErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: bounded(&message.into(), limits::ERROR_SUMMARY_MAX),
        }
    }
}
impl std::fmt::Display for BackendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}
impl std::error::Error for BackendError {}
pub enum BackendEvent {
    Update {
        epoch: u64,
        dsh_session_id: String,
        update: SessionUpdateView,
    },
    Permission {
        epoch: u64,
        dsh_session_id: String,
        request: PermissionRequestView,
        responder: PermResponder,
    },
    PermissionInvalid {
        epoch: u64,
        dsh_session_id: String,
        reason: String,
    },
    Died {
        epoch: u64,
        reason: String,
    },
}
pub trait Backend: Clone + Send + Sync + 'static {
    /// 默认不宣告历史能力，兼容现有不支持恢复的后端。
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::default()
    }
    /// 仅通过 ACP 公布的能力列举历史，不读取内部日志。
    fn list_sessions(
        &self,
        _cwd: Option<PathBuf>,
        _cursor: Option<String>,
    ) -> impl Future<Output = Result<SessionPage, BackendError>> + Send {
        async {
            Err(BackendError::new(
                BackendErrorKind::CapabilityUnsupported,
                "session/list unsupported",
            ))
        }
    }
    /// 恢复失败时不得创建替代会话。
    fn resume_session(
        &self,
        _sid: &str,
        _cwd: &Path,
    ) -> impl Future<Output = Result<(), BackendError>> + Send {
        async {
            Err(BackendError::new(
                BackendErrorKind::CapabilityUnsupported,
                "session/resume unsupported",
            ))
        }
    }

    fn new_session(&self, cwd: &Path) -> impl Future<Output = Result<String, BackendError>> + Send;
    fn prompt(
        &self,
        dsh_session_id: &str,
        text: String,
    ) -> impl Future<Output = Result<StopReasonView, BackendError>> + Send;
    fn cancel(&self, dsh_session_id: &str)
    -> impl Future<Output = Result<(), BackendError>> + Send;
    fn close_session(
        &self,
        dsh_session_id: &str,
    ) -> impl Future<Output = Result<(), BackendError>> + Send;
    fn terminate_process(&self) -> impl Future<Output = bool> + Send;
    fn terminate_process_with_budget(
        &self,
        _budget: Duration,
    ) -> impl Future<Output = bool> + Send {
        self.terminate_process()
    }
}
#[derive(Clone, Default)]
pub struct StartupControl(TerminationControl);
impl StartupControl {
    pub fn shutdown(&self, budget: Duration) {
        self.0.tighten(budget);
    }
}
pub trait BackendFactory: Send + 'static {
    type B: Backend;
    fn startup_shutdown_handle(&self) -> Option<StartupControl> {
        None
    }
    fn start(
        &mut self,
    ) -> impl Future<Output = Result<(Self::B, mpsc::Receiver<BackendEvent>, u32), BackendError>> + Send;
}

type NewSessionCall = (PathBuf, oneshot::Sender<Result<String, BackendError>>);
type PromptCall = (
    String,
    String,
    oneshot::Sender<Result<StopReasonView, BackendError>>,
);
type ListCall = (
    Option<PathBuf>,
    Option<String>,
    oneshot::Sender<Result<SessionPage, BackendError>>,
);
type ResumeCall = (String, PathBuf, oneshot::Sender<Result<(), BackendError>>);
type CloseCall = (String, oneshot::Sender<Result<(), BackendError>>);
#[derive(Clone)]
pub struct FakeBackend {
    list_tx: mpsc::Sender<ListCall>,
    resume_tx: mpsc::Sender<ResumeCall>,
    new_session_tx: mpsc::Sender<NewSessionCall>,
    prompt_tx: mpsc::Sender<PromptCall>,
    cancel_tx: mpsc::Sender<String>,
    close_tx: mpsc::Sender<CloseCall>,
}
fn fake_closed() -> BackendError {
    BackendError::new(BackendErrorKind::ProcessDied, "fake channel closed")
}
impl Backend for FakeBackend {
    /// 确定性测试后端支持受控的历史调用。
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            list: true,
            resume: true,
        }
    }
    /// 把历史调用交给测试驱动控制完成时机。
    async fn list_sessions(
        &self,
        cwd: Option<PathBuf>,
        cursor: Option<String>,
    ) -> Result<SessionPage, BackendError> {
        let (tx, rx) = oneshot::channel();
        self.list_tx
            .send((cwd, cursor, tx))
            .await
            .map_err(|_| fake_closed())?;
        rx.await.map_err(|_| fake_closed())?
    }
    /// 把恢复调用交给测试驱动验证会话标识和目录。
    async fn resume_session(&self, sid: &str, cwd: &Path) -> Result<(), BackendError> {
        let (tx, rx) = oneshot::channel();
        self.resume_tx
            .send((sid.into(), cwd.into(), tx))
            .await
            .map_err(|_| fake_closed())?;
        rx.await.map_err(|_| fake_closed())?
    }

    async fn new_session(&self, cwd: &Path) -> Result<String, BackendError> {
        let (tx, rx) = oneshot::channel();
        self.new_session_tx
            .send((cwd.to_owned(), tx))
            .await
            .map_err(|_| fake_closed())?;
        rx.await.map_err(|_| fake_closed())?
    }
    async fn prompt(&self, sid: &str, text: String) -> Result<StopReasonView, BackendError> {
        let (tx, rx) = oneshot::channel();
        self.prompt_tx
            .send((sid.into(), text, tx))
            .await
            .map_err(|_| fake_closed())?;
        rx.await.map_err(|_| fake_closed())?
    }
    async fn cancel(&self, sid: &str) -> Result<(), BackendError> {
        self.cancel_tx
            .send(sid.into())
            .await
            .map_err(|_| fake_closed())
    }
    async fn close_session(&self, sid: &str) -> Result<(), BackendError> {
        let (tx, rx) = oneshot::channel();
        self.close_tx
            .send((sid.into(), tx))
            .await
            .map_err(|_| fake_closed())?;
        rx.await.map_err(|_| fake_closed())?
    }
    async fn terminate_process(&self) -> bool {
        true
    }
}
pub struct FakeFactory {
    backend: FakeBackend,
    event_rx: Option<mpsc::Receiver<BackendEvent>>,
    failures: VecDeque<BackendError>,
}
impl BackendFactory for FakeFactory {
    type B = FakeBackend;
    async fn start(
        &mut self,
    ) -> Result<(Self::B, mpsc::Receiver<BackendEvent>, u32), BackendError> {
        if let Some(error) = self.failures.pop_front() {
            return Err(error);
        }
        let events = self.event_rx.take().ok_or_else(|| {
            BackendError::new(
                BackendErrorKind::Unavailable,
                "fake factory already started",
            )
        })?;
        Ok((self.backend.clone(), events, 0))
    }
}
pub struct FakeDriver {
    pub list_rx: mpsc::Receiver<ListCall>,
    pub resume_rx: mpsc::Receiver<ResumeCall>,
    pub new_session_rx: mpsc::Receiver<NewSessionCall>,
    pub prompt_rx: mpsc::Receiver<PromptCall>,
    pub cancel_rx: mpsc::Receiver<String>,
    pub close_rx: mpsc::Receiver<CloseCall>,
    pub event_tx: mpsc::Sender<BackendEvent>,
}
pub fn fake_backend() -> (FakeFactory, FakeDriver) {
    fake_backend_with_failures(0)
}
pub fn fake_backend_with_failures(count: usize) -> (FakeFactory, FakeDriver) {
    let (list_tx, list_rx) = mpsc::channel(32);
    let (resume_tx, resume_rx) = mpsc::channel(32);
    let (new_session_tx, new_session_rx) = mpsc::channel(128);
    let (prompt_tx, prompt_rx) = mpsc::channel(128);
    let (cancel_tx, cancel_rx) = mpsc::channel(128);
    let (close_tx, close_rx) = mpsc::channel(128);
    let (event_tx, event_rx) = mpsc::channel(limits::ACP_QUEUE_MAX_ITEMS);
    (
        FakeFactory {
            backend: FakeBackend {
                list_tx,
                resume_tx,
                new_session_tx,
                prompt_tx,
                cancel_tx,
                close_tx,
            },
            event_rx: Some(event_rx),
            failures: (0..count)
                .map(|_| {
                    BackendError::new(
                        BackendErrorKind::StartupClean,
                        "injected clean startup failure",
                    )
                })
                .collect(),
        },
        FakeDriver {
            list_rx,
            resume_rx,
            new_session_rx,
            prompt_rx,
            cancel_rx,
            close_rx,
            event_tx,
        },
    )
}

/// A single-frame credit prevents the SDK's internal unbounded transport channel
/// from reading ahead while an ordered callback is waiting on our bounded queue.
#[derive(Default)]
struct FrameCredit {
    ready: AtomicBool,
    waker: AtomicWaker,
}
impl FrameCredit {
    fn release(&self) {
        self.ready.store(true, Ordering::Release);
        self.waker.wake();
    }
    fn poll_take(&self, cx: &mut Context<'_>) -> Poll<()> {
        if self.ready.swap(false, Ordering::AcqRel) {
            return Poll::Ready(());
        }
        self.waker.register(cx.waker());
        if self.ready.swap(false, Ordering::AcqRel) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}
pub struct FrameLimit<R> {
    inner: Pin<Box<R>>,
    line_len: usize,
    max: usize,
    pending: Vec<u8>,
    offset: usize,
    credit: Option<Arc<FrameCredit>>,
    waiting: bool,
    credit_timer: Option<Pin<Box<tokio::time::Sleep>>>,
    first: bool,
}
impl<R> FrameLimit<R> {
    pub fn new(inner: R) -> Self {
        Self::with_max(inner, limits::ACP_FRAME_MAX)
    }
    pub fn with_max(inner: R, max: usize) -> Self {
        Self {
            inner: Box::pin(inner),
            line_len: 0,
            max,
            pending: Vec::new(),
            offset: 0,
            credit: None,
            waiting: false,
            credit_timer: None,
            first: true,
        }
    }
    fn gated(inner: R, credit: Arc<FrameCredit>) -> Self {
        let mut value = Self::new(inner);
        value.credit = Some(credit);
        value
    }
}
impl<R: AsyncRead> AsyncRead for FrameLimit<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if output.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if this.waiting {
            if let Some(credit) = &this.credit
                && credit.poll_take(cx).is_pending()
            {
                // Malformed frames and unsolicited responses can be
                // filtered inside the SDK without reaching our callback.
                // They must not permanently retain the reader credit.
                let timer = this
                    .credit_timer
                    .get_or_insert_with(|| Box::pin(tokio::time::sleep(limits::ACP_QUEUE_STALL)));
                if timer.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "ACP frame dispatch stalled",
                    )));
                }
                return Poll::Pending;
            }
            this.credit_timer = None;
            this.waiting = false;
        }
        if this.offset == this.pending.len() {
            let mut scratch = [0u8; 8192];
            let n = futures::ready!(this.inner.as_mut().poll_read(cx, &mut scratch))?;
            if n == 0 {
                return Poll::Ready(Ok(0));
            }
            this.pending.clear();
            this.pending.extend_from_slice(&scratch[..n]);
            this.offset = 0;
        }
        let mut count = 0;
        while this.offset < this.pending.len() && count < output.len() {
            let byte = this.pending[this.offset];
            if this.first && !byte.is_ascii_whitespace() {
                // Batches are outside our ACP v1 contract and defeat per-frame credits.
                if this.credit.is_some() && byte == b'[' {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "ACP batch frames are unsupported",
                    )));
                }
                this.first = false;
            }
            if byte == b'\n' {
                this.line_len = 0;
                this.first = true;
                this.waiting = this.credit.is_some();
            } else {
                this.line_len += 1;
                if this.line_len > this.max {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "ACP frame exceeds limit",
                    )));
                }
            }
            output[count] = byte;
            count += 1;
            this.offset += 1;
            if byte == b'\n' {
                break;
            }
        }
        Poll::Ready(Ok(count))
    }
}

type ReplyAck = oneshot::Sender<Result<(), String>>;
#[derive(Default)]
struct ReplyTracker {
    pending: StdMutex<HashMap<String, ReplyAck>>,
}
impl ReplyTracker {
    fn register(
        &self,
        id: &RequestId,
    ) -> Result<oneshot::Receiver<Result<(), String>>, agent_client_protocol::Error> {
        let key = serde_json::to_string(id)?;
        if key.len() > 1024 {
            return Err(agent_client_protocol::Error::new(
                -32603,
                "permission request ID exceeds budget",
            ));
        }
        let mut pending = self.pending.lock().expect("reply tracker mutex");
        if pending.len() >= limits::PENDING_PERMS_MAX + limits::ACP_QUEUE_MAX_ITEMS
            || pending.contains_key(&key)
        {
            return Err(agent_client_protocol::Error::new(
                -32603,
                "permission response tracker exhausted or duplicate request ID",
            ));
        }
        let (tx, rx) = oneshot::channel();
        pending.insert(key, tx);
        Ok(rx)
    }
    fn written(&self, bytes: &[u8]) {
        // Only outbound responses acknowledge permissions; requests and
        // notifications may independently carry a numeric ID in their payload.
        let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes) else {
            return;
        };
        if value.get("method").is_some()
            || (value.get("result").is_none() && value.get("error").is_none())
        {
            return;
        }
        if let Some(id) = value.get("id")
            && let Ok(key) = serde_json::to_string(id)
            && let Some(ack) = self
                .pending
                .lock()
                .expect("reply tracker mutex")
                .remove(&key)
        {
            let _ = ack.send(Ok(()));
        }
    }
    fn failed(&self, reason: &str) {
        for (_, ack) in self.pending.lock().expect("reply tracker mutex").drain() {
            let _ = ack.send(Err(reason.to_owned()));
        }
    }
}

/// Bound a whole blocked frame write/flush, including permission responses.
/// A tracked response is acknowledged after its final newline reaches the pipe.
struct WriteDeadline<W> {
    inner: Pin<Box<W>>,
    timeout: Duration,
    timer: Option<Pin<Box<tokio::time::Sleep>>>,
    frame: Vec<u8>,
    replies: Arc<ReplyTracker>,
}
impl<W> WriteDeadline<W> {
    fn new(inner: W, timeout: Duration, replies: Arc<ReplyTracker>) -> Self {
        Self {
            inner: Box::pin(inner),
            timeout,
            timer: None,
            frame: Vec::new(),
            replies,
        }
    }
    fn check_deadline(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let timer = self
            .timer
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(self.timeout)));
        match timer.as_mut().poll(cx) {
            Poll::Ready(()) => {
                self.replies.failed("ACP pipe write timed out");
                Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "ACP pipe write timed out",
                )))
            }
            Poll::Pending => Poll::Ready(Ok(())),
        }
    }
    fn record_written(&mut self, bytes: &[u8]) -> io::Result<()> {
        for byte in bytes {
            if *byte == b'\n' {
                self.replies.written(&self.frame);
                self.frame.clear();
            } else {
                if self.frame.len() >= limits::ACP_FRAME_MAX {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "outbound ACP frame exceeds limit",
                    ));
                }
                self.frame.push(*byte);
            }
        }
        Ok(())
    }
}
impl<W: AsyncWrite> AsyncWrite for WriteDeadline<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        futures::ready!(this.check_deadline(cx))?;
        match this.inner.as_mut().poll_write(cx, buf) {
            Poll::Ready(Ok(n)) => {
                this.record_written(&buf[..n])?;
                Poll::Ready(Ok(n))
            }
            Poll::Ready(Err(error)) => {
                this.replies.failed("ACP pipe write failed");
                Poll::Ready(Err(error))
            }
            Poll::Pending => Poll::Pending,
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        futures::ready!(this.check_deadline(cx))?;
        match this.inner.as_mut().poll_flush(cx) {
            Poll::Ready(result) => {
                this.timer = None;
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        futures::ready!(this.check_deadline(cx))?;
        match this.inner.as_mut().poll_close(cx) {
            Poll::Ready(result) => {
                this.timer = None;
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
impl<W> Drop for WriteDeadline<W> {
    fn drop(&mut self) {
        self.replies.failed("ACP response writer closed");
    }
}

fn sdk_error(error: agent_client_protocol::Error) -> BackendError {
    let peer_error = error
        .data
        .as_ref()
        .and_then(|d| d.get("dsh_mcp_peer_error"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    BackendError::new(
        if peer_error {
            BackendErrorKind::Rejected
        } else {
            BackendErrorKind::ProcessDied
        },
        error.message,
    )
}
fn protocol(message: impl Into<String>) -> BackendError {
    BackendError::new(BackendErrorKind::Protocol, message)
}
fn check_id(id: &str) -> Result<(), BackendError> {
    if id.is_empty() || id.len() > limits::REMOTE_ID_MAX {
        Err(protocol("remote identifier is empty or exceeds 1024 bytes"))
    } else {
        Ok(())
    }
}
fn validate_remote(value: &serde_json::Value) -> Result<(), BackendError> {
    // SDK 对坏条目采用跳过策略；历史页先核验必需字段，防止 cursor 越过未展示记录。
    if let Some(sessions) = value.get("sessions") {
        let entries = sessions
            .as_array()
            .ok_or_else(|| protocol("invalid history sessions"))?;
        for entry in entries {
            let id = entry
                .get("sessionId")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| protocol("history session identifier missing"))?;
            check_id(id)?;
            let cwd = entry
                .get("cwd")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| protocol("history cwd missing"))?;
            if !Path::new(cwd).is_absolute() {
                return Err(protocol("history cwd must be absolute"));
            }
        }
        if value
            .get("nextCursor")
            .is_some_and(|v| !v.is_null() && !v.is_string())
        {
            return Err(protocol("invalid history cursor"));
        }
    }
    // Walk values iteratively; the SDK parser already caps JSON nesting.
    let mut pending = vec![value];
    while let Some(value) = pending.pop() {
        match value {
            serde_json::Value::Object(object) => {
                for (key, value) in object {
                    if key == "_meta"
                        && serde_json::to_vec(value).map_or(true, |s| s.len() > limits::META_BUDGET)
                    {
                        return Err(protocol("remote metadata exceeds budget"));
                    }
                    if matches!(key.as_str(), "sessionId" | "toolCallId" | "messageId")
                        && let Some(id) = value.as_str()
                    {
                        check_id(id)?;
                    }
                    pending.push(value);
                }
            }
            serde_json::Value::Array(array) => pending.extend(array.iter()),
            _ => {}
        }
    }
    Ok(())
}
fn record_failure(failure: &tokio::sync::watch::Sender<Option<String>>, reason: impl Into<String>) {
    let reason = bounded(&reason.into(), limits::ERROR_SUMMARY_MAX);
    failure.send_if_modified(|current| {
        if current.is_none() {
            *current = Some(reason);
            true
        } else {
            false
        }
    });
}
async fn ingress(
    tx: &mpsc::Sender<BackendEvent>,
    event: BackendEvent,
    failure: &tokio::sync::watch::Sender<Option<String>>,
) -> Result<(), agent_client_protocol::Error> {
    match tokio::time::timeout(limits::ACP_QUEUE_STALL, tx.send(event)).await {
        Ok(Ok(())) => Ok(()),
        result => {
            let reason = if result.is_err() {
                "inbound queue congested"
            } else {
                "inbound event consumer closed"
            };
            record_failure(failure, reason);
            Err(agent_client_protocol::Error::new(-32603, reason))
        }
    }
}

struct ProcessOwner {
    process: Mutex<Option<DshProcess>>,
    clean: AtomicBool,
    stopping: AtomicBool,
    shutdown: Notify,
    driver: Mutex<Option<tokio::task::JoinHandle<()>>>,
    cleanup: TerminationControl,
}
impl ProcessOwner {
    async fn terminate(&self) -> bool {
        self.terminate_with_budget(limits::TERM_GRACE + limits::KILL_GRACE)
            .await
    }
    async fn terminate_with_budget(&self, budget: Duration) -> bool {
        self.cleanup.tighten(budget);
        self.stopping.store(true, Ordering::Release);
        self.shutdown.notify_waiters();
        // The SDK exposes no public shutdown method; completing its foreground
        // future closes the transport. Abort is a bounded fallback for stuck I/O.
        if let Some(mut driver) = self.driver.lock().await.take()
            && tokio::time::timeout(Duration::from_millis(100), &mut driver)
                .await
                .is_err()
        {
            driver.abort();
            let _ = tokio::time::timeout(Duration::from_millis(100), driver).await;
        }
        let mut slot = self.process.lock().await;
        if let Some(process) = slot.take() {
            self.clean.store(
                process.terminate_with_control(self.cleanup.clone()).await,
                Ordering::Release,
            );
        }
        self.clean.load(Ordering::Acquire)
    }
}
#[derive(Clone)]
pub struct DshBackend {
    connection: ConnectionTo<Agent>,
    owner: Arc<ProcessOwner>,
    control_timeout: Duration,
    start_timeout: Duration,
    pub supports_list: bool,
    pub supports_resume: bool,
}
impl Backend for DshBackend {
    /// 仅使用握手时后端实际公布的能力。
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            list: self.supports_list,
            resume: self.supports_resume,
        }
    }
    /// 保留后端分页 cursor，不裁剪整页或标识。
    async fn list_sessions(
        &self,
        cwd: Option<PathBuf>,
        cursor: Option<String>,
    ) -> Result<SessionPage, BackendError> {
        if !self.supports_list {
            return Err(BackendError::new(
                BackendErrorKind::CapabilityUnsupported,
                "session/list unsupported",
            ));
        }
        let response = tokio::time::timeout(
            self.control_timeout,
            self.connection
                .send_request(ListSessionsRequest::new().cwd(cwd).cursor(cursor))
                .block_task(),
        )
        .await
        .map_err(|_| BackendError::new(BackendErrorKind::Timeout, "session/list timed out"))?
        .map_err(sdk_error)?;
        SessionPage {
            sessions: response
                .sessions
                .into_iter()
                .map(|s| SessionSummary {
                    dsh_session_id: s.session_id.to_string(),
                    cwd: s.cwd,
                    title: s.title,
                    updated_at: s.updated_at,
                })
                .collect(),
            next_cursor: response.next_cursor,
        }
        .bounded()
    }
    /// 恢复后不发送提示词，路由安装由协调器完成。
    async fn resume_session(&self, sid: &str, cwd: &Path) -> Result<(), BackendError> {
        if !self.supports_resume {
            return Err(BackendError::new(
                BackendErrorKind::CapabilityUnsupported,
                "session/resume unsupported",
            ));
        }
        check_id(sid)?;
        tokio::time::timeout(
            self.start_timeout,
            self.connection
                .send_request(ResumeSessionRequest::new(sid.to_owned(), cwd))
                .block_task(),
        )
        .await
        .map_err(|_| BackendError::new(BackendErrorKind::Timeout, "session/resume timed out"))?
        .map_err(sdk_error)?;
        Ok(())
    }

    async fn new_session(&self, cwd: &Path) -> Result<String, BackendError> {
        let response = tokio::time::timeout(
            self.start_timeout,
            self.connection
                .send_request(NewSessionRequest::new(cwd))
                .block_task(),
        )
        .await
        .map_err(|_| BackendError::new(BackendErrorKind::Timeout, "session/new timed out"))?
        .map_err(sdk_error)?;
        let sid = response.session_id.to_string();
        check_id(&sid)?;
        Ok(sid)
    }
    async fn prompt(&self, sid: &str, text: String) -> Result<StopReasonView, BackendError> {
        let (tx, rx) = oneshot::channel();
        self.connection
            .prepare_request(PromptRequest::new(
                sid.to_owned(),
                vec![ContentBlock::Text(TextContent::new(text))],
            ))
            .on_receiving_result(move |response| async move {
                let _ = tx.send(response.map(|r| r.stop_reason.into()).map_err(sdk_error));
                Ok(())
            })
            .map_err(sdk_error)?;
        rx.await.map_err(|_| {
            BackendError::new(
                BackendErrorKind::ProcessDied,
                "prompt response channel closed",
            )
        })?
    }
    async fn cancel(&self, sid: &str) -> Result<(), BackendError> {
        self.connection
            .send_notification(CancelNotification::new(sid.to_owned()))
            .map_err(sdk_error)
    }
    async fn close_session(&self, sid: &str) -> Result<(), BackendError> {
        tokio::time::timeout(
            self.control_timeout,
            self.connection
                .send_request(CloseSessionRequest::new(sid.to_owned()))
                .block_task(),
        )
        .await
        .map_err(|_| BackendError::new(BackendErrorKind::Timeout, "session/close timed out"))?
        .map_err(sdk_error)?;
        Ok(())
    }
    async fn terminate_process(&self) -> bool {
        self.owner.terminate().await
    }
    async fn terminate_process_with_budget(&self, budget: Duration) -> bool {
        self.owner.terminate_with_budget(budget).await
    }
}
impl DshBackend {
    pub async fn terminate_process(&self) -> bool {
        self.owner.terminate().await
    }
}

pub struct DshFactory {
    config: Config,
    env: Vec<(OsString, OsString)>,
    startup: StartupControl,
}
impl DshFactory {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            env: Vec::new(),
            startup: StartupControl::default(),
        }
    }
    /// Per-child environment overrides; avoids modifying global process state.
    pub fn with_env(mut self, key: impl Into<OsString>, value: impl Into<OsString>) -> Self {
        self.env.push((key.into(), value.into()));
        self
    }
}
impl BackendFactory for DshFactory {
    type B = DshBackend;
    fn startup_shutdown_handle(&self) -> Option<StartupControl> {
        Some(self.startup.clone())
    }
    async fn start(
        &mut self,
    ) -> Result<(Self::B, mpsc::Receiver<BackendEvent>, u32), BackendError> {
        let config = self.config.clone();
        let env = self.env.clone();
        let startup = std::mem::take(&mut self.startup);
        let (reply, response) = oneshot::channel();
        // Ownership survives cancellation of the caller's startup future. Its
        // internal deadline always terminates/reaps before returning a failure.
        tokio::spawn(async move {
            let outcome = start_owned(config, env, startup).await;
            if let Err(Ok((backend, _, _))) = reply.send(outcome) {
                backend.terminate_process().await;
            }
        });
        response.await.map_err(|_| {
            BackendError::new(BackendErrorKind::Unavailable, "startup owner stopped")
        })?
    }
}

async fn start_owned(
    config: Config,
    env: Vec<(OsString, OsString)>,
    startup: StartupControl,
) -> Result<(DshBackend, mpsc::Receiver<BackendEvent>, u32), BackendError> {
    if startup.0.remaining().is_some() {
        return Err(BackendError::new(
            BackendErrorKind::StartupClean,
            "startup cancelled before spawn",
        ));
    }
    let mut process = DshProcess::spawn_with_env(&config.dsh_command, &config.dsh_args, &env)
        .map_err(|error| {
            BackendError::new(
                BackendErrorKind::StartupClean,
                format!("spawn failed: {error}"),
            )
        })?;
    let pid = process
        .pid()
        .ok_or_else(|| protocol("spawned child has no pid"))?;
    let (stdin, stdout) = process.take_stdio();
    let owner = Arc::new(ProcessOwner {
        process: Mutex::new(Some(process)),
        clean: AtomicBool::new(false),
        stopping: AtomicBool::new(false),
        shutdown: Notify::new(),
        driver: Mutex::new(None),
        cleanup: startup.0.clone(),
    });
    let credit = Arc::new(FrameCredit::default());
    let replies = Arc::new(ReplyTracker::default());
    let transport = agent_client_protocol::ByteStreams::new(
        WriteDeadline::new(stdin.compat_write(), config.close_timeout, replies.clone()),
        FrameLimit::gated(stdout.compat(), credit.clone()),
    );
    // One queued full-text event is at most one 4 MiB wire frame (plus small
    // adapter metadata). This preserves source byte counts without truncating
    // results and bounds encoded ingress below 8 MiB. The reader credit gives
    // backpressure instead of letting the SDK accumulate frames internally.
    let (event_tx, event_rx) = mpsc::channel(1);
    let (failure_tx, mut failure_rx) = tokio::sync::watch::channel(None);
    let (initialized_tx, initialized_rx) = oneshot::channel();
    let handler_tx = event_tx.clone();
    let handler_failure = failure_tx.clone();
    let driver_owner = owner.clone();
    let driver_failure = failure_tx.clone();
    let driver = tokio::spawn(async move {
        let handler = async move |mut dispatch: Dispatch, _cx: ConnectionTo<Agent>| {
            let result: Result<Handled<Dispatch>, agent_client_protocol::Error> = async {
                match &mut dispatch {
                    Dispatch::Response(response, _) => {
                        if let Ok(value) = response {
                            validate_remote(value).map_err(|e| {
                                agent_client_protocol::Error::new(-32603, e.message)
                            })?;
                        }
                        // Distinguish a peer's JSON-RPC error from local transport errors.
                        if let Err(error) = response {
                            error.data = Some(serde_json::json!({"dsh_mcp_peer_error": true}));
                        }
                        credit.release();
                        return Ok(Handled::No {
                            message: dispatch,
                            retry: false,
                        });
                    }
                    Dispatch::Notification(message) => {
                        validate_remote(&message.params)
                            .map_err(|e| agent_client_protocol::Error::new(-32603, e.message))?;
                        if message.method != "session/update" {
                            return Err(agent_client_protocol::Error::new(
                                -32603,
                                "unexpected ACP notification",
                            ));
                        }
                        let notification =
                            SessionNotification::parse_message(&message.method, &message.params)?;
                        ingress(
                            &handler_tx,
                            BackendEvent::Update {
                                epoch: 0,
                                dsh_session_id: notification.session_id.to_string(),
                                update: view_of_update(&notification.update),
                            },
                            &handler_failure,
                        )
                        .await?;
                    }
                    Dispatch::Request(message, _) => {
                        validate_remote(&message.params)
                            .map_err(|e| agent_client_protocol::Error::new(-32603, e.message))?;
                        if message.method != "session/request_permission" {
                            return Err(agent_client_protocol::Error::new(
                                -32603,
                                "unexpected ACP request",
                            ));
                        }
                        let request = RequestPermissionRequest::parse_message(
                            &message.method,
                            &message.params,
                        )?;
                        let view = view_of_permission_request(&request);
                        let sid = request.session_id.to_string();
                        let Dispatch::Request(_, responder) = dispatch else {
                            unreachable!()
                        };
                        let too_large = view.options.len() > limits::PERM_OPTIONS_MAX
                            || view
                                .options
                                .iter()
                                .any(|o| o.option_id.len() > limits::PERM_OPTION_ID_MAX)
                            || serde_json::to_vec(&view)
                                .map_or(true, |bytes| bytes.len() > limits::PERM_ITEM_MAX);
                        if too_large {
                            responder.respond(serde_json::to_value(
                                RequestPermissionResponse::new(RequestPermissionOutcome::Cancelled),
                            )?)?;
                            ingress(
                                &handler_tx,
                                BackendEvent::PermissionInvalid {
                                    epoch: 0,
                                    dsh_session_id: sid,
                                    reason: "permission_payload_too_large".into(),
                                },
                                &handler_failure,
                            )
                            .await?;
                        } else {
                            ingress(
                                &handler_tx,
                                BackendEvent::Permission {
                                    epoch: 0,
                                    dsh_session_id: sid,
                                    request: view,
                                    responder: PermResponder::Tracked {
                                        ack: replies.register(responder.id())?,
                                        responder: responder.cast(),
                                    },
                                },
                                &handler_failure,
                            )
                            .await?;
                        }
                    }
                }
                credit.release();
                Ok(Handled::Yes)
            }
            .await;
            if let Err(error) = &result {
                record_failure(&handler_failure, error.message.clone());
            }
            result
        };
        let result = Client
            .builder()
            .name("dsh-mcp")
            .on_receive_dispatch(handler, agent_client_protocol::on_receive_dispatch!())
            .on_close({
                let failure = driver_failure.clone();
                async move |_cx| {
                    record_failure(&failure, "ACP stdout EOF");
                    Ok(())
                }
            })
            .connect_with(transport, async move |connection| {
                let initialization = connection
                    .send_request(InitializeRequest::new(ProtocolVersion::V1))
                    .block_task()
                    .await?;
                if initialization.protocol_version != ProtocolVersion::V1 {
                    return Err(agent_client_protocol::Error::new(
                        -32603,
                        "dsh did not negotiate ACP v1",
                    ));
                }
                let caps = initialization.agent_capabilities.session_capabilities;
                if caps.close.is_none() {
                    return Err(agent_client_protocol::Error::new(
                        -32603,
                        "dsh missing session/close capability",
                    ));
                }
                let _ =
                    initialized_tx.send((connection, caps.list.is_some(), caps.resume.is_some()));
                if !driver_owner.stopping.load(Ordering::Acquire) {
                    driver_owner.shutdown.notified().await;
                }
                Ok(())
            })
            .await;
        match result {
            Err(error) => record_failure(&driver_failure, error.message),
            Ok(()) => record_failure(&driver_failure, "ACP connection stopped"),
        }
    });
    *owner.driver.lock().await = Some(driver);
    let initialization = tokio::select! {
        result = tokio::time::timeout(config.start_timeout, initialized_rx) => result.map_err(|_| "ACP initialize timed out"),
        _ = startup.0.requested() => Err("ACP initialize cancelled by shutdown"),
    };
    let (connection, supports_list, supports_resume) = match initialization {
        Ok(Ok(initialized)) => initialized,
        result => {
            let reason = failure_rx
                .borrow()
                .clone()
                .unwrap_or_else(|| result.err().unwrap_or("ACP initialize failed").into());
            let clean = owner.terminate().await;
            return Err(BackendError::new(
                if clean {
                    BackendErrorKind::StartupClean
                } else {
                    BackendErrorKind::Unavailable
                },
                reason,
            ));
        }
    };
    let backend = DshBackend {
        connection,
        owner: owner.clone(),
        control_timeout: config.close_timeout,
        start_timeout: config.start_timeout,
        supports_list,
        supports_resume,
    };
    tokio::spawn(async move {
        let reason = loop {
            if owner.stopping.load(Ordering::Acquire) {
                return;
            }
            if let Some(reason) = failure_rx.borrow().clone() {
                break reason;
            }
            {
                let mut process = owner.process.lock().await;
                if let Some(process) = process.as_mut() {
                    match process.try_wait() {
                        Ok(Some(status)) => break format!("dsh child exited: {status}"),
                        Err(error) => break format!("dsh child status failed: {error}"),
                        Ok(None) => {}
                    }
                }
            }
            tokio::select! { _ = tokio::time::sleep(Duration::from_millis(25)) => {}, _ = failure_rx.changed() => {} }
        };
        // Publish loss of contact before the TERM/KILL grace periods. The
        // coordinator can then reject new work immediately and may concurrently
        // request the same idempotent cleanup. A full queue retains this single
        // failure waiter until the consumer drains preceding ordered updates.
        let _ = event_tx.send(BackendEvent::Died { epoch: 0, reason }).await;
        owner.terminate().await;
    });
    Ok((backend, event_rx, pid))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::AsyncReadExt;
    #[tokio::test]
    async fn fake_backend_roundtrip_and_clean_failure() {
        let (mut factory, mut driver) = fake_backend_with_failures(1);
        assert_eq!(
            factory.start().await.err().unwrap().kind,
            BackendErrorKind::StartupClean
        );
        let (backend, _events, pid) = factory.start().await.unwrap();
        assert_eq!(pid, 0);
        let b = backend.clone();
        let call = tokio::spawn(async move { b.new_session(Path::new("/tmp/work")).await });
        let (cwd, reply) = driver.new_session_rx.recv().await.unwrap();
        assert_eq!(cwd, PathBuf::from("/tmp/work"));
        reply.send(Ok("s1".into())).unwrap();
        assert_eq!(call.await.unwrap().unwrap(), "s1");
        let b = backend.clone();
        let call = tokio::spawn(async move { b.prompt("s1", "task".into()).await });
        let (sid, text, reply) = driver.prompt_rx.recv().await.unwrap();
        assert_eq!((sid.as_str(), text.as_str()), ("s1", "task"));
        reply.send(Ok(StopReasonView::EndTurn)).unwrap();
        assert_eq!(call.await.unwrap().unwrap(), StopReasonView::EndTurn);
        backend.cancel("s1").await.unwrap();
        assert_eq!(driver.cancel_rx.recv().await.unwrap(), "s1");
        let b = backend.clone();
        let call = tokio::spawn(async move { b.close_session("s1").await });
        let (_, reply) = driver.close_rx.recv().await.unwrap();
        reply.send(Ok(())).unwrap();
        call.await.unwrap().unwrap();
        assert!(backend.terminate_process().await);
    }
    #[test]
    fn view_translation_preserves_text_and_permission_ids() {
        let view = view_of_update(&SessionUpdate::AgentMessageChunk(ContentChunk::new(
            ContentBlock::Text(TextContent::new("hello")),
        )));
        assert_eq!(
            view,
            SessionUpdateView::AgentMessageChunk {
                text: "hello".into(),
                message_id: None
            }
        );
        let request: RequestPermissionRequest = serde_json::from_value(serde_json::json!({"sessionId":"s1","toolCall":{"toolCallId":"t1"},"options":[{"optionId":"opaque:allow/once","name":"Allow once","kind":"allow_once"}]})).unwrap();
        let view = view_of_permission_request(&request);
        assert_eq!(view.options[0].option_id, "opaque:allow/once");
        assert_eq!(view.options[0].kind, PermKind::AllowOnce);
    }
    #[tokio::test]
    async fn frame_limit_applies_before_parse_and_resets_at_newlines() {
        let mut reader = FrameLimit::with_max(futures::io::Cursor::new(b"1234\n12345\n"), 4);
        let mut output = Vec::new();
        let error = reader.read_to_end(&mut output).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(output, b"1234\n");
    }
    #[tokio::test]
    async fn frame_credit_prevents_sdk_transport_readahead() {
        let credit = Arc::new(FrameCredit::default());
        let mut reader = FrameLimit::gated(futures::io::Cursor::new(b"one\ntwo\n"), credit.clone());
        let mut buf = [0u8; 32];
        let n = reader.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"one\n");
        assert!(
            tokio::time::timeout(Duration::from_millis(10), reader.read(&mut buf))
                .await
                .is_err()
        );
        credit.release();
        let n = reader.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"two\n");
    }
    #[test]
    fn remote_metadata_and_identifiers_are_rejected_without_truncation() {
        assert!(validate_remote(&serde_json::json!({"sessionId":"x".repeat(1025)})).is_err());
        assert!(
            validate_remote(
                &serde_json::json!({"_meta":{"large":"x".repeat(limits::META_BUDGET)}})
            )
            .is_err()
        );
    }
    fn config(command: &str, args: Vec<String>) -> Config {
        Config {
            dsh_command: command.into(),
            dsh_args: args,
            state_dir: PathBuf::from("/tmp"),
            socket_path: None,
            max_clients: 32,
            max_active_tasks: 4,
            max_open_sessions: 16,
            session_idle_ttl: Duration::from_secs(1800),
            max_task_records: 256,
            task_retention: Duration::from_secs(86400),
            start_timeout: Duration::from_secs(2),
            permission_timeout: Duration::from_secs(600),
            cancel_timeout: Duration::from_secs(30),
            close_timeout: Duration::from_secs(1),
            shutdown_timeout: Duration::from_secs(50),
        }
    }
    #[tokio::test]
    async fn startup_timeout_reaps_and_is_retryable_only_after_cleanup() {
        let temp = tempfile::tempdir().unwrap();
        let pidfile = temp.path().join("pid");
        let script =
            "import os,sys,time\nopen(sys.argv[1],'w').write(str(os.getpid()))\ntime.sleep(60)";
        let mut configuration = config(
            "/usr/bin/python3",
            vec!["-c".into(), script.into(), pidfile.to_string_lossy().into()],
        );
        configuration.start_timeout = Duration::from_millis(100);
        let mut factory = DshFactory::new(configuration);
        let error = factory.start().await.err().unwrap();
        assert_eq!(error.kind, BackendErrorKind::StartupClean);
        let pid: i32 = std::fs::read_to_string(pidfile).unwrap().parse().unwrap();
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
    }
    #[tokio::test]
    async fn actual_wire_callbacks_precede_prompt_response_and_eof_is_observed() {
        let script = r#"import sys,json,time
for line in sys.stdin:
 r=json.loads(line); m=r['method']; i=r.get('id')
 if m=='initialize': v={'protocolVersion':1,'agentCapabilities':{'sessionCapabilities':{'close':{}}}}
 elif m=='session/new': v={'sessionId':'remote-session'}
 elif m=='session/prompt':
  print(json.dumps({'jsonrpc':'2.0','method':'session/update','params':{'sessionId':'remote-session','update':{'sessionUpdate':'agent_message_chunk','content':{'type':'text','text':'last chunk'}}}}),flush=True)
  v={'stopReason':'end_turn'}
 elif m=='session/close': v={}
 else: continue
 print(json.dumps({'jsonrpc':'2.0','id':i,'result':v}),flush=True)
 if m=='session/prompt': break
"#;
        let mut factory = DshFactory::new(config(
            "/usr/bin/python3",
            vec!["-u".into(), "-c".into(), script.into()],
        ));
        let (backend, mut events, _) = factory.start().await.unwrap();
        assert_eq!(
            backend.new_session(Path::new("/tmp")).await.unwrap(),
            "remote-session"
        );
        assert_eq!(
            backend
                .prompt("remote-session", "hello".into())
                .await
                .unwrap(),
            StopReasonView::EndTurn
        );
        assert!(
            matches!(events.try_recv().unwrap(), BackendEvent::Update { update: SessionUpdateView::AgentMessageChunk { text, .. }, .. } if text=="last chunk")
        );
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), events.recv())
                .await
                .unwrap()
                .unwrap(),
            BackendEvent::Died { .. }
        ));
        assert!(backend.terminate_process().await);
    }
    #[tokio::test]
    async fn permission_delivery_ack_requires_written_newline() {
        use futures::AsyncWriteExt;
        let tracker = Arc::new(ReplyTracker::default());
        let id: RequestId = serde_json::from_value(serde_json::json!("permission-1")).unwrap();
        let mut ack = tracker.register(&id).unwrap();
        let mut writer = WriteDeadline::new(
            futures::io::Cursor::new(Vec::new()),
            Duration::from_secs(1),
            tracker,
        );
        writer.write_all(br#"{"jsonrpc":"2.0","id":"permission-1","result":{"outcome":{"outcome":"cancelled"}}}"#).await.unwrap();
        assert!(matches!(
            ack.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        writer.write_all(b"\n").await.unwrap();
        assert_eq!(ack.await.unwrap(), Ok(()));
    }
    #[tokio::test]
    async fn stalled_writer_times_out_and_fails_tracked_permission() {
        struct Stalled;
        impl AsyncWrite for Stalled {
            fn poll_write(
                self: Pin<&mut Self>,
                _: &mut Context<'_>,
                _: &[u8],
            ) -> Poll<io::Result<usize>> {
                Poll::Pending
            }
            fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Pending
            }
            fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Pending
            }
        }
        use futures::AsyncWriteExt;
        let tracker = Arc::new(ReplyTracker::default());
        let id: RequestId = serde_json::from_value(serde_json::json!(1)).unwrap();
        let ack = tracker.register(&id).unwrap();
        let mut writer = WriteDeadline::new(Stalled, Duration::from_millis(10), tracker);
        assert_eq!(
            writer.write_all(b"response\n").await.unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        assert!(ack.await.unwrap().is_err());
    }
    #[tokio::test]
    async fn actual_permission_response_is_confirmed_and_prompt_continues() {
        let script = r#"import sys,json
for line in sys.stdin:
 r=json.loads(line)
 if 'method' not in r:
  assert r['id']=='perm' and r['result']['outcome']['optionId']=='opaque:allow'
  print(json.dumps({'jsonrpc':'2.0','id':prompt_id,'result':{'stopReason':'end_turn'}}),flush=True)
  continue
 m=r['method']; i=r.get('id')
 if m=='initialize': v={'protocolVersion':1,'agentCapabilities':{'sessionCapabilities':{'close':{}}}}
 elif m=='session/prompt':
  prompt_id=i
  print(json.dumps({'jsonrpc':'2.0','id':'perm','method':'session/request_permission','params':{'sessionId':'s1','toolCall':{'toolCallId':'t1'},'options':[{'optionId':'opaque:allow','name':'Allow once','kind':'allow_once'}]}}),flush=True)
  continue
 else: v={}
 print(json.dumps({'jsonrpc':'2.0','id':i,'result':v}),flush=True)
"#;
        let mut factory = DshFactory::new(config(
            "/usr/bin/python3",
            vec!["-u".into(), "-c".into(), script.into()],
        ));
        let (backend, mut events, _) = factory.start().await.unwrap();
        let b = backend.clone();
        let prompt = tokio::spawn(async move { b.prompt("s1", "test".into()).await });
        let BackendEvent::Permission {
            request, responder, ..
        } = events.recv().await.unwrap()
        else {
            panic!("permission event required")
        };
        assert_eq!(request.options[0].option_id, "opaque:allow");
        let response: RequestPermissionResponse = serde_json::from_value(
            serde_json::json!({"outcome":{"outcome":"selected","optionId":"opaque:allow"}}),
        )
        .unwrap();
        responder.reply_confirmed(response).await.unwrap();
        assert_eq!(prompt.await.unwrap().unwrap(), StopReasonView::EndTurn);
        assert!(backend.terminate_process().await);
    }
    #[tokio::test(start_paused = true)]
    async fn filtered_malformed_or_unsolicited_frame_has_bounded_dispatch_wait() {
        for frame in [
            b"not-json\n".as_slice(),
            br#"{"jsonrpc":"2.0","id":"unknown","result":{}}
"#
            .as_slice(),
        ] {
            let mut reader = FrameLimit::gated(
                futures::io::Cursor::new(frame),
                Arc::new(FrameCredit::default()),
            );
            let mut buf = [0u8; 256];
            assert_eq!(reader.read(&mut buf).await.unwrap(), frame.len());
            // The SDK does not dispatch malformed input or responses with no
            // subscriber, so no handler can return the single-frame credit.
            let next = tokio::time::timeout(
                limits::ACP_QUEUE_STALL + Duration::from_secs(1),
                reader.read(&mut buf),
            )
            .await;
            assert!(matches!(next, Ok(Err(ref error)) if error.kind() == io::ErrorKind::TimedOut));
        }
    }
    #[tokio::test]
    async fn new_session_uses_start_budget_instead_of_close_budget() {
        let script = r#"import sys,json,time
for line in sys.stdin:
 r=json.loads(line)
 if r['method']=='initialize': v={'protocolVersion':1,'agentCapabilities':{'sessionCapabilities':{'close':{}}}}
 elif r['method']=='session/new':
  time.sleep(0.05)
  v={'sessionId':'s1'}
 else: v={}
 print(json.dumps({'jsonrpc':'2.0','id':r['id'],'result':v}),flush=True)
"#;
        let mut configuration = config(
            "/usr/bin/python3",
            vec!["-u".into(), "-c".into(), script.into()],
        );
        configuration.close_timeout = Duration::from_millis(10);
        let mut factory = DshFactory::new(configuration);
        let (backend, _events, _) = factory.start().await.unwrap();
        let result = backend.new_session(Path::new("/tmp")).await;
        assert!(backend.terminate_process().await);
        assert_eq!(result.unwrap(), "s1");
    }
    #[tokio::test]
    async fn eof_is_reported_before_term_ignoring_peer_is_reaped() {
        let script = r#"import sys,json,os,signal,time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
for line in sys.stdin:
 r=json.loads(line)
 if r['method']=='initialize': v={'protocolVersion':1,'agentCapabilities':{'sessionCapabilities':{'close':{}}}}
 elif r['method']=='session/new': v={'sessionId':'s1'}
 else: continue
 print(json.dumps({'jsonrpc':'2.0','id':r['id'],'result':v}),flush=True)
 if r['method']=='session/new':
  print(json.dumps({'jsonrpc':'2.0','method':'session/update','params':{'sessionId':'s1','update':{'sessionUpdate':'agent_message_chunk','content':{'type':'text','text':'queued before EOF'}}}}),flush=True)
  os.close(1)
  while True: time.sleep(60)
"#;
        let mut factory = DshFactory::new(config(
            "/usr/bin/python3",
            vec!["-u".into(), "-c".into(), script.into()],
        ));
        let (backend, mut events, pid) = factory.start().await.unwrap();
        assert_eq!(backend.new_session(Path::new("/tmp")).await.unwrap(), "s1");
        // Leave the single-item event queue full while the peer closes stdout.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(matches!(
            events.recv().await.unwrap(),
            BackendEvent::Update { .. }
        ));
        let before_cleanup = tokio::time::timeout(Duration::from_secs(1), events.recv()).await;
        let peer_still_alive = unsafe { libc::kill(pid as i32, 0) } == 0;
        // Always finish cleanup before asserting, including on the red path.
        let cleanup_started = tokio::time::Instant::now();
        let reaped =
            Backend::terminate_process_with_budget(&backend, Duration::from_millis(300)).await;
        assert!(reaped);
        assert!(
            cleanup_started.elapsed() < Duration::from_millis(600),
            "a short shutdown must tighten the monitor's existing cleanup deadline"
        );
        assert!(peer_still_alive, "Died must be observable before KILL/reap");
        assert!(
            matches!(before_cleanup, Ok(Some(BackendEvent::Died { .. }))),
            "EOF must be reported before the five-second TERM grace period"
        );
        assert!(
            !matches!(events.try_recv(), Ok(BackendEvent::Died { .. })),
            "one detected connection failure must produce one Died event"
        );
    }
}

#[cfg(test)]
mod history_validation_tests {
    use super::*;

    /// 真实 ACP 字节流保留不透明分页和恢复参数，无能力时不发送请求。
    #[tokio::test]
    async fn history_capabilities_and_wire_roundtrip() {
        let script = r#"import sys,json
enabled=sys.argv[1]=='yes'
for line in sys.stdin:
 r=json.loads(line);m=r['method'];i=r.get('id')
 if m=='initialize':
  caps={'close':{}}
  if enabled: caps.update({'list':{},'resume':{}})
  v={'protocolVersion':1,'agentCapabilities':{'sessionCapabilities':caps}}
 else:
  assert enabled, 'unsupported history request sent'
  p=r['params']
  if m=='session/list':
   assert p['cwd']=='/tmp' and p['cursor']=='page/+2='
   v={'sessions':[{'sessionId':'history:alpha/1','cwd':'/tmp','title':'history'}],'nextCursor':'next/+='}
  elif m=='session/resume':
   assert p['sessionId']=='history:alpha/1' and p['cwd']=='/tmp'
   v={}
  else: raise AssertionError(m)
 print(json.dumps({'jsonrpc':'2.0','id':i,'result':v}),flush=True)
"#;
        for enabled in [false, true] {
            let mut factory = DshFactory::new(Config {
                dsh_command: "/usr/bin/python3".into(),
                dsh_args: vec![
                    "-u".into(),
                    "-c".into(),
                    script.into(),
                    if enabled { "yes" } else { "no" }.into(),
                ],
                ..Config::default()
            });
            let (backend, _events, _) = factory.start().await.unwrap();
            assert_eq!(backend.capabilities().list, enabled);
            assert_eq!(backend.capabilities().resume, enabled);
            let page = backend
                .list_sessions(Some("/tmp".into()), Some("page/+2=".into()))
                .await;
            let resumed = backend
                .resume_session("history:alpha/1", Path::new("/tmp"))
                .await;
            if enabled {
                let page = page.unwrap();
                assert_eq!(page.sessions[0].dsh_session_id, "history:alpha/1");
                assert_eq!(page.next_cursor.as_deref(), Some("next/+="));
                resumed.unwrap();
            } else {
                assert_eq!(
                    page.unwrap_err().kind,
                    BackendErrorKind::CapabilityUnsupported
                );
                assert_eq!(
                    resumed.unwrap_err().kind,
                    BackendErrorKind::CapabilityUnsupported
                );
            }
            assert!(backend.terminate_process().await);
        }
    }
    /// SDK 会跳过缺少必需字段的历史条目，必须先拒绝原始坏页。
    #[test]
    fn malformed_history_entries_cannot_be_silently_skipped() {
        assert!(validate_remote(&serde_json::json!({"sessions":[{"sessionId":"ok","cwd":"/tmp"},{}],"nextCursor":"page2"})).is_err());
        assert!(validate_remote(&serde_json::json!({"sessions":[],"nextCursor":123})).is_err());
    }
}

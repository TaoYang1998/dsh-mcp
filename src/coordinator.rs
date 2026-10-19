//! Single writer for task admission, session ownership, permissions and stop evidence.
use crate::{
    acp::*,
    client::*,
    config::{Config, limits},
    events::*,
    history::*,
    permission::*,
    runtime_guard::{RuntimeGuard, wall_after, wall_now},
    task::*,
};
use agent_client_protocol::schema::v1::{
    RequestPermissionOutcome, RequestPermissionResponse, SelectedPermissionOutcome,
};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

type Reply = oneshot::Sender<Result<Value, TaskError>>;
#[derive(Clone, PartialEq, Eq)]
enum Fingerprint {
    Start(PathBuf, String),
    Follow(TaskId, String),
    Resume(PathBuf, String, String),
}
enum Op {
    Probe(String, Fingerprint),
    Start(String, PathBuf, PathBuf, String, Option<String>),
    Register(ClientId),
    Disconnect(ClientId),
    Attach(BridgeSessionId),
    ListTasks(ListTasksQuery),
    Status,
    Follow(String, TaskId, String),
    Get(TaskId, u64, usize),
    Cancel(TaskId),
    Resolve(TaskId, PermissionId, String),
    Close(TaskId),
}
enum Command {
    Call(ClientScope, Op, Reply),
    History(ClientScope, Option<PathBuf>, Option<String>, Reply),
    Shutdown(oneshot::Sender<bool>),
}

#[derive(Clone)]
pub struct CoordHandle {
    tx: mpsc::Sender<Command>,
    scope: ClientScope,
}

fn error(code: ErrorCode, message: impl Into<String>) -> TaskError {
    TaskError::new(code, message, false)
}
fn now() -> Instant {
    tokio::time::Instant::now().into_std()
}
fn validate_text(request: &str, prompt: &str) -> Result<(), TaskError> {
    if request.trim().is_empty() || request.len() > limits::REQUEST_ID_MAX {
        return Err(error(
            ErrorCode::InvalidArgument,
            "request_id must contain 1..128 UTF-8 bytes",
        ));
    }
    if prompt.trim().is_empty() || prompt.len() > limits::PROMPT_MAX {
        return Err(error(
            ErrorCode::InvalidArgument,
            "prompt must be nonempty and at most 64 KiB",
        ));
    }
    Ok(())
}

/// 校验并规范化目录，磁盘操作不阻塞协调器。
async fn canonical_directory(path: PathBuf) -> Result<PathBuf, TaskError> {
    if !path.is_absolute() {
        return Err(error(ErrorCode::InvalidArgument, "cwd must be absolute"));
    }
    tokio::task::spawn_blocking(move || {
        let canonical = std::fs::canonicalize(path)
            .map_err(|_| error(ErrorCode::InvalidArgument, "cwd unavailable"))?;
        if !canonical.is_dir() || serde_json::to_vec(&canonical).map_or(true, |v| v.len() > 4096) {
            return Err(error(ErrorCode::InvalidArgument, "invalid cwd"));
        }
        Ok(canonical)
    })
    .await
    .map_err(|_| error(ErrorCode::Internal, "directory lookup failed"))?
}
/// 历史能力错误与后端执行故障保持不同错误码。
fn history_error(e: BackendError) -> TaskError {
    let code = match e.kind {
        BackendErrorKind::CapabilityUnsupported => ErrorCode::CapabilityUnsupported,
        BackendErrorKind::ResponseTooLarge => ErrorCode::ResponseTooLarge,
        _ => ErrorCode::BackendUnavailable,
    };
    error(code, e.message)
}

impl CoordHandle {
    async fn call(&self, op: Op) -> Result<Value, TaskError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::Call(self.scope, op, tx))
            .await
            .map_err(|_| error(ErrorCode::BackendUnavailable, "coordinator stopped"))?;
        rx.await
            .map_err(|_| error(ErrorCode::BackendUnavailable, "coordinator stopped"))?
    }
    pub async fn start_task(
        &self,
        request: String,
        cwd: PathBuf,
        prompt: String,
    ) -> Result<Value, TaskError> {
        let found = self
            .call(Op::Probe(
                request.clone(),
                Fingerprint::Start(cwd.clone(), prompt.clone()),
            ))
            .await?;
        if !found.is_null() {
            return Ok(found);
        }
        validate_text(&request, &prompt)?;
        if !cwd.is_absolute() {
            return Err(error(
                ErrorCode::InvalidArgument,
                "cwd must be an existing absolute directory",
            ));
        }
        let path = cwd.clone();
        let canonical = tokio::task::spawn_blocking(move || {
            let canonical = std::fs::canonicalize(path)?;
            if !canonical.is_dir() {
                return Err(std::io::Error::other("not a directory"));
            }
            Ok::<_, std::io::Error>(canonical)
        })
        .await
        .map_err(|_| error(ErrorCode::Internal, "directory lookup failed"))?
        .map_err(|_| error(ErrorCode::InvalidArgument, "cwd is unavailable"))?;
        if serde_json::to_vec(&canonical).map_or(true, |b| b.len() > 4096) {
            return Err(error(
                ErrorCode::InvalidArgument,
                "cwd must be a directory with a bounded path",
            ));
        }
        self.call(Op::Start(request, cwd, canonical, prompt, None))
            .await
    }
    /// 连接身份由 daemon 赋予，不接受模型提供的 client_id。
    pub fn for_client(&self, id: ClientId) -> Self {
        Self {
            tx: self.tx.clone(),
            scope: ClientScope::Shared(id),
        }
    }
    /// 注册新连接，以便原子拒绝断连后的迟到操作。
    pub async fn register_client(&self, id: ClientId) -> Result<(), TaskError> {
        self.call(Op::Register(id)).await.map(|_| ())
    }
    /// 断连只释放归属，不影响已接纳任务。
    pub async fn disconnect_client(&self, id: ClientId) -> Result<(), TaskError> {
        self.call(Op::Disconnect(id)).await.map(|_| ())
    }
    /// 只读健康查询，不启动后端，不刷新会话 TTL。
    pub async fn get_status(&self) -> Result<Value, TaskError> {
        self.call(Op::Status).await
    }
    /// 显式认领无人拥有的存活会话，不抢占其他连接。
    pub async fn attach_session(&self, id: BridgeSessionId) -> Result<Value, TaskError> {
        self.call(Op::Attach(id)).await
    }
    /// 目录过滤使用物理路径，分页令牌绑定过滤条件。
    pub async fn list_tasks(&self, mut query: ListTasksQuery) -> Result<Value, TaskError> {
        if let Some(path) = query.cwd.take() {
            query.cwd = Some(canonical_directory(path).await?);
        }
        self.call(Op::ListTasks(query)).await
    }
    /// 列举仅启动一个共享 ACP，等待 I/O 不占用 actor。
    pub async fn list_sessions(
        &self,
        cwd: Option<PathBuf>,
        cursor: Option<String>,
    ) -> Result<Value, TaskError> {
        let cwd = match cwd {
            Some(p) => Some(canonical_directory(p).await?),
            None => None,
        };
        if cursor.as_ref().is_some_and(|c| c.len() > 4096) {
            return Err(error(ErrorCode::InvalidArgument, "cursor too long"));
        }
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::History(self.scope, cwd, cursor, tx))
            .await
            .map_err(|_| error(ErrorCode::BackendUnavailable, "coordinator stopped"))?;
        rx.await
            .map_err(|_| error(ErrorCode::BackendUnavailable, "coordinator stopped"))?
    }
    /// 原子接纳恢复轮次；任何恢复失败均不创建替代会话。
    pub async fn resume_task(
        &self,
        request: String,
        remote: String,
        cwd: PathBuf,
        prompt: String,
    ) -> Result<Value, TaskError> {
        let found = self
            .call(Op::Probe(
                request.clone(),
                Fingerprint::Resume(cwd.clone(), remote.clone(), prompt.clone()),
            ))
            .await?;
        if !found.is_null() {
            return Ok(found);
        }
        validate_text(&request, &prompt)?;
        if remote.is_empty() || remote.len() > limits::REMOTE_ID_MAX {
            return Err(error(
                ErrorCode::InvalidArgument,
                "invalid DSH session identifier",
            ));
        }
        let canonical = canonical_directory(cwd.clone()).await?;
        self.call(Op::Start(request, cwd, canonical, prompt, Some(remote)))
            .await
    }
    pub async fn follow_up(
        &self,
        request: String,
        task: TaskId,
        prompt: String,
    ) -> Result<Value, TaskError> {
        self.call(Op::Follow(request, task, prompt)).await
    }
    pub async fn get_task(
        &self,
        task: TaskId,
        cursor: u64,
        limit: usize,
    ) -> Result<Value, TaskError> {
        self.call(Op::Get(task, cursor, limit)).await
    }
    pub async fn cancel_task(&self, task: TaskId) -> Result<Value, TaskError> {
        self.call(Op::Cancel(task)).await
    }
    pub async fn resolve_permission(
        &self,
        task: TaskId,
        permission: PermissionId,
        option: String,
    ) -> Result<Value, TaskError> {
        self.call(Op::Resolve(task, permission, option)).await
    }
    pub async fn close_session(&self, task: TaskId) -> Result<Value, TaskError> {
        self.call(Op::Close(task)).await
    }
    /// True means all sessions closed, process reaped, and recovery marker synced away.
    pub async fn shutdown(&self) -> bool {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(Command::Shutdown(tx)).await.is_err() {
            return false;
        }
        rx.await.unwrap_or(false)
    }
}

enum Internal<B: Backend, F: BackendFactory<B = B>> {
    Started(
        F,
        Result<(B, mpsc::Receiver<BackendEvent>, u32), BackendError>,
    ),
    Created(u64, TaskId, Result<String, BackendError>),
    Prompt(u64, TaskId, Result<StopReasonView, BackendError>),
    Closed(u64, BridgeSessionId, Result<(), BackendError>),
    IsolationClosed(u64, BridgeSessionId, Result<(), BackendError>),
    CancelSent(u64, TaskId, Result<(), BackendError>),
    Delivered(u64, PermissionId, Result<(), String>),
    Terminated(bool),
    Cleared(bool),
    Listed(u64, Result<SessionPage, BackendError>, Reply),
}

pub fn spawn_coordinator<F: BackendFactory>(
    config: Config,
    factory: F,
    guard: Arc<RuntimeGuard>,
    recovery: bool,
) -> CoordHandle {
    let (tx, rx) = mpsc::channel(128);
    let (itx, irx) = mpsc::channel(128);
    let coord = Coordinator {
        config,
        factory: Some(factory),
        guard,
        backend: None,
        state: if recovery {
            BackendState::Unavailable
        } else {
            BackendState::Dormant
        },
        epoch: 1,
        recovery,
        initial_recovery: recovery,
        draining: false,
        ever_used: false,
        starting: false,
        startup_shutdown: None,
        isolation_reason: None,
        isolation_pending: HashSet::new(),
        shutdown_terminate_at: None,
        clients: HashSet::new(),
        owners: HashMap::new(),
        history_waiters: Vec::new(),
        history_inflight: 0,
        tasks: HashMap::new(),
        sessions: HashMap::new(),
        requests: HashMap::new(),
        prompts: HashMap::new(),
        compatibility_noted: HashSet::new(),
        permissions: PermissionManager::new(),
        tools: HashMap::new(),
        tool_order: VecDeque::new(),
        cancel_deadlines: HashMap::new(),
        itx,
        irx,
        rx,
        events: None,
        shutdown_waiters: Vec::new(),
        shutdown_deadline: None,
        terminating: false,
        termination_attempted: false,
        process_stopped: true,
        clearing: false,
        finished: false,
    };
    tokio::spawn(coord.run());
    CoordHandle {
        tx,
        scope: ClientScope::Standalone,
    }
}

struct Coordinator<F: BackendFactory> {
    config: Config,
    factory: Option<F>,
    guard: Arc<RuntimeGuard>,
    backend: Option<F::B>,
    state: BackendState,
    epoch: u64,
    recovery: bool,
    initial_recovery: bool,
    draining: bool,
    ever_used: bool,
    starting: bool,
    startup_shutdown: Option<StartupControl>,
    isolation_reason: Option<String>,
    isolation_pending: HashSet<BridgeSessionId>,
    shutdown_terminate_at: Option<Instant>,
    clients: HashSet<ClientId>,
    owners: HashMap<BridgeSessionId, ClientScope>,
    history_waiters: Vec<(Option<PathBuf>, Option<String>, Reply)>,
    history_inflight: usize,
    tasks: HashMap<TaskId, Task>,
    sessions: HashMap<BridgeSessionId, Session>,
    requests: HashMap<String, (TaskId, Fingerprint)>,
    prompts: HashMap<TaskId, String>,
    compatibility_noted: HashSet<TaskId>,
    permissions: PermissionManager,
    tools: HashMap<(BridgeSessionId, String), String>,
    tool_order: VecDeque<(BridgeSessionId, String)>,
    cancel_deadlines: HashMap<TaskId, Instant>,
    itx: mpsc::Sender<Internal<F::B, F>>,
    irx: mpsc::Receiver<Internal<F::B, F>>,
    rx: mpsc::Receiver<Command>,
    events: Option<mpsc::Receiver<BackendEvent>>,
    shutdown_waiters: Vec<oneshot::Sender<bool>>,
    shutdown_deadline: Option<Instant>,
    terminating: bool,
    termination_attempted: bool,
    process_stopped: bool,
    clearing: bool,
    finished: bool,
}

impl<F: BackendFactory> Coordinator<F> {
    async fn run(mut self) {
        let mut tick = tokio::time::interval(Duration::from_millis(10));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        while !self.finished {
            tokio::select! {
                command=self.rx.recv() => match command {
                    Some(Command::Call(scope,op,reply))=>{let r=self.on_call(scope,op);let _=reply.send(r);},
                    Some(Command::History(scope,cwd,cursor,reply))=>self.on_history(scope,cwd,cursor,reply),
                    Some(Command::Shutdown(reply))=>{self.shutdown_waiters.push(reply);self.begin_shutdown();},
                    None=>self.begin_shutdown(),
                },
                internal=self.irx.recv()=>if let Some(event)=internal {self.on_internal(event);},
                event=async { match self.events.as_mut() {Some(rx)=>rx.recv().await,None=>std::future::pending().await} }=>{
                    match event {Some(e)=>self.on_backend(e),None=>{
                        self.events=None;
                        if !self.intentional_transport_stop() {self.isolate_disconnected("ACP event stream closed".into());}
                    }}
                },
                _=tick.tick()=>self.maintenance(),
            }
            self.advance_shutdown();
        }
    }
    fn dedup(&self, request: &str, fingerprint: &Fingerprint) -> Result<Option<Value>, TaskError> {
        if let Some((id, previous)) = self.requests.get(request) {
            if previous != fingerprint {
                return Err(error(
                    ErrorCode::RequestConflict,
                    "request_id has different parameters",
                ));
            }
            return Ok(Some(self.accepted(*id, true)));
        }
        Ok(None)
    }
    fn accepted(&self, id: TaskId, duplicate: bool) -> Value {
        let task = &self.tasks[&id];
        json!({"task_id":id,"session_id":task.session_id,"status":task.status,"deduplicated":duplicate})
    }
    fn healthy(&self) -> Result<(), TaskError> {
        if self.initial_recovery {
            return Err(error(
                ErrorCode::RecoveryRequired,
                "cleanup must be acknowledged before dispatch",
            ));
        }
        if self.draining || self.state == BackendState::Unavailable {
            return Err(error(
                ErrorCode::BackendUnavailable,
                "backend is unavailable",
            ));
        }
        Ok(())
    }
    fn capacity(&mut self, new_session: bool) -> Result<(), TaskError> {
        self.evict(false);
        if self.tasks.values().filter(|t| t.status.is_active()).count()
            >= self.config.max_active_tasks
        {
            return Err(error(
                ErrorCode::CapacityExceeded,
                "active task capacity reached",
            ));
        }
        if new_session
            && self
                .sessions
                .values()
                .filter(|s| !matches!(s.state, SessionState::Closed | SessionState::Lost))
                .count()
                >= self.config.max_open_sessions
        {
            return Err(error(
                ErrorCode::CapacityExceeded,
                "open session capacity reached",
            ));
        }
        if self.tasks.len() >= self.config.max_task_records {
            self.evict(true);
        }
        if self.tasks.len() >= self.config.max_task_records {
            return Err(error(
                ErrorCode::CapacityExceeded,
                "task record capacity reached",
            ));
        }
        Ok(())
    }
    fn on_call(&mut self, scope: ClientScope, op: Op) -> Result<Value, TaskError> {
        self.live(scope)?;
        match op {
            Op::Register(id) => {
                if scope != ClientScope::Standalone {
                    return Err(error(ErrorCode::SessionNotOwned, "internal operation"));
                }
                self.clients.insert(id);
                Ok(Value::Null)
            }
            Op::Disconnect(id) => {
                if scope != ClientScope::Standalone {
                    return Err(error(ErrorCode::SessionNotOwned, "internal operation"));
                }
                self.clients.remove(&id);
                self.owners
                    .retain(|_, owner| *owner != ClientScope::Shared(id));
                Ok(Value::Null)
            }
            Op::Status => Ok(json!({
                "backend": {"state": self.state, "recovery_required": self.recovery},
                "draining": self.draining,
                "active_tasks": self.tasks.values().filter(|t| t.status.is_active()).count(),
                "open_sessions": self.sessions.values().filter(|s| s.state.counts_open()).count(),
                "task_records": self.tasks.len(),
                "task_retention_seconds": self.config.task_retention.as_secs(),
                "max_task_records": self.config.max_task_records,
            })),
            Op::Attach(sid) => {
                let session = self
                    .sessions
                    .get(&sid)
                    .ok_or_else(|| error(ErrorCode::TaskNotFound, "session not found"))?;
                if matches!(session.state, SessionState::Closed | SessionState::Lost) {
                    return Err(error(
                        ErrorCode::SessionLost,
                        "session is closed or lost; inspect backend health before explicit resume",
                    ));
                }
                if self.owners.get(&sid).is_some_and(|owner| *owner != scope) {
                    return Err(error(
                        ErrorCode::SessionOwned,
                        "session belongs to a live connection",
                    ));
                }
                self.owners.insert(sid, scope);
                Ok(
                    json!({"session_id":sid,"latest_task_id":session.latest_task_id,"session_state":session.state,"owner_state":"mine"}),
                )
            }
            Op::ListTasks(query) => self.task_page(scope, query),
            Op::Probe(r, fp) => Ok(self.dedup(&r, &fp)?.unwrap_or(Value::Null)),
            Op::Start(request, raw, cwd, prompt, resume) => {
                let fp = match &resume {
                    Some(remote) => Fingerprint::Resume(raw, remote.clone(), prompt.clone()),
                    None => Fingerprint::Start(raw, prompt.clone()),
                };
                if let Some(out) = self.dedup(&request, &fp)? {
                    return Ok(out);
                }
                validate_text(&request, &prompt)?;
                self.healthy()?;
                self.capacity(true)?;
                if resume.as_ref().is_some_and(|remote| {
                    self.sessions
                        .values()
                        .any(|s| s.state.counts_open() && s.dsh_session_id.as_ref() == Some(remote))
                        || self.tasks.values().any(|t| {
                            t.status.is_active()
                                && t.resumed_from_dsh_session_id.as_ref() == Some(remote)
                        })
                }) {
                    return Err(error(ErrorCode::SessionBusy, "DSH session already held"));
                }
                let id = Uuid::new_v4();
                let sid = Uuid::new_v4();
                self.sessions.insert(
                    sid,
                    Session {
                        session_id: sid,
                        dsh_session_id: None,
                        epoch: self.epoch,
                        cwd,
                        state: SessionState::Opening,
                        active_task_id: Some(id),
                        latest_task_id: Some(id),
                        last_activity: now(),
                    },
                );
                self.tasks
                    .insert(id, Task::new(id, request.clone(), sid, None));
                self.requests.insert(request, (id, fp));
                self.tasks.get_mut(&id).unwrap().resumed_from_dsh_session_id = resume;
                self.owners.insert(sid, scope);
                self.prompts.insert(id, prompt);
                self.tasks
                    .get_mut(&id)
                    .unwrap()
                    .events
                    .push(EventKind::StatusChange, json!({"status":"starting"}));
                if self.state == BackendState::Ready {
                    self.open(id);
                } else if self.state == BackendState::Dormant {
                    self.start_backend();
                }
                Ok(self.accepted(id, false))
            }
            Op::Follow(request, parent, prompt) => {
                let fp = Fingerprint::Follow(parent, prompt.clone());
                if let Some(out) = self.dedup(&request, &fp)? {
                    return Ok(out);
                }
                validate_text(&request, &prompt)?;
                self.healthy()?;
                let t = self
                    .tasks
                    .get(&parent)
                    .ok_or_else(|| error(ErrorCode::TaskNotFound, "task not found"))?;
                let sid = t.session_id;
                self.owned(scope, sid)?;
                let s = &self.sessions[&sid];
                if s.latest_task_id != Some(parent) {
                    let mut e = error(ErrorCode::StaleTask, "followup must use latest task");
                    e.details = Some(json!({"latest_task_id":s.latest_task_id}));
                    return Err(e);
                }
                let code = match s.state {
                    SessionState::Opening | SessionState::Busy => Some(ErrorCode::SessionBusy),
                    SessionState::Closing => Some(ErrorCode::SessionClosing),
                    SessionState::Closed => Some(ErrorCode::SessionClosed),
                    SessionState::Lost => Some(ErrorCode::SessionLost),
                    SessionState::Idle => None,
                };
                if let Some(code) = code {
                    return Err(error(code, "session is unavailable for followup"));
                }
                if !t.status.is_terminal()
                    || t.execution_state == ExecutionState::Unknown
                    || self.state != BackendState::Ready
                {
                    return Err(error(ErrorCode::SessionBusy, "session has no settled turn"));
                }
                self.capacity(false)?;
                let id = Uuid::new_v4();
                self.tasks
                    .insert(id, Task::new(id, request.clone(), sid, Some(parent)));
                self.requests.insert(request, (id, fp));
                self.prompts.insert(id, prompt);
                let s = self.sessions.get_mut(&sid).unwrap();
                s.active_task_id = Some(id);
                s.latest_task_id = Some(id);
                s.last_activity = now();
                s.state = SessionState::Busy;
                self.send_prompt(id);
                Ok(self.accepted(id, false))
            }
            Op::Get(id, cursor, limit) => self.snapshot(id, cursor, limit),
            Op::Cancel(id) => {
                if !self.tasks.contains_key(&id) {
                    return Err(error(ErrorCode::TaskNotFound, "task not found"));
                }
                self.owned(scope, self.tasks[&id].session_id)?;
                self.cancel(id, None);
                let t = &self.tasks[&id];
                Ok(json!({"task_id":id,"status":t.status,"execution_state":t.execution_state}))
            }
            Op::Close(id) => {
                let t = self
                    .tasks
                    .get(&id)
                    .ok_or_else(|| error(ErrorCode::TaskNotFound, "task not found"))?;
                let sid = t.session_id;
                self.owned(scope, sid)?;
                match self.sessions[&sid].state {
                    SessionState::Opening | SessionState::Busy => {
                        return Err(error(
                            ErrorCode::SessionBusy,
                            "cancel and await task settlement first",
                        ));
                    }
                    SessionState::Lost => {
                        return Err(error(ErrorCode::SessionLost, "session was lost"));
                    }
                    SessionState::Idle => self.close(sid),
                    _ => (),
                }
                Ok(json!({"session_id":sid,"state":self.sessions[&sid].state}))
            }
            Op::Resolve(task, pid, option) => {
                let t = self
                    .tasks
                    .get(&task)
                    .ok_or_else(|| error(ErrorCode::TaskNotFound, "task not found"))?;
                self.owned(scope, t.session_id)?;
                self.resolve(task, pid, option)
            }
        }
    }
    /// 失效连接不能在新归属生效后继续写入。
    fn live(&self, scope: ClientScope) -> Result<(), TaskError> {
        if let ClientScope::Shared(id) = scope
            && !self.clients.contains(&id)
        {
            return Err(error(ErrorCode::ClientDisconnected, "client disconnected"));
        }
        Ok(())
    }
    /// 写操作在唯一状态写入点核验当前归属。
    fn owned(&self, scope: ClientScope, sid: BridgeSessionId) -> Result<(), TaskError> {
        if self.owners.get(&sid) != Some(&scope) {
            return Err(error(
                ErrorCode::SessionNotOwned,
                "attach the unowned session before writing",
            ));
        }
        Ok(())
    }
    /// 列举不等待 ACP I/O，排队的启动等待者也有数量上限。
    fn on_history(
        &mut self,
        scope: ClientScope,
        cwd: Option<PathBuf>,
        cursor: Option<String>,
        reply: Reply,
    ) {
        if let Err(e) = self.live(scope).and_then(|_| self.healthy()) {
            let _ = reply.send(Err(e));
            return;
        }
        if self.state == BackendState::Ready {
            self.spawn_list(cwd, cursor, reply);
            return;
        }
        if self.history_waiters.len() >= 32 {
            let _ = reply.send(Err(error(
                ErrorCode::CapacityExceeded,
                "too many history requests",
            )));
            return;
        }
        self.history_waiters.push((cwd, cursor, reply));
        if self.state == BackendState::Dormant {
            self.start_backend();
        }
    }
    /// 一个异步列举结果携带 epoch，隔离后的迟到结果不能伪装健康。
    fn spawn_list(&mut self, cwd: Option<PathBuf>, cursor: Option<String>, reply: Reply) {
        if self.history_inflight >= 32 {
            let _ = reply.send(Err(error(
                ErrorCode::CapacityExceeded,
                "too many history requests",
            )));
            return;
        }
        // 调用方取消不代表 ACP 已终止请求，收到后台完成事件后才能归还额度。
        self.history_inflight += 1;
        let b = self.backend.clone().unwrap();
        let tx = self.itx.clone();
        let epoch = self.epoch;
        let budget = self.config.close_timeout;
        tokio::spawn(async move {
            let result = if !b.capabilities().list {
                Err(BackendError::new(
                    BackendErrorKind::CapabilityUnsupported,
                    "session/list unsupported",
                ))
            } else {
                tokio::time::timeout(budget, b.list_sessions(cwd, cursor))
                    .await
                    .unwrap_or_else(|_| {
                        Err(BackendError::new(
                            BackendErrorKind::Timeout,
                            "session/list timed out",
                        ))
                    })
                    .and_then(SessionPage::bounded)
            };
            let _ = tx.send(Internal::Listed(epoch, result, reply)).await;
        });
    }
    /// 分页绑定过滤条件和调用连接，避免游标串用。
    fn task_page(&self, scope: ClientScope, q: ListTasksQuery) -> Result<Value, TaskError> {
        if q.limit == 0 || q.limit > 100 {
            return Err(error(ErrorCode::InvalidArgument, "limit must be 1..100"));
        }
        let filter = serde_json::to_string(&(format!("{scope:?}"), q.scope, &q.cwd)).unwrap();
        let after = if let Some(cursor) = q.cursor {
            if cursor.len() > 8192 {
                return Err(error(ErrorCode::InvalidArgument, "cursor too long"));
            }
            let (f, time, id): (String, String, Uuid) = serde_json::from_str(&cursor)
                .map_err(|_| error(ErrorCode::InvalidArgument, "invalid cursor"))?;
            if f != filter {
                return Err(error(ErrorCode::InvalidArgument, "cursor filter mismatch"));
            }
            Some((time, id))
        } else {
            None
        };
        let mut rows: Vec<_> = self
            .tasks
            .values()
            .filter(|t| {
                let s = &self.sessions[&t.session_id];
                let owner = self.owners.get(&t.session_id);
                q.cwd.as_ref().is_none_or(|cwd| *cwd == s.cwd)
                    && match q.scope {
                        TaskScope::Mine => owner == Some(&scope),
                        TaskScope::Unowned => owner.is_none(),
                        TaskScope::All => true,
                    }
            })
            .filter(|t| {
                after
                    .as_ref()
                    .is_none_or(|(time, id)| (&t.created_at, t.task_id) > (time, *id))
            })
            .collect();
        rows.sort_by(|a, b| (&a.created_at, a.task_id).cmp(&(&b.created_at, b.task_id)));
        let more = rows.len() > q.limit;
        rows.truncate(q.limit);
        let next = if more {
            rows.last()
                .map(|t| serde_json::to_string(&(filter, &t.created_at, t.task_id)).unwrap())
        } else {
            None
        };
        let tasks: Vec<_> = rows
            .into_iter()
            .map(|t| {
                let s = &self.sessions[&t.session_id];
                let owner = self.owners.get(&t.session_id);
                let owner_state = if owner == Some(&scope) {
                    "mine"
                } else if owner.is_none() {
                    "unowned"
                } else {
                    "other"
                };
                json!({
                    "task_id": t.task_id,
                    "session_id": t.session_id,
                    "status": t.status,
                    "cwd": s.cwd,
                    "latest_task_id": s.latest_task_id,
                    "owner_state": owner_state,
                    "followup_available": s.state == SessionState::Idle
                        && s.latest_task_id == Some(t.task_id)
                        && !self.draining && self.state == BackendState::Ready,
                })
            })
            .collect();
        let result = json!({"tasks":tasks,"next_cursor":next});
        if serde_json::to_vec(&result).map_or(true, |v| v.len() > limits::TASKVIEW_BUDGET) {
            return Err(error(
                ErrorCode::ResponseTooLarge,
                "task page too large; use smaller limit",
            ));
        }
        Ok(result)
    }
    fn start_backend(&mut self) {
        self.state = BackendState::Starting;
        self.starting = true;
        self.process_stopped = false;
        let mut factory = self.factory.take().expect("one startup");
        self.startup_shutdown = factory.startup_shutdown_handle();
        let guard = self.guard.clone();
        let tx = self.itx.clone();
        tokio::spawn(async move {
            let g = guard.clone();
            let marked = tokio::task::spawn_blocking(move || g.mark_started(Uuid::new_v4())).await;
            let result = match marked {
                Ok(Ok(())) => {
                    let r = factory.start().await;
                    if let Ok((ref backend, _, pid)) = r {
                        let g = guard.clone();
                        if pid != 0
                            && !matches!(
                                tokio::task::spawn_blocking(move || g.mark_child_pid(pid)).await,
                                Ok(Ok(()))
                            )
                        {
                            backend.terminate_process().await;
                            Err(BackendError::new(
                                BackendErrorKind::Unavailable,
                                "cannot persist child lifecycle",
                            ))
                        } else {
                            r
                        }
                    } else {
                        r
                    }
                }
                _ => Err(BackendError::new(
                    BackendErrorKind::Unavailable,
                    "cannot persist execution marker",
                )),
            };
            if let Err(mpsc::error::SendError(Internal::Started(_, Ok((backend, _, _))))) =
                tx.send(Internal::Started(factory, result)).await
            {
                backend.terminate_process().await;
            }
        });
    }
    fn open(&mut self, id: TaskId) {
        let t = self.tasks.get_mut(&id).unwrap();
        if t.status == TaskStatus::Cancelling {
            self.settle(
                id,
                SettleOutcome::Cancelled {
                    execution: ExecutionState::NotStarted,
                },
            );
            self.sessions
                .get_mut(&self.tasks[&id].session_id)
                .unwrap()
                .state = SessionState::Closed;
            return;
        }
        t.phase = TaskPhase::AwaitSession;
        let sid = t.session_id;
        let cwd = self.sessions[&sid].cwd.clone();
        let b = self.backend.clone().unwrap();
        let tx = self.itx.clone();
        let epoch = self.epoch;
        let budget = self.config.start_timeout;
        let resume = t.resumed_from_dsh_session_id.clone();
        self.ever_used = true;
        tokio::spawn(async move {
            let exists = tokio::task::spawn_blocking({
                let cwd = cwd.clone();
                move || cwd.is_dir()
            })
            .await
            .unwrap_or(false);
            let result = if exists {
                tokio::time::timeout(budget, async {
                    if let Some(remote) = resume {
                        if !b.capabilities().resume {
                            return Err(BackendError::new(
                                BackendErrorKind::CapabilityUnsupported,
                                "session/resume unsupported",
                            ));
                        }
                        b.resume_session(&remote, &cwd).await?;
                        Ok(remote)
                    } else {
                        b.new_session(&cwd).await
                    }
                })
                .await
                .unwrap_or_else(|_| {
                    Err(BackendError::new(
                        BackendErrorKind::Timeout,
                        "session/new timed out",
                    ))
                })
            } else {
                Err(BackendError::new(
                    BackendErrorKind::Rejected,
                    "cwd is no longer available",
                ))
            };
            let _ = tx.send(Internal::Created(epoch, id, result)).await;
        });
    }
    fn send_prompt(&mut self, id: TaskId) {
        let t = self.tasks.get_mut(&id).unwrap();
        if t.status.is_terminal() || t.status == TaskStatus::Cancelling {
            return;
        }
        t.phase = TaskPhase::PromptSent;
        t.status = TaskStatus::Running;
        t.execution_state = ExecutionState::InProgress;
        t.started_at = Some(wall_now());
        t.events
            .push(EventKind::StatusChange, json!({"status":"running"}));
        let s = self.sessions.get_mut(&t.session_id).unwrap();
        s.state = SessionState::Busy;
        let remote = s.dsh_session_id.clone().unwrap();
        let b = self.backend.clone().unwrap();
        let prompt = self.prompts.remove(&id).unwrap();
        let epoch = self.epoch;
        let tx = self.itx.clone();
        tokio::spawn(async move {
            let result = b.prompt(&remote, prompt).await;
            let _ = tx.send(Internal::Prompt(epoch, id, result)).await;
        });
    }
    fn settle(&mut self, id: TaskId, outcome: SettleOutcome) {
        let Some(t) = self.tasks.get_mut(&id) else {
            return;
        };
        if !t.settle(outcome, &wall_now(), now()) {
            return;
        }
        t.events.push(
            EventKind::StatusChange,
            json!({"status":t.status,"execution_state":t.execution_state}),
        );
        let sid = t.session_id;
        self.prompts.remove(&id);
        self.compatibility_noted.remove(&id);
        self.cancel_deadlines.remove(&id);
        self.revoke(id);
        let s = self.sessions.get_mut(&sid).unwrap();
        if s.active_task_id == Some(id) {
            s.active_task_id = None;
            s.last_activity = now();
            if !matches!(
                s.state,
                SessionState::Closing | SessionState::Closed | SessionState::Lost
            ) {
                s.state = if s.dsh_session_id.is_some() {
                    SessionState::Idle
                } else {
                    SessionState::Closed
                };
            }
        }
        self.tools.retain(|(session, _), _| *session != sid);
        self.tool_order.retain(|(session, _)| *session != sid);
        if self.draining && self.sessions[&sid].state == SessionState::Idle {
            self.close(sid);
        }
    }
    fn cancel(&mut self, id: TaskId, cause: Option<TaskError>) {
        let Some(t) = self.tasks.get_mut(&id) else {
            return;
        };
        if t.status.is_terminal() {
            return;
        }
        if t.error.is_none() {
            t.error = cause;
        }
        if t.status == TaskStatus::Cancelling {
            return;
        }
        t.status = TaskStatus::Cancelling;
        t.cancel_requested_at = Some(wall_now());
        t.events
            .push(EventKind::StatusChange, json!({"status":"cancelling"}));
        let phase = t.phase;
        let sid = t.session_id;
        self.revoke(id);
        match phase {
            TaskPhase::AwaitBackend => {
                self.settle(
                    id,
                    SettleOutcome::Cancelled {
                        execution: ExecutionState::NotStarted,
                    },
                );
                self.sessions.get_mut(&sid).unwrap().state = SessionState::Closed;
            }
            TaskPhase::AwaitSession => (),
            TaskPhase::PromptSent => {
                let b = self.backend.clone().unwrap();
                let remote = self.sessions[&sid].dsh_session_id.clone().unwrap();
                let tx = self.itx.clone();
                let epoch = self.epoch;
                let budget = self.config.close_timeout;
                self.cancel_deadlines
                    .insert(id, now() + self.config.cancel_timeout);
                tokio::spawn(async move {
                    let r = tokio::time::timeout(budget, b.cancel(&remote))
                        .await
                        .unwrap_or_else(|_| {
                            Err(BackendError::new(
                                BackendErrorKind::Timeout,
                                "cancel write timed out",
                            ))
                        });
                    let _ = tx.send(Internal::CancelSent(epoch, id, r)).await;
                });
            }
        }
    }
    fn close(&mut self, sid: BridgeSessionId) {
        let Some(s) = self.sessions.get_mut(&sid) else {
            return;
        };
        if matches!(
            s.state,
            SessionState::Closing | SessionState::Closed | SessionState::Lost
        ) {
            return;
        }
        let Some(remote) = s.dsh_session_id.clone() else {
            return;
        };
        s.state = SessionState::Closing;
        let b = self.backend.clone().unwrap();
        let tx = self.itx.clone();
        let epoch = self.epoch;
        let budget = self.config.close_timeout;
        tokio::spawn(async move {
            let r = tokio::time::timeout(budget, b.close_session(&remote))
                .await
                .unwrap_or_else(|_| {
                    Err(BackendError::new(
                        BackendErrorKind::Timeout,
                        "session/close timed out",
                    ))
                });
            let _ = tx.send(Internal::Closed(epoch, sid, r)).await;
        });
    }
    fn drain_updates(&mut self) {
        for _ in 0..limits::ACP_QUEUE_MAX_ITEMS {
            let event = self.events.as_mut().and_then(|rx| rx.try_recv().ok());
            match event {
                Some(e) => self.on_backend(e),
                None => break,
            }
        }
    }
    fn on_internal(&mut self, event: Internal<F::B, F>) {
        match event {
            Internal::Listed(epoch, result, reply) => {
                self.history_inflight = self.history_inflight.saturating_sub(1);
                let result = if epoch != self.epoch {
                    Err(error(
                        ErrorCode::BackendUnavailable,
                        "backend changed during history query",
                    ))
                } else {
                    result.map_err(history_error).and_then(|page| {
                        serde_json::to_value(page)
                            .map_err(|_| error(ErrorCode::Internal, "history serialization failed"))
                    })
                };
                let _ = reply.send(result);
            }
            Internal::Started(factory, result) => {
                self.starting = false;
                // Keep the owner's handle after successful startup: shutdown
                // must also shorten cleanup already begun by its monitor.
                self.factory = Some(factory);
                match result {
                    Ok((b, rx, _)) => {
                        self.backend = Some(b);
                        self.events = Some(rx);
                        if self.state == BackendState::Unavailable || self.recovery {
                            self.terminate();
                            return;
                        }
                        self.state = if self.draining {
                            BackendState::Draining
                        } else {
                            BackendState::Ready
                        };
                        let waiting: Vec<_> = self
                            .tasks
                            .values()
                            .filter(|t| t.status == TaskStatus::Starting)
                            .map(|t| t.task_id)
                            .collect();
                        let lists = std::mem::take(&mut self.history_waiters);
                        for (cwd, cursor, reply) in lists {
                            if self.draining {
                                let _ = reply.send(Err(error(
                                    ErrorCode::BackendUnavailable,
                                    "shutting down",
                                )));
                            } else {
                                self.spawn_list(cwd, cursor, reply);
                            }
                        }
                        for id in waiting {
                            if self.draining {
                                self.cancel(id, None);
                            } else {
                                self.open(id);
                            }
                        }
                    }
                    Err(e) => {
                        for (_, _, reply) in self.history_waiters.drain(..) {
                            let _ = reply.send(Err(history_error(e.clone())));
                        }
                        self.process_stopped = matches!(e.kind, BackendErrorKind::StartupClean);
                        if self.process_stopped && !self.ever_used {
                            self.state = if self.draining {
                                BackendState::Draining
                            } else {
                                BackendState::Dormant
                            };
                            let ids: Vec<_> = self
                                .tasks
                                .values()
                                .filter(|t| t.status.is_active())
                                .map(|t| t.task_id)
                                .collect();
                            for id in ids {
                                self.settle(
                                    id,
                                    SettleOutcome::Failed {
                                        error: error(
                                            ErrorCode::BackendUnavailable,
                                            e.message.clone(),
                                        ),
                                        execution: ExecutionState::NotStarted,
                                    },
                                );
                            }
                            // Clear is asynchronous; hold admissions until its result.
                            self.clearing = true;
                            self.state = BackendState::Starting;
                            let guard = self.guard.clone();
                            let tx = self.itx.clone();
                            tokio::spawn(async move {
                                let ok = tokio::task::spawn_blocking(move || guard.clear())
                                    .await
                                    .is_ok_and(|r| r.is_ok());
                                let _ = tx.send(Internal::Cleared(ok)).await;
                            });
                        } else {
                            self.isolate_disconnected(e.message);
                        }
                    }
                }
            }
            Internal::Created(epoch, id, result) => {
                if epoch != self.epoch || !self.tasks.get(&id).is_some_and(|t| t.status.is_active())
                {
                    return;
                }
                let sid = self.tasks[&id].session_id;
                match result {
                    Ok(remote) => {
                        if remote.is_empty()
                            || remote.len() > limits::REMOTE_ID_MAX
                            || self.sessions.values().any(|s| {
                                s.session_id != sid
                                    && s.state.counts_open()
                                    && s.dsh_session_id.as_ref() == Some(&remote)
                            })
                        {
                            self.isolate("invalid or reused ACP session identifier".into());
                            return;
                        }
                        self.sessions.get_mut(&sid).unwrap().dsh_session_id = Some(remote);
                        if self.tasks[&id].status == TaskStatus::Cancelling || self.draining {
                            self.close(sid);
                        } else {
                            self.send_prompt(id);
                        }
                    }
                    Err(e)
                        if matches!(
                            e.kind,
                            BackendErrorKind::Rejected | BackendErrorKind::CapabilityUnsupported
                        ) =>
                    {
                        self.sessions.get_mut(&sid).unwrap().state = SessionState::Closed;
                        self.settle(
                            id,
                            SettleOutcome::Failed {
                                error: history_error(e),
                                execution: ExecutionState::NotStarted,
                            },
                        );
                    }
                    Err(e) => self.isolate_backend_error(e, None),
                }
            }
            Internal::Prompt(epoch, id, result) => {
                // ACP callbacks preceding the response must be reflected in the final snapshot.
                self.drain_updates();
                if epoch != self.epoch || !self.tasks.get(&id).is_some_and(|t| t.status.is_active())
                {
                    return;
                }
                match result {
                    Ok(reason) => {
                        let name = match &reason {
                            StopReasonView::EndTurn => "end_turn".into(),
                            StopReasonView::Cancelled => "cancelled".into(),
                            StopReasonView::MaxTokens => "max_tokens".into(),
                            StopReasonView::MaxTurnRequests => "max_turn_requests".into(),
                            StopReasonView::Refusal => "refusal".into(),
                            StopReasonView::Other(s) => s.clone(),
                        };
                        self.tasks.get_mut(&id).unwrap().stop_reason = Some(name.clone());
                        let outcome = if let Some(error) = self.tasks[&id].error.clone() {
                            SettleOutcome::Failed {
                                error,
                                execution: ExecutionState::Stopped,
                            }
                        } else {
                            match reason {
                                StopReasonView::EndTurn => {
                                    SettleOutcome::Completed { stop_reason: name }
                                }
                                StopReasonView::Cancelled => SettleOutcome::Cancelled {
                                    execution: ExecutionState::Stopped,
                                },
                                _ => SettleOutcome::Failed {
                                    error: error(
                                        ErrorCode::BackendUnavailable,
                                        format!("ACP stopped: {name}"),
                                    ),
                                    execution: ExecutionState::Stopped,
                                },
                            }
                        };
                        self.settle(id, outcome);
                    }
                    Err(e) if matches!(e.kind, BackendErrorKind::Rejected) => self.settle(
                        id,
                        SettleOutcome::Failed {
                            error: self.tasks[&id]
                                .error
                                .clone()
                                .unwrap_or_else(|| error(ErrorCode::BackendUnavailable, e.message)),
                            execution: ExecutionState::Stopped,
                        },
                    ),
                    Err(e) => self.isolate_backend_error(e, None),
                }
            }
            Internal::Closed(epoch, sid, result) => {
                self.drain_updates();
                if epoch != self.epoch {
                    if epoch.checked_add(1) == Some(self.epoch)
                        && self.isolation_pending.contains(&sid)
                    {
                        self.isolation_closed(self.epoch, sid, result);
                    }
                    return;
                }
                if let Err(e) = result {
                    self.isolate_backend_error(e, Some(sid));
                    return;
                }
                let Some(s) = self.sessions.get_mut(&sid) else {
                    return;
                };
                s.state = SessionState::Closed;
                if let Some(id) = s.active_task_id {
                    let t = &self.tasks[&id];
                    let execution = if t.phase == TaskPhase::PromptSent {
                        ExecutionState::Stopped
                    } else {
                        ExecutionState::NotStarted
                    };
                    let outcome = if let Some(e) = t.error.clone() {
                        SettleOutcome::Failed {
                            error: e,
                            execution,
                        }
                    } else {
                        SettleOutcome::Cancelled { execution }
                    };
                    self.settle(id, outcome);
                }
            }
            Internal::IsolationClosed(epoch, sid, result) => {
                self.isolation_closed(epoch, sid, result)
            }
            Internal::CancelSent(epoch, id, Err(e))
                if epoch == self.epoch
                    && self
                        .tasks
                        .get(&id)
                        .is_some_and(|t| t.status == TaskStatus::Cancelling) =>
            {
                self.isolate_backend_error(e, None)
            }
            Internal::CancelSent(..) => (),
            Internal::Delivered(epoch, pid, result) => {
                if epoch != self.epoch {
                    return;
                }
                if let Some(id) = self.permissions.task_of(pid) {
                    self.permissions.mark_delivery(pid, result.clone());
                    if let Some(t) = self.tasks.get_mut(&id) {
                        t.events.push(EventKind::PermissionResolved,json!({"permission_id":pid,"delivery":if result.is_ok(){"sent"}else{"failed"}}));
                    }
                }
                if let Err(e) = result {
                    self.isolate_disconnected(format!("permission response failed: {e}"));
                }
            }
            Internal::Terminated(ok) => {
                self.terminating = false;
                self.process_stopped = ok;
                if !ok {
                    self.recovery = true;
                }
            }
            Internal::Cleared(ok) => {
                self.clearing = false;
                if !ok {
                    self.recovery = true;
                    self.state = BackendState::Unavailable;
                } else if self.draining {
                    self.finish_shutdown(true);
                } else {
                    self.state = BackendState::Dormant;
                    if self
                        .tasks
                        .values()
                        .any(|t| t.status == TaskStatus::Starting)
                    {
                        self.start_backend();
                    }
                }
            }
        }
    }
    fn route(&self, remote: &str) -> Option<(BridgeSessionId, TaskId)> {
        self.sessions
            .values()
            .find(|s| {
                s.epoch == self.epoch
                    && s.dsh_session_id.as_deref() == Some(remote)
                    && matches!(s.state, SessionState::Busy | SessionState::Closing)
            })
            .and_then(|s| s.active_task_id.map(|task| (s.session_id, task)))
    }
    fn on_backend(&mut self, event: BackendEvent) {
        let epoch = match &event {
            BackendEvent::Died { epoch, .. }
            | BackendEvent::Update { epoch, .. }
            | BackendEvent::Permission { epoch, .. }
            | BackendEvent::PermissionInvalid { epoch, .. } => *epoch,
        };
        // Real callbacks belong to this receiver's connection (0); fake callbacks
        // can explicitly exercise rejection of callbacks from a different epoch.
        if epoch != 0 && epoch != self.epoch {
            if let BackendEvent::Permission { responder, .. } = event {
                let _ = responder.reply(RequestPermissionResponse::new(
                    RequestPermissionOutcome::Cancelled,
                ));
            }
            return;
        }
        match event {
            BackendEvent::PermissionInvalid {
                dsh_session_id,
                reason,
                ..
            } => {
                if let Some((_, id)) = self.route(&dsh_session_id) {
                    self.cancel(
                        id,
                        Some(error(ErrorCode::PermissionPayloadTooLarge, reason)),
                    );
                }
            }
            BackendEvent::Died { reason, .. } => {
                if !self.intentional_transport_stop() {
                    self.isolate_disconnected(reason);
                }
            }
            BackendEvent::Update {
                dsh_session_id,
                update,
                ..
            } => {
                let Some((sid, id)) = self.route(&dsh_session_id) else {
                    return;
                };
                let t = self.tasks.get_mut(&id).unwrap();
                if t.status.is_terminal() {
                    return;
                }
                let data = serde_json::to_value(&update).unwrap_or(Value::Null);
                let kind = match &update {
                    SessionUpdateView::AgentMessageChunk { text, message_id } => {
                        t.result_agg.push_chunk(text, message_id.as_deref());
                        if message_id.is_none() && self.compatibility_noted.insert(id) {
                            t.events.push(
                                EventKind::CompatNote,
                                json!({"reason":"missing_message_id","result_source":"turn_text"}),
                            );
                        }
                        EventKind::AgentMessage
                    }
                    SessionUpdateView::AgentThoughtChunk { .. } => EventKind::AgentThought,
                    SessionUpdateView::ToolCall {
                        tool_call_id,
                        title,
                        ..
                    } => {
                        let key = (sid, tool_call_id.clone());
                        if !self.tools.contains_key(&key) {
                            self.tool_order.push_back(key.clone());
                        }
                        self.tools.insert(
                            key,
                            truncate_utf8(title, limits::TOOL_SUMMARY_MAX / 6).0.into(),
                        );
                        EventKind::ToolCall
                    }
                    SessionUpdateView::ToolCallUpdate {
                        tool_call_id,
                        summary,
                        ..
                    } => {
                        if let Some(summary) = summary {
                            let key = (sid, tool_call_id.clone());
                            if !self.tools.contains_key(&key) {
                                self.tool_order.push_back(key.clone());
                            }
                            self.tools.insert(
                                key,
                                truncate_utf8(summary, limits::TOOL_SUMMARY_MAX / 6)
                                    .0
                                    .into(),
                            );
                        }
                        EventKind::ToolCallUpdate
                    }
                    SessionUpdateView::ConfigUpdate { .. } => EventKind::ConfigUpdate,
                    SessionUpdateView::Usage { .. } => EventKind::Usage,
                    SessionUpdateView::Other { .. } => EventKind::CompatNote,
                };
                t.events.push(kind, data);
                while self.tool_order.len() > limits::TOOL_SUMMARY_CACHE {
                    if let Some(key) = self.tool_order.pop_front() {
                        self.tools.remove(&key);
                    }
                }
            }
            BackendEvent::Permission {
                dsh_session_id,
                request,
                responder,
                ..
            } => {
                let Some((sid, id)) = self.route(&dsh_session_id) else {
                    let _ = responder.reply(RequestPermissionResponse::new(
                        RequestPermissionOutcome::Cancelled,
                    ));
                    return;
                };
                if self.tasks[&id].status == TaskStatus::Cancelling
                    || self.sessions[&sid].state == SessionState::Closing
                {
                    let _ = responder.reply(RequestPermissionResponse::new(
                        RequestPermissionOutcome::Cancelled,
                    ));
                    return;
                }
                let summary = request
                    .tool_call_id
                    .as_ref()
                    .and_then(|key| self.tools.get(&(sid, key.clone())))
                    .cloned();
                // Lifecycle titles are summaries, not a complete copy of tool inputs.
                let incomplete = true;
                match self.permissions.register(
                    id,
                    sid,
                    self.epoch,
                    request.tool_call_id,
                    summary,
                    incomplete,
                    request.options,
                    responder,
                    now() + self.config.permission_timeout,
                    wall_after(self.config.permission_timeout),
                ) {
                    Ok(p) => {
                        let t = self.tasks.get_mut(&id).unwrap();
                        t.status = TaskStatus::WaitingPermission;
                        t.events.push(
                            EventKind::PermissionRequested,
                            json!({"permission_id":p.permission_id,"tool_call_id":p.tool_call_id}),
                        );
                    }
                    Err(e) => {
                        let code = match e {
                            PermError::LimitExceeded => ErrorCode::PermissionLimitExceeded,
                            _ => ErrorCode::PermissionPayloadTooLarge,
                        };
                        self.cancel(
                            id,
                            Some(error(code, "permission request exceeds safe bounds")),
                        );
                    }
                }
            }
        }
    }
    fn deliver(&self, pid: PermissionId, responder: PermResponder, option: Option<String>) {
        let tx = self.itx.clone();
        let epoch = self.epoch;
        let budget = self.config.close_timeout;
        tokio::spawn(async move {
            let outcome = match option {
                Some(option) => {
                    RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new(option))
                }
                None => RequestPermissionOutcome::Cancelled,
            };
            let result = tokio::time::timeout(
                budget,
                responder.reply_confirmed(RequestPermissionResponse::new(outcome)),
            )
            .await
            .unwrap_or_else(|_| Err("permission response write timed out".into()));
            let _ = tx.send(Internal::Delivered(epoch, pid, result)).await;
        });
    }
    fn revoke(&mut self, id: TaskId) {
        for (pid, responder) in self.permissions.cancel_for_task(id) {
            self.deliver(pid, responder, None);
        }
    }
    fn resolve(
        &mut self,
        id: TaskId,
        pid: PermissionId,
        option: String,
    ) -> Result<Value, TaskError> {
        if !self.tasks.contains_key(&id) {
            return Err(error(ErrorCode::TaskNotFound, "task not found"));
        }
        if self.permissions.task_of(pid) != Some(id) {
            return Err(error(
                ErrorCode::PermissionNotFound,
                "permission not found for task",
            ));
        }
        let (decision, responder) = self.permissions.decide(pid, &option).map_err(|e| {
            error(
                match e {
                    PermError::NotFound => ErrorCode::PermissionNotFound,
                    PermError::AlreadyResolved => ErrorCode::PermissionAlreadyResolved,
                    PermError::Expired => ErrorCode::PermissionExpired,
                    PermError::Cancelled => ErrorCode::PermissionCancelled,
                    _ => ErrorCode::InvalidArgument,
                },
                "permission decision rejected",
            )
        })?;
        if let Some(responder) = responder {
            self.deliver(pid, responder, decision.option_id.clone());
        }
        self.refresh_permission_status(id);
        Ok(
            json!({"permission_id":pid,"decision":decision.kind,"delivery":decision.delivery,"task_status":self.tasks[&id].status}),
        )
    }
    fn refresh_permission_status(&mut self, id: TaskId) {
        if let Some(t) = self.tasks.get_mut(&id)
            && matches!(
                t.status,
                TaskStatus::Running | TaskStatus::WaitingPermission
            )
        {
            t.status = if self.permissions.pending_count(id) > 0 {
                TaskStatus::WaitingPermission
            } else {
                TaskStatus::Running
            };
        }
    }
    fn intentional_transport_stop(&self) -> bool {
        self.termination_attempted
            && !self.tasks.values().any(|t| t.status.is_active())
            && self
                .sessions
                .values()
                .all(|s| matches!(s.state, SessionState::Closed | SessionState::Lost))
    }
    fn isolate(&mut self, reason: String) {
        self.begin_isolation(reason, true, None);
    }
    fn isolate_disconnected(&mut self, reason: String) {
        self.begin_isolation(reason, false, None);
    }
    fn isolate_backend_error(
        &mut self,
        problem: BackendError,
        failed_session: Option<BridgeSessionId>,
    ) {
        let contactable = !matches!(
            problem.kind,
            BackendErrorKind::ProcessDied
                | BackendErrorKind::Unavailable
                | BackendErrorKind::StartupClean
        );
        self.begin_isolation(problem.message, contactable, failed_session);
    }
    fn isolation_failure(&mut self, id: TaskId, stopped: bool) {
        let t = &self.tasks[&id];
        if t.status.is_terminal() {
            return;
        }
        let sent = t.phase == TaskPhase::PromptSent;
        let execution = if !sent {
            ExecutionState::NotStarted
        } else if stopped {
            ExecutionState::Stopped
        } else {
            ExecutionState::Unknown
        };
        let reason = self
            .isolation_reason
            .as_deref()
            .unwrap_or("backend cleanup is unconfirmed");
        let message = if let Some(cause) = &t.error {
            format!("{}; {reason}", cause.message)
        } else {
            reason.into()
        };
        let code = if execution == ExecutionState::Unknown {
            ErrorCode::ExecutionStateUnknown
        } else {
            t.error
                .as_ref()
                .map_or(ErrorCode::BackendUnavailable, |cause| cause.code)
        };
        self.settle(
            id,
            SettleOutcome::Failed {
                error: error(code, message),
                execution,
            },
        );
    }
    fn begin_isolation(
        &mut self,
        reason: String,
        contactable: bool,
        failed_session: Option<BridgeSessionId>,
    ) {
        if self.state == BackendState::Unavailable {
            if !contactable && !self.isolation_pending.is_empty() {
                self.isolation_pending.clear();
                let ids: Vec<_> = self
                    .tasks
                    .values()
                    .filter(|t| t.status.is_active())
                    .map(|t| t.task_id)
                    .collect();
                for id in ids {
                    self.isolation_failure(id, false);
                }
                self.terminate();
            }
            return;
        }
        for (_, _, reply) in self.history_waiters.drain(..) {
            let _ = reply.send(Err(error(
                ErrorCode::BackendUnavailable,
                "backend isolated",
            )));
        }
        self.state = BackendState::Unavailable;
        self.recovery = true;
        self.epoch += 1;
        self.isolation_reason = Some(reason);
        let sessions: Vec<_> = self
            .sessions
            .values()
            .filter(|s| s.state != SessionState::Closed)
            .map(|s| (s.session_id, s.state, s.dsh_session_id.clone()))
            .collect();
        for (sid, _, _) in &sessions {
            self.sessions.get_mut(sid).unwrap().state = SessionState::Lost;
        }
        let ids: Vec<_> = self
            .tasks
            .values()
            .filter(|t| t.status.is_active())
            .map(|t| t.task_id)
            .collect();
        for id in ids {
            self.revoke(id);
            self.cancel_deadlines.remove(&id);
            let t = self.tasks.get_mut(&id).unwrap();
            if contactable
                && t.phase == TaskPhase::PromptSent
                && Some(t.session_id) != failed_session
            {
                t.status = TaskStatus::Cancelling;
                t.cancel_requested_at.get_or_insert_with(wall_now);
                t.events.push(
                    EventKind::StatusChange,
                    json!({"status":"cancelling","reason":"backend isolation"}),
                );
            } else {
                self.isolation_failure(id, false);
            }
        }
        if contactable && let Some(backend) = self.backend.clone() {
            for (sid, previous, remote) in sessions {
                if Some(sid) == failed_session {
                    continue;
                }
                let Some(remote) = remote else {
                    continue;
                };
                self.isolation_pending.insert(sid);
                // An existing close already owns its bounded response channel.
                if previous == SessionState::Closing {
                    continue;
                }
                let backend = backend.clone();
                let tx = self.itx.clone();
                let epoch = self.epoch;
                let budget = self.config.close_timeout;
                tokio::spawn(async move {
                    let result = tokio::time::timeout(budget, backend.close_session(&remote))
                        .await
                        .unwrap_or_else(|_| {
                            Err(BackendError::new(
                                BackendErrorKind::Timeout,
                                "isolation close timed out",
                            ))
                        });
                    let _ = tx.send(Internal::IsolationClosed(epoch, sid, result)).await;
                });
            }
        }
        // No route means there is no per-session stop proof left to obtain.
        let untracked: Vec<_> = self
            .tasks
            .values()
            .filter(|t| t.status.is_active() && !self.isolation_pending.contains(&t.session_id))
            .map(|t| t.task_id)
            .collect();
        for id in untracked {
            self.isolation_failure(id, false);
        }
        if self.isolation_pending.is_empty() {
            self.terminate();
        }
    }
    fn isolation_closed(
        &mut self,
        epoch: u64,
        sid: BridgeSessionId,
        result: Result<(), BackendError>,
    ) {
        if epoch != self.epoch || !self.isolation_pending.remove(&sid) {
            return;
        }
        if let Some(id) = self.sessions.get(&sid).and_then(|s| s.active_task_id) {
            self.isolation_failure(id, result.is_ok());
        }
        if self.isolation_pending.is_empty() {
            self.terminate();
        }
    }
    fn terminate(&mut self) {
        if self.termination_attempted || self.process_stopped || self.starting {
            return;
        }
        if let Some(b) = self.backend.clone() {
            let tx = self.itx.clone();
            self.terminating = true;
            self.termination_attempted = true;
            let budget = self
                .shutdown_deadline
                .map(|deadline| deadline.saturating_duration_since(now()))
                .unwrap_or(limits::TERM_GRACE + limits::KILL_GRACE + Duration::from_millis(250));
            tokio::spawn(async move {
                let cleanup_budget = budget.saturating_sub(Duration::from_millis(20));
                let ok =
                    tokio::time::timeout(budget, b.terminate_process_with_budget(cleanup_budget))
                        .await
                        .unwrap_or(false);
                let _ = tx.send(Internal::Terminated(ok)).await;
            });
        }
    }
    fn begin_shutdown(&mut self) {
        if self.draining {
            return;
        }
        self.draining = true;
        let reserve =
            (limits::TERM_GRACE + limits::KILL_GRACE).min(self.config.shutdown_timeout / 2);
        let deadline = now() + self.config.shutdown_timeout;
        self.shutdown_deadline = Some(deadline);
        self.shutdown_terminate_at = Some(deadline - reserve);
        if let Some(startup) = &self.startup_shutdown {
            startup.shutdown(
                self.config
                    .shutdown_timeout
                    .saturating_sub(Duration::from_millis(20)),
            );
        }
        if self.state != BackendState::Unavailable {
            self.state = BackendState::Draining;
        }
        let ids: Vec<_> = self
            .tasks
            .values()
            .filter(|t| t.status.is_active())
            .map(|t| t.task_id)
            .collect();
        for id in ids {
            self.cancel(id, None);
        }
        let idle: Vec<_> = self
            .sessions
            .values()
            .filter(|s| s.state == SessionState::Idle)
            .map(|s| s.session_id)
            .collect();
        for sid in idle {
            self.close(sid);
        }
    }
    fn advance_shutdown(&mut self) {
        if !self.draining {
            return;
        }
        if self.shutdown_deadline.is_some_and(|d| d <= now()) {
            self.isolate_disconnected("overall shutdown deadline exhausted".into());
            self.terminate();
            self.finish_shutdown(false);
            return;
        }
        if self.shutdown_terminate_at.is_some_and(|d| d <= now())
            && !self.termination_attempted
            && !self.process_stopped
        {
            self.isolate_disconnected(
                "shutdown cleanup budget exhausted before stop evidence".into(),
            );
            self.terminate();
        }
        if self.starting
            || self.clearing
            || self.tasks.values().any(|t| t.status.is_active())
            || !self.isolation_pending.is_empty()
        {
            return;
        }
        if self
            .sessions
            .values()
            .any(|s| !matches!(s.state, SessionState::Closed | SessionState::Lost))
        {
            return;
        }
        if !self.process_stopped {
            if self.termination_attempted && !self.terminating {
                self.finish_shutdown(false);
            } else {
                self.terminate();
            }
            return;
        }
        if self.recovery
            || self
                .sessions
                .values()
                .any(|s| s.state == SessionState::Lost)
        {
            self.finish_shutdown(false);
            return;
        }
        self.clearing = true;
        let guard = self.guard.clone();
        let tx = self.itx.clone();
        tokio::spawn(async move {
            let ok = tokio::task::spawn_blocking(move || guard.clear())
                .await
                .is_ok_and(|r| r.is_ok());
            let _ = tx.send(Internal::Cleared(ok)).await;
        });
    }
    fn finish_shutdown(&mut self, ok: bool) {
        for tx in self.shutdown_waiters.drain(..) {
            let _ = tx.send(ok);
        }
        self.finished = true;
    }
    fn maintenance(&mut self) {
        let expired = self.permissions.expired(now());
        for pid in expired {
            if let Some((id, decision, responder)) = self.permissions.expire_one(pid) {
                self.deliver(pid, responder, decision.option_id);
                if let Some(t) = self.tasks.get_mut(&id) {
                    t.events.push(
                        EventKind::PermissionAutoDenied,
                        json!({"permission_id":pid}),
                    );
                }
                self.refresh_permission_status(id);
            }
        }
        let cancel: Vec<_> = self
            .cancel_deadlines
            .iter()
            .filter(|(_, d)| **d <= now())
            .map(|(id, _)| *id)
            .collect();
        for id in cancel {
            self.cancel_deadlines.remove(&id);
            if let Some(t) = self.tasks.get(&id)
                && t.status == TaskStatus::Cancelling
            {
                self.close(t.session_id);
            }
        }
        let idle: Vec<_> = self
            .sessions
            .values()
            .filter(|s| {
                s.state == SessionState::Idle
                    && now().saturating_duration_since(s.last_activity)
                        >= self.config.session_idle_ttl
            })
            .map(|s| s.session_id)
            .collect();
        for sid in idle {
            self.close(sid);
        }
        self.evict(false);
    }
    fn evict(&mut self, pressure: bool) {
        let mut candidates: Vec<_> = self
            .tasks
            .values()
            .filter(|t| {
                t.status.is_terminal() && {
                    let s = &self.sessions[&t.session_id];
                    s.latest_task_id != Some(t.task_id)
                        || matches!(s.state, SessionState::Closed | SessionState::Lost)
                }
            })
            .filter_map(|t| t.finished_instant.map(|at| (at, t.task_id)))
            .collect();
        candidates.sort_unstable();
        for (at, id) in candidates {
            if now().saturating_duration_since(at) < self.config.task_retention
                && !(pressure && self.tasks.len() >= self.config.max_task_records)
            {
                continue;
            }
            if let Some(t) = self.tasks.remove(&id) {
                self.requests.remove(&t.request_id);
                for (pid, responder) in self.permissions.forget_task(id) {
                    self.deliver(pid, responder, None);
                }
                if !self
                    .tasks
                    .values()
                    .any(|task| task.session_id == t.session_id)
                {
                    self.sessions.remove(&t.session_id);
                    self.owners.remove(&t.session_id);
                }
            }
        }
    }
    fn snapshot(&self, id: TaskId, cursor: u64, limit: usize) -> Result<Value, TaskError> {
        let t = self
            .tasks
            .get(&id)
            .ok_or_else(|| error(ErrorCode::TaskNotFound, "task not found"))?;
        if !(1..=limits::GET_TASK_MAX_LIMIT).contains(&limit) || cursor > t.events.last_cursor() {
            return Err(error(ErrorCode::InvalidArgument, "invalid cursor or limit"));
        }
        let s = &self.sessions[&t.session_id];
        let page = t.events.page(cursor, limit);
        let mut v = json!({"task_id":id,"session_id":t.session_id,"parent_task_id":t.parent_task_id,"resumed_from_dsh_session_id":t.resumed_from_dsh_session_id,"status":t.status,"cwd":s.cwd,
            "created_at":t.created_at,"started_at":t.started_at,"finished_at":t.finished_at,"execution_state":t.execution_state,
            "session":{"state":s.state,"dsh_session_id":s.dsh_session_id,"latest_task_id":s.latest_task_id,
                "followup_available":s.state==SessionState::Idle && s.latest_task_id==Some(id) && !self.draining && self.state==BackendState::Ready,
                "unavailable_reason":if matches!(s.state,SessionState::Closed|SessionState::Lost|SessionState::Closing) {Some("session unavailable")}else {None}},
            "backend":{"state":self.state,"recovery_required":self.recovery},
            "pending_permissions":self.permissions.pending_snapshot(id),"result":t.result.clone().unwrap_or_else(||t.result_agg.finish()),
            "stop_reason":t.stop_reason,"error":t.error,"cancel_requested_at":t.cancel_requested_at,
            "events":[],"earliest_cursor":page.earliest_cursor,"next_cursor":cursor.max(page.earliest_cursor-1),"last_cursor":page.last_cursor,
            "gap":page.gap,"dropped_count":page.dropped_count,"has_more":false});
        let mut events = Vec::new();
        for event in page.events {
            let seq = event.seq;
            events.push(serde_json::to_value(event).unwrap());
            v["events"] = json!(&events);
            v["next_cursor"] = json!(seq);
            if serde_json::to_vec(&v).unwrap().len() > limits::TASKVIEW_BUDGET {
                events.pop();
                v["events"] = json!(&events);
                v["next_cursor"] = json!(
                    events
                        .last()
                        .map_or(cursor.max(page.earliest_cursor - 1), |e| e["seq"]
                            .as_u64()
                            .unwrap())
                );
                break;
            }
        }
        v["has_more"] = json!(v["next_cursor"].as_u64().unwrap() < page.last_cursor);
        Ok(v)
    }
}

#[cfg(test)]
mod lifecycle_regressions {
    use super::*;

    fn actor() -> (Coordinator<FakeFactory>, FakeDriver, tempfile::TempDir) {
        let state = tempfile::tempdir().unwrap();
        let guard = match RuntimeGuard::acquire(state.path()).unwrap() {
            crate::runtime_guard::AcquireOutcome::Clean(guard) => Arc::new(guard),
            _ => unreachable!(),
        };
        let config = Config::default();
        let (factory, driver) = fake_backend();
        let recovery = false;
        let (_tx, rx) = mpsc::channel(128);
        let (itx, irx) = mpsc::channel(128);
        let coord = Coordinator {
            config,
            factory: Some(factory),
            guard,
            backend: None,
            state: if recovery {
                BackendState::Unavailable
            } else {
                BackendState::Dormant
            },
            epoch: 1,
            recovery,
            initial_recovery: recovery,
            draining: false,
            ever_used: false,
            starting: false,
            startup_shutdown: None,
            isolation_reason: None,
            isolation_pending: HashSet::new(),
            shutdown_terminate_at: None,
            clients: HashSet::new(),
            owners: HashMap::new(),
            history_waiters: Vec::new(),
            history_inflight: 0,
            tasks: HashMap::new(),
            sessions: HashMap::new(),
            requests: HashMap::new(),
            prompts: HashMap::new(),
            compatibility_noted: HashSet::new(),
            permissions: PermissionManager::new(),
            tools: HashMap::new(),
            tool_order: VecDeque::new(),
            cancel_deadlines: HashMap::new(),
            itx,
            irx,
            rx,
            events: None,
            shutdown_waiters: Vec::new(),
            shutdown_deadline: None,
            terminating: false,
            termination_attempted: false,
            process_stopped: true,
            clearing: false,
            finished: false,
        };
        (coord, driver, state)
    }

    #[tokio::test]
    async fn close_response_drains_preceding_output_before_settlement() {
        let (mut actor, driver, _state) = actor();
        let (backend, events, _) = actor.factory.as_mut().unwrap().start().await.unwrap();
        actor.backend = Some(backend);
        actor.events = Some(events);
        actor.state = BackendState::Ready;
        let task_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let mut task = Task::new(task_id, "tail".into(), session_id, None);
        task.phase = TaskPhase::PromptSent;
        task.status = TaskStatus::Cancelling;
        task.execution_state = ExecutionState::InProgress;
        actor.tasks.insert(task_id, task);
        actor.sessions.insert(
            session_id,
            Session {
                session_id,
                dsh_session_id: Some("tail-session".into()),
                epoch: 1,
                cwd: PathBuf::from("/tmp"),
                state: SessionState::Closing,
                active_task_id: Some(task_id),
                latest_task_id: Some(task_id),
                last_activity: now(),
            },
        );
        driver
            .event_tx
            .try_send(BackendEvent::Update {
                epoch: 1,
                dsh_session_id: "tail-session".into(),
                update: SessionUpdateView::AgentMessageChunk {
                    text: "last partial output".into(),
                    message_id: Some("m".into()),
                },
            })
            .unwrap();
        actor.on_internal(Internal::Closed(1, session_id, Ok(())));
        assert_eq!(
            actor.tasks[&task_id]
                .result
                .as_ref()
                .map(|result| result.text.as_str()),
            Some("last partial output")
        );
    }

    #[tokio::test]
    async fn late_backend_start_after_isolation_cannot_restore_ready() {
        let (mut actor, _driver, _state) = actor();
        let mut factory = actor.factory.take().unwrap();
        let started = factory.start().await.unwrap();
        actor.state = BackendState::Unavailable;
        actor.recovery = true;
        actor.starting = true;
        actor.process_stopped = false;
        actor.on_internal(Internal::Started(factory, Ok(started)));
        assert_eq!(actor.state, BackendState::Unavailable);
        assert!(actor.termination_attempted);
    }
}

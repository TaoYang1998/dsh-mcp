# dsh-mcp 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 Rust MCP 服务 dsh-mcp，让 Codex 通过 6 个 MCP 工具把编码任务委派给本机 dsh（经 ACP v1），支持任务派发、查询、同会话返工、取消、权限裁决与会话关闭。

**Architecture:** 单 dsh ACP 子进程 + 内存任务注册表。MCP server（rmcp）←→ Coordinator actor（唯一业务状态写入者）←→ ACP adapter（agent-client-protocol SDK）←→ supervisor（tokio::process 管理 dsh 子进程）。runtime_guard 提供实例锁与恢复保护标记。

**Tech Stack:** Rust 1.95 / rmcp 3.5.1 / agent-client-protocol 3.4.0 / tokio 1 / serde / tracing / uuid / rustix。规格：`docs/superpowers/specs/2026-10-19-dsh-mcp-design.md`（下称"规格"），章节引用如"规格 §7"。

## Global Constraints

以下逐条来自规格，所有任务隐含遵守：

- ACP v1 stable；**禁止**启用 agent-client-protocol 的任何 `unstable*` feature；不使用 session/load、mode、fork、transcript、终端、客户端文件系统能力（规格 §1、§11.2）。
- dsh 启动：`DSH_MCP_DSH_COMMAND`（默认 `dsh`）+ `DSH_MCP_DSH_ARGS_JSON`（默认 `["--profile","acp"]`）；**禁止** shell 拼接/空格拆分命令行，逐项传 `Command::args`（规格 §11.1）。
- stdout 仅承载 MCP 协议；一切日志（含 dsh stderr）走 stderr（规格 §11.1）。
- 桥接不读取/复制 `~/.dsh/.credentials.yaml`，不打印完整环境变量，错误信息不附带凭据（规格 §5、§9、§11.1）。
- 模型/推理选项用 dsh 实时公布值；认证/模型错误保留原错误，**不静默切换模型**、不重放任务（规格 §8.1）。
- 只在取得明确准入前取消或 ACP 停止证据后报 `cancelled`；无法确认时报 `failed` + `execution_state=unknown`（规格 §4.2-5）。
- 接口返回成功接单后任务记录已存在；MCP 断连不取消已接单任务（规格 §4.2-6）。
- 取消阶梯：`session/cancel`(30s) → `session/close`(10s) → 后端隔离（TERM 5s → KILL 2s）→ 如实结算 unknown；不采取"只结束 future"的假取消；不按进程名批量杀进程（规格 §7）。
- 后端故障后不自动重启已使用的 dsh；恢复保护标记需人工 `--acknowledge-cleanup` 清除（规格 §8）。
- 权限永不默认放行、不硬编码 option_id；超时选实际提供的 reject_once，没有则回 ACP cancelled（规格 §10）。
- 资源上限（规格 §9）：任务事件 200 条/1 MiB、单事件 8 KiB、权限每任务 16 项、裁决记录 64、TaskView 256 KiB（meta 16 KiB + 权限快照 64 KiB + 结果 64 KiB + 事件）、错误摘要 4 KiB、MCP 响应硬上限 320 KiB、ACP 单帧 4 MiB、ACP 入站队列 128 项/8 MiB、拥塞 10s 判连接故障。
- 默认额度：`MAX_ACTIVE_TASKS=4`、`MAX_OPEN_SESSIONS=16`、`SESSION_IDLE_TTL=1800s`、`MAX_TASK_RECORDS=256`、`TASK_RETENTION=86400s`；prompt 上限 64 KiB；request_id 上限 128 字节（规格 §5、§6.2、§11.1）。
- 验收命令：`cargo fmt --check`、`cargo build --locked`、`cargo test --locked`、`cargo clippy --locked -- -D warnings`（规格 §13.3）。
- 不创建远程仓库、不推送、不修改 Codex 全局配置（规格 §14）。
- 网络访问（cargo 拉依赖）需代理：`https_proxy=http://127.0.0.1:7890`。

## 已核实的 API 事实（探针 `/tmp/apiprobe` 编译通过，2026-10-19）

- rmcp 3.5.1：`#[rmcp::tool_router]` + `#[rmcp::tool(description = "...")]` + `#[rmcp::tool_handler] impl ServerHandler`；参数 `Parameters<T>`（`rmcp::handler::server::wrapper::Parameters`，T 需 `Deserialize + schemars::JsonSchema`）；结构化返回 `Json<T>`（T 需 `Serialize + JsonSchema`）；`CallToolResult::structured(serde_json::Value)`（自动带文本回退）/ `CallToolResult::error(Vec<ContentBlock>)`（is_error=true）；`rmcp::ErrorData`；`rmcp::serve_server(handler, rmcp::transport::stdio()).await?` → `RunningService`（`.waiting().await`）；测试客户端 `rmcp::serve_client`（需 `client` feature）。
- schemars 必须与 rmcp 同大版本：**`schemars = "1"`**（rmcp 3.5.1 依赖 schemars 1.0；写 0.9 会 trait 不匹配）。
- ACP 3.4.0：`agent_client_protocol::Client.builder().name(..).on_receive_notification(async move |n: SessionNotification, _cx| {..}, agent_client_protocol::on_receive_notification!())`、`.on_receive_request(async move |req: RequestPermissionRequest, responder, _cx| {..}, on_receive_request!())`、`.connect_with(transport, |connection: ConnectionTo<Agent>| async move { .. }).await`。
- `ConnectionTo<Agent>`：`Clone + Send`；`.send_request(req).block_task().await`；`.send_notification(notif)?`。connect_with 的闭包返回即断连——保活靠闭包里 await 关停信号。
- 传输：`ByteStreams::new(child_stdin.compat_write(), child_stdout.compat())`（tokio-util `compat` feature，`TokioAsyncReadCompatExt`/`TokioAsyncWriteCompatExt`）；`ByteStreams` 实现 `ConnectTo`。
- schema（`agent_client_protocol::schema::v1`）：`InitializeRequest::new(ProtocolVersion::V1)`、`NewSessionRequest::new(cwd: PathBuf)`（响应含 `session_id`、`config_options`）、`PromptRequest::new(session_id, Vec<ContentBlock>)`、`CancelNotification::new(session_id)`、`CloseSessionRequest::new(session_id)`、`SessionNotification { session_id, update: SessionUpdate, .. }`、`SessionUpdate::{AgentMessageChunk(ContentChunk{content, message_id: Option<MessageId>}), AgentThoughtChunk, ToolCall(ToolCall{tool_call_id,title,name,status,..}), ToolCallUpdate, ConfigOptionUpdate, UsageUpdate, ..}`（`#[non_exhaustive]`，匹配必须带 `_`）、`RequestPermissionRequest{session_id, options: Vec<PermissionOption{option_id, name, kind: PermissionOptionKind{AllowOnce,AllowAlways,RejectOnce,RejectAlways}}, tool_call..}`、`RequestPermissionResponse::new(RequestPermissionOutcome::{Selected(SelectedPermissionOutcome::new(option_id)) | Cancelled})`、`StopReason::{EndTurn,MaxTokens,MaxTurnRequests,Refusal,Cancelled}`（non_exhaustive）。
- `init_response.agent_capabilities.session_capabilities`：`list`/`resume`/`close` 为 `Option<...>`，调用前必须核实。
- `Responder<RequestPermissionResponse>`：`Send`，`respond(self, resp)` 一次性消费；存进权限表待裁决。
- 关键类型若与本计划代码不符，以 `~/.cargo/registry/src/index.crates.io-*/{rmcp-3.5.1,agent-client-protocol-3.4.0,agent-client-protocol-schema-1.12.0}/src/` 为准修正。

## 文件结构

```text
Cargo.toml
src/
  main.rs          入口：--acknowledge-cleanup、配置、tracing、guard、coordinator、MCP 服务、信号与 draining
  config.rs        Config 解析与校验
  server.rs        DshMcpServer：6 个 MCP 工具、参数校验、结构化响应/错误
  coordinator.rs   Coordinator actor + Clock 抽象 + 命令/事件处理
  task.rs          Task/Session/BackendState 域模型与迁移规则、注册表
  permission.rs    待决权限表、一次性裁决、超时选择、记录环
  events.rs        事件环形缓冲、分页、UTF-8 安全截断、结果聚合
  acp.rs           SessionUpdateView 等视图类型、Backend trait、FakeBackend、DshBackend（SDK 适配）
  supervisor.rs    dsh 子进程 spawn/stderr 排空/TERM-KILL/wait
  runtime_guard.rs 状态目录、OS 文件锁、run-state.json、acknowledge_cleanup
tests/
  mcp_protocol.rs  MCP 协议级测试（duplex 传输）
  control_plane.rs 真实 dsh 控制面（无模型；无 dsh 时显式跳过）
scripts/
  e2e_real_model.py 真实模型验收脚本（人工触发，不进 CI）
README.md          安装、配置、六工具工作流、Codex 配置示例、恢复步骤
```

内部命名约定：桥接侧 ID 用 `Uuid`（task_id / session_id / permission_id）；dsh 侧会话 ID 用 `String`（`dsh_session_id`）。`Instant` 只用于超时判断；对外展示时间用 UTC RFC 3339 字符串。

---

### Task 1: 项目脚手架与配置

**Files:**
- Create: `Cargo.toml`
- Create: `src/lib.rs`（库根：声明各模块；`tests/` 集成测试依赖库目标）
- Create: `src/main.rs`（薄二进制入口）
- Create: `src/config.rs`

**Interfaces:**
- Consumes: 无
- Produces:
  - `pub struct Config { pub dsh_command: String, pub dsh_args: Vec<String>, pub state_dir: PathBuf, pub max_active_tasks: usize, pub max_open_sessions: usize, pub session_idle_ttl: Duration, pub max_task_records: usize, pub task_retention: Duration, pub start_timeout: Duration, pub permission_timeout: Duration, pub cancel_timeout: Duration, pub close_timeout: Duration, pub shutdown_timeout: Duration }`
  - `impl Config { pub fn from_env() -> Result<Config, ConfigError>; fn validate(&self) -> Result<(), ConfigError>; pub fn default_state_dir() -> PathBuf }`
  - `pub struct ConfigError(pub String)`（Display + Error）
  - 固定上限常量（`pub mod limits`）：`EVENT_KEEP_COUNT=200, EVENT_KEEP_BYTES=1<<20, EVENT_ITEM_MAX=8*1024, PROMPT_MAX=64*1024, REQUEST_ID_MAX=128, PENDING_PERMS_MAX=16, DECIDED_PERMS_KEEP=64, TOOL_SUMMARY_CACHE=128, TASKVIEW_BUDGET=256*1024, META_BUDGET=16*1024, PERM_SNAPSHOT_BUDGET=64*1024, RESULT_BUDGET=64*1024, ERROR_SUMMARY_MAX=4*1024, GET_TASK_DEFAULT_LIMIT=50, GET_TASK_MAX_LIMIT=100, MCP_RESPONSE_MAX=320*1024, ACP_FRAME_MAX=4*1024*1024, ACP_QUEUE_MAX_ITEMS=128, ACP_QUEUE_MAX_BYTES=8*1024*1024, ACP_QUEUE_STALL=Duration::from_secs(10), TERM_GRACE=Duration::from_secs(5), KILL_GRACE=Duration::from_secs(2)`

- [ ] **Step 1: 写 Cargo.toml**

```toml
[package]
name = "dsh-mcp"
version = "0.1.0"
edition = "2024"
license = "Apache-2.0"
description = "A Rust MCP server for delegating coding tasks to DeepSeek Harness via ACP."

[dependencies]
agent-client-protocol = "3.4.0"
rmcp = { version = "3.5.1", features = ["server", "macros", "transport-io"] }
schemars = "1"
tokio = { version = "1", features = ["rt-multi-thread", "macros", "process", "io-util", "io-std", "sync", "time", "signal"] }
tokio-util = { version = "0.7", features = ["compat"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
uuid = { version = "1", features = ["v4", "serde"] }
rustix = { version = "1", features = ["fs"] }
futures = "0.3"
anyhow = "1"
thiserror = "2"

[dev-dependencies]
rmcp = { version = "3.5.1", features = ["server", "macros", "transport-io", "client"] }
tempfile = "3"
```

- [ ] **Step 2: 写失败测试 `src/config.rs`（先写测试与空壳实现）**

`src/main.rs` 初始内容（薄入口；库目标承载全部模块，供 `tests/` 集成测试复用）：

```rust
fn main() {
    println!("dsh-mcp scaffold");
}
```

`src/lib.rs` 初始内容：

```rust
pub mod config;
```

后续任务在 lib.rs 追加 `pub mod events; pub mod task; pub mod permission; pub mod runtime_guard; pub mod acp; pub mod coordinator; pub mod supervisor; pub mod server;`（而非 main.rs；各任务中"Modify: src/main.rs（加 mod）"的记述统一理解为 lib.rs）。

`src/config.rs` 测试（文件底部 `#[cfg(test)] mod tests`）：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::sync::Mutex;

    // env 测试必须串行
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn clear() {
        for k in [
            "DSH_MCP_DSH_COMMAND", "DSH_MCP_DSH_ARGS_JSON", "DSH_MCP_STATE_DIR",
            "DSH_MCP_MAX_ACTIVE_TASKS", "DSH_MCP_MAX_OPEN_SESSIONS",
            "DSH_MCP_SESSION_IDLE_TTL_SECS", "DSH_MCP_MAX_TASK_RECORDS",
            "DSH_MCP_TASK_RETENTION_SECS", "DSH_MCP_START_TIMEOUT_SECS",
            "DSH_MCP_PERMISSION_TIMEOUT_SECS", "DSH_MCP_CANCEL_TIMEOUT_SECS",
            "DSH_MCP_CLOSE_TIMEOUT_SECS", "DSH_MCP_SHUTDOWN_TIMEOUT_SECS",
        ] {
            unsafe { env::remove_var(k) };
        }
    }

    #[test]
    fn defaults_are_applied() {
        let _g = ENV_LOCK.lock().unwrap();
        clear();
        let c = Config::from_env().unwrap();
        assert_eq!(c.dsh_command, "dsh");
        assert_eq!(c.dsh_args, vec!["--profile", "acp"]);
        assert_eq!(c.max_active_tasks, 4);
        assert_eq!(c.max_open_sessions, 16);
        assert_eq!(c.session_idle_ttl, Duration::from_secs(1800));
        assert_eq!(c.max_task_records, 256);
        assert_eq!(c.task_retention, Duration::from_secs(86400));
        assert_eq!(c.start_timeout, Duration::from_secs(30));
        assert_eq!(c.permission_timeout, Duration::from_secs(600));
        assert_eq!(c.cancel_timeout, Duration::from_secs(30));
        assert_eq!(c.close_timeout, Duration::from_secs(10));
        assert_eq!(c.shutdown_timeout, Duration::from_secs(50));
    }

    #[test]
    fn args_json_must_be_string_array() {
        let _g = ENV_LOCK.lock().unwrap();
        clear();
        unsafe { env::set_var("DSH_MCP_DSH_ARGS_JSON", "--profile acp") };
        assert!(Config::from_env().is_err());
        unsafe { env::set_var("DSH_MCP_DSH_ARGS_JSON", "[\"a b\", \"c\"]") };
        let c = Config::from_env().unwrap();
        assert_eq!(c.dsh_args, vec!["a b", "c"]);
    }

    #[test]
    fn rejects_zero_and_bad_ratios() {
        let _g = ENV_LOCK.lock().unwrap();
        clear();
        unsafe { env::set_var("DSH_MCP_MAX_ACTIVE_TASKS", "0") };
        assert!(Config::from_env().is_err());
        clear();
        unsafe {
            env::set_var("DSH_MCP_MAX_ACTIVE_TASKS", "8");
            env::set_var("DSH_MCP_MAX_OPEN_SESSIONS", "4");
        }
        assert!(Config::from_env().is_err()); // active > open
        clear();
        unsafe {
            env::set_var("DSH_MCP_MAX_OPEN_SESSIONS", "4");
            env::set_var("DSH_MCP_MAX_TASK_RECORDS", "5"); // < 4+4? 4 active + 4 sessions = 8
        }
        assert!(Config::from_env().is_err());
    }

    #[test]
    fn rejects_non_numeric_duration() {
        let _g = ENV_LOCK.lock().unwrap();
        clear();
        unsafe { env::set_var("DSH_MCP_CANCEL_TIMEOUT_SECS", "soon") };
        assert!(Config::from_env().is_err());
    }
}
```

注：Rust 2024 edition 中 `env::set_var`/`remove_var` 是 unsafe（测试进程内串行使用可接受）；本计划用 edition 2024，测试中的 `unsafe` 块为必需。单元测试命令统一 `cargo test <模块名>`（如 `cargo test config`），跨 lib/bin 目标均生效。

- [ ] **Step 3: 运行测试确认失败**

Run: `cargo test config`
Expected: 编译失败（`Config` 不存在）

- [ ] **Step 4: 实现 config.rs**

```rust
use std::env;
use std::path::PathBuf;
use std::time::Duration;

pub mod limits {
    use std::time::Duration;
    pub const EVENT_KEEP_COUNT: usize = 200;
    pub const EVENT_KEEP_BYTES: usize = 1 << 20;
    pub const EVENT_ITEM_MAX: usize = 8 * 1024;
    pub const PROMPT_MAX: usize = 64 * 1024;
    pub const REQUEST_ID_MAX: usize = 128;
    pub const PENDING_PERMS_MAX: usize = 16;
    pub const DECIDED_PERMS_KEEP: usize = 64;
    pub const TOOL_SUMMARY_CACHE: usize = 128;
    pub const TOOL_SUMMARY_MAX: usize = 4 * 1024;
    pub const PERM_ITEM_MAX: usize = 4 * 1024;
    pub const PERM_OPTIONS_MAX: usize = 8;
    pub const PERM_OPTION_ID_MAX: usize = 256;
    pub const TASKVIEW_BUDGET: usize = 256 * 1024;
    pub const META_BUDGET: usize = 16 * 1024;
    pub const PERM_SNAPSHOT_BUDGET: usize = 64 * 1024;
    pub const RESULT_BUDGET: usize = 64 * 1024;
    pub const ERROR_SUMMARY_MAX: usize = 4 * 1024;
    pub const GET_TASK_DEFAULT_LIMIT: usize = 50;
    pub const GET_TASK_MAX_LIMIT: usize = 100;
    pub const MCP_RESPONSE_MAX: usize = 320 * 1024;
    pub const ACP_FRAME_MAX: usize = 4 * 1024 * 1024;
    pub const ACP_QUEUE_MAX_ITEMS: usize = 128;
    pub const ACP_QUEUE_MAX_BYTES: usize = 8 * 1024 * 1024;
    pub const ACP_QUEUE_STALL: Duration = Duration::from_secs(10);
    pub const TERM_GRACE: Duration = Duration::from_secs(5);
    pub const KILL_GRACE: Duration = Duration::from_secs(2);
}

#[derive(Debug, Clone)]
pub struct Config {
    pub dsh_command: String,
    pub dsh_args: Vec<String>,
    pub state_dir: PathBuf,
    pub max_active_tasks: usize,
    pub max_open_sessions: usize,
    pub session_idle_ttl: Duration,
    pub max_task_records: usize,
    pub task_retention: Duration,
    pub start_timeout: Duration,
    pub permission_timeout: Duration,
    pub cancel_timeout: Duration,
    pub close_timeout: Duration,
    pub shutdown_timeout: Duration,
}

#[derive(Debug)]
pub struct ConfigError(pub String);

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "configuration_error: {}", self.0)
    }
}
impl std::error::Error for ConfigError {}

fn env_opt(name: &str) -> Result<Option<String>, ConfigError> {
    match env::var(name) {
        Ok(v) => Ok(Some(v)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => {
            Err(ConfigError(format!("{name} is not valid unicode")))
        }
    }
}

fn env_usize(name: &str, default: usize) -> Result<usize, ConfigError> {
    match env_opt(name)? {
        None => Ok(default),
        Some(v) => {
            let n: usize = v
                .parse()
                .map_err(|_| ConfigError(format!("{name} must be a positive integer")))?;
            if n == 0 {
                return Err(ConfigError(format!("{name} must be positive")));
            }
            Ok(n)
        }
    }
}

fn env_secs(name: &str, default: u64) -> Result<Duration, ConfigError> {
    Ok(Duration::from_secs(env_usize(name, default as usize)? as u64))
}

impl Config {
    pub fn default_state_dir() -> PathBuf {
        if let Ok(xdg) = env::var("XDG_STATE_HOME") {
            if !xdg.is_empty() {
                return PathBuf::from(xdg).join("dsh-mcp");
            }
        }
        let home = env::var("HOME").unwrap_or_else(|_| ".".to_string());
        PathBuf::from(home).join(".local/state/dsh-mcp")
    }

    pub fn from_env() -> Result<Config, ConfigError> {
        let dsh_command = env_opt("DSH_MCP_DSH_COMMAND")?.unwrap_or_else(|| "dsh".to_string());
        if dsh_command.is_empty() {
            return Err(ConfigError("DSH_MCP_DSH_COMMAND must not be empty".into()));
        }
        let dsh_args = match env_opt("DSH_MCP_DSH_ARGS_JSON")? {
            None => vec!["--profile".to_string(), "acp".to_string()],
            Some(v) => serde_json::from_str::<Vec<String>>(&v)
                .map_err(|_| ConfigError("DSH_MCP_DSH_ARGS_JSON must be a JSON string array".into()))?,
        };
        let state_dir = match env_opt("DSH_MCP_STATE_DIR")? {
            Some(v) => PathBuf::from(v),
            None => Self::default_state_dir(),
        };
        let c = Config {
            dsh_command,
            dsh_args,
            state_dir,
            max_active_tasks: env_usize("DSH_MCP_MAX_ACTIVE_TASKS", 4)?,
            max_open_sessions: env_usize("DSH_MCP_MAX_OPEN_SESSIONS", 16)?,
            session_idle_ttl: env_secs("DSH_MCP_SESSION_IDLE_TTL_SECS", 1800)?,
            max_task_records: env_usize("DSH_MCP_MAX_TASK_RECORDS", 256)?,
            task_retention: env_secs("DSH_MCP_TASK_RETENTION_SECS", 86400)?,
            start_timeout: env_secs("DSH_MCP_START_TIMEOUT_SECS", 30)?,
            permission_timeout: env_secs("DSH_MCP_PERMISSION_TIMEOUT_SECS", 600)?,
            cancel_timeout: env_secs("DSH_MCP_CANCEL_TIMEOUT_SECS", 30)?,
            close_timeout: env_secs("DSH_MCP_CLOSE_TIMEOUT_SECS", 10)?,
            shutdown_timeout: env_secs("DSH_MCP_SHUTDOWN_TIMEOUT_SECS", 50)?,
        };
        c.validate()?;
        Ok(c)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.max_active_tasks > self.max_open_sessions {
            return Err(ConfigError(
                "DSH_MCP_MAX_ACTIVE_TASKS must not exceed DSH_MCP_MAX_OPEN_SESSIONS".into(),
            ));
        }
        if self.max_task_records < self.max_open_sessions + self.max_active_tasks {
            return Err(ConfigError(
                "DSH_MCP_MAX_TASK_RECORDS must be at least MAX_OPEN_SESSIONS + MAX_ACTIVE_TASKS"
                    .into(),
            ));
        }
        Ok(())
    }
}
```

（测试模块保持 Step 2 内容在文件底部。）

- [ ] **Step 5: 运行测试确认通过**

Run: `cargo test config`
Expected: 4 个测试 PASS

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock src/main.rs src/config.rs
git commit -m "feat: scaffold project with env-based config"
```

---

### Task 2: 事件缓冲、分页与结果聚合（events.rs）

**Files:**
- Create: `src/events.rs`
- Modify: `src/main.rs`（加 `mod events;`）

**Interfaces:**
- Consumes: `config::limits`
- Produces:
  - `pub enum EventKind { AgentMessage, AgentThought, ToolCall, ToolCallUpdate, ConfigUpdate, Usage, StatusChange, PermissionRequested, PermissionResolved, PermissionAutoDenied, PermissionCancelled, BridgeError, CompatNote }`（`Serialize` → snake_case 字符串）
  - `pub struct Event { pub seq: u64, pub time: String, pub kind: EventKind, pub data: serde_json::Value, pub truncated: bool }`
  - `pub struct EventBuffer { .. }`：`fn new(keep_count, keep_bytes) -> Self`、`fn push(&mut self, kind: EventKind, data: Value) -> u64`（返回 seq）、`fn page(&self, cursor: u64, limit: usize) -> EventPage`、`fn last_cursor(&self) -> u64`、`fn earliest_cursor(&self) -> u64`
  - `pub struct EventPage { pub events: Vec<Event>, pub earliest_cursor: u64, pub next_cursor: u64, pub last_cursor: u64, pub gap: bool, pub dropped_count: u64, pub has_more: bool }`
  - `pub fn truncate_utf8(s: &str, max: usize) -> (&str, bool)`（截断保持 UTF-8 边界）
  - `pub fn sanitize_event_data(data: Value, max_bytes: usize) -> (Value, bool)`：JSON 序列化超 max_bytes 时替换为 `{"summary": <裁剪文本>, "original_bytes": n}`，返回 truncated=true；字符串字段截断不产生无效 UTF-8
  - `pub enum ResultSource { LastAssistantMessage, TurnText, Empty }`（Serialize snake_case）
  - `pub struct TaskResult { pub text: String, pub truncated: bool, pub total_bytes: usize, pub source: ResultSource }`（Serialize）
  - `pub struct ResultAggregator { .. }`：`fn new() -> Self`、`fn push_chunk(&mut self, text: &str, message_id: Option<&str>)`、`fn finish(&self) -> TaskResult`

行为契约（规格 §9.1/§9.2）：
- seq 从 1 单调递增；缓冲满（条数或字节先触及）淘汰最早项。
- `earliest_cursor`：当前最早保留 seq；无事件时为 `last_cursor+1`（`last_cursor = 最新已分配 seq`，无事件为 0）。
- `page(cursor, limit)`：返回 seq>cursor 的事件（cursor 小于 earliest_cursor-1 时从最早保留项开始，`gap=true`，`dropped_count=earliest_cursor-cursor-1`）；`next_cursor`=最后返回事件 seq，无返回时为 `max(cursor, earliest_cursor-1)`；`has_more = next_cursor < last_cursor`。
- 聚合：message_id 出现且变化时重置当前消息文本；保留最后一条消息前 64 KiB（`limits::RESULT_BUDGET`），`total_bytes` 为该消息原始 UTF-8 字节数；全部 chunk 无 message_id 时整轮聚合，`source=TurnText`；无文本 → 空串 + `source=Empty`。

- [ ] **Step 1: 写失败测试**

`src/events.rs` 底部测试模块：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn seq_and_basic_pagination() {
        let mut b = EventBuffer::new(200, 1 << 20);
        for i in 0..5 {
            assert_eq!(b.push(EventKind::AgentMessage, json!({"i": i})), i + 1);
        }
        let p = b.page(0, 3);
        assert_eq!(p.events.len(), 3);
        assert_eq!(p.earliest_cursor, 1);
        assert_eq!(p.next_cursor, 3);
        assert_eq!(p.last_cursor, 5);
        assert!(!p.gap);
        assert_eq!(p.dropped_count, 0);
        assert!(p.has_more);
        let p2 = b.page(3, 100);
        assert_eq!(p2.events.len(), 2);
        assert_eq!(p2.next_cursor, 5);
        assert!(!p2.has_more);
        let p3 = b.page(5, 100);
        assert!(p3.events.is_empty());
        assert_eq!(p3.next_cursor, 5);
        assert!(!p3.has_more);
    }

    #[test]
    fn eviction_by_count_reports_gap() {
        let mut b = EventBuffer::new(3, 1 << 20);
        for i in 0..5 {
            b.push(EventKind::ToolCall, json!({"i": i}));
        }
        assert_eq!(b.earliest_cursor(), 3);
        assert_eq!(b.last_cursor(), 5);
        let p = b.page(0, 100);
        assert!(p.gap);
        assert_eq!(p.dropped_count, 2); // seq 1..=2 已淘汰
        assert_eq!(p.events[0].seq, 3);
        assert_eq!(p.next_cursor, 5);
        assert!(!p.has_more);
    }

    #[test]
    fn eviction_by_bytes() {
        let mut b = EventBuffer::new(200, 64); // 极小的字节上限
        for i in 0..10 {
            b.push(EventKind::AgentMessage, json!({"payload": "x".repeat(20), "i": i}));
        }
        assert!(b.earliest_cursor() > 1);
        assert!(b.last_cursor() == 10);
    }

    #[test]
    fn empty_buffer_cursor_rules() {
        let b = EventBuffer::new(200, 1 << 20);
        assert_eq!(b.earliest_cursor(), 1); // last_cursor(0)+1
        let p = b.page(0, 50);
        assert!(p.events.is_empty());
        assert_eq!(p.next_cursor, 0);
        assert!(!p.has_more);
        assert!(!p.gap);
    }

    #[test]
    fn truncate_utf8_keeps_valid_boundaries() {
        let (s, t) = truncate_utf8("héllo wörld", 4);
        assert!(std::str::from_utf8(s.as_bytes()).is_ok());
        assert!(t);
        let (s2, t2) = truncate_utf8("abc", 10);
        assert_eq!(s2, "abc");
        assert!(!t2);
        // 多字节字符不能被切半
        let (s3, _) = truncate_utf8("你好世界", 3);
        assert_eq!(s3, "你");
    }

    #[test]
    fn sanitize_oversized_event_data() {
        let big = json!({"text": "y".repeat(9000)});
        let (v, truncated) = sanitize_event_data(big, limits::EVENT_ITEM_MAX);
        assert!(truncated);
        assert!(serde_json::to_string(&v).unwrap().len() <= limits::EVENT_ITEM_MAX);
        assert!(v.get("original_bytes").is_some());
        let (v2, t2) = sanitize_event_data(json!({"a": 1}), limits::EVENT_ITEM_MAX);
        assert!(!t2);
        assert_eq!(v2, json!({"a": 1}));
    }

    #[test]
    fn result_aggregation_by_message_id() {
        let mut a = ResultAggregator::new();
        a.push_chunk("hello ", Some("m1"));
        a.push_chunk("world", Some("m1"));
        a.push_chunk("next msg", Some("m2")); // 新 message 重置
        let r = a.finish();
        assert_eq!(r.text, "next msg");
        assert_eq!(r.source, ResultSource::LastAssistantMessage);
        assert_eq!(r.total_bytes, 8);
        assert!(!r.truncated);
    }

    #[test]
    fn result_aggregation_turn_text_fallback() {
        let mut a = ResultAggregator::new();
        a.push_chunk("part1 ", None);
        a.push_chunk("part2", None);
        let r = a.finish();
        assert_eq!(r.text, "part1 part2");
        assert_eq!(r.source, ResultSource::TurnText);
    }

    #[test]
    fn result_aggregation_empty_and_truncation() {
        let a = ResultAggregator::new();
        let r = a.finish();
        assert_eq!(r.text, "");
        assert_eq!(r.source, ResultSource::Empty);
        let mut b = ResultAggregator::new();
        b.push_chunk(&"z".repeat(70 * 1024), Some("m"));
        let r2 = b.finish();
        assert_eq!(r2.text.len(), limits::RESULT_BUDGET);
        assert!(r2.truncated);
        assert_eq!(r2.total_bytes, 70 * 1024);
    }
}
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test events`
Expected: 编译失败（模块不存在）

- [ ] **Step 3: 实现 events.rs**

```rust
use crate::config::limits;
use serde::Serialize;
use serde_json::Value;
use std::collections::VecDeque;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    AgentMessage,
    AgentThought,
    ToolCall,
    ToolCallUpdate,
    ConfigUpdate,
    Usage,
    StatusChange,
    PermissionRequested,
    PermissionResolved,
    PermissionAutoDenied,
    PermissionCancelled,
    BridgeError,
    CompatNote,
}

#[derive(Debug, Clone, Serialize)]
pub struct Event {
    pub seq: u64,
    pub time: String, // UTC RFC 3339
    #[serde(rename = "type")]
    pub kind: EventKind,
    pub data: Value,
    pub truncated: bool,
}

fn utc_now() -> String {
    // 不引入 chrono：用 std 生成 RFC 3339 秒级时间
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    rfc3339_from_unix(secs)
}

pub fn rfc3339_from_unix(secs: u64) -> String {
    // 简化 UTC 转换（civil date 算法）
    let days = secs / 86400;
    let rem = secs % 86400;
    let (h, mi, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    // Howard Hinnant civil_from_days
    let z = days as i64 + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
}

/// 截断到不超过 max 字节且保持 UTF-8 边界；返回 (切片, 是否截断)
pub fn truncate_utf8(s: &str, max: usize) -> (&str, bool) {
    if s.len() <= max {
        return (s, false);
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    (&s[..end], true)
}

/// 事件 data 超限时替换为有界摘要；保证输出为有效 JSON/UTF-8。
pub fn sanitize_event_data(data: Value, max_bytes: usize) -> (Value, bool) {
    let serialized = serde_json::to_string(&data).unwrap_or_else(|_| "null".to_string());
    if serialized.len() <= max_bytes {
        return (data, false);
    }
    let original_bytes = serialized.len();
    let head_budget = max_bytes.saturating_sub(128); // 留给包装字段
    let (head, _) = truncate_utf8(&serialized, head_budget.max(0));
    (
        serde_json::json!({
            "summary": head,
            "original_bytes": original_bytes,
        }),
        true,
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventPage {
    pub events: Vec<Event>,
    pub earliest_cursor: u64,
    pub next_cursor: u64,
    pub last_cursor: u64,
    pub gap: bool,
    pub dropped_count: u64,
    pub has_more: bool,
}

#[derive(Debug)]
pub struct EventBuffer {
    events: VecDeque<Event>,
    next_seq: u64,
    total_bytes: usize,
    keep_count: usize,
    keep_bytes: usize,
}

impl EventBuffer {
    pub fn new(keep_count: usize, keep_bytes: usize) -> Self {
        Self {
            events: VecDeque::new(),
            next_seq: 1,
            total_bytes: 0,
            keep_count,
            keep_bytes,
        }
    }

    /// 写入事件；data 先按单事件上限 sanitize。返回分配的 seq。
    pub fn push(&mut self, kind: EventKind, data: Value) -> u64 {
        let (data, truncated) = sanitize_event_data(data, limits::EVENT_ITEM_MAX);
        let seq = self.next_seq;
        self.next_seq += 1;
        let size = serde_json::to_string(&data).map(|s| s.len()).unwrap_or(0) + 64;
        self.events.push_back(Event {
            seq,
            time: utc_now(),
            kind,
            data,
            truncated,
        });
        self.total_bytes += size;
        while self.events.len() > self.keep_count
            || (self.total_bytes > self.keep_bytes && self.events.len() > 1)
        {
            if let Some(ev) = self.events.pop_front() {
                let ev_size =
                    serde_json::to_string(&ev.data).map(|s| s.len()).unwrap_or(0) + 64;
                self.total_bytes = self.total_bytes.saturating_sub(ev_size);
            }
        }
        seq
    }

    pub fn last_cursor(&self) -> u64 {
        self.next_seq - 1
    }

    pub fn earliest_cursor(&self) -> u64 {
        self.events
            .front()
            .map(|e| e.seq)
            .unwrap_or_else(|| self.last_cursor() + 1)
    }

    pub fn page(&self, cursor: u64, limit: usize) -> EventPage {
        let last = self.last_cursor();
        let earliest = self.earliest_cursor();
        let gap = cursor + 1 < earliest;
        let dropped_count = if gap { earliest - cursor - 1 } else { 0 };
        let events: Vec<Event> = self
            .events
            .iter()
            .filter(|e| e.seq > cursor)
            .take(limit)
            .cloned()
            .collect();
        let next_cursor = events
            .last()
            .map(|e| e.seq)
            .unwrap_or_else(|| cursor.max(earliest.saturating_sub(1)));
        EventPage {
            events,
            earliest_cursor: earliest,
            next_cursor,
            last_cursor: last,
            gap,
            dropped_count,
            has_more: next_cursor < last,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultSource {
    LastAssistantMessage,
    TurnText,
    Empty,
}

#[derive(Debug, Clone, Serialize)]
pub struct TaskResult {
    pub text: String,
    pub truncated: bool,
    pub total_bytes: usize,
    pub source: ResultSource,
}

#[derive(Debug, Default)]
pub struct ResultAggregator {
    current: String,
    current_id: Option<String>,
    total_bytes: usize,
    truncated: bool,
    saw_message_id: bool,
    saw_text: bool,
}

impl ResultAggregator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_chunk(&mut self, text: &str, message_id: Option<&str>) {
        if text.is_empty() {
            return;
        }
        self.saw_text = true;
        match message_id {
            Some(id) => {
                self.saw_message_id = true;
                if self.current_id.as_deref() != Some(id) {
                    // 新消息：重置聚合（只保留最后一条消息）
                    self.current.clear();
                    self.total_bytes = 0;
                    self.truncated = false;
                    self.current_id = Some(id.to_string());
                }
            }
            None => {
                if self.saw_message_id {
                    // 规格 §9.2：不带 messageId 的更新降级为整轮聚合
                    self.saw_message_id = false;
                    self.current_id = None;
                    // 保留已有文本，继续整轮累计
                }
            }
        }
        self.total_bytes += text.len();
        let remaining = limits::RESULT_BUDGET.saturating_sub(self.current.len());
        if remaining == 0 {
            self.truncated = true;
            return;
        }
        let (piece, cut) = truncate_utf8(text, remaining);
        self.current.push_str(piece);
        if cut {
            self.truncated = true;
        }
    }

    pub fn finish(&self) -> TaskResult {
        if !self.saw_text {
            return TaskResult {
                text: String::new(),
                truncated: false,
                total_bytes: 0,
                source: ResultSource::Empty,
            };
        }
        TaskResult {
            text: self.current.clone(),
            truncated: self.truncated,
            total_bytes: self.total_bytes,
            source: if self.saw_message_id || self.current_id.is_some() {
                ResultSource::LastAssistantMessage
            } else {
                ResultSource::TurnText
            },
        }
    }
}
```

注意 `result_aggregation_turn_text_fallback` 测试中 `finish().source` 判定：聚合器从未见 message_id → TurnText。上面实现用 `saw_message_id || current_id.is_some()` 区分；混合场景（先有 id 后无）以最后一次出现的形态为准——`saw_message_id` 在降级时被清 false，`current_id` 为 None，归入 TurnText，符合规格 §9.2 降级语义。

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test events`
Expected: 9 个测试 PASS

- [ ] **Step 5: Commit**

```bash
git add src/events.rs src/main.rs
git commit -m "feat: bounded event buffer with cursor pagination and result aggregation"
```

---

### Task 3: 域模型与状态机（task.rs）

**Files:**
- Create: `src/task.rs`
- Modify: `src/main.rs`（加 `mod task;`）

**Interfaces:**
- Consumes: `events::{EventBuffer, ResultAggregator, TaskResult}`、`config::limits`
- Produces:
  - `pub type TaskId = Uuid; pub type BridgeSessionId = Uuid; pub type PermissionId = Uuid;`
  - `pub enum TaskStatus { Starting, Running, WaitingPermission, Cancelling, Completed, Failed, Cancelled }`（Serialize snake_case；`fn is_terminal(&self) -> bool`、`fn is_active(&self) -> bool`（starting/running/waiting_permission/cancelling，规格 §4.2-2））
  - `pub enum ExecutionState { NotStarted, InProgress, Stopped, Unknown }`
  - `pub enum SessionState { Opening, Idle, Busy, Closing, Closed, Lost }`（`fn accepts_prompt(&self) -> bool` 仅 Idle）
  - `pub enum BackendState { Dormant, Starting, Ready, Draining, Unavailable }`
  - `pub struct TaskError { pub code: ErrorCode, pub message: String, pub retryable: bool, pub details: Option<Value> }`（Serialize）
  - `pub enum ErrorCode { InvalidArgument, RequestConflict, TaskNotFound, StaleTask, SessionBusy, SessionClosing, SessionClosed, SessionLost, CapacityExceeded, PermissionNotFound, PermissionAlreadyResolved, PermissionExpired, PermissionCancelled, PermissionLimitExceeded, PermissionPayloadTooLarge, ExecutionStateUnknown, BackendUnavailable, RecoveryRequired, InstanceInUse, ConfigurationError, Internal }`（Serialize snake_case）
  - `pub struct Task { pub task_id, pub request_id: String, pub session_id: BridgeSessionId, pub parent_task_id: Option<TaskId>, pub status: TaskStatus, pub created_at: String, pub started_at: Option<String>, pub finished_at: Option<String>, pub finished_instant: Option<std::time::Instant>, pub stop_reason: Option<String>, pub result: Option<TaskResult>, pub error: Option<TaskError>, pub execution_state: ExecutionState, pub cancel_requested_at: Option<String>, pub events: EventBuffer, pub result_agg: ResultAggregator, pub phase: TaskPhase }`（`finished_instant` 供保留策略用单调时钟判断，与展示用 wall time 分离）
  - `pub enum TaskPhase { AwaitBackend, AwaitSession, PromptSent }`（内部阶段，starting 覆盖前两者）
  - `pub struct Session { pub session_id: BridgeSessionId, pub dsh_session_id: Option<String>, pub epoch: u64, pub cwd: PathBuf, pub state: SessionState, pub active_task_id: Option<TaskId>, pub latest_task_id: Option<TaskId>, pub last_activity: std::time::Instant }`
  - `impl Task { pub fn new(task_id, request_id, session_id, parent_task_id) -> Self; pub fn settle(&mut self, outcome: SettleOutcome, wall_now: &str, now: std::time::Instant) -> bool }`——终态只结算一次，已终态返回 false（规格 §4.2-4）。cwd 只存于 Session。
  - `pub enum SettleOutcome { Completed{stop_reason:String}, Failed{error:TaskError, execution:ExecutionState}, Cancelled{execution:ExecutionState} }`

不变量实现点：
- `settle` 只在非终态时生效；Completed 时 `result = result_agg.finish()`、`execution_state=Stopped`；Cancelled 的 execution 由调用方给出（NotStarted 或 Stopped）。
- `TaskStatus::is_active` 含 Cancelling（取消期间不释放额度，规格 §4.2-2）。

- [ ] **Step 1: 写失败测试**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn mk_task() -> Task {
        Task::new(
            Uuid::new_v4(),
            "req-1".into(),
            Uuid::new_v4(),
            None,
        )
    }

    #[test]
    fn status_predicates() {
        assert!(TaskStatus::Starting.is_active());
        assert!(TaskStatus::WaitingPermission.is_active());
        assert!(TaskStatus::Cancelling.is_active());
        assert!(!TaskStatus::Completed.is_active());
        assert!(TaskStatus::Completed.is_terminal());
        assert!(TaskStatus::Failed.is_terminal());
        assert!(TaskStatus::Cancelled.is_terminal());
    }

    #[test]
    fn settle_only_once() {
        let mut t = mk_task();
        assert!(t.settle(
            SettleOutcome::Completed { stop_reason: "end_turn".into() },
            "2026-10-19T00:00:00Z",
            Instant::now(),
        ));
        assert_eq!(t.status, TaskStatus::Completed);
        assert_eq!(t.execution_state, ExecutionState::Stopped);
        // 第二次结算被拒绝：取消旧任务不能影响已完成的任务
        assert!(!t.settle(
            SettleOutcome::Cancelled { execution: ExecutionState::Stopped },
            "2026-10-19T00:00:01Z",
            Instant::now(),
        ));
        assert_eq!(t.status, TaskStatus::Completed);
    }

    #[test]
    fn cancelled_records_execution_evidence() {
        let mut t = mk_task();
        t.settle(
            SettleOutcome::Cancelled { execution: ExecutionState::NotStarted },
            "2026-10-19T00:00:00Z",
            Instant::now(),
        );
        assert_eq!(t.execution_state, ExecutionState::NotStarted);
    }

    #[test]
    fn failed_can_carry_unknown_execution() {
        let mut t = mk_task();
        t.settle(
            SettleOutcome::Failed {
                error: TaskError {
                    code: ErrorCode::ExecutionStateUnknown,
                    message: "no stop evidence".into(),
                    retryable: false,
                    details: None,
                },
                execution: ExecutionState::Unknown,
            },
            "2026-10-19T00:00:00Z",
            Instant::now(),
        );
        assert_eq!(t.status, TaskStatus::Failed);
        assert_eq!(t.execution_state, ExecutionState::Unknown);
        assert_eq!(t.error.unwrap().code, ErrorCode::ExecutionStateUnknown);
    }

    #[test]
    fn session_prompt_gate() {
        assert!(SessionState::Idle.accepts_prompt());
        assert!(!SessionState::Busy.accepts_prompt());
        assert!(!SessionState::Opening.accepts_prompt());
        assert!(!SessionState::Closing.accepts_prompt());
        assert!(!SessionState::Closed.accepts_prompt());
        assert!(!SessionState::Lost.accepts_prompt());
    }

    #[test]
    fn task_events_and_result_available_after_terminal() {
        let mut t = mk_task();
        t.events.push(crate::events::EventKind::AgentMessage, json!({"text":"hi"}));
        t.result_agg.push_chunk("hi", Some("m1"));
        t.settle(
            SettleOutcome::Completed { stop_reason: "end_turn".into() },
            "2026-10-19T00:00:00Z",
            Instant::now(),
        );
        assert_eq!(t.result.as_ref().unwrap().text, "hi");
        assert_eq!(t.events.last_cursor(), 1);
    }
}
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test task`
Expected: 编译失败

- [ ] **Step 3: 实现 task.rs**

```rust
use crate::events::{EventBuffer, ResultAggregator, TaskResult};
use crate::config::limits;
use serde::Serialize;
use serde_json::Value;
use std::path::PathBuf;
use std::time::Instant;
use uuid::Uuid;

pub type TaskId = Uuid;
pub type BridgeSessionId = Uuid;
pub type PermissionId = Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Starting,
    Running,
    WaitingPermission,
    Cancelling,
    Completed,
    Failed,
    Cancelled,
}

impl TaskStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
    pub fn is_active(self) -> bool {
        matches!(
            self,
            Self::Starting | Self::Running | Self::WaitingPermission | Self::Cancelling
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionState {
    NotStarted,
    InProgress,
    Stopped,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    Opening,
    Idle,
    Busy,
    Closing,
    Closed,
    Lost,
}

impl SessionState {
    pub fn accepts_prompt(self) -> bool {
        matches!(self, Self::Idle)
    }
    pub fn counts_open(self) -> bool {
        matches!(self, Self::Opening | Self::Idle | Self::Busy | Self::Closing)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendState {
    Dormant,
    Starting,
    Ready,
    Draining,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    InvalidArgument,
    RequestConflict,
    TaskNotFound,
    StaleTask,
    SessionBusy,
    SessionClosing,
    SessionClosed,
    SessionLost,
    CapacityExceeded,
    PermissionNotFound,
    PermissionAlreadyResolved,
    PermissionExpired,
    PermissionCancelled,
    PermissionLimitExceeded,
    PermissionPayloadTooLarge,
    ExecutionStateUnknown,
    BackendUnavailable,
    RecoveryRequired,
    InstanceInUse,
    ConfigurationError,
    Internal,
}

#[derive(Debug, Clone, Serialize)]
pub struct TaskError {
    pub code: ErrorCode,
    pub message: String,
    pub retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

impl TaskError {
    pub fn new(code: ErrorCode, message: impl Into<String>, retryable: bool) -> Self {
        // 错误摘要有界（规格 §9.1）
        let msg = message.into();
        let (m, _) = crate::events::truncate_utf8(&msg, limits::ERROR_SUMMARY_MAX);
        Self {
            code,
            message: m.to_string(),
            retryable,
            details: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskPhase {
    AwaitBackend,
    AwaitSession,
    PromptSent,
}

#[derive(Debug)]
pub struct Task {
    pub task_id: TaskId,
    pub request_id: String,
    pub session_id: BridgeSessionId,
    pub parent_task_id: Option<TaskId>,
    pub status: TaskStatus,
    pub created_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub stop_reason: Option<String>,
    pub result: Option<TaskResult>,
    pub error: Option<TaskError>,
    pub execution_state: ExecutionState,
    pub cancel_requested_at: Option<String>,
    pub events: EventBuffer,
    pub result_agg: ResultAggregator,
    pub phase: TaskPhase,
}

pub enum SettleOutcome {
    Completed { stop_reason: String },
    Failed { error: TaskError, execution: ExecutionState },
    Cancelled { execution: ExecutionState },
}

impl Task {
    pub fn new(
        task_id: TaskId,
        request_id: String,
        session_id: BridgeSessionId,
        parent_task_id: Option<TaskId>,
    ) -> Self {
        Self {
            task_id,
            request_id,
            session_id,
            parent_task_id,
            status: TaskStatus::Starting,
            created_at: crate::events::rfc3339_now(),
            started_at: None,
            finished_at: None,
            finished_instant: None,
            stop_reason: None,
            result: None,
            error: None,
            execution_state: ExecutionState::NotStarted,
            cancel_requested_at: None,
            events: EventBuffer::new(limits::EVENT_KEEP_COUNT, limits::EVENT_KEEP_BYTES),
            result_agg: ResultAggregator::new(),
            phase: TaskPhase::AwaitBackend,
        }
    }

    /// 终态结算，恰好一次。已终态返回 false。
    pub fn settle(&mut self, outcome: SettleOutcome, wall_now: &str, now: Instant) -> bool {
        if self.status.is_terminal() {
            return false;
        }
        self.finished_at = Some(wall_now.to_string());
        self.finished_instant = Some(now);
        match outcome {
            SettleOutcome::Completed { stop_reason } => {
                self.status = TaskStatus::Completed;
                self.stop_reason = Some(stop_reason);
                self.execution_state = ExecutionState::Stopped;
                self.result = Some(self.result_agg.finish());
            }
            SettleOutcome::Failed { error, execution } => {
                self.status = TaskStatus::Failed;
                self.error = Some(error);
                self.execution_state = execution;
                let r = self.result_agg.finish();
                if r.source != crate::events::ResultSource::Empty {
                    self.result = Some(r);
                }
            }
            SettleOutcome::Cancelled { execution } => {
                self.status = TaskStatus::Cancelled;
                self.execution_state = execution;
                let r = self.result_agg.finish();
                if r.source != crate::events::ResultSource::Empty {
                    self.result = Some(r);
                }
            }
        }
        true
    }
}

#[derive(Debug)]
pub struct Session {
    pub session_id: BridgeSessionId,
    pub dsh_session_id: Option<String>,
    pub epoch: u64,
    pub cwd: PathBuf,
    pub state: SessionState,
    pub active_task_id: Option<TaskId>,
    pub latest_task_id: Option<TaskId>,
    pub last_activity: Instant,
}
```

并在 events.rs 增加 `pub fn rfc3339_now() -> String`（复用已有私有 `utc_now`，改 pub 并别名；同步把 `Event.time` 生成改为调用它）。

- [ ] **Step 4: 运行确认通过**

Run: `cargo test task`
Expected: 6 个测试 PASS

- [ ] **Step 5: Commit**

```bash
git add src/task.rs src/main.rs src/events.rs
git commit -m "feat: domain model with settle-once task lifecycle"
```

---

### Task 4: 权限裁决状态机（permission.rs）

**Files:**
- Create: `src/permission.rs`
- Modify: `src/main.rs`（加 `mod permission;`）

**Interfaces:**
- Consumes: `task::{TaskId, BridgeSessionId, PermissionId, ErrorCode, TaskError}`、`config::limits`、ACP 类型 `RequestPermissionResponse`
- Produces:
  - `pub enum PermKind { AllowOnce, AllowAlways, RejectOnce, RejectAlways, Other }`
  - `pub struct PermissionOptionView { pub option_id: String, pub name: String, pub kind: PermKind }`
  - `pub enum PermResponder { Real(Responder<RequestPermissionResponse>), Test(Box<dyn FnOnce(RequestPermissionResponse) + Send>) }`（`fn reply(self, response: RequestPermissionResponse) -> Result<(), String>`）
  - `pub struct PendingPermission { pub permission_id, pub task_id, pub session_id, pub epoch: u64, pub tool_call_id: Option<String>, pub tool_summary: Option<String>, pub details_incomplete: bool, pub options: Vec<PermissionOptionView>, pub expires_at: String, pub deadline: Instant }`
  - `pub struct PendingPermissionView { permission_id, tool_call_id, tool_summary, details_incomplete, options, expires_at }`（Serialize，给 get_task 快照）
  - `pub enum PermError { NotFound, InvalidOption, AlreadyResolved, Expired, Cancelled, PayloadTooLarge, LimitExceeded }`
  - `pub enum DecisionKind { Selected, AutoDenied, Cancelled }`
  - `pub struct Decision { pub kind: DecisionKind, pub option_id: Option<String> }`
  - `pub struct PermissionManager { .. }`：
    - `fn register(&mut self, task_id, session_id, epoch, tool_call_id: Option<String>, tool_summary: Option<String>, details_incomplete: bool, options: Vec<PermissionOptionView>, responder: PermResponder, deadline: Instant, expires_at: String) -> Result<PendingPermissionView, PermError>`
    - `fn decide(&mut self, permission_id: PermissionId, option_id: &str) -> Result<(Decision, Option<PermResponder>), PermError>`——首次裁决返回 `Some(responder)` 供异步回复；幂等重复返回 `None`
    - `fn cancel_for_task(&mut self, task_id) -> Vec<(PermissionId, PermResponder)>`
    - `fn expire_one(&mut self, permission_id) -> Option<(TaskId, Decision, PermResponder)>`——选实际提供的 RejectOnce；没有拒绝类选项则 outcome=Cancelled（规格 §10）
    - `fn pending_snapshot(&self, task_id) -> Vec<PendingPermissionView>`
    - `fn pending_count(&self, task_id) -> usize`
    - `fn task_of(&self, permission_id) -> Option<TaskId>`

规则（规格 §9.3/§10）：
- register 前校验：options ≤ `PERM_OPTIONS_MAX`（8）、每个 option_id ≤ 256 字节、单项序列化 ≤ `PERM_ITEM_MAX`（4 KiB）；违反 → `PayloadTooLarge`（调用方回 ACP cancelled 并取消任务）。
- 同任务 pending ≥ `PENDING_PERMS_MAX`（16）→ `LimitExceeded`（调用方回复 cancelled 并走任务取消）。
- decide：pending→selected 恰好一次；同 option_id 重复 → 返回已有裁决（不含 responder，`PermResponder::none()` 哨兵或单独枚举）；不同 option → `AlreadyResolved`；任务已取消/超时已抢先 → `Cancelled`/`Expired`。
- 已决记录环最多 `DECIDED_PERMS_KEEP`（64），淘汰后 decide 返回 `NotFound`。
- decide 返回的 responder 用于异步回复 `RequestPermissionOutcome::Selected(...)`；auto-deny/cancel 回复 `Cancelled`。

`PermResponder` 无"空"变体：decide 的幂等重复路径由 `Option<PermResponder>` 的 `None` 表达。

- [ ] **Step 1: 写失败测试**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    fn opts() -> Vec<PermissionOptionView> {
        vec![
            PermissionOptionView {
                option_id: "allow".into(),
                name: "Allow once".into(),
                kind: PermKind::AllowOnce,
            },
            PermissionOptionView {
                option_id: "reject".into(),
                name: "Reject once".into(),
                kind: PermKind::RejectOnce,
            },
        ]
    }

    fn capture() -> (PermResponder, Arc<Mutex<Vec<RequestPermissionOutcome>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen2 = seen.clone();
        (
            PermResponder::Test(Box::new(move |resp| {
                seen2.lock().unwrap().push(resp.outcome);
            })),
            seen,
        )
    }

    fn register_one(m: &mut PermissionManager, task: TaskId) -> (PermissionId, Arc<Mutex<Vec<RequestPermissionOutcome>>>) {
        let (r, seen) = capture();
        let v = m
            .register(
                task,
                BridgeSessionId::new_v4(),
                1,
                Some("tc-1".into()),
                Some("write file /tmp/x".into()),
                false,
                opts(),
                r,
                Instant::now() + Duration::from_secs(600),
                "2026-10-19T00:10:00Z".into(),
            )
            .unwrap();
        (v.permission_id, seen)
    }

    #[test]
    fn register_and_snapshot() {
        let mut m = PermissionManager::new();
        let task = TaskId::new_v4();
        let (pid, _) = register_one(&mut m, task);
        assert_eq!(m.pending_count(task), 1);
        let snap = m.pending_snapshot(task);
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].permission_id, pid);
        assert_eq!(snap[0].tool_call_id.as_deref(), Some("tc-1"));
        assert_eq!(m.task_of(pid), Some(task));
    }

    #[test]
    fn decide_once_and_idempotent_replay() {
        let mut m = PermissionManager::new();
        let task = TaskId::new_v4();
        let (pid, seen) = register_one(&mut m, task);
        let (d, responder) = m.decide(pid, "allow").unwrap();
        assert_eq!(d.kind, DecisionKind::Selected);
        assert_eq!(d.option_id.as_deref(), Some("allow"));
        responder
            .expect("first decision carries the responder")
            .reply(RequestPermissionResponse::new(
                RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new("allow")),
            ))
            .unwrap();
        assert_eq!(seen.lock().unwrap().len(), 1);
        // 同选项重复提交：幂等返回已有裁决，不再有 responder
        let (d2, r2) = m.decide(pid, "allow").unwrap();
        assert_eq!(d2.kind, DecisionKind::Selected);
        assert!(r2.is_none());
        // 改选：拒绝
        assert!(matches!(
            m.decide(pid, "reject"),
            Err(PermError::AlreadyResolved)
        ));
        assert_eq!(seen.lock().unwrap().len(), 1); // 未二次回复
    }

    #[test]
    fn decide_unknown_option_and_unknown_id() {
        let mut m = PermissionManager::new();
        let task = TaskId::new_v4();
        let (pid, _) = register_one(&mut m, task);
        assert!(matches!(
            m.decide(pid, "not-an-option"),
            Err(PermError::InvalidOption)
        ));
        assert!(matches!(
            m.decide(PermissionId::new_v4(), "allow"),
            Err(PermError::NotFound)
        ));
    }

    #[test]
    fn timeout_auto_denies_with_reject_once() {
        let mut m = PermissionManager::new();
        let task = TaskId::new_v4();
        let (pid, seen) = register_one(&mut m, task);
        let (t, decision, responder) = m.expire_one(pid).unwrap();
        assert_eq!(t, task);
        assert_eq!(decision.kind, DecisionKind::AutoDenied);
        assert_eq!(decision.option_id.as_deref(), Some("reject"));
        responder
            .reply(RequestPermissionResponse::new(
                RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new("reject")),
            ))
            .unwrap();
        assert_eq!(seen.lock().unwrap().len(), 1);
        // 已过期后用户再裁决
        assert!(matches!(
            m.decide(pid, "allow"),
            Err(PermError::Expired)
        ));
    }

    #[test]
    fn timeout_without_reject_option_cancels() {
        let mut m = PermissionManager::new();
        let task = TaskId::new_v4();
        let (r, _seen) = capture();
        let v = m
            .register(
                task,
                BridgeSessionId::new_v4(),
                1,
                None,
                None,
                true,
                vec![PermissionOptionView {
                    option_id: "allow".into(),
                    name: "Allow".into(),
                    kind: PermKind::AllowOnce,
                }],
                r,
                Instant::now() + Duration::from_secs(600),
                "t".into(),
            )
            .unwrap();
        let (_, decision, _) = m.expire_one(v.permission_id).unwrap();
        assert_eq!(decision.kind, DecisionKind::Cancelled);
        assert_eq!(decision.option_id, None);
    }

    #[test]
    fn cancel_for_task_replies_cancelled_to_all_pending() {
        let mut m = PermissionManager::new();
        let task = TaskId::new_v4();
        let (_p1, seen1) = register_one(&mut m, task);
        let (_p2, seen2) = register_one(&mut m, task);
        let responders = m.cancel_for_task(task);
        assert_eq!(responders.len(), 2);
        for (_id, r) in responders {
            r.reply(RequestPermissionResponse::new(RequestPermissionOutcome::Cancelled))
                .unwrap();
        }
        assert!(matches!(
            seen1.lock().unwrap()[0],
            RequestPermissionOutcome::Cancelled
        ));
        assert!(matches!(
            seen2.lock().unwrap()[0],
            RequestPermissionOutcome::Cancelled
        ));
        assert_eq!(m.pending_count(task), 0);
    }

    #[test]
    fn payload_limits_enforced() {
        let mut m = PermissionManager::new();
        let task = TaskId::new_v4();
        let (r, _) = capture();
        let too_many: Vec<PermissionOptionView> = (0..9)
            .map(|i| PermissionOptionView {
                option_id: format!("o{i}"),
                name: "x".into(),
                kind: PermKind::AllowOnce,
            })
            .collect();
        assert!(matches!(
            m.register(task, BridgeSessionId::new_v4(), 1, None, None, false, too_many, r,
                Instant::now(), "t".into()),
            Err(PermError::PayloadTooLarge)
        ));
        let (r2, _) = capture();
        let long_id = vec![PermissionOptionView {
            option_id: "x".repeat(257),
            name: "x".into(),
            kind: PermKind::AllowOnce,
        }];
        assert!(matches!(
            m.register(task, BridgeSessionId::new_v4(), 1, None, None, false, long_id, r2,
                Instant::now(), "t".into()),
            Err(PermError::PayloadTooLarge)
        ));
    }

    #[test]
    fn pending_limit_per_task() {
        let mut m = PermissionManager::new();
        let task = TaskId::new_v4();
        for _ in 0..16 {
            register_one(&mut m, task);
        }
        let (r, _) = capture();
        assert!(matches!(
            m.register(task, BridgeSessionId::new_v4(), 1, None, None, false, opts(), r,
                Instant::now(), "t".into()),
            Err(PermError::LimitExceeded)
        ));
    }

    #[test]
    fn decided_ring_eviction() {
        let mut m = PermissionManager::new();
        let task = TaskId::new_v4();
        let mut first_pid = None;
        for i in 0..70 {
            let (pid, _) = register_one(&mut m, task);
            if i == 0 {
                first_pid = Some(pid);
            }
            m.decide(pid, "allow").unwrap();
        }
        // 已决记录只保留 64 个，最早的被淘汰后按 not_found 处理
        assert!(matches!(
            m.decide(first_pid.unwrap(), "allow"),
            Err(PermError::NotFound)
        ));
    }
}
```

`PermError` 需含 `InvalidOption` 变体（decide 的 option_id 不在公布选项中，规格 §5.5"只能选择该请求公布的原始 option_id"）。

- [ ] **Step 2: 运行确认失败**

Run: `cargo test permission`
Expected: 编译失败

- [ ] **Step 3: 实现 permission.rs**

```rust
use crate::config::limits;
use crate::task::{BridgeSessionId, PermissionId, TaskId};
use agent_client_protocol::schema::v1::{
    RequestPermissionOutcome, RequestPermissionResponse, Responder, SelectedPermissionOutcome,
};
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::time::Instant;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PermKind {
    AllowOnce,
    AllowAlways,
    RejectOnce,
    RejectAlways,
    Other,
}

#[derive(Debug, Clone, Serialize)]
pub struct PermissionOptionView {
    pub option_id: String,
    pub name: String,
    pub kind: PermKind,
}

/// 一次性回复通道：真实 ACP responder 或测试捕获。
pub enum PermResponder {
    Real(Responder<RequestPermissionResponse>),
    Test(Box<dyn FnOnce(RequestPermissionResponse) + Send>),
}

impl PermResponder {
    pub fn reply(self, response: RequestPermissionResponse) -> Result<(), String> {
        match self {
            PermResponder::Real(r) => r.respond(response).map_err(|e| e.to_string()),
            PermResponder::Test(f) => {
                f(response);
                Ok(())
            }
        }
    }
}
```

完整实现（接上）：

```rust
#[derive(Debug, Clone, Serialize)]
pub struct PendingPermissionView {
    pub permission_id: PermissionId,
    pub tool_call_id: Option<String>,
    pub tool_summary: Option<String>,
    pub details_incomplete: bool,
    pub options: Vec<PermissionOptionView>,
    pub expires_at: String,
}

pub struct PendingPermission {
    pub view: PendingPermissionView,
    pub task_id: TaskId,
    pub session_id: BridgeSessionId,
    pub epoch: u64,
    pub deadline: Instant,
    responder: Option<PermResponder>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionKind {
    Selected,
    AutoDenied,
    Cancelled,
}

#[derive(Debug, Clone)]
pub struct Decision {
    pub kind: DecisionKind,
    pub option_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PermError {
    NotFound,
    InvalidOption,
    AlreadyResolved,
    Expired,
    Cancelled,
    PayloadTooLarge,
    LimitExceeded,
}

struct DecidedRecord {
    permission_id: PermissionId,
    task_id: TaskId,
    decision: Decision,
}

#[derive(Default)]
pub struct PermissionManager {
    pending: HashMap<PermissionId, PendingPermission>,
    decided: VecDeque<DecidedRecord>,
}

impl PermissionManager {
    pub fn new() -> Self {
        Self::default()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn register(
        &mut self,
        task_id: TaskId,
        session_id: BridgeSessionId,
        epoch: u64,
        tool_call_id: Option<String>,
        tool_summary: Option<String>,
        details_incomplete: bool,
        options: Vec<PermissionOptionView>,
        responder: PermResponder,
        deadline: Instant,
        expires_at: String,
    ) -> Result<PendingPermissionView, PermError> {
        if options.len() > limits::PERM_OPTIONS_MAX
            || options
                .iter()
                .any(|o| o.option_id.len() > limits::PERM_OPTION_ID_MAX)
        {
            return Err(PermError::PayloadTooLarge);
        }
        let view = PendingPermissionView {
            permission_id: Uuid::new_v4(),
            tool_call_id,
            tool_summary,
            details_incomplete,
            options,
            expires_at,
        };
        let size = serde_json::to_string(&view).map(|s| s.len()).unwrap_or(usize::MAX);
        if size > limits::PERM_ITEM_MAX {
            return Err(PermError::PayloadTooLarge);
        }
        let count = self
            .pending
            .values()
            .filter(|p| p.task_id == task_id)
            .count();
        if count >= limits::PENDING_PERMS_MAX {
            return Err(PermError::LimitExceeded);
        }
        let p = PendingPermission {
            view: view.clone(),
            task_id,
            session_id,
            epoch,
            deadline,
            responder: Some(responder),
        };
        self.pending.insert(view.permission_id, p);
        Ok(view)
    }

    fn record_decision(&mut self, permission_id: PermissionId, task_id: TaskId, decision: Decision) {
        self.decided.push_back(DecidedRecord {
            permission_id,
            task_id,
            decision,
        });
        while self.decided.len() > limits::DECIDED_PERMS_KEEP {
            self.decided.pop_front();
        }
    }

    pub fn decide(
        &mut self,
        permission_id: PermissionId,
        option_id: &str,
    ) -> Result<(Decision, Option<PermResponder>), PermError> {
        if let Some(rec) = self.decided.iter().find(|r| r.permission_id == permission_id) {
            return match rec.decision.kind {
                DecisionKind::Selected
                    if rec.decision.option_id.as_deref() == Some(option_id) =>
                {
                    Ok((rec.decision.clone(), None))
                }
                DecisionKind::Selected => Err(PermError::AlreadyResolved),
                DecisionKind::AutoDenied => Err(PermError::Expired),
                DecisionKind::Cancelled => Err(PermError::Cancelled),
            };
        }
        let p = self.pending.get(&permission_id).ok_or(PermError::NotFound)?;
        if !p.view.options.iter().any(|o| o.option_id == option_id) {
            return Err(PermError::InvalidOption);
        }
        let mut p = self.pending.remove(&permission_id).unwrap();
        let decision = Decision {
            kind: DecisionKind::Selected,
            option_id: Some(option_id.to_string()),
        };
        self.record_decision(permission_id, p.task_id, decision.clone());
        Ok((decision, p.responder.take()))
    }

    pub fn cancel_for_task(&mut self, task_id: TaskId) -> Vec<(PermissionId, PermResponder)> {
        let ids: Vec<PermissionId> = self
            .pending
            .iter()
            .filter(|(_, p)| p.task_id == task_id)
            .map(|(id, _)| *id)
            .collect();
        let mut out = Vec::new();
        for id in ids {
            if let Some(mut p) = self.pending.remove(&id) {
                self.record_decision(
                    id,
                    task_id,
                    Decision {
                        kind: DecisionKind::Cancelled,
                        option_id: None,
                    },
                );
                if let Some(r) = p.responder.take() {
                    out.push((id, r));
                }
            }
        }
        out
    }

    /// 超时自动裁决：优先选实际提供的 RejectOnce；无拒绝类选项 → Cancelled。
    /// 不硬编码 option_id（规格 §10）。
    pub fn expire_one(
        &mut self,
        permission_id: PermissionId,
    ) -> Option<(TaskId, Decision, PermResponder)> {
        let mut p = self.pending.remove(&permission_id)?;
        let reject = p
            .view
            .options
            .iter()
            .find(|o| o.kind == PermKind::RejectOnce)
            .map(|o| o.option_id.clone());
        let decision = match reject {
            Some(id) => Decision {
                kind: DecisionKind::AutoDenied,
                option_id: Some(id),
            },
            None => Decision {
                kind: DecisionKind::Cancelled,
                option_id: None,
            },
        };
        let task_id = p.task_id;
        self.record_decision(permission_id, task_id, decision.clone());
        Some((task_id, decision, p.responder.take()?))
    }

    pub fn pending_snapshot(&self, task_id: TaskId) -> Vec<PendingPermissionView> {
        let mut v: Vec<_> = self
            .pending
            .values()
            .filter(|p| p.task_id == task_id)
            .map(|p| p.view.clone())
            .collect();
        v.sort_by_key(|p| p.expires_at.clone());
        v
    }

    pub fn pending_count(&self, task_id: TaskId) -> usize {
        self.pending.values().filter(|p| p.task_id == task_id).count()
    }

    pub fn task_of(&self, permission_id: PermissionId) -> Option<TaskId> {
        self.pending
            .get(&permission_id)
            .map(|p| p.task_id)
            .or_else(|| {
                self.decided
                    .iter()
                    .find(|r| r.permission_id == permission_id)
                    .map(|r| r.task_id)
            })
    }
}
```

测试适配点：
- import 加 `use agent_client_protocol::schema::v1::{RequestPermissionOutcome, RequestPermissionResponse, SelectedPermissionOutcome};` 与 `use uuid::Uuid;`（`TaskId::new_v4()` → `Uuid::new_v4()`，因 TaskId 是 Uuid 别名）。
- `decide` 返回 `Option<PermResponder>`：首个测试改 `let Some(responder) = r1 else { panic!() }` 形态断言。
- expire 测试中 `DecisionKind::Cancelled` 对应无拒绝选项场景；回复的 ACP outcome 恒为 `RequestPermissionOutcome::Cancelled`（cancel）或 `Selected(reject_id)`（auto_deny）——由协调层构造，`expire_one` 只返回 Decision。

- [ ] **Step 4: 运行确认通过**

Run: `cargo test permission`
Expected: 9 个测试 PASS

- [ ] **Step 5: Commit**

```bash
git add src/permission.rs src/main.rs
git commit -m "feat: one-shot permission decision state machine"
```

---

### Task 5: 实例锁与恢复保护标记（runtime_guard.rs）

**Files:**
- Create: `src/runtime_guard.rs`
- Modify: `src/main.rs`（加 `mod runtime_guard;`）

**Interfaces:**
- Consumes: rustix fs/flock；`config::Config.state_dir`
- Produces:
  - `pub enum GuardError { InstanceInUse, Io(std::io::Error) }`
  - `pub enum AcquireOutcome { Clean(RuntimeGuard), Recovery(RuntimeGuard) }`——发现旧标记时仍持有锁进入恢复模式（规格 §8.2：继续提供 MCP，禁止新派发）
  - `pub struct RuntimeGuard { .. }`：
    - `pub fn acquire(dir: &Path) -> Result<AcquireOutcome, GuardError>`——建目录 0700；打开 `instance.lock`（O_CLOEXEC）；flock 排他非阻塞，冲突 → `InstanceInUse`；存在 `run-state.json` → `Ok(AcquireOutcome::Recovery(..))`；干净 → `Ok(AcquireOutcome::Clean(..))`
    - `pub fn mark_started(&self, instance_id: Uuid) -> io::Result<()>`——原子写（tmp+rename+fsync 文件与目录）`run-state.json`，0600，内容 `{"instance_id","phase":"starting","started_at"}`
    - `pub fn mark_child_pid(&self, pid: u32) -> io::Result<()>`——重写标记补充 `phase:"running","child_pid"`
    - `pub fn clear(&self) -> io::Result<()>`——仅当全部会话确认关闭且子进程退出后调用：删除标记 + fsync 目录
    - `pub fn acknowledge_cleanup(dir: &Path) -> Result<(), GuardError>`——`--acknowledge-cleanup` 入口：取得同一独占锁（冲突 → InstanceInUse），删除标记，退出
  - 锁 FD 带 CLOEXEC，不传入子进程；锁随进程退出由 OS 释放，不以"锁文件存在"代替 OS 锁（规格 §8.2、§11.2）

- [ ] **Step 1: 核实 rustix flock API**

Run: `grep -rn "pub fn flock" ~/.cargo/registry/src/*/rustix-1*/src/fs/ | head -3; grep -rn "enum FlockOperation" -A 12 ~/.cargo/registry/src/*/rustix-1*/src/fs/*.rs | head -20`
Expected: 确认 `rustix::fs::flock(fd, FlockOperation)` 与 `FlockOperation::{LockExclusive, NonBlocking, Unlock}` 变体名；若名称不同，按实际 API 调整下方实现（保持语义：排他+非阻塞）。

- [ ] **Step 2: 写失败测试**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn double_acquire_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let g1 = RuntimeGuard::acquire(dir.path()).unwrap();
        assert!(matches!(
            RuntimeGuard::acquire(dir.path()),
            Err(GuardError::InstanceInUse)
        ));
        drop(g1); // 锁随 FD 关闭释放
        assert!(RuntimeGuard::acquire(dir.path()).is_ok());
    }

    #[test]
    fn stale_marker_enters_recovery_mode() {
        let dir = tempfile::tempdir().unwrap();
        {
            let g = unwrap_clean(RuntimeGuard::acquire(dir.path()));
            g.mark_started(Uuid::new_v4()).unwrap();
            g.mark_child_pid(12345).unwrap();
            // 模拟异常退出：不调用 clear
        }
        // 旧标记 → 恢复模式（仍持有锁，可服务 MCP 但禁新派发）
        assert!(matches!(
            RuntimeGuard::acquire(dir.path()),
            Ok(AcquireOutcome::Recovery(_))
        ));
        // 人工确认后恢复
        RuntimeGuard::acknowledge_cleanup(dir.path()).unwrap();
        assert!(matches!(
            RuntimeGuard::acquire(dir.path()),
            Ok(AcquireOutcome::Clean(_))
        ));
    }

    fn unwrap_clean(r: Result<AcquireOutcome, GuardError>) -> RuntimeGuard {
        match r {
            Ok(AcquireOutcome::Clean(g)) => g,
            _ => panic!("expected clean acquire"),
        }
    }

    #[test]
    fn clean_lifecycle_allows_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let g = unwrap_clean(RuntimeGuard::acquire(dir.path()));
            g.mark_started(Uuid::new_v4()).unwrap();
            g.clear().unwrap();
        }
        assert!(RuntimeGuard::acquire(dir.path()).is_ok());
    }

    #[test]
    fn marker_content_excludes_secrets() {
        let dir = tempfile::tempdir().unwrap();
        let g = unwrap_clean(RuntimeGuard::acquire(dir.path()));
        g.mark_started(Uuid::new_v4()).unwrap();
        g.mark_child_pid(42).unwrap();
        let raw = std::fs::read_to_string(dir.path().join("run-state.json")).unwrap();
        let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert!(v.get("instance_id").is_some());
        assert!(v.get("child_pid").is_some());
        assert!(v.get("prompt").is_none());
        assert!(v.get("env").is_none());
        // 文件权限 0600
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(dir.path().join("run-state.json"))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }
    }
}
```

（测试需 `tempfile` dev-dependency——已在 Task 1 Cargo.toml 中。`Uuid` 需 `use uuid::Uuid`。）

- [ ] **Step 3: 运行确认失败**

Run: `cargo test runtime_guard`
Expected: 编译失败

- [ ] **Step 4: 实现 runtime_guard.rs**

```rust
use rustix::fs::{FlockOperation, Mode, OFlags, flock, open};
use serde_json::json;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use uuid::Uuid;

const LOCK_FILE: &str = "instance.lock";
const MARKER_FILE: &str = "run-state.json";

#[derive(Debug)]
pub enum GuardError {
    InstanceInUse,
    Io(io::Error),
}

/// acquire 结果：干净启动，或发现旧标记进入恢复模式（仍持锁）。
pub enum AcquireOutcome {
    Clean(RuntimeGuard),
    Recovery(RuntimeGuard),
}

impl std::fmt::Display for GuardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InstanceInUse => write!(f, "instance_in_use"),
            Self::Io(e) => write!(f, "io: {e}"),
        }
    }
}
impl std::error::Error for GuardError {}
impl From<io::Error> for GuardError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

pub struct RuntimeGuard {
    dir: PathBuf,
    _lock: fs::File, // 持有锁至进程退出
}

fn acquire_lock(dir: &Path) -> Result<fs::File, GuardError> {
    let file = open(
        dir.join(LOCK_FILE),
        OFlags::CREATE | OFlags::RDWR | OFlags::CLOEXEC,
        Mode::from_bits_truncate(0o600),
    )
    .map_err(io::Error::from)?;
    flock(&file, FlockOperation::LockExclusive | FlockOperation::NonBlocking).map_err(|e| {
        if e == rustix::io::Errno::WOULDBLOCK {
            GuardError::InstanceInUse
        } else {
            GuardError::Io(io::Error::from(e))
        }
    })?;
    Ok(file)
}

fn write_atomic(dir: &Path, name: &str, contents: &str) -> io::Result<()> {
    let tmp = dir.join(format!(".{name}.tmp"));
    {
        use std::io::Write;
        let mut f = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            f.set_permissions(fs::Permissions::from_mode(0o600))?;
        }
        f.write_all(contents.as_bytes())?;
        f.sync_all()?;
    }
    fs::rename(&tmp, dir.join(name))?;
    // fsync 目录项
    let d = fs::File::open(dir)?;
    d.sync_all()?;
    Ok(())
}

impl RuntimeGuard {
    pub fn acquire(dir: &Path) -> Result<AcquireOutcome, GuardError> {
        fs::create_dir_all(dir)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(dir, fs::Permissions::from_mode(0o700))?;
        }
        let lock = acquire_lock(dir)?;
        let guard = RuntimeGuard {
            dir: dir.to_path_buf(),
            _lock: lock,
        };
        if dir.join(MARKER_FILE).exists() {
            // 旧标记：恢复模式，保留锁与标记，禁止新派发（规格 §8.2）
            return Ok(AcquireOutcome::Recovery(guard));
        }
        Ok(AcquireOutcome::Clean(guard))
    }

    pub fn mark_started(&self, instance_id: Uuid) -> io::Result<()> {
        let body = json!({
            "instance_id": instance_id,
            "phase": "starting",
            "started_at": crate::events::rfc3339_now(),
        });
        write_atomic(&self.dir, MARKER_FILE, &body.to_string())
    }

    pub fn mark_child_pid(&self, pid: u32) -> io::Result<()> {
        let path = self.dir.join(MARKER_FILE);
        let prev: serde_json::Value = fs::read_to_string(&path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_else(|| json!({}));
        let body = json!({
            "instance_id": prev.get("instance_id").cloned().unwrap_or(json!(null)),
            "phase": "running",
            "started_at": prev.get("started_at").cloned().unwrap_or(json!(null)),
            "child_pid": pid,
        });
        write_atomic(&self.dir, MARKER_FILE, &body.to_string())
    }

    /// 仅在全部会话确认关闭、子进程退出且无未知清理结果后调用。
    pub fn clear(&self) -> io::Result<()> {
        match fs::remove_file(self.dir.join(MARKER_FILE)) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        fs::File::open(&self.dir)?.sync_all()
    }

    /// `dsh-mcp --acknowledge-cleanup`：人工确认清理后取得独占锁并删除标记。
    pub fn acknowledge_cleanup(dir: &Path) -> Result<(), GuardError> {
        let lock = acquire_lock(dir)?;
        match fs::remove_file(dir.join(MARKER_FILE)) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(GuardError::Io(e)),
        }
        fs::File::open(dir)
            .and_then(|d| d.sync_all())
            .map_err(GuardError::Io)?;
        drop(lock);
        Ok(())
    }
}
```

- [ ] **Step 5: 运行确认通过**

Run: `cargo test runtime_guard`
Expected: 4 个测试 PASS

- [ ] **Step 6: Commit**

```bash
git add src/runtime_guard.rs src/main.rs
git commit -m "feat: instance lock and recovery-protection marker"
```

---

### Task 6: ACP 视图类型、Backend trait 与 FakeBackend（acp.rs 前半）

**Files:**
- Create: `src/acp.rs`
- Modify: `src/main.rs`（加 `mod acp;`）

**Interfaces:**
- Consumes: ACP schema v1 类型；`permission::{PermKind, PermissionOptionView, PermResponder}`
- Produces（供 coordinator / fake 测试使用）：
  - `pub enum SessionUpdateView { AgentMessageChunk { text: String, message_id: Option<String> }, AgentThoughtChunk { text: String }, ToolCall { tool_call_id: String, title: String, name: Option<String>, status: String }, ToolCallUpdate { tool_call_id: String, status: Option<String>, summary: Option<String> }, ConfigUpdate { summary: String }, Usage { summary: String }, Other { kind: String } }`
  - `pub fn view_of_update(u: &SessionUpdate) -> SessionUpdateView`（SDK → 视图翻译，non_exhaustive 用 `_` 兜底）
  - `pub struct PermissionRequestView { pub tool_call_id: Option<String>, pub options: Vec<PermissionOptionView> }`
  - `pub fn view_of_permission_request(r: &RequestPermissionRequest) -> PermissionRequestView`
  - `pub enum StopReasonView { EndTurn, MaxTokens, MaxTurnRequests, Refusal, Cancelled, Other(String) }`
  - `pub enum BackendErrorKind { Timeout, Protocol, ProcessDied, Unavailable, Rejected }`
  - `pub struct BackendError { pub kind: BackendErrorKind, pub message: String }`（Display/Error）
  - `pub enum BackendEvent { Update { epoch: u64, dsh_session_id: String, update: SessionUpdateView }, Permission { epoch: u64, dsh_session_id: String, request: PermissionRequestView, responder: PermResponder }, Died { epoch: u64, reason: String } }`
  - `pub trait Backend: Clone + Send + 'static { async fn new_session(&self, cwd: &Path) -> Result<String, BackendError>; async fn prompt(&self, dsh_session_id: &str, text: String) -> Result<StopReasonView, BackendError>; async fn cancel(&self, dsh_session_id: &str) -> Result<(), BackendError>; async fn close_session(&self, dsh_session_id: &str) -> Result<(), BackendError>; }`（`#[allow(async_fn_in_trait)]`）
  - `pub trait BackendFactory: Send + 'static { type B: Backend; async fn start(&mut self) -> Result<(Self::B, mpsc::UnboundedReceiver<BackendEvent>, u32), BackendError>; }`（返回后端句柄、事件流、child PID）
  - `pub struct FakeBackend` / `pub struct FakeDriver` / `pub fn fake_backend() -> (FakeFactory, FakeDriver)`：

```rust
pub struct FakeDriver {
    pub new_session_rx: mpsc::UnboundedReceiver<(PathBuf, oneshot::Sender<Result<String, BackendError>>)>,
    pub prompt_rx: mpsc::UnboundedReceiver<(String, String, oneshot::Sender<Result<StopReasonView, BackendError>>)>,
    pub cancel_rx: mpsc::UnboundedReceiver<String>,
    pub close_rx: mpsc::UnboundedReceiver<(String, oneshot::Sender<Result<(), BackendError>>)>,
    pub event_tx: mpsc::UnboundedSender<BackendEvent>, // 测试注入 update/permission/died
    pub fail_starts: usize, // start() 前 N 次失败（构造时设定）
}
```

- [ ] **Step 1: 写失败测试**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[tokio::test]
    async fn fake_backend_roundtrip() {
        let (mut factory, mut driver) = fake_backend();
        let (backend, _events, pid) = factory.start().await.unwrap();
        assert_eq!(pid, 0); // fake 无真实进程，pid=0

        // new_session
        let b = backend.clone();
        let j = tokio::spawn(async move { b.new_session(Path::new("/tmp/t")).await });
        let (cwd, reply) = driver.new_session_rx.recv().await.unwrap();
        assert_eq!(cwd, PathBuf::from("/tmp/t"));
        reply.send(Ok("dsh-sess-1".into())).unwrap();
        assert_eq!(j.await.unwrap().unwrap(), "dsh-sess-1");

        // prompt 挂起直到测试结算
        let b = backend.clone();
        let j = tokio::spawn(async move { b.prompt("dsh-sess-1", "do it".into()).await });
        let (sid, text, reply) = driver.prompt_rx.recv().await.unwrap();
        assert_eq!((sid.as_str(), text.as_str()), ("dsh-sess-1", "do it"));
        reply.send(Ok(StopReasonView::EndTurn)).unwrap();
        assert!(matches!(j.await.unwrap().unwrap(), StopReasonView::EndTurn));

        // cancel 即发即忘语义（fire-and-forget 通知）
        backend.cancel("dsh-sess-1").await.unwrap();
        assert_eq!(driver.cancel_rx.recv().await.unwrap(), "dsh-sess-1");

        // close
        let b = backend.clone();
        let j = tokio::spawn(async move { b.close_session("dsh-sess-1").await });
        let (sid, reply) = driver.close_rx.recv().await.unwrap();
        assert_eq!(sid, "dsh-sess-1");
        reply.send(Ok(())).unwrap();
        j.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn fake_factory_start_failure() {
        let (mut factory, _driver) = fake_backend_with_failures(1);
        assert!(factory.start().await.is_err());
        assert!(factory.start().await.is_ok());
    }

    #[test]
    fn view_translation_covers_known_updates() {
        // 用 SDK 类型构造，验证翻译不 panic 且字段正确
        use agent_client_protocol::schema::v1::{ContentBlock, ContentChunk, SessionUpdate, TextContent};
        let u = SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
            TextContent::new("hello"),
        )));
        match view_of_update(&u) {
            SessionUpdateView::AgentMessageChunk { text, message_id } => {
                assert_eq!(text, "hello");
                assert_eq!(message_id, None);
            }
            other => panic!("wrong view: {other:?}"),
        }
    }
}
```

注：`ContentChunk::new` 签名以 crate 源码为准（`content.rs` 1891 行附近）；若构造函数不同（如必填 message_id），按实际构造器调整测试，语义不变。

- [ ] **Step 2: 运行确认失败**

Run: `cargo test acp`
Expected: 编译失败

- [ ] **Step 3: 实现 acp.rs（本任务只含视图/trait/fake；真实 DshBackend 在 Task 9）**

```rust
use crate::permission::{PermKind, PermResponder, PermissionOptionView};
use agent_client_protocol::schema::v1::{
    RequestPermissionRequest, SessionUpdate, StopReason,
};
use serde_json::json;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use tokio::sync::{mpsc, oneshot};

// ---------- 视图类型（与 SDK 解耦的内部表示） ----------

#[derive(Debug, Clone, PartialEq)]
pub enum SessionUpdateView {
    AgentMessageChunk { text: String, message_id: Option<String> },
    AgentThoughtChunk { text: String },
    ToolCall { tool_call_id: String, title: String, name: Option<String>, status: String },
    ToolCallUpdate { tool_call_id: String, status: Option<String>, summary: Option<String> },
    ConfigUpdate { summary: String },
    Usage { summary: String },
    Other { kind: String },
}

fn block_text(b: &agent_client_protocol::schema::v1::ContentBlock) -> String {
    use agent_client_protocol::schema::v1::ContentBlock as B;
    match b {
        B::Text(t) => t.text.clone(),
        _ => String::new(),
    }
}

pub fn view_of_update(u: &SessionUpdate) -> SessionUpdateView {
    match u {
        SessionUpdate::AgentMessageChunk(c) => SessionUpdateView::AgentMessageChunk {
            text: block_text(&c.content),
            message_id: c.message_id.as_ref().map(|m| m.to_string()),
        },
        SessionUpdate::AgentThoughtChunk(c) => SessionUpdateView::AgentThoughtChunk {
            text: block_text(&c.content),
        },
        SessionUpdate::ToolCall(tc) => SessionUpdateView::ToolCall {
            tool_call_id: tc.tool_call_id.to_string(),
            title: tc.title.clone(),
            name: tc.name.clone(),
            status: format!("{:?}", tc.status),
        },
        SessionUpdate::ToolCallUpdate(tu) => SessionUpdateView::ToolCallUpdate {
            tool_call_id: tu.tool_call_id.to_string(),
            status: tu.fields.status.map(|s| format!("{s:?}")),
            summary: tu
                .fields
                .title
                .clone()
                .or_else(|| tu.fields.content.as_ref().map(|c| format!("{} item(s)", c.len()))),
        },
        SessionUpdate::ConfigOptionUpdate(c) => SessionUpdateView::ConfigUpdate {
            summary: format!("{} option(s)", c.config_options.len()),
        },
        SessionUpdate::UsageUpdate(uu) => SessionUpdateView::Usage {
            summary: serde_json::to_string(uu).unwrap_or_else(|_| "usage".into()),
        },
        other => SessionUpdateView::Other {
            kind: format!("{other:?}"),
        },
    }
}

#[derive(Debug, Clone)]
pub struct PermissionRequestView {
    pub tool_call_id: Option<String>,
    pub options: Vec<PermissionOptionView>,
}

pub fn view_of_permission_request(r: &RequestPermissionRequest) -> PermissionRequestView {
    PermissionRequestView {
        // tool_call 为必填字段（仅携带 toolCallId 时详情经事件缓存补全，规格 §2）
        tool_call_id: Some(r.tool_call.tool_call_id.to_string()),
        options: r
            .options
            .iter()
            .map(|o| PermissionOptionView {
                option_id: o.option_id.to_string(),
                name: o.name.clone(),
                kind: match o.kind {
                    agent_client_protocol::schema::v1::PermissionOptionKind::AllowOnce => PermKind::AllowOnce,
                    agent_client_protocol::schema::v1::PermissionOptionKind::AllowAlways => PermKind::AllowAlways,
                    agent_client_protocol::schema::v1::PermissionOptionKind::RejectOnce => PermKind::RejectOnce,
                    agent_client_protocol::schema::v1::PermissionOptionKind::RejectAlways => PermKind::RejectAlways,
                    _ => PermKind::Other,
                },
            })
            .collect(),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum StopReasonView {
    EndTurn,
    MaxTokens,
    MaxTurnRequests,
    Refusal,
    Cancelled,
    Other(String),
}

impl From<StopReason> for StopReasonView {
    fn from(s: StopReason) -> Self {
        match s {
            StopReason::EndTurn => Self::EndTurn,
            StopReason::MaxTokens => Self::MaxTokens,
            StopReason::MaxTurnRequests => Self::MaxTurnRequests,
            StopReason::Refusal => Self::Refusal,
            StopReason::Cancelled => Self::Cancelled,
            other => Self::Other(format!("{other:?}")),
        }
    }
}

// ---------- 后端抽象 ----------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendErrorKind {
    Timeout,
    Protocol,
    ProcessDied,
    Unavailable,
    Rejected,
}

#[derive(Debug, Clone)]
pub struct BackendError {
    pub kind: BackendErrorKind,
    pub message: String,
}

impl BackendError {
    pub fn new(kind: BackendErrorKind, message: impl Into<String>) -> Self {
        Self { kind, message: message.into() }
    }
}

impl std::fmt::Display for BackendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}
impl std::error::Error for BackendError {}

pub enum BackendEvent {
    Update { epoch: u64, dsh_session_id: String, update: SessionUpdateView },
    Permission { epoch: u64, dsh_session_id: String, request: PermissionRequestView, responder: PermResponder },
    Died { epoch: u64, reason: String },
}

#[allow(async_fn_in_trait)]
pub trait Backend: Clone + Send + 'static {
    async fn new_session(&self, cwd: &Path) -> Result<String, BackendError>;
    async fn prompt(&self, dsh_session_id: &str, text: String) -> Result<StopReasonView, BackendError>;
    async fn cancel(&self, dsh_session_id: &str) -> Result<(), BackendError>;
    async fn close_session(&self, dsh_session_id: &str) -> Result<(), BackendError>;
}

#[allow(async_fn_in_trait)]
pub trait BackendFactory: Send + 'static {
    type B: Backend;
    /// spawn + initialize；返回 (句柄, 事件流, child_pid)
    async fn start(&mut self) -> Result<(Self::B, mpsc::UnboundedReceiver<BackendEvent>, u32), BackendError>;
}

// ---------- 测试假后端 ----------

type NewSessionCall = (PathBuf, oneshot::Sender<Result<String, BackendError>>);
type PromptCall = (String, String, oneshot::Sender<Result<StopReasonView, BackendError>>);
type CloseCall = (String, oneshot::Sender<Result<(), BackendError>>);

#[derive(Clone)]
pub struct FakeBackend {
    new_session_tx: mpsc::UnboundedSender<NewSessionCall>,
    prompt_tx: mpsc::UnboundedSender<PromptCall>,
    cancel_tx: mpsc::UnboundedSender<String>,
    close_tx: mpsc::UnboundedSender<CloseCall>,
}

impl Backend for FakeBackend {
    async fn new_session(&self, cwd: &Path) -> Result<String, BackendError> {
        let (tx, rx) = oneshot::channel();
        self.new_session_tx
            .send((cwd.to_path_buf(), tx))
            .map_err(|_| BackendError::new(BackendErrorKind::ProcessDied, "fake closed"))?;
        rx.await.map_err(|_| BackendError::new(BackendErrorKind::ProcessDied, "fake dropped"))?
    }

    async fn prompt(&self, dsh_session_id: &str, text: String) -> Result<StopReasonView, BackendError> {
        let (tx, rx) = oneshot::channel();
        self.prompt_tx
            .send((dsh_session_id.to_string(), text, tx))
            .map_err(|_| BackendError::new(BackendErrorKind::ProcessDied, "fake closed"))?;
        rx.await.map_err(|_| BackendError::new(BackendErrorKind::ProcessDied, "fake dropped"))?
    }

    async fn cancel(&self, dsh_session_id: &str) -> Result<(), BackendError> {
        self.cancel_tx
            .send(dsh_session_id.to_string())
            .map_err(|_| BackendError::new(BackendErrorKind::ProcessDied, "fake closed"))
    }

    async fn close_session(&self, dsh_session_id: &str) -> Result<(), BackendError> {
        let (tx, rx) = oneshot::channel();
        self.close_tx
            .send((dsh_session_id.to_string(), tx))
            .map_err(|_| BackendError::new(BackendErrorKind::ProcessDied, "fake closed"))?;
        rx.await.map_err(|_| BackendError::new(BackendErrorKind::ProcessDied, "fake dropped"))?
    }
}

pub struct FakeFactory {
    backend: FakeBackend,
    event_rx: Option<mpsc::UnboundedReceiver<BackendEvent>>,
    fail_starts: VecDeque<BackendError>,
}

impl BackendFactory for FakeFactory {
    type B = FakeBackend;
    async fn start(&mut self) -> Result<(FakeBackend, mpsc::UnboundedReceiver<BackendEvent>, u32), BackendError> {
        if let Some(e) = self.fail_starts.pop_front() {
            return Err(e);
        }
        Ok((
            self.backend.clone(),
            self.event_rx.take().expect("fake factory start called twice"),
            0,
        ))
    }
}

pub struct FakeDriver {
    pub new_session_rx: mpsc::UnboundedReceiver<NewSessionCall>,
    pub prompt_rx: mpsc::UnboundedReceiver<PromptCall>,
    pub cancel_rx: mpsc::UnboundedReceiver<String>,
    pub close_rx: mpsc::UnboundedReceiver<CloseCall>,
    pub event_tx: mpsc::UnboundedSender<BackendEvent>,
}

pub fn fake_backend() -> (FakeFactory, FakeDriver) {
    fake_backend_with_failures(0)
}

pub fn fake_backend_with_failures(n: usize) -> (FakeFactory, FakeDriver) {
    let (new_session_tx, new_session_rx) = mpsc::unbounded_channel();
    let (prompt_tx, prompt_rx) = mpsc::unbounded_channel();
    let (cancel_tx, cancel_rx) = mpsc::unbounded_channel();
    let (close_tx, close_rx) = mpsc::unbounded_channel();
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let backend = FakeBackend { new_session_tx, prompt_tx, cancel_tx, close_tx };
    let factory = FakeFactory {
        backend,
        event_rx: Some(event_rx),
        fail_starts: (0..n)
            .map(|_| BackendError::new(BackendErrorKind::ProcessDied, "injected start failure"))
            .collect(),
    };
    (
        factory,
        FakeDriver { new_session_rx, prompt_rx, cancel_rx, close_rx, event_tx },
    )
}
```

视图翻译中引用的 SDK 字段（`r.tool_call`、`ConfigOptionUpdate.config_options`、`ToolCallUpdateFields.{status,title,content}`）若与 crate 实际字段不符，以 `agent-client-protocol-schema-1.12.0/src/v1/{client.rs,tool_call.rs}` 为准调整；语义保持"工具详情用于摘要、未知更新归 Other"。`json!` 若暂未用到可删。

- [ ] **Step 4: 运行确认通过**

Run: `cargo test acp`
Expected: 3 个测试 PASS

- [ ] **Step 5: Commit**

```bash
git add src/acp.rs src/main.rs
git commit -m "feat: ACP view types, backend trait, and fake backend for tests"
```

---

### Task 7: Coordinator 核心（actor、准入、去重、权限流、额度）

**Files:**
- Create: `src/coordinator.rs`
- Modify: `src/main.rs`（加 `mod coordinator;`）

**Interfaces:**
- Consumes: `task::*`、`permission::*`、`events::*`、`acp::{Backend, BackendFactory, BackendEvent, StopReasonView, SessionUpdateView, BackendError, BackendErrorKind}`、`config::{Config, limits}`、`runtime_guard::RuntimeGuard`
- Produces:
  - `pub trait Clock: Clone + Send + Sync + 'static { fn now(&self) -> Instant; fn sleep(&self, dur: Duration) -> Pin<Box<dyn Future<Output=()> + Send>>; }`；`pub struct TokioClock;`、`pub struct ManualClock`（`fn advance(&self, dur: Duration)`）
  - `pub struct StartTaskOut { pub task_id: TaskId, pub session_id: BridgeSessionId, pub status: TaskStatus, pub deduplicated: bool }`（Serialize）
  - `pub struct TaskView { .. }`（字段严格按规格 §5.2；Serialize）
  - `pub struct CancelOut { pub task_id: TaskId, pub status: TaskStatus, pub execution_state: ExecutionState }`
  - `pub struct ResolveOut { pub permission_id: PermissionId, pub decision: String, pub delivery: String, pub task_status: TaskStatus }`
  - `pub struct CloseSessionOut { pub session_id: BridgeSessionId, pub state: SessionState }`
  - `pub struct CoordinatorHandle<B: Backend> { tx: mpsc::UnboundedSender<Command<B>> }`（Clone）：`start_task / follow_up / get_task / cancel_task / resolve_permission / close_session / shutdown`，均为 async 薄封装（oneshot reply）
  - `pub trait CoordApi: Send + Sync`——同签名方法返回 `Pin<Box<dyn Future<Output=...> + Send + '_>>`（显式盒化使 trait 可 dyn）；`impl<B: Backend> CoordApi for CoordinatorHandle<B>` 转发
  - `#[derive(Clone)] pub struct CoordHandle(Arc<dyn CoordApi>)`——类型擦除句柄，提供同签名 async 便捷方法；server.rs 与测试统一使用它（避免 rmcp 宏作用于泛型 impl 的风险）
  - `pub fn spawn_coordinator<B: Backend, F: BackendFactory<B = B>, C: Clock>(config: Config, factory: F, clock: C, guard: Arc<RuntimeGuard>, recovery_required: bool) -> CoordHandle`

**Actor 内部事件（本模块私有）：**

```rust
enum InternalEvent<B: Backend> {
    BackendStarted { epoch: u64, result: Result<(B, mpsc::UnboundedReceiver<BackendEvent>, u32), BackendError> },
    SessionCreated { task_id: TaskId, epoch: u64, result: Result<String, BackendError> },
    PromptSettled { task_id: TaskId, epoch: u64, result: Result<StopReasonView, BackendError> },
    CloseDone { session_id: BridgeSessionId, epoch: u64, result: Result<(), BackendError> },
    CancelTimeout { task_id: TaskId, epoch: u64 },
    CloseTimeout { task_id: TaskId, epoch: u64 },
    PermissionTimeout { permission_id: PermissionId, epoch: u64 },
    IdleTtl { session_id: BridgeSessionId, epoch: u64 },
    RetentionSweep,
    ShutdownDeadline,
    Backend(BackendEvent),
}
```

**核心结构：**

```rust
pub enum Command<B: Backend> {
    StartTask { request_id: String, cwd: PathBuf, prompt: String, reply: oneshot::Sender<Result<StartTaskOut, TaskError>> },
    FollowUp { request_id: String, task_id: TaskId, prompt: String, reply: oneshot::Sender<Result<StartTaskOut, TaskError>> },
    GetTask { task_id: TaskId, cursor: u64, limit: usize, reply: oneshot::Sender<Result<TaskView, TaskError>> },
    CancelTask { task_id: TaskId, reply: oneshot::Sender<Result<CancelOut, TaskError>> },
    ResolvePermission { task_id: TaskId, permission_id: PermissionId, option_id: String, reply: oneshot::Sender<Result<ResolveOut, TaskError>> },
    CloseSession { task_id: TaskId, reply: oneshot::Sender<Result<CloseSessionOut, TaskError>> },
    Internal(InternalEvent<B>),
    Shutdown,
}

struct Coord<B: Backend, C: Clock> {
    config: Config,
    clock: C,
    factory: Option<F>,
    backend: Option<B>,
    backend_state: BackendState,
    backend_ever_used: bool,      // §8.1：已使用过的后端崩溃不自动重启
    epoch: u64,
    child_pid: Option<u32>,
    sessions: HashMap<BridgeSessionId, Session>,
    tasks: HashMap<TaskId, Task>,
    by_request: HashMap<String, (TaskId, RequestFingerprint)>,
    permissions: PermissionManager,
    tool_summaries: VecDeque<(String, String)>, // tool_call_id → 摘要，cap TOOL_SUMMARY_CACHE
    pending_start: VecDeque<TaskId>,            // 等待后端 ready 的任务
    self_tx: mpsc::UnboundedSender<Command<B>>,
    guard: Arc<RuntimeGuard>,
    draining: bool,
    shutdown_reply: Vec<oneshot::Sender<()>>,
}

#[derive(PartialEq)]
struct RequestFingerprint {
    tool: &'static str, // "start_task" | "send_followup"
    cwd: Option<String>,          // 原始 cwd（未规范化）
    parent_task_id: Option<TaskId>,
    prompt: String,
}
```

**关键行为（actor 单写入者；所有后端调用 spawn 驱动任务，结果经 InternalEvent 回流；定时器为 `clock.sleep` + self_tx 发送，携带 epoch 与期望状态，过期即忽略）：**

1. `start_task`：去重（同 request_id 同指纹 → 返回既有任务+deduplicated=true；同 ID 异指纹 → RequestConflict）→ 校验（prompt 非空 ≤64KiB、request_id ≤128B、cwd 绝对存在目录，`fs::canonicalize` 存规范化值，比较指纹用原始串）→ 后端门禁（Unavailable/Draining → BackendUnavailable；guard 恢复态 → RecoveryRequired）→ 额度（active ≥ max → CapacityExceeded；open sessions ≥ max → CapacityExceeded）→ 建 Session(Opening)+Task(Starting/AwaitBackend)，立即 reply → `drive(task_id)`。
2. `drive`：backend Ready → spawn new_session；Dormant → state=Starting，spawn factory.start()（START_TIMEOUT 包裹），任务入 pending_start；Starting → 任务入 pending_start。
3. `BackendStarted Ok`：state=Ready，存 backend，spawn 事件转发任务（BackendEvent → Command::Internal(Backend(..))），`guard.mark_child_pid(pid)`（失败 → 拒绝新执行并走隔离，§8.2"标记更新失败时拒绝开始新的执行"）；drain pending_start → 逐个 spawn new_session。`Err`：若 `!backend_ever_used`（首次启动即失败且未发 session/new）→ `guard.clear()`，state=Dormant，受影响 starting 任务 Failed+NotStarted（错误透传），允许下次重新启动；否则 → 隔离路径（Task 8）。
4. `SessionCreated Ok`：session.dsh_session_id 就位；若任务已被请求取消 → spawn close_session，结算 Cancelled(NotStarted)（§5.4）；否则 spawn prompt（task.phase=PromptSent，status=Running，execution=InProgress，started_at，session=Busy/active_task）。
5. `PromptSettled Ok(stop)`：EndTurn→Completed；Cancelled→Cancelled(Stopped)；MaxTokens/MaxTurnRequests/Refusal/Other→Failed(保留 stop_reason 与部分输出，Stopped)。竞争规则：取消意图不覆盖已完成（cancelling 中收到 EndTurn 仍 Completed）；cancelling 中收到 Cancelled → Cancelled(Stopped)。结算后 session=Idle、active_task=None、记录 last_activity、调度 IdleTtl。
6. `PromptSettled Err`：kind=Rejected/Protocol（远端明确错误响应）→ Failed(Stopped)；kind=Timeout/ProcessDied/Unavailable → 按无停止证据处理：Failed+Unknown+`execution_state_unknown`，并触发隔离（Task 8）。
7. `follow_up`：去重同构（指纹 tool="send_followup"）→ 目标任务必须存在、是该会话 latest_task、已终态、execution_state≠Unknown（否则 StaleTask 带 latest_task_id / SessionBusy / InvalidArgument）→ 会话必须 Idle（Busy/Closing/Closed/Lost → 对应错误码）→ 后端 Ready（否则 BackendUnavailable）→ 额度 → 建 Task(Running 前置 Starting+AwaitSession 中的相位直接 PromptSent 路径：复用已有会话，spawn prompt)。
8. `resolve_permission`：manager.decide（task_id 不匹配 → PermissionNotFound）→ 首次裁决有 responder：构造 `RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new(option_id))` 回复，delivery=sent/failed（failed → 隔离路径）；重复同选项 → delivery="sent"（幂等回显已有裁决，decision 不变）。pending 归零且任务 WaitingPermission → Running。事件 PermissionResolved。
9. `BackendEvent::Permission`：epoch 不符/会话未知/无活跃任务/会话非 Busy → responder 回 Cancelled。否则 register：PayloadTooLarge → 回 Cancelled + 任务取消流程（错误 permission_payload_too_large）；LimitExceeded → 回 Cancelled + 任务取消（permission_limit_exceeded）；Ok → 任务 Running→WaitingPermission（不覆盖 Cancelling/终态）、事件 PermissionRequested、调度 PermissionTimeout。tool_summary 从 tool_summaries 按 tool_call_id 查，缺失 → details_incomplete=true。
10. `PermissionTimeout`：manager.expire_one → 按 Decision 回复（AutoDenied→Selected(reject)；Cancelled→Cancelled）；事件 PermissionAutoDenied；pending 归零且 WaitingPermission → Running（§10：权限超时不直接失败任务）。
11. `BackendEvent::Update`：路由（dsh_session_id+epoch → session → active_task）；AgentMessageChunk → result_agg.push_chunk(text, message_id) + 事件 AgentMessage（无 message_id 且此前见过 id → 追加 CompatNote 事件记录降级，§9.2）；ToolCall → 事件 + tool_summaries 合并缓存；其余 → 对应事件类型。
12. `get_task`：纯快照。cursor > last_cursor → InvalidArgument。TaskView 预算装配：meta（≤16KiB）→ 权限快照（≤64KiB）→ result（≤64KiB，超出再截断置 truncated）→ 事件按序填充至总 256KiB，装不下即停并据实设 has_more/next_cursor。`followup_available = session.state==Idle && backend Ready && !draining`；`unavailable_reason` 否则给原因。
13. IdleTtl：仍 Idle 且 last_activity 未变 → 走关闭流程（同 close_session 内部路径）。
14. RetentionSweep（每 60s 自调度）：淘汰 `finished_instant` 距 `clock.now()` 超过 task_retention 的终态任务（单调时钟，测试可虚拟推进）；固定保留 opening/idle/busy/closing 会话的 latest_task；总数超 max_task_records 时按完成时间从老到新淘汰非固定记录；by_request 随任务清除。准入时若无可淘汰且已满 → CapacityExceeded（§6.2）。
15. `SessionView`/`BackendView` 渲染：backend.recovery_required =（构造参数 recovery_required）||（backend_state==Unavailable）。
16. 恢复保护纪律：recovery_required 模式下 shutdown 完成时**不调** `guard.clear()`（标记留待人工 acknowledge）；正常模式且全部会话确认关闭、子进程已 terminate 才 clear。

- [ ] **Step 1: 写失败测试（测试工具 + 规格 §13.1 第 1–5、8–10 行场景）**

`src/coordinator.rs` 测试模块：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::acp::{BackendEvent, StopReasonView, fake_backend, fake_backend_with_failures};
    use crate::permission::{PermKind, PermissionOptionView};
    use crate::task::*;
    use serde_json::json;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;
    use uuid::Uuid;

    fn test_config() -> Config {
        Config {
            dsh_command: "dsh".into(),
            dsh_args: vec![],
            state_dir: PathBuf::from("/tmp/unused"),
            max_active_tasks: 4,
            max_open_sessions: 16,
            session_idle_ttl: Duration::from_secs(1800),
            max_task_records: 256,
            task_retention: Duration::from_secs(86400),
            start_timeout: Duration::from_secs(30),
            permission_timeout: Duration::from_secs(600),
            cancel_timeout: Duration::from_secs(30),
            close_timeout: Duration::from_secs(10),
            shutdown_timeout: Duration::from_secs(50),
        }
    }

    struct Harness {
        handle: CoordHandle,
        driver: crate::acp::FakeDriver,
        clock: ManualClock,
        _guard_dir: tempfile::TempDir,
    }

    fn spawn_harness(config: Config) -> Harness {
        let (factory, driver) = fake_backend();
        spawn_with_factory(config, factory, driver)
    }

    fn spawn_with_factory<F>(config: Config, factory: F, driver: crate::acp::FakeDriver) -> Harness
    where
        F: crate::acp::BackendFactory<B = crate::acp::FakeBackend>,
    {
        let clock = ManualClock::new();
        let dir = tempfile::tempdir().unwrap();
        let guard = match crate::runtime_guard::RuntimeGuard::acquire(dir.path()).unwrap() {
            crate::runtime_guard::AcquireOutcome::Clean(g) => Arc::new(g),
            crate::runtime_guard::AcquireOutcome::Recovery(_) => panic!("fresh tempdir must be clean"),
        };
        let handle = spawn_coordinator(config, factory, clock.clone(), guard, false);
        Harness { handle, driver, clock, _guard_dir: dir }
    }

    fn cwd() -> PathBuf {
        std::env::temp_dir()
    }

    /// 轮询任务直到谓词满足（异步驱动任务传播需要多个调度点，单次 yield 不可靠）。
    async fn wait_until(h: &Harness, task_id: TaskId, pred: impl Fn(&TaskView) -> bool) -> TaskView {
        for _ in 0..500 {
            let v = h.handle.get_task(task_id, 0, 50).await.unwrap();
            if pred(&v) {
                return v;
            }
            tokio::task::yield_now().await;
        }
        panic!("condition not met in time");
    }

    type PromptReply = oneshot::Sender<Result<StopReasonView, crate::acp::BackendError>>;

    /// 跑通 starting→running：回 new_session，收到 prompt 调用；返回任务与 prompt 结算句柄。
    async fn run_to_running(h: &Harness, request_id: &str) -> (StartTaskOut, PromptReply) {
        let out = h
            .handle
            .start_task(request_id.into(), cwd(), "do something".into())
            .await
            .unwrap();
        let (_cwd, reply) = h.driver.new_session_rx.recv().await.unwrap();
        reply.send(Ok("dsh-1".into())).unwrap();
        let (_sid, _text, prompt_reply) = h.driver.prompt_rx.recv().await.unwrap();
        (out, prompt_reply)
    }

    #[tokio::test]
    async fn start_task_returns_immediately_and_reaches_running() {
        let h = spawn_harness(test_config());
        let out = h
            .handle
            .start_task("r1".into(), cwd(), "do it".into())
            .await
            .unwrap();
        assert_eq!(out.status, TaskStatus::Starting);
        assert!(!out.deduplicated);
        let (_c, reply) = h.driver.new_session_rx.recv().await.unwrap();
        reply.send(Ok("dsh-1".into())).unwrap();
        let (_s, _t, prompt_reply) = h.driver.prompt_rx.recv().await.unwrap();
        let v = wait_until(&h, out.task_id, |v| v.status == TaskStatus::Running).await;
        assert_eq!(v.execution_state, ExecutionState::InProgress);
        prompt_reply.send(Ok(StopReasonView::EndTurn)).unwrap();
        let v = wait_until(&h, out.task_id, |v| v.status == TaskStatus::Completed).await;
        assert_eq!(v.execution_state, ExecutionState::Stopped);
    }

    #[tokio::test]
    async fn duplicate_request_id_dedups_and_conflicts() {
        let h = spawn_harness(test_config());
        let a = h.handle.start_task("r1".into(), cwd(), "x".into()).await.unwrap();
        let b = h.handle.start_task("r1".into(), cwd(), "x".into()).await.unwrap();
        assert_eq!(a.task_id, b.task_id);
        assert!(b.deduplicated);
        let err = h.handle.start_task("r1".into(), cwd(), "different".into()).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::RequestConflict);
        // 只发生一次 session/new
        let (_c, reply) = h.driver.new_session_rx.recv().await.unwrap();
        assert!(h.driver.new_session_rx.try_recv().is_err());
        drop(reply);
    }

    #[tokio::test]
    async fn followup_reuses_session_and_serializes() {
        let h = spawn_harness(test_config());
        let (out, p1) = run_to_running(&h, "r1").await;
        // 会话忙时追问 → session_busy
        let err = h.handle.follow_up("r2".into(), out.task_id, "revise".into()).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::SessionBusy);
        // 完成首任务
        p1.send(Ok(StopReasonView::EndTurn)).unwrap();
        wait_until(&h, out.task_id, |v| v.status == TaskStatus::Completed).await;
        // 追问：复用会话（无新 session/new），新 task_id，parent 指向
        let f = h.handle.follow_up("r2".into(), out.task_id, "revise".into()).await.unwrap();
        assert_ne!(f.task_id, out.task_id);
        assert_eq!(f.session_id, out.session_id);
        assert!(h.driver.new_session_rx.try_recv().is_err());
        let (sid, text, _p2) = h.driver.prompt_rx.recv().await.unwrap();
        assert_eq!(sid, "dsh-1");
        assert_eq!(text, "revise");
        let v = h.handle.get_task(f.task_id, 0, 50).await.unwrap();
        assert_eq!(v.parent_task_id, Some(out.task_id));
    }

    #[tokio::test]
    async fn followup_on_stale_task_rejected() {
        let h = spawn_harness(test_config());
        let (t1, p1) = run_to_running(&h, "r1").await;
        p1.send(Ok(StopReasonView::EndTurn)).unwrap();
        wait_until(&h, t1.task_id, |v| v.status == TaskStatus::Completed).await;
        let t2 = h.handle.follow_up("r2".into(), t1.task_id, "x".into()).await.unwrap();
        let (_s, _t, p2) = h.driver.prompt_rx.recv().await.unwrap();
        p2.send(Ok(StopReasonView::EndTurn)).unwrap();
        wait_until(&h, t2.task_id, |v| v.status == TaskStatus::Completed).await;
        // 对旧祖先追问 → stale_task 并带 latest_task_id
        let err = h.handle.follow_up("r3".into(), t1.task_id, "x".into()).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::StaleTask);
        assert_eq!(
            err.details.unwrap()["latest_task_id"],
            serde_json::Value::String(t2.task_id.to_string())
        );
    }

    #[tokio::test]
    async fn capacity_exceeded_is_immediate() {
        let mut cfg = test_config();
        cfg.max_active_tasks = 1;
        let h = spawn_harness(cfg);
        let (_t1, _p1) = run_to_running(&h, "r1").await;
        let err = h.handle.start_task("r2".into(), cwd(), "x".into()).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::CapacityExceeded);
    }

    #[tokio::test]
    async fn permission_flow_full_cycle() {
        let h = spawn_harness(test_config());
        let (out, _p1) = run_to_running(&h, "r1").await;
        // dsh 发来权限请求
        let (resp_tx, resp_rx) = oneshot::channel::<agent_client_protocol::schema::v1::RequestPermissionResponse>();
        h.driver
            .event_tx
            .send(BackendEvent::Permission {
                epoch: 1,
                dsh_session_id: "dsh-1".into(),
                request: crate::acp::PermissionRequestView {
                    tool_call_id: Some("tc-9".into()),
                    options: vec![
                        PermissionOptionView { option_id: "allow".into(), name: "Allow".into(), kind: PermKind::AllowOnce },
                        PermissionOptionView { option_id: "reject".into(), name: "Reject".into(), kind: PermKind::RejectOnce },
                    ],
                },
                responder: crate::permission::PermResponder::Test(Box::new(move |r| {
                    let _ = resp_tx.send(r);
                })),
            })
            .unwrap();
        let v = wait_until(&h, out.task_id, |v| v.status == TaskStatus::WaitingPermission).await;
        assert_eq!(v.pending_permissions.len(), 1);
        let pid = v.pending_permissions[0].permission_id;
        // 裁决
        let r = h.handle.resolve_permission(out.task_id, pid, "allow".into()).await.unwrap();
        assert_eq!(r.delivery, "sent");
        assert_eq!(r.task_status, TaskStatus::Running);
        let outcome = resp_rx.await.unwrap().outcome;
        assert!(matches!(
            outcome,
            agent_client_protocol::schema::v1::RequestPermissionOutcome::Selected(_)
        ));
        // 重复同选项幂等；改选报错
        let r2 = h.handle.resolve_permission(out.task_id, pid, "allow".into()).await.unwrap();
        assert_eq!(r2.decision, "selected");
        let err = h.handle.resolve_permission(out.task_id, pid, "reject".into()).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::PermissionAlreadyResolved);
    }

    #[tokio::test]
    async fn permission_timeout_auto_denies() {
        let h = spawn_harness(test_config());
        let (out, _p1) = run_to_running(&h, "r1").await;
        let (resp_tx, resp_rx) = oneshot::channel::<agent_client_protocol::schema::v1::RequestPermissionResponse>();
        h.driver
            .event_tx
            .send(BackendEvent::Permission {
                epoch: 1,
                dsh_session_id: "dsh-1".into(),
                request: crate::acp::PermissionRequestView {
                    tool_call_id: None,
                    options: vec![PermissionOptionView {
                        option_id: "reject".into(),
                        name: "Reject once".into(),
                        kind: PermKind::RejectOnce,
                    }],
                },
                responder: crate::permission::PermResponder::Test(Box::new(move |r| {
                    let _ = resp_tx.send(r);
                })),
            })
            .unwrap();
        wait_until(&h, out.task_id, |v| v.status == TaskStatus::WaitingPermission).await;
        // 推进虚拟时钟超过 permission_timeout
        h.clock.advance(Duration::from_secs(601));
        let resp = resp_rx.await.unwrap();
        assert!(matches!(
            resp.outcome,
            agent_client_protocol::schema::v1::RequestPermissionOutcome::Selected(_)
        ));
        let v = wait_until(&h, out.task_id, |v| v.status == TaskStatus::Running).await;
        assert!(v.pending_permissions.is_empty()); // 超时不直接失败任务
    }

    #[tokio::test]
    async fn events_pagination_and_gap() {
        let h = spawn_harness(test_config());
        let (out, p1) = run_to_running(&h, "r1").await;
        for i in 0..5 {
            h.driver
                .event_tx
                .send(BackendEvent::Update {
                    epoch: 1,
                    dsh_session_id: "dsh-1".into(),
                    update: crate::acp::SessionUpdateView::AgentMessageChunk {
                        text: format!("chunk-{i}"),
                        message_id: Some("m1".into()),
                    },
                })
                .unwrap();
        }
        // 等最后一个 chunk 进缓冲（last_cursor 持续增长）
        wait_until(&h, out.task_id, |v| v.last_cursor >= 5).await;
        let v1 = h.handle.get_task(out.task_id, 0, 2).await.unwrap();
        assert_eq!(v1.events.len(), 2);
        assert!(v1.has_more);
        let v2 = h.handle.get_task(out.task_id, v1.next_cursor, 100).await.unwrap();
        assert!(!v2.gap);
        assert_eq!(v2.last_cursor, v2.next_cursor);
        assert!(!v2.has_more);
        // 结果聚合独立于事件分页
        p1.send(Ok(StopReasonView::EndTurn)).unwrap();
        let v3 = wait_until(&h, out.task_id, |v| v.status == TaskStatus::Completed).await;
        assert!(v3.result.as_ref().unwrap().text.contains("chunk-4"));
        assert_eq!(v3.result.as_ref().unwrap().source, crate::events::ResultSource::LastAssistantMessage);
    }

    #[tokio::test]
    async fn idle_ttl_closes_session() {
        let mut cfg = test_config();
        cfg.session_idle_ttl = Duration::from_secs(60);
        let h = spawn_harness(cfg);
        let (out, p1) = run_to_running(&h, "r1").await;
        p1.send(Ok(StopReasonView::EndTurn)).unwrap();
        wait_until(&h, out.task_id, |v| v.status == TaskStatus::Completed).await;
        h.clock.advance(Duration::from_secs(61));
        let (sid, reply) = h.driver.close_rx.recv().await.unwrap();
        assert_eq!(sid, "dsh-1");
        reply.send(Ok(())).unwrap();
        let v = wait_until(&h, out.task_id, |v| v.session.state == SessionState::Closed).await;
        assert!(!v.session.followup_available);
        // 关闭后追问
        let err = h.handle.follow_up("r2".into(), out.task_id, "x".into()).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::SessionClosed);
    }

    #[tokio::test]
    async fn first_start_failure_returns_to_dormant_and_allows_retry() {
        let (factory, driver) = fake_backend_with_failures(1);
        let h = spawn_with_factory(test_config(), factory, driver);
        let out = h.handle.start_task("r1".into(), cwd(), "x".into()).await.unwrap();
        let v = wait_until(&h, out.task_id, |v| v.status == TaskStatus::Failed).await;
        assert_eq!(v.execution_state, ExecutionState::NotStarted);
        // 不进入恢复保护：下一次派发允许重新启动
        let out2 = h.handle.start_task("r2".into(), cwd(), "y".into()).await.unwrap();
        let (_c, reply) = h.driver.new_session_rx.recv().await.unwrap();
        reply.send(Ok("dsh-2".into())).unwrap();
        let (_s, _t, _p) = h.driver.prompt_rx.recv().await.unwrap();
        wait_until(&h, out2.task_id, |v| v.status == TaskStatus::Running).await;
    }
}
```

注：测试假定 `BackendEvent::Permission` 的 epoch 为 1——actor 初始 epoch=1，每次后端重启 +1。Harness 中 RuntimeGuard 用临时目录（从未写 marker → acquire 得 Clean）。`oneshot` 需 `use tokio::sync::oneshot;`（文件顶部 super::* 已含则省略）。

- [ ] **Step 2: 运行确认失败**

Run: `cargo test coordinator`
Expected: 编译失败

- [ ] **Step 3: 实现 coordinator.rs**

按本节"核心结构"与"关键行为 1–15"实现。要点：

- `spawn_coordinator` 创建 unbounded 命令通道，spawn actor 任务（`run()` 内 `while let Some(cmd) = rx.recv().await` 分发到 `on_command`/`on_internal`）。
- 所有 spawn 驱动任务的模式：

```rust
fn spawn_new_session(&self, task_id: TaskId) {
    let Some(backend) = self.backend.clone() else { return };
    // 任务不存 cwd；经 task.session_id 取会话的规范化 cwd（规格 §5.1：创建会话前再次检查目录）
    let cwd = self.sessions[&self.tasks[&task_id].session_id].cwd.clone();
    let epoch = self.epoch;
    let tx = self.self_tx.clone();
    let clock = self.clock.clone();
    let budget = self.config.start_timeout;
    tokio::spawn(async move {
        let result = tokio::select! {
            r = backend.new_session(&cwd) => r,
            _ = clock.sleep(budget) => Err(BackendError::new(BackendErrorKind::Timeout, "session/new timed out")),
        };
        let _ = tx.send(Command::Internal(InternalEvent::SessionCreated { task_id, epoch, result }));
    });
}
```

- prompt 无超时（规格 §11.1），但任务取消期间 cancel 通知正常发。
- spawn new_session 前再次 `std::fs::metadata(&cwd)` 检查目录仍存在（规格 §5.1）；消失则任务 Failed+NotStarted、撤销会话预留。
- `ManualClock`：内部 `Mutex<(Instant, Vec<(Instant, oneshot::Sender<()>)>)>`；`advance` 推进并放出到期 sleeper；`sleep` 注册 sleeper。测试中事件传播用 `wait_until` 轮询而非固定次数 yield。
- `CoordApi`/`CoordHandle`：trait 方法返回 `Pin<Box<dyn Future<Output = _> + Send + '_>>`；`CoordinatorHandle<B>` 的 impl 逐项 `Box::pin(async move { ... })` 转发；`CoordHandle(Arc<dyn CoordApi>)` 的便捷方法直接 `self.0.method(..).await`。`spawn_coordinator` 返回 `CoordHandle`。
- TaskView 预算装配实现为 `fn render_view(task, session, backend_state, page, perms, config) -> TaskView`：先序列化固定字段，再按预算逐节追加；用 `serde_json::to_string` 估算字节（含转义）。路由键（dsh_session_id、tool_call_id、option_id、task/session/permission uuid）永不截断——超长入站标识按协议错误处理（规格 §9.1 末）。
- `get_task` 的 `session.dsh_session_id` 与 `latest_task_id`、`followup_available` 从会话记录渲染；会话记录保留至最后一条关联任务被淘汰（规格 §6.2），因此 get_task 总能取到会话视图。
- `Epoch` 语义：初始 1；每次后端状态离开 Ready（故障/隔离）时 +1；所有回调与定时器事件携带发出时的 epoch，处理前比对当前 epoch，不符即忽略（规格 §4.2-3）。
- `recovery_required`：构造参数存入 actor；为 true 时 start/followup 直接 `RecoveryRequired`，`BackendView.recovery_required` 恒 true，不启动后端。

- [ ] **Step 4: 运行确认通过**

Run: `cargo test coordinator`
Expected: 全部测试 PASS

- [ ] **Step 5: Commit**

```bash
git add src/coordinator.rs src/main.rs
git commit -m "feat: coordinator actor with admission, dedup, permissions, quotas"
```

---

### Task 8: 取消阶梯、故障隔离与关停（coordinator.rs 补全）

**Files:**
- Modify: `src/coordinator.rs`

**Interfaces:**
- Consumes: Task 7 全部
- Produces:
  - `CoordinatorHandle::shutdown(&self) -> oneshot 完成通知`（main 使用）
  - `Backend` trait 增补：`async fn terminate_process(&self)`（DshBackend 实现 TERM/KILL 阶梯；FakeBackend 记录调用次数：`pub terminate_count: Arc<Mutex<usize>>`，经 `FakeFactory` 透出）
  - `TaskView.backend.recovery_required` 在隔离后为 true

行为（规格 §7、§8，编号对应）：

1. `cancel_task`：未知 → TaskNotFound；终态 → 返回当前视图不改状态；已 Cancelling → 幂等返回（不新建计时器）。否则：cancel_requested_at、status=Cancelling、撤销全部待决权限（回复 Cancelled、停权限计时器）、事件 StatusChange。
2. 按相位走阶梯：`AwaitBackend/AwaitSession`（prompt 未发）→ 置取消标记；`SessionCreated` 到达时若已标记 → spawn close_session，`CloseDone Ok` → 结算 Cancelled(NotStarted)，session=Closed；`PromptSent` → spawn `backend.cancel(dsh_sid)`（通知即忘），arm `CancelTimeout{cancel_timeout}`。
3. `CancelTimeout`（任务仍 Cancelling）→ session=Closing，spawn `close_session`，arm `CloseTimeout{close_timeout}`。
4. `CloseDone Ok`（取消路径）→ Cancelled(Stopped)，session=Closed，释放会话额度。
5. `CloseTimeout` / `CloseDone Err` / `SessionCreated Err(Timeout)` / `PromptSettled Err(Timeout|ProcessDied|Unavailable)` / `BackendEvent::Died` → **隔离**：`backend_state=Unavailable`、`epoch+=1`、旧 epoch 全部非 Closed 会话 → Lost；其上活跃任务：prompt 未发 → Failed+NotStarted，已发 → Failed+Unknown+`execution_state_unknown`；spawn `terminate_process()`；保留 run-state 标记（不调 clear）；后续 start/followup → BackendUnavailable（TaskView.backend.recovery_required=true）。
6. `SessionCreated Err(Rejected|Protocol)`（明确错误响应）→ 仅该任务 Failed+NotStarted，撤销会话预留、释放额度，会话记录删除（规格 §8.1 第 2 段）。
7. `PromptSettled Err(Rejected|Protocol)` → 仅该任务 Failed+Stopped，会话回 Idle（同上）。
8. 竞争：Cancelling 中 `PromptSettled(EndTurn)` → Completed（§6.1）；Cancelling 中 `PromptSettled(Cancelled)` → Cancelled(Stopped)；所有迟到事件先查任务终态（settle-once 兜底）。
9. `shutdown`：draining=true；待决权限全部回 Cancelled；活跃任务按 §7 并行取消；Idle 会话直接 close；arm `ShutdownDeadline{shutdown_timeout}`；全部会话 Closed/Lost 且 `terminate_process` 完成 → `guard.clear()` → 唤醒 shutdown 等待者；Deadline 先到 → 保留标记、记录日志，唤醒等待者（main 以非零码退出）。
10. 保留策略补充：会话 Lost/Closed 后不再固定其任务；任务淘汰时清 by_request；会话记录在最后关联任务淘汰后删除。

- [ ] **Step 1: 写失败测试（规格 §13.1 第 5–7、13 行场景）**

在 Task 7 测试模块内追加（复用 Harness/run_to_running/wait_until；`run_to_running` 返回 `(StartTaskOut, PromptReply)`）：

```rust
#[tokio::test]
async fn cancel_before_prompt_settles_not_started() {
    let h = spawn_harness(test_config());
    let out = h.handle.start_task("r1".into(), cwd(), "x".into()).await.unwrap();
    let c = h.handle.cancel_task(out.task_id).await.unwrap();
    assert_eq!(c.status, TaskStatus::Cancelling);
    // session/new 已在飞：等待其结果后应关闭会话
    let (_cwd, reply) = h.driver.new_session_rx.recv().await.unwrap();
    reply.send(Ok("dsh-1".into())).unwrap();
    let (sid, close_reply) = h.driver.close_rx.recv().await.unwrap();
    assert_eq!(sid, "dsh-1");
    close_reply.send(Ok(())).unwrap();
    // 结算后仍不得发送过 prompt
    assert!(h.driver.prompt_rx.try_recv().is_err());
    let v = wait_until(&h, out.task_id, |v| v.status == TaskStatus::Cancelled).await;
    assert_eq!(v.execution_state, ExecutionState::NotStarted);
}

#[tokio::test]
async fn cancel_running_task_via_acp_cancel() {
    let h = spawn_harness(test_config());
    let (out, p1) = run_to_running(&h, "r1").await;
    h.handle.cancel_task(out.task_id).await.unwrap();
    assert_eq!(h.driver.cancel_rx.recv().await.unwrap(), "dsh-1");
    // dsh 以 cancelled 结算 prompt
    p1.send(Ok(StopReasonView::Cancelled)).unwrap();
    let v = wait_until(&h, out.task_id, |v| v.status == TaskStatus::Cancelled).await;
    assert_eq!(v.execution_state, ExecutionState::Stopped);
    // 会话回 idle，可追问
    let f = h.handle.follow_up("r2".into(), out.task_id, "again".into()).await.unwrap();
    assert_eq!(f.session_id, out.session_id);
}

#[tokio::test]
async fn cancel_escalates_to_close_after_timeout() {
    let h = spawn_harness(test_config());
    let (out, _p1) = run_to_running(&h, "r1").await;
    h.handle.cancel_task(out.task_id).await.unwrap();
    let _ = h.driver.cancel_rx.recv().await.unwrap();
    // prompt 不结算，推进取消超时
    h.clock.advance(Duration::from_secs(31));
    let (sid, close_reply) = h.driver.close_rx.recv().await.unwrap();
    assert_eq!(sid, "dsh-1");
    close_reply.send(Ok(())).unwrap();
    let v = wait_until(&h, out.task_id, |v| v.status == TaskStatus::Cancelled).await;
    assert_eq!(v.execution_state, ExecutionState::Stopped);
    assert_eq!(v.session.state, SessionState::Closed);
    let err = h.handle.follow_up("r2".into(), out.task_id, "x".into()).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::SessionClosed);
}

#[tokio::test]
async fn close_timeout_isolates_backend_and_reports_unknown() {
    let h = spawn_harness(test_config());
    let (out, _p1) = run_to_running(&h, "r1").await;
    h.handle.cancel_task(out.task_id).await.unwrap();
    let _ = h.driver.cancel_rx.recv().await.unwrap();
    h.clock.advance(Duration::from_secs(31)); // cancel timeout → close
    let (_sid, _close_reply) = h.driver.close_rx.recv().await.unwrap();
    // close 永不回复
    h.clock.advance(Duration::from_secs(11)); // close timeout → 隔离
    let v = wait_until(&h, out.task_id, |v| v.status == TaskStatus::Failed).await;
    assert_eq!(v.execution_state, ExecutionState::Unknown);
    assert_eq!(v.error.unwrap().code, ErrorCode::ExecutionStateUnknown);
    assert!(v.backend.recovery_required);
    let err = h.handle.start_task("r2".into(), cwd(), "x".into()).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::BackendUnavailable);
    assert_eq!(*h.driver.terminate_count.lock().unwrap(), 1);
}

#[tokio::test]
async fn completion_wins_over_cancel_intent() {
    let h = spawn_harness(test_config());
    let (out, p1) = run_to_running(&h, "r1").await;
    h.handle.cancel_task(out.task_id).await.unwrap();
    let _ = h.driver.cancel_rx.recv().await.unwrap();
    // 取消已发出，但 dsh 正常完成本轮
    p1.send(Ok(StopReasonView::EndTurn)).unwrap();
    let v = wait_until(&h, out.task_id, |v| v.status.is_terminal()).await;
    assert_eq!(v.status, TaskStatus::Completed);
}

#[tokio::test]
async fn backend_death_fails_epoch_without_replay() {
    let h = spawn_harness(test_config());
    let (out, _p1) = run_to_running(&h, "r1").await;
    h.driver
        .event_tx
        .send(BackendEvent::Died { epoch: 1, reason: "child exited".into() })
        .unwrap();
    let v = wait_until(&h, out.task_id, |v| v.status == TaskStatus::Failed).await;
    assert_eq!(v.execution_state, ExecutionState::Unknown);
    assert!(v.backend.recovery_required);
    // 不自动重启：新派发直接拒绝，factory.start 未被二次调用（fake 第二次 start 会 panic）
    let err = h.handle.start_task("r2".into(), cwd(), "x".into()).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::BackendUnavailable);
}

#[tokio::test]
async fn explicit_session_close_and_reuse_errors() {
    let h = spawn_harness(test_config());
    let (out, p1) = run_to_running(&h, "r1").await;
    // 忙会话拒绝关闭
    let err = h.handle.close_session(out.task_id).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::SessionBusy);
    p1.send(Ok(StopReasonView::EndTurn)).unwrap();
    wait_until(&h, out.task_id, |v| v.status == TaskStatus::Completed).await;
    let c = h.handle.close_session(out.task_id).await.unwrap();
    assert_eq!(c.state, SessionState::Closing);
    let (_sid, reply) = h.driver.close_rx.recv().await.unwrap();
    reply.send(Ok(())).unwrap();
    let v = wait_until(&h, out.task_id, |v| v.session.state == SessionState::Closed).await;
    assert!(!v.session.followup_available);
    let err = h.handle.follow_up("r2".into(), out.task_id, "x".into()).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::SessionClosed);
}

#[tokio::test]
async fn five_sequential_tasks_do_not_leak_quota() {
    let h = spawn_harness(test_config());
    let mut prev = None;
    for i in 0..5 {
        let rid = format!("r{i}");
        let out = match prev {
            None => {
                let o = h.handle.start_task(rid.clone(), cwd(), "x".into()).await.unwrap();
                let (_c, reply) = h.driver.new_session_rx.recv().await.unwrap();
                reply.send(Ok("dsh-1".into())).unwrap();
                o
            }
            Some(p) => h.handle.follow_up(rid.clone(), p, "x".into()).await.unwrap(),
        };
        let (_s, _t, p) = h.driver.prompt_rx.recv().await.unwrap();
        p.send(Ok(StopReasonView::EndTurn)).unwrap();
        wait_until(&h, out.task_id, |v| v.status == TaskStatus::Completed).await;
        prev = Some(out.task_id);
    }
}

#[tokio::test]
async fn double_cancel_is_idempotent() {
    let h = spawn_harness(test_config());
    let (out, p1) = run_to_running(&h, "r1").await;
    h.handle.cancel_task(out.task_id).await.unwrap();
    h.handle.cancel_task(out.task_id).await.unwrap();
    let _ = h.driver.cancel_rx.recv().await.unwrap();
    assert!(h.driver.cancel_rx.try_recv().is_err()); // 第二次 cancel 未重复通知
    p1.send(Ok(StopReasonView::Cancelled)).unwrap();
    wait_until(&h, out.task_id, |v| v.status == TaskStatus::Cancelled).await;
}

#[tokio::test]
async fn shutdown_drains_sessions_and_clears_marker() {
    let h = spawn_harness(test_config());
    let (out, p1) = run_to_running(&h, "r1").await;
    let shutdown = {
        let handle = h.handle.clone();
        tokio::spawn(async move { handle.shutdown().await })
    };
    // 活跃任务被取消
    let _ = h.driver.cancel_rx.recv().await.unwrap();
    p1.send(Ok(StopReasonView::Cancelled)).unwrap();
    // 会话随后被关闭
    let (_sid, reply) = h.driver.close_rx.recv().await.unwrap();
    reply.send(Ok(())).unwrap();
    shutdown.await.unwrap();
    let v = h.handle.get_task(out.task_id, 0, 50).await.unwrap();
    assert_eq!(v.status, TaskStatus::Cancelled);
    assert_eq!(v.session.state, SessionState::Closed);
}

/// 规格 §13.1：两项权限只裁决其一，任务保持 waiting_permission，另一项继续可见可裁决。
#[tokio::test]
async fn two_permissions_partial_resolve_stays_waiting() {
    let h = spawn_harness(test_config());
    let (out, _p1) = run_to_running(&h, "r1").await;
    let mk_req = |tc: &str| crate::acp::PermissionRequestView {
        tool_call_id: Some(tc.into()),
        options: vec![
            PermissionOptionView { option_id: format!("allow-{tc}"), name: "Allow".into(), kind: PermKind::AllowOnce },
            PermissionOptionView { option_id: format!("reject-{tc}"), name: "Reject".into(), kind: PermKind::RejectOnce },
        ],
    };
    for tc in ["tc-a", "tc-b"] {
        h.driver
            .event_tx
            .send(BackendEvent::Permission {
                epoch: 1,
                dsh_session_id: "dsh-1".into(),
                request: mk_req(tc),
                responder: crate::permission::PermResponder::Test(Box::new(|_| {})),
            })
            .unwrap();
    }
    let v = wait_until(&h, out.task_id, |v| {
        v.status == TaskStatus::WaitingPermission && v.pending_permissions.len() == 2
    })
    .await;
    let first = v.pending_permissions[0].permission_id;
    let r = h
        .handle
        .resolve_permission(out.task_id, first, v.pending_permissions[0].options[0].option_id.clone())
        .await
        .unwrap();
    assert_eq!(r.task_status, TaskStatus::WaitingPermission); // 仍有一项待决
    let v = h.handle.get_task(out.task_id, 0, 50).await.unwrap();
    assert_eq!(v.pending_permissions.len(), 1);
    assert_eq!(v.pending_permissions[0].tool_call_id.as_deref(), Some("tc-b"));
}

#[tokio::test]
async fn retention_caps_records_and_keeps_pinned_latest() {    let mut cfg = test_config();
    cfg.max_active_tasks = 1;
    cfg.max_open_sessions = 2;
    cfg.max_task_records = 6; // ≥ 2+2
    cfg.task_retention = Duration::from_secs(3600);
    let h = spawn_harness(cfg);
    // 同会话连续完成 8 个任务（会话保持开启，latest 固定保留）
    let mut first_id: Option<TaskId> = None;
    let mut prev: Option<TaskId> = None;
    for i in 0..8 {
        let rid = format!("r{i}");
        let out = match prev {
            None => {
                let o = h.handle.start_task(rid, cwd(), "x".into()).await.unwrap();
                let (_c, reply) = h.driver.new_session_rx.recv().await.unwrap();
                reply.send(Ok("dsh-1".into())).unwrap();
                o
            }
            Some(p) => h.handle.follow_up(rid, p, "x".into()).await.unwrap(),
        };
        let (_s, _t, p) = h.driver.prompt_rx.recv().await.unwrap();
        p.send(Ok(StopReasonView::EndTurn)).unwrap();
        wait_until(&h, out.task_id, |v| v.status == TaskStatus::Completed).await;
        if i == 0 {
            first_id = Some(out.task_id);
        }
        prev = Some(out.task_id);
    }
    let latest = prev.unwrap();
    let first = first_id.unwrap();
    // 推进虚拟时钟越过保留期 + 扫描间隔（60s 自调度）
    h.clock.advance(Duration::from_secs(3600 + 120));
    // 最早且非固定的任务被淘汰 → task_not_found；会话最新任务固定保留
    wait_until(&h, latest, |v| v.task_id == latest).await; // 确认 actor 仍响应
    let err = h.handle.get_task(first, 0, 1).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::TaskNotFound);
    let v = h.handle.get_task(latest, 0, 1).await.unwrap();
    assert_eq!(v.session.state, SessionState::Idle);
}
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test coordinator`
Expected: 编译失败（terminate_process 等新接口缺失）

- [ ] **Step 3: 实现**

- `Backend` trait 增加 `async fn terminate_process(&self)`；FakeBackend 增加 `terminate_count: Arc<Mutex<usize>>`（`fake_backend*` 构造函数同步更新，`FakeDriver` 暴露该计数）。
- 隔离逻辑集中在 `fn isolate_backend(&mut self, reason: String)`；幂等（已 Unavailable 直接返回）。
- 取消/关闭/关停计时器均携带 epoch + task_id/session_id，触发时复核状态，不符即弃。
- shutdown 流程在 actor 内跟踪 `pending_closes: usize`，归零且进程已 terminate 后 `guard.clear()`。

- [ ] **Step 4: 运行确认通过**

Run: `cargo test coordinator`
Expected: Task 7+8 全部测试 PASS

- [ ] **Step 5: Commit**

```bash
git add src/coordinator.rs src/acp.rs
git commit -m "feat: cancellation ladder, backend isolation, graceful shutdown"
```

---

### Task 9: 真实 ACP 后端与子进程监督（acp.rs 后半 + supervisor.rs）

**Files:**
- Create: `src/supervisor.rs`
- Modify: `src/acp.rs`（追加 DshBackend/DshFactory/FrameLimit）
- Modify: `Cargo.toml`（加 `libc = "0.2"`）
- Modify: `src/main.rs`（加 `mod supervisor;`）

**Interfaces:**
- Consumes: Task 6 的 trait/视图；探针已核实的 SDK API
- Produces:
  - `pub struct DshProcess`：`fn spawn(command: &str, args: &[String]) -> io::Result<DshProcess>`、`fn take_stdio(&mut self) -> (ChildStdin, ChildStdout)`、`fn pid(&self) -> Option<u32>`、`async fn terminate(self)`（SIGTERM → 等 TERM_GRACE → SIGKILL → 等 KILL_GRACE → wait/reap；全程 tracing）
  - `pub struct FrameLimit<R>`：futures `AsyncRead` 包装，行（JSON 帧）超 `ACP_FRAME_MAX` 返回 `io::ErrorKind::InvalidData` 错误（规格 §3：JSON 解析前限制）
  - `pub struct DshFactory { config: Config }` 实现 `BackendFactory<B = DshBackend>`；`DshBackend` 实现 `Backend`（含 `terminate_process`）
  - `DshBackend { connection: ConnectionTo<Agent>, process: Arc<Mutex<Option<DshProcess>>> }`

实现要点：

1. `DshFactory::start`（START_TIMEOUT 由调用方包裹之外的 spawn+initialize 内部计时）：
   - `DshProcess::spawn(&config.dsh_command, &config.dsh_args)`（`Command::new`+逐项 args，无 shell；stdin/stdout/stderr piped；stderr 排空任务：逐行截断 4 KiB 后 `tracing::debug!(target: "dsh.stderr")`）。
   - 传输：`ByteStreams::new(stdin.compat_write(), FrameLimit::new(stdout.compat()))`。
   - `Client.builder().name("dsh-mcp")` 注册两个 handler 后 `connect_with(transport, closure)`，整体 spawn。
   - **handler 拥塞规则（规格 §3）**：handler → coordinator 用有界 `mpsc::channel(ACP_QUEUE_MAX_ITEMS)`；handler 内 `select!{ tx.send(...) , sleep(ACP_QUEUE_STALL) => 报故障 }`——阻塞会反压 SDK reader；10s 拥塞发 `BackendEvent::Died{reason:"inbound queue congested"}`。**不丢弃**权限/终态/故障消息。字节级 8 MiB 上限以事件 8 KiB 单条截断 + 128 项近似（128×8KiB=1MiB ≪ 8MiB，天然满足）。
   - `on_receive_notification(SessionNotification)` → `BackendEvent::Update{epoch, dsh_session_id: n.session_id.to_string(), update: view_of_update(&n.update)}`。
   - `on_receive_request(RequestPermissionRequest)` → `BackendEvent::Permission{epoch, dsh_session_id, request: view_of_permission_request(&r), responder: PermResponder::Real(responder)}`。
   - closure 内：`initialize`（`InitializeRequest::new(ProtocolVersion::V1)`）→ 校验 `agent_capabilities.session_capabilities.close.is_some()`，缺失 → Err(Protocol,"dsh missing session/close capability")；记录 list/resume 支持位 → `conn_tx.send((connection, caps))` → `std::future::pending().await` 保活。
   - `connect_with` 返回（对端 EOF/错误/帧超限）→ `event_tx.send(BackendEvent::Died{epoch, reason})`。
   - 入口等待 `conn_rx` 成功 → 返回 `(DshBackend, event_rx, pid)`。
2. `DshBackend::new_session/prompt/cancel/close_session` 薄封装 `connection.send_request(...).block_task().await` / `send_notification(CancelNotification)`；错误映射：SDK 的 JSON-RPC 错误响应 → `Rejected`；连接/EOF/内部 → `ProcessDied`。**先执行 Step 1 的 Error 分类核实。**
3. `terminate_process`：取出 `process`（`Arc<Mutex<..>>` swap None）→ `DshProcess::terminate()`。
4. `StopReason` → `StopReasonView`（Task 6 已实现 From）。
5. dsh stderr、本进程 tracing 全部走 stderr（规格 §11.1）。

- [ ] **Step 1: 核实 SDK Error 分类 API 与 SessionId 显示**

Run: `grep -n "pub struct Error\|pub fn code\|pub enum.*Error" ~/.cargo/registry/src/*/agent-client-protocol-3.4.0/src/jsonrpc.rs | head -10; grep -rn "impl std::fmt::Display for SessionId\|pub struct SessionId" ~/.cargo/registry/src/*/agent-client-protocol-schema-1.12.0/src/v1/*.rs | head -5`
Expected: 确认 Error 上区分"对端错误响应"与"本地传输错误"的方法（如 `code()`/`data()` 或变体）；确认 SessionId 可 `to_string()`/`Display`。按实际 API 调整 `DshBackend` 错误映射（语义：对端 JSON-RPC error response → Rejected；其余 → ProcessDied）。

- [ ] **Step 2: 写失败测试**

`src/supervisor.rs` 与 `src/acp.rs` 底部：

```rust
#[cfg(test)]
mod supervisor_tests {
    use super::super::supervisor::DshProcess;
    use std::time::{Duration, Instant};

    #[tokio::test]
    async fn spawn_and_terminate_reaps_child() {
        let p = DshProcess::spawn("sleep", &["1000".to_string()]).unwrap();
        assert!(p.pid().is_some());
        let pid = p.pid().unwrap() as i32;
        let t0 = Instant::now();
        p.terminate().await;
        assert!(t0.elapsed() < Duration::from_secs(10));
        // 已被 wait/reap：kill(0) 报 ESRCH
        let alive = unsafe { libc::kill(pid, 0) };
        assert_eq!(alive, -1);
    }

    #[tokio::test]
    async fn argv_passed_verbatim_no_shell() {
        // 含空格与 shell 字符的参数必须原样作为单个 argv 传递
        let arg = "a; rm -rf ~ $(echo hi)".to_string();
        let mut p = DshProcess::spawn("/bin/echo", &[arg.clone()]).unwrap();
        let (stdin, mut stdout) = p.take_stdio();
        drop(stdin);
        use tokio::io::AsyncReadExt;
        let mut buf = String::new();
        stdout.read_to_string(&mut buf).await.unwrap();
        assert_eq!(buf.trim_end(), arg);
        p.terminate().await;
    }

    #[tokio::test]
    async fn spawn_missing_command_errors() {
        assert!(DshProcess::spawn("definitely-not-a-real-binary-xyz", &[]).is_err());
    }
}

#[cfg(test)]
mod frame_limit_tests {
    use super::FrameLimit;
    use futures::AsyncReadExt;

    #[tokio::test]
    async fn lines_within_limit_pass() {
        let data: &[u8] = b"{\"a\":1}\n{\"b\":2}\n";
        let mut r = FrameLimit::new(data, 1024);
        let mut out = String::new();
        r.read_to_string(&mut out).await.unwrap();
        assert_eq!(out.len(), data.len());
    }

    #[tokio::test]
    async fn oversized_frame_rejected_before_parse() {
        let mut data = vec![b'x'; 5 * 1024 * 1024];
        data.push(b'\n');
        let mut r = FrameLimit::new(data.as_slice(), 4 * 1024 * 1024);
        let mut out = Vec::new();
        let err = r.read_to_end(&mut out).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }
}
```

- [ ] **Step 3: 运行确认失败**

Run: `cargo test`
Expected: 编译失败

- [ ] **Step 4: 实现 supervisor.rs 与 acp.rs 后半**

`src/supervisor.rs`：

```rust
use crate::config::limits;
use std::io;
use std::process::Stdio;
use tokio::io::AsyncBufReadExt;
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

pub struct DshProcess {
    child: Child,
    stderr_drain: tokio::task::JoinHandle<()>,
}

impl DshProcess {
    pub fn spawn(command: &str, args: &[String]) -> io::Result<DshProcess> {
        let mut cmd = Command::new(command);
        cmd.args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(false); // 显式生命周期管理，不依赖 Drop
        let mut child = cmd.spawn()?;
        let stderr = child.stderr.take().expect("stderr piped");
        let stderr_drain = tokio::spawn(async move {
            let mut lines = tokio::io::BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let (l, truncated) = crate::events::truncate_utf8(&line, 4096);
                tracing::debug!(target: "dsh.stderr", truncated, "{l}");
            }
        });
        Ok(DshProcess { child, stderr_drain })
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

    /// 有界退出：TERM → TERM_GRACE → KILL → KILL_GRACE → wait/reap（规格 §7.4）。
    pub async fn terminate(mut self) {
        let Some(pid) = self.child.id() else {
            return;
        };
        #[cfg(unix)]
        unsafe {
            libc::kill(pid as i32, libc::SIGTERM);
        }
        let exited = tokio::time::timeout(limits::TERM_GRACE, self.child.wait()).await;
        if exited.is_err() {
            let _ = self.child.start_kill();
            let _ = tokio::time::timeout(limits::KILL_GRACE, self.child.wait()).await;
            let _ = self.child.wait().await; // 确保 reap
        }
        self.stderr_drain.abort();
    }
}
```

`FrameLimit`（acp.rs）：`struct FrameLimit<R> { inner: std::pin::Pin<Box<R>>, line_len: usize, max: usize }`；实现 `futures::AsyncRead`：`poll_read` 委托 inner 后扫描 `buf[..n]`：遇 `\n` 归零 `line_len`，否则累加；超 `max` 返回 `Poll::Ready(Err(io::Error::new(InvalidData, "ACP frame exceeds limit")))`。

`DshFactory`/`DshBackend` 按"实现要点 1–4"。`DshFactory::new(config: Config) -> Self` 公开构造器。`DshFactory` 每次 `start` 用新事件通道；`epoch` 由 coordinator 管理（start 返回的事件流不自带 epoch——coordinator 在 BackendStarted 时给转发任务绑定当前 epoch；实现：转发任务把 `BackendEvent` 原样转发，handler 里 epoch 字段由 factory 在 start 时从 coordinator 拿不到——因此 **epoch 由 coordinator 在收到事件时标记**：handler 填 0，coordinator 的转发任务（BackendStarted 时 spawn，捕获当时 epoch）把 epoch 改写为绑定值后再入 actor。调整 Task 6：`BackendEvent` 构造时 epoch 传 0，由转发任务填充）。注意 Task 7/8 测试直接构造 `BackendEvent{epoch:1,...}` 经 `event_tx` 发送——fake 路径由 driver 直发，epoch 由测试填写，语义一致。

**超时孤儿的回收**（coordinator 侧，补充 Task 8 隔离流程）：`factory.start()` 被 START_TIMEOUT 超时判负后，内部已 spawn 的连接任务仍可能稍后完成并回送 `BackendStarted{epoch=旧, Ok(..)}`——此时 epoch 已因隔离 +1，actor 按过期事件忽略前，必须先对孤儿句柄调用 `terminate_process()` 再丢弃，防止 dsh 子进程泄漏（`kill_on_drop(false)` 下 drop 不杀进程）。

- [ ] **Step 5: 运行确认通过**

Run: `cargo test`
Expected: 全部 PASS（含 supervisor 真进程测试）

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock src/supervisor.rs src/acp.rs src/main.rs
git commit -m "feat: real ACP backend over tokio process with frame limits and supervision"
```

---

### Task 10: MCP 工具层（server.rs）

**Files:**
- Create: `src/server.rs`
- Create: `tests/mcp_protocol.rs`
- Modify: `src/lib.rs`（加 `pub mod server;` 等；见 Task 1 修订说明）
- Modify: `src/coordinator.rs`（`TaskView`/`StartTaskOut` 等输出类型补 `Serialize`）

**Interfaces:**
- Consumes: `coordinator::CoordinatorHandle`（6 个方法 + shutdown）、`task::*`
- Produces:
  - `pub struct DshMcpServer { coord: CoordHandle }`（Clone，非泛型——`CoordHandle` 已类型擦除）
  - `impl DshMcpServer { pub fn new(coord: CoordHandle) -> Self; pub fn coord(&self) -> &CoordHandle }`
  - `#[rmcp::tool_router] impl DshMcpServer`：6 个工具；`#[rmcp::tool_handler(name = "dsh-mcp", instructions = "...")] impl ServerHandler`
  - `fn ok_result<T: Serialize>(v: &T, text: String) -> Result<CallToolResult, ErrorData>`——structured_content=完整 JSON；content=简短文本摘要（不复制整份，规格 §9.1）
  - `fn err_result(e: TaskError) -> Result<CallToolResult, ErrorData>`——`CallToolResult::error`（is_error=true），content 为 `{code,message,retryable,details}` JSON

参数类型（schemars 字段描述即工具文档，写明限制）：

```rust
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct StartTaskParams {
    /// 调用方生成的幂等请求 ID（≤128 字节）。相同 ID 与相同参数的重试返回同一任务。
    pub request_id: String,
    /// 任务工作目录，必须已存在的绝对路径。
    pub cwd: String,
    /// 任务内容（非空 UTF-8，≤64 KiB）。
    pub prompt: String,
}
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct GetTaskParams {
    pub task_id: String,
    /// 事件分页光标：上次响应的 next_cursor；省略从头取（含 gap 报告）。
    pub cursor: Option<u64>,
    /// 1..=100，默认 50。
    pub limit: Option<usize>,
}
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct FollowUpParams {
    pub request_id: String,
    /// 该会话最新且已终态的任务 ID。
    pub task_id: String,
    pub prompt: String,
}
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct CancelTaskParams { pub task_id: String }
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ResolvePermissionParams {
    pub task_id: String,
    pub permission_id: String,
    /// 必须是该请求公布的原始 option_id。
    pub option_id: String,
}
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct CloseSessionParams {
    /// 该会话任一仍保留的任务 ID。
    pub task_id: String,
}
```

task_id/permission_id 的 `String` → `Uuid` 解析失败 → 业务错误 `invalid_argument`（isError=true；规格 §5：JSON-RPC 错误只用于协议/结构错误）。

工具描述（`#[rmcp::tool(description = ...)]`）要点：
- start_task：后台执行并立即返回 task_id；用 get_task 轮询；重试必须带同一 request_id。
- get_task：completed 只代表一轮执行结束，需求验收须核对实际文件与测试。
- send_followup：只能作用于该会话最新已终态任务；会话忙返回 session_busy。
- cancel_task：请求取消；返回 cancelling，停止证据经 get_task 观察。
- resolve_permission：裁决等待中的权限；不裁决会超时自动拒绝。
- close_session：显式释放会话上下文；关闭后不可追问。

- [ ] **Step 1: 核实 rmcp 内容构造与客户端测试 API**

Run: `grep -n "pub fn text" ~/.cargo/registry/src/*/rmcp-3.5.1/src/model/content.rs | head -5; grep -n "pub async fn list_tools\|pub async fn call_tool\|pub fn peer" ~/.cargo/registry/src/*/rmcp-3.5.1/src/service.rs ~/.cargo/registry/src/*/rmcp-3.5.1/src/service/*.rs 2>/dev/null | head -8; grep -n "struct CallToolRequestParams" -A 12 ~/.cargo/registry/src/*/rmcp-3.5.1/src/model.rs | grep "pub fn\|pub [a-z]" | head -8`
Expected: 确认 `ContentBlock::text(...)` 或 `Content::text(...)` 的实际路径；`Peer` 上 `list_tools`/`call_tool` 的方法名与 `CallToolRequestParams` 构造方式。按实际 API 调整测试代码。

- [ ] **Step 2: 写失败测试 `tests/mcp_protocol.rs`**

```rust
use dsh_mcp::acp::fake_backend;
use dsh_mcp::coordinator::{ManualClock, spawn_coordinator};
use dsh_mcp::runtime_guard::RuntimeGuard;
use dsh_mcp::server::DshMcpServer;
use rmcp::model::CallToolRequestParams;
use std::sync::Arc;

async fn spawn_pair() -> (rmcp::service::RunningService<rmcp::service::RoleClient, ()>, dsh_mcp::acp::FakeDriver) {
    let (factory, driver) = fake_backend();
    let dir = tempfile::tempdir().unwrap();
    let guard = match RuntimeGuard::acquire(dir.path()).unwrap() {
        dsh_mcp::runtime_guard::AcquireOutcome::Clean(g) => Arc::new(g),
        dsh_mcp::runtime_guard::AcquireOutcome::Recovery(_) => panic!("fresh tempdir must be clean"),
    };
    let coord = spawn_coordinator(test_config(), factory, ManualClock::new(), guard, false);
    let server = DshMcpServer::new(coord);
    let (client_io, server_io) = tokio::io::duplex(1 << 20);
    let server_task = tokio::spawn(async move {
        let running = rmcp::serve_server(server, server_io).await.unwrap();
        running.waiting().await.unwrap();
    });
    let client = rmcp::serve_client((), client_io).await.unwrap();
    std::mem::forget(server_task); // 测试进程结束即清
    (client, driver)
}
```

注：`serve_client`/`serve_server` 直接接受实现了 tokio AsyncRead+AsyncWrite 的 `DuplexStream`；若类型不匹配，用 `tokio::io::split` 拆成 `(ReadHalf, WriteHalf)` 元组传入。`test_config()` 在测试文件内复制 Task 7 的构造。客户端 handler 类型若不接受 `()`，用 `rmcp::handler::client::ClientHandler::default()`。

测试用例：

```rust
#[tokio::test]
async fn tools_list_has_six_tools_with_schemas() {
    let (client, _d) = spawn_pair().await;
    let tools = client.peer().list_tools(Default::default()).await.unwrap();
    assert_eq!(tools.tools.len(), 6);
    for name in ["start_task", "get_task", "send_followup", "cancel_task", "resolve_permission", "close_session"] {
        let t = tools.tools.iter().find(|t| t.name == name).unwrap_or_else(|| panic!("missing {name}"));
        assert!(t.description.is_some());
        assert!(t.input_schema.get("properties").is_some());
    }
}

#[tokio::test]
async fn start_task_invalid_cwd_is_business_error() {
    let (client, _d) = spawn_pair().await;
    let r = client.peer().call_tool(CallToolRequestParams::new("start_task").with_arguments(
        serde_json::json!({"request_id":"r1","cwd":"relative/path","prompt":"x"}).as_object().unwrap().clone(),
    )).await.unwrap();
    assert_eq!(r.is_error, Some(true));
    let text = r.content[0].as_text().unwrap().text.clone();
    let v: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(v["code"], "invalid_argument");
    assert!(v["retryable"].is_boolean());
}

#[tokio::test]
async fn start_and_poll_task_while_running() {
    let (client, mut d) = spawn_pair().await;
    let r = client.peer().call_tool(CallToolRequestParams::new("start_task").with_arguments(
        serde_json::json!({"request_id":"r1","cwd":std::env::temp_dir().to_string_lossy(),"prompt":"hello"}).as_object().unwrap().clone(),
    )).await.unwrap();
    assert_eq!(r.is_error, Some(false));
    let v: serde_json::Value = serde_json::from_value(r.structured_content.unwrap()).unwrap();
    let task_id = v["task_id"].as_str().unwrap().to_string();
    // 任务在后台推进；get_task 在执行期间正常响应
    let (_cwd, reply) = d.new_session_rx.recv().await.unwrap();
    reply.send(Ok("dsh-1".into())).unwrap();
    let (_s, _t, _p) = d.prompt_rx.recv().await.unwrap();
    let g = client.peer().call_tool(CallToolRequestParams::new("get_task").with_arguments(
        serde_json::json!({"task_id": task_id}).as_object().unwrap().clone(),
    )).await.unwrap();
    let gv: serde_json::Value = serde_json::from_value(g.structured_content.unwrap()).unwrap();
    assert_eq!(gv["status"], "running");
    assert_eq!(gv["execution_state"], "in_progress");
    // 任务运行中 cancel_task 也应及时返回（规格 §13.1 协议级要求）
    let c = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        client.peer().call_tool(CallToolRequestParams::new("cancel_task").with_arguments(
            serde_json::json!({"task_id": task_id}).as_object().unwrap().clone(),
        )),
    ).await.unwrap().unwrap();
    assert_eq!(c.is_error, Some(false));
    let cv: serde_json::Value = serde_json::from_value(c.structured_content.unwrap()).unwrap();
    assert_eq!(cv["status"], "cancelling");
}
```

- [ ] **Step 3: 运行确认失败**

Run: `cargo test --test mcp_protocol`
Expected: 编译失败（server.rs 不存在）

- [ ] **Step 4: 实现 server.rs**

结构：

```rust
use crate::acp::Backend;
use crate::coordinator::{CoordinatorHandle, TaskView};
use crate::coordinator::{CoordHandle, TaskView};
use crate::task::{ErrorCode, TaskError};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, ContentBlock};
use rmcp::{ErrorData, ServerHandler};
use serde::Serialize;
use std::path::PathBuf;

#[derive(Clone)]
pub struct DshMcpServer {
    coord: CoordHandle,
}

fn ok_result<T: Serialize>(v: &T, text: String) -> Result<CallToolResult, ErrorData> {
    let value = serde_json::to_value(v)
        .map_err(|e| ErrorData::internal_error(e.to_string(), None))?;
    let mut r = CallToolResult::structured(value);
    r.content = vec![ContentBlock::text(text)];
    Ok(r)
}

fn err_result(e: TaskError) -> Result<CallToolResult, ErrorData> {
    let body = serde_json::json!({
        "code": e.code,
        "message": e.message,
        "retryable": e.retryable,
        "details": e.details,
    });
    Ok(CallToolResult::error(vec![ContentBlock::text(body.to_string())]))
}

impl DshMcpServer {
    pub fn new(coord: CoordHandle) -> Self {
        Self { coord }
    }
    pub fn coord(&self) -> &CoordHandle {
        &self.coord
    }
}

#[rmcp::tool_router]
impl DshMcpServer {
    #[rmcp::tool(description = "Delegate a new coding task to dsh in the given absolute working directory. Returns immediately with task_id; poll get_task for progress. Retries must reuse the same request_id.")]
    async fn start_task(&self, Parameters(p): Parameters<StartTaskParams>) -> Result<CallToolResult, ErrorData> {
        match self.coord.start_task(p.request_id, PathBuf::from(p.cwd), p.prompt).await {
            Ok(out) => ok_result(&out, format!("task {} session {} status {:?} dedup {}", out.task_id, out.session_id, out.status, out.deduplicated)),
            Err(e) => err_result(e),
        }
    }
    // get_task / send_followup / cancel_task / resolve_permission / close_session 同构：
    // 解析 Uuid 参数（失败 → invalid_argument 业务错误），调用 coord 方法，ok_result/err_result。
    // get_task 文本回退：format!("task {id} status={status} cursor {next}/{last} has_more={has_more}")
}

#[rmcp::tool_handler(name = "dsh-mcp", instructions = "Delegate coding tasks to local DeepSeek Harness. Workflow: start_task → poll get_task → send_followup for rework on the latest finished task → cancel_task to stop → close_session to release context. completed only means one turn finished; verify real files and tests before accepting.")]
impl ServerHandler for DshMcpServer {}
```

- [ ] **Step 5: 运行确认通过**

Run: `cargo test --test mcp_protocol`
Expected: 3 个测试 PASS

- [ ] **Step 6: Commit**

```bash
git add src/server.rs src/lib.rs tests/mcp_protocol.rs src/coordinator.rs
git commit -m "feat: MCP tool layer with structured responses and business errors"
```

---

### Task 11: main.rs 组装、信号与关停

**Files:**
- Modify: `src/main.rs`（整体替换为薄二进制）

**Interfaces:**
- Consumes: 全部模块
- Produces: 二进制行为：`dsh-mcp`（MCP stdio 服务）；`dsh-mcp --acknowledge-cleanup`（人工清理确认）；退出码：0 正常/清理完成，1 关停预算耗尽（标记保留），2 配置错误/实例冲突/acknowledge 失败

main 流程：

```rust
use dsh_mcp::acp::DshFactory;
use dsh_mcp::config::Config;
use dsh_mcp::coordinator::{TokioClock, spawn_coordinator};
use dsh_mcp::runtime_guard::{AcquireOutcome, GuardError, RuntimeGuard};
use dsh_mcp::server::DshMcpServer;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    std::process::exit(real_main().await);
}

async fn real_main() -> i32 {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args: Vec<String> = std::env::args().collect();
    let config = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            return 2;
        }
    };

    if args.iter().any(|a| a == "--acknowledge-cleanup") {
        return match RuntimeGuard::acknowledge_cleanup(&config.state_dir) {
            Ok(()) => {
                eprintln!("recovery marker cleared; safe to start dsh-mcp again");
                0
            }
            Err(e) => {
                eprintln!("acknowledge-cleanup failed: {e}");
                2
            }
        };
    }

    let (guard, recovery) = match RuntimeGuard::acquire(&config.state_dir) {
        Ok(AcquireOutcome::Clean(g)) => (Arc::new(g), false),
        Ok(AcquireOutcome::Recovery(g)) => {
            eprintln!("recovery_required: previous run did not clean up; serving read-only tools only. Stop other instances, verify no dsh work is still running, then run: dsh-mcp --acknowledge-cleanup");
            (Arc::new(g), true)
        }
        Err(GuardError::InstanceInUse) => {
            eprintln!("instance_in_use: another dsh-mcp holds the state dir lock");
            return 2;
        }
        Err(e) => {
            eprintln!("runtime guard error: {e}");
            return 2;
        }
    };
    if !recovery {
        if let Err(e) = guard.mark_started(uuid::Uuid::new_v4()) {
            eprintln!("failed to write run-state marker: {e}");
            return 2;
        }
    }

    let factory = DshFactory::new(config.clone());
    let coord = spawn_coordinator(config.clone(), factory, TokioClock, guard.clone(), recovery);
    let server = DshMcpServer::new(coord.clone());

    let running = match rmcp::serve_server(server, rmcp::transport::stdio()).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("mcp serve error: {e}");
            return 1;
        }
    };

    // MCP stdin EOF 或 SIGINT/SIGTERM → draining（规格 §8.3）
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
    tokio::select! {
        _ = running.waiting() => {},
        _ = tokio::signal::ctrl_c() => {},
        _ = sigterm.recv() => {},
    }

    coord.shutdown().await;
    // 关停预算耗尽时标记保留 → 非零退出
    if !recovery && config.state_dir.join("run-state.json").exists() { 1 } else { 0 }
}
```

注意：`waiting()` 语义以 rmcp 3.5.1 为准（等待服务结束）；若 EOF 不自动结束服务，需另起任务读 stdin EOF 后取消——用 Step 2 的冒烟测试判定。

- [ ] **Step 1: 运行 `cargo build` 确认编译**

Run: `cargo build`
Expected: 编译通过

- [ ] **Step 2: 冒烟测试（shell 脚本手工验证，写成测试）**

`tests/smoke.rs`（真实进程，fake 配置不可行——用不存在命令验证错误路径 + EOF 关停路径）：

```rust
#[tokio::test]
async fn eof_triggers_clean_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_dsh-mcp"))
        .env("DSH_MCP_STATE_DIR", dir.path())
        .env("DSH_MCP_DSH_COMMAND", "definitely-not-a-real-binary-xyz")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    drop(child.stdin.take()); // MCP 客户端断连
    let status = tokio::time::timeout(
        std::time::Duration::from_secs(70), // > 默认关停预算 50s 的余量
        child.wait(),
    )
    .await
    .expect("bridge did not exit after EOF")
    .unwrap();
    // 从未启动后端：清理干净，标记删除，退出码 0
    assert_eq!(status.code(), Some(0));
    assert!(!dir.path().join("run-state.json").exists());
}

#[test]
fn acknowledge_cleanup_without_marker_is_ok() {
    let dir = tempfile::tempdir().unwrap();
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_dsh-mcp"))
        .arg("--acknowledge-cleanup")
        .env("DSH_MCP_STATE_DIR", dir.path())
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(0));
}

#[tokio::test]
async fn sigterm_triggers_clean_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let child = std::process::Command::new(env!("CARGO_BIN_EXE_dsh-mcp"))
        .env("DSH_MCP_STATE_DIR", dir.path())
        .env("DSH_MCP_DSH_COMMAND", "definitely-not-a-real-binary-xyz")
        .stdin(std::process::Stdio::piped()) // 保持打开，验证信号路径而非 EOF
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let pid = child.id();
    unsafe { libc::kill(pid as i32, libc::SIGTERM) };
    let status = tokio::time::timeout(
        std::time::Duration::from_secs(70),
        tokio::task::spawn_blocking(move || child.wait_with_output()),
    )
    .await
    .expect("bridge did not exit after SIGTERM")
    .unwrap()
    .unwrap()
    .status;
    assert_eq!(status.code(), Some(0));
    assert!(!dir.path().join("run-state.json").exists());
}
```

`CARGO_BIN_EXE_dsh-mcp` 要求存在 bin 目标（main.rs 即满足）。

- [ ] **Step 3: 运行确认**

Run: `cargo test --test smoke`
Expected: 3 个测试 PASS（EOF/SIGTERM 约数秒——无任务时 draining 应立即完成；若超过预算说明 EOF/信号未被识别，按"注意"修复）

- [ ] **Step 4: Commit**

```bash
git add src/main.rs src/coordinator.rs tests/smoke.rs
git commit -m "feat: wire entrypoint with signals, draining, and recovery modes"
```

---

### Task 12: 真实 dsh 控制面集成测试（无模型）

**Files:**
- Create: `tests/control_plane.rs`

**Interfaces:**
- Consumes: `acp::DshFactory`（Task 9）、`BackendFactory/Backend` trait
- Produces: 控制面验收；未安装 dsh 的环境显式跳过并打印原因（规格 §13.2）

- [ ] **Step 1: 写测试**

```rust
use dsh_mcp::acp::{Backend, BackendFactory, DshFactory};
use dsh_mcp::config::Config;

fn dsh_command() -> Option<(String, Vec<String>)> {
    if let Ok(cmd) = std::env::var("DSH_MCP_TEST_DSH_COMMAND") {
        let args: Vec<String> = std::env::var("DSH_MCP_TEST_DSH_ARGS_JSON")
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_else(|| vec!["--profile".into(), "acp".into()]);
        return Some((cmd, args));
    }
    // 尝试 PATH 中的 dsh
    match std::process::Command::new("dsh").arg("--help").output() {
        Ok(o) if o.status.success() => Some(("dsh".into(), vec!["--profile".into(), "acp".into()])),
        _ => None,
    }
}

fn test_config(cmd: String, args: Vec<String>) -> Config {
    Config {
        dsh_command: cmd,
        dsh_args: args,
        state_dir: tempfile::tempdir().unwrap().keep(),
        max_active_tasks: 4,
        max_open_sessions: 16,
        session_idle_ttl: std::time::Duration::from_secs(1800),
        max_task_records: 256,
        task_retention: std::time::Duration::from_secs(86400),
        start_timeout: std::time::Duration::from_secs(30),
        permission_timeout: std::time::Duration::from_secs(600),
        cancel_timeout: std::time::Duration::from_secs(30),
        close_timeout: std::time::Duration::from_secs(10),
        shutdown_timeout: std::time::Duration::from_secs(50),
    }
}

#[tokio::test]
async fn dsh_control_plane_roundtrip() {
        let Some((cmd, args)) = dsh_command() else {
        eprintln!("SKIP: dsh not installed; set DSH_MCP_TEST_DSH_COMMAND to run control-plane tests");
        return;
    };
    // 隔离 dsh home，避免加载真实凭据/配置（规格 §13.2）
    let dsh_home = tempfile::tempdir().unwrap();
    unsafe { std::env::set_var("DSH_HOME", dsh_home.path()) };

    let config = test_config(cmd, args);
    let mut factory = DshFactory::new(config);
    let (backend, mut events, pid) = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        factory.start(),
    )
    .await
    .expect("dsh start timed out")
    .expect("dsh start failed");
    assert!(pid > 0);

    let workdir = tempfile::tempdir().unwrap();
    let session = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        backend.new_session(workdir.path()),
    )
    .await
    .expect("session/new timed out")
    .expect("session/new failed");
    assert!(!session.is_empty());

    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        backend.close_session(&session),
    )
    .await
    .expect("session/close timed out")
    .expect("session/close failed");

    backend.terminate_process().await;
    // 连接应随进程退出报告 Died（或直接因 EOF 结束）
    while let Some(ev) = events.recv().await {
        if matches!(ev, dsh_mcp::acp::BackendEvent::Died { .. }) {
            break;
        }
    }
}
```

`Config` 需 `Clone`；`tempfile::TempDir::keep()` 若不存在于当前 tempfile 版本，改用 `into_path()`（行为等价：保留目录）。`DSH_HOME` 的 set_var 在 Rust 2024 为 unsafe；测试为独立进程，接受该 unsafe（本计划 edition 2021，冗余 unsafe 块按 Task 1 注记处理）。

- [ ] **Step 2: 运行（本机有 dsh）**

Run: `DSH_MCP_TEST_DSH_COMMAND=/home/agent_coding/.nvm/versions/node/v24.14.0/bin/node DSH_MCP_TEST_DSH_ARGS_JSON='["/home/agent_coding/.npm/_npx/b86ed90107c62dab/node_modules/@deepseek-ai/dsh/lib/bin.js","--profile","acp"]' cargo test --test control_plane -- --nocapture`
Expected: PASS；记录实际控制面行为（session/new 是否成功、close 是否正常）。若空 DSH_HOME 下握手需要额外配置（规格 §13.2 提示未实测），记录实际缺口并补夹具或如实标注。

- [ ] **Step 3: Commit**

```bash
git add tests/control_plane.rs
git commit -m "test: real dsh control-plane roundtrip (no model calls)"
```

---

### Task 13: 真实模型验收、README 与最终门禁

**Files:**
- Create: `scripts/e2e_real_model.py`
- Create: `README.md`
- Modify: 无源码改动（除非验收暴露问题）

**Interfaces:**
- Consumes: 完整二进制
- Produces: 验收记录（写入 README"验证记录"节）

- [ ] **Step 1: 写 `scripts/e2e_real_model.py`（stdlib-only，MCP stdio 换行 JSON-RPC）**

```python
#!/usr/bin/env python3
"""dsh-mcp 真实模型验收（规格 §13.3）。人工触发，不进 CI。

用法:
  DSH_MCP_BIN=./target/release/dsh-mcp \
  DSH_MCP_DSH_COMMAND=/path/to/node \
  DSH_MCP_DSH_ARGS_JSON='["/path/to/dsh/lib/bin.js","--profile","acp"]' \
  python3 scripts/e2e_real_model.py

覆盖: (1) 派发+轮询+文件核对+request_id 去重; (2) 追问返工+显式关闭+关闭后追问报错;
(3) 双临时目录并发隔离; (4) 取消持续写文件任务+停止证据; (5) 权限路径(若 dsh 触发);
(7) 清理。强制故障路径由单元测试覆盖（fake ACP），此处不制造真实未知执行。
"""
import json, os, shutil, subprocess, sys, tempfile, time, uuid

BIN = os.environ.get("DSH_MCP_BIN", "./target/release/dsh-mcp")

class Mcp:
    def __init__(self):
        env = dict(os.environ)
        env["DSH_MCP_STATE_DIR"] = tempfile.mkdtemp(prefix="dsh-mcp-state-")
        self.proc = subprocess.Popen(
            [BIN], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, text=True, env=env)
        self.next_id = 0
        self._rpc("initialize", {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {"name": "e2e-acceptance", "version": "0.1.0"}})
        self._notify("notifications/initialized", {})

    def _notify(self, method, params):
        self.proc.stdin.write(json.dumps(
            {"jsonrpc": "2.0", "method": method, "params": params}) + "\n")
        self.proc.stdin.flush()

    def _rpc(self, method, params):
        self.next_id += 1
        rid = self.next_id
        self.proc.stdin.write(json.dumps(
            {"jsonrpc": "2.0", "id": rid, "method": method, "params": params}) + "\n")
        self.proc.stdin.flush()
        while True:
            line = self.proc.stdout.readline()
            if not line:
                raise RuntimeError("bridge stdout EOF")
            msg = json.loads(line)
            if msg.get("id") == rid:
                return msg

    def tool(self, name, arguments):
        msg = self._rpc("tools/call", {"name": name, "arguments": arguments})
        result = msg.get("result") or {}
        if result.get("isError"):
            return None, json.loads(result["content"][0]["text"])
        return result.get("structuredContent"), None

    def wait_terminal(self, task_id, timeout=180):
        deadline = time.time() + timeout
        cursor = 0
        saw_permission = False
        while time.time() < deadline:
            view, err = self.tool("get_task", {"task_id": task_id, "cursor": cursor, "limit": 50})
            assert err is None, err
            cursor = view["next_cursor"]
            if view["status"] in ("completed", "failed", "cancelled"):
                return view, saw_permission
            if view["status"] == "waiting_permission":
                # 授权验收环境：逐项裁决第一个 allow_once 选项，让权限路径真实走通
                for perm in view.get("pending_permissions", []):
                    saw_permission = True
                    allow = next((o for o in perm["options"] if o["kind"] == "allow_once"),
                                 perm["options"][0])
                    out, perr = mcp.tool("resolve_permission", {
                        "task_id": task_id,
                        "permission_id": perm["permission_id"],
                        "option_id": allow["option_id"]})
                    assert perr is None, perr
                time.sleep(1)
                continue
            time.sleep(1)
        raise TimeoutError(f"task {task_id} not terminal in {timeout}s")

    def close(self):
        self.proc.stdin.close()
        return self.proc.wait(timeout=70)

def main():
    failures = []
    mcp = Mcp()

    def check(name, cond, detail=""):
        print(("PASS" if cond else "FAIL"), name, detail)
        if not cond:
            failures.append(name)

    # (1) 派发写文件任务 + request_id 去重
    d1 = tempfile.mkdtemp(prefix="dsh-task1-")
    rid = "req-" + uuid.uuid4().hex
    out, err = mcp.tool("start_task", {
        "request_id": rid, "cwd": d1,
        "prompt": "Create a file hello.txt containing exactly: hello dsh-mcp. Then stop."})
    check("start_task accepted", err is None and out["status"] == "starting")
    dup, err2 = mcp.tool("start_task", {
        "request_id": rid, "cwd": d1,
        "prompt": "Create a file hello.txt containing exactly: hello dsh-mcp. Then stop."})
    check("duplicate request_id deduplicated",
          err2 is None and dup["task_id"] == out["task_id"] and dup["deduplicated"])
    conflict, err3 = mcp.tool("start_task", {
        "request_id": rid, "cwd": d1, "prompt": "different prompt"})
    check("same id different params conflicts",
          conflict is None and err3 and err3["code"] == "request_conflict")
    v, perm1 = mcp.wait_terminal(out["task_id"])
    check("task completed", v["status"] == "completed", v.get("error") or "")
    hello = os.path.join(d1, "hello.txt")
    check("file actually written",
          os.path.exists(hello) and "hello dsh-mcp" in open(hello).read())

    # (2) 追问返工 + 显式关闭 + 关闭后追问报错
    fu, err = mcp.tool("send_followup", {
        "request_id": "req-" + uuid.uuid4().hex, "task_id": out["task_id"],
        "prompt": "Append a line: v2"})
    check("followup accepted", err is None and fu["parent_task_id"] == out["task_id"])
    v2, perm2 = mcp.wait_terminal(fu["task_id"])
    check("followup completed", v2["status"] == "completed", v2.get("error") or "")
    check("rework landed", "v2" in open(hello).read())
    cs, err = mcp.tool("close_session", {"task_id": fu["task_id"]})
    check("close_session accepted", err is None)
    v3 = mcp.tool("get_task", {"task_id": fu["task_id"]})[0]
    deadline = time.time() + 30
    while v3["session"]["state"] != "closed" and time.time() < deadline:
        time.sleep(1)
        v3 = mcp.tool("get_task", {"task_id": fu["task_id"]})[0]
    check("session closed", v3["session"]["state"] == "closed")
    _, err = mcp.tool("send_followup", {
        "request_id": "req-" + uuid.uuid4().hex, "task_id": fu["task_id"], "prompt": "x"})
    check("followup after close rejected", err and err["code"] == "session_closed")

    # (3) 双目录并发
    d2 = tempfile.mkdtemp(prefix="dsh-task2-")
    d3 = tempfile.mkdtemp(prefix="dsh-task3-")
    t2, _ = mcp.tool("start_task", {
        "request_id": "req-" + uuid.uuid4().hex, "cwd": d2,
        "prompt": "Create marker.txt with content: two"})
    t3, _ = mcp.tool("start_task", {
        "request_id": "req-" + uuid.uuid4().hex, "cwd": d3,
        "prompt": "Create marker.txt with content: three"})
    v4, perm3 = mcp.wait_terminal(t2["task_id"])
    v5, perm4 = mcp.wait_terminal(t3["task_id"])
    check("concurrent tasks completed", v4["status"] == "completed" and v5["status"] == "completed")
    check("workspace isolation",
          open(os.path.join(d2, "marker.txt")).read().strip() == "two"
          and open(os.path.join(d3, "marker.txt")).read().strip() == "three")

    # (4) 取消持续写文件任务
    d4 = tempfile.mkdtemp(prefix="dsh-task4-")
    t4, _ = mcp.tool("start_task", {
        "request_id": "req-" + uuid.uuid4().hex, "cwd": d4,
        "prompt": ("Run a bash loop: for i in $(seq 1 300); do echo $i >> numbers.txt; "
                   "sleep 1; done. Do not stop early.")})
    time.sleep(8)  # 让循环真实开始
    c, err = mcp.tool("cancel_task", {"task_id": t4["task_id"]})
    check("cancel accepted", err is None and c["status"] == "cancelling")
    v6, _perm5 = mcp.wait_terminal(t4["task_id"], timeout=90)
    check("cancelled with stop evidence",
          v6["status"] in ("cancelled", "failed") and v6["execution_state"] in ("stopped", "unknown"))
    nums = os.path.join(d4, "numbers.txt")
    if v6["execution_state"] == "stopped" and os.path.exists(nums):
        n1 = sum(1 for _ in open(nums))
        time.sleep(3)
        n2 = sum(1 for _ in open(nums))
        check("writes stopped after cancel", n1 == n2, f"{n1} -> {n2}")

    # (5) 权限路径：等待循环内已主动裁决 allow_once；统计是否真实出现过
    seen_perm = any([perm1, perm2, perm3, perm4])
    print("NOTE permission path exercised in real run:", seen_perm)
    # 未触发不判 FAIL（规格 §13.3-5：如实记录覆盖缺失）

    # (7) 关停与清理
    code = mcp.close()
    check("bridge exited cleanly", code == 0)
    shutil.rmtree(d1, ignore_errors=True)
    shutil.rmtree(d2, ignore_errors=True)
    shutil.rmtree(d3, ignore_errors=True)
    shutil.rmtree(d4, ignore_errors=True)

    print("\n%d checks failed" % len(failures))
    return 1 if failures else 0

if __name__ == "__main__":
    sys.exit(main())
```

注意：MCP 响应键名 `structuredContent`/`isError` 以 rmcp 实际序列化（serde camelCase）为准——实现时在协议测试（Task 10）中顺手核实一次并在脚本中校准。

- [ ] **Step 2: 发布构建 + 运行真实模型验收**

Run:
```bash
cargo build --release
DSH_MCP_BIN=./target/release/dsh-mcp \
DSH_MCP_DSH_COMMAND=/home/agent_coding/.nvm/versions/node/v24.14.0/bin/node \
DSH_MCP_DSH_ARGS_JSON='["/home/agent_coding/.npm/_npx/b86ed90107c62dab/node_modules/@deepseek-ai/dsh/lib/bin.js","--profile","acp"]' \
python3 scripts/e2e_real_model.py
```
Expected: 全部 check PASS；权限 NOTE 如实记录。失败项先修再复跑；dsh 凭据/网络失败如实记录为未验证（不伪造通过）。

- [ ] **Step 3: 核实 Codex 配置键名并写 README**

Run: `curl --proxy http://127.0.0.1:7890 --noproxy '' -L --connect-timeout 10 --max-time 25 -sS 'https://learn.chatgpt.com/docs/config-file/config-reference' | grep -o 'mcp_servers[^"]*' | head -5`（或当时官方文档 URL，规格 §14 要求发布前核对）

README.md 内容（完整写入）：

```markdown
# dsh-mcp

A Rust MCP server for delegating coding tasks to DeepSeek Harness via ACP.
用 Rust 编写的 MCP 服务，通过 ACP v1 调度本机 DeepSeek Harness（dsh），支持任务执行、会话续接、进度查询和取消。

## 架构

Codex ──MCP stdio──> dsh-mcp ──ACP v1 stdio──> dsh --profile acp ──> DeepSeek API

- 单 dsh 子进程，一条 ACP 连接复用多个会话；同一会话任务串行，不同会话可并发（上限 MAX_ACTIVE_TASKS）。
- 任务元数据在内存；桥接重启后任务记录不恢复（dsh 侧会话仍持久化，本版不做映射重建）。
- 后端故障进入恢复保护：禁止新派发，人工确认清理后以 `dsh-mcp --acknowledge-cleanup` 解除。

## 安装与配置

cargo build --release  # 产出 target/release/dsh-mcp

环境变量（均有默认值；无效值启动即报错）：
DSH_MCP_DSH_COMMAND（默认 dsh）、DSH_MCP_DSH_ARGS_JSON（默认 ["--profile","acp"]，JSON 字符串数组，逐项传递无 shell）、
DSH_MCP_STATE_DIR（默认 $XDG_STATE_HOME/dsh-mcp 或 ~/.local/state/dsh-mcp）、
DSH_MCP_MAX_ACTIVE_TASKS=4、DSH_MCP_MAX_OPEN_SESSIONS=16、DSH_MCP_SESSION_IDLE_TTL_SECS=1800、
DSH_MCP_MAX_TASK_RECORDS=256、DSH_MCP_TASK_RETENTION_SECS=86400、
DSH_MCP_START_TIMEOUT_SECS=30、DSH_MCP_PERMISSION_TIMEOUT_SECS=600、
DSH_MCP_CANCEL_TIMEOUT_SECS=30、DSH_MCP_CLOSE_TIMEOUT_SECS=10、DSH_MCP_SHUTDOWN_TIMEOUT_SECS=50。

绝对路径 node 启动示例：
DSH_MCP_DSH_COMMAND=/path/to/node
DSH_MCP_DSH_ARGS_JSON='["/path/to/dsh/lib/bin.js","--profile","acp"]'

## Codex MCP 配置示例

（键名以当时官方文档核对为准——发布前重新核对）

[mcp_servers.dsh-mcp]
command = "/absolute/path/to/dsh-mcp"
env = { DSH_MCP_DSH_COMMAND = "/path/to/node", DSH_MCP_DSH_ARGS_JSON = "[\"/path/to/dsh/lib/bin.js\",\"--profile\",\"acp\"]" }

## 六个工具的工作流

start_task(request_id, cwd, prompt) → get_task(task_id, cursor, limit) 轮询
→ send_followup(request_id, task_id, prompt) 同会话返工（仅最新已终态任务）
→ cancel_task(task_id) / resolve_permission(task_id, permission_id, option_id) / close_session(task_id)

- request_id 由调用方生成：相同 ID+参数重试返回同一任务（不重复派发）；同 ID 不同参数报 request_conflict。
- completed 只代表一轮执行结束；验收必须核对真实文件与测试。
- waiting_permission 时查看 get_task 的 pending_permissions，用 resolve_permission 裁决；超时自动拒绝。

## 异常恢复

桥接异常退出会留下恢复保护标记，下次启动进入 recovery_required（工具可用、禁止新派发）。
确认无遗留 dsh 执行后运行：dsh-mcp --acknowledge-cleanup，再重启。

## 已验证环境

- dsh @deepseek-ai/dsh 0.2.0-rc.2（ACP v1）；rmcp 3.5.1；agent-client-protocol 3.4.0；rustc 1.95。
- 验证记录：见下节（构建/单元/控制面/真实模型四组结果，含未覆盖项）。

## 验证记录

（由 Task 13 Step 4 填入实际运行结果）

## License

Apache-2.0
```

- [ ] **Step 4: 最终门禁 + 填写验证记录**

Run:
```bash
cargo fmt --check && cargo build --locked && cargo test --locked && cargo clippy --locked -- -D warnings
```
Expected: 全绿。把三组测试（mock 单元 / 控制面 / 真实模型）的实际结果与未覆盖项写入 README"验证记录"，然后提交。

- [ ] **Step 5: Commit**

```bash
git add scripts/e2e_real_model.py README.md
git commit -m "docs: README with usage, recovery, and verification records"
```

---

## 执行方式说明

- 每个任务按 Step 顺序执行；任务间有依赖（1→2→…→13），不并行。
- 所有 `cargo` 联网命令前缀 `https_proxy=http://127.0.0.1:7890`。
- 遇到 crate API 与本计划不符：以 `~/.cargo/registry/src/index.crates.io-*/` 中锁定版本的源码为准修正，并在提交信息中说明偏差。

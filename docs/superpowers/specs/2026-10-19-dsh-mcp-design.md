# dsh-mcp 第一版设计规格

日期：2026-10-19。状态：设计已经用户确认，待实现。

## 1. 目标与范围

实现一个 Rust MCP 服务端 `dsh-mcp`，向 Codex 提供任务工具，通过 ACP v1 调用本机 dsh（DeepSeek Harness，`@deepseek-ai/dsh` 0.2.0-rc.2）执行编码任务。

数据流：

```text
Codex ──MCP (stdio)──> dsh-mcp ──ACP v1 (stdio JSON-RPC)──> dsh --profile acp ──> DeepSeek API
```

第一版范围（来自 HANDOFF.md 第 7 节，已确认）：

- Linux 本机、MCP stdio 传输
- 单 dsh ACP 子进程，一条 ACP 连接复用多个会话
- 任务派发、状态/进度查询、同会话追问返工、取消、权限裁决
- 不做：跨桥接进程重启的任务恢复、模型/推理选项选择、多 agent 通用调度、HTTP 传输

## 2. 已核实的环境事实

- 工作目录 `/home/workspace/dsh-mcp`，git 仓库（仅 Initial commit），Apache-2.0 LICENSE 已存在。
- 工具链：cargo 1.95.0 / rustc 1.95.0（`~/.cargo/bin/`），满足两个 SDK 的 rust_version 1.88 要求。
- 本机 dsh：`/home/agent_coding/.nvm/versions/node/v24.14.0/bin/node /home/agent_coding/.npm/_npx/b86ed90107c62dab/node_modules/@deepseek-ai/dsh/lib/bin.js --profile acp`。此路径为本机定位，**不硬编码**进可分发代码，通过配置注入。
- 联网需走代理 `http://127.0.0.1:7890`；crates.io API 对本机出口返回 403，sparse index（index.crates.io）可用。
- 已发布 crate 版本（2026-10-19 经 sparse index 核实）：`rmcp` 最新 3.5.1，`agent-client-protocol` 最新 3.4.0。

## 3. 本机 dsh ACP 接口约束（来自 dsh-acp README，已读原文）

- ACP v1，stdio JSON-RPC；stdout 仅协议，日志走 stderr。
- 支持：`initialize`、`authenticate`（立即成功，不代表模型凭据可用）、`session/new`（需绝对工作目录）、`session/list`、`session/resume`、`session/close`、`session/set_config_option`、`session/prompt`（每会话同时一个提示词）、`session/cancel` / `$/cancel_request`、`session/update`、`session/request_permission`（一次性允许/拒绝选项）。
- 不支持：`session/load`、mode、fork/删除、附加目录、transcript 回放、终端、计划、elicitation、客户端文件系统操作。
- 一个连接可运行多个相互独立的会话。
- 模型/提供方选项来自 dsh 实时 LLM 服务目录；不要照抄旧模型名，失败不静默切换。
- `session/prompt` 在 agent 空闲且有序更新交付后才结算。

## 4. 架构决策（已与用户确认）

**方案 A：单 dsh ACP 子进程 + 内存任务注册表。**

- 桥接进程懒启动一个 `dsh --profile acp` 子进程，一条 ACP 连接复用多个会话。
- 任务元数据在桥接进程内存（HashMap），MCP 工具只做内存查询，dsh 执行期间 MCP 正常响应。
- 桥接重启丢任务元数据（dsh 侧会话仍持久化；第一版不做映射重建，README 记录此限制）。

被否决方案：每任务一个子进程（开销大、与会话复用模型不匹配）；headless 一次性进程（HANDOFF 明确排除）。

## 5. 模块划分

```text
Cargo.toml
src/
  main.rs    入口：配置加载、tracing 初始化（只写 stderr）、启动 MCP stdio server
  config.rs  配置结构与来源（env 优先，默认值兜底）
  acp.rs     ACP 连接封装：spawn、initialize、session 各方法、update/permission 回调分发
  task.rs    任务注册表与状态机（内存）
  server.rs  rmcp ServerHandler，5 个 MCP 工具
tests/       集成测试
```

## 6. 依赖

| crate | 版本 | features / 说明 |
|---|---|---|
| rmcp | 3.5.1 | `server`, `macros`, `transport-io` |
| agent-client-protocol | 3.4.0 | 默认（v1 stable）；**不开** `unstable*`（v2 草案等） |
| tokio | 1 | `rt-multi-thread`, `macros`, `process`, `io-util`, `sync`, `time` |
| serde / serde_json | 1 | 数据结构 |
| tracing / tracing-subscriber | 0.1 / 0.3 | 日志只写 stderr |
| uuid | 1 | task_id / permission_id 生成 |

精确 API 以实现时核对的 crate 源码/docs.rs 为准；版本号在 Cargo.toml 中用兼容语义化版本声明（如 `rmcp = "3.5"`），由 Cargo.lock 锁定。

## 7. MCP 工具契约

### `start_task(cwd: string, prompt: string) -> { task_id: string }`

- 校验 `cwd` 为已存在的绝对目录，否则返回参数错误。
- 新建 dsh 会话（`session/new`），提交初始 prompt（`session/prompt`），后台执行。
- 立即返回 `task_id`（uuid）。

### `get_task(task_id: string, cursor?: integer, limit?: integer) -> TaskView`

返回：

- `task_id`、`status`、`cwd`、`created_at`、`parent_task_id`（追问任务）、`dsh_session_id`
- `events`：自 `cursor` 起的增量事件与下一光标；`limit` 与单事件大小有上限（默认 100 条 / 事件正文截断 4000 字符，可配）
- `pending_permission`：状态为 `waiting_permission` 时的权限请求详情（permission_id、工具调用信息、可选选项列表）
- 终态时含 `result`（最终文本，超限截断）或 `error`
- 未知 task_id 返回错误

### `send_followup(task_id: string, prompt: string) -> { task_id: string }`

- 复用目标任务所在的 dsh 会话发起追问；返回**新** task_id，`parent_task_id` 指回原任务。
- 该会话有活跃任务时返回明确错误（会话忙），由调用方稍后重试；同会话任务严格串行。

### `cancel_task(task_id: string) -> { status: string }`

- 先 `session/cancel`（ACP），有界等待（默认 30s，可配）进入 `cancelled`。
- 超时升级 `session/close` 释放该会话；再超时（进程无响应）才杀 dsh 子进程（会影响该进程上其他会话，记录事件）。
- 不采取"只结束桥接层 future"的假取消。

### `resolve_permission(task_id: string, permission_id: string, option_id: string) -> { status: string }`

- 裁决待决权限请求，将选中的 option_id 通过 ACP 返回给 dsh。
- option_id 必须属于该请求公布的选项，否则参数错误。

## 8. 任务状态机与权限流

状态：`running` → `waiting_permission` → `running` → `completed` / `failed` / `cancelled`。`cancel_task` 在 `running` 和 `waiting_permission` 下均可发起，迁移到 `cancelled`（待决权限随取消作废）。

- `running`：prompt 已准入，执行中。
- `waiting_permission`：有 ≥1 个待决 `session/request_permission`；裁决后回到 `running`。待决超过 `permission_timeout`（默认 600s，可配）自动选择该请求的**拒绝类选项**，记录 `permission_auto_denied` 事件，状态回到 `running`（任务后续成败由 dsh 决定）。永不永久挂起。
- `completed`：`session/prompt` 正常结算（stopReason 正常结束），保存最终文本。
- `failed`：协议错误、agent 失败、输出失败、子进程崩溃等，保存错误信息。
- `cancelled`：经 `cancel_task` 或会话关闭取消。

事件：每任务环形缓冲（默认 200 条，可配），类型映射自 `session/update`（assistant 消息、thought、工具生命周期、配置变化、上下文用量）及桥接层自身事件（状态迁移、权限自动拒绝等）。事件带单调递增序号作为光标。

映射规则：task_id ≠ dsh session_id；一个任务对应一个 dsh 会话上的一轮 prompt；追问任务共享父任务的 dsh session_id。每会话同时最多一个活跃任务（桥接侧互斥，与 dsh 限制一致）。最大并发会话数默认 4（可配），超限 `start_task` 报错。

## 9. 配置

环境变量（均有默认值）：

| 变量 | 默认 | 说明 |
|---|---|---|
| `DSH_MCP_DSH_COMMAND` | `dsh` | dsh 启动命令（本机验证时用绝对 node + bin.js 路径） |
| `DSH_MCP_DSH_ARGS` | `--profile acp` | 空格分隔的参数 |
| `DSH_MCP_PERMISSION_TIMEOUT_SECS` | `600` | 权限待决自动拒绝超时 |
| `DSH_MCP_CANCEL_TIMEOUT_SECS` | `30` | 取消升级阈值 |
| `DSH_MCP_EVENT_BUFFER_SIZE` | `200` | 每任务事件环形缓冲容量 |
| `DSH_MCP_MAX_SESSIONS` | `4` | 最大并发 dsh 会话数 |

桥接不读取、不复制 `~/.dsh/.credentials.yaml`；凭据完全由 dsh 进程自己管理。

## 10. 错误处理

- dsh 子进程崩溃/stdio 断开：全部活跃任务置 `failed`（原因记录为进程退出）；下一次 `start_task` 懒重启子进程并重新 initialize。
- ACP JSON-RPC 错误响应：对应任务置 `failed`，保留原始 error message。
- initialize 能力协商失败：`start_task` 返回错误，不静默重试退化协议版本。
- 权限裁决到达时任务已终止：返回明确错误。

## 11. 测试与验收计划

单元测试（mock ACP 层）：

- 任务状态机迁移（含 waiting_permission 往返、超时自动拒绝）
- 事件环形缓冲与光标分页（含截断）
- 会话串行互斥（忙会话拒绝追问）

集成测试 1（控制面，不调模型，CI 可跑）：临时 `DSH_HOME`，真 dsh 子进程，`initialize` → `session/new`（/tmp 临时目录）→ `session/close`。

集成测试 2（端到端，已获用户授权，默认 `DSH_HOME` 用真实凭据，/tmp 新建临时工作区，会消耗 API 额度）：

1. `start_task` 派发一个小的真实写文件任务 → 轮询 `get_task` 到 `completed` → 核对文件确实生成
2. `send_followup` 返工（修改该文件）→ 完成并核对
3. `cancel_task`：启动长任务后取消，确认状态 `cancelled` 且 dsh 不再写文件
4. 权限路径：若任务触发 `session/request_permission`，验证 `waiting_permission` → `resolve_permission` 放行 → 继续；若 dsh 默认 preset 不触发权限请求，如实记录该路径未做真实覆盖（单元测试仍覆盖状态机）
5. 测试结束 `session/close` 清理；临时工作区删除

验证命令：`cargo build`、`cargo test`、`cargo clippy -- -D warnings`。

交付物附 Codex `mcp_servers` 配置示例（README 中的文本，不修改用户全局配置）。

## 12. 明确不做（第一版）

- 跨桥接进程的任务恢复与 task↔session 映射重建
- `session/set_config_option` 的 MCP 暴露（模型选择）
- 并发会话数 > `DSH_MCP_MAX_SESSIONS` 的排队机制
- MCP resource/prompt 能力、Streamable HTTP 传输
- 远程仓库创建与推送（用户未授权）

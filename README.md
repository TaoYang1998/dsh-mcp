# dsh-mcp

用 Rust 编写的 MCP stdio 服务，通过 ACP v1 调度 DeepSeek Harness（dsh）。Codex 负责整理需求、派发和验收；dsh 在指定工作目录执行任务。当前面向 Linux 本机运行。

任务派发立即返回 ID，查询、取消和权限裁决可以在模型执行期间继续使用。一个 dsh 连接承载多个独立会话，每个会话同时只运行一轮 prompt。dsh 按需启动；桥接崩溃后不会自动重放任务。

## 构建与启动

```bash
cargo build --release --locked
./target/release/dsh-mcp --help
```

需要 Rust 1.95.0、Node 24.14.0，以及 `@deepseek-ai/dsh@0.2.0-rc.2`。这些版本已固定在 CI 配置中；实际验收结果应以执行日志为准。dsh 可以按其官方安装方式安装，例如：

```bash
npm install --global @deepseek-ai/dsh@0.2.0-rc.2
```

默认子进程命令为 `dsh --profile acp`。部署环境应先配置好 dsh 的模型、认证和网络。桥接继承环境，由 dsh 管理凭据；桥接不会读取凭据文件。stdout 仅承载 MCP，日志与 dsh stderr 写到 stderr。

命令与参数分别配置，参数中的空格、引号和 shell 字符按原值传给子进程：

```bash
DSH_MCP_DSH_COMMAND=/absolute/path/to/node \
DSH_MCP_DSH_ARGS_JSON='["/absolute/path/to/dsh/lib/bin.js","--profile","acp"]' \
DSH_MCP_STATE_DIR=/absolute/path/to/private/dsh-mcp-state \
./target/release/dsh-mcp
```

以上路径都是需要替换的部署示例。桥接不经 shell 解析命令，也不会自动拆分参数字符串；代理地址按部署环境设置。

## 接入 Codex

按 [OpenAI 官方 Codex MCP 文档](https://developers.openai.com/codex/mcp/)（2026-10-19 核对），stdio 服务使用 `[mcp_servers.<name>]` 配置 `command`、`args` 和 `env`。可将以下示例按实际路径调整后放入 Codex 的 `~/.codex/config.toml`，或受信任项目的 `.codex/config.toml`：

```toml
[mcp_servers.dsh]
command = "/absolute/path/to/dsh-mcp/target/release/dsh-mcp"
args = []
startup_timeout_sec = 10
tool_timeout_sec = 30
# 按部署需要转发；凭据仍由 dsh 管理。
env_vars = ["DSH_HOME", "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY"]

[mcp_servers.dsh.env]
DSH_MCP_DSH_COMMAND = "/absolute/path/to/node"
DSH_MCP_DSH_ARGS_JSON = '["/absolute/path/to/dsh/lib/bin.js","--profile","acp"]'
DSH_MCP_STATE_DIR = "/absolute/path/to/private/dsh-mcp-state"
```

官方文档中的 `tool_timeout_sec` 限制一次 MCP 调用；模型任务在后台运行，持续时间不受此值限制。本仓库只提供配置示例，不修改本机 Codex 配置。

可在项目指令中约定：

> Codex 负责分析、任务拆分、派发与验收；将具体修改和测试交给 dsh MCP。每次新意图使用新的 request_id，重试沿用原 ID。派发后持续查询；遇到权限请求只选择公布的原始选项。追问使用会话最新 task_id，完成后核对实际 diff 和测试，再显式关闭会话。取消后等待终态和停止证据，不能把 cancelling 当作已停止。

这是一项协作约定。cwd 不构成文件系统沙箱；MCP 接入也不保证 dsh 显示在 Codex 原生子代理面板中。

## 共享运行与恢复

新增 `--daemon`（常驻共享协调器）和 `--proxy`（每个 Codex 的轻量 stdio 接入），无参数仍为原独立 stdio 模式。现有生产入口保持原状；部署候选、连接归属、11 个工具与故障恢复步骤见 [共享服务运行说明](docs/shared-daemon.md)。

新增工具：`list_sessions`、`resume_task`、`list_tasks`、`attach_session`、`get_status`。旧六工具参数不变。上下文恢复不回放旧桥接 task ID；共享不减少独立模型上下文的 token。

## 任务工具

| 工具 | 参数 | 用途 |
|---|---|---|
| `start_task` | `request_id`, `cwd`, `prompt` | 在存在的绝对目录创建任务和会话，立即返回任务 ID |
| `get_task` | `task_id`, 可选 `cursor`, `limit` | 查询状态、增量事件、结果、会话和完整待决权限快照 |
| `send_followup` | `request_id`, `task_id`, `prompt` | 在同一会话继续执行；必须使用最新轮次的任务 ID |
| `cancel_task` | `task_id` | 请求有界取消；继续查询直到终态 |
| `resolve_permission` | `task_id`, `permission_id`, `option_id` | 选择该权限请求公布的原始选项 |
| `close_session` | `task_id` | 关闭该任务所属的闲置会话，释放额度和上下文 |

典型参数依次为：

```json
{"request_id":"work-001","cwd":"/absolute/project","prompt":"实现需求并运行相关测试"}
{"task_id":"返回的任务ID","cursor":0,"limit":50}
{"request_id":"work-002","task_id":"最新任务ID","prompt":"根据验收结果修正问题"}
{"task_id":"活动任务ID"}
{"task_id":"任务ID","permission_id":"待决权限ID","option_id":"公布的原始选项ID"}
{"task_id":"已结算任务ID"}
```

上述六行分别对应表格中的六个原有工具，并非一份 JSON-RPC 批量请求。实际工作流按任务状态选择调用：正常完成后可以追问或关闭；有活动任务时先取消并等待结算，再关闭。

`start_task`、`send_followup` 和 `resume_task` 共用 request_id 去重范围。同一 ID、同一原始参数返回同一任务；不同参数返回 `request_conflict`。任务记录过期后不再保证该 ID 去重。prompt 最多 64 KiB，request_id 最多 128 UTF-8 字节。

`get_task` 的 `status` 为 `starting`、`running`、`waiting_permission`、`cancelling`、`completed`、`cancelled` 或 `failed`。`completed` 只表示本轮执行已结束，需求是否完成仍需验收文件和测试。`execution_state` 区分 `not_started`、`in_progress`、`stopped`、`unknown`；未知执行状态不会被报告成取消成功。

权限等待不会默认放行。查询结果中的 `pending_permissions` 独立于事件分页，包含完整权限 ID、原始选项和截止时间。`resolve_permission` 返回的 `delivery=pending/sent/failed` 表示裁决送达状态，不能证明对应操作已经执行。同一选项重试幂等，改选会报错；超时拒绝或取消。

查询时将上次 `next_cursor` 作为新 cursor；`has_more=true` 时继续分页。事件会被淘汰，`gap`、`dropped_count` 和 `earliest_cursor` 如实反映缺失。最终 assistant 文本独立汇总，事件淘汰不删除结果。业务错误通过 `isError=true` 和结构化 `{code,message,retryable,details}` 返回，例如 `capacity_exceeded`、`stale_task`、`session_closed`、`backend_unavailable` 和 `recovery_required`。

## 配置与容量

环境变量优先；无效值在启动时返回 `configuration_error`。容量和秒数要求正整数，超出容量或单调时钟可表示范围的值会被拒绝。`MAX_ACTIVE_TASKS <= MAX_OPEN_SESSIONS`，`MAX_TASK_RECORDS >= MAX_OPEN_SESSIONS + MAX_ACTIVE_TASKS`。

| 环境变量 | 默认值 |
|---|---|
| `DSH_MCP_DSH_COMMAND` | `dsh` |
| `DSH_MCP_DSH_ARGS_JSON` | `["--profile","acp"]` |
| `DSH_MCP_STATE_DIR` | `$XDG_STATE_HOME/dsh-mcp`；XDG 为空或未设置时为 `$HOME/.local/state/dsh-mcp` |
| `DSH_MCP_SOCKET` | 共享模式必须显式配置绝对路径 |
| `DSH_MCP_MAX_CLIENTS` | `32`（包含未完成握手的连接） |
| `DSH_MCP_MAX_ACTIVE_TASKS` | `4` |
| `DSH_MCP_MAX_OPEN_SESSIONS` | `16` |
| `DSH_MCP_SESSION_IDLE_TTL_SECS` | `1800` |
| `DSH_MCP_MAX_TASK_RECORDS` | `256` |
| `DSH_MCP_TASK_RETENTION_SECS` | `86400` |
| `DSH_MCP_START_TIMEOUT_SECS` | `30` |
| `DSH_MCP_PERMISSION_TIMEOUT_SECS` | `600` |
| `DSH_MCP_CANCEL_TIMEOUT_SECS` | `30` |
| `DSH_MCP_CLOSE_TIMEOUT_SECS` | `10` |
| `DSH_MCP_SHUTDOWN_TIMEOUT_SECS` | `50` |

未关闭的会话占用会话额度；读任务不刷新闲置计时。TTL 或显式关闭释放会话额度。活动任务、待决权限、尚可追问会话的最新任务等记录不会为新任务随意淘汰；容量用尽时立即拒绝新任务，不排队。

事件默认保留最近 200 条且不超过 1 MiB，单条事件最多 8 KiB。TaskView 的序列化预算为 256 KiB，完整 MCP 工具响应上限为 320 KiB。桥接任务、prompt、事件和 request_id 仅保存在内存，桥接重启后不恢复旧任务或事件；DSH 自身持久化的非活跃模型会话可通过 `list_sessions`/`resume_task` 显式恢复为新桥接任务。

## 关停与人工恢复

每个独立 stdio 实例必须使用独立状态目录；共享模式仅 daemon 持有一个稳定状态目录，各 proxy 不持锁。目录权限为 0700、文件权限为 0600。启动提供工具前取得独占 OS 文件锁；同目录第二个实例返回 `instance_in_use`。锁描述符不会传给 dsh。

启动 dsh 之前持久化 `run-state.json`，只记录实例 ID、时间、阶段和子进程 PID。只有确认所有会话关闭、子进程退出且没有未知清理结果后才删除标记。MCP stdin EOF、SIGINT 和 SIGTERM 都触发有界关停；清理未确认时保留标记并以非零码退出。

发现旧标记时仍可发现 MCP 工具，但新派发会返回 `recovery_required`，不会启动 dsh。人工恢复步骤：

1. 停止使用该状态目录的桥接实例。
2. 检查并清理遗留执行。PID 已消失不能证明后代执行已经结束；桥接不会按进程名批量杀用户进程。
3. 使用同一状态目录运行本地命令：

   ```bash
   DSH_MCP_STATE_DIR=/absolute/path/to/private/dsh-mcp-state \
     /absolute/path/to/dsh-mcp --acknowledge-cleanup
   ```

4. 重新启动桥接。

确认命令必须取得同一独占锁；它只清除标记，不启动或杀进程，也不作为 MCP 工具提供。异常执行不会自动重新派发。退出码为：正常且清理完成 `0`；运行后的清理未确认 `1`；配置、命令参数、锁或人工确认命令失败 `2`。

## 验证

本次实现的实际结果及未覆盖项见[验证记录](docs/validation.md)。

确定性测试不调用模型：

```bash
cargo fmt --all -- --check
cargo build --locked
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
```

真实 dsh 控制面测试使用独立 `DSH_HOME`、状态目录和 cwd，只执行 initialize → session/new → session/close → 子进程退出。没有发送模型 prompt：

```bash
DSH_MCP_REAL_DSH=1 \
DSH_MCP_TEST_NODE=/absolute/path/to/node \
DSH_MCP_TEST_DSH_CLI=/absolute/path/to/dsh/lib/bin.js \
cargo test --locked --test control_plane -- --nocapture
```

未安装夹具时本地测试可以显式跳过；`DSH_MCP_REAL_DSH=1` 要求夹具存在，缺失会失败。CI 安装固定版本并强制运行这一组。

真实模型验收会消耗部署环境的模型额度。以下脚本使用 Python 标准库，经真实 MCP stdio 调用六工具，验证临时目录写入、request_id 去重、同会话追问、两个目录并发、关闭与关闭后拒绝追问：

```bash
cargo build --locked
python3 scripts/e2e_real_model.py \
  --bridge target/debug/dsh-mcp \
  --permission-policy allow-once \
  --test-cancel \
  --report /tmp/dsh-mcp-real-model-report.json
```

脚本默认权限策略为 `deny`；显式选择 `allow-once` 才会从公布的选项中选一次性允许，不选择永久允许。`--test-cancel` 增加最多 30 秒的前台写文件工作负载，取消后采样验证文件不再变化；该证据仅针对本次工作负载。实际模型未触发权限请求时，报告会说明真实权限路径未覆盖；未知权限 ID 的错误检查不能代替真实授权回路验收。

脚本为 dsh 创建临时 `DSH_HOME`，不读取或复制用户凭据文件；dsh 使用部署环境提供的认证和配置。如果临时环境没有可用的模型提供方或凭据，验收会实际失败并保留诊断，不修改用户现有 dsh 历史。只有会话关闭、桥接正常关停且运行标记清除均得到确认后，才自动删除成功运行的临时目录；失败或未知状态保留目录和报告，供诊断及人工清理。`--keep` 可保留成功的测试目录。实际运行结果应分别记录确定性测试、真实控制面和真实模型，不能把未执行的测试写成通过。

设计与验收边界见 [设计文档](docs/superpowers/specs/2026-10-19-dsh-mcp-design.md)。许可证为 [Apache-2.0](LICENSE)。

# 共享 DSH ACP 的运行约定

本次交付包含源码、隔离验证和部署候选；2026-10-20 已追加通过实际 Codex 0.162.1 与真实 DSH 的完整链路验收。没有更改现有 launcher、Codex 配置、模型配置或 Web 服务，没有安装或启动生产 daemon。

## 接入与归属

一个 `dsh-mcp --daemon` 持有一个协调器和按需启动的 ACP；每个 Codex 使用独立 `dsh-mcp --proxy` 连接 Unix socket。代理只转发字节，stdout 只承载 MCP，不持有运行锁，不自动重连、重放或退回独立模式。无参数的原独立 stdio 模式继续可用。

每连接由 daemon 分配随机身份。写操作在协调器核对当前归属；断线后任务继续运行，session 变为 unowned。新连接执行 `list_tasks(scope="unowned")`，选择目标后 `attach_session(session_id)`；接管不新建模型轮次、不放行权限、不刷新 TTL。其他活连接持有的 session 不能抢占。同 UID 客户端可以读取其他任务，归属用于防误操作，不隔离同账户恶意进程。

## 工具

保留 `start_task`、`get_task`、`send_followup`、`cancel_task`、`resolve_permission`、`close_session` 的原参数，新增：

| 工具 | 用途 |
|---|---|
| `list_sessions(cwd?, cursor?)` | ACP 历史列举，保留不透明 cursor；不创建任务 |
| `resume_task(request_id, dsh_session_id, cwd, prompt)` | 恢复非活跃历史并提交新一轮；产生新的桥接 task/session ID |
| `list_tasks(cwd?, scope?, cursor?, limit?)` | 查询当前 daemon 尚保留的任务；默认 mine、每页 50，最大 100 |
| `attach_session(session_id)` | 认领 unowned 的存活会话，运行中或等待权限时也允许 |
| `get_status()` | 不启动 ACP 的只读健康查询，包含状态、恢复保护及容量使用 |

恢复失败不会创建新上下文。跨 ACP 进程或 Web 的占用检测未获得保证，只恢复已确认非活跃历史；列举结果不代表外部进程未占用。关闭但仍保留记录的历史可以恢复；已经 lost 的会话需先核对后端健康。

## 时间与资源限制

- 全局最多 4 个活动任务、16 个未关闭会话，不排队；权限等待和取消期间仍占活动额度。
- 会话空闲 1800 秒关闭，查询或接管不延长 TTL。
- 桥接最多 256 条任务记录，可回收终态记录默认保留 86400 秒；容量压力可提前淘汰。活动任务与未关闭会话的最新任务不淘汰。
- 去重记录随任务一起淘汰，跨 daemon 重启也不保留。相同 request_id 只在原记录保留期间保证去重；不能自动重发结果未知的写请求。
- `DSH_MCP_MAX_CLIENTS` 默认 32，包含未完成握手的连接；握手期限 10 秒。
- 每条 MCP 输入在 JSON 解析前限制至 1 MiB，每连接最多 16 个未完成请求和 16 个待发送响应；编码响应沿用 320 KiB 上限，写期限 10 秒。超限只断开对应连接，已接纳任务继续运行。
- 历史列举全局最多 32 个在途请求，并受后端控制调用期限约束；客户端取消等待后仍占额度，直到后台调用完成，防止通过反复取消绕过资源上限。
- socket 父目录必须由当前账户持有、权限 0700，socket 权限 0600；仅接受同 UID。路径不安全或锁冲突时拒绝启动，不移除活 socket；仅在持有路径锁且连接明确被拒绝时回收遗留 socket。

`DSH_MCP_TASK_RETENTION_SECS`、`DSH_MCP_SESSION_IDLE_TTL_SECS` 与原容量配置仍可设置。本次没有更改 24 小时记录保留或 30 分钟会话 TTL 的默认值。

## 故障恢复

MCP initialize/ping 成功仅代表传输可用。先查看 `get_status().backend`；unavailable 或 recovery_required 时不能继续派单、追加或恢复。一次会话关闭失败也可能隔离整个 ACP，其他任务会失去上下文归属或执行停止证据。

1. 保留任务快照与错误证据，停止接纳新工作，正常停止 daemon。
2. 核对 ACP 及执行后代的实际清理结果；父 PID 消失不能单独证明后代停止。
3. 仅在确认清理后，用相同 `DSH_MCP_STATE_DIR` 执行 `dsh-mcp --acknowledge-cleanup`。该命令只清标记，不负责杀进程。
4. 重启 daemon，确认不再处于恢复保护且允许接纳执行，再通过 list/resume 恢复非活跃历史。

不自动清标记、重放 prompt 或更换 ACP。旧 task ID、request_id、事件 cursor 不跨重启恢复。客户端断线期间的待决权限仍按 600 秒期限处理，只能选择原始 option_id。

## 部署候选

[用户 systemd unit](../deploy/systemd/dsh-acp-mcp.service) 使用版本化二进制路径，`TimeoutStopSec=70s`，覆盖原 50 秒桥接关停预算；不自动重启。将 [环境模板](../deploy/daemon.env.example) 的命令和参数替换为现有 launcher 使用的绝对路径，凭据继续来自受保护配置；systemd 不继承交互 shell 的环境。模板含占位路径，必须填妥才能安装。

新 Codex 配置候选，路径按安装用户展开：

```toml
[mcp_servers.dsh_acp]
command = "/absolute/versioned/path/dsh-mcp"
args = ["--proxy"]
startup_timeout_sec = 10

[mcp_servers.dsh_acp.env]
DSH_MCP_SOCKET = "/run/user/USER_UID/dsh-mcp/acp.sock"
```

实际安装前备份当前配置与 launcher，处理正在运行的任务，验证模型 profile、认证来源和版本化路径。隔离 Codex 0.162.1 已完成原生工具发现和调用验收；生产切换后仍需核对实际进程与配置。本次保留生产入口，因此生产切换步骤未执行。

回退前先处理共享任务、正常停止并确认清理，再恢复旧 launcher 配置；若恢复保护仍在，先按上述步骤核对，不能自动清除。

## 验证入口

```bash
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo build --release --locked
python3 scripts/verify-shared-daemon.py --binary target/debug/dsh-mcp
python3 scripts/verify-recovery.py --binary target/debug/dsh-mcp
```

以上两个脚本仅调用确定性 ACP fixture，不调用模型；临时目录和进程均由脚本拥有。沙箱若限制 Unix socket 或信号，需在获准的环境运行同一测试，不能据沙箱失败推断生产服务失败。

2026-10-20 按用户后续要求补充真实验收：实际 Codex app-server 发现 11 个工具并完成原生调用；新共享版本使用现有 DSH profile 完成实际文件写入、同会话回忆与关闭首个 ACP 后的历史恢复回忆，均精确匹配，最终清理已确认。生产入口未切换。真实流程未触发权限请求，该层仍未覆盖。详细证据见 [验证记录](validation.md)。

可重复的原生接入验收（以下 Codex 路径需替换）：

```bash
python3 scripts/verify-codex-native.py --binary target/release/dsh-mcp --codex /absolute/path/to/codex --report /tmp/dsh-native-report.json
# 下一条显式调用已配置的真实 DSH 模型，使用临时目录与新建会话。
python3 scripts/verify-codex-real.py --binary target/release/dsh-mcp --codex /absolute/path/to/codex --report /tmp/dsh-real-report.json
```

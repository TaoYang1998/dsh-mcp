# dsh-mcp 第一版设计规格

日期：2026-10-19。修订：2。状态：按用户要求重新设计，待实现与联调。

本文替代同路径旧稿。项目目标和技术方向沿用已有讨论；本次重新确定接口、状态模型和异常处理，不沿用旧稿的“设计已经用户确认”状态。`HANDOFF.md` 保留历史背景，当前实现约定以本文为准。

## 1. 目标、边界与成功标准

实现一个 Rust MCP 服务，让 Codex 把具体编码任务交给本机 DeepSeek Harness（dsh），保留同一上下文进行返工，并获取足够的执行证据完成验收。

职责分工：

- Codex：分析需求、拆分任务、明确工作目录与文件责任、派发、审查 diff 和测试结果、决定是否返工。
- dsh：读取项目、修改文件、执行命令和测试、报告结果。
- dsh-mcp：任务准入、会话生命周期、协议转换、事件与结果保存、权限请求转交、取消和退出管理。

MCP 接入提供外部执行工具；Codex 的工作分工由项目指令约定。第一版不承诺在 Codex 原生子代理面板显示 dsh，也不强制限制 Codex 自身的写权限。

第一版支持 Linux 本机、MCP stdio、ACP v1、文本任务、多个会话并发、同会话串行返工。成功标准为：正常任务与返工能落地文件；查询始终可用；重试不重复派发；取消结果真实反映停止证据；资源有界；故障后不会自动继续使用失效会话。

暂不实现 HTTP、远程执行、多后端通用调度、自动 worktree、模型切换、任务排队、跨桥接进程任务恢复、旧会话自动 resume、ACP 草案扩展。工作区隔离由 Codex 安排；不同会话指向同一目录时，桥接不提供文件写入隔离。

## 2. 接口依据与证据边界

以下是已读本机文档、源码和发布包得到的约束，不代表已完成端到端联调：

| 项目 | 依据与约束 |
|---|---|
| dsh | 本机 `@deepseek-ai/dsh` 为 `0.2.0-rc.2`；启动入口 `dsh --profile acp` |
| ACP | stdio JSON-RPC、v1；支持 initialize、session/new、session/prompt、session/cancel、session/close、session/resume、session/update、session/request_permission |
| 同会话并发 | 同一会话只能有一个未结算 prompt；多个会话可以独立执行 |
| prompt 结算 | 本机实现等待 agent 空闲和有序更新排空后结算；取消通知发送成功本身不代表执行已停止 |
| 会话关闭 | 本机实现取消任务、排空更新、处理后代、flush 并释放会话；关闭失败也可能移除服务端会话引用，不能假定失败后仍可复用 |
| 不支持项 | 不使用 session/load、mode、fork、transcript 回放、客户端文件系统及终端能力 |
| 权限 | 本机请求提供一次性允许/拒绝选项；请求可能只有 toolCallId，详情需要结合此前工具事件 |
| 结束原因 | 本机可能将不同内部结束情形映射成 end_turn；该值不等于用户需求已完成 |
| 子进程 | 本机 Linux 执行后端存在 detached 的 systemd scope 启动路径；杀掉 dsh 主进程不足以证明所有执行进程都已停止 |
| Rust SDK | 前序核验过已发布的 rmcp 3.5.1、agent-client-protocol 3.4.0；实现以发布包 API 和锁文件为准 |

本机包目录为 `/home/agent_coding/.npm/_npx/b86ed90107c62dab/node_modules/@deepseek-ai/`。主要依据：`dsh-acp/README.zh.md`、`dsh-acp/lib/index.js`、`dsh-subprocess-local/lib/index.js`。上述路径用于本机复核，不进入产品默认配置。

## 3. 架构选择

选择 **单 dsh ACP 子进程 + 明确的会话管理 + 内存任务注册表**。

| 方案 | 取舍 |
|---|---|
| 单进程、多会话（采用） | 保留会话上下文，启动和连接管理集中；进程故障会影响所有会话，必须整体隔离故障 |
| 每会话一个 dsh 进程 | 可缩小 ACP 进程故障范围，但增加启动与监督成本；仍不能单靠杀父进程保证执行后代停止 |
| headless 一次性进程 | 适合小型验证；当前需要持续事件、权限裁决和会话控制，第一版继续采用 ACP |

```text
Codex
  │ MCP stdio，6 个任务工具
  ▼
MCP server ──► Coordinator（任务、会话、权限、额度、事件）
                       │ 命令 / 有序事件
                       ▼
                  ACP adapter
                       │ ACP v1 stdio
                       ▼
                 一个 dsh 进程
                  ├─ session A：多轮 prompt 串行
                  └─ session B：与 A 并发

Supervisor：启动、退出观察、超时、恢复保护标记
```

只有 Coordinator 修改业务状态。采用 actor 或等价的串行状态处理；ACP I/O、进程等待、文件同步在独立异步任务中执行，禁止持有全局状态锁等待它们。MCP 查询读快照；长操作提交后立即返回，调用方通过查询观察进展。

ACP reader 必须持续消费消息。权限回调注册待决项后等待独立 responder，不能阻塞后续消息读取。单个 ACP JSON 帧最多 4 MiB，在 JSON 解析前限制读取大小；入站队列最多 128 项且编码内容总计不超过 8 MiB。超过单帧限制或队列持续拥塞 10 秒按连接故障处理，不能为腾空间静默丢弃权限、终态或连接故障。事件保留缓冲与协议接收队列分离。

## 4. 领域对象与不变量

### 4.1 对象

| 对象 | 关键字段 |
|---|---|
| Backend | `state`、`epoch`、child handle、退出信息；state 为 dormant / starting / ready / draining / unavailable |
| Session | 桥接生成的 `session_id`、可空 `dsh_session_id`、`epoch`、规范化 cwd、state、active_task_id、latest_task_id、last_activity_at |
| Task | task_id、request_id、session_id、parent_task_id、status、时间戳、stop_reason、result、error、execution_state、事件缓冲 |
| Permission | permission_id、task_id、session_id、epoch、ACP responder、工具摘要、原始 options、deadline、裁决状态 |

桥接 session_id 与 dsh session_id 分离。公开 ID 使用 UUID；ACP session_id 在 session/new 成功前为 null。超时使用单调时钟，展示时间使用 UTC RFC 3339。

Session 状态：`opening → idle ↔ busy → closing → closed`；连接丢失或会话操作结果不可确认时进入 `lost`。opening、busy、closing、lost 均不接受新 prompt。closed/lost 会话在本实例中不恢复，追问返回明确错误。

### 4.2 必须维持的不变量

1. 每个会话最多一个活跃任务，任务占位从准入开始，直到终态结算与本轮事件屏障完成。
2. 全局活跃任务数计入 starting、running、waiting_permission、cancelling；权限等待和取消期间不释放额度。
3. 所有异步回调携带 epoch、session_id 和 task_id；过期回调不能修改新任务或触发新任务的关闭。
4. 任务终态只结算一次。取消旧任务不能影响该会话后来的任务。
5. 只在取得明确的准入前取消或 ACP 停止证据后报告 cancelled；无法确认执行状态时报告 failed + execution_state=unknown。
6. 接口返回成功接单后，任务记录已存在。MCP 请求断连不取消已接单任务；只能通过 cancel_task 或桥接关停流程取消。
7. 任务历史与会话可用性分开：旧任务可以 completed，但其会话已经 closed/lost。

## 5. MCP 工具契约

共 6 个工具。工具输出使用结构化内容，并提供等价的简短文本回退。业务失败以 `isError=true` 和 `{code,message,retryable,details}` 返回；JSON-RPC 层错误仅用于协议/参数结构错误。错误信息有大小限制，不附带凭据或完整环境变量。

### 5.1 start_task

```text
start_task(request_id: string, cwd: string, prompt: string)
  → {task_id, session_id, status, deduplicated}
```

- request_id 由调用方生成，最长 128 字节，用于重试去重；prompt 必须为非空 UTF-8 文本，最多 64 KiB。
- cwd 必须为存在的绝对目录，按文件系统解析规范路径。创建会话前再次检查目录可用性；这不是目录访问沙箱。
- 先检查 request_id，再原子校验后端健康、任务额度、会话额度和记录容量。成功后预留额度、写入 starting 任务，立即返回。
- 后台懒启动 dsh、initialize、session/new，安装会话到任务的路由后发送 prompt。后续失败通过 get_task 返回；不会把已返回的 task_id 变成不存在。
- 相同 request_id 和相同参数返回同一任务及当前状态；相同 ID 配不同参数返回 request_conflict。去重范围覆盖 start_task 与 send_followup，保存期限同任务记录。记录过期后不保证去重；调用方每次新意图使用新 ID。
- 参数比较包含工具名、原始 cwd/prompt 或父 task_id，不通过摘要碰撞判断相等；接纳后另存规范化 cwd。
- 额度不足立即返回 capacity_exceeded；不排队。多个同时启动请求只能产生一个后端初始化流程。

### 5.2 get_task

```text
get_task(task_id: string, cursor?: integer, limit?: integer) → TaskView
```

TaskView 固定包括：

```text
task_id, session_id, parent_task_id, status, cwd
created_at, started_at?, finished_at?
execution_state: not_started | in_progress | stopped | unknown
session: {state, dsh_session_id?, latest_task_id, followup_available, unavailable_reason?}
backend: {state, recovery_required}
events, earliest_cursor, next_cursor, last_cursor, gap, dropped_count, has_more
pending_permissions: [{permission_id, tool_call_id, tool_summary,
                       details_incomplete, options, expires_at}]
result?: {text, truncated, total_bytes, source}
stop_reason?, error?, cancel_requested_at?
```

- cursor 为调用方最后收到的序号，返回 `seq > cursor`；省略等同 0。分页规则见第 9 节。
- pending_permissions 始终是完整的当前待决快照，不受事件分页影响；最多 16 个。
- completed 代表一次执行轮次已结束。需求是否完成仍由 Codex 核对实际文件与测试。
- failed/cancelled 也可返回部分 result，并明确标注其来源；查询不调用 ACP、不刷新会话闲置计时。
- 未知或已过期 task_id 返回 task_not_found，不无限保留墓碑来区分两者。

### 5.3 send_followup

```text
send_followup(request_id: string, task_id: string, prompt: string)
  → {task_id, session_id, status, deduplicated}
```

复用会话、创建新的任务；parent_task_id 指向传入任务。先处理请求去重，再检查目标必须为该会话最新任务、已终结、execution_state 不为 unknown，且会话 idle、后端 ready、全局额度充足。

旧祖先 task_id 返回 stale_task，并携带 latest_task_id；避免将“继续当前会话”误解为从旧轮次分叉。会话忙、关闭中、已关闭、失效分别返回 session_busy / session_closing / session_closed / session_lost。第一版不自动 resume，也不静默创建空上下文替代旧会话。

### 5.4 cancel_task

```text
cancel_task(task_id: string) → {task_id, status, execution_state}
```

原子记录取消意图，返回 cancelling；后台执行有界取消流程。重复调用不创建新的计时器或重复升级。对已终态任务返回当前结果，不更改其状态、不关闭后续任务使用的会话。

starting 阶段也允许取消。尚未发送 prompt 时禁止再发送；如 session/new 已发出，等待其结果并关闭新会话，无法确认结果则执行故障隔离。详细结算规则见第 7 节。

### 5.5 resolve_permission

```text
resolve_permission(task_id: string, permission_id: string, option_id: string)
  → {permission_id, decision, delivery, task_status}
```

只能选择该请求公布的原始 option_id。原子将 pending 改为 selected，然后异步回复 ACP；`delivery` 为 pending / sent / failed，工具返回不承诺 dsh 已实际执行。送达或失败写入任务事件；传输失败按后端故障处理。

同一请求重复提交同一选项返回已有裁决；改选返回 permission_already_resolved。超时或取消抢先发生时返回 permission_expired / permission_cancelled；未知 ID 返回 permission_not_found。已决记录最多保存最近 64 个，超出后返回 not_found。任务终止后不能创建新裁决，但保留记录上的相同重复请求仍可查询既有结果。

### 5.6 close_session

```text
close_session(task_id: string) → {session_id, state}
```

通过任一尚保留的任务定位会话，显式释放上下文。只允许 idle 会话，忙会话返回 session_busy，调用方先取消并等待结算；重复关闭返回 closing/closed。返回 closing 后后台调用 session/close，使用 get_task 的 session 字段查询结果。

关闭不修改历史任务终态；同会话的所有历史任务都显示 followup_available=false。关闭失败按第 8 节处理，不能当作关闭成功，也不能再次使用该会话。

## 6. 状态机与会话回收

### 6.1 任务状态

```text
starting ──► running ◄──► waiting_permission
    │           │                │
    └───────────┴────────────────┴──► cancelling

starting / running / waiting_permission / cancelling
                    └──► completed | failed | cancelled
```

| 状态 | 含义与结算 |
|---|---|
| starting | 已接单，正在启动后端、创建会话或准备发送 prompt；execution_state=not_started |
| running | prompt 已交给 ACP 连接，等待远端结算；不声称已收到单独的“准入成功”回执；execution_state=in_progress |
| waiting_permission | 有未裁决权限项；可能还有其他工具并行执行；execution_state=in_progress |
| cancelling | 取消/关闭流程尚未取得停止证据；保留当前 execution_state 和任务额度 |
| completed | prompt 返回 end_turn，轮次更新已处理；execution_state=stopped，保留原始 stop_reason |
| cancelled | prompt 返回 cancelled，或未提交 prompt 已撤销，或取消中的会话成功关闭；execution_state 为 stopped 或 not_started |
| failed | 启动、协议、执行失败或非正常 stop_reason；是否停止由 execution_state 单独表达 |

max_tokens、max_turn_requests、refusal 及未知 stop_reason 映射 failed，保留原值和部分输出，不自动重试。远端明确返回的 prompt 错误，在已验证 dsh 结算语义下可标 stopped；传输中断或请求结果未知不能这样推断。

权限变化只在非 cancelling 的执行任务间切换 running/waiting_permission。轮次完成与取消竞争时，以 Coordinator 首次处理的明确结算证据为准：取消意图并不覆盖已完成结果；即使已经 cancelling，收到正常完成也可结算 completed。若已开始 session/close，仍须等关闭完成后才释放会话使用权。

### 6.2 额度与保留策略

- `MAX_ACTIVE_TASKS=4`：包括 starting、等待权限和取消中的任务；start_task 和 send_followup 共用额度。
- `MAX_OPEN_SESSIONS=16`：包括 opening、idle、busy、closing；收到关闭成功才能释放。任务完成只释放任务额度。
- idle 会话 30 分钟未接纳新任务后自动 close。计时从本轮结算开始；查询不会延长。定时回收与新任务准入由 Coordinator 原子排序。
- 会话额度满时立即报 capacity_exceeded；调用方显式关闭闲置会话或等待 TTL。第一版不在接单过程中隐式淘汰另一个会话。
- 终态任务保留 24 小时，总记录上限 256。opening/idle/busy/closing 会话的最新任务固定保留；closed/lost 会话不再固定其任务。其余终态记录按完成时间淘汰。若没有可淘汰记录，拒绝接单。
- closed/lost 会话元数据随最后一条关联任务记录清除；权限裁决记录、事件、请求去重记录随所属任务清除。dsh 自身持久化历史的磁盘保留由 dsh 管理。

默认额度下，连续完成 5 个独立任务必须成功；完成 16 个且都保留上下文后，第 17 个才受会话额度限制。

## 7. 取消与停止证据

取消属于后台状态机，MCP 调用不等待 30 秒。每次升级必须再次核对 epoch、任务归属和是否已终结。

1. **撤销待决权限**：将所有尚待决 responder 以 ACP cancelled outcome 完成，停止权限计时器。取消之后到达的权限请求立即回复 cancelled。已先行选中的许可可能已送出，仍必须取消实际执行。
2. **请求取消**：对已发送 prompt 的会话发送 session/cancel，等待该 prompt 响应，默认 30 秒。通知发送成功不是停止证据。
3. **关闭会话**：没有取得结算证据则将会话置 closing，调用 session/close，再等 10 秒。成功关闭可作为该会话停止证据；该会话不再支持追问。
4. **后端隔离**：关闭超时/报错或 ACP 失联，禁止新接单；尽量关闭其他会话，执行有界的 dsh 退出流程（TERM 5 秒，再 KILL 2 秒），并 wait/reap 直接子进程。
5. **如实结算**：未取得停止证据的任务为 failed，error.code=execution_state_unknown，execution_state=unknown。其他会话只有取得各自的停止证据才能标 stopped；共用进程被杀不能作为该证据。

强制进程退出是回收桥接持有的直接子进程句柄的措施。第一版不提供跨 systemd scope 的强制清理保证，不通过进程名批量杀用户进程。所有 unknown 情形进入第 8 节的恢复保护，不能返回“取消成功”或自动重新派发。

文档中的 stopped 指本轮按已验证 ACP 生命周期结算；它不承诺撤销已完成的写入，也不承诺停止任务自行创建的外部服务。正常取消必须在临时工作区实测；强制取消必须测试 unknown 分支。

## 8. 后端故障、关停与恢复保护

### 8.1 连接与会话失效

子进程退出、stdio EOF、无法解析的协议流、会话关闭报错、会话操作超时造成结果不确定时，Coordinator 原子将 backend 置 unavailable，阻止 start/followup；当前 epoch 全部会话置 lost，随后按第 7 节有界清理仍可联系的会话和进程。历史任务终态保留；尚未发送 prompt 的活动任务失败并保持 not_started，其他任务依据停止证据结算 stopped 或 unknown。进程故障不触发任务重放。

明确的 session/new 错误响应只使对应 starting 任务失败，撤销该会话预留并释放额度；明确的 prompt 错误响应只影响对应任务，在已确认轮次结算后会话回 idle。认证/模型配置错误保留实际错误，不偷偷切换模型。session/new 超时不能等同于远端未创建会话，必须进入上述后端隔离流程。

第一版不在已经使用的后端崩溃后自动重启 dsh；因此也不会将旧 dsh_session_id 直接提交到新连接。恢复需人工确认后重启桥接，跨重启任务记录不恢复。epoch 仍用于拒绝旧回调和防止实现后续重连时串写状态。

首次启动在尚未发送 session/new/prompt 前失败，且已确认子进程退出，可以清除本次启动占位和运行标记并回到 dormant；受影响 starting 任务失败。下一次新派发允许重新启动，不重放失败任务。

### 8.2 持久化恢复保护标记

仅靠内存中的 unavailable 会被 MCP 客户端自动重启绕过，因此引入小型本地运行标记：

- 状态目录由 `DSH_MCP_STATE_DIR` 指定；默认 `$XDG_STATE_HOME/dsh-mcp`，未设时为 `~/.local/state/dsh-mcp`。目录 0700、文件 0600；不同实例需不同目录。
- 提供 MCP 工具之前取得目录内的独占 OS 文件锁并保持整个进程生命周期，锁 FD 不传给子进程。锁冲突直接报 instance_in_use 并退出。
- 首次启动 dsh **之前**，原子写入并同步 `run-state.json`，记录实例 ID、启动时间和生命周期阶段；启动后补充 child PID。记录中不保存 prompt、凭据或完整环境变量。
- 只有全部会话已确认关闭、子进程已退出且没有未知清理结果时才删除标记并同步目录；标记更新失败时拒绝开始新的执行。
- 启动时若存在旧标记，将 backend 置 unavailable，继续提供 MCP 工具发现，但新派发报 recovery_required，禁止启动 dsh。不能依据 PID 已消失就当作清理完成，因为执行后代可能仍存活。
- 提供本地命令 `dsh-mcp --acknowledge-cleanup`：人工先停止桥接实例，检查并清理遗留执行后运行，取得同一独占锁，删除标记并退出，再重新启动桥接。该命令不会杀进程或启动任务，也不提供给 Codex 作为自动 MCP 工具。

该标记保存恢复保护信息；任务、prompt、事件仍仅保存在内存。异常退出后需要人工恢复属于第一版明确的可用性取舍。

### 8.3 MCP 断开与进程退出

MCP stdin EOF、SIGINT、SIGTERM 进入 draining，停止接单，将待决权限回复 cancelled，取消活动任务并关闭全部会话。各会话并行清理，整个关停预算默认 50 秒，最后执行进程退出与 wait/reap；预算耗尽保留恢复保护标记并以非零退出码结束。

有界清理成功才删除标记。无法捕获的桥接 SIGKILL 会留下标记，下一次启动被保护阻止。不得依赖 Rust child handle 的 Drop 等同于完成后代清理。

## 9. 事件、结果与内存边界

### 9.1 有损事件、有界响应

每任务事件序号从 1 单调增加，记录 `{seq,time,type,data,truncated}`。保留最多 200 条且总计最多 1 MiB，先触及哪个限制就淘汰最早项。单事件保留最多 8 KiB 序列化内容；超限保留类型、关联 ID、裁剪后的摘要与原大小，不截断成无效 JSON/UTF-8。

get_task 默认 limit=50，允许 1..100；cursor 必须为非负整数且不大于 last_cursor。TaskView 的 JSON 序列化预算为 256 KiB：状态/标识/错误元数据最多 16 KiB，权限快照最多 64 KiB，结果对象最多 64 KiB，剩余空间用于事件及分页字段；错误摘要最多 4 KiB。预算计算包含 JSON 转义，所有文本裁剪保持 UTF-8 边界并标记截断，不能裁掉权限 ID 或选项。

先放完整状态、权限快照和结果，再按序加入能容纳的事件；必要时减少事件数，确保 has_more 准确。文本回退仅提供摘要和 ID，不再复制整份 TaskView，因此完整 MCP 工具响应的序列化硬上限为 320 KiB。对超长远端标识/元数据，无法保留完整路由键时按协议错误处理，不能截断 ID 后继续路由。

分页契约：

- earliest_cursor 是当前最早保留的事件序号；无事件时为 last_cursor+1。
- 若 cursor < earliest_cursor-1，gap=true，dropped_count=earliest_cursor-cursor-1，从最早可用事件开始返回。
- next_cursor 是本次最后返回事件的 seq；没有返回事件时取 max(cursor, earliest_cursor-1)。last_cursor 是快照时最新序号。
- has_more 表示本次 next_cursor 之后还有已保留事件。所有分页字段与事件来自同一快照。
- 第一次查询省略 cursor 也必须报告此前已覆盖的事件；不能把部分记录包装成完整历史。

### 9.2 结果独立汇总

result 从 agent_message_chunk 的文本独立累计，按本机 messageId 区分消息；默认取最后一条 assistant 消息的文本，source=last_assistant_message。无文本时返回空字符串和明确 source，不从 thought 或工具输出臆造结论。

最多保留最后一条消息前 64 KiB 文本，同时统计原始 UTF-8 字节数和 truncated；新 messageId 出现时重置当前消息聚合。查询时若 JSON 转义使结果对象超过 64 KiB，再按编码预算裁剪并置 truncated=true。失败和取消保存已有部分输出。事件被淘汰不影响结果汇总；不能靠最后 200 条事件重建最终文本。

收到不带 messageId 的消息更新时，不猜测消息边界，改为本轮 assistant 文本聚合，source=turn_text，保留同样的大小上限并记录兼容事件。实现须验证 SDK 是否保留本机扩展字段；不保留时使用该显式降级路径。

### 9.3 控制数据不依赖事件留存

待决权限存独立表，单任务最多 16 项；工具摘要按 tool_call_id 合并缓存，最多 128 项，每项 4 KiB，早期信息被淘汰则 details_incomplete=true。权限请求、响应与任务终态的归属不能从事件环形缓冲反查。

每个权限项序列化大小上限 4 KiB，选项最多 8 个，option_id 最多 256 字节。若不能在完整保留选项 ID/语义的前提下满足预算，则以 ACP cancelled 回复该请求并取消任务，报告 permission_payload_too_large；不自动允许或篡改选项。

## 10. 权限裁决状态机

每项权限只有一次从 pending 迁移到 selected / auto_denied / cancelled 的机会；用户裁决、定时器和任务取消通过同一 Coordinator 排序，responder 只能消费一次。

- 新请求按 epoch/session 路由到当前任务，分配 permission_id；无归属、会话关闭中或任务已终止时直接回复 ACP cancelled。
- pending 数量大于 0 时任务为 waiting_permission；归零才回 running，且不得覆盖 cancelling/终态。
- 默认每项等待 600 秒。超时选择请求实际提供的 reject_once；若没有拒绝类选项则回复 ACP cancelled。不得硬编码 option_id，也不得使用默认允许。
- 一项超时只结算该项；其余待决项继续展示。达到 16 项上限时，对新增项回复 cancelled，并走任务取消流程，保存 permission_limit_exceeded 原因。
- 工具详情优先结合工具生命周期事件；详情不完整时明确展示，允许选择拒绝或取消整个任务。
- 权限超时不直接代表整个任务失败；任务正常继续后的成败由 prompt 结算决定。权限内部故障触发的取消则以 failed 记录故障原因和停止证据。

## 11. 配置与依赖

### 11.1 配置

环境变量优先；无效值启动即报 configuration_error，不静默替换。时长和容量均要求正值；MAX_ACTIVE_TASKS 不得大于 MAX_OPEN_SESSIONS，MAX_TASK_RECORDS 至少为 MAX_OPEN_SESSIONS + MAX_ACTIVE_TASKS。

| 变量（均带 DSH_MCP_ 前缀） | 默认值 | 含义 |
|---|---|---|
| DSH_COMMAND | dsh | 单独的可执行文件名或路径 |
| DSH_ARGS_JSON | `["--profile","acp"]` | JSON 字符串数组，逐项传给 Command::args |
| STATE_DIR | 见第 8 节 | 实例锁与恢复保护标记 |
| MAX_ACTIVE_TASKS | 4 | 活跃任务额度 |
| MAX_OPEN_SESSIONS | 16 | 未关闭会话额度 |
| SESSION_IDLE_TTL_SECS | 1800 | 闲置会话保留时间 |
| MAX_TASK_RECORDS | 256 | 任务记录总上限 |
| TASK_RETENTION_SECS | 86400 | 非固定终态记录保留时间 |
| START_TIMEOUT_SECS | 30 | spawn+initialize 的总预算；session/new 另用同样的预算 |
| PERMISSION_TIMEOUT_SECS | 600 | 每项权限等待时间 |
| CANCEL_TIMEOUT_SECS | 30 | 取消到关闭升级阈值 |
| CLOSE_TIMEOUT_SECS | 10 | 会话关闭预算 |
| SHUTDOWN_TIMEOUT_SECS | 50 | 整个桥接关停预算 |

事件、结果、输入和响应大小采用第 9 节固定默认上限，第一版不开放每个内部容量为配置。prompt 不设通用执行超时：允许长任务，用户显式取消；控制 RPC 与关停始终有界。ACP 写入/权限回复超时使用 CLOSE_TIMEOUT_SECS。

绝对 Node 启动示例：DSH_COMMAND 指向 node，DSH_ARGS_JSON 为 `["/absolute/path/to/dsh/lib/bin.js","--profile","acp"]`。不使用 shell 解析、空格拆分或命令字符串拼接；示例中的文件路径由部署环境填写。

dsh 继承部署环境管理的配置和凭据；桥接不读取凭据文件，不打印完整环境变量。stdout 仅 MCP 协议，桥接日志和 dsh stderr 走 stderr；持续排空 dsh stderr，日志限长，避免管道塞满。API 网络代理由启动环境设置，不把本机 7890 端口硬编码为产品默认。

### 11.2 Rust 依赖与适配边界

| 依赖 | 选择 |
|---|---|
| rmcp | 3.5.1 起，server / macros / transport-io |
| agent-client-protocol | 3.4.0 起，使用 v1 stable，不启用 unstable 功能 |
| tokio | 1，rt-multi-thread / macros / process / io-util / io-std / sync / time / signal |
| tokio-util | 0.7，compat；适配 Tokio 管道到 ACP 使用的 futures I/O traits |
| serde / serde_json | 1，协议和内部数据结构 |
| tracing / tracing-subscriber | 0.1 / 0.3，stderr 日志 |
| uuid | 1，v4 / serde |
| rustix | 1，fs；使用 OS advisory lock，锁随进程退出释放，不能以“锁文件存在”代替 OS 锁 |

由 tokio::process 负责启动与监督，通过 compat 接入 ACP；不假定 ACP 默认 features 自带进程启动器。使用发布版 SDK、Cargo.lock 和编译测试确认 API。新增辅助依赖以实际实现需要为准，不能为适配方便切换到未验证的 ACP 草案接口。

## 12. 模块职责

```text
src/
  main.rs          配置、日志、入口命令、信号和 MCP 生命周期
  config.rs        配置解析、容量约束
  server.rs        MCP schema、参数校验、结构化响应
  coordinator.rs   唯一业务状态写入者、额度、去重、回调归属
  task.rs          任务与会话模型、迁移规则
  permission.rs    待决项、一次性裁决、截止时间
  events.rs        有界事件、分页、结果聚合
  acp.rs           SDK 适配、能力协商、请求与有序回调
  supervisor.rs    子进程、超时、EOF、退出和 wait/reap
  runtime_guard.rs 实例锁、运行标记、人工清理确认入口
tests/             fake ACP、MCP 协议和授权的真实 dsh 验收
```

Coordinator 面向内部 ACP trait/命令事件工作，单元测试无需真实 SDK、网络或模型。ACP adapter 不决定任务成功，supervisor 不直接改任务状态；两者只报告可观察事实。

## 13. 验证与验收

### 13.1 确定性测试：必跑，不调用模型

使用可控 fake ACP 和虚拟时间，覆盖下列行为；重点检验外部结果和不变量：

| 场景 | 必须观察到的结果 |
|---|---|
| 同 request_id 并发重试、响应丢失后重试 | 一个任务、一次 prompt；不同内容报 request_conflict |
| 同时 start/followup | 全局额度不超限；每会话至多一个 prompt；初始化只发生一次 |
| 连续完成 5 个任务 | 不因活跃任务额度泄漏而拒绝；关闭/TTL 后会话额度可再次使用 |
| 旧任务追问、关闭后追问 | stale_task/session_closed，绝不创建替代会话 |
| starting 时取消、完成与取消竞争 | 不遗漏新建会话；不重复终结；取消旧任务不影响下一轮 |
| cancel 超时 → close 成功 | cancelled 或对应内部故障的 failed，停止证据明确，会话 closed |
| close 超时、子进程异常退出 | unknown 任务如实报告；backend unavailable；旧会话失效；不重放 |
| 两项权限，只裁决其中一项 | 仍 waiting_permission，另一项继续可见和可裁决 |
| 权限裁决/超时/取消竞争 | responder 恰好一次结算；重复同选项幂等；超时不放行 |
| 超过 200 条事件/1 MiB、旧 cursor | gap 和丢失数量正确；分页无重复；最终文本不受事件淘汰影响 |
| UTF-8、长工具输出、权限大载荷 | 输出有效 JSON/UTF-8，大小有界；不截掉可选项却接受权限 |
| MCP EOF、SIGTERM、桥接异常结束 | 有界关停；未确认清理时标记保留；下次启动被阻止 |
| 状态目录锁、旧标记、确认命令 | 双实例被拒绝；清理确认需独占锁；不会自动清除异常标记 |
| argv 含空格、引号与 shell 字符 | 参数原样到达子进程，不发生 shell 执行 |

补充 MCP 协议级测试：initialize、tools/list、六个工具的 schema 与错误输出，以及任务运行中 get_task/取消工具仍能及时返回。压力测试按固定负载验证记录数和保留字节上限。

### 13.2 真实 dsh 控制面

使用临时 DSH_HOME、状态目录和工作目录，不提交 prompt，运行 initialize → session/new → session/close → 子进程退出。确认 capability 检查、stdout 协议纯净、兼容适配与标记清除。

CI 必须显式安装并固定受测 dsh/Node 版本；开发机未安装 dsh 时可以显式跳过本组并报告原因。空 DSH_HOME 下能否完成控制面握手仍需实测，若需要额外配置则补固定测试夹具，不能把未运行记为通过。

### 13.3 真实模型验收

沿用原设计中已授权的临时目录真实模型验证范围；消耗 API 额度，真实凭据交给 dsh 管理。执行：

1. 派发小型写文件任务，轮询到终态，核对真实文件；重复 request_id 确认没有重复执行。
2. 追问修改同一文件，核对会话上下文与变更结果；显式关闭后验证追问错误。
3. 并发两个独立临时目录任务，确认事件、结果和权限归属不串线。
4. 取消受控的持续写文件任务，确认停止证据和多次采样后文件不再变化；这是该测试工作负载的证据，不扩展成任意外部进程停止保证。
5. 若实际 preset 触发权限请求，验证拒绝/允许和继续执行；否则报告该路径真实覆盖缺失，确定性测试仍为必跑。
6. 强制故障使用 fake ACP/可控进程夹具验证 unknown 与恢复保护；不为测试而遗留真实模型创建的未知执行。
7. 关闭会话、关停进程、确认标记清除，再删除临时目录；清理失败保留诊断与目录位置。

构建检查：`cargo fmt --check`、`cargo build --locked`、`cargo test --locked`、`cargo clippy --locked -- -D warnings`。交付记录分别列明 mock、控制面、真实模型三组实际结果。

## 14. 交付与限制说明

交付 Rust 源码、Cargo.lock、测试、README 和 Apache-2.0 许可信息。README 包含安装与 command/args 配置、六工具工作流、Codex 派发/验收指令示例、容量/TTL、运行标记与人工恢复步骤、已验证的 dsh 版本。

提供 Codex MCP stdio 配置示例；发布前对照当时官方配置文档核对键名。当前文档修订不修改全局配置、不创建远程仓库、不推送代码。

第一版的明确限制：会话和任务跨桥接重启不可续接；异常后端故障需要人工确认清理；并发目录不自动隔离；事件和结果有大小上限；completed 只说明一轮执行结束。Codex 必须根据文件 diff、测试和需求验收，不把 dsh 的最终文本当作唯一证据。

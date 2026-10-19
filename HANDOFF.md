# dsh-mcp：新 session 交接文档

更新日期：2026-10-19。目标目录：`/home/workspace/dsh-mcp`。

本文记录上一 session 的用户意图、已核实事实、建议方案和未完成工作。建议方案不代表已实现或已通过联调；新 session 应依据用户的新指令继续推进。

## 1. 用户目标与当前状态

用户希望建立以下工作流：

```text
用户提出需求
    ↓
Codex：分析、整理需求、拆分任务、派发、验收
    ↓ MCP
dsh-mcp：任务与会话管理、协议转换
    ↓ ACP
dsh（DeepSeek Harness）：执行具体任务、修改代码、运行测试、接受返工
```

- 用户希望自己维护这个集成项目，已经创建 `/home/workspace/dsh-mcp`，准备在新 session 开始实现。
- 仓库名称采用 `dsh-mcp`。GitHub 账号、远程 URL、发布安排尚未提供，不要据此自动创建远程仓库或推送。
- 用户不太熟悉 TypeScript，讨论后的当前技术方向是 **Rust + MCP 服务端 + ACP 客户端**。
- 目标目录已有 Git 仓库和 Apache-2.0 `LICENSE`；交接前 `git status --short` 无输出，未发现项目代码。
- 本次只写交接文档；没有创建 Cargo 项目，没有安装 MCP 桥接程序，没有修改 Codex 或 dsh 的全局配置。
- 已完成资料核验与帮助命令检查；**尚未执行真实模型任务，也尚未完成 Codex → 桥接层 → dsh 的端到端联调**。

可用的项目描述：

> A Rust MCP server for delegating coding tasks to DeepSeek Harness via ACP.

中文：用 Rust 编写的 MCP 服务，通过 ACP 调度 DeepSeek Harness，支持任务执行、会话续接、进度查询和取消。

## 2. 用户的沟通偏好

- 用中文，简洁直接、事实优先，不要客套、恭维或无意义寒暄。
- 不要在结尾附加泛泛的引导性问题。
- 判断、假设和未验证事项要明确标注，不能把文档支持说成已实测成功。
- 用户不喜欢重复确认已经授权的事项；在当前授权范围内自主推进，确实缺信息时问具体问题。
- 用户目标中的“Codex 只做主控”描述的是最终产品工作流；当前桥接工具尚未建立，不要误报这一分工已经生效。

## 3. 本机环境与网络

### 工作目录和权限

- 项目目录：`/home/workspace/dsh-mcp`。
- 上一 session 工作目录为 `/home/agent_coding/test`，写入白名单只包括该目录和 `/tmp`，所以写本项目需通过工具的提权流程。
- 新 session 最好直接以 `/home/workspace/dsh-mcp` 为工作目录，并遵守届时实际的沙箱权限。
- 沙箱内的 PATH、进程和监听端口视图可能不完整，不能仅凭 `command -v` 或 `ss` 没有结果就断言宿主机未安装/未运行。

### 本机代理：已经验证有效

- HTTP 代理：`http://127.0.0.1:7890`。
- `/etc/mihomo/config.yaml` 另声明 SOCKS 端口 `7891`；本次成功使用的是 HTTP `7890`。
- 上一 session 默认命令环境没有 `http_proxy` / `https_proxy` / `all_proxy`。
- 沙箱内直接连接 `127.0.0.1:7890` 失败；在沙箱外显式指定代理后，官方文档成功返回 HTTP 200。
- 后续联网应先按此方式尝试，不要再次把未走代理的失败简单归因为官网不可访问。

读取公开文档的示例：

```bash
curl --proxy http://127.0.0.1:7890 --noproxy '' \
  -L --connect-timeout 10 --max-time 25 -sS \
  'https://developers.openai.com/codex/mcp/'
```

需要时通过工具的 `sandbox_permissions=require_escalated` 在沙箱外执行，不绕过审批。

已观察到的网络限制：GitHub API 有时返回共享出口 IP 限流；公开 raw 文件通常仍可读取。部分请求出现暂时的 TLS 中断/超时，重试可成功。crates.io API 曾返回 403，因此尚未核实最新已发布 crate 版本，不要直接把 GitHub HEAD 版本当成已发布版本。

## 4. dsh 安装定位与已做验证

### 精确路径

本机 dsh 为 npm/npx 缓存中的 `@deepseek-ai/dsh`，版本 **`0.2.0-rc.2`**。

```text
Node:
/home/agent_coding/.nvm/versions/node/v24.14.0/bin/node

dsh CLI:
/home/agent_coding/.npm/_npx/b86ed90107c62dab/node_modules/@deepseek-ai/dsh/lib/bin.js

dsh 用户配置/数据目录:
/home/agent_coding/.dsh
```

可以用绝对路径启动，避免依赖当前终端是否能找到 `dsh`：

```bash
/home/agent_coding/.nvm/versions/node/v24.14.0/bin/node \
  /home/agent_coding/.npm/_npx/b86ed90107c62dab/node_modules/@deepseek-ai/dsh/lib/bin.js \
  --profile acp
```

这是当前机器的可用定位，不适合硬编码进可分发项目。产品应允许配置 command/args；npx 缓存升级或清理后路径可能变化。

已确认 `~/.cargo/bin/cargo` 与 `~/.cargo/bin/rustc` 路径存在；**尚未执行版本检查或 Rust 构建**。

### 实际运行过的验证

- 顶层 `dsh --help` 成功。
- `dsh --profile headless --help` 成功，退出码 0。
- `dsh --profile sdk --help` 成功，退出码 0。
- `dsh --profile acp --help` 成功，退出码 0。
- 三个 profile 帮助检查分别使用 `/tmp/codex-dsh-headless-help`、`/tmp/codex-dsh-sdk-help`、`/tmp/codex-dsh-acp-help` 作为 `DSH_HOME`，避免修改真实配置或加载真实凭据。
- 没有通过这些检查发起模型任务。

真实调用需要另外验证 dsh 的模型凭据、profile、网络及权限；Web profile 的设置不能未经检查就当作 ACP profile 的设置。不要打印或复制 `~/.dsh/.credentials.yaml` 的内容到日志、文档或仓库。

## 5. dsh 的三种自动化入口

### A. ACP：当前推荐的产品接入方向

命令：`dsh --profile acp`。

- 使用 stdio 上的 JSON-RPC，提供标准 ACP v1 自动化接口。
- 适用于持久会话、追问、取消、进度和权限处理。
- stdout 仅用于协议；诊断日志应走 stderr。
- `initialize` 是能力协商入口，应按实际公布的能力调用接口。
- 不要因 Rust SDK 提供草案 ACP v2 就直接启用 v2；本机 dsh 文档明确是 ACP v1。

以下内容来自本机 `@deepseek-ai/dsh-acp/README.zh.md`，对实现很重要：

| 接口 | 本机版本文档描述的行为 |
|---|---|
| `initialize` | 协商 ACP v1 和支持的能力 |
| `session/new` | 创建持久会话，需要绝对工作目录 |
| `session/list` | 列出可恢复的持久根会话，可按 cwd 筛选 |
| `session/resume` | 恢复非活跃会话；恢复历史但不回放旧更新 |
| `session/close` | 取消、排空更新、释放后代、flush 持久化、关闭指定会话 |
| `session/set_config_option` | 更新实际公布的 model 或 reasoning_effort |
| `session/prompt` | 每个会话同时只允许一个提示词；Agent 空闲且有序更新交付后才结算 |
| `session/cancel` / `$/cancel_request` | 取消执行 |
| `session/update` | 已提交消息、thought、工具生命周期、配置变化、上下文用量 |
| `session/request_permission` | 客户端处理一次性允许/拒绝请求 |

明确限制：

- **恢复使用 `session/resume`，本机 dsh 不支持 `session/load`。** 不要照搬某些通用 ACP 桥接器的 load 假设。
- 不支持 mode、会话 fork/删除、附加工作目录、transcript 回放、客户端文件系统操作、终端、计划、elicitation 等交互扩展。
- 事件更新是语义层更新，不等同于模型原始 token 流，也不等同于完整 dsh Web UI。
- 一个连接支持多个独立会话，但每个会话同时只有一个正在处理的 prompt。
- 文档中 `authenticate` 立即成功只代表 ACP 连接层不要求认证，不代表底层模型调用无需凭据。

### B. headless：可用于最小验证或备选后端

```bash
dsh --profile headless --json "任务说明"
dsh --profile headless --json --session-id "实际会话ID" "返工要求"
```

- 一次进程运行一个任务后退出；任务可从参数或 stdin 输入。
- `--json` 输出按行 JSON：session、status、text、thinking、tool_call、tool_result、final 等。
- `--session-id` 延续已持久化的会话；未知 ID 会失败，不会偷偷新建空会话。
- 续接受工作目录、会话归属及 preset 等限制。
- 完成与失败需要结合退出码及结束原因判断；有 final 事件并不必然成功。
- 它适合快速验证，但用户目前倾向长期维护 Rust + ACP，不要未经说明自动把正式方案改回 headless。

### C. dsh 自有 SDK：已确认存在，但不是当前首选

命令：`dsh --profile sdk`，通过 stdio JSON-RPC 驱动。

本机版本文档说明：`session/prompt` 返回 messageId 表示消息入队，后续通知不自动提供每次请求的完整结果归属；没有逐会话关闭或逐提示词取消方法。ACP 对当前“派发、追问、取消、验收”需求更直接。

## 6. Rust 技术方向

官方库已确认存在且支持所需角色：

| 部分 | 库/用途 |
|---|---|
| MCP 服务端 | `rmcp`，官方 Rust MCP SDK |
| ACP 客户端 | `agent-client-protocol`，官方 Rust ACP SDK |
| 异步与进程 | 可考虑 `tokio`；核对两端 SDK 的实际运行时、I/O trait 和 Send 要求 |
| 数据结构 | 可考虑 `serde` / `serde_json` |
| 日志 | 可考虑 `tracing`，协议 stdout 不写普通日志 |

这是技术选择建议，尚未锁定依赖版本。需要针对可发布 crate 版本、本机编译器以及 dsh 的 ACP v1 能力完成兼容性验证。

注意：ACP Rust 仓库还提供 `agent-client-protocol-rmcp` 及 MCP-over-ACP 集成。其存在不能直接证明它已经实现了“把 ACP agent 暴露成 MCP 任务工具”这个方向，不要混淆两种协议组合的调用方向。

许可证：目标目录现有 Apache-2.0 `LICENSE`。两个 Rust SDK 仓库的 workspace 许可声明也为 Apache-2.0。Cargo 项目可以填写 `license = "Apache-2.0"`，仍应按实际依赖履行声明义务。

## 7. 建议的第一版范围（尚未实现）

先做 Linux 本机、MCP stdio、一个 dsh 会话、任务串行；先完成闭环，再扩展并发和跨进程恢复。

建议对 Codex 提供四个 MCP 工具，名称可以在设计时调整：

| 工具 | 作用 |
|---|---|
| `start_task` | 接收任务、绝对 cwd，返回任务 ID；后台执行，避免一个 MCP 调用长时间阻塞 |
| `get_task` | 返回状态、增量事件、最终结果或错误，控制返回大小 |
| `send_followup` | 在对应 dsh 会话中发起补充/返工；同一会话仍要串行 |
| `cancel_task` | 请求 ACP 取消并进行有界清理 |

重要设计点：

- task ID 与 dsh session ID 是不同概念，需要明确映射；追问是新一轮任务，但应复用同一 dsh 上下文。
- 任务应能区分 running、waiting_permission、completed、failed、cancelled 等状态；具体模型由下一 session 设计。
- MCP 启动、查询、取消工具应能在 dsh 执行期间继续响应。
- 权限回调不能无人处理而永久挂起。需要定义转交用户/主控的流程或明确的受限策略；若采用人工处理，可能需要增加专门的权限决策工具。不要默认全放行来省事。
- 取消先走 ACP，超时后再清理进程；不要只结束桥接层里的 future 而让 dsh 继续写文件。
- 实际模型及推理选项使用 ACP 实时公布的值，不照抄上一 session 的模型名称，不在失败时静默切换模型或提供方。
- ACP 权限确认和操作系统沙箱是两回事；“Codex 不写代码”的提示词约定不等于强制只读。
- dsh 的输出是执行证据的一部分；最终需求验收仍由 Codex 检查实际 diff、测试结果和任务要求完成。
- 尽量保持接口和实现聚焦，不在第一版扩展成支持所有代理、模型、平台的通用调度系统。

建议先后顺序：

1. 检查目标目录规则、Rust 工具链和官方 crate 发布版本；形成简短可审查设计。
2. 做最小 ACP 控制面验证：启动本机 dsh、initialize、创建/关闭临时目录会话；先不发送真实模型任务。
3. 建立 Rust MCP 服务端，完成工具发现与任务状态管理。
4. 接通一次任务、进度、完成/失败，以及同会话返工。
5. 验证取消、进程退出、协议错误、权限等待等关键路径。
6. 在明确授权的临时工作区做真实模型读写及返工验收，记录实际通过和未验证项。
7. 再提供 Codex 的 MCP 配置示例；发布和修改全局配置按新 session 的实际授权处理。

这些是建议，不是现成代码或完成记录。不要仅凭文档/帮助命令就宣布端到端可用。

## 8. 已发现的现成项目，可参考实现

用户知晓这些项目后仍倾向自己维护仓库；不要未经说明改成只安装它们。

### kxress/acp-mcp

- 仓库：https://github.com/kxress/acp-mcp
- 已读 README 和源码归档，检索时源码版本为 0.2.5。
- 提供单次任务、持久会话、事件查询、追问、取消、权限处理、自定义 ACP 启动命令。
- 尚未安装或与本机 dsh 联调。
- 与 dsh 的具体能力仍须匹配，例如 session/resume 与 session/load 的区别。

### Oortonaut/mcacp

- 仓库：https://github.com/Oortonaut/mcacp
- 文档说明可通过 npm 使用，支持 MCP→ACP、自定义启动命令、会话和权限管理。
- 只核实了文档/包元数据，没有运行验证。

### dsh-streaming-mcp-bridge

- 仓库：https://github.com/yabolee-kkk/dsh-streaming-mcp-bridge
- npm：https://www.npmjs.com/package/dsh-streaming-mcp-bridge
- 检索到的 npm 版本为 0.1.1；已下载包到临时目录只读审查，没有安装或执行。
- 提供 Streamable HTTP MCP 的 dsh_run_stream 工具。
- npm 包绑定 dsh 0.1.0-rc.6 系列 SDK，且独立运行路径含旧源码目录假设；不应当作已经适配本机 0.2.0-rc.2 的现成结论。
- 作为参考时区分仓库 HEAD 文档与实际发布包，二者不一定一致。

### 其他易混淆项

- `GongRzhe/ACP-MCP-Server` 名称中的 ACP 是 Agent Communication Protocol，与本项目需要的 Agent Client Protocol 不同，不能直接套用。
- “支持 MCP 的 dsh 插件”可能指 dsh 调用外部 MCP 工具，与本项目的调用方向相反。

## 9. 机器上已有的旧集成不是本项目目标

- `/home/agent_coding/.codex/agents/deepseek.toml` 和 `deepseek-explorer.toml` 是 Codex 原生角色配置，切换模型但不启动 dsh。
- `/home/agent_coding/test/deepseek-worker/` 有旧 MCP worker 实现，启动的是另一个 Codex CLI 进程，不是 DeepSeek Harness。
- 旧 worker 可参考进程超时/清理等经验，但不要直接当作 dsh 适配器。旧文档中的“验证通过”只针对旧实现。
- 上次检查 Codex 配置没有注册该旧 MCP，也没有 dsh-mcp。
- 通过 MCP 接入的 dsh 是外部工作代理，不能承诺自动显示在 Codex 原生子代理面板中。

## 10. 核验过的资料入口

### 官方网页/仓库

- Codex MCP：https://learn.chatgpt.com/docs/extend/mcp?surface=cli
- Codex 子代理：https://learn.chatgpt.com/docs/agent-configuration/subagents
- Codex 配置：https://learn.chatgpt.com/docs/config-file/config-reference
- MCP SDK 列表：https://modelcontextprotocol.io/docs/sdk
- Rust MCP SDK：https://github.com/modelcontextprotocol/rust-sdk
- Rust ACP SDK：https://github.com/agentclientprotocol/rust-sdk
- DeepSeek Harness 仓库（安装包 repository 字段）：https://github.com/deepseek-ai/deepseek-harness
- Apache-2.0：https://www.apache.org/licenses/LICENSE-2.0

### 本机版本对应的 dsh 文档

公共目录前缀：

```text
/home/agent_coding/.npm/_npx/b86ed90107c62dab/node_modules/@deepseek-ai/
```

优先阅读下列相对路径：

```text
dsh/README.zh.md
dsh-acp/README.zh.md
dsh-acp-app/README.zh.md
dsh-headless/README.zh.md
dsh-sdk-app/README.zh.md
dsh-sdk-jsonrpc-server/README.zh.md
dsh-sdk-protocol/README.zh.md
```

本机文档与已安装版本对应，比假定最新版接口更适合首次联调。远程仓库可能继续变化，正式选依赖时应锁定版本并再核对。

## 11. 新 session 的启动提示

用户可在新 session 发送：

> 请先阅读 `/home/workspace/dsh-mcp/HANDOFF.md` 和本目录适用规则，接着实现 dsh-mcp。采用 Rust，向 Codex 提供 MCP 工具，通过 ACP v1 调用本机 dsh。先实现一个会话的任务派发、状态查询、同会话返工和取消，完成必要验证。已核实的背景不要重复从零调查；未验证的接口兼容性和权限处理要实际确认。


# 会话管理与共享 daemon 验证记录

日期：2026-10-19。对应 [会话管理设计](superpowers/specs/2026-10-19-dsh-acp-session-management-design.md)。本次已完成两期源码、隔离验证与部署候选；生产 launcher、Codex 配置、DSH profile 和 Web 服务保持原状。2026-10-20 追加完成 Codex 原生接入和真实 DSH 验收，见下方记录。

## 本次最终结果

| 检查 | 实际结果 |
|---|---|
| `cargo fmt --check` | 通过 |
| `cargo clippy --locked --offline --all-targets -- -D warnings` | 通过，无警告 |
| `cargo test --locked --offline` | 140 项通过，0 失败 |
| `cargo build --release --locked --offline` | 通过 |
| Python 脚本 AST 语法检查 | 通过，未执行真实模型脚本 |
| `git diff --check` | 通过 |
| Codex 0.162.1 原生接入 | 通过，connected，11 工具全部发现与原生调用 |
| 新共享版本连接真实 DSH | 通过，3 个模型轮次完成写入、追加与跨 ACP 恢复 |
| 候选 unit 静态检查 | 临时副本仅替换为受测二进制路径后，`systemd-analyze --user verify` 通过 |

140 项包括库测试 85、入口测试 3、已安装 DSH 的无模型控制面测试 1、协调器 20、MCP 协议 6、传输资源边界 4、独立 stdio 生命周期 6、会话管理 13、共享传输与恢复 2。完整测试在沙箱外执行：沙箱限制 Unix socket、SO_PASSCRED，并影响原有 SIGTERM 测试。通过最小复现与未修改基线的对照确认环境限制后，使用相同测试在获准环境验证，没有放松生产行为以适配沙箱。

证据目录：`/tmp/dsh-acp-session-management-20261019-n8isry4x/`，最终输出为 `final-tests.log`、`clippy.log`、`release-build.log`、`systemd-verify-tested-path.log`。开发副本为其下 `work/`。原目录已有未提交和未跟踪实现，采用完整工作树快照与哈希核对回写，不覆盖其他并发改动。

## 本次覆盖范围

- ACP list/resume 的实际 JSON 字节流、握手能力缺失、特殊 ID/cursor、原始坏历史页拒绝、响应预算。
- 恢复与追加路由、跨工具去重、已持有历史拒绝、关闭后重新恢复、取消恢复不发送 prompt、后台查询不阻塞控制面。
- 连接归属、断线后任务继续、显式接管、迟到写入拒绝、权限原始选项校验、过滤与分页绑定。
- 真实 daemon + 多个独立 stdio proxy、一个懒启动 ACP、全局四任务与第五个拒绝、超大帧仅断开对应连接、32 连接包含未握手客户端、10 秒握手释放、第二 daemon 拒绝、正常清理。
- 后端隔离可查询、拒绝恢复、重启保留 recovery_required、确认已知无执行后代 fixture 清理后人工确认、再显式恢复新桥接任务。
- 帧、未完成请求、待发送响应与写期限；客户端取消不泄漏传输槽位，也不能绕过全局 32 个后台历史请求上限。
- 记录保留和空闲 TTL 相互独立，容量提前淘汰与去重失效同步，默认仍为会话 1800 秒 / 可回收终态记录 86400 秒。

自查先复现再修正了标识校验、SDK 跳过坏历史条目、沿符号链接创建目录、取消后槽位与后台查询累积等问题。测试还修正了临时目录权限的环境假设与旧工具数断言；最终完整输出均为通过。

## 后续真实验收（2026-10-20）

用户后续明确要求验收 Codex 接入并真实测试 dsh_mcp；这些验收任务由 Codex 主代理控制，没有委派实现工作。生产入口仍未切换。

1. 当前会话实际加载的旧生产入口：通过原生 `start_task` 创建临时文件，Codex 独立逐字节核对；用最新任务调用 `send_followup`，不重发随机代号且禁止读取文件，回答精确匹配；查询确认会话 closed，后端仍 ready。报告：`live-real-report.json`。
2. 新共享版本 + 实际 Codex 0.162.1 app-server：通过临时进程配置覆盖连接 `--proxy`，创建 ephemeral 会话，不启动 Codex 模型轮次。`mcpServerStatus/list` 返回 connected 和全部 11 个工具；通过 `mcpServer/tool/call` 验证只读懒启动、业务错误以及 fixture 的 list/resume/query/cancel/close。报告：`codex-native-report.json`。
3. 新共享版本 + 实际 Codex + 现有真实 DSH profile：完成 3 个 DSH 模型轮次。第一轮实际写入临时 `probe.txt` 并独立校验字节，第二轮在同会话精确回忆随机代号；确认 closed 并正常停止首个 daemon / ACP 后，重新启动，旧桥接 task ID 如实失效，经 `list_sessions(cwd=...)` 找到历史，再 `resume_task` 且不重发代号，第三轮精确回答。新桥接 task/session ID 不复用旧值，DSH ID 保持一致。最后确认恢复会话 closed、daemon 正常退出、socket 和运行标记已清除。报告：`codex-real-report.json`，文件 SHA-256 为 `86c473a381ceb194de9ccd447f89104523d3358d3f68f28fa86e9471232a34be`。

以上报告均在 `/tmp/dsh-acp-session-management-20261019-n8isry4x/`。验收前后生产 Codex 配置与 launcher 的哈希一致。真实测试仅操作自己创建的临时目录和会话，未修改模型 profile、Web 服务或生产接入配置。真实 DSH 验收观察到权限请求 0 次，因此仍不能宣称真实模型权限裁决回路已覆盖；原有确定性权限测试继续有效。

可重复入口为 `scripts/verify-codex-native.py`（fixture，不调用模型）和 `scripts/verify-codex-real.py`（显式调用当前已配置的真实 DSH profile，会消耗模型额度）。失败不自动确认清理，真实脚本保留临时目录供诊断。

## 仍未执行的生产步骤

版本化生产安装、user service 启动和 Codex 配置切换未执行；正式 unit 的目标二进制路径尚未安装。临时 unit 副本仅替换 ExecStart 为受测路径后静态校验通过。隔离新 Codex 的原生接入已通过，但生产切换后还应核对实际配置与进程。部署和回退步骤见 [运行说明](shared-daemon.md)。

---

以下保留首版实现的历史验证记录，仅用于基线追溯；其中的工具数、测试数及真实模型尝试不代表本次运行结果。

# 实现与验证记录

日期：2026-10-19。对应[第一版设计规格](superpowers/specs/2026-10-19-dsh-mcp-design.md)修订 2。

已实现 Rust MCP stdio 服务、六个任务工具、ACP v1 适配、任务与会话管理、权限裁决、容量限制、取消、关停及异常恢复保护。源码和测试保留在当前工作分支，未修改全局 Codex/dsh 配置。

## 受测环境

| 组件 | 版本 |
|---|---|
| Rust / Cargo | 1.95.0 |
| Node | 24.14.0 |
| dsh | `@deepseek-ai/dsh@0.2.0-rc.2` |
| ACP SDK | `agent-client-protocol 3.4.0`，stable v1 |
| MCP SDK | `rmcp 3.5.1` |

## 最终检查

| 命令 | 实际结果 |
|---|---|
| `cargo fmt --all -- --check` | 通过 |
| `cargo build --locked` | 通过 |
| `cargo test --locked` | 113 项通过，0 失败 |
| `cargo clippy --locked --all-targets -- -D warnings` | 通过，无警告 |
| Python 验收脚本语法检查及 `--help` | 通过 |
| `git diff --check` | 通过 |

Rust 测试包括库测试 78 项、入口参数测试 3 项、协调器集成测试 20 项、MCP 协议测试 5 项、二进制生命周期测试 6 项，以及真实 dsh 控制面测试 1 项。

全量测试在沙箱外运行：本环境中已用最小程序确认沙箱会改变 SIGTERM 的行为。二进制生命周期测试实际验证了 EOF、持有 stdin 时的 SIGTERM、桥接崩溃后的恢复保护，以及 1 秒关停预算下启动中、运行中和已开始故障清理的子进程回收。

全量检查曾暴露并发创建子进程时的锁释放竞态。新增测试用重复描述符确定性复现后，改为持有者退出时显式解锁；独立审查通过，随后全量测试通过。异常恢复标记仍保留至确认清理。

## 真实 dsh 控制面

使用空的临时 `DSH_HOME`，完成 ACP v1 initialize、`session/new`、`session/close`，并回收直接子进程。该组未发送 prompt、未调用模型；还以 `DSH_MCP_REAL_DSH=1` 单独强制运行，1 项通过，未跳过。CI 已配置安装固定版本的 Node/dsh 并强制执行该测试；尚未运行远程 CI。

## 真实模型验收

实际运行了 `scripts/e2e_real_model.py --permission-policy allow-once --test-cancel`，使用独立的临时 `DSH_HOME`、状态目录和工作目录。MCP 初始化及六工具发现通过；首个模型任务失败，报告 `backend_unavailable`。

额外的最小诊断确认 dsh 返回：

```text
llm-deepseek: no API key for provider route "deepseek-official"
```

失败任务的 `execution_state=stopped`；桥接正常关停，运行标记已清除。验收脚本报告 `cleanup_confirmed=true`、`passed=false`，没有观察到权限请求。失败报告位于 `/tmp/dsh-mcp-real-model-report.json`，临时目录按脚本约定保留。未读取或复制用户凭据文件。

因此，真实模型写文件、去重后的实际文件内容、追问、并发目录隔离、真实权限回路和受控写入取消尚未完成验收。部署环境提供可用的模型认证后，可按 [README](../README.md#验证) 中的命令重跑。

## 适配与保证范围

- SDK 内部接收队列不受调用方容量配置约束，适配层在 JSON 帧进入 SDK 前施加帧大小限制与消费确认。真实后端事件通道采用容量 1，使完整消息文本和最多 4 MiB 的帧仍处于规格的 8 MiB 队列预算内；持续拥塞 10 秒按故障处理。
- 缺少 `messageId` 时显式降级为本轮 assistant 文本汇总，并记录兼容事件；不推测消息边界。
- MCP EOF、SIGINT、SIGTERM 触发有界清理。只有会话清理和直接子进程回收得到确认后，才清除运行标记；无法确认执行停止时保留恢复保护。
- 取消的停止证据来自 ACP 轮次或会话结算。强制结束 dsh 不保证清理跨 systemd scope 的任意执行后代，此时如实返回未知状态。

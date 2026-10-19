# DSH 会话管理交接

2026-10-19：历史恢复、共享 daemon、独立 stdio proxy、任务发现与显式接管已实现并在隔离环境验证。当前生产 launcher、Codex 配置、模型 profile 与 Web 服务保持原状，未切换共享部署。

入口：

- [运行说明与部署候选](../shared-daemon.md)
- [实际验证记录](../validation.md)
- [设计规格](specs/2026-10-19-dsh-acp-session-management-design.md)
- [一期实施记录](plans/2026-10-19-dsh-acp-history-implementation.md)
- [二期实施记录](plans/2026-10-19-dsh-acp-shared-daemon-implementation.md)

源码：`/home/workspace/dsh-mcp`。原目录包含大量此前未提交、未跟踪的源码，不能覆盖或重置。开发与证据保存在 `/tmp/dsh-acp-session-management-20261019-n8isry4x`，完整基线含 SHA-256，回写只涉及本次变更文件。

用户明确要求开发工作全部由 Codex 处理，不委派 DeepSeek 或其他子代理，并选择“完整实现，先保留现有生产入口”。后续不要重新实现已完成部分，也不要自行进行生产切换。

若用户另行要求切换生产，先核对并备份现有 `/home/agent_coding/.codex/tools/dsh-acp/launch.py` 与 Codex 配置，再按运行说明准备版本化二进制、受保护环境和稳定状态目录。模型由原 DSH profile 管理；不要打印认证内容。生产切换和真实模型调用需要遵从用户当时的指令与环境权限。

2026-10-20 用户后续要求真实验收，已通过实际 Codex 0.162.1 原生工具发现和调用，以及新共享版本连接真实 DSH 的文件写入、同会话回忆和跨 ACP 进程恢复随机代号。报告见验证记录。真实流程未触发权限请求，生产切换仍未执行。恢复的是 DSH 持久化上下文，旧桥接 task ID、request_id 和事件不跨重启恢复；不承诺接管外部 Web 活跃 session。

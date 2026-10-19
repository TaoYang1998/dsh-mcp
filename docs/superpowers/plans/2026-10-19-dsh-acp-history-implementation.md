# DSH ACP 历史会话恢复实施记录

日期：2026-10-19。状态：一期源码与确定性验收完成，已整合二期。按用户要求全部由 Codex 直接处理，未调用 DeepSeek 或其他子代理；保留生产入口。

目标与约束以 [设计规格](../specs/2026-10-19-dsh-acp-session-management-design.md) 为准。这里只恢复 DSH 持久化的非活跃上下文，旧桥接任务 ID、事件和 request_id 不跨重启恢复。

## 1. 完整基线

- [x] 保存原目录的当前文件清单与 SHA-256，完整复制未提交、未跟踪的源码和文档；在 `/tmp/dsh-acp-session-management-20261019-n8isry4x/work` 开发，未用不完整 HEAD 替代工作树。
- [x] 运行基线测试，记录沙箱对 Unix socket / SIGTERM 的限制，同一生命周期测试在沙箱外通过。
- [x] 不修改生产 launcher、Codex 配置、DSH profile 或 Web 服务。

## 2. ACP 历史接口

- [x] 新增 `src/history.rs`，扩展 Backend、FakeDriver 和真实 ACP SDK 的 list/resume 请求。
- [x] 只使用握手公布的能力，无能力返回 `capability_unsupported`；恢复失败不创建替代上下文。
- [x] 原样保留 DSH ID 和分页 cursor；只裁剪展示标题，整页超预算明确报错。
- [x] 原始历史页先核验必需字段，防止 SDK 静默跳过坏条目后越过分页记录。
- [x] 使用真实 Python ACP 字节流测试能力缺失、特殊 ID/cursor 和 resume 参数，不调用模型。

涉及文件：`src/acp.rs`、`src/history.rs`、`src/task.rs`、`src/lib.rs`、`tests/fixtures/fake_acp.py`。

## 3. 恢复准入与控制面

- [x] 扩展去重指纹，start/followup/resume 共用 request_id 范围；先去重，再目录和容量校验与预留。
- [x] 在 actor 外等待 resume，成功后安装路由再 prompt；取消期间不发送新 prompt；未知恢复结果沿用整体隔离。
- [x] 拒绝目标仍被持有或正在恢复的 session，允许已关闭历史生成新的桥接任务；后续使用最新 task ID 追加。
- [x] 列举懒启动 ACP，I/O 不阻塞健康查询；全局最多 32 个在途历史请求，取消调用方不提前归还后台额度。

涉及文件：`src/coordinator.rs`、`tests/session_management.rs`；原有协调器与控制面测试继续回归。

## 4. MCP 与一期验收

- [x] 增加 `list_sessions`、`resume_task`、`get_status`，原六工具参数不变；二期整合后共 11 个工具。
- [x] 验证恢复来源、新桥接 ID、恢复与追加共用上下文路由、去重、关闭后恢复、取消、分页和错误输出。
- [x] 运行格式、Clippy、完整测试和 release 构建；具体证据见 [验证记录](../../validation.md)。
- [x] 更新 README、运行说明和二期计划，明确生产入口保留与恢复边界。

实际测试入口为 `tests/session_management.rs`、`tests/mcp_protocol.rs`、`src/acp.rs` 历史测试及 `scripts/verify-shared-daemon.py` / `scripts/verify-recovery.py`。测试名按最终实现命名，不依赖早期计划占位名。

## 追加真实验收（2026-10-20）

- [x] 按用户后续真实验收指令运行跨 ACP 进程随机代号回忆、实际文件写入和后续追加，独立核对精确匹配。
- [x] 实际 Codex 0.162.1 app-server 在隔离配置下原生发现 11 个工具并完成调用。

报告见验证记录。生产入口仍按用户要求保持原状；后续生产切换不包含在此次隔离验收中。

# DSH ACP 共享 daemon 实施记录

日期：2026-10-19。状态：二期源码、隔离验证与部署候选完成。用户选择“完整实现，先保留现有生产入口”；全部由 Codex 直接处理，未委派模型。

设计依据：[方案](../specs/2026-10-19-dsh-acp-session-management-design.md)。一个 daemon 拥有一个协调器与按需启动的 ACP，每个 Codex 使用独立 stdio proxy，经 Unix socket 接入；无参数的旧独立模式继续可用。

## 1. 连接归属与发现

- [x] 新增 `ClientScope`、存活连接注册表与 actor 内 owner 映射；连接身份不能由 MCP 参数指定。
- [x] 新增 `list_tasks` 与显式 `attach_session`。读操作可跨 owner；写操作在 actor 核验当前归属，不能抢占活连接。
- [x] 断线仅释放归属，已接纳任务继续；接管不提交模型轮次、不放行权限、不刷新 TTL。
- [x] 验证断连后迟到操作、只读跨连接访问、权限原始选项、分页过滤与去重不转移归属。

涉及文件：`src/client.rs`、`src/coordinator.rs`、`src/server.rs`、`tests/session_management.rs`、`tests/mcp_protocol.rs`。

## 2. daemon、proxy 与资源边界

- [x] 增加 `--daemon`、`--proxy`，旧 stdio EOF 仍整体关停；单条共享连接 EOF 只注销当前 owner。
- [x] 仅 daemon 持稳定运行锁，socket 路径另有锁；拒绝非 socket、符号链接、不安全目录与活监听者，只回收确认遗留的 socket。
- [x] 父目录 0700、socket 0600、同 UID；proxy 不持锁、不启动 ACP、不自动重连或退回独立模式。
- [x] 默认 32 连接，未初始化连接计入容量；10 秒握手期限。配置值超出信号量表示范围时明确报错。
- [x] 解析前 1 MiB 帧上限，每连接最多 16 未完成请求 / 16 待发送响应、320 KiB 响应编码上限、10 秒写期限；取消通知释放本连接请求槽位，后台历史 I/O 另有全局上限。

涉及文件：`src/daemon.rs`、`src/proxy.rs`、`src/mcp_transport.rs`、`src/main.rs`、`src/config.rs`、Cargo 文件。

## 3. 控制面与故障恢复验收

- [x] 真实 daemon + 两个 stdio proxy 验证独立握手、懒启动一个 ACP、合计四任务与第五个拒绝、断线接管、list/resume、超大连接隔离、连接上限与握手到期、第二 daemon 拒绝、正常关停清理。
- [x] 真实进程验证后端故障可见、拒绝恢复、重启仍保留保护、确认 fixture 清理后人工 acknowledge、再显式恢复新任务。
- [x] 确定性测试覆盖权限归属、未知执行隔离、会话 TTL、记录保留、容量提前淘汰与同步删除去重。
- [x] 原独立 stdio 的生命周期回归通过；运行格式、Clippy、完整测试和 release 构建，证据见 [验证记录](../../validation.md)。

入口：`tests/shared_transport.rs` 调用 `scripts/verify-shared-daemon.py` 与 `scripts/verify-recovery.py`；另外有 `tests/mcp_transport_bounds.rs`、`tests/session_management.rs` 和原有测试。脚本只使用自己创建的临时目录及确定性 ACP fixture，不调用模型。

## 4. 部署候选与交付

- [x] 准备 user systemd unit、环境模板、proxy 配置示例与 [运行说明](../../shared-daemon.md)。版本化二进制、私有 RuntimeDirectory、70 秒关停期限、禁止自动重启。
- [x] 静态校验 unit：正式目标路径尚未安装；仅在临时副本替换为受测二进制路径后 `systemd-analyze --user verify` 通过。
- [x] 文档说明 backend unavailable 与传输成功的区别，按停止 / 核对后代清理 / 同目录人工确认 / 重启 / 显式恢复的顺序处理。
- [x] 自行审查实际 diff、资源边界和证据；修复发现的问题后重新验证。没有实现用 DSH 会话需要关闭。

## 待另行执行的生产步骤

- [ ] 备份当前配置与 launcher，处理生产任务，安装版本化二进制并填写受保护的 daemon 环境文件。
- [ ] 启动 user service 并切换 Codex 到 `--proxy`，验证两个生产 stdio 客户端及新 Codex 原生接入。
- [x] 2026-10-20 按用户后续指令完成隔离 Codex 原生接入，以及真实 DSH 的随机代号回忆、跨 ACP 恢复与文件写入验收，生产入口未切换。

前两项生产安装与切换仍未执行。回退时先处理共享任务并正常停止确认清理，再恢复旧 launcher；不能自动清除恢复标记或重放未知写请求。

use std::process::Command;
/// 通过真实进程、管道和 Unix socket 验证共享服务，绝不调用模型。
#[test]
fn shared_daemon_real_transport_contract() {
    let out = Command::new("python3")
        .args([
            "scripts/verify-shared-daemon.py",
            "--binary",
            env!("CARGO_BIN_EXE_dsh-mcp"),
        ])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}
/// 故障与重启均保留恢复保护，人工确认后才允许显式恢复。
#[test]
fn recovery_requires_cleanup_acknowledgment_before_resume() {
    let out = Command::new("python3")
        .args([
            "scripts/verify-recovery.py",
            "--binary",
            env!("CARGO_BIN_EXE_dsh-mcp"),
        ])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

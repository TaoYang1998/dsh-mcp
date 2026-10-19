//! Installed dsh control-plane check. No prompt or real model invocation.
use dsh_mcp::{
    acp::{Backend, BackendFactory, DshFactory},
    config::Config,
};
use std::{path::Path, time::Duration};

#[tokio::test]
async fn installed_dsh_initialize_new_and_close_without_model() {
    let node = std::env::var("DSH_MCP_TEST_NODE")
        .unwrap_or_else(|_| "/home/agent_coding/.nvm/versions/node/v24.14.0/bin/node".into());
    let cli = std::env::var("DSH_MCP_TEST_DSH_CLI").unwrap_or_else(|_| {
        "/home/agent_coding/.npm/_npx/b86ed90107c62dab/node_modules/@deepseek-ai/dsh/lib/bin.js"
            .into()
    });
    if !Path::new(&node).is_file() || !Path::new(&cli).is_file() {
        assert_ne!(
            std::env::var("DSH_MCP_REAL_DSH").as_deref(),
            Ok("1"),
            "explicit dsh control-plane fixture is missing"
        );
        eprintln!("installed dsh fixture unavailable; control-plane check skipped");
        return;
    }
    let home = tempfile::tempdir().unwrap();
    let cwd = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let config = Config {
        socket_path: None,
        max_clients: 32,
        dsh_command: node,
        dsh_args: vec![cli, "--profile".into(), "acp".into()],
        state_dir: state.path().into(),
        max_active_tasks: 4,
        max_open_sessions: 16,
        session_idle_ttl: Duration::from_secs(1800),
        max_task_records: 256,
        task_retention: Duration::from_secs(86400),
        start_timeout: Duration::from_secs(30),
        permission_timeout: Duration::from_secs(600),
        cancel_timeout: Duration::from_secs(30),
        close_timeout: Duration::from_secs(10),
        shutdown_timeout: Duration::from_secs(50),
    };
    let mut factory = DshFactory::new(config).with_env("DSH_HOME", home.path());
    let (backend, _events, pid) = factory
        .start()
        .await
        .expect("dsh ACP v1 initialize and close capability");
    assert!(pid > 0);
    // Always terminate/reap even if either control RPC rejects this environment.
    let result = async {
        let sid = backend.new_session(cwd.path()).await?;
        backend.close_session(&sid).await
    }
    .await;
    let reaped = backend.terminate_process().await;
    assert!(reaped, "direct child must be reaped");
    result.expect("session/new and session/close control plane");
}

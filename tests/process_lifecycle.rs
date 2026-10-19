//! Exercise the compiled binary, real pipes, signals and persistent recovery guard.
use serde_json::{Value, json};
use std::{process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
};

struct Client {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    sequence: u64,
}
impl Client {
    fn spawn(state: &std::path::Path, crash: bool) -> Self {
        Self::spawn_hanging(state, crash, "")
    }
    fn spawn_hanging(state: &std::path::Path, crash: bool, mode: &str) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_dsh-mcp"));
        command
            .env("DSH_MCP_STATE_DIR", state)
            .env("DSH_MCP_DSH_COMMAND", "python3")
            .env(
                "DSH_MCP_DSH_ARGS_JSON",
                serde_json::to_string(&[
                    "-u",
                    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/fake_acp.py"),
                ])
                .unwrap(),
            )
            .env("DSH_MCP_FIXTURE_CRASH", if crash { "1" } else { "0" })
            .env("DSH_MCP_FIXTURE_HANG", mode)
            .env("DSH_MCP_FIXTURE_PID_FILE", state.join("peer.pid"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        if !mode.is_empty() {
            command.env("DSH_MCP_SHUTDOWN_TIMEOUT_SECS", "1");
        }
        let mut child = command.spawn().unwrap();
        Self {
            input: child.stdin.take().unwrap(),
            output: BufReader::new(child.stdout.take().unwrap()),
            child,
            sequence: 0,
        }
    }
    async fn request(&mut self, method: &str, params: Value) -> Value {
        self.sequence += 1;
        let id = self.sequence;
        let bytes =
            serde_json::to_vec(&json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}))
                .unwrap();
        self.input.write_all(&bytes).await.unwrap();
        self.input.write_all(b"\n").await.unwrap();
        self.input.flush().await.unwrap();
        loop {
            let mut line = String::new();
            let n = tokio::time::timeout(Duration::from_secs(5), self.output.read_line(&mut line))
                .await
                .unwrap()
                .unwrap();
            assert_ne!(n, 0, "MCP stdout EOF");
            let response: Value =
                serde_json::from_str(&line).expect("stdout must contain only JSON frames");
            if response["id"] == id {
                assert!(response.get("error").is_none(), "{response}");
                return response["result"].clone();
            }
        }
    }
    async fn initialize(&mut self) {
        self.request("initialize",json!({"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"process-tests","version":"1"}})).await;
        self.input
            .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n")
            .await
            .unwrap();
        self.input.flush().await.unwrap();
    }
    async fn tool(&mut self, name: &str, args: Value) -> Value {
        self.request("tools/call", json!({"name":name,"arguments":args}))
            .await
    }
    async fn running(&mut self, cwd: &std::path::Path) -> String {
        let out = self
            .tool(
                "start_task",
                json!({"request_id":"run","cwd":cwd,"prompt":"hold for cancellation"}),
            )
            .await;
        let id = out["structuredContent"]["task_id"]
            .as_str()
            .unwrap()
            .to_owned();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let v = self.tool("get_task", json!({"task_id":id})).await;
                if v["structuredContent"]["status"] == "running"
                    && v["structuredContent"]["result"]["text"] == "fixture running"
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        id
    }
}

#[tokio::test]
async fn short_shutdown_reaps_hanging_startup_before_bridge_exits() {
    short_shutdown_reaps("startup", true).await;
}

#[tokio::test]
async fn short_shutdown_reaps_running_term_ignoring_peer_and_keeps_marker() {
    short_shutdown_reaps("running", false).await;
}

#[tokio::test]
async fn short_shutdown_tightens_already_running_monitor_cleanup() {
    short_shutdown_reaps("disconnected", false).await;
}

async fn short_shutdown_reaps(mode: &str, expected_clean: bool) {
    let state = tempfile::tempdir().unwrap();
    let cwd = tempfile::tempdir().unwrap();
    let mut client = Client::spawn_hanging(state.path(), false, mode);
    client.initialize().await;
    let task = client
        .tool(
            "start_task",
            json!({"request_id":"short","cwd":cwd.path(),"prompt":"hold"}),
        )
        .await;
    let peer: i32 = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Ok(pid) = std::fs::read_to_string(state.path().join("peer.pid"))
                && let Ok(pid) = pid.parse()
            {
                break pid;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    if mode != "startup" {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let value = client
                    .tool(
                        "get_task",
                        json!({"task_id":task["structuredContent"]["task_id"]}),
                    )
                    .await;
                if value["structuredContent"]["result"]["text"] == "fixture running"
                    && (mode != "disconnected"
                        || value["structuredContent"]["backend"]["state"] == "unavailable")
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    drop(client.input);
    let status = tokio::time::timeout(Duration::from_secs(3), client.child.wait())
        .await
        .unwrap()
        .unwrap();
    let reaped = unsafe { libc::kill(peer, 0) } == -1;
    if !reaped {
        unsafe {
            libc::kill(peer, libc::SIGKILL);
        }
    }
    assert!(
        reaped,
        "{mode} peer {peer} must already be reaped when bridge exits"
    );
    assert_eq!(status.success(), expected_clean, "{mode}: {status}");
    assert_eq!(
        state.path().join("run-state.json").exists(),
        !expected_clean
    );
}

#[tokio::test]
async fn stdin_eof_cancels_closes_reaps_and_clears_marker() {
    let state = tempfile::tempdir().unwrap();
    let cwd = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path(), false);
    client.initialize().await;
    client.running(cwd.path()).await;
    assert!(state.path().join("run-state.json").exists());
    drop(client.input);
    let status = tokio::time::timeout(Duration::from_secs(5), client.child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(status.success());
    assert!(!state.path().join("run-state.json").exists());
}

#[tokio::test]
async fn sigterm_with_stdin_held_open_finishes_bounded_cleanup() {
    let state = tempfile::tempdir().unwrap();
    let cwd = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path(), false);
    client.initialize().await;
    client.running(cwd.path()).await;
    let pid = client.child.id().unwrap();
    assert_eq!(unsafe { libc::kill(pid as i32, libc::SIGTERM) }, 0);
    let status = tokio::time::timeout(Duration::from_secs(5), client.child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(status.success());
    assert!(!state.path().join("run-state.json").exists());
}

#[tokio::test]
async fn uncatchable_bridge_exit_keeps_marker_and_restart_requires_manual_ack() {
    let state = tempfile::tempdir().unwrap();
    let cwd = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path(), false);
    client.initialize().await;
    client.running(cwd.path()).await;
    // Fixture has no execution descendants; retain enough direct-process identity
    // to explicitly reap/stop the peer after deliberately crashing its bridge.
    let marker: Value =
        serde_json::from_slice(&std::fs::read(state.path().join("run-state.json")).unwrap())
            .unwrap();
    let peer = marker["child_pid"].as_u64().unwrap() as i32;
    client.child.kill().await.unwrap();
    client.child.wait().await.unwrap();
    drop(client.input);
    unsafe { libc::kill(peer, libc::SIGTERM) };
    assert!(state.path().join("run-state.json").exists());
    let mut restarted = Client::spawn(state.path(), false);
    restarted.initialize().await;
    let tools = restarted.request("tools/list", json!({})).await;
    assert_eq!(tools["tools"].as_array().unwrap().len(), 11);
    let rejected = restarted
        .tool(
            "start_task",
            json!({"request_id":"new","cwd":cwd.path(),"prompt":"no replay"}),
        )
        .await;
    assert_eq!(rejected["isError"], true);
    assert_eq!(rejected["structuredContent"]["code"], "recovery_required");
    drop(restarted.input);
    let status = tokio::time::timeout(Duration::from_secs(5), restarted.child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(!status.success());
    let cleared = Command::new(env!("CARGO_BIN_EXE_dsh-mcp"))
        .arg("--acknowledge-cleanup")
        .env("DSH_MCP_STATE_DIR", state.path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .unwrap();
    assert!(cleared.success());
    assert!(!state.path().join("run-state.json").exists());
}

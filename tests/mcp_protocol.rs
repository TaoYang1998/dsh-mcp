use dsh_mcp::{
    acp::{FakeDriver, StopReasonView, fake_backend},
    config::{Config, limits},
    coordinator::{CoordHandle, spawn_coordinator},
    runtime_guard::{AcquireOutcome, RuntimeGuard},
    server::DshMcpServer,
};
use rmcp::{
    RoleClient,
    model::{CallToolRequestParams, CallToolResult},
    service::RunningService,
};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::task::JoinHandle;

struct Harness {
    client: RunningService<RoleClient, ()>,
    driver: FakeDriver,
    coord: CoordHandle,
    server: JoinHandle<()>,
    _state: tempfile::TempDir,
}

impl Harness {
    async fn new() -> Self {
        let (factory, driver) = fake_backend();
        let state = tempfile::tempdir().unwrap();
        let guard = match RuntimeGuard::acquire(state.path()).unwrap() {
            AcquireOutcome::Clean(guard) => Arc::new(guard),
            AcquireOutcome::Recovery(_) => panic!("fresh state requires no recovery"),
        };
        let config = Config {
            state_dir: state.path().into(),
            ..Config::default()
        };
        let coord = spawn_coordinator(config, factory, guard, false);
        let server_handler = DshMcpServer::new(coord.clone());
        let (client_io, server_io) = tokio::io::duplex(1 << 20);
        let server = tokio::spawn(async move {
            let running = rmcp::serve_server(server_handler, server_io).await.unwrap();
            running.waiting().await.unwrap();
        });
        let client = rmcp::serve_client((), client_io).await.unwrap();
        Self {
            client,
            driver,
            coord,
            server,
            _state: state,
        }
    }

    async fn call(&self, tool: &str, arguments: Value) -> CallToolResult {
        tokio::time::timeout(
            Duration::from_secs(3),
            self.client.peer().call_tool(
                CallToolRequestParams::new(tool.to_owned())
                    .with_arguments(arguments.as_object().unwrap().clone()),
            ),
        )
        .await
        .expect("tool must respond while prompt remains unresolved")
        .unwrap()
    }

    async fn finish(mut self) {
        let shutdown = self.coord.shutdown();
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                result = &mut shutdown => { assert!(result); break; }
                Some((_session, reply)) = self.driver.close_rx.recv() => { let _ = reply.send(Ok(())); }
            }
        }
        self.client.cancel().await.unwrap();
        self.server.await.unwrap();
    }
}

#[tokio::test]
async fn initialize_and_tools_list_publish_history_and_shared_schemas() {
    let h = Harness::new().await;
    let info = h.client.peer().peer_info().unwrap();
    assert_eq!(info.server_info.as_ref().unwrap().name, "dsh-mcp");
    assert!(info.capabilities.tools.is_some());
    let instructions = info.instructions.as_deref().unwrap();
    assert!(instructions.contains("completed"));
    assert!(instructions.contains("sandbox"));
    let list = h.client.peer().list_tools(None).await.unwrap();
    assert_eq!(list.tools.len(), 11);
    for name in [
        "list_sessions",
        "resume_task",
        "list_tasks",
        "attach_session",
        "get_status",
    ] {
        assert!(list.tools.iter().any(|t| t.name == name));
    }
    let resume = list.tools.iter().find(|t| t.name == "resume_task").unwrap();
    assert_eq!(
        resume.input_schema["properties"]["dsh_session_id"]["type"],
        "string"
    );
    assert!(
        resume.input_schema["required"]
            .as_array()
            .unwrap()
            .contains(&json!("prompt"))
    );
    for name in [
        "start_task",
        "get_task",
        "send_followup",
        "cancel_task",
        "resolve_permission",
        "close_session",
    ] {
        let tool = list.tools.iter().find(|tool| tool.name == name).unwrap();
        assert!(!tool.description.as_deref().unwrap().is_empty());
        assert!(tool.input_schema["properties"].is_object());
        assert!(
            tool.input_schema["required"]
                .as_array()
                .is_some_and(|fields| !fields.is_empty())
        );
    }
    let get = list
        .tools
        .iter()
        .find(|tool| tool.name == "get_task")
        .unwrap();
    assert_eq!(get.input_schema["properties"]["task_id"]["type"], "string");
    h.finish().await;
}

#[tokio::test]
async fn invalid_cwd_and_uuids_are_structured_business_errors() {
    let h = Harness::new().await;
    let cases = [
        (
            "start_task",
            json!({"request_id":"r1","cwd":"relative/path","prompt":"x"}),
        ),
        ("get_task", json!({"task_id":"bad"})),
        (
            "send_followup",
            json!({"request_id":"r2","task_id":"bad","prompt":"x"}),
        ),
        ("cancel_task", json!({"task_id":"bad"})),
        (
            "resolve_permission",
            json!({"task_id":uuid::Uuid::new_v4(),"permission_id":"bad","option_id":"allow"}),
        ),
        ("close_session", json!({"task_id":"bad"})),
    ];
    for (tool, arguments) in cases {
        let result = h.call(tool, arguments).await;
        assert_eq!(result.is_error, Some(true));
        let body = result
            .structured_content
            .as_ref()
            .expect("business errors are structured");
        assert_eq!(body["code"], "invalid_argument");
        assert!(body["retryable"].is_boolean());
        assert!(body.get("details").is_some());
        let fallback: Value =
            serde_json::from_str(&result.content[0].as_text().unwrap().text).unwrap();
        assert_eq!(fallback["code"], body["code"]);
        assert!(serde_json::to_vec(&result).unwrap().len() <= limits::MCP_RESPONSE_MAX);
    }
    h.finish().await;
}

#[tokio::test]
async fn malformed_parameter_structure_is_a_protocol_error() {
    let h = Harness::new().await;
    for arguments in [
        json!({}),
        json!({"task_id":5}),
        json!({"task_id":"id","cursor":-1}),
    ] {
        assert!(
            h.client
                .peer()
                .call_tool(
                    CallToolRequestParams::new("get_task")
                        .with_arguments(arguments.as_object().unwrap().clone())
                )
                .await
                .is_err()
        );
    }
    h.finish().await;
}

#[tokio::test]
async fn start_query_and_cancel_remain_responsive_with_unresolved_prompt() {
    let mut h = Harness::new().await;
    let accepted = h
        .call(
            "start_task",
            json!({"request_id":"run-1","cwd":std::env::temp_dir(),"prompt":"hello"}),
        )
        .await;
    assert_eq!(accepted.is_error, Some(false));
    let id = accepted.structured_content.as_ref().unwrap()["task_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let (_cwd, opened) =
        tokio::time::timeout(Duration::from_secs(3), h.driver.new_session_rx.recv())
            .await
            .unwrap()
            .unwrap();
    opened.send(Ok("remote-1".into())).unwrap();
    let (_session, _prompt, prompt_reply) =
        tokio::time::timeout(Duration::from_secs(3), h.driver.prompt_rx.recv())
            .await
            .unwrap()
            .unwrap();
    let running = h.call("get_task", json!({"task_id":id})).await;
    let view = running.structured_content.as_ref().unwrap();
    assert_eq!(view["status"], "running");
    assert_eq!(view["execution_state"], "in_progress");
    for arguments in [
        json!({"task_id":id,"limit":0}),
        json!({"task_id":id,"limit":101}),
        json!({"task_id":id,"cursor":view["last_cursor"].as_u64().unwrap()+1}),
    ] {
        let invalid = h.call("get_task", arguments).await;
        assert_eq!(invalid.is_error, Some(true));
        assert_eq!(
            invalid.structured_content.unwrap()["code"],
            "invalid_argument"
        );
    }

    assert!(running.content[0].as_text().unwrap().text.len() < 1024);
    let duplicate = h
        .call(
            "start_task",
            json!({"request_id":"run-1","cwd":std::env::temp_dir(),"prompt":"hello"}),
        )
        .await;
    assert_eq!(
        duplicate.structured_content.as_ref().unwrap()["task_id"],
        id
    );
    assert_eq!(
        duplicate.structured_content.as_ref().unwrap()["deduplicated"],
        true
    );
    let cancel = h.call("cancel_task", json!({"task_id":id})).await;
    assert_eq!(
        cancel.structured_content.as_ref().unwrap()["status"],
        "cancelling"
    );
    assert_eq!(h.driver.cancel_rx.recv().await.unwrap(), "remote-1");
    prompt_reply.send(Ok(StopReasonView::Cancelled)).unwrap();
    for _ in 0..30 {
        let result = h.call("get_task", json!({"task_id":id})).await;
        if result.structured_content.as_ref().unwrap()["status"] == "cancelled" {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(
        h.call("get_task", json!({"task_id":id}))
            .await
            .structured_content
            .unwrap()["status"],
        "cancelled"
    );
    h.finish().await;
}

#[tokio::test]
async fn unknown_task_is_a_business_error() {
    let h = Harness::new().await;
    let id = uuid::Uuid::new_v4();
    let unknown = h.call("get_task", json!({"task_id":id})).await;
    assert_eq!(unknown.is_error, Some(true));
    assert_eq!(
        unknown.structured_content.unwrap()["code"],
        "task_not_found"
    );
    h.finish().await;
}

#[tokio::test]
async fn status_and_resume_tools_keep_structured_contracts() {
    let mut h = Harness::new().await;
    let status = h
        .call("get_status", json!({}))
        .await
        .structured_content
        .unwrap();
    assert_eq!(status["backend"]["state"], "dormant");
    assert!(h.driver.new_session_rx.try_recv().is_err());
    let cwd = tempfile::tempdir().unwrap();
    let out=h.call("resume_task",json!({"request_id":"wire-resume","dsh_session_id":"history:+/opaque","cwd":cwd.path(),"prompt":"recall"})).await.structured_content.unwrap();
    let (remote, path, reply) = h.driver.resume_rx.recv().await.unwrap();
    assert_eq!(remote, "history:+/opaque");
    assert_eq!(path, cwd.path());
    reply.send(Ok(())).unwrap();
    let (remote, _, done) = h.driver.prompt_rx.recv().await.unwrap();
    assert_eq!(remote, "history:+/opaque");
    done.send(Ok(StopReasonView::EndTurn)).unwrap();
    loop {
        let v = h
            .call("get_task", json!({"task_id":out["task_id"]}))
            .await
            .structured_content
            .unwrap();
        if v["status"] == "completed" {
            assert_eq!(v["resumed_from_dsh_session_id"], remote);
            break;
        }
        tokio::task::yield_now().await;
    }
    h.finish().await;
}

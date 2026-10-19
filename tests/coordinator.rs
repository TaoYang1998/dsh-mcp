use dsh_mcp::{acp::*, config::Config, coordinator::*, runtime_guard::*};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::oneshot;
use uuid::Uuid;

struct Harness {
    handle: CoordHandle,
    driver: FakeDriver,
    _state: tempfile::TempDir,
    cwd: tempfile::TempDir,
}

fn harness(mut config: Config) -> Harness {
    let state = tempfile::tempdir().unwrap();
    config.state_dir = state.path().into();
    let guard = match RuntimeGuard::acquire(state.path()).unwrap() {
        AcquireOutcome::Clean(g) => Arc::new(g),
        _ => panic!("new directory must be clean"),
    };
    let (factory, driver) = fake_backend();
    Harness {
        handle: spawn_coordinator(config, factory, guard, false),
        driver,
        _state: state,
        cwd: tempfile::tempdir().unwrap(),
    }
}

fn id(v: &Value) -> Uuid {
    Uuid::parse_str(v["task_id"].as_str().unwrap()).unwrap()
}

async fn view(h: &Harness, task: Uuid, key: &str, expected: Value) -> Value {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let v = h.handle.get_task(task, 0, 100).await.unwrap();
            if v.pointer(key) == Some(&expected) {
                return v;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("task did not reach {key}={expected}"))
}

async fn running(
    h: &mut Harness,
    request: &str,
) -> (Value, oneshot::Sender<Result<StopReasonView, BackendError>>) {
    let out = h
        .handle
        .start_task(request.into(), h.cwd.path().into(), "work".into())
        .await
        .unwrap();
    let (_, reply) = h.driver.new_session_rx.recv().await.unwrap();
    reply.send(Ok(format!("remote-{request}"))).unwrap();
    let (_, _, prompt) = h.driver.prompt_rx.recv().await.unwrap();
    view(h, id(&out), "/status", json!("running")).await;
    (out, prompt)
}

async fn terminal(h: &Harness, task: Uuid) -> Value {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let value = h.handle.get_task(task, 0, 100).await.unwrap();
            if matches!(
                value["status"].as_str(),
                Some("completed" | "failed" | "cancelled")
            ) {
                return value;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn internal_permission_fault_survives_end_turn() {
    for overflow in [false, true] {
        let mut h = harness(Config::default());
        let (out, prompt) = running(&mut h, "fault").await;
        if overflow {
            for _ in 0..17 {
                h.driver
                    .event_tx
                    .send(BackendEvent::Permission {
                        epoch: 1,
                        dsh_session_id: "remote-fault".into(),
                        request: PermissionRequestView {
                            tool_call_id: None,
                            options: vec![],
                        },
                        responder: dsh_mcp::permission::PermResponder::Test(Box::new(|_| {})),
                    })
                    .await
                    .unwrap();
            }
        } else {
            h.driver
                .event_tx
                .send(BackendEvent::PermissionInvalid {
                    epoch: 1,
                    dsh_session_id: "remote-fault".into(),
                    reason: "oversized permission".into(),
                })
                .await
                .unwrap();
        }
        h.driver.cancel_rx.recv().await.unwrap();
        prompt.send(Ok(StopReasonView::EndTurn)).unwrap();
        let value = terminal(&h, id(&out)).await;
        assert_eq!(value["status"], "failed");
        assert_eq!(value["execution_state"], "stopped");
        assert_eq!(value["stop_reason"], "end_turn");
        assert_eq!(
            value["error"]["code"],
            if overflow {
                "permission_limit_exceeded"
            } else {
                "permission_payload_too_large"
            }
        );
    }
}

#[tokio::test]
async fn internal_permission_fault_after_user_cancel_survives_prompt_rejection() {
    let mut h = harness(Config::default());
    let (out, prompt) = running(&mut h, "fault").await;
    h.handle.cancel_task(id(&out)).await.unwrap();
    h.driver.cancel_rx.recv().await.unwrap();
    h.driver
        .event_tx
        .send(BackendEvent::PermissionInvalid {
            epoch: 1,
            dsh_session_id: "remote-fault".into(),
            reason: "oversized permission".into(),
        })
        .await
        .unwrap();
    prompt
        .send(Err(BackendError::new(
            BackendErrorKind::Rejected,
            "remote rejected prompt",
        )))
        .unwrap();
    let value = terminal(&h, id(&out)).await;
    assert_eq!(value["error"]["code"], "permission_payload_too_large");
    assert_eq!(value["execution_state"], "stopped");
}

#[tokio::test]
async fn missing_message_id_emits_one_compat_note_per_turn() {
    let mut h = harness(Config::default());
    let (out, prompt) = running(&mut h, "compat").await;
    for text in ["one", "two", "three"] {
        h.driver
            .event_tx
            .send(BackendEvent::Update {
                epoch: 1,
                dsh_session_id: "remote-compat".into(),
                update: SessionUpdateView::AgentMessageChunk {
                    text: text.into(),
                    message_id: None,
                },
            })
            .await
            .unwrap();
    }
    prompt.send(Ok(StopReasonView::EndTurn)).unwrap();
    let value = terminal(&h, id(&out)).await;
    assert_eq!(value["result"]["source"], "turn_text");
    assert_eq!(value["result"]["text"], "onetwothree");
    assert_eq!(
        value["events"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["type"] == "compat_note")
            .count(),
        1
    );
}

#[tokio::test]
async fn concurrent_retries_dispatch_once_and_detect_conflict() {
    let mut h = harness(Config::default());
    let cwd: PathBuf = h.cwd.path().into();
    let (a, b) = tokio::join!(
        h.handle
            .start_task("same".into(), cwd.clone(), "work".into()),
        h.handle
            .start_task("same".into(), cwd.clone(), "work".into())
    );
    let a = a.unwrap();
    let b = b.unwrap();
    assert_eq!(a["task_id"], b["task_id"]);
    assert_ne!(a["deduplicated"], b["deduplicated"]);
    let err = h
        .handle
        .start_task("same".into(), cwd, "changed".into())
        .await
        .unwrap_err();
    assert_eq!(
        serde_json::to_value(err.code).unwrap(),
        json!("request_conflict")
    );
    let (_, reply) = h.driver.new_session_rx.recv().await.unwrap();
    reply.send(Ok("one".into())).unwrap();
    let (_, _, prompt) = h.driver.prompt_rx.recv().await.unwrap();
    assert!(h.driver.new_session_rx.try_recv().is_err());
    prompt.send(Ok(StopReasonView::EndTurn)).unwrap();
    view(&h, id(&a), "/status", json!("completed")).await;
}

#[tokio::test]
async fn completed_tasks_release_active_capacity_and_followups_require_latest() {
    let mut h = harness(Config::default());
    let mut first = None;
    for i in 0..5 {
        let (out, prompt) = running(&mut h, &format!("r{i}")).await;
        prompt.send(Ok(StopReasonView::EndTurn)).unwrap();
        view(&h, id(&out), "/status", json!("completed")).await;
        first.get_or_insert(out);
    }
    let first = first.unwrap();
    let follow = h
        .handle
        .follow_up("follow".into(), id(&first), "again".into())
        .await
        .unwrap();
    assert_eq!(follow["session_id"], first["session_id"]);
    let (_, _, prompt) = h.driver.prompt_rx.recv().await.unwrap();
    // Cancelling a completed ancestor must not cancel its successor.
    h.handle.cancel_task(id(&first)).await.unwrap();
    assert!(h.driver.cancel_rx.try_recv().is_err());
    prompt.send(Ok(StopReasonView::EndTurn)).unwrap();
    view(&h, id(&follow), "/status", json!("completed")).await;
    let err = h
        .handle
        .follow_up("stale".into(), id(&first), "again".into())
        .await
        .unwrap_err();
    assert_eq!(serde_json::to_value(err.code).unwrap(), json!("stale_task"));
    h.handle.close_session(id(&follow)).await.unwrap();
    let (_, reply) = h.driver.close_rx.recv().await.unwrap();
    reply.send(Ok(())).unwrap();
    view(&h, id(&first), "/session/state", json!("closed")).await;
    assert!(
        h.handle
            .follow_up("closed".into(), id(&follow), "again".into())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn cancel_while_opening_never_prompts_and_closes_created_session() {
    let mut h = harness(Config::default());
    let out = h
        .handle
        .start_task("r".into(), h.cwd.path().into(), "work".into())
        .await
        .unwrap();
    let (_, reply) = h.driver.new_session_rx.recv().await.unwrap();
    h.handle.cancel_task(id(&out)).await.unwrap();
    reply.send(Ok("late".into())).unwrap();
    let (_, reply) = h.driver.close_rx.recv().await.unwrap();
    reply.send(Ok(())).unwrap();
    let v = view(&h, id(&out), "/status", json!("cancelled")).await;
    assert_eq!(v["execution_state"], "not_started");
    assert!(h.driver.prompt_rx.try_recv().is_err());
}

#[tokio::test]
async fn cancel_escalates_to_close_and_unknown_never_replays() {
    let cfg = Config {
        cancel_timeout: Duration::from_millis(10),
        close_timeout: Duration::from_millis(10),
        ..Config::default()
    };
    let mut h = harness(cfg);
    let (out, _prompt) = running(&mut h, "r").await;
    h.handle.cancel_task(id(&out)).await.unwrap();
    h.driver.cancel_rx.recv().await.unwrap();
    let (_, _unanswered) = h.driver.close_rx.recv().await.unwrap();
    let v = view(&h, id(&out), "/execution_state", json!("unknown")).await;
    assert_eq!(v["status"], "failed");
    assert_eq!(v["backend"]["state"], "unavailable");
    assert_eq!(v["backend"]["recovery_required"], true);
    assert!(h._state.path().join("run-state.json").exists());
    assert!(
        h.handle
            .start_task("new".into(), h.cwd.path().into(), "work".into())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn successful_close_after_cancel_is_stop_evidence() {
    let mut h = harness(Config {
        cancel_timeout: Duration::from_millis(10),
        ..Config::default()
    });
    let (out, _prompt) = running(&mut h, "r").await;
    h.handle.cancel_task(id(&out)).await.unwrap();
    h.driver.cancel_rx.recv().await.unwrap();
    let (_, reply) = h.driver.close_rx.recv().await.unwrap();
    reply.send(Ok(())).unwrap();
    let v = view(&h, id(&out), "/status", json!("cancelled")).await;
    assert_eq!(v["execution_state"], "stopped");
    assert_eq!(v["session"]["state"], "closed");
}

#[tokio::test]
async fn result_survives_event_eviction_and_response_has_bounded_valid_json() {
    let mut h = harness(Config::default());
    let (out, prompt) = running(&mut h, "r").await;
    for _ in 0..230 {
        h.driver
            .event_tx
            .send(BackendEvent::Update {
                epoch: 1,
                dsh_session_id: "remote-r".into(),
                update: SessionUpdateView::AgentMessageChunk {
                    text: "\\\"\n你好".repeat(100),
                    message_id: Some("message".into()),
                },
            })
            .await
            .unwrap();
    }
    prompt.send(Ok(StopReasonView::EndTurn)).unwrap();
    let v = view(&h, id(&out), "/status", json!("completed")).await;
    assert_eq!(v["gap"], true);
    assert!(v["dropped_count"].as_u64().unwrap() > 0);
    assert!(v["result"]["total_bytes"].as_u64().unwrap() > 65536);
    assert_eq!(v["result"]["truncated"], true);
    assert!(serde_json::to_vec(&v).unwrap().len() <= 256 * 1024);
}

#[tokio::test]
async fn two_permissions_remain_independent_and_duplicate_decision_delivers_once() {
    use dsh_mcp::permission::*;
    let mut h = harness(Config::default());
    let (out, _prompt) = running(&mut h, "r").await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    for n in 0..2 {
        let seen = seen.clone();
        h.driver
            .event_tx
            .send(BackendEvent::Permission {
                epoch: 1,
                dsh_session_id: "remote-r".into(),
                request: PermissionRequestView {
                    tool_call_id: Some(format!("tool{n}")),
                    options: vec![PermissionOptionView {
                        option_id: "actual-id".into(),
                        name: "Reject once".into(),
                        kind: PermKind::RejectOnce,
                    }],
                },
                responder: PermResponder::Test(Box::new(move |r| seen.lock().unwrap().push(r))),
            })
            .await
            .unwrap();
    }
    let v = view(
        &h,
        id(&out),
        "/pending_permissions/1/options/0/option_id",
        json!("actual-id"),
    )
    .await;
    let pid = Uuid::parse_str(
        v["pending_permissions"][0]["permission_id"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    h.handle
        .resolve_permission(id(&out), pid, "actual-id".into())
        .await
        .unwrap();
    h.handle
        .resolve_permission(id(&out), pid, "actual-id".into())
        .await
        .unwrap();
    let v = h.handle.get_task(id(&out), 0, 100).await.unwrap();
    assert_eq!(v["status"], "waiting_permission");
    assert_eq!(v["pending_permissions"].as_array().unwrap().len(), 1);
    tokio::task::yield_now().await;
    assert_eq!(seen.lock().unwrap().len(), 1);
    h.handle.cancel_task(id(&out)).await.unwrap();
    view(&h, id(&out), "/pending_permissions", json!([])).await;
}

#[tokio::test]
async fn idle_close_then_shutdown_clears_marker() {
    let mut h = harness(Config::default());
    let (out, prompt) = running(&mut h, "r").await;
    prompt.send(Ok(StopReasonView::EndTurn)).unwrap();
    view(&h, id(&out), "/status", json!("completed")).await;
    let handle = h.handle.clone();
    let shutdown = tokio::spawn(async move { handle.shutdown().await });
    let (_, reply) = h.driver.close_rx.recv().await.unwrap();
    reply.send(Ok(())).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), shutdown)
            .await
            .unwrap()
            .unwrap()
    );
    assert!(!h._state.path().join("run-state.json").exists());
}

#[tokio::test]
async fn permission_timeout_rejects_actual_option_without_allowing() {
    use dsh_mcp::permission::*;
    let mut h = harness(Config {
        permission_timeout: Duration::from_millis(10),
        ..Config::default()
    });
    let (out, _prompt) = running(&mut h, "r").await;
    let (tx, rx) = oneshot::channel();
    h.driver
        .event_tx
        .send(BackendEvent::Permission {
            epoch: 1,
            dsh_session_id: "remote-r".into(),
            request: PermissionRequestView {
                tool_call_id: None,
                options: vec![PermissionOptionView {
                    option_id: "opaque-deny-729".into(),
                    name: "Reject once".into(),
                    kind: PermKind::RejectOnce,
                }],
            },
            responder: PermResponder::Test(Box::new(move |r| {
                let _ = tx.send(r);
            })),
        })
        .await
        .unwrap();
    let response = tokio::time::timeout(Duration::from_secs(1), rx)
        .await
        .unwrap()
        .unwrap();
    let response = serde_json::to_value(response).unwrap();
    assert_eq!(response["outcome"]["optionId"], "opaque-deny-729");
    view(&h, id(&out), "/status", json!("running")).await;
}

#[tokio::test]
async fn stale_epoch_updates_and_permissions_never_attach_to_current_task() {
    use dsh_mcp::permission::*;
    let mut h = harness(Config::default());
    let (out, _prompt) = running(&mut h, "r").await;
    h.driver
        .event_tx
        .send(BackendEvent::Update {
            epoch: 999,
            dsh_session_id: "remote-r".into(),
            update: SessionUpdateView::AgentMessageChunk {
                text: "wrong epoch".into(),
                message_id: None,
            },
        })
        .await
        .unwrap();
    let (tx, rx) = oneshot::channel();
    h.driver
        .event_tx
        .send(BackendEvent::Permission {
            epoch: 999,
            dsh_session_id: "remote-r".into(),
            request: PermissionRequestView {
                tool_call_id: None,
                options: vec![],
            },
            responder: PermResponder::Test(Box::new(move |r| {
                let _ = tx.send(r);
            })),
        })
        .await
        .unwrap();
    let response = tokio::time::timeout(Duration::from_secs(1), rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(response).unwrap()["outcome"]["outcome"],
        "cancelled"
    );
    let v = h.handle.get_task(id(&out), 0, 100).await.unwrap();
    assert_eq!(v["result"]["text"], "");
    assert_eq!(v["pending_permissions"], json!([]));
}

#[tokio::test]
async fn backend_death_invalidates_all_sessions_without_replay() {
    let mut h = harness(Config::default());
    let (a, _pa) = running(&mut h, "a").await;
    let (b, _pb) = running(&mut h, "b").await;
    h.driver
        .event_tx
        .send(BackendEvent::Died {
            epoch: 1,
            reason: "fixture EOF".into(),
        })
        .await
        .unwrap();
    for out in [a, b] {
        let v = view(&h, id(&out), "/execution_state", json!("unknown")).await;
        assert_eq!(v["session"]["state"], "lost");
    }
    assert!(h.driver.new_session_rx.try_recv().is_err());
    assert!(!h.handle.shutdown().await);
    assert!(h._state.path().join("run-state.json").exists());
}

#[tokio::test(start_paused = true)]
async fn idle_ttl_and_retention_release_sessions_without_queries_extending_ttl() {
    let cfg = Config {
        session_idle_ttl: Duration::from_secs(60),
        task_retention: Duration::from_secs(120),
        ..Config::default()
    };
    let mut h = harness(cfg);
    let (out, prompt) = running(&mut h, "r").await;
    prompt.send(Ok(StopReasonView::EndTurn)).unwrap();
    view(&h, id(&out), "/status", json!("completed")).await;
    tokio::time::advance(Duration::from_secs(30)).await;
    h.handle.get_task(id(&out), 0, 1).await.unwrap();
    tokio::time::advance(Duration::from_secs(31)).await;
    let (_, reply) = h.driver.close_rx.recv().await.unwrap();
    reply.send(Ok(())).unwrap();
    view(&h, id(&out), "/session/state", json!("closed")).await;
    tokio::time::advance(Duration::from_secs(61)).await;
    tokio::task::yield_now().await;
    assert!(h.handle.get_task(id(&out), 0, 1).await.is_err());
}

#[tokio::test]
async fn active_capacity_is_shared_between_new_tasks_and_followups() {
    let mut h = harness(Config {
        max_active_tasks: 1,
        ..Config::default()
    });
    let (a, pa) = running(&mut h, "a").await;
    assert!(
        h.handle
            .start_task("b".into(), h.cwd.path().into(), "work".into())
            .await
            .is_err()
    );
    pa.send(Ok(StopReasonView::EndTurn)).unwrap();
    view(&h, id(&a), "/status", json!("completed")).await;
    let follow = h
        .handle
        .follow_up("f".into(), id(&a), "work".into())
        .await
        .unwrap();
    assert!(
        h.handle
            .start_task("c".into(), h.cwd.path().into(), "work".into())
            .await
            .is_err()
    );
    assert_eq!(follow["session_id"], a["session_id"]);
}

#[tokio::test]
async fn session_closing_stays_unavailable_after_prompt_finishes() {
    let mut h = harness(Config {
        cancel_timeout: Duration::from_millis(10),
        ..Config::default()
    });
    let (a, prompt) = running(&mut h, "a").await;
    h.handle.cancel_task(id(&a)).await.unwrap();
    let (_, close) = h.driver.close_rx.recv().await.unwrap();
    prompt.send(Ok(StopReasonView::EndTurn)).unwrap();
    let v = view(&h, id(&a), "/status", json!("completed")).await;
    assert_eq!(v["session"]["state"], "closing");
    assert_eq!(v["session"]["followup_available"], false);
    assert!(
        h.handle
            .follow_up("f".into(), id(&a), "work".into())
            .await
            .is_err()
    );
    close.send(Ok(())).unwrap();
    view(&h, id(&a), "/session/state", json!("closed")).await;
}

#[derive(Clone)]
struct Unreapable(FakeBackend);
impl Backend for Unreapable {
    async fn new_session(&self, cwd: &std::path::Path) -> Result<String, BackendError> {
        self.0.new_session(cwd).await
    }
    async fn prompt(&self, sid: &str, prompt: String) -> Result<StopReasonView, BackendError> {
        self.0.prompt(sid, prompt).await
    }
    async fn cancel(&self, sid: &str) -> Result<(), BackendError> {
        self.0.cancel(sid).await
    }
    async fn close_session(&self, sid: &str) -> Result<(), BackendError> {
        self.0.close_session(sid).await
    }
    async fn terminate_process(&self) -> bool {
        false
    }
}
struct UnreapableFactory(FakeFactory);
impl BackendFactory for UnreapableFactory {
    type B = Unreapable;
    async fn start(
        &mut self,
    ) -> Result<(Unreapable, tokio::sync::mpsc::Receiver<BackendEvent>, u32), BackendError> {
        self.0.start().await.map(|(b, e, p)| (Unreapable(b), e, p))
    }
}

#[tokio::test]
async fn failed_process_cleanup_finishes_shutdown_with_marker_retained() {
    let state = tempfile::tempdir().unwrap();
    let cwd = tempfile::tempdir().unwrap();
    let guard = match RuntimeGuard::acquire(state.path()).unwrap() {
        AcquireOutcome::Clean(g) => Arc::new(g),
        _ => unreachable!(),
    };
    let (f, mut driver) = fake_backend();
    let handle = spawn_coordinator(
        Config {
            shutdown_timeout: Duration::from_millis(50),
            ..Config::default()
        },
        UnreapableFactory(f),
        guard,
        false,
    );
    let out = handle
        .start_task("r".into(), cwd.path().into(), "work".into())
        .await
        .unwrap();
    let (_, reply) = driver.new_session_rx.recv().await.unwrap();
    reply.send(Ok("s".into())).unwrap();
    let (_, _, reply) = driver.prompt_rx.recv().await.unwrap();
    reply.send(Ok(StopReasonView::EndTurn)).unwrap();
    loop {
        if handle.get_task(id(&out), 0, 1).await.unwrap()["status"] == "completed" {
            break;
        }
        tokio::task::yield_now().await;
    }
    let close = tokio::spawn(async move {
        let (_, r) = driver.close_rx.recv().await.unwrap();
        r.send(Ok(())).unwrap();
    });
    let ok = tokio::time::timeout(Duration::from_millis(250), handle.shutdown())
        .await
        .expect("shutdown must stay bounded after cleanup fails");
    assert!(!ok);
    assert!(state.path().join("run-state.json").exists());
    close.await.unwrap();
}

#[derive(Clone)]
struct DelayedReapBackend {
    backend: FakeBackend,
    reaped: Arc<std::sync::atomic::AtomicBool>,
}
impl Backend for DelayedReapBackend {
    async fn new_session(&self, cwd: &std::path::Path) -> Result<String, BackendError> {
        self.backend.new_session(cwd).await
    }
    async fn prompt(&self, sid: &str, prompt: String) -> Result<StopReasonView, BackendError> {
        self.backend.prompt(sid, prompt).await
    }
    async fn cancel(&self, sid: &str) -> Result<(), BackendError> {
        self.backend.cancel(sid).await
    }
    async fn close_session(&self, sid: &str) -> Result<(), BackendError> {
        self.backend.close_session(sid).await
    }
    async fn terminate_process(&self) -> bool {
        tokio::time::sleep(Duration::from_millis(20)).await;
        self.reaped
            .store(true, std::sync::atomic::Ordering::Release);
        true
    }
}
struct DelayedReapFactory {
    factory: FakeFactory,
    reaped: Arc<std::sync::atomic::AtomicBool>,
}
impl BackendFactory for DelayedReapFactory {
    type B = DelayedReapBackend;
    async fn start(
        &mut self,
    ) -> Result<(Self::B, tokio::sync::mpsc::Receiver<BackendEvent>, u32), BackendError> {
        self.factory.start().await.map(|(backend, events, pid)| {
            (
                DelayedReapBackend {
                    backend,
                    reaped: self.reaped.clone(),
                },
                events,
                pid,
            )
        })
    }
}

#[tokio::test]
async fn short_shutdown_budget_reserves_time_for_process_reaping() {
    let state = tempfile::tempdir().unwrap();
    let cwd = tempfile::tempdir().unwrap();
    let guard = match RuntimeGuard::acquire(state.path()).unwrap() {
        AcquireOutcome::Clean(guard) => Arc::new(guard),
        _ => unreachable!(),
    };
    let (factory, mut driver) = fake_backend();
    let reaped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let handle = spawn_coordinator(
        Config {
            shutdown_timeout: Duration::from_millis(160),
            ..Config::default()
        },
        DelayedReapFactory {
            factory,
            reaped: reaped.clone(),
        },
        guard,
        false,
    );
    let task = handle
        .start_task("reserve-reap".into(), cwd.path().into(), "work".into())
        .await
        .unwrap();
    let (_, opened) = driver.new_session_rx.recv().await.unwrap();
    opened.send(Ok("running".into())).unwrap();
    let (_, _, _prompt) = driver.prompt_rx.recv().await.unwrap();
    let ok = tokio::time::timeout(Duration::from_millis(400), handle.shutdown())
        .await
        .unwrap();
    assert!(!ok, "forced process exit cannot prove the task stopped");
    assert!(
        reaped.load(std::sync::atomic::Ordering::Acquire),
        "shutdown returned before the direct child was reaped"
    );
    assert!(state.path().join("run-state.json").exists());
    assert!(!task["task_id"].is_null());
}

#[tokio::test]
async fn isolation_closes_other_contactable_sessions_and_keeps_their_stop_proofs() {
    let mut h = harness(Config {
        cancel_timeout: Duration::from_millis(10),
        close_timeout: Duration::from_millis(100),
        ..Config::default()
    });
    let (a, _a_prompt) = running(&mut h, "isolation-a").await;
    let (b, _b_prompt) = running(&mut h, "isolation-b").await;
    h.handle.cancel_task(id(&a)).await.unwrap();
    let (sid, close) = h.driver.close_rx.recv().await.unwrap();
    assert_eq!(sid, "remote-isolation-a");
    close
        .send(Err(BackendError::new(
            BackendErrorKind::Rejected,
            "close rejected",
        )))
        .unwrap();
    let (sid, close) = tokio::time::timeout(Duration::from_millis(500), h.driver.close_rx.recv())
        .await
        .expect("isolation must attempt to close other reachable sessions")
        .unwrap();
    assert_eq!(sid, "remote-isolation-b");
    // Queries remain available while the per-session cleanup response is pending.
    let pending = h.handle.get_task(id(&b), 0, 1).await.unwrap();
    assert_eq!(pending["backend"]["state"], "unavailable");
    assert_eq!(pending["status"], "cancelling");
    close.send(Ok(())).unwrap();
    let stopped = view(&h, id(&b), "/execution_state", json!("stopped")).await;
    assert_eq!(stopped["status"], "failed");
    let unknown = view(&h, id(&a), "/execution_state", json!("unknown")).await;
    assert_eq!(unknown["backend"]["recovery_required"], true);
    assert!(h._state.path().join("run-state.json").exists());
}

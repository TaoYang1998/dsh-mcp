use dsh_mcp::{acp::*, client::*, config::Config, coordinator::*, history::*, runtime_guard::*};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

struct Harness {
    handle: CoordHandle,
    driver: FakeDriver,
    _state: tempfile::TempDir,
    cwd: tempfile::TempDir,
}
impl Harness {
    /// 为恢复和归属测试创建独立状态目录，避免影响生产 ACP。
    fn new(config: Config) -> Self {
        let state = tempfile::tempdir().unwrap();
        let AcquireOutcome::Clean(guard) = RuntimeGuard::acquire(state.path()).unwrap() else {
            panic!()
        };
        let (factory, driver) = fake_backend();
        Self {
            handle: spawn_coordinator(config, factory, Arc::new(guard), false),
            driver,
            _state: state,
            cwd: tempfile::tempdir().unwrap(),
        }
    }
    /// 等待指定任务快照达到条件，失败时保留最后状态。
    async fn wait(&self, task: Uuid, key: &str, expected: Value) -> Value {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let v = self.handle.get_task(task, 0, 100).await.unwrap();
                if v.pointer(key) == Some(&expected) {
                    return v;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap()
    }
}
/// 提取桥接任务标识，不把 DSH 会话标识当 UUID 解析。
fn task(v: &Value) -> Uuid {
    Uuid::parse_str(v["task_id"].as_str().unwrap()).unwrap()
}

/// 恢复和追加复用后端上下文，去重不会重复发送恢复请求。
#[tokio::test]
async fn resume_deduplicates_preserves_opaque_ids_and_reuses_context() {
    let mut h = Harness::new(Config::default());
    let out = h
        .handle
        .resume_task(
            "resume-1".into(),
            "history:alpha/1".into(),
            h.cwd.path().into(),
            "remember".into(),
        )
        .await
        .unwrap();
    let (sid, cwd, reply) = h.driver.resume_rx.recv().await.unwrap();
    assert_eq!(sid, "history:alpha/1");
    assert_eq!(cwd, h.cwd.path());
    let same = h
        .handle
        .resume_task("resume-1".into(), sid.clone(), cwd, "remember".into())
        .await
        .unwrap();
    assert_eq!(same["task_id"], out["task_id"]);
    assert_eq!(same["deduplicated"], true);
    assert!(
        h.handle
            .start_task("resume-1".into(), h.cwd.path().into(), "remember".into())
            .await
            .is_err()
    );
    reply.send(Ok(())).unwrap();
    let (remote, _, done) = h.driver.prompt_rx.recv().await.unwrap();
    assert_eq!(remote, sid);
    done.send(Ok(StopReasonView::EndTurn)).unwrap();
    let v = h.wait(task(&out), "/status", json!("completed")).await;
    assert_eq!(v["parent_task_id"], Value::Null);
    assert_eq!(v["resumed_from_dsh_session_id"], sid);
    let next = h
        .handle
        .follow_up("follow".into(), task(&out), "recall".into())
        .await
        .unwrap();
    let (remote, _, done) = h.driver.prompt_rx.recv().await.unwrap();
    assert_eq!(remote, sid);
    assert_eq!(next["session_id"], out["session_id"]);
    done.send(Ok(StopReasonView::EndTurn)).unwrap();
    assert!(h.driver.new_session_rx.try_recv().is_err());
}

/// 恢复被拒绝时不新建会话，也不发送提示词。
#[tokio::test]
async fn failed_resume_never_creates_a_replacement_or_sends_prompt() {
    let mut h = Harness::new(Config::default());
    let out = h
        .handle
        .resume_task(
            "bad".into(),
            "missing".into(),
            h.cwd.path().into(),
            "work".into(),
        )
        .await
        .unwrap();
    let (_, _, reply) = h.driver.resume_rx.recv().await.unwrap();
    reply
        .send(Err(BackendError::new(
            BackendErrorKind::Rejected,
            "missing",
        )))
        .unwrap();
    let v = h.wait(task(&out), "/status", json!("failed")).await;
    assert_eq!(v["execution_state"], "not_started");
    assert!(h.driver.new_session_rx.try_recv().is_err());
    assert!(h.driver.prompt_rx.try_recv().is_err());
}

/// 历史 I/O 不阻塞健康查询，分页游标原样传递。
#[tokio::test]
async fn pending_history_does_not_block_queries_and_preserves_cursor() {
    let mut h = Harness::new(Config::default());
    let list = tokio::spawn({
        let c = h.handle.clone();
        async move { c.list_sessions(None, Some("page/+2=".into())).await }
    });
    let (_, cursor, reply) = h.driver.list_rx.recv().await.unwrap();
    assert_eq!(cursor.as_deref(), Some("page/+2="));
    let status = tokio::time::timeout(Duration::from_millis(200), h.handle.get_status())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status["backend"]["state"], "ready");
    reply
        .send(Ok(SessionPage {
            sessions: vec![SessionSummary {
                dsh_session_id: "opaque:1".into(),
                cwd: h.cwd.path().into(),
                title: None,
                updated_at: None,
            }],
            next_cursor: Some("next/+=".into()),
        }))
        .unwrap();
    assert_eq!(list.await.unwrap().unwrap()["next_cursor"], "next/+=");
}

/// 断线后只能由新连接显式接管，旧连接的迟到写入必须被拒绝。
#[tokio::test]
async fn shared_disconnect_attach_and_late_writes_are_atomic() {
    let mut h = Harness::new(Config::default());
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    h.handle.register_client(a).await.unwrap();
    h.handle.register_client(b).await.unwrap();
    let ca = h.handle.for_client(a);
    let cb = h.handle.for_client(b);
    let out = ca
        .start_task("owner".into(), h.cwd.path().into(), "work".into())
        .await
        .unwrap();
    let (_, reply) = h.driver.new_session_rx.recv().await.unwrap();
    reply.send(Ok("remote-owner".into())).unwrap();
    let (_, _, _done) = h.driver.prompt_rx.recv().await.unwrap();
    let sid = Uuid::parse_str(out["session_id"].as_str().unwrap()).unwrap();
    assert!(cb.get_task(task(&out), 0, 1).await.is_ok());
    assert!(cb.cancel_task(task(&out)).await.is_err());
    assert!(cb.attach_session(sid).await.is_err());
    h.handle.disconnect_client(a).await.unwrap();
    cb.attach_session(sid).await.unwrap();
    cb.attach_session(sid).await.unwrap();
    assert!(
        ca.start_task("late".into(), h.cwd.path().into(), "work".into())
            .await
            .is_err()
    );
    assert!(ca.cancel_task(task(&out)).await.is_err());
    let duplicate = cb
        .start_task("owner".into(), h.cwd.path().into(), "work".into())
        .await
        .unwrap();
    assert_eq!(duplicate["task_id"], out["task_id"]);
    let page = cb.list_tasks(ListTasksQuery::default()).await.unwrap();
    assert_eq!(page["tasks"][0]["owner_state"], "mine");
    cb.cancel_task(task(&out)).await.unwrap();
    assert_eq!(h.driver.cancel_rx.recv().await.unwrap(), "remote-owner");
}

/// 历史页超界必须报错，不能裁剪后端标识或跳过记录。
#[test]
fn history_budget_never_truncates_identifiers_or_cursors() {
    let page = SessionPage {
        sessions: vec![SessionSummary {
            dsh_session_id: "x".repeat(4097),
            cwd: "/tmp".into(),
            title: None,
            updated_at: None,
        }],
        next_cursor: None,
    };
    assert!(page.bounded().is_err());
}

/// 关闭后的历史允许再次恢复，但桥接任务标识必须更新。
#[tokio::test]
async fn closed_history_can_resume_again_without_reusing_bridge_task_ids() {
    let mut h = Harness::new(Config::default());
    let a = h
        .handle
        .resume_task(
            "round1".into(),
            "history:repeat".into(),
            h.cwd.path().into(),
            "one".into(),
        )
        .await
        .unwrap();
    let (_, _, reply) = h.driver.resume_rx.recv().await.unwrap();
    reply.send(Ok(())).unwrap();
    let (_, _, done) = h.driver.prompt_rx.recv().await.unwrap();
    done.send(Ok(StopReasonView::EndTurn)).unwrap();
    h.wait(task(&a), "/status", json!("completed")).await;
    assert!(
        h.handle
            .resume_task(
                "busy".into(),
                "history:repeat".into(),
                h.cwd.path().into(),
                "two".into()
            )
            .await
            .is_err()
    );
    h.handle.close_session(task(&a)).await.unwrap();
    let (_, reply) = h.driver.close_rx.recv().await.unwrap();
    reply.send(Ok(())).unwrap();
    h.wait(task(&a), "/session/state", json!("closed")).await;
    let b = h
        .handle
        .resume_task(
            "round2".into(),
            "history:repeat".into(),
            h.cwd.path().into(),
            "two".into(),
        )
        .await
        .unwrap();
    let (_, _, reply) = h.driver.resume_rx.recv().await.unwrap();
    reply.send(Ok(())).unwrap();
    let (remote, _, done) = h.driver.prompt_rx.recv().await.unwrap();
    assert_eq!(remote, "history:repeat");
    done.send(Ok(StopReasonView::EndTurn)).unwrap();
    assert_ne!(a["task_id"], b["task_id"]);
    assert_ne!(a["session_id"], b["session_id"]);
}

/// 恢复期间取消应等待恢复结果后关闭，不能发送提示词。
#[tokio::test]
async fn cancelled_resume_closes_without_ever_submitting_prompt() {
    let mut h = Harness::new(Config::default());
    let a = h
        .handle
        .resume_task(
            "cancel".into(),
            "history:cancel".into(),
            h.cwd.path().into(),
            "do not send".into(),
        )
        .await
        .unwrap();
    let (_, _, reply) = h.driver.resume_rx.recv().await.unwrap();
    h.handle.cancel_task(task(&a)).await.unwrap();
    reply.send(Ok(())).unwrap();
    let (sid, reply) = h.driver.close_rx.recv().await.unwrap();
    assert_eq!(sid, "history:cancel");
    reply.send(Ok(())).unwrap();
    let v = h.wait(task(&a), "/status", json!("cancelled")).await;
    assert_eq!(v["execution_state"], "not_started");
    assert!(h.driver.prompt_rx.try_recv().is_err());
    assert!(h.driver.new_session_rx.try_recv().is_err());
}

/// 只有当前归属方能裁决原始选项，后端隔离后恢复请求必须拒绝。
#[tokio::test]
async fn shared_permissions_require_current_owner_and_isolation_blocks_resume() {
    use dsh_mcp::permission::*;
    let mut h = Harness::new(Config::default());
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    h.handle.register_client(a).await.unwrap();
    h.handle.register_client(b).await.unwrap();
    let ca = h.handle.for_client(a);
    let cb = h.handle.for_client(b);
    let out = ca
        .start_task("perm".into(), h.cwd.path().into(), "work".into())
        .await
        .unwrap();
    let (_, reply) = h.driver.new_session_rx.recv().await.unwrap();
    reply.send(Ok("perm-sid".into())).unwrap();
    let (_, _, _done) = h.driver.prompt_rx.recv().await.unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    h.driver
        .event_tx
        .send(BackendEvent::Permission {
            epoch: 1,
            dsh_session_id: "perm-sid".into(),
            request: PermissionRequestView {
                tool_call_id: None,
                options: vec![PermissionOptionView {
                    option_id: "published:allow".into(),
                    name: "Allow once".into(),
                    kind: PermKind::AllowOnce,
                }],
            },
            responder: PermResponder::Test(Box::new(move |r| {
                let _ = tx.send(r);
            })),
        })
        .await
        .unwrap();
    let v = h
        .wait(task(&out), "/status", json!("waiting_permission"))
        .await;
    let pid = Uuid::parse_str(
        v["pending_permissions"][0]["permission_id"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert!(
        cb.resolve_permission(task(&out), pid, "published:allow".into())
            .await
            .is_err()
    );
    h.handle.disconnect_client(a).await.unwrap();
    cb.attach_session(Uuid::parse_str(out["session_id"].as_str().unwrap()).unwrap())
        .await
        .unwrap();
    assert!(
        cb.resolve_permission(task(&out), pid, "invented".into())
            .await
            .is_err()
    );
    cb.resolve_permission(task(&out), pid, "published:allow".into())
        .await
        .unwrap();
    assert!(rx.await.is_ok());
    h.driver
        .event_tx
        .send(BackendEvent::Died {
            epoch: 1,
            reason: "test failure".into(),
        })
        .await
        .unwrap();
    h.wait(task(&out), "/backend/state", json!("unavailable"))
        .await;
    assert!(
        cb.resume_task(
            "unsafe-resume".into(),
            "perm-sid".into(),
            h.cwd.path().into(),
            "work".into()
        )
        .await
        .is_err()
    );
    let status = cb.get_status().await.unwrap();
    assert_eq!(status["backend"]["recovery_required"], true);
    assert!(h._state.path().join("run-state.json").exists());
    assert!(h.driver.resume_rx.try_recv().is_err());
}

/// 会话 TTL 与记录保留独立，记录到期时查询和去重同步失效。
#[tokio::test(start_paused = true)]
async fn retention_expiry_removes_query_and_dedup_but_does_not_destroy_dsh_history() {
    let mut h = Harness::new(Config {
        session_idle_ttl: Duration::from_secs(30),
        task_retention: Duration::from_secs(60),
        ..Config::default()
    });
    let a = h
        .handle
        .start_task("expiring".into(), h.cwd.path().into(), "work".into())
        .await
        .unwrap();
    let (_, r) = h.driver.new_session_rx.recv().await.unwrap();
    r.send(Ok("expired-history".into())).unwrap();
    let (_, _, r) = h.driver.prompt_rx.recv().await.unwrap();
    r.send(Ok(StopReasonView::EndTurn)).unwrap();
    h.wait(task(&a), "/status", json!("completed")).await;
    tokio::time::advance(Duration::from_secs(31)).await;
    let (_, r) = h.driver.close_rx.recv().await.unwrap();
    r.send(Ok(())).unwrap();
    h.wait(task(&a), "/session/state", json!("closed")).await;
    tokio::time::advance(Duration::from_secs(31)).await;
    tokio::task::yield_now().await;
    assert!(h.handle.get_task(task(&a), 0, 1).await.is_err());
    let new = h
        .handle
        .start_task("expiring".into(), h.cwd.path().into(), "work".into())
        .await
        .unwrap();
    assert_ne!(new["task_id"], a["task_id"]);
    assert_eq!(new["deduplicated"], false);
}

/// 记录容量不足时提前回收旧终态任务，同时移除其去重记录。
#[tokio::test]
async fn record_capacity_can_evict_before_retention_deadline_and_removes_dedup() {
    let mut h = Harness::new(Config {
        max_open_sessions: 1,
        max_active_tasks: 1,
        max_task_records: 2,
        ..Config::default()
    });
    let mut old = None;
    for n in 0..3 {
        let out = h
            .handle
            .start_task(format!("pressure-{n}"), h.cwd.path().into(), "work".into())
            .await
            .unwrap();
        let (_, reply) = h.driver.new_session_rx.recv().await.unwrap();
        reply.send(Ok(format!("remote-{n}"))).unwrap();
        let (_, _, reply) = h.driver.prompt_rx.recv().await.unwrap();
        reply.send(Ok(StopReasonView::EndTurn)).unwrap();
        h.wait(task(&out), "/status", json!("completed")).await;
        h.handle.close_session(task(&out)).await.unwrap();
        let (_, reply) = h.driver.close_rx.recv().await.unwrap();
        reply.send(Ok(())).unwrap();
        h.wait(task(&out), "/session/state", json!("closed")).await;
        if n == 0 {
            old = Some(out);
        }
    }
    let old = old.unwrap();
    assert!(h.handle.get_task(task(&old), 0, 1).await.is_err());
    let new = h
        .handle
        .start_task("pressure-0".into(), h.cwd.path().into(), "work".into())
        .await
        .unwrap();
    assert_eq!(new["deduplicated"], false);
    assert_ne!(old["task_id"], new["task_id"]);
}

/// 分页不会重复记录，也不接受其他筛选条件的游标。
#[tokio::test]
async fn task_pagination_is_bound_to_scope_and_cwd() {
    let mut h = Harness::new(Config::default());
    for n in 0..2 {
        let _ = h
            .handle
            .start_task(format!("page-{n}"), h.cwd.path().into(), "work".into())
            .await
            .unwrap();
        let (_, reply) = h.driver.new_session_rx.recv().await.unwrap();
        reply.send(Ok(format!("page-remote-{n}"))).unwrap();
        let (_, _, done) = h.driver.prompt_rx.recv().await.unwrap();
        done.send(Ok(StopReasonView::EndTurn)).unwrap();
    }
    let first = h
        .handle
        .list_tasks(ListTasksQuery {
            limit: 1,
            ..ListTasksQuery::default()
        })
        .await
        .unwrap();
    let cursor = first["next_cursor"].as_str().unwrap().to_owned();
    let second = h
        .handle
        .list_tasks(ListTasksQuery {
            limit: 1,
            cursor: Some(cursor.clone()),
            ..ListTasksQuery::default()
        })
        .await
        .unwrap();
    assert_ne!(first["tasks"][0]["task_id"], second["tasks"][0]["task_id"]);
    assert!(second["next_cursor"].is_null());
    assert!(
        h.handle
            .list_tasks(ListTasksQuery {
                scope: TaskScope::All,
                cursor: Some(cursor),
                ..ListTasksQuery::default()
            })
            .await
            .is_err()
    );
}

/// 无效的恢复标识应在接单前拒绝，不能使健康后端进入整体隔离。
#[tokio::test]
async fn oversized_resume_identifier_is_rejected_before_admission() {
    let h = Harness::new(Config::default());
    assert!(
        h.handle
            .resume_task(
                "oversized-id".into(),
                "x".repeat(1025),
                h.cwd.path().into(),
                "work".into()
            )
            .await
            .is_err()
    );
    assert_eq!(
        h.handle.get_status().await.unwrap()["backend"]["state"],
        "dormant"
    );
}

/// 调用方取消等待后，后台历史请求也必须保持全局有界。
#[tokio::test]
async fn cancelled_history_callers_cannot_accumulate_unbounded_backend_requests() {
    let mut h = Harness::new(Config::default());
    let mut calls = Vec::new();
    let mut replies = Vec::new();
    for _ in 0..32 {
        let handle = h.handle.clone();
        calls.push(tokio::spawn(async move {
            handle.list_sessions(None, None).await
        }));
        let (_, _, reply) = h.driver.list_rx.recv().await.unwrap();
        replies.push(reply);
    }
    for call in calls {
        call.abort();
    }
    let result = tokio::time::timeout(
        Duration::from_millis(200),
        h.handle.list_sessions(None, None),
    )
    .await
    .unwrap();
    assert!(result.is_err());
    assert_eq!(
        h.handle.get_status().await.unwrap()["backend"]["state"],
        "ready"
    );
    for reply in replies {
        let _ = reply.send(Ok(SessionPage {
            sessions: vec![],
            next_cursor: None,
        }));
    }
}

use crate::config::limits;
use crate::task::{BridgeSessionId, PermissionId, TaskId};
use agent_client_protocol::Responder;
use agent_client_protocol::schema::v1::{RequestPermissionOutcome, RequestPermissionResponse};
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::time::Instant;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PermKind {
    AllowOnce,
    AllowAlways,
    RejectOnce,
    RejectAlways,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PermissionOptionView {
    pub option_id: String,
    pub name: String,
    pub kind: PermKind,
}

pub enum PermResponder {
    Real(Responder<RequestPermissionResponse>),
    Tracked {
        responder: Responder<RequestPermissionResponse>,
        ack: tokio::sync::oneshot::Receiver<Result<(), String>>,
    },
    Test(Box<dyn FnOnce(RequestPermissionResponse) + Send>),
}
impl PermResponder {
    /// Completes only once the response newline was physically written to ACP.
    /// The caller supplies its control-operation deadline around this future.
    pub async fn reply_confirmed(self, response: RequestPermissionResponse) -> Result<(), String> {
        match self {
            Self::Tracked { responder, ack } => {
                responder
                    .respond(response)
                    .map_err(|error| error.to_string())?;
                ack.await
                    .map_err(|_| "ACP response writer closed".to_owned())?
            }
            other => other.reply(response),
        }
    }
    pub fn reply(self, response: RequestPermissionResponse) -> Result<(), String> {
        match self {
            Self::Real(r) => r.respond(response).map_err(|error| error.to_string()),
            Self::Tracked { responder, .. } => responder
                .respond(response)
                .map_err(|error| error.to_string()),
            Self::Test(f) => {
                f(response);
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PendingPermissionView {
    pub permission_id: PermissionId,
    pub tool_call_id: Option<String>,
    pub tool_summary: Option<String>,
    pub details_incomplete: bool,
    pub options: Vec<PermissionOptionView>,
    pub expires_at: String,
}

pub struct PendingPermission {
    pub view: PendingPermissionView,
    pub task_id: TaskId,
    pub session_id: BridgeSessionId,
    pub epoch: u64,
    pub deadline: Instant,
    responder: PermResponder,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionKind {
    Selected,
    AutoDenied,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionDelivery {
    Pending,
    #[serde(rename = "sent")]
    Delivered,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Decision {
    pub kind: DecisionKind,
    pub option_id: Option<String>,
    pub delivery: DecisionDelivery,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery_error: Option<String>,
}
impl Decision {
    fn new(kind: DecisionKind, option_id: Option<String>) -> Self {
        Self {
            kind,
            option_id,
            delivery: DecisionDelivery::Pending,
            delivery_error: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DecidedPermissionView {
    pub permission_id: PermissionId,
    pub decision: Decision,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermError {
    NotFound,
    InvalidOption,
    AlreadyResolved,
    Expired,
    Cancelled,
    PayloadTooLarge,
    LimitExceeded,
}
impl std::fmt::Display for PermError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for PermError {}

struct DecidedRecord {
    task_id: TaskId,
    decision: Decision,
    expired: bool,
}

#[derive(Default)]
pub struct PermissionManager {
    pending: HashMap<PermissionId, PendingPermission>,
    decided: HashMap<PermissionId, DecidedRecord>,
    decided_order: HashMap<TaskId, VecDeque<PermissionId>>,
}
impl PermissionManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Failed registrations consume their responder with ACP cancelled, so callers
    /// can enter task cancellation without losing the once-only reply channel.
    #[allow(clippy::too_many_arguments)]
    pub fn register(
        &mut self,
        task_id: TaskId,
        session_id: BridgeSessionId,
        epoch: u64,
        tool_call_id: Option<String>,
        tool_summary: Option<String>,
        details_incomplete: bool,
        options: Vec<PermissionOptionView>,
        responder: PermResponder,
        deadline: Instant,
        expires_at: String,
    ) -> Result<PendingPermissionView, PermError> {
        let view = PendingPermissionView {
            permission_id: Uuid::new_v4(),
            tool_call_id,
            tool_summary,
            details_incomplete,
            options,
            expires_at,
        };
        let error = if view.options.len() > limits::PERM_OPTIONS_MAX
            || view
                .options
                .iter()
                .any(|option| option.option_id.len() > limits::PERM_OPTION_ID_MAX)
            || serde_json::to_vec(&view).unwrap().len() > limits::PERM_ITEM_MAX
        {
            Some(PermError::PayloadTooLarge)
        } else if self.pending_count(task_id) >= limits::PENDING_PERMS_MAX {
            Some(PermError::LimitExceeded)
        } else {
            let existing = self
                .pending
                .values()
                .filter(|pending| pending.task_id == task_id);
            let (count, bytes) = existing.fold((0usize, 0usize), |(count, bytes), pending| {
                (
                    count + 1,
                    bytes + serde_json::to_vec(&pending.view).unwrap().len(),
                )
            });
            // n existing entries plus one new entry require n commas and two
            // array brackets. Options remain intact if registration is refused.
            let snapshot_bytes = bytes + serde_json::to_vec(&view).unwrap().len() + count + 2;
            (snapshot_bytes > limits::PERM_SNAPSHOT_BUDGET).then_some(PermError::PayloadTooLarge)
        };
        if let Some(error) = error {
            if let Err(delivery_error) = responder.reply(RequestPermissionResponse::new(
                RequestPermissionOutcome::Cancelled,
            )) {
                tracing::warn!(%delivery_error, "failed to deliver cancelled permission registration");
            }
            return Err(error);
        }
        self.pending.insert(
            view.permission_id,
            PendingPermission {
                view: view.clone(),
                task_id,
                session_id,
                epoch,
                deadline,
                responder,
            },
        );
        Ok(view)
    }

    fn record_decision(
        &mut self,
        id: PermissionId,
        task_id: TaskId,
        decision: Decision,
        expired: bool,
    ) {
        self.decided.insert(
            id,
            DecidedRecord {
                task_id,
                decision,
                expired,
            },
        );
        let order = self.decided_order.entry(task_id).or_default();
        order.push_back(id);
        while order.len() > limits::DECIDED_PERMS_KEEP {
            if let Some(evicted) = order.pop_front() {
                self.decided.remove(&evicted);
            }
        }
    }

    pub fn decide(
        &mut self,
        id: PermissionId,
        option_id: &str,
    ) -> Result<(Decision, Option<PermResponder>), PermError> {
        if let Some(record) = self.decided.get(&id) {
            if record.expired {
                return Err(PermError::Expired);
            }
            return match record.decision.kind {
                DecisionKind::Selected
                    if record.decision.option_id.as_deref() == Some(option_id) =>
                {
                    Ok((record.decision.clone(), None))
                }
                DecisionKind::Selected => Err(PermError::AlreadyResolved),
                DecisionKind::AutoDenied => Err(PermError::Expired),
                DecisionKind::Cancelled => Err(PermError::Cancelled),
            };
        }
        let pending = self.pending.get(&id).ok_or(PermError::NotFound)?;
        if !pending
            .view
            .options
            .iter()
            .any(|option| option.option_id == option_id)
        {
            return Err(PermError::InvalidOption);
        }
        let pending = self
            .pending
            .remove(&id)
            .expect("checked pending permission");
        let decision = Decision::new(DecisionKind::Selected, Some(option_id.into()));
        self.record_decision(id, pending.task_id, decision.clone(), false);
        Ok((decision, Some(pending.responder)))
    }

    pub fn cancel_for_task(&mut self, task_id: TaskId) -> Vec<(PermissionId, PermResponder)> {
        let ids: Vec<_> = self
            .pending
            .iter()
            .filter(|(_, p)| p.task_id == task_id)
            .map(|(id, _)| *id)
            .collect();
        let mut responders = Vec::with_capacity(ids.len());
        for id in ids {
            let pending = self.pending.remove(&id).expect("owned pending permission");
            self.record_decision(
                id,
                task_id,
                Decision::new(DecisionKind::Cancelled, None),
                false,
            );
            responders.push((id, pending.responder));
        }
        responders
    }

    pub fn expire_one(&mut self, id: PermissionId) -> Option<(TaskId, Decision, PermResponder)> {
        let pending = self.pending.remove(&id)?;
        let reject = pending
            .view
            .options
            .iter()
            .find(|option| option.kind == PermKind::RejectOnce)
            .map(|option| option.option_id.clone());
        let decision = Decision::new(
            if reject.is_some() {
                DecisionKind::AutoDenied
            } else {
                DecisionKind::Cancelled
            },
            reject,
        );
        self.record_decision(id, pending.task_id, decision.clone(), true);
        Some((pending.task_id, decision, pending.responder))
    }

    pub fn pending_snapshot(&self, task_id: TaskId) -> Vec<PendingPermissionView> {
        let mut pending: Vec<_> = self
            .pending
            .values()
            .filter(|p| p.task_id == task_id)
            .collect();
        pending.sort_by_key(|p| (p.deadline, p.view.permission_id));
        pending.into_iter().map(|p| p.view.clone()).collect()
    }
    pub fn pending_count(&self, task_id: TaskId) -> usize {
        self.pending
            .values()
            .filter(|p| p.task_id == task_id)
            .count()
    }
    pub fn task_of(&self, id: PermissionId) -> Option<TaskId> {
        self.pending
            .get(&id)
            .map(|p| p.task_id)
            .or_else(|| self.decided.get(&id).map(|record| record.task_id))
    }
    pub fn expired(&self, now: Instant) -> Vec<PermissionId> {
        let mut expired: Vec<_> = self
            .pending
            .iter()
            .filter(|(_, p)| p.deadline <= now)
            .map(|(id, p)| (p.deadline, *id))
            .collect();
        expired.sort_unstable();
        expired.into_iter().map(|(_, id)| id).collect()
    }
    pub fn mark_delivery(&mut self, id: PermissionId, result: Result<(), String>) -> bool {
        let Some(record) = self.decided.get_mut(&id) else {
            return false;
        };
        if record.decision.delivery != DecisionDelivery::Pending {
            return false;
        }
        match result {
            Ok(()) => record.decision.delivery = DecisionDelivery::Delivered,
            Err(message) => {
                record.decision.delivery = DecisionDelivery::Failed;
                record.decision.delivery_error = Some(
                    crate::events::truncate_json_text(&message, limits::ERROR_SUMMARY_MAX)
                        .0
                        .into(),
                );
            }
        }
        true
    }
    pub fn decision_snapshot(&self, task_id: TaskId) -> Vec<DecidedPermissionView> {
        self.decided_order
            .get(&task_id)
            .into_iter()
            .flatten()
            .filter_map(|id| {
                self.decided.get(id).map(|record| DecidedPermissionView {
                    permission_id: *id,
                    decision: record.decision.clone(),
                })
            })
            .collect()
    }
    /// Eviction forgets routing and replay records, returning all unanswered channels.
    pub fn forget_task(&mut self, task_id: TaskId) -> Vec<(PermissionId, PermResponder)> {
        let ids: Vec<_> = self
            .pending
            .iter()
            .filter(|(_, p)| p.task_id == task_id)
            .map(|(id, _)| *id)
            .collect();
        let responders = ids
            .into_iter()
            .map(|id| {
                (
                    id,
                    self.pending
                        .remove(&id)
                        .expect("owned pending permission")
                        .responder,
                )
            })
            .collect();
        if let Some(order) = self.decided_order.remove(&task_id) {
            for id in order {
                self.decided.remove(&id);
            }
        }
        responders
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_client_protocol::schema::v1::{
        RequestPermissionOutcome, RequestPermissionResponse, SelectedPermissionOutcome,
    };
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    fn opts() -> Vec<PermissionOptionView> {
        vec![
            PermissionOptionView {
                option_id: "allow".into(),
                name: "Allow once".into(),
                kind: PermKind::AllowOnce,
            },
            PermissionOptionView {
                option_id: "reject".into(),
                name: "Reject once".into(),
                kind: PermKind::RejectOnce,
            },
        ]
    }

    fn capture() -> (PermResponder, Arc<Mutex<Vec<RequestPermissionOutcome>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen2 = seen.clone();
        (
            PermResponder::Test(Box::new(move |resp| {
                seen2.lock().unwrap().push(resp.outcome);
            })),
            seen,
        )
    }

    fn register_one(
        m: &mut PermissionManager,
        task: TaskId,
    ) -> (PermissionId, Arc<Mutex<Vec<RequestPermissionOutcome>>>) {
        let (r, seen) = capture();
        let v = m
            .register(
                task,
                BridgeSessionId::new_v4(),
                1,
                Some("tc-1".into()),
                Some("write file /tmp/x".into()),
                false,
                opts(),
                r,
                Instant::now() + Duration::from_secs(600),
                "2026-10-19T00:10:00Z".into(),
            )
            .unwrap();
        (v.permission_id, seen)
    }

    #[test]
    fn register_and_snapshot() {
        let mut m = PermissionManager::new();
        let task = TaskId::new_v4();
        let (pid, _) = register_one(&mut m, task);
        assert_eq!(m.pending_count(task), 1);
        let snap = m.pending_snapshot(task);
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].permission_id, pid);
        assert_eq!(snap[0].tool_call_id.as_deref(), Some("tc-1"));
        assert_eq!(m.task_of(pid), Some(task));
    }

    #[test]
    fn decide_once_and_idempotent_replay() {
        let mut m = PermissionManager::new();
        let task = TaskId::new_v4();
        let (pid, seen) = register_one(&mut m, task);
        let (d, responder) = m.decide(pid, "allow").unwrap();
        assert_eq!(d.kind, DecisionKind::Selected);
        assert_eq!(d.option_id.as_deref(), Some("allow"));
        responder
            .expect("first decision carries the responder")
            .reply(RequestPermissionResponse::new(
                RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new("allow")),
            ))
            .unwrap();
        assert_eq!(seen.lock().unwrap().len(), 1);
        // 同选项重复提交：幂等返回已有裁决，不再有 responder
        let (d2, r2) = m.decide(pid, "allow").unwrap();
        assert_eq!(d2.kind, DecisionKind::Selected);
        assert!(r2.is_none());
        // 改选：拒绝
        assert!(matches!(
            m.decide(pid, "reject"),
            Err(PermError::AlreadyResolved)
        ));
        assert_eq!(seen.lock().unwrap().len(), 1); // 未二次回复
    }

    #[test]
    fn decide_unknown_option_and_unknown_id() {
        let mut m = PermissionManager::new();
        let task = TaskId::new_v4();
        let (pid, _) = register_one(&mut m, task);
        assert!(matches!(
            m.decide(pid, "not-an-option"),
            Err(PermError::InvalidOption)
        ));
        assert!(matches!(
            m.decide(PermissionId::new_v4(), "allow"),
            Err(PermError::NotFound)
        ));
    }

    #[test]
    fn timeout_auto_denies_with_reject_once() {
        let mut m = PermissionManager::new();
        let task = TaskId::new_v4();
        let (pid, seen) = register_one(&mut m, task);
        let (t, decision, responder) = m.expire_one(pid).unwrap();
        assert_eq!(t, task);
        assert_eq!(decision.kind, DecisionKind::AutoDenied);
        assert_eq!(decision.option_id.as_deref(), Some("reject"));
        responder
            .reply(RequestPermissionResponse::new(
                RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new("reject")),
            ))
            .unwrap();
        assert_eq!(seen.lock().unwrap().len(), 1);
        // 已过期后用户再裁决
        assert!(matches!(m.decide(pid, "allow"), Err(PermError::Expired)));
    }

    #[test]
    fn timeout_without_reject_option_cancels() {
        let mut m = PermissionManager::new();
        let task = TaskId::new_v4();
        let (r, _seen) = capture();
        let v = m
            .register(
                task,
                BridgeSessionId::new_v4(),
                1,
                None,
                None,
                true,
                vec![PermissionOptionView {
                    option_id: "allow".into(),
                    name: "Allow".into(),
                    kind: PermKind::AllowOnce,
                }],
                r,
                Instant::now() + Duration::from_secs(600),
                "t".into(),
            )
            .unwrap();
        let (_, decision, _) = m.expire_one(v.permission_id).unwrap();
        assert_eq!(decision.kind, DecisionKind::Cancelled);
        assert_eq!(decision.option_id, None);
    }

    #[test]
    fn cancel_for_task_replies_cancelled_to_all_pending() {
        let mut m = PermissionManager::new();
        let task = TaskId::new_v4();
        let (_p1, seen1) = register_one(&mut m, task);
        let (_p2, seen2) = register_one(&mut m, task);
        let responders = m.cancel_for_task(task);
        assert_eq!(responders.len(), 2);
        for (_id, r) in responders {
            r.reply(RequestPermissionResponse::new(
                RequestPermissionOutcome::Cancelled,
            ))
            .unwrap();
        }
        assert!(matches!(
            seen1.lock().unwrap()[0],
            RequestPermissionOutcome::Cancelled
        ));
        assert!(matches!(
            seen2.lock().unwrap()[0],
            RequestPermissionOutcome::Cancelled
        ));
        assert_eq!(m.pending_count(task), 0);
    }

    #[test]
    fn payload_limits_enforced() {
        let mut m = PermissionManager::new();
        let task = TaskId::new_v4();
        let (r, _) = capture();
        let too_many: Vec<PermissionOptionView> = (0..9)
            .map(|i| PermissionOptionView {
                option_id: format!("o{i}"),
                name: "x".into(),
                kind: PermKind::AllowOnce,
            })
            .collect();
        assert!(matches!(
            m.register(
                task,
                BridgeSessionId::new_v4(),
                1,
                None,
                None,
                false,
                too_many,
                r,
                Instant::now(),
                "t".into()
            ),
            Err(PermError::PayloadTooLarge)
        ));
        let (r2, _) = capture();
        let long_id = vec![PermissionOptionView {
            option_id: "x".repeat(257),
            name: "x".into(),
            kind: PermKind::AllowOnce,
        }];
        assert!(matches!(
            m.register(
                task,
                BridgeSessionId::new_v4(),
                1,
                None,
                None,
                false,
                long_id,
                r2,
                Instant::now(),
                "t".into()
            ),
            Err(PermError::PayloadTooLarge)
        ));
    }

    #[test]
    fn pending_limit_per_task() {
        let mut m = PermissionManager::new();
        let task = TaskId::new_v4();
        for _ in 0..16 {
            register_one(&mut m, task);
        }
        let (r, _) = capture();
        assert!(matches!(
            m.register(
                task,
                BridgeSessionId::new_v4(),
                1,
                None,
                None,
                false,
                opts(),
                r,
                Instant::now(),
                "t".into()
            ),
            Err(PermError::LimitExceeded)
        ));
    }

    #[test]
    fn decided_ring_eviction() {
        let mut m = PermissionManager::new();
        let task = TaskId::new_v4();
        let mut first_pid = None;
        for i in 0..70 {
            let (pid, _) = register_one(&mut m, task);
            if i == 0 {
                first_pid = Some(pid);
            }
            m.decide(pid, "allow").unwrap();
        }
        // 已决记录只保留 64 个，最早的被淘汰后按 not_found 处理
        assert!(matches!(
            m.decide(first_pid.unwrap(), "allow"),
            Err(PermError::NotFound)
        ));
    }
}

#[cfg(test)]
mod boundary_tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    fn register(m: &mut PermissionManager, task: TaskId) -> (PermissionId, Arc<Mutex<usize>>) {
        let count = Arc::new(Mutex::new(0));
        let sink = count.clone();
        let v = m
            .register(
                task,
                BridgeSessionId::new_v4(),
                1,
                None,
                None,
                false,
                vec![PermissionOptionView {
                    option_id: "allow".into(),
                    name: "Allow".into(),
                    kind: PermKind::AllowOnce,
                }],
                PermResponder::Test(Box::new(move |_| {
                    *sink.lock().unwrap() += 1;
                })),
                Instant::now() + Duration::from_secs(5),
                "2026-10-19T00:00:05Z".into(),
            )
            .unwrap();
        (v.permission_id, count)
    }

    #[test]
    fn decisions_are_retained_per_task_and_forgotten_with_task() {
        let mut m = PermissionManager::new();
        let first_task = TaskId::new_v4();
        let second_task = TaskId::new_v4();
        let (first, _) = register(&mut m, first_task);
        m.decide(first, "allow").unwrap();
        for _ in 0..70 {
            let (id, _) = register(&mut m, second_task);
            m.decide(id, "allow").unwrap();
        }
        assert_eq!(m.task_of(first), Some(first_task));
        assert_eq!(
            m.decision_snapshot(second_task).len(),
            limits::DECIDED_PERMS_KEEP
        );
        assert!(m.forget_task(first_task).is_empty());
        assert_eq!(m.task_of(first), None);
        assert!(m.decision_snapshot(first_task).is_empty());
    }

    #[test]
    fn delivery_status_survives_replays_and_is_recorded_once() {
        let mut m = PermissionManager::new();
        let task = TaskId::new_v4();
        let (id, _) = register(&mut m, task);
        let (d, _) = m.decide(id, "allow").unwrap();
        assert_eq!(d.delivery, DecisionDelivery::Pending);
        assert!(m.mark_delivery(id, Err("broken channel".into())));
        assert!(!m.mark_delivery(id, Ok(())));
        let (again, responder) = m.decide(id, "allow").unwrap();
        assert!(responder.is_none());
        assert_eq!(again.delivery, DecisionDelivery::Failed);
        assert_eq!(again.delivery_error.as_deref(), Some("broken channel"));
    }

    #[test]
    fn deadlines_and_forget_do_not_lose_pending_responders() {
        let mut m = PermissionManager::new();
        let task = TaskId::new_v4();
        let (id, count) = register(&mut m, task);
        assert!(m.expired(Instant::now()).is_empty());
        assert_eq!(m.expired(Instant::now() + Duration::from_secs(6)), vec![id]);
        let responders = m.forget_task(task);
        assert_eq!(responders.len(), 1);
        responders
            .into_iter()
            .next()
            .unwrap()
            .1
            .reply(RequestPermissionResponse::new(
                RequestPermissionOutcome::Cancelled,
            ))
            .unwrap();
        assert_eq!(*count.lock().unwrap(), 1);
        assert_eq!(m.task_of(id), None);
    }

    #[test]
    fn registration_failure_replies_cancelled_once() {
        let mut m = PermissionManager::new();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        let error = m.register(
            TaskId::new_v4(),
            BridgeSessionId::new_v4(),
            1,
            None,
            None,
            false,
            vec![PermissionOptionView {
                option_id: "x".repeat(257),
                name: "Allow".into(),
                kind: PermKind::AllowOnce,
            }],
            PermResponder::Test(Box::new(move |r| {
                sink.lock().unwrap().push(r.outcome);
            })),
            Instant::now(),
            "t".into(),
        );
        assert!(matches!(error, Err(PermError::PayloadTooLarge)));
        assert!(matches!(
            seen.lock().unwrap().as_slice(),
            [RequestPermissionOutcome::Cancelled]
        ));
    }
    #[test]
    fn whole_snapshot_budget_includes_array_brackets_and_commas() {
        let mut manager = PermissionManager::new();
        let task = TaskId::new_v4();
        let session = BridgeSessionId::new_v4();
        let mut options = vec![PermissionOptionView {
            option_id: "once".into(),
            name: String::new(),
            kind: PermKind::AllowOnce,
        }];
        let template = PendingPermissionView {
            permission_id: PermissionId::new_v4(),
            tool_call_id: None,
            tool_summary: None,
            details_incomplete: false,
            options: options.clone(),
            expires_at: "t".into(),
        };
        let overhead = serde_json::to_vec(&template).unwrap().len();
        options[0].name = "x".repeat(limits::PERM_ITEM_MAX - overhead);
        let capture = || {
            let outcomes = Arc::new(Mutex::new(Vec::new()));
            let sink = outcomes.clone();
            (
                PermResponder::Test(Box::new(move |response| {
                    sink.lock().unwrap().push(response.outcome);
                })),
                outcomes,
            )
        };
        for _ in 0..15 {
            let (responder, _) = capture();
            let view = manager
                .register(
                    task,
                    session,
                    1,
                    None,
                    None,
                    false,
                    options.clone(),
                    responder,
                    Instant::now(),
                    "t".into(),
                )
                .unwrap();
            assert_eq!(
                serde_json::to_vec(&view).unwrap().len(),
                limits::PERM_ITEM_MAX
            );
        }
        let (responder, outcomes) = capture();
        assert!(matches!(
            manager.register(
                task,
                session,
                1,
                None,
                None,
                false,
                options,
                responder,
                Instant::now(),
                "t".into()
            ),
            Err(PermError::PayloadTooLarge)
        ));
        assert!(matches!(
            outcomes.lock().unwrap().as_slice(),
            [RequestPermissionOutcome::Cancelled]
        ));
        assert_eq!(manager.pending_count(task), 15);
        assert!(
            serde_json::to_vec(&manager.pending_snapshot(task))
                .unwrap()
                .len()
                <= limits::PERM_SNAPSHOT_BUDGET
        );
    }
    #[test]
    fn confirmed_delivery_serializes_as_sent() {
        assert_eq!(
            serde_json::to_value(DecisionDelivery::Delivered).unwrap(),
            "sent"
        );
    }
}

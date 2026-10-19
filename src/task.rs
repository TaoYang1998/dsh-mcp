use crate::config::limits;
use crate::events::{EventBuffer, ResultAggregator, TaskResult};
use serde::Serialize;
use serde_json::Value;
use std::path::PathBuf;
use std::time::Instant;
use uuid::Uuid;

pub type TaskId = Uuid;
pub type BridgeSessionId = Uuid;
pub type PermissionId = Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Starting,
    Running,
    WaitingPermission,
    Cancelling,
    Completed,
    Failed,
    Cancelled,
}

impl TaskStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
    pub fn is_active(self) -> bool {
        matches!(
            self,
            Self::Starting | Self::Running | Self::WaitingPermission | Self::Cancelling
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionState {
    NotStarted,
    InProgress,
    Stopped,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    Opening,
    Idle,
    Busy,
    Closing,
    Closed,
    Lost,
}

impl SessionState {
    pub fn accepts_prompt(self) -> bool {
        matches!(self, Self::Idle)
    }
    pub fn counts_open(self) -> bool {
        matches!(
            self,
            Self::Opening | Self::Idle | Self::Busy | Self::Closing
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendState {
    Dormant,
    Starting,
    Ready,
    Draining,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    InvalidArgument,
    RequestConflict,
    CapabilityUnsupported,
    ResponseTooLarge,
    SessionOwned,
    SessionNotOwned,
    ClientDisconnected,
    TaskNotFound,
    StaleTask,
    SessionBusy,
    SessionClosing,
    SessionClosed,
    SessionLost,
    CapacityExceeded,
    PermissionNotFound,
    PermissionAlreadyResolved,
    PermissionExpired,
    PermissionCancelled,
    PermissionLimitExceeded,
    PermissionPayloadTooLarge,
    ExecutionStateUnknown,
    BackendUnavailable,
    RecoveryRequired,
    InstanceInUse,
    ConfigurationError,
    Internal,
}

#[derive(Debug, Clone, Serialize)]
pub struct TaskError {
    pub code: ErrorCode,
    pub message: String,
    pub retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

impl TaskError {
    pub fn new(code: ErrorCode, message: impl Into<String>, retryable: bool) -> Self {
        // 错误摘要有界（规格 §9.1）
        let msg = message.into();
        let (m, _) = crate::events::truncate_json_text(&msg, limits::ERROR_SUMMARY_MAX);
        Self {
            code,
            message: m.to_string(),
            retryable,
            details: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskPhase {
    AwaitBackend,
    AwaitSession,
    PromptSent,
}

#[derive(Debug)]
pub struct Task {
    pub task_id: TaskId,
    pub request_id: String,
    /// 恢复来源，仅历史恢复轮次有值。
    pub resumed_from_dsh_session_id: Option<String>,
    pub session_id: BridgeSessionId,
    pub parent_task_id: Option<TaskId>,
    pub status: TaskStatus,
    pub created_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub finished_instant: Option<Instant>,
    pub stop_reason: Option<String>,
    pub result: Option<TaskResult>,
    pub error: Option<TaskError>,
    pub execution_state: ExecutionState,
    pub cancel_requested_at: Option<String>,
    pub events: EventBuffer,
    pub result_agg: ResultAggregator,
    pub phase: TaskPhase,
}

pub enum SettleOutcome {
    Completed {
        stop_reason: String,
    },
    Failed {
        error: TaskError,
        execution: ExecutionState,
    },
    Cancelled {
        execution: ExecutionState,
    },
}

impl Task {
    pub fn new(
        task_id: TaskId,
        request_id: String,
        session_id: BridgeSessionId,
        parent_task_id: Option<TaskId>,
    ) -> Self {
        Self {
            task_id,
            request_id,
            resumed_from_dsh_session_id: None,
            session_id,
            parent_task_id,
            status: TaskStatus::Starting,
            created_at: crate::events::rfc3339_now(),
            started_at: None,
            finished_at: None,
            finished_instant: None,
            stop_reason: None,
            result: None,
            error: None,
            execution_state: ExecutionState::NotStarted,
            cancel_requested_at: None,
            events: EventBuffer::new(limits::EVENT_KEEP_COUNT, limits::EVENT_KEEP_BYTES),
            result_agg: ResultAggregator::new(),
            phase: TaskPhase::AwaitBackend,
        }
    }

    /// 终态结算，恰好一次。已终态返回 false。
    pub fn settle(&mut self, outcome: SettleOutcome, wall_now: &str, now: Instant) -> bool {
        if self.status.is_terminal() {
            return false;
        }
        self.finished_at = Some(wall_now.to_string());
        self.finished_instant = Some(now);
        match outcome {
            SettleOutcome::Completed { stop_reason } => {
                self.status = TaskStatus::Completed;
                self.stop_reason = Some(stop_reason);
                self.execution_state = ExecutionState::Stopped;
                self.result = Some(self.result_agg.finish());
            }
            SettleOutcome::Failed { error, execution } => {
                self.status = TaskStatus::Failed;
                self.error = Some(error);
                self.execution_state = execution;
                let r = self.result_agg.finish();
                if r.source != crate::events::ResultSource::Empty {
                    self.result = Some(r);
                }
            }
            SettleOutcome::Cancelled { execution } => {
                self.status = TaskStatus::Cancelled;
                self.execution_state = execution;
                let r = self.result_agg.finish();
                if r.source != crate::events::ResultSource::Empty {
                    self.result = Some(r);
                }
            }
        }
        true
    }
}

#[derive(Debug)]
pub struct Session {
    pub session_id: BridgeSessionId,
    pub dsh_session_id: Option<String>,
    pub epoch: u64,
    pub cwd: PathBuf,
    pub state: SessionState,
    pub active_task_id: Option<TaskId>,
    pub latest_task_id: Option<TaskId>,
    pub last_activity: Instant,
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn mk_task() -> Task {
        Task::new(Uuid::new_v4(), "req-1".into(), Uuid::new_v4(), None)
    }

    #[test]
    fn status_predicates() {
        assert!(TaskStatus::Starting.is_active());
        assert!(TaskStatus::WaitingPermission.is_active());
        assert!(TaskStatus::Cancelling.is_active());
        assert!(!TaskStatus::Completed.is_active());
        assert!(TaskStatus::Completed.is_terminal());
        assert!(TaskStatus::Failed.is_terminal());
        assert!(TaskStatus::Cancelled.is_terminal());
    }

    #[test]
    fn settle_only_once() {
        let mut t = mk_task();
        assert!(t.settle(
            SettleOutcome::Completed {
                stop_reason: "end_turn".into()
            },
            "2026-10-19T00:00:00Z",
            Instant::now(),
        ));
        assert_eq!(t.status, TaskStatus::Completed);
        assert_eq!(t.execution_state, ExecutionState::Stopped);
        // 第二次结算被拒绝：取消旧任务不能影响已完成的任务
        assert!(!t.settle(
            SettleOutcome::Cancelled {
                execution: ExecutionState::Stopped
            },
            "2026-10-19T00:00:01Z",
            Instant::now(),
        ));
        assert_eq!(t.status, TaskStatus::Completed);
    }

    #[test]
    fn cancelled_records_execution_evidence() {
        let mut t = mk_task();
        t.settle(
            SettleOutcome::Cancelled {
                execution: ExecutionState::NotStarted,
            },
            "2026-10-19T00:00:00Z",
            Instant::now(),
        );
        assert_eq!(t.execution_state, ExecutionState::NotStarted);
    }

    #[test]
    fn failed_can_carry_unknown_execution() {
        let mut t = mk_task();
        t.settle(
            SettleOutcome::Failed {
                error: TaskError {
                    code: ErrorCode::ExecutionStateUnknown,
                    message: "no stop evidence".into(),
                    retryable: false,
                    details: None,
                },
                execution: ExecutionState::Unknown,
            },
            "2026-10-19T00:00:00Z",
            Instant::now(),
        );
        assert_eq!(t.status, TaskStatus::Failed);
        assert_eq!(t.execution_state, ExecutionState::Unknown);
        assert_eq!(t.error.unwrap().code, ErrorCode::ExecutionStateUnknown);
    }

    #[test]
    fn session_prompt_gate() {
        assert!(SessionState::Idle.accepts_prompt());
        assert!(!SessionState::Busy.accepts_prompt());
        assert!(!SessionState::Opening.accepts_prompt());
        assert!(!SessionState::Closing.accepts_prompt());
        assert!(!SessionState::Closed.accepts_prompt());
        assert!(!SessionState::Lost.accepts_prompt());
    }

    #[test]
    fn task_events_and_result_available_after_terminal() {
        let mut t = mk_task();
        t.events
            .push(crate::events::EventKind::AgentMessage, json!({"text":"hi"}));
        t.result_agg.push_chunk("hi", Some("m1"));
        t.settle(
            SettleOutcome::Completed {
                stop_reason: "end_turn".into(),
            },
            "2026-10-19T00:00:00Z",
            Instant::now(),
        );
        assert_eq!(t.result.as_ref().unwrap().text, "hi");
        assert_eq!(t.events.last_cursor(), 1);
    }
}

#[cfg(test)]
mod boundary_tests {
    use super::*;

    #[test]
    fn failure_and_cancellation_keep_partial_result_and_monotonic_finish() {
        for cancelled in [true, false] {
            let mut task = Task::new(Uuid::new_v4(), "r".into(), Uuid::new_v4(), None);
            task.result_agg.push_chunk("partial", None);
            let now = Instant::now();
            let outcome = if cancelled {
                SettleOutcome::Cancelled {
                    execution: ExecutionState::Stopped,
                }
            } else {
                SettleOutcome::Failed {
                    error: TaskError::new(ErrorCode::Internal, "failure", false),
                    execution: ExecutionState::Stopped,
                }
            };
            assert!(task.settle(outcome, "2026-10-19T00:00:00Z", now));
            assert_eq!(task.result.unwrap().text, "partial");
            assert_eq!(task.finished_instant, Some(now));
        }
    }

    #[test]
    fn errors_bound_serialized_escaped_summary() {
        let e = TaskError::new(ErrorCode::Internal, "\\\"\n你".repeat(3000), false);
        assert!(serde_json::to_vec(&e.message).unwrap().len() <= limits::ERROR_SUMMARY_MAX);
    }
}

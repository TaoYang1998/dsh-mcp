//! MCP tools translate typed requests into coordinator operations.
use crate::{
    client::{ListTasksQuery, TaskScope},
    config::limits,
    coordinator::CoordHandle,
    events::{sanitize_event_data, truncate_json_text},
    task::{ErrorCode, TaskError},
};
use rmcp::{
    ErrorData, ServerHandler,
    handler::server::wrapper::Parameters,
    model::{CallToolResult, ContentBlock},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StartTaskParams {
    /// Caller-generated idempotency key, 1..128 UTF-8 bytes. Reuse the same key and arguments when retrying.
    pub request_id: String,
    /// Existing absolute working directory on this machine. Directory selection does not create a sandbox.
    pub cwd: String,
    /// Nonempty task instructions, at most 64 KiB in UTF-8.
    pub prompt: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetTaskParams {
    /// UUID returned by start_task or send_followup.
    pub task_id: String,
    /// Last received next_cursor, a nonnegative integer. Omit or use 0 to read from the beginning, including any history gap.
    pub cursor: Option<u64>,
    /// Maximum number of events, 1..100; defaults to 50. Pending permissions are always a complete snapshot.
    #[schemars(range(min = 1, max = 100))]
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FollowUpParams {
    /// Caller-generated idempotency key, 1..128 UTF-8 bytes. Reuse it only for retries of identical arguments.
    pub request_id: String,
    /// UUID of the latest terminal task in an available session.
    pub task_id: String,
    /// Nonempty follow-up instructions, at most 64 KiB in UTF-8.
    pub prompt: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CancelTaskParams {
    /// UUID of the task whose cancellation is requested.
    pub task_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResolvePermissionParams {
    /// UUID of the task that owns this permission request.
    pub task_id: String,
    /// UUID from the task's current pending_permissions snapshot.
    pub permission_id: String,
    /// Exact original option_id advertised by that request; do not invent or alter it.
    pub option_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CloseSessionParams {
    /// UUID of any retained task in the idle session to close.
    pub task_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ListSessionsParams {
    /// 可选的绝对工作目录筛选。
    pub cwd: Option<String>,
    /// 后端上一页返回的不透明 cursor。
    pub cursor: Option<String>,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResumeTaskParams {
    /// 在任务记录保留期间用于同参数重试去重。
    pub request_id: String,
    /// 非活跃的 DSH 历史会话标识，不要求 UUID。
    pub dsh_session_id: String,
    /// 必须与历史会话一致的绝对工作目录。
    pub cwd: String,
    /// 恢复后提交的新轮次，不自动回放旧指令。
    pub prompt: String,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ListTasksParams {
    /// 可选的工作目录筛选。
    pub cwd: Option<String>,
    /// 默认 mine；可显式查找 unowned 或 all。
    pub scope: Option<TaskScope>,
    /// 本连接同一筛选条件的上一页 cursor。
    pub cursor: Option<String>,
    /// 每页 1 至 100，默认 50。
    pub limit: Option<usize>,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AttachSessionParams {
    /// 当前 daemon 保留的桥接会话 UUID。
    pub session_id: String,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StatusParams {}

#[derive(Clone)]
pub struct DshMcpServer {
    coord: CoordHandle,
}

impl DshMcpServer {
    pub fn new(coord: CoordHandle) -> Self {
        Self { coord }
    }
    pub fn coord(&self) -> &CoordHandle {
        &self.coord
    }
}

fn parse_id(text: &str, field: &str) -> Result<Uuid, TaskError> {
    Uuid::parse_str(text).map_err(|_| {
        TaskError::new(
            ErrorCode::InvalidArgument,
            format!("{field} must be a UUID"),
            false,
        )
    })
}

/// The coordinator bounds the full structured view. This final check includes
/// MCP content encoding and leaves room for the JSON-RPC response envelope.
fn ok_result<T: Serialize>(value: &T, text: String) -> Result<CallToolResult, ErrorData> {
    let value = serde_json::to_value(value)
        .map_err(|_| ErrorData::internal_error("cannot serialize tool result", None))?;
    let mut result = CallToolResult::structured(value);
    result.content = vec![ContentBlock::text(truncate_json_text(&text, 1024).0)];
    if serde_json::to_vec(&result)
        .map_or(true, |bytes| bytes.len() > limits::MCP_RESPONSE_MAX - 1024)
    {
        return err_result(TaskError::new(
            ErrorCode::Internal,
            "tool response exceeds its encoded size budget",
            false,
        ));
    }
    Ok(result)
}

fn err_result(error: TaskError) -> Result<CallToolResult, ErrorData> {
    let message = truncate_json_text(&error.message, limits::ERROR_SUMMARY_MAX).0;
    let details = error.details.map(|details| {
        let (mut details, truncated) = sanitize_event_data(details, limits::META_BUDGET / 2);
        if truncated && let Some(fields) = details.as_object_mut() {
            fields.insert("truncated".into(), json!(true));
        }
        details
    });
    Ok(CallToolResult::structured_error(json!({
        "code": error.code,
        "message": message,
        "retryable": error.retryable,
        "details": details,
    })))
}

fn summarize(value: &Value) -> String {
    for field in ["sessions", "tasks"] {
        if let Some(items) = value.get(field).and_then(Value::as_array) {
            return format!("{} {}", items.len(), field);
        }
    }
    if let Some(backend) = value.get("backend")
        && value.get("task_id").is_none()
    {
        return format!(
            "backend={} recovery_required={}",
            backend["state"], backend["recovery_required"]
        );
    }

    let text = |key: &str| value.get(key).and_then(Value::as_str).unwrap_or("unknown");
    if value.get("permission_id").is_some() {
        return format!(
            "permission {} decision={} delivery={} task_status={}",
            text("permission_id"),
            text("decision"),
            text("delivery"),
            text("task_status")
        );
    }
    if value.get("task_id").is_some() {
        if value.get("next_cursor").is_some() {
            return format!(
                "task {} status={} cursor {}/{} has_more={} gap={} pending_permissions={}",
                text("task_id"),
                text("status"),
                value["next_cursor"],
                value["last_cursor"],
                value["has_more"],
                value["gap"],
                value["pending_permissions"].as_array().map_or(0, Vec::len)
            );
        }
        let mut summary = format!("task {} status={}", text("task_id"), text("status"));
        if let Some(session) = value.get("session_id").and_then(Value::as_str) {
            summary.push_str(&format!(" session={session}"));
        }
        if let Some(deduplicated) = value.get("deduplicated").and_then(Value::as_bool) {
            summary.push_str(&format!(" deduplicated={deduplicated}"));
        }
        return summary;
    }
    format!("session {} state={}", text("session_id"), text("state"))
}

fn finish(result: Result<Value, TaskError>) -> Result<CallToolResult, ErrorData> {
    match result {
        Ok(value) => {
            let text = summarize(&value);
            ok_result(&value, text)
        }
        Err(error) => err_result(error),
    }
}

#[rmcp::tool_router]
impl DshMcpServer {
    /// 列举持久化历史；结果不证明跨进程会话当前未被占用。
    #[rmcp::tool(
        description = "List persisted inactive DSH sessions with an opaque cursor. Does not create tasks. A listed session is not proof it is inactive in another ACP or Web process."
    )]
    async fn list_sessions(
        &self,
        Parameters(p): Parameters<ListSessionsParams>,
    ) -> Result<CallToolResult, ErrorData> {
        finish(
            self.coord
                .list_sessions(p.cwd.map(PathBuf::from), p.cursor)
                .await,
        )
    }
    /// 恢复后开始新轮次，旧桥接任务与事件不恢复。
    #[rmcp::tool(
        description = "Explicitly resume an inactive persisted DSH session and submit a new turn. Returns a new bridge task ID; old task IDs and events are not restored. Never falls back to a new context. Inspect get_status before recovering lost sessions."
    )]
    async fn resume_task(
        &self,
        Parameters(p): Parameters<ResumeTaskParams>,
    ) -> Result<CallToolResult, ErrorData> {
        finish(
            self.coord
                .resume_task(
                    p.request_id,
                    p.dsh_session_id,
                    PathBuf::from(p.cwd),
                    p.prompt,
                )
                .await,
        )
    }
    /// 查找本 daemon 尚保留的任务，查询不延长保留期。
    #[rmcp::tool(
        description = "Discover retained tasks. Defaults to mine; use unowned to find disconnected sessions and attach_session before writing. Records and dedup keys expire together or may be evicted early under capacity pressure."
    )]
    async fn list_tasks(
        &self,
        Parameters(p): Parameters<ListTasksParams>,
    ) -> Result<CallToolResult, ErrorData> {
        finish(
            self.coord
                .list_tasks(ListTasksQuery {
                    cwd: p.cwd.map(PathBuf::from),
                    scope: p.scope.unwrap_or_default(),
                    cursor: p.cursor,
                    limit: p.limit.unwrap_or(50),
                })
                .await,
        )
    }
    /// 归属变更不提交提示词，不自动放行待决权限。
    #[rmcp::tool(
        description = "Claim an unowned live session, including running or waiting-permission work. Never steals from a live connection. Does not start a model turn, approve permissions or reset idle TTL."
    )]
    async fn attach_session(
        &self,
        Parameters(p): Parameters<AttachSessionParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let sid = match parse_id(&p.session_id, "session_id") {
            Ok(s) => s,
            Err(e) => return err_result(e),
        };
        finish(self.coord.attach_session(sid).await)
    }
    /// 健康状态与 MCP 握手成功分开报告。
    #[rmcp::tool(
        description = "Read backend health, recovery protection and resource counts without starting ACP or refreshing TTL. Successful MCP ping does not imply task admission is healthy. Recovery requires confirmed cleanup; no automatic replay."
    )]
    async fn get_status(
        &self,
        Parameters(_p): Parameters<StatusParams>,
    ) -> Result<CallToolResult, ErrorData> {
        finish(self.coord.get_status().await)
    }

    #[rmcp::tool(
        description = "Run a coding task in a local absolute working directory. Returns immediately with task_id; poll get_task. Reuse the same request_id and arguments for retries. dsh may modify local files; this is not a sandbox."
    )]
    async fn start_task(
        &self,
        Parameters(params): Parameters<StartTaskParams>,
    ) -> Result<CallToolResult, ErrorData> {
        finish(
            self.coord
                .start_task(params.request_id, PathBuf::from(params.cwd), params.prompt)
                .await,
        )
    }

    #[rmcp::tool(
        description = "Read task status, bounded event pages, partial/final result and the complete pending permission snapshot. completed means one execution turn finished; acceptance still requires checking actual files and tests. Poll with next_cursor; gap reports evicted history."
    )]
    async fn get_task(
        &self,
        Parameters(params): Parameters<GetTaskParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let id = match parse_id(&params.task_id, "task_id") {
            Ok(id) => id,
            Err(error) => return err_result(error),
        };
        finish(
            self.coord
                .get_task(
                    id,
                    params.cursor.unwrap_or(0),
                    params.limit.unwrap_or(limits::GET_TASK_DEFAULT_LIMIT),
                )
                .await,
        )
    }

    #[rmcp::tool(
        description = "Continue the existing session from its latest terminal task, creating a new task. Use a fresh request_id for new instructions and reuse it for identical retries. A busy session returns session_busy; closed or lost sessions cannot continue."
    )]
    async fn send_followup(
        &self,
        Parameters(params): Parameters<FollowUpParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let id = match parse_id(&params.task_id, "task_id") {
            Ok(id) => id,
            Err(error) => return err_result(error),
        };
        finish(
            self.coord
                .follow_up(params.request_id, id, params.prompt)
                .await,
        )
    }

    #[rmcp::tool(
        description = "Request task cancellation and return promptly. cancelling records intent; poll get_task for confirmed stop evidence. Repeated cancellation is idempotent, and cancelling an old terminal task does not stop a newer task."
    )]
    async fn cancel_task(
        &self,
        Parameters(params): Parameters<CancelTaskParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let id = match parse_id(&params.task_id, "task_id") {
            Ok(id) => id,
            Err(error) => return err_result(error),
        };
        finish(self.coord.cancel_task(id).await)
    }

    #[rmcp::tool(
        description = "Resolve one pending permission using its exact published option_id. Unanswered requests expire and are automatically rejected or cancelled. A decision does not prove execution; delivery is reported separately. Repeating the same decision is idempotent."
    )]
    async fn resolve_permission(
        &self,
        Parameters(params): Parameters<ResolvePermissionParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let task = match parse_id(&params.task_id, "task_id") {
            Ok(id) => id,
            Err(error) => return err_result(error),
        };
        let permission = match parse_id(&params.permission_id, "permission_id") {
            Ok(id) => id,
            Err(error) => return err_result(error),
        };
        finish(
            self.coord
                .resolve_permission(task, permission, params.option_id)
                .await,
        )
    }

    #[rmcp::tool(
        description = "Release an idle session's retained context using any retained task ID. Busy sessions must be cancelled and settled first. A closing response is asynchronous; inspect get_task.session for the outcome. A closed session cannot receive follow-ups."
    )]
    async fn close_session(
        &self,
        Parameters(params): Parameters<CloseSessionParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let id = match parse_id(&params.task_id, "task_id") {
            Ok(id) => id,
            Err(error) => return err_result(error),
        };
        finish(self.coord.close_session(id).await)
    }
}

#[rmcp::tool_handler(
    name = "dsh-mcp",
    instructions = "Delegate coding tasks to the local DeepSeek Harness. start_task returns immediately; poll get_task, resolve pending permissions, send_followup on the latest finished task for rework, cancel_task to stop, and close_session to release context. completed means a turn finished; verify actual files and tests before accepting the work. Tasks operate on local files in cwd; directory selection is not a sandbox."
)]
impl ServerHandler for DshMcpServer {
    async fn call_tool(
        &self,
        request: rmcp::model::CallToolRequestParams,
        context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> Result<rmcp::model::CallToolResponse, ErrorData> {
        // rmcp's router turns extraction errors into tool results. Validate first
        // so parameter structure errors retain this server's JSON-RPC contract.
        let arguments = Value::Object(request.arguments.clone().unwrap_or_default());
        let valid = match request.name.as_ref() {
            "list_sessions" => serde_json::from_value::<ListSessionsParams>(arguments).is_ok(),
            "resume_task" => serde_json::from_value::<ResumeTaskParams>(arguments).is_ok(),
            "list_tasks" => serde_json::from_value::<ListTasksParams>(arguments).is_ok(),
            "attach_session" => serde_json::from_value::<AttachSessionParams>(arguments).is_ok(),
            "get_status" => serde_json::from_value::<StatusParams>(arguments).is_ok(),
            "start_task" => serde_json::from_value::<StartTaskParams>(arguments).is_ok(),
            "get_task" => serde_json::from_value::<GetTaskParams>(arguments).is_ok(),
            "send_followup" => serde_json::from_value::<FollowUpParams>(arguments).is_ok(),
            "cancel_task" => serde_json::from_value::<CancelTaskParams>(arguments).is_ok(),
            "resolve_permission" => {
                serde_json::from_value::<ResolvePermissionParams>(arguments).is_ok()
            }
            "close_session" => serde_json::from_value::<CloseSessionParams>(arguments).is_ok(),
            _ => true,
        };
        if !valid {
            return Err(ErrorData::invalid_params(
                "invalid tool argument structure",
                None,
            ));
        }
        Self::tool_router()
            .call(rmcp::handler::server::tool::ToolCallContext::new(
                self, request, context,
            ))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn response_keeps_full_structured_view_without_copying_into_text() {
        let value = json!({"task_id":"task-1", "result":{"text":"\\\"\n你".repeat(30000)}});
        let result = ok_result(&value, "task task-1 status=running".into()).unwrap();
        assert_eq!(result.structured_content, Some(value));
        assert!(result.content[0].as_text().unwrap().text.len() < 128);
        assert!(serde_json::to_vec(&result).unwrap().len() <= limits::MCP_RESPONSE_MAX);
    }

    #[test]
    fn oversized_responses_become_bounded_structured_business_errors() {
        let value = json!({"text":"x".repeat(limits::MCP_RESPONSE_MAX)});
        let result = ok_result(&value, "oversized".into()).unwrap();
        assert_eq!(result.is_error, Some(true));
        assert_eq!(result.structured_content.unwrap()["code"], "internal");
    }

    #[test]
    fn error_details_and_escaped_messages_cannot_exceed_response_limit() {
        let mut error = TaskError::new(ErrorCode::Internal, "\\\"\n你".repeat(30000), false);
        error.details = Some(json!({"text":"x".repeat(limits::MCP_RESPONSE_MAX)}));
        let result = err_result(error).unwrap();
        assert!(serde_json::to_vec(&result).unwrap().len() <= limits::MCP_RESPONSE_MAX);
        assert_eq!(result.is_error, Some(true));
        assert!(
            result.structured_content.unwrap()["details"]
                .get("truncated")
                .is_some()
        );
    }
}

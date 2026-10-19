//! 历史会话保持后端的不透明标识和分页语义。
use crate::{
    acp::{BackendError, BackendErrorKind},
    config::limits,
    events::truncate_json_text,
};
use serde::Serialize;
use std::path::PathBuf;
#[derive(Clone, Copy, Default)]
pub struct BackendCapabilities {
    /// 后端是否公布历史列举能力。
    pub list: bool,
    /// 后端是否公布恢复能力。
    pub resume: bool,
}
#[derive(Clone, Debug, Serialize)]
pub struct SessionSummary {
    /// DSH 持久化会话标识，不要求 UUID。
    pub dsh_session_id: String,
    /// 后端报告的原始工作目录。
    pub cwd: PathBuf,
    /// 可裁剪的展示标题。
    pub title: Option<String>,
    /// 可选的最近更新时间。
    pub updated_at: Option<String>,
}
#[derive(Clone, Debug, Serialize)]
pub struct SessionPage {
    /// 完整后端页，不静默截掉记录。
    pub sessions: Vec<SessionSummary>,
    /// 后端不透明的下一页游标。
    pub next_cursor: Option<String>,
}
impl SessionPage {
    /// 仅裁剪展示文本，整页超界时报错，避免跳过未展示的记录。
    pub fn bounded(mut self) -> Result<Self, BackendError> {
        let large = || {
            BackendError::new(
                BackendErrorKind::ResponseTooLarge,
                "history page exceeds its size budget",
            )
        };
        if self.next_cursor.as_ref().is_some_and(|s| s.len() > 4096) {
            return Err(large());
        }
        for s in &mut self.sessions {
            if s.dsh_session_id.is_empty()
                || s.dsh_session_id.len() > limits::REMOTE_ID_MAX
                || !s.cwd.is_absolute()
                || serde_json::to_vec(&s.cwd).map_or(true, |v| v.len() > 4096)
            {
                return Err(large());
            }
            s.title = s
                .title
                .as_ref()
                .map(|s| truncate_json_text(s, 1024).0.to_owned());
            if s.updated_at.as_ref().is_some_and(|s| s.len() > 128) {
                return Err(large());
            }
        }
        if serde_json::to_vec(&self).map_or(true, |v| v.len() > limits::TASKVIEW_BUDGET) {
            return Err(large());
        }
        Ok(self)
    }
}

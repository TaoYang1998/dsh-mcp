//! 连接归属是防误操作约束，不作为同账户进程间的安全隔离。
use std::path::PathBuf;
use uuid::Uuid;
pub type ClientId = Uuid;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientScope {
    Standalone,
    Shared(ClientId),
}
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum TaskScope {
    #[default]
    Mine,
    Unowned,
    All,
}
#[derive(Clone)]
pub struct ListTasksQuery {
    /// 规范化的工作目录筛选。
    pub cwd: Option<PathBuf>,
    /// 相对当前连接的归属筛选。
    pub scope: TaskScope,
    /// 绑定连接身份和筛选条件的分页令牌。
    pub cursor: Option<String>,
    /// 每页条数，范围 1 至 100。
    pub limit: usize,
}
impl Default for ListTasksQuery {
    /// 默认仅查询本连接记录，避免误操作其他客户端任务。
    fn default() -> Self {
        Self {
            cwd: None,
            scope: TaskScope::Mine,
            cursor: None,
            limit: 50,
        }
    }
}

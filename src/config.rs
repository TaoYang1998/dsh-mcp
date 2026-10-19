use std::env;
use std::path::PathBuf;
use std::time::Duration;

pub mod limits {
    use std::time::Duration;
    pub const EVENT_KEEP_COUNT: usize = 200;
    pub const EVENT_KEEP_BYTES: usize = 1 << 20;
    pub const EVENT_ITEM_MAX: usize = 8 * 1024;
    pub const PROMPT_MAX: usize = 64 * 1024;
    pub const REQUEST_ID_MAX: usize = 128;
    pub const PENDING_PERMS_MAX: usize = 16;
    pub const DECIDED_PERMS_KEEP: usize = 64;
    pub const TOOL_SUMMARY_CACHE: usize = 128;
    pub const TOOL_SUMMARY_MAX: usize = 4 * 1024;
    pub const PERM_ITEM_MAX: usize = 4 * 1024;
    pub const PERM_OPTIONS_MAX: usize = 8;
    pub const PERM_OPTION_ID_MAX: usize = 256;
    pub const TASKVIEW_BUDGET: usize = 256 * 1024;
    pub const META_BUDGET: usize = 16 * 1024;
    pub const PERM_SNAPSHOT_BUDGET: usize = 64 * 1024;
    pub const RESULT_BUDGET: usize = 64 * 1024;
    pub const ERROR_SUMMARY_MAX: usize = 4 * 1024;
    pub const GET_TASK_DEFAULT_LIMIT: usize = 50;
    pub const GET_TASK_MAX_LIMIT: usize = 100;
    pub const MCP_RESPONSE_MAX: usize = 320 * 1024;
    pub const ACP_FRAME_MAX: usize = 4 * 1024 * 1024;
    pub const ACP_QUEUE_MAX_ITEMS: usize = 128;
    pub const ACP_QUEUE_MAX_BYTES: usize = 8 * 1024 * 1024;
    pub const ACP_QUEUE_STALL: Duration = Duration::from_secs(10);
    pub const TERM_GRACE: Duration = Duration::from_secs(5);
    pub const KILL_GRACE: Duration = Duration::from_secs(2);
}

#[derive(Debug, Clone)]
pub struct Config {
    pub dsh_command: String,
    pub dsh_args: Vec<String>,
    pub state_dir: PathBuf,
    pub max_active_tasks: usize,
    pub max_open_sessions: usize,
    pub session_idle_ttl: Duration,
    pub max_task_records: usize,
    pub task_retention: Duration,
    pub start_timeout: Duration,
    pub permission_timeout: Duration,
    pub cancel_timeout: Duration,
    pub close_timeout: Duration,
    pub shutdown_timeout: Duration,
}

#[derive(Debug)]
pub struct ConfigError(pub String);

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "configuration_error: {}", self.0)
    }
}
impl std::error::Error for ConfigError {}

fn env_opt(name: &str) -> Result<Option<String>, ConfigError> {
    match env::var(name) {
        Ok(v) => Ok(Some(v)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => {
            Err(ConfigError(format!("{name} is not valid unicode")))
        }
    }
}

fn env_usize(name: &str, default: usize) -> Result<usize, ConfigError> {
    match env_opt(name)? {
        None => Ok(default),
        Some(v) => {
            let n: usize = v
                .parse()
                .map_err(|_| ConfigError(format!("{name} must be a positive integer")))?;
            if n == 0 {
                return Err(ConfigError(format!("{name} must be positive")));
            }
            Ok(n)
        }
    }
}

fn env_secs(name: &str, default: u64) -> Result<Duration, ConfigError> {
    Ok(Duration::from_secs(
        env_usize(name, default as usize)? as u64
    ))
}

impl Config {
    pub fn default_state_dir() -> PathBuf {
        if let Ok(xdg) = env::var("XDG_STATE_HOME")
            && !xdg.is_empty()
        {
            return PathBuf::from(xdg).join("dsh-mcp");
        }
        let home = env::var("HOME").unwrap_or_else(|_| ".".to_string());
        PathBuf::from(home).join(".local/state/dsh-mcp")
    }

    pub fn from_env() -> Result<Config, ConfigError> {
        let dsh_command = env_opt("DSH_MCP_DSH_COMMAND")?.unwrap_or_else(|| "dsh".to_string());
        if dsh_command.is_empty() {
            return Err(ConfigError("DSH_MCP_DSH_COMMAND must not be empty".into()));
        }
        let dsh_args = match env_opt("DSH_MCP_DSH_ARGS_JSON")? {
            None => vec!["--profile".to_string(), "acp".to_string()],
            Some(v) => serde_json::from_str::<Vec<String>>(&v).map_err(|_| {
                ConfigError("DSH_MCP_DSH_ARGS_JSON must be a JSON string array".into())
            })?,
        };
        let state_dir = match env_opt("DSH_MCP_STATE_DIR")? {
            Some(v) => PathBuf::from(v),
            None => Self::default_state_dir(),
        };
        let c = Config {
            dsh_command,
            dsh_args,
            state_dir,
            max_active_tasks: env_usize("DSH_MCP_MAX_ACTIVE_TASKS", 4)?,
            max_open_sessions: env_usize("DSH_MCP_MAX_OPEN_SESSIONS", 16)?,
            session_idle_ttl: env_secs("DSH_MCP_SESSION_IDLE_TTL_SECS", 1800)?,
            max_task_records: env_usize("DSH_MCP_MAX_TASK_RECORDS", 256)?,
            task_retention: env_secs("DSH_MCP_TASK_RETENTION_SECS", 86400)?,
            start_timeout: env_secs("DSH_MCP_START_TIMEOUT_SECS", 30)?,
            permission_timeout: env_secs("DSH_MCP_PERMISSION_TIMEOUT_SECS", 600)?,
            cancel_timeout: env_secs("DSH_MCP_CANCEL_TIMEOUT_SECS", 30)?,
            close_timeout: env_secs("DSH_MCP_CLOSE_TIMEOUT_SECS", 10)?,
            shutdown_timeout: env_secs("DSH_MCP_SHUTDOWN_TIMEOUT_SECS", 50)?,
        };
        c.validate()?;
        Ok(c)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.max_active_tasks > self.max_open_sessions {
            return Err(ConfigError(
                "DSH_MCP_MAX_ACTIVE_TASKS must not exceed DSH_MCP_MAX_OPEN_SESSIONS".into(),
            ));
        }
        if self.max_task_records < self.max_open_sessions + self.max_active_tasks {
            return Err(ConfigError(
                "DSH_MCP_MAX_TASK_RECORDS must be at least MAX_OPEN_SESSIONS + MAX_ACTIVE_TASKS"
                    .into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::sync::Mutex;

    // env 测试必须串行
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn clear() {
        for k in [
            "DSH_MCP_DSH_COMMAND",
            "DSH_MCP_DSH_ARGS_JSON",
            "DSH_MCP_STATE_DIR",
            "DSH_MCP_MAX_ACTIVE_TASKS",
            "DSH_MCP_MAX_OPEN_SESSIONS",
            "DSH_MCP_SESSION_IDLE_TTL_SECS",
            "DSH_MCP_MAX_TASK_RECORDS",
            "DSH_MCP_TASK_RETENTION_SECS",
            "DSH_MCP_START_TIMEOUT_SECS",
            "DSH_MCP_PERMISSION_TIMEOUT_SECS",
            "DSH_MCP_CANCEL_TIMEOUT_SECS",
            "DSH_MCP_CLOSE_TIMEOUT_SECS",
            "DSH_MCP_SHUTDOWN_TIMEOUT_SECS",
        ] {
            unsafe { env::remove_var(k) };
        }
    }

    #[test]
    fn defaults_are_applied() {
        let _g = ENV_LOCK.lock().unwrap();
        clear();
        let c = Config::from_env().unwrap();
        assert_eq!(c.dsh_command, "dsh");
        assert_eq!(c.dsh_args, vec!["--profile", "acp"]);
        assert_eq!(c.max_active_tasks, 4);
        assert_eq!(c.max_open_sessions, 16);
        assert_eq!(c.session_idle_ttl, Duration::from_secs(1800));
        assert_eq!(c.max_task_records, 256);
        assert_eq!(c.task_retention, Duration::from_secs(86400));
        assert_eq!(c.start_timeout, Duration::from_secs(30));
        assert_eq!(c.permission_timeout, Duration::from_secs(600));
        assert_eq!(c.cancel_timeout, Duration::from_secs(30));
        assert_eq!(c.close_timeout, Duration::from_secs(10));
        assert_eq!(c.shutdown_timeout, Duration::from_secs(50));
    }

    #[test]
    fn args_json_must_be_string_array() {
        let _g = ENV_LOCK.lock().unwrap();
        clear();
        unsafe { env::set_var("DSH_MCP_DSH_ARGS_JSON", "--profile acp") };
        assert!(Config::from_env().is_err());
        unsafe { env::set_var("DSH_MCP_DSH_ARGS_JSON", "[\"a b\", \"c\"]") };
        let c = Config::from_env().unwrap();
        assert_eq!(c.dsh_args, vec!["a b", "c"]);
    }

    #[test]
    fn rejects_zero_and_bad_ratios() {
        let _g = ENV_LOCK.lock().unwrap();
        clear();
        unsafe { env::set_var("DSH_MCP_MAX_ACTIVE_TASKS", "0") };
        assert!(Config::from_env().is_err());
        clear();
        unsafe {
            env::set_var("DSH_MCP_MAX_ACTIVE_TASKS", "8");
            env::set_var("DSH_MCP_MAX_OPEN_SESSIONS", "4");
        }
        assert!(Config::from_env().is_err()); // active > open
        clear();
        unsafe {
            env::set_var("DSH_MCP_MAX_OPEN_SESSIONS", "4");
            env::set_var("DSH_MCP_MAX_TASK_RECORDS", "5"); // < 4+4? 4 active + 4 sessions = 8
        }
        assert!(Config::from_env().is_err());
    }

    #[test]
    fn rejects_non_numeric_duration() {
        let _g = ENV_LOCK.lock().unwrap();
        clear();
        unsafe { env::set_var("DSH_MCP_CANCEL_TIMEOUT_SECS", "soon") };
        assert!(Config::from_env().is_err());
    }
}

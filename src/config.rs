use std::env;
use std::path::PathBuf;
use std::time::Duration;

pub mod limits {
    /// 沿用 ACP 已有的远端标识上限，恢复入口必须在接单前一致校验。
    pub const REMOTE_ID_MAX: usize = 1024;
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
    /// 共享传输路径，独立模式无需配置。
    pub socket_path: Option<PathBuf>,
    /// 全部连接上限，包含未完成握手的连接。
    pub max_clients: usize,
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

fn env_opt(
    lookup: &mut impl FnMut(&str) -> Option<std::ffi::OsString>,
    name: &str,
) -> Result<Option<String>, ConfigError> {
    lookup(name)
        .map(|value| {
            value
                .into_string()
                .map_err(|_| ConfigError(format!("{name} is not valid unicode")))
        })
        .transpose()
}

fn positive<T: std::str::FromStr + PartialEq + Default>(
    value: &str,
    name: &str,
) -> Result<T, ConfigError> {
    let number = value
        .parse::<T>()
        .map_err(|_| ConfigError(format!("{name} must be a positive integer")))?;
    if number == T::default() {
        return Err(ConfigError(format!("{name} must be positive")));
    }
    Ok(number)
}

fn env_usize(
    lookup: &mut impl FnMut(&str) -> Option<std::ffi::OsString>,
    name: &str,
    default: usize,
) -> Result<usize, ConfigError> {
    match env_opt(lookup, name)? {
        Some(value) => positive(&value, name),
        None => Ok(default),
    }
}

fn env_secs(
    lookup: &mut impl FnMut(&str) -> Option<std::ffi::OsString>,
    name: &str,
    default: u64,
) -> Result<Duration, ConfigError> {
    let duration = Duration::from_secs(match env_opt(lookup, name)? {
        Some(value) => positive(&value, name)?,
        None => default,
    });
    if std::time::Instant::now().checked_add(duration).is_none() {
        return Err(ConfigError(format!(
            "{name} exceeds the supported monotonic deadline range"
        )));
    }
    Ok(duration)
}

fn state_dir(
    lookup: &mut impl FnMut(&str) -> Option<std::ffi::OsString>,
) -> Result<PathBuf, ConfigError> {
    if let Some(value) = env_opt(lookup, "DSH_MCP_STATE_DIR")? {
        if value.is_empty() || value.contains('\0') {
            return Err(ConfigError(
                "DSH_MCP_STATE_DIR must be a nonempty path without NUL".into(),
            ));
        }
        return Ok(PathBuf::from(value));
    }
    if let Some(value) = env_opt(lookup, "XDG_STATE_HOME")?
        && !value.is_empty()
    {
        let path = PathBuf::from(&value);
        if !path.is_absolute() || value.contains('\0') {
            return Err(ConfigError(
                "XDG_STATE_HOME must be an absolute path without NUL".into(),
            ));
        }
        return Ok(path.join("dsh-mcp"));
    }
    let home = env_opt(lookup, "HOME")?.ok_or_else(|| {
        ConfigError("HOME is required when no state directory is configured".into())
    })?;
    let home_path = PathBuf::from(&home);
    if home.is_empty() || !home_path.is_absolute() || home.contains('\0') {
        return Err(ConfigError(
            "HOME must be an absolute path without NUL".into(),
        ));
    }
    Ok(home_path.join(".local/state/dsh-mcp"))
}

impl Default for Config {
    fn default() -> Self {
        Self {
            socket_path: None,
            max_clients: 32,
            dsh_command: "dsh".into(),
            dsh_args: vec!["--profile".into(), "acp".into()],
            state_dir: Self::default_state_dir(),
            max_active_tasks: 4,
            max_open_sessions: 16,
            session_idle_ttl: Duration::from_secs(1800),
            max_task_records: 256,
            task_retention: Duration::from_secs(86400),
            start_timeout: Duration::from_secs(30),
            permission_timeout: Duration::from_secs(600),
            cancel_timeout: Duration::from_secs(30),
            close_timeout: Duration::from_secs(10),
            shutdown_timeout: Duration::from_secs(50),
        }
    }
}

impl Config {
    /// Best-effort defaults for programmatic callers. Startup uses strict `from_env` validation.
    pub fn default_state_dir() -> PathBuf {
        let mut lookup = |name: &str| {
            if name == "DSH_MCP_STATE_DIR" {
                None
            } else {
                env::var_os(name)
            }
        };
        state_dir(&mut lookup).unwrap_or_else(|_| {
            env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("/"))
                .join(".local/state/dsh-mcp")
        })
    }

    pub fn from_env() -> Result<Config, ConfigError> {
        Self::from_lookup(|name| env::var_os(name))
    }

    fn from_lookup(
        mut lookup: impl FnMut(&str) -> Option<std::ffi::OsString>,
    ) -> Result<Config, ConfigError> {
        let dsh_command =
            env_opt(&mut lookup, "DSH_MCP_DSH_COMMAND")?.unwrap_or_else(|| "dsh".to_string());
        let dsh_args = match env_opt(&mut lookup, "DSH_MCP_DSH_ARGS_JSON")? {
            None => vec!["--profile".to_string(), "acp".to_string()],
            Some(value) => serde_json::from_str::<Vec<String>>(&value).map_err(|_| {
                ConfigError("DSH_MCP_DSH_ARGS_JSON must be a JSON string array".into())
            })?,
        };
        let c = Config {
            dsh_command,
            dsh_args,
            state_dir: state_dir(&mut lookup)?,
            socket_path: lookup("DSH_MCP_SOCKET").map(PathBuf::from),
            max_clients: env_usize(&mut lookup, "DSH_MCP_MAX_CLIENTS", 32)?,
            max_active_tasks: env_usize(&mut lookup, "DSH_MCP_MAX_ACTIVE_TASKS", 4)?,
            max_open_sessions: env_usize(&mut lookup, "DSH_MCP_MAX_OPEN_SESSIONS", 16)?,
            session_idle_ttl: env_secs(&mut lookup, "DSH_MCP_SESSION_IDLE_TTL_SECS", 1800)?,
            max_task_records: env_usize(&mut lookup, "DSH_MCP_MAX_TASK_RECORDS", 256)?,
            task_retention: env_secs(&mut lookup, "DSH_MCP_TASK_RETENTION_SECS", 86400)?,
            start_timeout: env_secs(&mut lookup, "DSH_MCP_START_TIMEOUT_SECS", 30)?,
            permission_timeout: env_secs(&mut lookup, "DSH_MCP_PERMISSION_TIMEOUT_SECS", 600)?,
            cancel_timeout: env_secs(&mut lookup, "DSH_MCP_CANCEL_TIMEOUT_SECS", 30)?,
            close_timeout: env_secs(&mut lookup, "DSH_MCP_CLOSE_TIMEOUT_SECS", 10)?,
            shutdown_timeout: env_secs(&mut lookup, "DSH_MCP_SHUTDOWN_TIMEOUT_SECS", 50)?,
        };
        c.validate()?;
        Ok(c)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.max_clients > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(ConfigError(
                "DSH_MCP_MAX_CLIENTS exceeds the supported connection capacity".into(),
            ));
        }
        if self.dsh_command.is_empty() || self.dsh_command.contains('\0') {
            return Err(ConfigError(
                "DSH_MCP_DSH_COMMAND must be nonempty and contain no NUL".into(),
            ));
        }
        if self.dsh_args.iter().any(|arg| arg.contains('\0')) {
            return Err(ConfigError(
                "DSH_MCP_DSH_ARGS_JSON arguments must contain no NUL".into(),
            ));
        }
        if self.max_active_tasks > self.max_open_sessions {
            return Err(ConfigError(
                "DSH_MCP_MAX_ACTIVE_TASKS must not exceed DSH_MCP_MAX_OPEN_SESSIONS".into(),
            ));
        }
        let minimum_records = self
            .max_open_sessions
            .checked_add(self.max_active_tasks)
            .ok_or_else(|| {
                ConfigError("MAX_OPEN_SESSIONS + MAX_ACTIVE_TASKS overflows capacity".into())
            })?;
        if self.max_task_records < minimum_records {
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
    use std::collections::HashMap;
    use std::ffi::OsString;

    /// 连接数配置必须在信号量可表示范围内，不能在启动 daemon 时崩溃。
    #[test]
    fn shared_connection_capacity_rejects_unrepresentable_values() {
        assert!(parse(&[("DSH_MCP_MAX_CLIENTS", "0")]).is_err());
        assert!(parse(&[("DSH_MCP_MAX_CLIENTS", &usize::MAX.to_string())]).is_err());
        assert_eq!(parse(&[]).unwrap().max_clients, 32);
    }

    fn parse(values: &[(&str, &str)]) -> Result<Config, ConfigError> {
        let mut vars: HashMap<String, OsString> = values
            .iter()
            .map(|(k, v)| (k.to_string(), OsString::from(v)))
            .collect();
        vars.entry("HOME".into())
            .or_insert_with(|| "/home/tester".into());
        Config::from_lookup(|name| vars.get(name).cloned())
    }

    #[test]
    fn defaults_are_applied_without_mutating_process_environment() {
        let c = parse(&[]).unwrap();
        assert_eq!(c.dsh_command, "dsh");
        assert_eq!(c.dsh_args, vec!["--profile", "acp"]);
        assert_eq!(
            c.state_dir,
            PathBuf::from("/home/tester/.local/state/dsh-mcp")
        );
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
        let default = Config::default();
        assert_eq!(default.start_timeout, Duration::from_secs(30));
        assert_eq!(default.shutdown_timeout, Duration::from_secs(50));
    }

    #[test]
    fn args_json_preserves_shell_characters_as_arguments() {
        assert!(parse(&[("DSH_MCP_DSH_ARGS_JSON", "--profile acp")]).is_err());
        assert!(parse(&[("DSH_MCP_DSH_ARGS_JSON", "[42]")]).is_err());
        let c = parse(&[(
            "DSH_MCP_DSH_ARGS_JSON",
            r#"["a b", "'quote'", "$(touch should-not-exist)"]"#,
        )])
        .unwrap();
        assert_eq!(c.dsh_args, ["a b", "'quote'", "$(touch should-not-exist)"]);
    }

    #[test]
    fn rejects_zero_capacity_and_duration_and_invalid_ratios() {
        for name in [
            "DSH_MCP_MAX_ACTIVE_TASKS",
            "DSH_MCP_MAX_OPEN_SESSIONS",
            "DSH_MCP_MAX_TASK_RECORDS",
            "DSH_MCP_SESSION_IDLE_TTL_SECS",
            "DSH_MCP_TASK_RETENTION_SECS",
            "DSH_MCP_START_TIMEOUT_SECS",
            "DSH_MCP_PERMISSION_TIMEOUT_SECS",
            "DSH_MCP_CANCEL_TIMEOUT_SECS",
            "DSH_MCP_CLOSE_TIMEOUT_SECS",
            "DSH_MCP_SHUTDOWN_TIMEOUT_SECS",
        ] {
            assert!(parse(&[(name, "0")]).is_err(), "{name}");
        }
        assert!(
            parse(&[
                ("DSH_MCP_MAX_ACTIVE_TASKS", "8"),
                ("DSH_MCP_MAX_OPEN_SESSIONS", "4")
            ])
            .is_err()
        );
        assert!(
            parse(&[
                ("DSH_MCP_MAX_OPEN_SESSIONS", "4"),
                ("DSH_MCP_MAX_TASK_RECORDS", "5")
            ])
            .is_err()
        );
        assert!(parse(&[("DSH_MCP_CANCEL_TIMEOUT_SECS", "soon")]).is_err());
    }

    #[test]
    fn capacity_sum_overflow_is_configuration_error() {
        let max = usize::MAX.to_string();
        assert!(
            parse(&[
                ("DSH_MCP_MAX_ACTIVE_TASKS", &max),
                ("DSH_MCP_MAX_OPEN_SESSIONS", &max),
                ("DSH_MCP_MAX_TASK_RECORDS", &max)
            ])
            .is_err()
        );
    }

    #[test]
    fn unrepresentable_monotonic_deadlines_are_configuration_errors() {
        let extreme = u64::MAX.to_string();
        for name in [
            "DSH_MCP_SESSION_IDLE_TTL_SECS",
            "DSH_MCP_TASK_RETENTION_SECS",
            "DSH_MCP_START_TIMEOUT_SECS",
            "DSH_MCP_PERMISSION_TIMEOUT_SECS",
            "DSH_MCP_CANCEL_TIMEOUT_SECS",
            "DSH_MCP_CLOSE_TIMEOUT_SECS",
            "DSH_MCP_SHUTDOWN_TIMEOUT_SECS",
        ] {
            assert!(parse(&[(name, &extreme)]).is_err(), "{name}");
        }
    }

    #[test]
    fn rejects_empty_state_dir_command_and_embedded_nul() {
        for (name, value) in [
            ("DSH_MCP_STATE_DIR", ""),
            ("DSH_MCP_STATE_DIR", "a\0b"),
            ("DSH_MCP_DSH_COMMAND", ""),
            ("DSH_MCP_DSH_COMMAND", "dsh\0extra"),
            ("DSH_MCP_DSH_ARGS_JSON", r#"["nul\u0000arg"]"#),
        ] {
            assert!(parse(&[(name, value)]).is_err(), "{name}");
        }
    }

    #[test]
    fn state_dir_override_and_absolute_xdg_are_respected() {
        assert_eq!(
            parse(&[("XDG_STATE_HOME", "/state")]).unwrap().state_dir,
            PathBuf::from("/state/dsh-mcp")
        );
        assert_eq!(
            parse(&[
                ("DSH_MCP_STATE_DIR", "relative-state"),
                ("XDG_STATE_HOME", "relative-xdg")
            ])
            .unwrap()
            .state_dir,
            PathBuf::from("relative-state")
        );
        assert!(parse(&[("XDG_STATE_HOME", "relative-xdg")]).is_err());
        assert_eq!(
            parse(&[("XDG_STATE_HOME", "")]).unwrap().state_dir,
            PathBuf::from("/home/tester/.local/state/dsh-mcp")
        );
    }

    #[test]
    fn invalid_unicode_is_rejected_without_echoing_environment_value() {
        use std::os::unix::ffi::OsStringExt;
        let bad = OsString::from_vec(vec![0xff, 0xfe]);
        for invalid_name in [
            "DSH_MCP_DSH_COMMAND",
            "DSH_MCP_STATE_DIR",
            "DSH_MCP_CANCEL_TIMEOUT_SECS",
            "XDG_STATE_HOME",
            "HOME",
        ] {
            let result = Config::from_lookup(|name| {
                if name == invalid_name {
                    Some(bad.clone())
                } else if name == "HOME" {
                    Some("/home/tester".into())
                } else {
                    None
                }
            });
            assert!(result.is_err(), "{invalid_name}");
            assert!(result.unwrap_err().to_string().contains(invalid_name));
        }
    }
}

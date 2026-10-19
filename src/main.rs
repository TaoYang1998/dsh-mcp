use dsh_mcp::acp::DshFactory;
use dsh_mcp::config::Config;
use dsh_mcp::coordinator::spawn_coordinator;
use dsh_mcp::runtime_guard::{AcquireOutcome, RuntimeGuard};
use dsh_mcp::server::DshMcpServer;
use std::ffi::OsString;
use std::process::ExitCode;
use std::sync::Arc;
use tokio::signal::unix::{Signal, SignalKind, signal};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Serve,
    Daemon,
    Proxy,
    AcknowledgeCleanup,
    Help,
}

fn parse_mode(args: impl IntoIterator<Item = OsString>) -> Result<Mode, ()> {
    let mut args = args.into_iter();
    let mode = match args.next() {
        None => Mode::Serve,
        Some(arg) if arg == "--daemon" => Mode::Daemon,
        Some(arg) if arg == "--proxy" => Mode::Proxy,
        Some(arg) if arg == "--acknowledge-cleanup" => Mode::AcknowledgeCleanup,
        Some(arg) if arg == "--help" || arg == "-h" => Mode::Help,
        Some(_) => return Err(()),
    };
    if args.next().is_some() {
        return Err(());
    }
    Ok(mode)
}

fn usage() {
    eprintln!(
        "Usage: dsh-mcp [--daemon | --proxy | --acknowledge-cleanup | --help]\n\nNo arguments: serve MCP over stdio.\n--acknowledge-cleanup: after manually checking and cleaning abandoned work, clear the recovery marker under the exclusive instance lock."
    );
}

fn main() -> ExitCode {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(_) => {
            eprintln!("failed to initialize async runtime");
            return ExitCode::from(2);
        }
    };
    let code = runtime.block_on(real_main());
    // Tokio stdin may still own an uncancellable blocking read after a signal.
    // All execution cleanup has already been awaited in real_main.
    runtime.shutdown_background();
    ExitCode::from(code)
}

async fn signal_received(interrupt: &mut Signal, terminate: &mut Signal) {
    tokio::select! {
        _ = interrupt.recv() => {},
        _ = terminate.recv() => {},
    }
}

async fn real_main() -> u8 {
    if tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .try_init()
        .is_err()
    {
        eprintln!("failed to initialize stderr logging");
        return 2;
    }
    let mode = match parse_mode(std::env::args_os().skip(1)) {
        Ok(mode) => mode,
        Err(()) => {
            usage();
            return 2;
        }
    };
    if mode == Mode::Help {
        usage();
        return 0;
    }
    if mode == Mode::Proxy {
        let result = match dsh_mcp::proxy::socket_from_env() {
            Ok(path) => dsh_mcp::proxy::run(path).await,
            Err(e) => Err(e),
        };
        return match result {
            Ok(()) => 0,
            Err(_) => {
                eprintln!("proxy connection failed; check daemon status and DSH_MCP_SOCKET");
                1
            }
        };
    }
    let config = match Config::from_env() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}");
            return 2;
        }
    };
    if mode == Mode::Daemon {
        return match dsh_mcp::daemon::serve(config).await {
            Ok(()) => 0,
            Err(e) => {
                eprintln!("daemon failed: {e}");
                1
            }
        };
    }
    if mode == Mode::AcknowledgeCleanup {
        return match RuntimeGuard::acknowledge_cleanup(&config.state_dir) {
            Ok(()) => {
                eprintln!("recovery marker cleared");
                0
            }
            Err(error) => {
                eprintln!("acknowledge-cleanup failed: {error}");
                2
            }
        };
    }

    // Register both handlers before constructing any execution-capable service.
    let (mut interrupt, mut terminate) = match (
        signal(SignalKind::interrupt()),
        signal(SignalKind::terminate()),
    ) {
        (Ok(interrupt), Ok(terminate)) => (interrupt, terminate),
        _ => {
            eprintln!("failed to install SIGINT/SIGTERM handlers");
            return 2;
        }
    };
    let (guard, recovery) = match RuntimeGuard::acquire(&config.state_dir) {
        Ok(AcquireOutcome::Clean(guard)) => (Arc::new(guard), false),
        Ok(AcquireOutcome::Recovery(guard)) => {
            eprintln!(
                "recovery_required: previous execution cleanup is unconfirmed; task admission is disabled. Stop the bridge, check and clean abandoned work, then run dsh-mcp --acknowledge-cleanup with the same state directory."
            );
            (Arc::new(guard), true)
        }
        Err(error) => {
            eprintln!("runtime guard error: {error}");
            return 2;
        }
    };
    let factory = DshFactory::new(config.clone());
    let coord = spawn_coordinator(config, factory, guard, recovery);
    let server = DshMcpServer::new(coord.clone());
    let initialized = tokio::select! {
        result = rmcp::serve_server(server, rmcp::transport::stdio()) => Some(result),
        _ = signal_received(&mut interrupt, &mut terminate) => {
            tracing::debug!("MCP initialization interrupted by signal");
            None
        },
    };
    if let Some(result) = initialized {
        match result {
            Ok(running) => {
                let cancellation = running.cancellation_token();
                tokio::select! {
                    result = running.waiting() => {
                        if result.is_err() {
                            tracing::warn!("MCP service ended with an error");
                        }
                    },
                    _ = signal_received(&mut interrupt, &mut terminate) => cancellation.cancel(),
                }
            }
            Err(error) => {
                // Do not log peer-controlled initialization payloads.
                if !matches!(
                    error,
                    rmcp::service::ServerInitializeError::ConnectionClosed(_)
                ) {
                    tracing::warn!("MCP initialization ended before service became ready");
                }
            }
        }
    }
    tracing::debug!("coordinator shutdown starting");
    let clean = coord.shutdown().await;
    tracing::debug!(clean, "coordinator shutdown finished");
    if clean { 0 } else { 1 }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;

    #[test]
    fn no_arguments_start_stdio_and_cleanup_is_local_only() {
        assert_eq!(parse_mode([]), Ok(Mode::Serve));
        assert_eq!(
            parse_mode([OsString::from("--acknowledge-cleanup")]),
            Ok(Mode::AcknowledgeCleanup)
        );
        assert_eq!(parse_mode([OsString::from("--help")]), Ok(Mode::Help));
    }

    #[test]
    fn unknown_or_extra_arguments_are_rejected() {
        assert!(parse_mode([OsString::from("--unknown")]).is_err());
        assert!(
            parse_mode([
                OsString::from("--acknowledge-cleanup"),
                OsString::from("--unknown")
            ])
            .is_err()
        );
        assert!(
            parse_mode([
                OsString::from("--acknowledge-cleanup"),
                OsString::from("--acknowledge-cleanup")
            ])
            .is_err()
        );
    }

    #[test]
    fn invalid_unicode_argument_is_usage_error() {
        use std::os::unix::ffi::OsStringExt;
        assert!(parse_mode([OsString::from_vec(vec![0xff])]).is_err());
    }
}

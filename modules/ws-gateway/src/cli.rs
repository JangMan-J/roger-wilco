//! Command-line configuration and atomic port-file helper for `roger-ws-gateway`.

use std::fmt;
use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

pub const USAGE: &str = "\
Usage: roger-ws-gateway --bind <address:port> --socket <path|tcp://address:port> --store-dir <dir>
                        [--port-file <path>] [--token-env <NAME>]

Environment: ROGER_CLIENT_TOKEN (non-empty) is sent as the first line on every
tcp:// connection to the host daemon.";

/// Environment variable naming the token sent on `tcp://` host-daemon connections.
pub const CLIENT_TOKEN_ENV: &str = "ROGER_CLIENT_TOKEN";

/// Target address of the host `roger daemon`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DaemonSocket {
    Unix(PathBuf),
    Tcp(String),
}

impl DaemonSocket {
    pub fn parse(raw: &str) -> io::Result<Self> {
        if raw.is_empty() {
            return Err(bad(
                "expected --socket <path|tcp://address:port>, saw an empty value".into(),
            ));
        }
        if let Some(addr) = raw.strip_prefix("tcp://") {
            if addr.is_empty() {
                return Err(bad(
                    "expected --socket tcp://<address:port>, saw tcp:// with no address".into(),
                ));
            }
            Ok(DaemonSocket::Tcp(addr.to_string()))
        } else {
            Ok(DaemonSocket::Unix(PathBuf::from(raw)))
        }
    }
}

impl fmt::Display for DaemonSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DaemonSocket::Unix(p) => write!(f, "{}", p.display()),
            DaemonSocket::Tcp(addr) => write!(f, "tcp://{addr}"),
        }
    }
}

/// How to reach the host daemon: its address plus the optional client token
/// sent first on `tcp://` connections.
#[derive(Clone, PartialEq, Eq)]
pub struct Dialer {
    pub socket: DaemonSocket,
    pub client_token: Option<String>,
}

impl fmt::Debug for Dialer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Dialer")
            .field("socket", &self.socket)
            .field("client_token", &self.client_token.as_ref().map(|_| "<set>"))
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub bind: String,
    pub dialer: Dialer,
    pub store_dir: PathBuf,
    pub port_file: Option<PathBuf>,
    pub bearer_token: Option<String>,
    #[cfg(feature = "test-hooks")]
    pub hold_before_ack: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CliAction {
    Help,
    Run(Config),
}

fn bad(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, msg)
}

/// Parse command-line arguments (`args` includes `argv[0]`).
pub fn parse_args(args: &[String], env: &dyn Fn(&str) -> Option<String>) -> io::Result<CliAction> {
    if args
        .iter()
        .skip(1)
        .any(|a| matches!(a.as_str(), "--help" | "-h"))
    {
        return Ok(CliAction::Help);
    }

    let mut bind: Option<String> = None;
    let mut socket: Option<DaemonSocket> = None;
    let mut store_dir: Option<PathBuf> = None;
    let mut port_file: Option<PathBuf> = None;
    let mut token_env: Option<String> = None;
    #[cfg(feature = "test-hooks")]
    let mut hold_before_ack: Option<PathBuf> = None;

    let mut it = args.iter().skip(1);
    while let Some(flag) = it.next() {
        let mut next_val = || {
            it.next()
                .cloned()
                .ok_or_else(|| bad(format!("expected a value after {flag}, saw none")))
        };
        match flag.as_str() {
            "--bind" => {
                let v = next_val()?;
                if v.is_empty() {
                    return Err(bad("expected --bind <address:port>, saw an empty value".into()));
                }
                bind = Some(v);
            }
            "--socket" => {
                let v = next_val()?;
                socket = Some(DaemonSocket::parse(&v)?);
            }
            "--store-dir" => {
                let v = next_val()?;
                if v.is_empty() {
                    return Err(bad("expected --store-dir <dir>, saw an empty value".into()));
                }
                store_dir = Some(PathBuf::from(v));
            }
            "--port-file" => {
                let v = next_val()?;
                if v.is_empty() {
                    return Err(bad("expected --port-file <path>, saw an empty value".into()));
                }
                port_file = Some(PathBuf::from(v));
            }
            "--token-env" => {
                let v = next_val()?;
                if v.is_empty() {
                    return Err(bad(
                        "expected --token-env <NAME>, saw an empty variable name".into(),
                    ));
                }
                token_env = Some(v);
            }
            #[cfg(feature = "test-hooks")]
            "--hold-before-ack" => {
                let v = next_val()?;
                if v.is_empty() {
                    return Err(bad(
                        "expected --hold-before-ack <marker>, saw an empty path".into(),
                    ));
                }
                hold_before_ack = Some(PathBuf::from(v));
            }
            other => {
                return Err(bad(format!(
                    "expected a flag (--bind, --socket, --store-dir, --port-file, --token-env, --help), saw {other}"
                )))
            }
        }
    }

    let bind = bind.ok_or_else(|| bad("missing --bind".into()))?;
    let socket = socket.ok_or_else(|| bad("missing --socket".into()))?;
    let store_dir = store_dir.ok_or_else(|| bad("missing --store-dir".into()))?;

    let bearer_token = match token_env {
        None => None,
        Some(name) => {
            let val = env(&name).filter(|v| !v.is_empty()).ok_or_else(|| {
                bad(format!(
                    "expected environment variable {name} to be set and non-empty, saw unset or empty"
                ))
            })?;
            Some(val)
        }
    };
    let client_token = env(CLIENT_TOKEN_ENV).filter(|v| !v.is_empty());

    Ok(CliAction::Run(Config {
        bind,
        dialer: Dialer {
            socket,
            client_token,
        },
        store_dir,
        port_file,
        bearer_token,
        #[cfg(feature = "test-hooks")]
        hold_before_ack,
    }))
}

/// Atomically write `<port>\n` to `path` via `<path>.tmp` -> `sync_all` -> `rename`.
pub fn write_port_file(path: &Path, port: u16) -> io::Result<()> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    let mut file = File::create(&tmp)?;
    file.write_all(format!("{port}\n").as_bytes())?;
    file.sync_all()?;
    std::fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        crate::durlog::sync_dir(parent)?;
    }
    Ok(())
}

/// Constant-time byte slice equality check for Bearer token comparison.
pub use roger_wire::wire::same_secret_bytes as constant_time_eq;

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(items: &[&str]) -> Vec<String> {
        std::iter::once("roger-ws-gateway")
            .chain(items.iter().copied())
            .map(str::to_string)
            .collect()
    }

    #[test]
    fn parses_required_and_optional_flags_and_client_token() {
        let env = |k: &str| match k {
            "GW_TOKEN" => Some("secret-123".to_string()),
            CLIENT_TOKEN_ENV => Some("daemon-tok".to_string()),
            _ => None,
        };
        let got = parse_args(
            &argv(&[
                "--bind",
                "127.0.0.1:0",
                "--socket",
                "tcp://127.0.0.1:9000",
                "--store-dir",
                "/tmp/stores",
                "--port-file",
                "/tmp/gw.port",
                "--token-env",
                "GW_TOKEN",
            ]),
            &env,
        )
        .unwrap();
        let CliAction::Run(cfg) = got else {
            panic!("expected Run");
        };
        assert_eq!(cfg.bind, "127.0.0.1:0");
        assert_eq!(
            cfg.dialer.socket,
            DaemonSocket::Tcp("127.0.0.1:9000".into())
        );
        assert_eq!(cfg.dialer.client_token.as_deref(), Some("daemon-tok"));
        assert_eq!(cfg.store_dir, PathBuf::from("/tmp/stores"));
        assert_eq!(cfg.port_file, Some(PathBuf::from("/tmp/gw.port")));
        assert_eq!(cfg.bearer_token.as_deref(), Some("secret-123"));
    }

    #[test]
    fn empty_client_token_counts_as_unset() {
        let env = |k: &str| (k == CLIENT_TOKEN_ENV).then(String::new);
        let CliAction::Run(cfg) = parse_args(
            &argv(&["--bind", "x:1", "--socket", "/s", "--store-dir", "/d"]),
            &env,
        )
        .unwrap() else {
            panic!("expected Run");
        };
        assert_eq!(cfg.dialer.client_token, None);
    }

    #[test]
    fn help_flag_returns_help_action() {
        let env = |_: &str| None;
        assert_eq!(
            parse_args(&argv(&["--help"]), &env).unwrap(),
            CliAction::Help
        );
        assert_eq!(parse_args(&argv(&["-h"]), &env).unwrap(), CliAction::Help);
    }

    #[test]
    fn missing_or_unknown_flags_report_expected_and_saw() {
        let env = |_: &str| None;
        let err = parse_args(&argv(&["--socket", "/s", "--store-dir", "/d"]), &env)
            .unwrap_err()
            .to_string();
        assert_eq!(err, "missing --bind");

        let err = parse_args(&argv(&["--unknown"]), &env)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("expected") && err.contains("saw --unknown"),
            "{err}"
        );

        let err = parse_args(
            &argv(&[
                "--bind",
                "127.0.0.1:0",
                "--socket",
                "/s",
                "--store-dir",
                "/d",
                "--token-env",
                "MISSING_ENV",
            ]),
            &env,
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("MISSING_ENV") && err.contains("saw unset or empty"),
            "{err}"
        );
    }

    #[test]
    fn constant_time_comparison_matches_exact_bytes() {
        assert!(constant_time_eq(b"token-abc", b"token-abc"));
        assert!(!constant_time_eq(b"token-abc", b"token-abd"));
        assert!(!constant_time_eq(b"token-abc", b"token-ab"));
    }
}

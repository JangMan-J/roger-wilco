mod audit;
mod claims;
#[allow(dead_code)]
mod cold;
#[allow(dead_code)]
mod sha256;
#[allow(dead_code)]
pub mod sql;
mod coordinator;
mod daemon;
mod durlog;
mod headers;
mod json;
mod mcp;
mod net;
mod session;

use std::io;
use std::path::PathBuf;

fn arg(args: &[String], name: &str) -> Option<String> {
    args.windows(2).find(|w| w[0] == name).map(|w| w[1].clone())
}

fn need(args: &[String], name: &str) -> io::Result<String> {
    arg(args, name)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, format!("missing {name}")))
}

/// Every `--header key=value`, split at the first `=`, as the object a send
/// carries; None when there is none. Bounds are the coordinator's to check.
fn headers(args: &[String]) -> io::Result<Option<json::Value>> {
    let mut pairs = Vec::new();
    for w in args.windows(2).filter(|w| w[0] == "--header") {
        let (k, v) = w[1].split_once('=').ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("--header expects key=value, saw {}", w[1]),
            )
        })?;
        pairs.push((k.to_string(), json::s(v)));
    }
    Ok((!pairs.is_empty()).then_some(json::Value::Obj(pairs)))
}

/// Read the fleet token from the environment to keep it out of command-line listings.
fn token() -> io::Result<String> {
    std::env::var("ROGER_TOKEN")
        .ok()
        .filter(|t| !t.is_empty())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing ROGER_TOKEN"))
}

const USAGE: &str = "Usage: roger <command> [flags]

Commands:
  coordinator --bind <address:port> --data <log>
              [--port-file <path>] [--unregister-after-ms <ms>]
  daemon      --coordinator <address:port> --host-id <host> [--socket <path>] [--listen <address:port>] [--port-file <path>]
              [--client-token-env <NAME>] --data <log>
  session     --socket <path> --store <path> [--id <address>] [--label <text>] [--declare <text>]
  adapter     --socket <path> --store <path> [--id <address>] [--label <text>] [--declare <text>]
              [--registered]   (also tell the client `registered` once the coordinator holds the Register)
              [--superseded]   (also emit replacement notices on stdout; stderr always warns)
              [--watch]        (also emit committed Board changes as board_event lines)
  mcp         [--socket <path>] [--store <file>] [--id <address> | --id-env <NAME>]
              [--label <text>] [--declare <text>]   (MCP server on stdio; a session)
  send        --socket <path> --from <address> --to <address|*> --body <text> [--mail <id>]
              [--header <key=value>]...
  claim       --socket <path> --owner <address> --key <text> --ttl-ms <ms>
  renew       --socket <path> --owner <address> --key <text> --ttl-ms <ms>
  release     --socket <path> --owner <address> --key <text>
  board       --socket <path> [--from <address>]
  watch       --socket <path>   (prints committed Board changes until the connection ends)
  label       --socket <path> --from <address> --label <text>
  receipt     --socket <path> --mail <id>
  audit       [--coordinator-log <file>] [--daemon-log <file>]... [--store <file>]...
              [--address <address>] [--bodies] [--json]   (read-only; no lock, no write)

Set ROGER_TOKEN for coordinator and daemon. Quote '*' to broadcast.
Client --socket accepts a Unix socket path or tcp://<address:port>; over tcp:// a
client sends ROGER_CLIENT_TOKEN first when set (required by --client-token-env).
Session addresses and mail IDs are generated when omitted.
Claim refusal exits with status 3; an unknown command exits with status 2.
Use --help or -h for this help.";

fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    #[cfg(feature = "test-hooks")]
    if let Some(marker) = arg(&args, "--hold-before-ack") {
        let _ = session::HOLD_BEFORE_ACK.set(marker.into());
    }
    #[cfg(feature = "test-hooks")]
    if let Some(ms) = arg(&args, "--link-pace-ms").and_then(|ms| ms.parse().ok()) {
        let _ = net::LINK_PACE.set(std::time::Duration::from_millis(ms));
    }
    let cmd = args.get(1).map(|s| s.as_str()).unwrap_or("");
    if matches!(cmd, "--help" | "-h" | "help") {
        println!("{USAGE}");
        return Ok(());
    }
    match cmd {
        "coordinator" => coordinator::run(
            &need(&args, "--bind")?,
            arg(&args, "--port-file").map(PathBuf::from),
            PathBuf::from(need(&args, "--data")?),
            token()?,
            match arg(&args, "--unregister-after-ms") {
                Some(ms) => ms.parse().map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("--unregister-after-ms expects milliseconds, saw {ms}"),
                    )
                })?,
                None => coordinator::UNREGISTER_AFTER_MS,
            },
        ),
        "daemon" => {
            let coordinator = need(&args, "--coordinator")?;
            let token = token()?;
            let host_id = need(&args, "--host-id")?;
            let socket = arg(&args, "--socket").map(PathBuf::from);
            let listen = arg(&args, "--listen");
            let port_file = arg(&args, "--port-file").map(PathBuf::from);
            let client_token = match arg(&args, "--client-token-env") {
                Some(name) => Some(
                    std::env::var(&name)
                        .ok()
                        .filter(|t| !t.is_empty())
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!("expected a non-empty token in ${name} (--client-token-env), saw none"),
                            )
                        })?,
                ),
                None => None,
            };
            if socket.is_none() && listen.is_none() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "missing --socket",
                ));
            }
            if port_file.is_some() && listen.is_none() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "expected --listen with --port-file, saw none",
                ));
            }
            daemon::run(daemon::Config {
                coordinator,
                token,
                host_id,
                socket,
                listen,
                port_file,
                client_token,
                data: PathBuf::from(need(&args, "--data")?),
            })
        }
        "session" => session::run(
            PathBuf::from(need(&args, "--socket")?),
            match arg(&args, "--id") {
                Some(id) => id,
                None => session::gen_uuid()?,
            },
            arg(&args, "--label"),
            PathBuf::from(need(&args, "--store")?),
            arg(&args, "--declare"),
        ),
        "adapter" => session::adapter(
            PathBuf::from(need(&args, "--socket")?),
            match arg(&args, "--id") {
                Some(id) => id,
                None => session::gen_uuid()?,
            },
            arg(&args, "--label"),
            PathBuf::from(need(&args, "--store")?),
            arg(&args, "--declare"),
            (
                args.iter().any(|a| a == "--registered"),
                args.iter().any(|a| a == "--superseded"),
                args.iter().any(|a| a == "--watch"),
            ),
        ),
        "mcp" => mcp::run(&args),
        "send" => session::send(
            PathBuf::from(need(&args, "--socket")?),
            need(&args, "--from")?,
            need(&args, "--to")?,
            match arg(&args, "--mail") {
                Some(m) => m,
                None => session::gen_uuid()?,
            },
            need(&args, "--body")?,
            headers(&args)?,
        ),
        "claim" | "renew" | "release" => {
            let op = claims::Op::parse(if cmd == "claim" { "acquire" } else { cmd })
                .expect("the match arm names only claim ops");
            let ttl_ms = match arg(&args, "--ttl-ms") {
                Some(t) => t.parse().map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("--ttl-ms expects milliseconds, saw {t}"),
                    )
                })?,
                None => 0,
            };
            let granted = session::claim(
                PathBuf::from(need(&args, "--socket")?),
                &op,
                need(&args, "--owner")?,
                need(&args, "--key")?,
                ttl_ms,
            )?;
            if !granted {
                std::process::exit(3);
            }
            Ok(())
        }
        "board" => session::board(
            PathBuf::from(need(&args, "--socket")?),
            arg(&args, "--from"),
        ),
        "receipt" => session::receipt(
            PathBuf::from(need(&args, "--socket")?),
            need(&args, "--mail")?,
        ),
        "label" => session::relabel(
            PathBuf::from(need(&args, "--socket")?),
            need(&args, "--from")?,
            need(&args, "--label")?,
        ),
        "watch" => session::watch(PathBuf::from(need(&args, "--socket")?)),
        "audit" => audit::run(&args[2..]),
        _ => {
            eprintln!("{USAGE}");
            std::process::exit(2);
        }
    }
}

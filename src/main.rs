mod audit;
mod claims;
#[allow(dead_code)]
mod cold;
mod coordinator;
mod daemon;
mod headers;
mod mcp;
mod net;
mod session;
#[allow(dead_code)]
pub mod sql;

#[allow(unused_imports)]
pub use roger_wire::{durlog, json, sha256};
use std::io;
use std::path::PathBuf;

fn arg(args: &[String], name: &str) -> Option<String> {
    args.windows(2).find(|w| w[0] == name).map(|w| w[1].clone())
}

fn args_named(args: &[String], name: &str) -> Vec<String> {
    args.windows(2)
        .filter(|w| w[0] == name)
        .map(|w| w[1].clone())
        .collect()
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
  coordinator --bind <address:port> --data <path>
              [--port-file <path>] [--unregister-after-ms <ms>]
              [--slices <n>] [--cold-retain-days <days>] [--no-cold-sweep]
  daemon      --coordinator <address:port> --host-id <host> [--socket <path>] [--listen <address:port>] [--port-file <path>]
              [--client-token-env <NAME>] --data <log>
  session     --socket <path> --store <path> [--id <address>] [--label <text>] [--declare <text>]
              [--subscribe <group>]...
  adapter     --socket <path> --store <path> [--id <address>] [--label <text>] [--declare <text>]
              [--registered]   (also tell the client `registered` once the coordinator holds the Register)
              [--superseded]   (also emit replacement notices on stdout; stderr always warns)
              [--watch]        (also emit committed Board changes as board_event lines)
              [--groups]       (opt in to subscribe, unsubscribe, groups, and group sends)
  mcp         [--socket <path>] [--store <file>] [--id <address> | --id-env <NAME>]
              [--label <text>] [--declare <text>] [--groups]   (MCP server on stdio; a session)
  send        --socket <path> --from <address> (--to <address|*> | --group <group>) --body <text>
              [--mail <id>] [--header <key=value>]... [--expected-article <n>]
  subscribe   --socket <path> --session <address> --group <group> [--after <n> | --latest]
  unsubscribe --socket <path> --session <address> --group <group>
  groups      --socket <path> [--session <address>] [--prefix <prefix>] [--include-retired]
  articles    --socket <path> --group <group> [--after <n>] [--limit <n>] [--follow] [--json]
  group-policy --socket <path> --group <group> [--keep-last <n|none>] [--ttl-ms <ms|none>] [--retire | --reopen]
  claim       --socket <path> --owner <address> --key <text> --ttl-ms <ms>
  renew       --socket <path> --owner <address> --key <text> --ttl-ms <ms>
  release     --socket <path> --owner <address> --key <text>
  board       --socket <path> [--from <address>]
  watch       --socket <path>   (prints committed Board changes until the connection ends)
  label       --socket <path> --from <address> --label <text>
  receipt     --socket <path> --mail <id>
  audit       [--coordinator-log <file>] [--daemon-log <file>]... [--store <file>]...
              [--address <address>] [--bodies] [--json]   (concurrent read-only)

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
        "coordinator" => coordinator::run(coordinator::Config::from_args(&args, token()?)?),
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
        "session" => session::run_with_groups(
            PathBuf::from(need(&args, "--socket")?),
            match arg(&args, "--id") {
                Some(id) => id,
                None => session::gen_uuid()?,
            },
            arg(&args, "--label"),
            PathBuf::from(need(&args, "--store")?),
            arg(&args, "--declare"),
            args_named(&args, "--subscribe"),
        ),
        "adapter" => session::adapter_with_groups(
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
                args.iter().any(|a| a == "--groups"),
            ),
        ),
        "mcp" => mcp::run(&args),
        "send" => {
            let socket = PathBuf::from(need(&args, "--socket")?);
            let from = need(&args, "--from")?;
            let to = arg(&args, "--to");
            let group = arg(&args, "--group");
            let mail = match arg(&args, "--mail") {
                Some(m) => m,
                None => session::gen_uuid()?,
            };
            let body = need(&args, "--body")?;
            let hdrs = headers(&args)?;
            let expected_article = match arg(&args, "--expected-article") {
                Some(n) => Some(n.parse::<i64>().map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("--expected-article expects a non-negative integer, saw {n}"),
                    )
                })?),
                None => None,
            };
            match (to, group) {
                (Some(_), Some(_)) => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "expected either --to or --group, saw both",
                )),
                (Some(to), None) => {
                    if expected_article.is_some() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "--expected-article requires --group",
                        ));
                    }
                    session::send(socket, from, to, mail, body, hdrs)
                }
                (None, Some(grp)) => {
                    session::send_group(socket, from, grp, mail, body, hdrs, expected_article)
                }
                (None, None) => Err(io::Error::new(io::ErrorKind::InvalidInput, "missing --to")),
            }
        }
        "subscribe" => {
            let after = match arg(&args, "--after") {
                Some(n) => Some(n.parse::<i64>().map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("--after expects a non-negative integer, saw {n}"),
                    )
                })?),
                None => None,
            };
            let latest = args.iter().any(|a| a == "--latest");
            if after.is_some() && latest {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "expected at most one of --after or --latest, saw both",
                ));
            }
            session::subscribe(
                PathBuf::from(need(&args, "--socket")?),
                need(&args, "--session")?,
                need(&args, "--group")?,
                after,
                latest,
            )
        }
        "unsubscribe" => session::unsubscribe(
            PathBuf::from(need(&args, "--socket")?),
            need(&args, "--session")?,
            need(&args, "--group")?,
        ),
        "groups" => session::groups(
            PathBuf::from(need(&args, "--socket")?),
            arg(&args, "--session"),
            arg(&args, "--prefix"),
            args.iter().any(|a| a == "--include-retired"),
        ),
        "articles" => {
            let after = match arg(&args, "--after") {
                Some(n) => n.parse::<i64>().map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("--after expects a non-negative integer, saw {n}"),
                    )
                })?,
                None => 0,
            };
            let limit = match arg(&args, "--limit") {
                Some(n) => n.parse::<usize>().map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("--limit expects a positive integer, saw {n}"),
                    )
                })?,
                None => 100,
            };
            session::articles(
                PathBuf::from(need(&args, "--socket")?),
                need(&args, "--group")?,
                after,
                limit,
                args.iter().any(|a| a == "--follow"),
                args.iter().any(|a| a == "--json"),
            )
        }
        "group-policy" => {
            let parse_clearable = |flag: &str| -> io::Result<Option<Option<i64>>> {
                match arg(&args, flag) {
                    None => Ok(None),
                    Some(v) if v == "none" => Ok(Some(None)),
                    Some(v) => {
                        let n = v.parse::<i64>().map_err(|_| {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!("{flag} expects a non-negative integer or 'none', saw {v}"),
                            )
                        })?;
                        if n < 0 {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!("{flag} expects a non-negative integer or 'none', saw {v}"),
                            ));
                        }
                        Ok(Some(Some(n)))
                    }
                }
            };
            let retire = args.iter().any(|a| a == "--retire");
            let reopen = args.iter().any(|a| a == "--reopen");
            if retire && reopen {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "expected at most one of --retire or --reopen, saw both",
                ));
            }
            let retired = if retire {
                Some(true)
            } else if reopen {
                Some(false)
            } else {
                None
            };
            session::group_policy(
                PathBuf::from(need(&args, "--socket")?),
                need(&args, "--group")?,
                parse_clearable("--keep-last")?,
                parse_clearable("--ttl-ms")?,
                retired,
            )
        }
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

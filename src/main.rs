mod audit;
mod backup;
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

#[derive(Default)]
pub(crate) struct Flags {
    values: Vec<(String, String)>,
    switches: Vec<String>,
}

impl Flags {
    fn get(&self, name: &str) -> Option<String> {
        self.values
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.clone())
    }

    fn all(&self, name: &str) -> Vec<String> {
        self.values
            .iter()
            .filter(|(key, _)| key == name)
            .map(|(_, value)| value.clone())
            .collect()
    }

    fn has(&self, name: &str) -> bool {
        self.switches.iter().any(|key| key == name)
    }

    fn need(&self, name: &str) -> io::Result<String> {
        self.get(name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, format!("missing {name}")))
    }
}

pub(crate) fn parse_flags(
    cmd: &str,
    args: &[String],
    valued: &[&str],
    switches: &[&str],
) -> io::Result<Flags> {
    #[cfg(feature = "test-hooks")]
    const HOOKS: &[&str] = &["--hold-before-ack", "--link-pace-ms"];
    #[cfg(not(feature = "test-hooks"))]
    const HOOKS: &[&str] = &[];
    let mut flags = Flags::default();
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        let flag = arg.as_str();
        if valued.contains(&flag) || HOOKS.contains(&flag) {
            let value = args.next().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("expected a value after {flag}, saw none"),
                )
            })?;
            flags.values.push((arg.clone(), value.clone()));
        } else if switches.contains(&flag) {
            flags.switches.push(arg.clone());
        } else {
            let expected = [valued, switches, HOOKS].concat().join(", ");
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("expected one of {expected} for {cmd}, saw {arg}"),
            ));
        }
    }
    Ok(flags)
}

type FlagSpec = (&'static [&'static str], &'static [&'static str]);

fn flag_spec(cmd: &str) -> Option<FlagSpec> {
    Some(match cmd {
        "backup" => (&["--data", "--dest", "--kind"], &[]),
        "coordinator" => (
            &[
                "--bind",
                "--data",
                "--port-file",
                "--unregister-after-ms",
                "--slices",
                "--cold-retain-days",
                "--cold-retain-ms",
                "--cold-day-ms",
            ],
            &["--no-cold-sweep"],
        ),
        "daemon" => (
            &[
                "--coordinator",
                "--host-id",
                "--socket",
                "--listen",
                "--port-file",
                "--client-token-env",
                "--data",
            ],
            &[],
        ),
        "session" => (
            &[
                "--socket",
                "--store",
                "--id",
                "--label",
                "--declare",
                "--subscribe",
            ],
            &[],
        ),
        "adapter" => (
            &["--socket", "--store", "--id", "--label", "--declare"],
            &["--registered", "--superseded", "--watch", "--groups"],
        ),
        "mcp" => (
            &[
                "--socket",
                "--store",
                "--id",
                "--id-env",
                "--label",
                "--declare",
            ],
            &["--groups"],
        ),
        "send" => (
            &[
                "--socket",
                "--from",
                "--to",
                "--group",
                "--body",
                "--mail",
                "--header",
                "--expected-article",
            ],
            &[],
        ),
        "subscribe" => (
            &["--socket", "--session", "--group", "--after"],
            &["--latest"],
        ),
        "unsubscribe" => (&["--socket", "--session", "--group"], &[]),
        "groups" => (
            &["--socket", "--session", "--prefix"],
            &["--include-retired"],
        ),
        "articles" => (
            &["--socket", "--group", "--after", "--limit"],
            &["--follow", "--json"],
        ),
        "group-policy" => (
            &["--socket", "--group", "--keep-last", "--ttl-ms"],
            &["--retire", "--reopen"],
        ),
        "claim" | "renew" | "release" => (&["--socket", "--owner", "--key", "--ttl-ms"], &[]),
        "board" => (&["--socket", "--from"], &[]),
        "watch" => (&["--socket"], &[]),
        "label" => (&["--socket", "--from", "--label"], &[]),
        "receipt" => (&["--socket", "--mail"], &[]),
        _ => return None,
    })
}

/// Every `--header key=value`, split at the first `=`, as the object a send
/// carries; None when there is none. Bounds are the coordinator's to check.
fn headers(flags: &Flags) -> io::Result<Option<json::Value>> {
    let mut pairs = Vec::new();
    for header in flags.all("--header") {
        let (k, v) = header.split_once('=').ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("--header expects key=value, saw {header}"),
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
  backup      --data <path> --dest <dir> [--kind coordinator|daemon]   (live, crash-safe copy)

Set ROGER_TOKEN for coordinator and daemon. Quote '*' to broadcast.
Client --socket accepts a Unix socket path or tcp://<address:port>; over tcp:// a
client sends ROGER_CLIENT_TOKEN first when set (required by --client-token-env).
Session addresses and mail IDs are generated when omitted.
Claim refusal exits with status 3; an unknown command exits with status 2.
Use --help or -h for this help.";

fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let cmd = args.get(1).map(|s| s.as_str()).unwrap_or("");
    if matches!(cmd, "--help" | "-h" | "help") {
        println!("{USAGE}");
        return Ok(());
    }
    let flags = match flag_spec(cmd) {
        Some((v, sw)) => parse_flags(cmd, &args[2..], v, sw)?,
        None => Flags::default(),
    };
    #[cfg(feature = "test-hooks")]
    if let Some(marker) = flags.get("--hold-before-ack") {
        let _ = session::HOLD_BEFORE_ACK.set(marker.into());
    }
    #[cfg(feature = "test-hooks")]
    if let Some(ms) = flags.get("--link-pace-ms").and_then(|ms| ms.parse().ok()) {
        let _ = net::LINK_PACE.set(std::time::Duration::from_millis(ms));
    }
    match cmd {
        "coordinator" => coordinator::run(coordinator::Config::from_args(&args, token()?)?),
        "daemon" => {
            let coordinator = flags.need("--coordinator")?;
            let token = token()?;
            let host_id = flags.need("--host-id")?;
            let socket = flags.get("--socket").map(PathBuf::from);
            let listen = flags.get("--listen");
            let port_file = flags.get("--port-file").map(PathBuf::from);
            let client_token = match flags.get("--client-token-env") {
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
                data: PathBuf::from(flags.need("--data")?),
            })
        }
        "session" => session::run_with_groups(
            PathBuf::from(flags.need("--socket")?),
            match flags.get("--id") {
                Some(id) => id,
                None => session::gen_uuid()?,
            },
            flags.get("--label"),
            PathBuf::from(flags.need("--store")?),
            flags.get("--declare"),
            flags.all("--subscribe"),
        ),
        "adapter" => session::adapter_with_groups(
            PathBuf::from(flags.need("--socket")?),
            match flags.get("--id") {
                Some(id) => id,
                None => session::gen_uuid()?,
            },
            flags.get("--label"),
            PathBuf::from(flags.need("--store")?),
            flags.get("--declare"),
            (
                flags.has("--registered"),
                flags.has("--superseded"),
                flags.has("--watch"),
                flags.has("--groups"),
            ),
        ),
        "mcp" => mcp::run(&args),
        "send" => {
            let socket = PathBuf::from(flags.need("--socket")?);
            let from = flags.need("--from")?;
            let to = flags.get("--to");
            let group = flags.get("--group");
            let mail = match flags.get("--mail") {
                Some(m) => m,
                None => session::gen_uuid()?,
            };
            let body = flags.need("--body")?;
            let hdrs = headers(&flags)?;
            let expected_article = match flags.get("--expected-article") {
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
            let after = match flags.get("--after") {
                Some(n) => Some(n.parse::<i64>().map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("--after expects a non-negative integer, saw {n}"),
                    )
                })?),
                None => None,
            };
            let latest = flags.has("--latest");
            if after.is_some() && latest {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "expected at most one of --after or --latest, saw both",
                ));
            }
            session::subscribe(
                PathBuf::from(flags.need("--socket")?),
                flags.need("--session")?,
                flags.need("--group")?,
                after,
                latest,
            )
        }
        "unsubscribe" => session::unsubscribe(
            PathBuf::from(flags.need("--socket")?),
            flags.need("--session")?,
            flags.need("--group")?,
        ),
        "groups" => session::groups(
            PathBuf::from(flags.need("--socket")?),
            flags.get("--session"),
            flags.get("--prefix"),
            flags.has("--include-retired"),
        ),
        "articles" => {
            let after = match flags.get("--after") {
                Some(n) => n.parse::<i64>().map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("--after expects a non-negative integer, saw {n}"),
                    )
                })?,
                None => 0,
            };
            let limit = match flags.get("--limit") {
                Some(n) => n.parse::<usize>().map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("--limit expects a positive integer, saw {n}"),
                    )
                })?,
                None => 100,
            };
            session::articles(
                PathBuf::from(flags.need("--socket")?),
                flags.need("--group")?,
                after,
                limit,
                flags.has("--follow"),
                flags.has("--json"),
            )
        }
        "group-policy" => {
            let parse_clearable = |flag: &str| -> io::Result<Option<Option<i64>>> {
                match flags.get(flag) {
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
            let retire = flags.has("--retire");
            let reopen = flags.has("--reopen");
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
                PathBuf::from(flags.need("--socket")?),
                flags.need("--group")?,
                parse_clearable("--keep-last")?,
                parse_clearable("--ttl-ms")?,
                retired,
            )
        }
        "claim" | "renew" | "release" => {
            let op = claims::Op::parse(if cmd == "claim" { "acquire" } else { cmd })
                .expect("the match arm names only claim ops");
            let ttl_ms = match flags.get("--ttl-ms") {
                Some(t) => t.parse().map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("--ttl-ms expects milliseconds, saw {t}"),
                    )
                })?,
                None => 0,
            };
            let granted = session::claim(
                PathBuf::from(flags.need("--socket")?),
                &op,
                flags.need("--owner")?,
                flags.need("--key")?,
                ttl_ms,
            )?;
            if !granted {
                std::process::exit(3);
            }
            Ok(())
        }
        "board" => session::board(PathBuf::from(flags.need("--socket")?), flags.get("--from")),
        "receipt" => session::receipt(
            PathBuf::from(flags.need("--socket")?),
            flags.need("--mail")?,
        ),
        "label" => session::relabel(
            PathBuf::from(flags.need("--socket")?),
            flags.need("--from")?,
            flags.need("--label")?,
        ),
        "watch" => session::watch(PathBuf::from(flags.need("--socket")?)),
        "audit" => audit::run(&args[2..]),
        "backup" => backup::run(&args[2..]),
        _ => {
            eprintln!("{USAGE}");
            std::process::exit(2);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(cmd: &str, args: &[&str]) -> io::Result<Flags> {
        let (valued, switches) = flag_spec(cmd).unwrap();
        let args: Vec<_> = args.iter().map(|arg| arg.to_string()).collect();
        parse_flags(cmd, &args, valued, switches)
    }

    #[test]
    fn a_value_can_look_like_a_flag() {
        let flags = parse("send", &["--body", "--group", "--to", "recipient"]).unwrap();
        assert_eq!(flags.need("--body").unwrap(), "--group");
        assert_eq!(flags.need("--to").unwrap(), "recipient");
        assert_eq!(flags.get("--group"), None);
    }

    #[test]
    fn repeated_headers_keep_their_order() {
        let flags = parse("send", &["--header", "a=1", "--header", "b=2"]).unwrap();
        assert_eq!(flags.get("--header").as_deref(), Some("a=1"));
        assert_eq!(flags.all("--header"), ["a=1", "b=2"]);
    }

    #[test]
    fn a_valued_flag_requires_an_argument() {
        let err = parse("send", &["--body"]).err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(err.to_string(), "expected a value after --body, saw none");
    }

    #[test]
    fn an_unknown_flag_names_the_expected_flags_and_token() {
        let err = parse("watch", &["--unknown"]).err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        #[cfg(feature = "test-hooks")]
        let expected =
            "expected one of --socket, --hold-before-ack, --link-pace-ms for watch, saw --unknown";
        #[cfg(not(feature = "test-hooks"))]
        let expected = "expected one of --socket for watch, saw --unknown";
        assert_eq!(err.to_string(), expected);
    }

    #[test]
    fn switches_take_no_value() {
        let flags = parse("adapter", &["--registered", "--label", "--watch"]).unwrap();
        assert!(flags.has("--registered"));
        assert!(!flags.has("--watch"));
        assert_eq!(flags.get("--label").as_deref(), Some("--watch"));
    }

    #[cfg(feature = "test-hooks")]
    #[test]
    fn test_hook_flags_are_valued() {
        let flags = parse(
            "watch",
            &["--hold-before-ack", "marker", "--link-pace-ms", "10"],
        )
        .unwrap();
        assert_eq!(flags.get("--hold-before-ack").as_deref(), Some("marker"));
        assert_eq!(flags.get("--link-pace-ms").as_deref(), Some("10"));
    }
}

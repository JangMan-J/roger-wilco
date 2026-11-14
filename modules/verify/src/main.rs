use roger_verify::roster;
use roger_verify::verify::{verify_text, Options};
use std::io::{self, Read, Write};
use std::process::ExitCode;

const USAGE: &str =
    "usage: roger-verify --roster <file> [--audit <file>|-] [--json] [--allow-undeclared]
  reads `roger audit --bodies --json` output (stdin unless --audit names a file)
  exit 0: everything verified; 1: a failure was found; 2: usage or unreadable input";

struct Args {
    roster: String,
    audit: Option<String>,
    json: bool,
    allow_undeclared: bool,
}

fn parse_args(args: &[String]) -> Result<Option<Args>, String> {
    let mut roster = None;
    let mut audit = None;
    let mut json = false;
    let mut allow_undeclared = false;
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        let mut value = || {
            it.next()
                .cloned()
                .ok_or_else(|| format!("expected a value after {flag}, saw none"))
        };
        match flag.as_str() {
            "-h" | "--help" => return Ok(None),
            "--roster" => roster = Some(value()?),
            "--audit" => audit = Some(value()?),
            "--json" => json = true,
            "--allow-undeclared" => allow_undeclared = true,
            other => return Err(format!("expected a flag of the usage, saw {other}")),
        }
    }
    let roster = roster.ok_or("expected --roster <file>, saw none")?;
    Ok(Some(Args {
        roster,
        audit: audit.filter(|a| a != "-"),
        json,
        allow_undeclared,
    }))
}

fn run(args: &Args) -> Result<u8, String> {
    let roster_text = std::fs::read_to_string(&args.roster)
        .map_err(|e| format!("expected a readable roster at {}, saw {e}", args.roster))?;
    let roster = roster::parse(&roster_text)?;
    let audit = match &args.audit {
        Some(path) => std::fs::read_to_string(path)
            .map_err(|e| format!("expected a readable audit at {path}, saw {e}"))?,
        None => {
            let mut s = String::new();
            io::stdin()
                .read_to_string(&mut s)
                .map_err(|e| format!("expected audit JSON on stdin, saw {e}"))?;
            s
        }
    };
    let opts = Options {
        allow_undeclared: args.allow_undeclared,
    };
    let (reports, status) = verify_text(&audit, &roster, opts)?;
    let stdout = io::stdout();
    let mut out = io::BufWriter::new(stdout.lock());
    for r in &reports {
        let line = if args.json { r.json() } else { r.text() };
        writeln!(out, "{line}").map_err(|e| format!("writing stdout: {e}"))?;
    }
    out.flush().map_err(|e| format!("writing stdout: {e}"))?;
    Ok(status)
}

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let args = match parse_args(&argv) {
        Ok(Some(a)) => a,
        Ok(None) => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Err(e) => {
            eprintln!("roger-verify: {e}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    match run(&args) {
        Ok(status) => ExitCode::from(status),
        Err(e) => {
            eprintln!("roger-verify: {e}");
            ExitCode::from(2)
        }
    }
}

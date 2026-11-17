use roger_cold_shipper::archive::{
    format_restore, format_ship, format_verify, restore, ship, verify_archive, watch_stdin,
};
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::process::ExitCode;

const USAGE: &str = "usage:
  roger-cold-shipper ship --slices-dir <path> --dest <archive-dir> [--json]
  roger-cold-shipper restore --dest <archive-dir> --slice <slice-name> --cold-dir <local-cold-dir> [--day <bucket>] [--json]
  roger-cold-shipper verify --dest <archive-dir> [--json]
  roger-cold-shipper watch-stdin --slices-dir <path> --dest <archive-dir> [--json]";

#[derive(Debug, PartialEq, Eq)]
enum Command {
    Ship {
        slices_dir: PathBuf,
        dest: PathBuf,
        json: bool,
    },
    Restore {
        dest: PathBuf,
        slice: String,
        cold_dir: PathBuf,
        day: Option<i64>,
        json: bool,
    },
    Verify {
        dest: PathBuf,
        json: bool,
    },
    WatchStdin {
        slices_dir: PathBuf,
        dest: PathBuf,
        json: bool,
    },
}

fn parse_args(args: &[String]) -> Result<Option<Command>, String> {
    let Some(sub) = args.first() else {
        return Err(
            "expected subcommand (ship, restore, verify, watch-stdin), saw none".to_string(),
        );
    };
    match sub.as_str() {
        "-h" | "--help" => Ok(None),
        "ship" => parse_ship(&args[1..]),
        "restore" => parse_restore(&args[1..]),
        "verify" => parse_verify(&args[1..]),
        "watch-stdin" => parse_watch_stdin(&args[1..]),
        other => Err(format!(
            "expected subcommand (ship, restore, verify, watch-stdin), saw {other}"
        )),
    }
}

fn next_val<'a>(it: &mut std::slice::Iter<'a, String>, flag: &str) -> Result<&'a str, String> {
    it.next()
        .map(String::as_str)
        .ok_or_else(|| format!("expected value after {flag}, saw none"))
}

fn parse_ship(args: &[String]) -> Result<Option<Command>, String> {
    let mut slices_dir = None;
    let mut dest = None;
    let mut json = false;
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "-h" | "--help" => return Ok(None),
            "--slices-dir" => slices_dir = Some(PathBuf::from(next_val(&mut it, flag)?)),
            "--dest" => dest = Some(PathBuf::from(next_val(&mut it, flag)?)),
            "--json" => json = true,
            other => return Err(format!("expected flag for ship, saw {other}")),
        }
    }
    let slices_dir = slices_dir.ok_or("expected --slices-dir <path>, saw none")?;
    let dest = dest.ok_or("expected --dest <archive-dir>, saw none")?;
    Ok(Some(Command::Ship {
        slices_dir,
        dest,
        json,
    }))
}

fn parse_restore(args: &[String]) -> Result<Option<Command>, String> {
    let mut dest = None;
    let mut slice = None;
    let mut cold_dir = None;
    let mut day = None;
    let mut json = false;
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "-h" | "--help" => return Ok(None),
            "--dest" => dest = Some(PathBuf::from(next_val(&mut it, flag)?)),
            "--slice" => slice = Some(next_val(&mut it, flag)?.to_string()),
            "--cold-dir" => cold_dir = Some(PathBuf::from(next_val(&mut it, flag)?)),
            "--day" => {
                let raw = next_val(&mut it, flag)?;
                let parsed = raw
                    .parse::<i64>()
                    .map_err(|_| format!("expected integer bucket after --day, saw {raw}"))?;
                day = Some(parsed);
            }
            "--json" => json = true,
            other => return Err(format!("expected flag for restore, saw {other}")),
        }
    }
    let dest = dest.ok_or("expected --dest <archive-dir>, saw none")?;
    let slice = slice.ok_or("expected --slice <slice-name>, saw none")?;
    let cold_dir = cold_dir.ok_or("expected --cold-dir <local-cold-dir>, saw none")?;
    Ok(Some(Command::Restore {
        dest,
        slice,
        cold_dir,
        day,
        json,
    }))
}

fn parse_verify(args: &[String]) -> Result<Option<Command>, String> {
    let mut dest = None;
    let mut json = false;
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "-h" | "--help" => return Ok(None),
            "--dest" => dest = Some(PathBuf::from(next_val(&mut it, flag)?)),
            "--json" => json = true,
            other => return Err(format!("expected flag for verify, saw {other}")),
        }
    }
    let dest = dest.ok_or("expected --dest <archive-dir>, saw none")?;
    Ok(Some(Command::Verify { dest, json }))
}

fn parse_watch_stdin(args: &[String]) -> Result<Option<Command>, String> {
    let mut slices_dir = None;
    let mut dest = None;
    let mut json = false;
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "-h" | "--help" => return Ok(None),
            "--slices-dir" => slices_dir = Some(PathBuf::from(next_val(&mut it, flag)?)),
            "--dest" => dest = Some(PathBuf::from(next_val(&mut it, flag)?)),
            "--json" => json = true,
            other => return Err(format!("expected flag for watch-stdin, saw {other}")),
        }
    }
    let slices_dir = slices_dir.ok_or("expected --slices-dir <path>, saw none")?;
    let dest = dest.ok_or("expected --dest <archive-dir>, saw none")?;
    Ok(Some(Command::WatchStdin {
        slices_dir,
        dest,
        json,
    }))
}

fn write_stdout_line(text: &str) -> Result<(), String> {
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    writeln!(out, "{text}").map_err(|e| format!("writing stdout: {e}"))?;
    out.flush().map_err(|e| format!("flushing stdout: {e}"))
}

fn execute(cmd: Command) -> Result<u8, String> {
    match cmd {
        Command::Ship {
            slices_dir,
            dest,
            json,
        } => {
            let summary = ship(&slices_dir, &dest)?;
            let text = format_ship(&summary, json)?;
            write_stdout_line(&text)?;
            Ok(0)
        }
        Command::Restore {
            dest,
            slice,
            cold_dir,
            day,
            json,
        } => {
            let summary = restore(&dest, &slice, &cold_dir, day)?;
            let text = format_restore(&summary, json)?;
            write_stdout_line(&text)?;
            Ok(0)
        }
        Command::Verify { dest, json } => {
            let summary = verify_archive(&dest);
            let text = format_verify(&summary, json)?;
            write_stdout_line(&text)?;
            Ok(if summary.intact { 0 } else { 1 })
        }
        Command::WatchStdin {
            slices_dir,
            dest,
            json,
        } => {
            let stdin = io::stdin();
            let stdout = io::stdout();
            watch_stdin(
                stdin.lock(),
                BufWriter::new(stdout.lock()),
                &slices_dir,
                &dest,
                json,
            )?;
            Ok(0)
        }
    }
}

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let cmd = match parse_args(&argv) {
        Ok(Some(c)) => c,
        Ok(None) => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Err(e) => {
            eprintln!("roger-cold-shipper: {e}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    match execute(cmd) {
        Ok(code) => ExitCode::from(code),
        Err(e) => {
            eprintln!("roger-cold-shipper: {e}");
            ExitCode::from(1)
        }
    }
}

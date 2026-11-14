use roger_mcp_gateway::cli::{parse_args, CliAction, USAGE};
use roger_mcp_gateway::http::run_server;
use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let cfg = match parse_args(&args, &|name| std::env::var(name).ok()) {
        Ok(CliAction::Help) => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Ok(CliAction::Run(cfg)) => cfg,
        Err(e) => {
            eprintln!("roger-mcp-gateway: {e}\n\n{USAGE}");
            return ExitCode::from(2);
        }
    };

    if let Err(e) = run_server(cfg, shutdown_signal()).await {
        eprintln!("roger-mcp-gateway: {e}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        if let Ok(mut sigterm) = signal(SignalKind::terminate()) {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = sigterm.recv() => {}
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

//! TCP listener: each accepted connection is upgraded to a WebSocket and served
//! as one session.

use crate::cli::{write_port_file, Config};
use crate::durlog::blocking_io;
use crate::session::{self, Ctx};
use crate::upgrade::{prepare, refusal_response, Opened};
use std::future::Future;
use std::io::{self, Write as _};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};

struct Shared {
    ctx: Arc<Ctx>,
    store_dir: PathBuf,
    bearer: Option<String>,
}

/// Bind, publish the port, and serve until `shutdown` resolves.
pub async fn run_server(cfg: Config, shutdown: impl Future<Output = ()>) -> io::Result<()> {
    let listener = TcpListener::bind(&cfg.bind)
        .await
        .map_err(|e| io::Error::new(e.kind(), format!("expected to bind {}, saw {e}", cfg.bind)))?;
    let port = listener.local_addr()?.port();
    let store_dir = cfg.store_dir.clone();
    blocking_io(|| std::fs::create_dir_all(&store_dir))?;
    if let Some(pf) = &cfg.port_file {
        blocking_io(|| write_port_file(pf, port))?;
    }
    let shared = Arc::new(Shared {
        ctx: Arc::new(Ctx {
            dialer: cfg.dialer,
            #[cfg(feature = "test-hooks")]
            hold_before_ack: cfg.hold_before_ack,
        }),
        store_dir: cfg.store_dir,
        bearer: cfg.bearer_token,
    });
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => return Ok(()),
            accepted = listener.accept() => {
                let stream = match accepted {
                    Ok((stream, _)) => stream,
                    Err(e) => {
                        let _ = writeln!(io::stderr().lock(), "roger-ws-gateway: accept: {e}");
                        continue;
                    }
                };
                let _ = stream.set_nodelay(true);
                tokio::spawn(handle(Arc::clone(&shared), stream));
            }
        }
    }
}

// The callback's error type is fixed by the WebSocket handshake.
#[allow(clippy::result_large_err)]
async fn handle(shared: Arc<Shared>, stream: TcpStream) {
    let mut slot: Option<Opened> = None;
    let callback = |req: &Request, resp: Response| -> Result<Response, ErrorResponse> {
        let opened =
            prepare(req, shared.bearer.as_deref(), &shared.store_dir).map_err(refusal_response)?;
        slot = Some(opened);
        Ok(resp)
    };
    let Ok(ws) = tokio_tungstenite::accept_hdr_async(stream, callback).await else {
        return;
    };
    let Some(opened) = slot.take() else {
        return;
    };
    session::run(Arc::clone(&shared.ctx), ws, opened).await;
}

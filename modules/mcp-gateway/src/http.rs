//! MCP `2026-07-28` sessionless Streamable HTTP + SEP-3415 SSE push gateway
//! over HTTP/1.1 and HTTP/2 (`h2c`).

use crate::cli::{constant_time_eq, write_port_file, Config};
use crate::durlog::blocking_io;
use crate::mcp::{self, Parsed, RpcErr, ValidatedEventReq};
use crate::session::SessionManager;
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::{Body, Frame, Incoming};
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as AutoConnBuilder;
use serde_json::{Map, Value};
use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
// no-poll: Instant bounds SEP-3415 stream heartbeat and subscriptions/listen keepalive.
use tokio::time::{Duration, Instant};

/// 16 MiB limit on `POST <endpoint>` bodies.
pub const MAX_POST_BODY_BYTES: usize = 16 * 1024 * 1024;
const LISTEN_KEEPALIVE_SECS: u64 = 30;

pub struct GatewayState {
    pub endpoint: String,
    pub bind_loopback: bool,
    pub bearer_token: Option<String>,
    pub sessions: Arc<SessionManager>,
    pub shutdown_rx: watch::Receiver<bool>,
}

pub enum GatewayBody {
    Full(Full<Bytes>),
    Stream(SseStreamBody),
}

impl Body for GatewayBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match self.get_mut() {
            GatewayBody::Full(full) => Pin::new(full).poll_frame(cx),
            GatewayBody::Stream(stream) => Pin::new(stream).poll_frame(cx),
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            GatewayBody::Full(full) => full.is_end_stream(),
            GatewayBody::Stream(_) => false,
        }
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        match self {
            GatewayBody::Full(full) => full.size_hint(),
            GatewayBody::Stream(_) => hyper::body::SizeHint::default(),
        }
    }
}

pub struct SseStreamBody {
    rx: mpsc::Receiver<Bytes>,
}

impl Body for SseStreamBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(chunk)) => Poll::Ready(Some(Ok(Frame::data(chunk)))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

fn is_loopback_bind(bind: &str) -> bool {
    let host = if let Some(rest) = bind.strip_prefix('[') {
        rest.split(']').next().unwrap_or("")
    } else {
        bind.rsplit_once(':').map_or(bind, |(h, _)| h)
    };
    matches!(host, "127.0.0.1" | "::1" | "localhost")
}

/// Bind and serve the gateway until `shutdown` resolves.
pub async fn run_server(cfg: Config, shutdown: impl Future<Output = ()>) -> io::Result<SocketAddr> {
    std::fs::create_dir_all(&cfg.store_dir)?;
    let listener = TcpListener::bind(&cfg.bind)
        .await
        .map_err(|e| io::Error::new(e.kind(), format!("expected to bind {}, saw {e}", cfg.bind)))?;
    let local_addr = listener.local_addr()?;
    if let Some(port_file) = &cfg.port_file {
        write_port_file(port_file, local_addr.port())?;
    }

    let sessions = SessionManager::new(
        cfg.socket,
        cfg.store_dir,
        cfg.groups,
        #[cfg(feature = "test-hooks")]
        cfg.hold_before_ack,
    );
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let state = Arc::new(GatewayState {
        endpoint: cfg.endpoint,
        bind_loopback: is_loopback_bind(&cfg.bind),
        bearer_token: cfg.bearer_token,
        sessions,
        shutdown_rx,
    });

    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => {
                let _ = shutdown_tx.send(true);
                break;
            }
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(pair) => pair,
                    Err(_) => continue,
                };
                let _ = stream.set_nodelay(true);
                let state = Arc::clone(&state);
                tokio::spawn(async move {
                    let svc = service_fn(move |req| {
                        let state = Arc::clone(&state);
                        async move { Ok::<_, Infallible>(handle_request(state, req).await) }
                    });
                    let builder = AutoConnBuilder::new(TokioExecutor::new());
                    let _ = builder.serve_connection(TokioIo::new(stream), svc).await;
                });
            }
        }
    }
    Ok(local_addr)
}

pub async fn handle_request(
    state: Arc<GatewayState>,
    req: Request<Incoming>,
) -> Response<GatewayBody> {
    let path = req.uri().path();
    if path == "/healthz" {
        return if req.method() == Method::GET {
            raw_json_response(StatusCode::OK, r#"{"ok":true}"#)
        } else {
            let mut resp = error_response(
                StatusCode::METHOD_NOT_ALLOWED,
                &format!("expected GET on /healthz, saw {}", req.method()),
            );
            resp.headers_mut()
                .insert(http::header::ALLOW, HeaderValue::from_static("GET"));
            resp
        };
    }

    if path != state.endpoint {
        return error_response(
            StatusCode::NOT_FOUND,
            &format!("expected path {} or /healthz, saw {path}", state.endpoint),
        );
    }

    if let Err(reason) = validate_origin(req.headers(), state.bind_loopback) {
        return error_response(StatusCode::FORBIDDEN, &reason);
    }

    if let Some(expected) = &state.bearer_token {
        if !authorized(req.headers(), expected.as_bytes()) {
            let mut resp = error_response(
                StatusCode::UNAUTHORIZED,
                "expected valid Authorization: Bearer token, saw missing or invalid credentials",
            );
            resp.headers_mut().insert(
                http::header::WWW_AUTHENTICATE,
                HeaderValue::from_static("Bearer"),
            );
            return resp;
        }
    }

    match *req.method() {
        Method::POST => handle_post(&state, req).await,
        Method::GET | Method::DELETE => {
            let mut resp = error_response(
                StatusCode::METHOD_NOT_ALLOWED,
                &format!("expected POST on {}, saw {}", state.endpoint, req.method()),
            );
            resp.headers_mut()
                .insert(http::header::ALLOW, HeaderValue::from_static("POST"));
            resp
        }
        ref other => {
            let mut resp = error_response(
                StatusCode::METHOD_NOT_ALLOWED,
                &format!("expected POST, saw {other}"),
            );
            resp.headers_mut()
                .insert(http::header::ALLOW, HeaderValue::from_static("POST"));
            resp
        }
    }
}

fn validate_origin(hdrs: &HeaderMap, bind_loopback: bool) -> Result<(), String> {
    let Some(raw) = hdrs.get(http::header::ORIGIN) else {
        return Ok(());
    };
    let origin = std::str::from_utf8(raw.as_bytes())
        .map_err(|_| "expected UTF-8 Origin header, saw invalid bytes".to_string())?
        .trim();
    if origin.is_empty() || origin == "null" {
        return Err("expected valid Origin URL, saw empty or null".into());
    }
    let rest = origin
        .strip_prefix("http://")
        .or_else(|| origin.strip_prefix("https://"))
        .ok_or_else(|| format!("expected http:// or https:// Origin, saw {origin}"))?;
    let authority = rest.strip_suffix('/').unwrap_or(rest);
    if authority.is_empty() || authority.contains(['/', '?', '#', '@', ' ']) {
        return Err(format!("expected valid Origin authority, saw {origin}"));
    }
    let host = if let Some(bracketed) = authority.strip_prefix('[') {
        let Some((h, port_part)) = bracketed.split_once(']') else {
            return Err(format!("expected valid IPv6 Origin host, saw {origin}"));
        };
        if !port_part.is_empty()
            && !port_part
                .strip_prefix(':')
                .is_some_and(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
        {
            return Err(format!("expected valid Origin port, saw {origin}"));
        }
        h
    } else if let Some((h, p)) = authority.rsplit_once(':') {
        if h.is_empty() || p.is_empty() || !p.bytes().all(|b| b.is_ascii_digit()) {
            return Err(format!("expected valid Origin host:port, saw {origin}"));
        }
        h
    } else {
        authority
    };
    if bind_loopback && !matches!(host, "localhost" | "127.0.0.1" | "::1") {
        return Err(format!(
            "expected loopback Origin for local gateway, saw {origin}"
        ));
    }
    Ok(())
}

fn authorized(hdrs: &HeaderMap, expected_token: &[u8]) -> bool {
    let Some(raw) = hdrs.get(http::header::AUTHORIZATION) else {
        return false;
    };
    let bytes = raw.as_bytes();
    if bytes.len() < 7 || !bytes[..7].eq_ignore_ascii_case(b"Bearer ") {
        return false;
    }
    constant_time_eq(&bytes[7..], expected_token)
}

fn header_utf8(hdrs: &HeaderMap, name: &str) -> Result<Option<String>, String> {
    let Some(raw) = hdrs.get(name) else {
        return Ok(None);
    };
    let s = std::str::from_utf8(raw.as_bytes())
        .map_err(|_| format!("expected UTF-8 in {name}, saw invalid bytes"))?;
    Ok((!s.is_empty()).then(|| s.to_string()))
}

pub fn decode_mcp_name_header(raw: &str) -> Result<String, String> {
    if let Some(inner) = raw
        .strip_prefix("=?base64?")
        .and_then(|s| s.strip_suffix("?="))
    {
        let bytes = decode_base64(inner)?;
        return String::from_utf8(bytes)
            .map_err(|_| "expected UTF-8 inside base64 Mcp-Name header".to_string());
    }
    Ok(raw.to_string())
}

fn decode_base64(input: &str) -> Result<Vec<u8>, String> {
    let bytes = input.as_bytes();
    if !bytes.len().is_multiple_of(4) {
        return Err("expected padded base64 in Mcp-Name sentinel".into());
    }
    let val = |b: u8| -> Result<u8, String> {
        match b {
            b'A'..=b'Z' => Ok(b - b'A'),
            b'a'..=b'z' => Ok(b - b'a' + 26),
            b'0'..=b'9' => Ok(b - b'0' + 52),
            b'+' => Ok(62),
            b'/' => Ok(63),
            _ => Err(format!("invalid base64 byte {b:#x}")),
        }
    };
    let mut out = Vec::with_capacity((bytes.len() / 4) * 3);
    for chunk in bytes.as_chunks::<4>().0 {
        let pad = chunk.iter().rev().take_while(|&&b| b == b'=').count();
        if pad > 2 || (pad > 0 && chunk[2] == b'=' && chunk[3] != b'=') {
            return Err("invalid base64 padding".into());
        }
        let a = val(chunk[0])?;
        let b = val(chunk[1])?;
        let c = if chunk[2] == b'=' { 0 } else { val(chunk[2])? };
        let d = if chunk[3] == b'=' { 0 } else { val(chunk[3])? };
        out.push((a << 2) | (b >> 4));
        if chunk[2] != b'=' {
            out.push((b << 4) | (c >> 2));
        }
        if chunk[3] != b'=' {
            out.push((c << 6) | d);
        }
    }
    Ok(out)
}

fn accept_allows_media(hdrs: &HeaderMap, target: &str) -> bool {
    let mut saw_accept = false;
    let target_prefix = target.split('/').next().unwrap_or("");
    for val in hdrs.get_all(http::header::ACCEPT) {
        saw_accept = true;
        let Ok(text) = val.to_str() else {
            continue;
        };
        for item in text.split(',') {
            let mut parts = item.split(';');
            let media = parts.next().unwrap_or("").trim();
            let mut q_zero = false;
            for param in parts {
                if let Some((k, v)) = param.trim().split_once('=') {
                    if k.trim().eq_ignore_ascii_case("q") {
                        if let Ok(q) = v.trim().parse::<f32>() {
                            if q <= 0.0 {
                                q_zero = true;
                            }
                        }
                    }
                }
            }
            if q_zero {
                continue;
            }
            if media.eq_ignore_ascii_case(target)
                || media == "*/*"
                || media
                    .strip_suffix("/*")
                    .is_some_and(|p| p.eq_ignore_ascii_case(target_prefix))
            {
                return true;
            }
        }
    }
    !saw_accept
}

fn validate_request_headers(
    hdrs: &HeaderMap,
    method: &str,
    params: &Value,
) -> Result<String, RpcErr> {
    let hdr_pv = match header_utf8(hdrs, "mcp-protocol-version") {
        Ok(Some(v)) => v,
        Ok(None) => {
            return Err(RpcErr::header_mismatch(
                "expected MCP-Protocol-Version header, saw none",
            ))
        }
        Err(e) => return Err(RpcErr::header_mismatch(e)),
    };
    let hdr_method = match header_utf8(hdrs, "mcp-method") {
        Ok(Some(v)) => v,
        Ok(None) => {
            return Err(RpcErr::header_mismatch(
                "expected Mcp-Method header, saw none",
            ))
        }
        Err(e) => return Err(RpcErr::header_mismatch(e)),
    };
    if hdr_method != method {
        return Err(RpcErr::header_mismatch(format!(
            "Header mismatch: Mcp-Method header value '{hdr_method}' does not match body method '{method}'"
        )));
    }
    if matches!(
        method,
        "tools/call" | "events/poll" | "events/stream" | "events/subscribe" | "events/unsubscribe"
    ) {
        let raw_name = match header_utf8(hdrs, "mcp-name") {
            Ok(Some(v)) => v,
            Ok(None) => {
                return Err(RpcErr::header_mismatch(
                    "expected Mcp-Name header, saw none",
                ))
            }
            Err(e) => return Err(RpcErr::header_mismatch(e)),
        };
        let decoded = decode_mcp_name_header(&raw_name).map_err(RpcErr::header_mismatch)?;
        let body_name = params.get("name").and_then(Value::as_str).unwrap_or("");
        if body_name.is_empty() || decoded != body_name {
            return Err(RpcErr::header_mismatch(format!(
                "Header mismatch: Mcp-Name header value '{decoded}' does not match body params.name '{body_name}'"
            )));
        }
    }
    Ok(hdr_pv)
}

async fn handle_post(state: &GatewayState, req: Request<Incoming>) -> Response<GatewayBody> {
    if let Some(cl) = req.headers().get(http::header::CONTENT_LENGTH) {
        if let Ok(s) = cl.to_str() {
            if let Ok(len) = s.trim().parse::<u64>() {
                if len > MAX_POST_BODY_BYTES as u64 {
                    return error_response(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        &format!("expected body of at most {MAX_POST_BODY_BYTES} bytes, saw {len}"),
                    );
                }
            }
        }
    }

    let (parts, mut body) = req.into_parts();
    let mut buf = Vec::new();
    while let Some(frame_res) = body.frame().await {
        let frame = match frame_res {
            Ok(f) => f,
            Err(e) => {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    &format!("expected readable HTTP request body, saw {e}"),
                )
            }
        };
        if let Ok(chunk) = frame.into_data() {
            if buf.len().saturating_add(chunk.len()) > MAX_POST_BODY_BYTES {
                return error_response(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    &format!(
                        "expected body of at most {MAX_POST_BODY_BYTES} bytes, saw a larger stream"
                    ),
                );
            }
            buf.extend_from_slice(&chunk);
        }
    }

    let text = match std::str::from_utf8(&buf) {
        Ok(t) => t.trim(),
        Err(_) => {
            return json_value_response(
                StatusCode::BAD_REQUEST,
                &mcp::reply_err(
                    &Value::Null,
                    -32700,
                    "expected UTF-8 JSON body, saw non-UTF-8 bytes",
                ),
            );
        }
    };

    let (id, method, params) = match mcp::parse_message(text) {
        Parsed::Notification => return empty_response(StatusCode::ACCEPTED),
        Parsed::Ignore => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "expected a JSON-RPC request or notification, saw a response",
            )
        }
        Parsed::Bad { id, code, message } => {
            return json_value_response(
                StatusCode::BAD_REQUEST,
                &mcp::reply_err(&id, code, &message),
            );
        }
        Parsed::Request { id, method, params } => (id, method, params),
    };

    let hdr_pv = match validate_request_headers(&parts.headers, &method, &params) {
        Ok(v) => v,
        Err(err) => return rpc_err_response(StatusCode::BAD_REQUEST, &id, err),
    };

    // Validate _meta and confirm MCP-Protocol-Version matches _meta["io.modelcontextprotocol/protocolVersion"].
    let body_pv = params
        .get("_meta")
        .and_then(Value::as_object)
        .and_then(|m| m.get("io.modelcontextprotocol/protocolVersion"))
        .and_then(Value::as_str);
    if let Some(pv) = body_pv {
        if hdr_pv != pv {
            return rpc_err_response(
                StatusCode::BAD_REQUEST,
                &id,
                RpcErr::header_mismatch(format!(
                    "Header mismatch: MCP-Protocol-Version '{hdr_pv}' does not match _meta protocolVersion '{pv}'"
                )),
            );
        }
    }
    if let Err(err) = mcp::validate_meta(&params) {
        return rpc_err_response(StatusCode::BAD_REQUEST, &id, err);
    }

    let is_stream = matches!(method.as_str(), "events/stream" | "subscriptions/listen");
    if is_stream {
        if !accept_allows_media(&parts.headers, "text/event-stream") {
            return error_response(
                StatusCode::NOT_ACCEPTABLE,
                "expected Accept header to include text/event-stream or */*, saw neither",
            );
        }
    } else if !accept_allows_media(&parts.headers, "application/json") {
        return error_response(
            StatusCode::NOT_ACCEPTABLE,
            "expected Accept header to include application/json or */*, saw neither",
        );
    }

    match method.as_str() {
        "server/discover" => json_value_response(
            StatusCode::OK,
            &mcp::reply_ok(&id, mcp::discover_result()),
        ),
        "tools/list" => json_value_response(
            StatusCode::OK,
            &mcp::reply_ok(&id, mcp::tools_list(state.sessions.groups())),
        ),
        "tools/call" => {
            let result = match mcp::call_tool(&state.sessions, &params).await {
                Ok(v) => {
                    let rendered =
                        serde_json::to_string(&v).expect("tool output serializes as JSON");
                    mcp::tool_result(&rendered, false)
                }
                Err(reason) => mcp::tool_result(&reason, true),
            };
            json_value_response(StatusCode::OK, &mcp::reply_ok(&id, result))
        }
        "events/list" => json_value_response(
            StatusCode::OK,
            &mcp::reply_ok(&id, mcp::events_list_result()),
        ),
        "events/poll" => match mcp::events_poll(&state.sessions, &params).await {
            Ok(v) => json_value_response(StatusCode::OK, &mcp::reply_ok(&id, v)),
            Err(err) => {
                let status = if err.code == -32602 {
                    StatusCode::BAD_REQUEST
                } else {
                    StatusCode::OK
                };
                rpc_err_response(status, &id, err)
            }
        },
        "events/stream" => {
            let req = match mcp::validate_event_req(&state.sessions, &params, false) {
                Ok(r) => r,
                Err(err) => {
                    let status = if err.code == -32602 {
                        StatusCode::BAD_REQUEST
                    } else {
                        StatusCode::OK
                    };
                    return rpc_err_response(status, &id, err);
                }
            };
            if req.name == "roger.board" {
                if let Err(e) = req.session.ensure_watch().await {
                    return rpc_err_response(
                        StatusCode::BAD_GATEWAY,
                        &id,
                        RpcErr {
                            code: -32603,
                            message: e.to_string(),
                            data: None,
                        },
                    );
                }
            }
            sse_response(spawn_events_stream(req, id, state.shutdown_rx.clone()))
        }
        "subscriptions/listen" => {
            if let Some(n) = params.get("notifications") {
                if !n.is_object() {
                    return rpc_err_response(
                        StatusCode::BAD_REQUEST,
                        &id,
                        RpcErr::invalid_params("expected notifications to be an object"),
                    );
                }
            }
            sse_response(spawn_subscriptions_listen(id, state.shutdown_rx.clone()))
        }
        unknown => rpc_err_response(
            StatusCode::NOT_FOUND,
            &id,
            RpcErr {
                code: -32601,
                message: format!(
                    "expected server/discover, tools/list, tools/call, subscriptions/listen, or events/*, saw {unknown}"
                ),
                data: None,
            },
        ),
    }
}

fn sse_frame_data(json_line: &str) -> Bytes {
    Bytes::from(format!("data: {json_line}\n\n"))
}

fn stream_complete_frame(id: &Value) -> Bytes {
    let payload = mcp::reply_ok(
        id,
        serde_json::json!({
            "resultType": "complete",
            "_meta": {
                "io.modelcontextprotocol/subscriptionId": id,
                "io.modelcontextprotocol/serverInfo": {
                    "name": "roger-mcp-gateway",
                    "version": env!("CARGO_PKG_VERSION"),
                }
            }
        }),
    );
    sse_frame_data(&serde_json::to_string(&payload).expect("JSON serializes"))
}

fn stream_terminated_frame(id: &Value, reason: &str) -> Bytes {
    let mut params = Map::new();
    params.insert(
        "error".into(),
        serde_json::json!({
            "code": -32023,
            "message": "NotFound",
            "data": { "kind": "session", "reason": reason },
        }),
    );
    sse_frame_data(&mcp::format_notification(
        "notifications/events/terminated",
        params,
        id,
    ))
}

fn spawn_subscriptions_listen(id: Value, mut shutdown_rx: watch::Receiver<bool>) -> SseStreamBody {
    let (tx, rx) = mpsc::channel::<Bytes>(16);
    tokio::spawn(async move {
        let mut ack_params = Map::new();
        ack_params.insert("notifications".into(), Value::Object(Map::new()));
        let ack =
            mcp::format_notification("notifications/subscriptions/acknowledged", ack_params, &id);
        if tx.send(sse_frame_data(&ack)).await.is_err() {
            return;
        }
        loop {
            tokio::select! {
                _ = tx.closed() => break,
                res = shutdown_rx.changed() => {
                    if res.is_err() || *shutdown_rx.borrow() {
                        let _ = tx.send(stream_complete_frame(&id)).await;
                        break;
                    }
                }
                // no-poll: periodic SSE comment keepalive on quiet subscriptions/listen stream.
                _ = tokio::time::sleep(Duration::from_secs(LISTEN_KEEPALIVE_SECS)) => {
                    if tx.send(Bytes::from_static(b":\n\n")).await.is_err() {
                        break;
                    }
                }
            }
        }
    });
    SseStreamBody { rx }
}

fn spawn_events_stream(
    req: ValidatedEventReq,
    id: Value,
    shutdown_rx: watch::Receiver<bool>,
) -> SseStreamBody {
    let (tx, rx) = mpsc::channel::<Bytes>(64);
    tokio::spawn(async move {
        match req.name.as_str() {
            "roger.mail" => run_mail_sse(req, id, tx, shutdown_rx).await,
            "roger.board" => run_board_sse(req, id, tx, shutdown_rx).await,
            _ => run_reg_sse(req, id, tx, shutdown_rx).await,
        }
    });
    SseStreamBody { rx }
}

async fn run_mail_sse(
    req: ValidatedEventReq,
    id: Value,
    tx: mpsc::Sender<Bytes>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let session = req.session;
    let mut seen_seq = session.mail_seq.load(Ordering::SeqCst);
    let cache_arc = Arc::clone(&session.store_cache);
    let cursor_opt = req.cursor.clone();
    let max_age_ms = req.max_age_ms;
    let init = blocking_io(move || -> io::Result<_> {
        let mut cache = cache_arc.lock().unwrap();
        cache.refresh()?;
        let now = crate::session::now_ms();
        let (start, truncated, start_cursor) = mcp::resolve_mail_window_indexed(
            &cache.entries,
            &cache.record_index,
            cursor_opt.as_deref(),
            max_age_ms,
            now,
        );
        let latest_cursor = if start >= cache.entries.len() {
            start_cursor.clone()
        } else {
            mcp::tail_cursor(&cache.entries)
        };
        let initial = cache.entries[start..].to_vec();
        Ok((
            start_cursor,
            truncated,
            latest_cursor,
            initial,
            cache.generation,
            cache.entries.len(),
        ))
    });
    let (start_cursor, truncated, mut latest_cursor, initial_entries, mut seen_gen, mut next_idx) =
        match init {
            Ok(v) => v,
            Err(e) => {
                let _ = tx.send(stream_terminated_frame(&id, &e.to_string())).await;
                return;
            }
        };

    let mut active_params = Map::new();
    active_params.insert("cursor".into(), Value::String(start_cursor));
    active_params.insert("truncated".into(), Value::Bool(truncated));
    active_params.insert("heartbeatMs".into(), Value::Number(req.heartbeat_ms.into()));
    let active = mcp::format_notification("notifications/events/active", active_params, &id);
    if tx.send(sse_frame_data(&active)).await.is_err() {
        return;
    }

    for entry in &initial_entries {
        let rec = mcp::record_of(entry).to_string();
        if rec.is_empty() {
            continue;
        }
        latest_cursor = rec;
        if req.filter.matches(entry) {
            let occ = match mcp::mail_occurrence(entry, true) {
                Value::Object(m) => m,
                _ => Map::new(),
            };
            let ev = mcp::format_notification("notifications/events/event", occ, &id);
            if tx.send(sse_frame_data(&ev)).await.is_err() {
                return;
            }
        }
    }

    let hb_dur = Duration::from_millis(req.heartbeat_ms as u64);
    // no-poll: SEP-3415 stream heartbeat bound when quiet.
    let mut next_hb = Instant::now() + hb_dur;

    loop {
        let notified = session.mail_notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        if session.closed.load(Ordering::SeqCst) {
            let _ = tx
                .send(stream_terminated_frame(&id, "session closed"))
                .await;
            return;
        }
        let cur_seq = session.mail_seq.load(Ordering::SeqCst);
        if cur_seq != seen_seq {
            seen_seq = cur_seq;
            let cache_arc = Arc::clone(&session.store_cache);
            let delta = blocking_io(move || -> io::Result<_> {
                let mut cache = cache_arc.lock().unwrap();
                cache.refresh()?;
                if cache.generation != seen_gen {
                    Ok((cache.generation, cache.entries.len(), cache.entries.clone()))
                } else if next_idx < cache.entries.len() {
                    Ok((
                        seen_gen,
                        cache.entries.len(),
                        cache.entries[next_idx..].to_vec(),
                    ))
                } else {
                    Ok((seen_gen, next_idx, Vec::new()))
                }
            });
            if let Ok((new_gen, new_next, now_all)) = delta {
                seen_gen = new_gen;
                next_idx = new_next;
                let mut sent_any = false;
                for entry in now_all {
                    let rec = mcp::record_of(&entry).to_string();
                    if rec.is_empty() {
                        continue;
                    }
                    latest_cursor = rec;
                    if req.filter.matches(&entry) {
                        let occ = match mcp::mail_occurrence(&entry, true) {
                            Value::Object(m) => m,
                            _ => Map::new(),
                        };
                        let ev = mcp::format_notification("notifications/events/event", occ, &id);
                        if tx.send(sse_frame_data(&ev)).await.is_err() {
                            return;
                        }
                        sent_any = true;
                    }
                }
                if sent_any {
                    // no-poll: reset heartbeat bound after sending an event.
                    next_hb = Instant::now() + hb_dur;
                }
            }
            continue;
        }

        tokio::select! {
            _ = tx.closed() => return,
            res = shutdown_rx.changed() => {
                if res.is_err() || *shutdown_rx.borrow() {
                    let _ = tx.send(stream_complete_frame(&id)).await;
                    return;
                }
            }
            _ = &mut notified => {}
            // no-poll: SEP-3415 stream heartbeat bound.
            _ = tokio::time::sleep_until(next_hb) => {
                let mut hb = Map::new();
                hb.insert("cursor".into(), Value::String(latest_cursor.clone()));
                let msg = mcp::format_notification("notifications/events/heartbeat", hb, &id);
                if tx.send(sse_frame_data(&msg)).await.is_err() {
                    return;
                }
                // no-poll: schedule next SEP-3415 heartbeat.
                next_hb = Instant::now() + hb_dur;
            }
        }
    }
}

async fn run_board_sse(
    req: ValidatedEventReq,
    id: Value,
    tx: mpsc::Sender<Bytes>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let session = req.session;
    let mut seq_cursor = session.board_feed.lock().unwrap().next_seq;
    let mut evt_counter = 0u64;

    let mut active_params = Map::new();
    active_params.insert("cursor".into(), Value::Null);
    active_params.insert("truncated".into(), Value::Bool(false));
    active_params.insert("heartbeatMs".into(), Value::Number(req.heartbeat_ms.into()));
    let active = mcp::format_notification("notifications/events/active", active_params, &id);
    if tx.send(sse_frame_data(&active)).await.is_err() {
        return;
    }

    let hb_dur = Duration::from_millis(req.heartbeat_ms as u64);
    // no-poll: SEP-3415 stream heartbeat bound.
    let mut next_hb = Instant::now() + hb_dur;

    loop {
        let notified = session.board_notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        if session.closed.load(Ordering::SeqCst) {
            let _ = tx
                .send(stream_terminated_frame(&id, "session closed"))
                .await;
            return;
        }

        let batch: Vec<(u64, i64, Value)> = {
            let feed = session.board_feed.lock().unwrap();
            feed.items
                .iter()
                .filter(|(s, _)| *s > seq_cursor)
                .map(|(s, n)| (*s, n.at_ms, n.data.clone()))
                .collect()
        };
        if !batch.is_empty() {
            for (seq, at_ms, data) in batch {
                seq_cursor = seq;
                evt_counter += 1;
                let mut params = Map::new();
                params.insert(
                    "eventId".into(),
                    Value::String(format!("board-{seq}-{evt_counter}")),
                );
                params.insert("name".into(), Value::String("roger.board".into()));
                params.insert(
                    "timestamp".into(),
                    Value::String(mcp::format_iso8601_utc(at_ms)),
                );
                params.insert("data".into(), data);
                params.insert("cursor".into(), Value::Null);
                let msg = mcp::format_notification("notifications/events/event", params, &id);
                if tx.send(sse_frame_data(&msg)).await.is_err() {
                    return;
                }
            }
            // no-poll: reset heartbeat bound after sending an event.
            next_hb = Instant::now() + hb_dur;
            continue;
        }

        tokio::select! {
            _ = tx.closed() => return,
            res = shutdown_rx.changed() => {
                if res.is_err() || *shutdown_rx.borrow() {
                    let _ = tx.send(stream_complete_frame(&id)).await;
                    return;
                }
            }
            _ = &mut notified => {}
            // no-poll: SEP-3415 stream heartbeat bound.
            _ = tokio::time::sleep_until(next_hb) => {
                let mut hb = Map::new();
                hb.insert("cursor".into(), Value::Null);
                let msg = mcp::format_notification("notifications/events/heartbeat", hb, &id);
                if tx.send(sse_frame_data(&msg)).await.is_err() {
                    return;
                }
                // no-poll: schedule next SEP-3415 heartbeat.
                next_hb = Instant::now() + hb_dur;
            }
        }
    }
}

async fn run_reg_sse(
    req: ValidatedEventReq,
    id: Value,
    tx: mpsc::Sender<Bytes>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let session = req.session;
    let mut seq_cursor = 0u64;

    let mut active_params = Map::new();
    active_params.insert("cursor".into(), Value::Null);
    active_params.insert("truncated".into(), Value::Bool(false));
    active_params.insert("heartbeatMs".into(), Value::Number(req.heartbeat_ms.into()));
    let active = mcp::format_notification("notifications/events/active", active_params, &id);
    if tx.send(sse_frame_data(&active)).await.is_err() {
        return;
    }

    let hb_dur = Duration::from_millis(req.heartbeat_ms as u64);
    // no-poll: SEP-3415 stream heartbeat bound.
    let mut next_hb = Instant::now() + hb_dur;

    loop {
        let notified = session.reg_notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        if session.closed.load(Ordering::SeqCst) {
            let _ = tx
                .send(stream_terminated_frame(&id, "session closed"))
                .await;
            return;
        }

        let batch: Vec<(u64, i64, Value)> = {
            let feed = session.reg_feed.lock().unwrap();
            feed.items
                .iter()
                .filter(|(s, _)| *s > seq_cursor)
                .map(|(s, n)| (*s, n.at_ms, n.data.clone()))
                .collect()
        };
        if !batch.is_empty() {
            for (seq, at_ms, data) in batch {
                seq_cursor = seq;
                let mut params = Map::new();
                params.insert("eventId".into(), Value::String(format!("reg-{seq}")));
                params.insert("name".into(), Value::String("roger.registration".into()));
                params.insert(
                    "timestamp".into(),
                    Value::String(mcp::format_iso8601_utc(at_ms)),
                );
                params.insert("data".into(), data);
                params.insert("cursor".into(), Value::Null);
                let msg = mcp::format_notification("notifications/events/event", params, &id);
                if tx.send(sse_frame_data(&msg)).await.is_err() {
                    return;
                }
            }
            // no-poll: reset heartbeat bound after sending an event.
            next_hb = Instant::now() + hb_dur;
            continue;
        }

        tokio::select! {
            _ = tx.closed() => return,
            res = shutdown_rx.changed() => {
                if res.is_err() || *shutdown_rx.borrow() {
                    let _ = tx.send(stream_complete_frame(&id)).await;
                    return;
                }
            }
            _ = &mut notified => {}
            // no-poll: SEP-3415 stream heartbeat bound.
            _ = tokio::time::sleep_until(next_hb) => {
                let mut hb = Map::new();
                hb.insert("cursor".into(), Value::Null);
                let msg = mcp::format_notification("notifications/events/heartbeat", hb, &id);
                if tx.send(sse_frame_data(&msg)).await.is_err() {
                    return;
                }
                // no-poll: schedule next SEP-3415 heartbeat.
                next_hb = Instant::now() + hb_dur;
            }
        }
    }
}

fn sse_response(body: SseStreamBody) -> Response<GatewayBody> {
    Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "text/event-stream")
        .header(http::header::CACHE_CONTROL, "no-cache")
        .header("x-accel-buffering", "no")
        .body(GatewayBody::Stream(body))
        .expect("valid SSE response")
}

fn rpc_err_response(status: StatusCode, id: &Value, err: RpcErr) -> Response<GatewayBody> {
    json_value_response(
        status,
        &mcp::reply_err_data(id, err.code, &err.message, err.data),
    )
}

fn json_value_response(status: StatusCode, value: &Value) -> Response<GatewayBody> {
    let body = serde_json::to_vec(value).expect("JSON response serializes");
    Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(GatewayBody::Full(Full::new(Bytes::from(body))))
        .expect("valid JSON response")
}

fn raw_json_response(status: StatusCode, raw_json: &'static str) -> Response<GatewayBody> {
    Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(GatewayBody::Full(Full::new(Bytes::from_static(
            raw_json.as_bytes(),
        ))))
        .expect("valid static JSON response")
}

fn empty_response(status: StatusCode) -> Response<GatewayBody> {
    Response::builder()
        .status(status)
        .body(GatewayBody::Full(Full::new(Bytes::new())))
        .expect("valid empty response")
}

fn error_response(status: StatusCode, message: &str) -> Response<GatewayBody> {
    let mut m = Map::new();
    m.insert("error".into(), Value::String(message.to_string()));
    json_value_response(status, &Value::Object(m))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_mcp_name_decoding_matches_spec_examples() {
        assert_eq!(decode_mcp_name_header("roger_send").unwrap(), "roger_send");
        assert_eq!(
            decode_mcp_name_header("=?base64?SGVsbG8sIOS4lueVjA==?=").unwrap(),
            "Hello, 世界"
        );
        assert_eq!(
            decode_mcp_name_header("=?base64?PT9iYXNlNjQ/bGl0ZXJhbD89?=").unwrap(),
            "=?base64?literal?="
        );
        assert!(decode_mcp_name_header("=?base64?not_valid!?=").is_err());
    }

    #[test]
    fn origin_validation_accepts_loopback_and_rejects_malformed_or_remote() {
        let mut h = HeaderMap::new();
        assert!(validate_origin(&h, true).is_ok());

        h.insert(
            http::header::ORIGIN,
            HeaderValue::from_static("http://127.0.0.1:8080"),
        );
        assert!(validate_origin(&h, true).is_ok());

        h.insert(
            http::header::ORIGIN,
            HeaderValue::from_static("http://localhost:3000"),
        );
        assert!(validate_origin(&h, true).is_ok());

        h.insert(
            http::header::ORIGIN,
            HeaderValue::from_static("https://[::1]:8443"),
        );
        assert!(validate_origin(&h, true).is_ok());

        h.insert(
            http::header::ORIGIN,
            HeaderValue::from_static("https://evil.example"),
        );
        assert!(validate_origin(&h, true).is_err());
        assert!(validate_origin(&h, false).is_ok());

        h.insert(http::header::ORIGIN, HeaderValue::from_static("null"));
        assert!(validate_origin(&h, true).is_err());
    }

    #[test]
    fn accept_header_negotiation_handles_json_and_sse() {
        let mut h = HeaderMap::new();
        assert!(accept_allows_media(&h, "text/event-stream"));
        assert!(accept_allows_media(&h, "application/json"));

        h.insert(
            http::header::ACCEPT,
            HeaderValue::from_static("application/json"),
        );
        assert!(accept_allows_media(&h, "application/json"));
        assert!(!accept_allows_media(&h, "text/event-stream"));

        h.insert(
            http::header::ACCEPT,
            HeaderValue::from_static("application/json, text/event-stream;q=0.9"),
        );
        assert!(accept_allows_media(&h, "application/json"));
        assert!(accept_allows_media(&h, "text/event-stream"));

        h.insert(http::header::ACCEPT, HeaderValue::from_static("text/plain"));
        assert!(!accept_allows_media(&h, "application/json"));
        assert!(!accept_allows_media(&h, "text/event-stream"));
    }
}

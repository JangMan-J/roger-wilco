//! MCP `2026-07-28` sessionless Streamable HTTP + SEP-3415 SSE push gateway
//! over HTTP/1.1 and HTTP/2 (`h2c`).

use crate::cli::{constant_time_eq, write_port_file, Config};
use crate::durlog::{blocking_io, gen_uuid};
use crate::mcp::{self, Parsed, RpcErr, ValidatedEventReq};
use crate::mrtr;
use crate::session::SessionManager;
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::{Body, Frame, Incoming};
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as AutoConnBuilder;
use roger_wire::sha256::hex_digest;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch, Notify};
// no-poll: Instant bounds SEP-3415 stream heartbeat and subscriptions/listen keepalive.
use tokio::time::{Duration, Instant};

/// 16 MiB limit on `POST <endpoint>` bodies.
pub const MAX_POST_BODY_BYTES: usize = 16 * 1024 * 1024;
const LISTEN_KEEPALIVE_SECS: u64 = 30;

#[derive(Clone)]
pub struct GatewayTask {
    pub task_id: String,
    pub status: String,
    pub created_at: String,
    pub last_updated_at: String,
    pub result: Option<Value>,
    pub cancelled: Arc<AtomicBool>,
    pub cancel_notify: Arc<Notify>,
}

impl GatewayTask {
    pub fn to_value(&self, result_type: &str) -> Value {
        let mut m = Map::new();
        m.insert("resultType".into(), Value::String(result_type.into()));
        m.insert("taskId".into(), Value::String(self.task_id.clone()));
        m.insert("status".into(), Value::String(self.status.clone()));
        m.insert("createdAt".into(), Value::String(self.created_at.clone()));
        m.insert(
            "lastUpdatedAt".into(),
            Value::String(self.last_updated_at.clone()),
        );
        m.insert("ttlMs".into(), Value::Number(mcp::DISCOVER_TTL_MS.into()));
        // no-poll: SEP-2663 Task wire field name, not a periodic scan.
        m.insert("pollIntervalMs".into(), Value::Number(1_000.into()));
        if let Some(res) = &self.result {
            m.insert("result".into(), res.clone());
        }
        m.insert("_meta".into(), mcp::server_info_meta());
        Value::Object(m)
    }

    pub fn to_notif_params(&self) -> Map<String, Value> {
        let mut m = Map::new();
        m.insert("taskId".into(), Value::String(self.task_id.clone()));
        m.insert("status".into(), Value::String(self.status.clone()));
        m.insert("createdAt".into(), Value::String(self.created_at.clone()));
        m.insert(
            "lastUpdatedAt".into(),
            Value::String(self.last_updated_at.clone()),
        );
        m.insert("ttlMs".into(), Value::Number(mcp::DISCOVER_TTL_MS.into()));
        // no-poll: SEP-2663 Task wire field name, not a periodic scan.
        m.insert("pollIntervalMs".into(), Value::Number(1_000.into()));
        if let Some(res) = &self.result {
            m.insert("result".into(), res.clone());
        }
        m
    }
}

pub struct TaskStreamSink {
    pub sub_id: Value,
    pub task_ids: Vec<String>,
    pub tx: mpsc::Sender<Bytes>,
}

pub struct GatewayState {
    pub endpoint: String,
    pub bind_addr: SocketAddr,
    pub bind_loopback: bool,
    pub bearer_token: Option<String>,
    pub public_url: Option<String>,
    pub auth_servers: Vec<String>,
    pub scopes: Vec<String>,
    pub mrtr_secret: Vec<u8>,
    pub tasks: Mutex<HashMap<String, GatewayTask>>,
    pub task_sinks: Mutex<Vec<TaskStreamSink>>,
    pub sessions: Arc<SessionManager>,
    pub shutdown_rx: watch::Receiver<bool>,
}

impl GatewayState {
    pub fn emit_task(&self, task: &GatewayTask) {
        let targets: Vec<(Value, mpsc::Sender<Bytes>)> = {
            let mut sinks = self.task_sinks.lock().unwrap();
            sinks.retain(|s| !s.tx.is_closed());
            sinks
                .iter()
                .filter(|s| s.task_ids.iter().any(|id| id == &task.task_id))
                .map(|s| (s.sub_id.clone(), s.tx.clone()))
                .collect()
        };
        for (sub_id, tx) in targets {
            let frame = sse_frame_data(&mcp::format_notification(
                "notifications/tasks",
                task.to_notif_params(),
                &sub_id,
            ));
            let _ = tx.try_send(frame);
        }
    }
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
    let mrtr_seed = gen_uuid().unwrap_or_else(|_| "roger-mrtr-default-seed".to_string());
    let state = Arc::new(GatewayState {
        endpoint: cfg.endpoint,
        bind_addr: local_addr,
        bind_loopback: is_loopback_bind(&cfg.bind),
        bearer_token: cfg.bearer_token,
        public_url: cfg.public_url,
        auth_servers: cfg.auth_servers,
        scopes: cfg.scopes,
        mrtr_secret: mrtr_seed.into_bytes(),
        tasks: Mutex::new(HashMap::new()),
        task_sinks: Mutex::new(Vec::new()),
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

fn clean_endpoint(endpoint: &str) -> &str {
    let trimmed = endpoint.trim_end_matches('/');
    if trimmed.is_empty() {
        ""
    } else {
        trimmed
    }
}

fn resolve_public_endpoint_url(state: &GatewayState, hdrs: &HeaderMap) -> String {
    if let Some(pub_url) = &state.public_url {
        return pub_url.clone();
    }
    let host = hdrs
        .get(http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .filter(|h| !h.is_empty() && !h.contains(['/', ' ', '\\', '@']))
        .map(str::to_string)
        .unwrap_or_else(|| state.bind_addr.to_string());
    format!("http://{host}{}", state.endpoint)
}

fn origin_of_url(url: &str) -> &str {
    if let Some(rest) = url.strip_prefix("https://") {
        let end = rest.find('/').map_or(url.len(), |i| 8 + i);
        &url[..end]
    } else if let Some(rest) = url.strip_prefix("http://") {
        let end = rest.find('/').map_or(url.len(), |i| 7 + i);
        &url[..end]
    } else {
        url
    }
}

fn build_server_card(mcp_url: &str) -> Value {
    serde_json::json!({
        "$schema": "https://static.modelcontextprotocol.io/schemas/v1/server-card.schema.json",
        "name": "io.github.jangman-j/roger-mcp-gateway",
        "version": env!("CARGO_PKG_VERSION"),
        "title": "Roger-Wilco MCP Gateway",
        "description": "Durable mail, presence, groups, and exclusive claims over MCP 2026-07-28 and SEP-3415 events.",
        "remotes": [
            {
                "type": "streamable-http",
                "url": mcp_url,
                "supportedProtocolVersions": [mcp::VERSION],
                "supportedVersions": [mcp::VERSION]
            }
        ]
    })
}

fn discovery_json_response(
    req_hdrs: &HeaderMap,
    content_type: &'static str,
    payload: &Value,
) -> Response<GatewayBody> {
    let body = serde_json::to_vec(payload).expect("discovery JSON serializes");
    let etag = format!("\"{}\"", &hex_digest(&body)[..32]);
    if req_hdrs
        .get(http::header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|inm| inm.trim() == etag)
    {
        return Response::builder()
            .status(StatusCode::NOT_MODIFIED)
            .header(http::header::ETAG, etag)
            .header(http::header::CACHE_CONTROL, "public, max-age=300")
            .header(http::header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            .body(GatewayBody::Full(Full::new(Bytes::new())))
            .expect("valid 304 response");
    }
    Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, content_type)
        .header(http::header::ETAG, etag)
        .header(http::header::CACHE_CONTROL, "public, max-age=300")
        .header(http::header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
        .body(GatewayBody::Full(Full::new(Bytes::from(body))))
        .expect("valid discovery response")
}

fn method_not_allowed_get(path: &str, method: &Method) -> Response<GatewayBody> {
    let mut resp = error_response(
        StatusCode::METHOD_NOT_ALLOWED,
        &format!("expected GET on {path}, saw {method}"),
    );
    resp.headers_mut()
        .insert(http::header::ALLOW, HeaderValue::from_static("GET"));
    resp
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
            method_not_allowed_get("/healthz", req.method())
        };
    }

    let clean_ep = clean_endpoint(&state.endpoint);
    let card_path = format!("{clean_ep}/server-card");
    if path == card_path || path == "/.well-known/ai-catalog.json" {
        if req.method() != Method::GET {
            return method_not_allowed_get(path, req.method());
        }
        let mcp_url = resolve_public_endpoint_url(&state, req.headers());
        let card = build_server_card(&mcp_url);
        return if path == "/.well-known/ai-catalog.json" {
            let catalog = serde_json::json!({
                "specVersion": "1.0",
                "entries": [
                    {
                        "identifier": "urn:air:roger-wilco:mcp:gateway",
                        "type": "application/mcp-server-card+json",
                        "data": card
                    }
                ]
            });
            discovery_json_response(req.headers(), "application/ai-catalog+json", &catalog)
        } else {
            discovery_json_response(req.headers(), "application/mcp-server-card+json", &card)
        };
    }

    let prm_subpath = format!("/.well-known/oauth-protected-resource{clean_ep}");
    if !state.auth_servers.is_empty()
        && (path == prm_subpath || path == "/.well-known/oauth-protected-resource")
    {
        if req.method() != Method::GET {
            return method_not_allowed_get(path, req.method());
        }
        let mcp_url = resolve_public_endpoint_url(&state, req.headers());
        let mut prm = Map::new();
        prm.insert("resource".into(), Value::String(mcp_url));
        prm.insert(
            "authorization_servers".into(),
            Value::Array(
                state
                    .auth_servers
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect(),
            ),
        );
        prm.insert(
            "bearer_methods_supported".into(),
            serde_json::json!(["header"]),
        );
        if !state.scopes.is_empty() {
            prm.insert(
                "scopes_supported".into(),
                Value::Array(state.scopes.iter().cloned().map(Value::String).collect()),
            );
        }
        return json_value_response(StatusCode::OK, &Value::Object(prm));
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
            let challenge = if state.auth_servers.is_empty() {
                HeaderValue::from_static("Bearer")
            } else {
                let mcp_url = resolve_public_endpoint_url(&state, req.headers());
                let origin = origin_of_url(&mcp_url);
                let prm_url = format!("{origin}{prm_subpath}");
                let raw = if state.scopes.is_empty() {
                    format!("Bearer resource_metadata=\"{prm_url}\"")
                } else {
                    format!(
                        "Bearer resource_metadata=\"{prm_url}\", scope=\"{}\"",
                        state.scopes.join(" ")
                    )
                };
                HeaderValue::from_str(&raw).unwrap_or_else(|_| HeaderValue::from_static("Bearer"))
            };
            let mut resp = error_response(
                StatusCode::UNAUTHORIZED,
                "expected valid Authorization: Bearer token, saw missing or invalid credentials",
            );
            resp.headers_mut()
                .insert(http::header::WWW_AUTHENTICATE, challenge);
            return resp;
        }
    }

    match *req.method() {
        Method::POST => handle_post(Arc::clone(&state), req).await,
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

pub fn is_plain_header_safe(raw: &str) -> bool {
    if raw.starts_with("=?base64?") && raw.ends_with("?=") {
        return false;
    }
    if raw.starts_with([' ', '\t']) || raw.ends_with([' ', '\t']) {
        return false;
    }
    raw.bytes()
        .all(|b| (0x21..=0x7e).contains(&b) || b == b' ' || b == b'\t')
}

pub fn decode_mcp_name_header(raw: &str) -> Result<String, String> {
    if let Some(inner) = raw
        .strip_prefix("=?base64?")
        .and_then(|s| s.strip_suffix("?="))
    {
        let bytes = decode_base64(inner)?;
        return String::from_utf8(bytes)
            .map_err(|_| "expected UTF-8 inside base64 MCP header".to_string());
    }
    if !is_plain_header_safe(raw) {
        return Err(
            "expected plain visible ASCII MCP header value without outer whitespace".into(),
        );
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
    let mut best_spec: i32 = -1;
    let mut best_allowed = false;
    for val in hdrs.get_all(http::header::ACCEPT) {
        saw_accept = true;
        let Ok(text) = val.to_str() else {
            continue;
        };
        for item in text.split(',') {
            let mut parts = item.split(';');
            let media = parts.next().unwrap_or("").trim();
            if media.is_empty() {
                continue;
            }
            let spec = if media.eq_ignore_ascii_case(target) {
                2
            } else if media
                .strip_suffix("/*")
                .is_some_and(|p| p.eq_ignore_ascii_case(target_prefix))
            {
                1
            } else if media == "*/*" {
                0
            } else {
                continue;
            };
            let mut q = 1.0f32;
            let mut valid_q = true;
            for param in parts {
                if let Some((k, v)) = param.trim().split_once('=') {
                    if k.trim().eq_ignore_ascii_case("q") {
                        match v.trim().parse::<f32>() {
                            Ok(parsed) if (0.0..=1.0).contains(&parsed) => q = parsed,
                            _ => valid_q = false,
                        }
                    }
                }
            }
            if !valid_q {
                continue;
            }
            if spec > best_spec {
                best_spec = spec;
                best_allowed = q > 0.0;
            } else if spec == best_spec && q <= 0.0 {
                best_allowed = false;
            }
        }
    }
    if !saw_accept {
        return true;
    }
    best_spec >= 0 && best_allowed
}

/// Validate `Mcp-Param-*` headers against `x-mcp-header` annotations in `inputSchema`.
pub fn validate_mcp_param_headers(
    hdrs: &HeaderMap,
    input_schema: &Value,
    arguments: Option<&Value>,
) -> Result<(), RpcErr> {
    fn collect<'a>(
        node: &'a Value,
        path: &mut Vec<&'a str>,
        out: &mut Vec<(Vec<&'a str>, &'a str, &'a str)>,
    ) {
        let Some(obj) = node.as_object() else {
            return;
        };
        if let Some(hdr_name) = obj.get("x-mcp-header").and_then(Value::as_str) {
            if let Some(ty) = obj.get("type").and_then(Value::as_str) {
                out.push((path.clone(), hdr_name, ty));
            }
        }
        if let Some(props) = obj.get("properties").and_then(Value::as_object) {
            for (k, sub) in props {
                path.push(k.as_str());
                collect(sub, path, out);
                path.pop();
            }
        }
    }

    let mut annotated = Vec::new();
    let mut path = Vec::new();
    collect(input_schema, &mut path, &mut annotated);
    if annotated.is_empty() {
        return Ok(());
    }

    for (prop_path, hdr_suffix, prop_type) in annotated {
        let hdr_lower = format!("mcp-param-{}", hdr_suffix.to_ascii_lowercase());
        let raw_hdr = header_utf8(hdrs, &hdr_lower).map_err(RpcErr::header_mismatch)?;
        let mut cur = arguments;
        for seg in &prop_path {
            cur = cur.and_then(Value::as_object).and_then(|m| m.get(*seg));
        }
        match cur {
            None | Some(Value::Null) => {
                if raw_hdr.is_some() {
                    return Err(RpcErr::header_mismatch(format!(
                        "Header mismatch: Mcp-Param-{hdr_suffix} must be omitted when body parameter is absent or null"
                    )));
                }
            }
            Some(body_val) => {
                let Some(raw) = raw_hdr else {
                    return Err(RpcErr::header_mismatch(format!(
                        "Header mismatch: missing required Mcp-Param-{hdr_suffix} header"
                    )));
                };
                let decoded = decode_mcp_name_header(&raw).map_err(RpcErr::header_mismatch)?;
                let matched = match prop_type {
                    "string" => body_val.as_str().is_some_and(|s| s == decoded),
                    "boolean" => body_val
                        .as_bool()
                        .is_some_and(|b| decoded == if b { "true" } else { "false" }),
                    "integer" => {
                        // Compare integer header and JSON numeric value numerically (e.g. 42.0 == 42).
                        let hdr_i = decoded.parse::<i64>().ok();
                        let body_i = body_val.as_i64().or_else(|| {
                            body_val.as_f64().and_then(|f| {
                                (f.is_finite()
                                    && f.fract() == 0.0
                                    && (-9_007_199_254_740_991.0..=9_007_199_254_740_991.0)
                                        .contains(&f))
                                .then_some(f as i64)
                            })
                        });
                        hdr_i.is_some() && hdr_i == body_i
                    }
                    _ => true,
                };
                if !matched {
                    return Err(RpcErr::header_mismatch(format!(
                        "Header mismatch: Mcp-Param-{hdr_suffix} does not match request body"
                    )));
                }
            }
        }
    }
    Ok(())
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
    } else if method == "resources/read" {
        let raw_name = match header_utf8(hdrs, "mcp-name") {
            Ok(Some(v)) => v,
            Ok(None) => {
                return Err(RpcErr::header_mismatch(
                    "expected Mcp-Name header on resources/read, saw none",
                ))
            }
            Err(e) => return Err(RpcErr::header_mismatch(e)),
        };
        let decoded = decode_mcp_name_header(&raw_name).map_err(RpcErr::header_mismatch)?;
        let body_uri = params.get("uri").and_then(Value::as_str).unwrap_or("");
        if body_uri.is_empty() || decoded != body_uri {
            return Err(RpcErr::header_mismatch(format!(
                "Header mismatch: Mcp-Name header value '{decoded}' does not match body params.uri '{body_uri}'"
            )));
        }
    } else if matches!(method, "tasks/get" | "tasks/cancel" | "tasks/update") {
        let raw_name = match header_utf8(hdrs, "mcp-name") {
            Ok(Some(v)) => v,
            Ok(None) => {
                return Err(RpcErr::header_mismatch(
                    "expected Mcp-Name header on tasks/*, saw none",
                ))
            }
            Err(e) => return Err(RpcErr::header_mismatch(e)),
        };
        let decoded = decode_mcp_name_header(&raw_name).map_err(RpcErr::header_mismatch)?;
        let body_tid = params.get("taskId").and_then(Value::as_str).unwrap_or("");
        if body_tid.is_empty() || decoded != body_tid {
            return Err(RpcErr::header_mismatch(format!(
                "Header mismatch: Mcp-Name header value '{decoded}' does not match body params.taskId '{body_tid}'"
            )));
        }
    }
    Ok(hdr_pv)
}

async fn handle_post(state: Arc<GatewayState>, req: Request<Incoming>) -> Response<GatewayBody> {
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

    // Pre-2026-07-28 backwards-compatibility tolerance: a legacy `initialize`
    // handshake without modern headers/_meta receives a clear -32022 naming 2026-07-28.
    if method == "initialize"
        && (parts.headers.get("mcp-protocol-version").is_none() || params.get("_meta").is_none())
    {
        let requested = params
            .get("protocolVersion")
            .and_then(Value::as_str)
            .unwrap_or("legacy");
        return rpc_err_response(
            StatusCode::BAD_REQUEST,
            &id,
            RpcErr::unsupported_version(requested),
        );
    }

    let hdr_pv = match validate_request_headers(&parts.headers, &method, &params) {
        Ok(v) => v,
        Err(err) => return rpc_err_response(StatusCode::BAD_REQUEST, &id, err),
    };

    if matches!(
        method.as_str(),
        "tools/call" | "events/poll" | "events/stream" | "events/subscribe" | "events/unsubscribe"
    ) {
        let target_name = params.get("name").and_then(Value::as_str).unwrap_or("");
        let schema_opt = if method == "tools/call" {
            mcp::tool_schema(state.sessions.groups(), target_name)
        } else {
            mcp::event_schema(target_name)
        };
        if let Some(schema) = schema_opt {
            if let Some(in_schema) = schema.get("inputSchema") {
                if let Err(err) =
                    validate_mcp_param_headers(&parts.headers, in_schema, params.get("arguments"))
                {
                    return rpc_err_response(StatusCode::BAD_REQUEST, &id, err);
                }
            }
        }
    }

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
        "tools/list" => match mcp::tools_list_paged(state.sessions.groups(), &params) {
            Ok(v) => json_value_response(StatusCode::OK, &mcp::reply_ok(&id, v)),
            Err(err) => rpc_err_response(StatusCode::BAD_REQUEST, &id, err),
        },
        "tools/call" => {
            let target_name = params.get("name").and_then(Value::as_str);
            let principal = state.bearer_token.as_deref().map(|_| "bearer");
            if let Err(err) = mrtr::validate_mrtr_params(
                &state.mrtr_secret,
                &params,
                "tools/call",
                target_name,
                principal,
                crate::session::now_ms(),
            ) {
                return rpc_err_response(StatusCode::BAD_REQUEST, &id, err);
            }
            if params
                .get("_meta")
                .and_then(Value::as_object)
                .and_then(|m| m.get("io.modelcontextprotocol/task"))
                == Some(&Value::Bool(true))
            {
                return match spawn_gateway_tool_task(&state, &params) {
                    Ok(v) => json_value_response(StatusCode::OK, &mcp::reply_ok(&id, v)),
                    Err(err) => rpc_err_response(StatusCode::BAD_REQUEST, &id, err),
                };
            }
            let result = match mcp::call_tool(&state.sessions, &params).await {
                Ok(v) => {
                    let rendered =
                        serde_json::to_string(&v).expect("tool output serializes as JSON");
                    mcp::tool_result_structured(&rendered, Some(v), false)
                }
                Err(reason) => mcp::tool_result(&reason, true),
            };
            json_value_response(StatusCode::OK, &mcp::reply_ok(&id, result))
        }
        "skills/list" => match mcp::skills_list_paged(&params) {
            Ok(v) => json_value_response(StatusCode::OK, &mcp::reply_ok(&id, v)),
            Err(err) => rpc_err_response(StatusCode::BAD_REQUEST, &id, err),
        },
        "skills/get" => match mcp::skills_get_result(&params) {
            Ok(v) => json_value_response(StatusCode::OK, &mcp::reply_ok(&id, v)),
            Err(err) => rpc_err_response(StatusCode::BAD_REQUEST, &id, err),
        },
        "resources/list" => match mcp::resources_list_paged(&params) {
            Ok(v) => json_value_response(StatusCode::OK, &mcp::reply_ok(&id, v)),
            Err(err) => rpc_err_response(StatusCode::BAD_REQUEST, &id, err),
        },
        "resources/templates/list" => match mcp::resource_templates_list_paged(&params) {
            Ok(v) => json_value_response(StatusCode::OK, &mcp::reply_ok(&id, v)),
            Err(err) => rpc_err_response(StatusCode::BAD_REQUEST, &id, err),
        },
        "resources/read" => match mcp::resources_read_result(&params) {
            Ok(v) => json_value_response(StatusCode::OK, &mcp::reply_ok(&id, v)),
            Err(err) => rpc_err_response(StatusCode::BAD_REQUEST, &id, err),
        },
        "resources/directory/read" => match mcp::resources_directory_read(&params) {
            Ok(v) => json_value_response(StatusCode::OK, &mcp::reply_ok(&id, v)),
            Err(err) => rpc_err_response(StatusCode::BAD_REQUEST, &id, err),
        },
        "tasks/get" => match gateway_tasks_get(&state, &params) {
            Ok(v) => json_value_response(StatusCode::OK, &mcp::reply_ok(&id, v)),
            Err(err) => rpc_err_response(StatusCode::BAD_REQUEST, &id, err),
        },
        "tasks/cancel" => match gateway_tasks_cancel(&state, &params) {
            Ok(v) => json_value_response(StatusCode::OK, &mcp::reply_ok(&id, v)),
            Err(err) => rpc_err_response(StatusCode::BAD_REQUEST, &id, err),
        },
        "tasks/update" => match gateway_tasks_update(&state, &params) {
            Ok(v) => json_value_response(StatusCode::OK, &mcp::reply_ok(&id, v)),
            Err(err) => rpc_err_response(StatusCode::BAD_REQUEST, &id, err),
        },
        "events/list" => match mcp::events_list_paged(&params) {
            Ok(v) => json_value_response(StatusCode::OK, &mcp::reply_ok(&id, v)),
            Err(err) => rpc_err_response(StatusCode::BAD_REQUEST, &id, err),
        },
        "events/subscribe" | "events/unsubscribe" => {
            let err = mcp::events_webhook_unsupported(&params);
            let status = if err.code == -32602 {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::OK
            };
            rpc_err_response(status, &id, err)
        }
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
            let mut task_ids: Vec<String> = Vec::new();
            if let Some(n) = params.get("notifications") {
                let Some(n_obj) = n.as_object() else {
                    return rpc_err_response(
                        StatusCode::BAD_REQUEST,
                        &id,
                        RpcErr::invalid_params("expected notifications to be an object"),
                    );
                };
                if let Some(raw_tids) = n_obj.get("taskIds") {
                    let Some(arr) = raw_tids.as_array() else {
                        return rpc_err_response(
                            StatusCode::BAD_REQUEST,
                            &id,
                            RpcErr::invalid_params("expected notifications.taskIds to be an array"),
                        );
                    };
                    if !mcp::client_declares_ext(&params, "io.modelcontextprotocol/tasks") {
                        return rpc_err_response(
                            StatusCode::BAD_REQUEST,
                            &id,
                            RpcErr::missing_capability(serde_json::json!({
                                "extensions": { "io.modelcontextprotocol/tasks": {} }
                            })),
                        );
                    }
                    for item in arr {
                        let Some(tid) = item.as_str() else {
                            return rpc_err_response(
                                StatusCode::BAD_REQUEST,
                                &id,
                                RpcErr::invalid_params("expected taskIds entries to be strings"),
                            );
                        };
                        if !tid.is_empty() && !task_ids.contains(&tid.to_string()) {
                            task_ids.push(tid.to_string());
                        }
                    }
                }
            }
            sse_response(spawn_subscriptions_listen(
                &state,
                id,
                task_ids,
                state.shutdown_rx.clone(),
            ))
        }
        unknown => rpc_err_response(
            StatusCode::NOT_FOUND,
            &id,
            RpcErr {
                code: -32601,
                message: format!(
                    "expected server/discover, tools/list, tools/call, subscriptions/listen, or events/* (supported protocol versions: {}), saw {unknown}",
                    mcp::VERSION
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

fn spawn_gateway_tool_task(state: &Arc<GatewayState>, params: &Value) -> Result<Value, RpcErr> {
    if !mcp::client_declares_ext(params, "io.modelcontextprotocol/tasks") {
        return Err(RpcErr::missing_capability(serde_json::json!({
            "extensions": { "io.modelcontextprotocol/tasks": {} }
        })));
    }
    let task_id = blocking_io(gen_uuid).map_err(|e| RpcErr {
        code: -32603,
        message: e.to_string(),
        data: None,
    })?;
    let ts = mcp::format_iso8601_utc(crate::session::now_ms());
    let cancelled = Arc::new(AtomicBool::new(false));
    let cancel_notify = Arc::new(Notify::new());
    let seed = GatewayTask {
        task_id: task_id.clone(),
        status: "working".into(),
        created_at: ts.clone(),
        last_updated_at: ts,
        result: None,
        cancelled: Arc::clone(&cancelled),
        cancel_notify: Arc::clone(&cancel_notify),
    };
    let created_response = seed.to_value("task");
    state.tasks.lock().unwrap().insert(task_id.clone(), seed);

    let state_c = Arc::clone(state);
    let params_c = params.clone();
    tokio::spawn(async move {
        let notified = cancel_notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        let outcome = tokio::select! {
            _ = &mut notified => None,
            res = mcp::call_tool(&state_c.sessions, &params_c) => Some(res),
        };

        let snap = {
            let mut guard = state_c.tasks.lock().unwrap();
            let Some(entry) = guard.get_mut(&task_id) else {
                return;
            };
            if entry.status != "working" {
                return;
            }
            match outcome {
                None => {
                    entry.status = "cancelled".into();
                }
                Some(res) if !cancelled.load(Ordering::SeqCst) => {
                    let tool_out = match res {
                        Ok(v) => {
                            let rendered =
                                serde_json::to_string(&v).expect("tool output serializes");
                            mcp::tool_result_structured(&rendered, Some(v), false)
                        }
                        Err(reason) => mcp::tool_result(&reason, true),
                    };
                    entry.status = "completed".into();
                    entry.result = Some(tool_out);
                }
                Some(_) => {
                    entry.status = "cancelled".into();
                }
            }
            entry.last_updated_at = mcp::format_iso8601_utc(crate::session::now_ms());
            entry.clone()
        };
        state_c.emit_task(&snap);
    });

    Ok(created_response)
}

fn require_gateway_task_id(params: &Value) -> Result<&str, RpcErr> {
    params
        .get("taskId")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| RpcErr::invalid_params("expected params.taskId to be a non-empty string"))
}

fn gateway_tasks_get(state: &GatewayState, params: &Value) -> Result<Value, RpcErr> {
    let tid = require_gateway_task_id(params)?;
    let guard = state.tasks.lock().unwrap();
    let Some(entry) = guard.get(tid) else {
        return Err(RpcErr::invalid_params(format!("unknown task: {tid}")));
    };
    Ok(entry.to_value("complete"))
}

fn gateway_tasks_cancel(state: &GatewayState, params: &Value) -> Result<Value, RpcErr> {
    let tid = require_gateway_task_id(params)?;
    let (changed, snap) = {
        let mut guard = state.tasks.lock().unwrap();
        let Some(entry) = guard.get_mut(tid) else {
            return Err(RpcErr::invalid_params(format!("unknown task: {tid}")));
        };
        let changed = entry.status == "working" || entry.status == "input_required";
        if changed {
            entry.cancelled.store(true, Ordering::SeqCst);
            entry.status = "cancelled".into();
            entry.last_updated_at = mcp::format_iso8601_utc(crate::session::now_ms());
        }
        (changed, entry.clone())
    };
    if changed {
        snap.cancel_notify.notify_waiters();
        state.emit_task(&snap);
    }
    Ok(snap.to_value("complete"))
}

fn gateway_tasks_update(state: &GatewayState, params: &Value) -> Result<Value, RpcErr> {
    let tid = require_gateway_task_id(params)?;
    if !params.get("inputResponses").is_some_and(Value::is_object) {
        return Err(RpcErr::invalid_params(
            "expected params.inputResponses to be an object",
        ));
    }
    let guard = state.tasks.lock().unwrap();
    let Some(entry) = guard.get(tid) else {
        return Err(RpcErr::invalid_params(format!("unknown task: {tid}")));
    };
    if entry.status != "input_required" {
        return Err(RpcErr::invalid_params("task is not awaiting input"));
    }
    Ok(entry.to_value("complete"))
}

fn spawn_subscriptions_listen(
    state: &GatewayState,
    id: Value,
    task_ids: Vec<String>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> SseStreamBody {
    let (tx, rx) = mpsc::channel::<Bytes>(32);
    let mut notif_map = Map::new();
    if !task_ids.is_empty() {
        notif_map.insert(
            "taskIds".into(),
            Value::Array(task_ids.iter().cloned().map(Value::String).collect()),
        );
        state.task_sinks.lock().unwrap().push(TaskStreamSink {
            sub_id: id.clone(),
            task_ids,
            tx: tx.clone(),
        });
    }
    tokio::spawn(async move {
        let mut ack_params = Map::new();
        ack_params.insert("notifications".into(), Value::Object(notif_map));
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

        h.insert(
            http::header::ACCEPT,
            HeaderValue::from_static("application/json;q=0, */*"),
        );
        assert!(!accept_allows_media(&h, "application/json"));
        assert!(accept_allows_media(&h, "text/event-stream"));
    }

    #[test]
    fn mcp_param_headers_validate_string_bool_integer_and_omission() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": {
                "region": { "type": "string", "x-mcp-header": "Region" },
                "limit": { "type": "integer", "x-mcp-header": "Limit" },
                "flag": { "type": "boolean", "x-mcp-header": "Flag" }
            }
        });
        let mut h = HeaderMap::new();
        h.insert("mcp-param-region", HeaderValue::from_static("us-west1"));
        h.insert("mcp-param-limit", HeaderValue::from_static("42"));
        h.insert("mcp-param-flag", HeaderValue::from_static("true"));
        assert!(validate_mcp_param_headers(
            &h,
            &schema,
            Some(&serde_json::json!({"region": "us-west1", "limit": 42.0, "flag": true}))
        )
        .is_ok());

        // Missing required header when body parameter is present -> -32020
        let mut missing = HeaderMap::new();
        assert_eq!(
            validate_mcp_param_headers(
                &missing,
                &schema,
                Some(&serde_json::json!({"region": "us-west1"}))
            )
            .unwrap_err()
            .code,
            -32020
        );

        // Header present when body parameter is null -> -32020
        missing.insert("mcp-param-region", HeaderValue::from_static("us-west1"));
        assert_eq!(
            validate_mcp_param_headers(
                &missing,
                &schema,
                Some(&serde_json::json!({"region": null}))
            )
            .unwrap_err()
            .code,
            -32020
        );
    }
}

//! Streamable HTTP + Server-Sent Events (SSE) push gateway service over HTTP/1.1 and HTTP/2 (`h2c`).

use crate::cli::{constant_time_eq, write_port_file, Config};
use crate::mcp::{self, Parsed};
use crate::session::{GatewaySession, InitParams, SessionManager};
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
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::net::TcpListener;
use tokio::sync::futures::OwnedNotified;

/// 16 MiB limit on `POST <endpoint>` bodies.
pub const MAX_POST_BODY_BYTES: usize = 16 * 1024 * 1024;

pub struct GatewayState {
    pub endpoint: String,
    pub bearer_token: Option<String>,
    pub sessions: Arc<SessionManager>,
}

pub enum GatewayBody {
    Full(Full<Bytes>),
    Sse(SseBody),
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
            GatewayBody::Sse(sse) => Pin::new(sse).poll_frame(cx),
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            GatewayBody::Full(full) => full.is_end_stream(),
            GatewayBody::Sse(_) => false,
        }
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        match self {
            GatewayBody::Full(full) => full.size_hint(),
            GatewayBody::Sse(_) => hyper::body::SizeHint::default(),
        }
    }
}

/// Pure event-driven SSE body: yields `id: <seq>\nevent: message\ndata: <json>\n\n` frames
/// directly from the session's ordered event log using `Notify`, with zero polling.
pub struct SseBody {
    session: Arc<GatewaySession>,
    cursor: u64,
    waiter: Option<Pin<Box<OwnedNotified>>>,
}

impl SseBody {
    pub fn new(session: Arc<GatewaySession>, cursor: u64) -> Self {
        Self {
            session,
            cursor,
            waiter: None,
        }
    }
}

impl Body for SseBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        loop {
            if this.waiter.is_none() {
                let mut fut = Box::pin(Arc::clone(&this.session.event_notify).notified_owned());
                let _ = fut.as_mut().poll(cx);
                this.waiter = Some(fut);
            }

            let next_item = {
                let mut log = this.session.event_log.lock().unwrap();
                let idx = log.events.partition_point(|(seq, _)| *seq <= this.cursor);
                if let Some((seq, payload)) = log.events.get(idx).cloned() {
                    this.cursor = seq;
                    if log.delivered_up_to < seq {
                        log.delivered_up_to = seq;
                    }
                    Some((seq, payload))
                } else if log.closed {
                    return Poll::Ready(None);
                } else {
                    None
                }
            };

            if let Some((seq, payload)) = next_item {
                this.waiter = None;
                let frame = format!("id: {seq}\nevent: message\ndata: {payload}\n\n");
                return Poll::Ready(Some(Ok(Frame::data(Bytes::from(frame)))));
            }

            let waiter = this.waiter.as_mut().expect("waiter initialized above");
            match waiter.as_mut().poll(cx) {
                Poll::Ready(()) => {
                    this.waiter = None;
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
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
        #[cfg(feature = "test-hooks")]
        cfg.hold_before_ack,
    );
    let state = Arc::new(GatewayState {
        endpoint: cfg.endpoint,
        bearer_token: cfg.bearer_token,
        sessions,
    });

    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => {
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
        Method::GET => handle_get(&state, req),
        Method::DELETE => handle_delete(&state, req).await,
        ref other => {
            let mut resp = error_response(
                StatusCode::METHOD_NOT_ALLOWED,
                &format!("expected POST, GET, or DELETE, saw {other}"),
            );
            resp.headers_mut().insert(
                http::header::ALLOW,
                HeaderValue::from_static("GET, POST, DELETE"),
            );
            resp
        }
    }
}

fn authorized(headers: &HeaderMap, expected_token: &[u8]) -> bool {
    let Some(raw) = headers.get(http::header::AUTHORIZATION) else {
        return false;
    };
    let bytes = raw.as_bytes();
    if bytes.len() < 7 || !bytes[..7].eq_ignore_ascii_case(b"Bearer ") {
        return false;
    }
    constant_time_eq(&bytes[7..], expected_token)
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
                StatusCode::OK,
                None,
                None,
                &mcp::reply_err(
                    &Value::Null,
                    -32700,
                    "expected UTF-8 JSON body, saw non-UTF-8 bytes",
                ),
            );
        }
    };

    match mcp::parse_message(text) {
        Parsed::Request { id, method, params } if method == "initialize" => {
            let init_params = match extract_init_params(&parts.headers, parts.uri.query(), &params)
            {
                Ok(p) => p,
                Err(reason) => return error_response(StatusCode::BAD_REQUEST, &reason),
            };
            match state.sessions.initialize(init_params).await {
                Ok(session) => {
                    let payload = mcp::reply_ok(&id, mcp::initialize_result(&params));
                    json_value_response(
                        StatusCode::OK,
                        Some(&session.session_id),
                        Some(&session.address),
                        &payload,
                    )
                }
                Err(e) => {
                    let code = if e.kind() == io::ErrorKind::WouldBlock {
                        StatusCode::CONFLICT
                    } else {
                        StatusCode::BAD_GATEWAY
                    };
                    error_response(code, &e.to_string())
                }
            }
        }
        Parsed::Bad { id, code, message } => {
            if let Some(raw_sid) = parts.headers.get("mcp-session-id") {
                let Ok(sid) = std::str::from_utf8(raw_sid.as_bytes()) else {
                    return error_response(
                        StatusCode::BAD_REQUEST,
                        "expected UTF-8 Mcp-Session-Id header, saw invalid bytes",
                    );
                };
                if sid.is_empty() {
                    return error_response(
                        StatusCode::BAD_REQUEST,
                        "expected non-empty Mcp-Session-Id header, saw empty value",
                    );
                }
                if state.sessions.get(sid).is_none() {
                    return error_response(
                        StatusCode::NOT_FOUND,
                        "expected an active Mcp-Session-Id, saw unknown or terminated session",
                    );
                }
            }
            json_value_response(
                StatusCode::OK,
                None,
                None,
                &mcp::reply_err(&id, code, &message),
            )
        }
        other => {
            let session = match require_session(state, &parts.headers) {
                Ok(s) => s,
                Err((status, msg)) => return error_response(status, msg),
            };
            match other {
                Parsed::Notification | Parsed::Ignore => {
                    empty_response(StatusCode::ACCEPTED, Some(&session.session_id))
                }
                Parsed::Request { id, method, params } => match method.as_str() {
                    "ping" => json_value_response(
                        StatusCode::OK,
                        Some(&session.session_id),
                        None,
                        &mcp::reply_ok(&id, Value::Object(Map::new())),
                    ),
                    "tools/list" => json_value_response(
                        StatusCode::OK,
                        Some(&session.session_id),
                        None,
                        &mcp::reply_ok(&id, mcp::tools_list()),
                    ),
                    "tools/call" => {
                        let result = match mcp::call_tool(&session, &params).await {
                            Ok(v) => {
                                let rendered = serde_json::to_string(&v)
                                    .expect("tool output serializes as JSON");
                                mcp::tool_result(&rendered, false)
                            }
                            Err(reason) => mcp::tool_result(&reason, true),
                        };
                        json_value_response(
                            StatusCode::OK,
                            Some(&session.session_id),
                            None,
                            &mcp::reply_ok(&id, result),
                        )
                    }
                    unknown => {
                        let msg = format!(
                            "expected initialize, ping, tools/list, or tools/call, saw {unknown}"
                        );
                        json_value_response(
                            StatusCode::OK,
                            Some(&session.session_id),
                            None,
                            &mcp::reply_err(&id, -32601, &msg),
                        )
                    }
                },
                Parsed::Bad { .. } => unreachable!(),
            }
        }
    }
}

fn handle_get(state: &GatewayState, req: Request<Incoming>) -> Response<GatewayBody> {
    let session = match require_session(state, req.headers()) {
        Ok(s) => s,
        Err((status, msg)) => return error_response(status, msg),
    };

    if !accept_allows_sse(req.headers()) {
        return error_response(
            StatusCode::NOT_ACCEPTABLE,
            "expected Accept header to include text/event-stream or */*, saw neither",
        );
    }

    let last_event_id = match parse_last_event_id(req.headers()) {
        Ok(v) => v,
        Err(reason) => return error_response(StatusCode::BAD_REQUEST, &reason),
    };

    let cursor = session.initial_sse_cursor(last_event_id);
    let sid = session.session_id.clone();
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "text/event-stream")
        .header(http::header::CACHE_CONTROL, "no-cache");
    if let Ok(hv) = HeaderValue::from_bytes(sid.as_bytes()) {
        builder = builder.header("mcp-session-id", hv);
    }
    builder
        .body(GatewayBody::Sse(SseBody::new(session, cursor)))
        .expect("valid SSE response")
}

async fn handle_delete(state: &GatewayState, req: Request<Incoming>) -> Response<GatewayBody> {
    let sid = match extract_session_id(req.headers()) {
        Ok(id) => id,
        Err((status, msg)) => return error_response(status, msg),
    };
    if state.sessions.delete(&sid).await {
        raw_json_response(StatusCode::OK, r#"{"ok":true}"#)
    } else {
        error_response(
            StatusCode::NOT_FOUND,
            "expected an active Mcp-Session-Id, saw unknown or terminated session",
        )
    }
}

fn extract_session_id(headers: &HeaderMap) -> Result<String, (StatusCode, &'static str)> {
    let Some(raw) = headers.get("mcp-session-id") else {
        return Err((
            StatusCode::BAD_REQUEST,
            "expected Mcp-Session-Id header, saw none",
        ));
    };
    let text = std::str::from_utf8(raw.as_bytes()).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            "expected UTF-8 Mcp-Session-Id header, saw invalid bytes",
        )
    })?;
    if text.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "expected non-empty Mcp-Session-Id header, saw empty value",
        ));
    }
    Ok(text.to_string())
}

fn require_session(
    state: &GatewayState,
    headers: &HeaderMap,
) -> Result<Arc<GatewaySession>, (StatusCode, &'static str)> {
    let sid = extract_session_id(headers)?;
    state.sessions.get(&sid).ok_or((
        StatusCode::NOT_FOUND,
        "expected an active Mcp-Session-Id, saw unknown or terminated session",
    ))
}

fn accept_allows_sse(headers: &HeaderMap) -> bool {
    let mut saw_accept = false;
    for val in headers.get_all(http::header::ACCEPT) {
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
            if !q_zero && (media.eq_ignore_ascii_case("text/event-stream") || media == "*/*") {
                return true;
            }
        }
    }
    !saw_accept
}

fn parse_last_event_id(headers: &HeaderMap) -> Result<Option<u64>, String> {
    let Some(raw) = headers.get("last-event-id") else {
        return Ok(None);
    };
    let text = std::str::from_utf8(raw.as_bytes())
        .map_err(|_| "expected UTF-8 Last-Event-ID header, saw invalid bytes".to_string())?
        .trim();
    if text.is_empty() {
        return Ok(None);
    }
    text.parse::<u64>()
        .map(Some)
        .map_err(|_| format!("expected Last-Event-ID to be an unsigned integer, saw {text:?}"))
}

fn header_utf8(headers: &HeaderMap, name: &str) -> Result<Option<String>, String> {
    let Some(raw) = headers.get(name) else {
        return Ok(None);
    };
    let s = std::str::from_utf8(raw.as_bytes())
        .map_err(|_| format!("expected UTF-8 in {name}, saw invalid bytes"))?;
    Ok((!s.is_empty()).then(|| s.to_string()))
}

pub fn percent_decode(raw: &str) -> Result<String, String> {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return Err(format!(
                    "expected two hex digits after '%', saw {:?}",
                    &raw[i..]
                ));
            }
            let hi = (bytes[i + 1] as char).to_digit(16).ok_or_else(|| {
                format!(
                    "expected hex digit in percent escape, saw {:?}",
                    &raw[i..i + 3]
                )
            })?;
            let lo = (bytes[i + 2] as char).to_digit(16).ok_or_else(|| {
                format!(
                    "expected hex digit in percent escape, saw {:?}",
                    &raw[i..i + 3]
                )
            })?;
            out.push(((hi << 4) | lo) as u8);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out)
        .map_err(|_| "expected percent-decoded UTF-8, saw invalid UTF-8 bytes".to_string())
}

pub fn query_param(query: Option<&str>, key: &str) -> Result<Option<String>, String> {
    let Some(q) = query else {
        return Ok(None);
    };
    for pair in q.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (raw_k, raw_v) = pair.split_once('=').unwrap_or((pair, ""));
        let k = percent_decode(raw_k)?;
        if k == key {
            let v = percent_decode(raw_v)?;
            if !v.is_empty() {
                return Ok(Some(v));
            }
        }
    }
    Ok(None)
}

fn meta_str(params: &Value, key: &str) -> Option<String> {
    params
        .get("_meta")
        .and_then(Value::as_object)
        .and_then(|m| m.get(key))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn extract_init_params(
    headers: &HeaderMap,
    query: Option<&str>,
    params: &Value,
) -> Result<InitParams, String> {
    let address = match header_utf8(headers, "x-roger-address")? {
        Some(a) => Some(a),
        None => match query_param(query, "id")? {
            Some(a) => Some(a),
            None => match query_param(query, "address")? {
                Some(a) => Some(a),
                None => meta_str(params, "roger_address"),
            },
        },
    };

    let label = match header_utf8(headers, "x-roger-label")? {
        Some(l) => Some(l),
        None => match query_param(query, "label")? {
            Some(l) => Some(l),
            None => meta_str(params, "roger_label"),
        },
    };

    let declared = match header_utf8(headers, "x-roger-declare")? {
        Some(d) => Some(d),
        None => match query_param(query, "declare")? {
            Some(d) => Some(d),
            None => meta_str(params, "roger_declare"),
        },
    };

    let watch = match header_utf8(headers, "x-roger-watch")? {
        Some(w) => watch_flag(&w, "X-Roger-Watch")?,
        None => match query_param(query, "watch")? {
            Some(w) => watch_flag(&w, "query watch")?,
            None => match params.get("_meta").and_then(|m| m.get("roger_watch")) {
                None | Some(Value::Null) => false,
                Some(Value::Bool(b)) => *b,
                Some(Value::String(w)) => watch_flag(w, "_meta.roger_watch")?,
                Some(Value::Number(n)) => watch_flag(&n.to_string(), "_meta.roger_watch")?,
                Some(other) => {
                    return Err(format!(
                        "expected _meta.roger_watch to be true, false, 1 or 0, saw {other}"
                    ))
                }
            },
        },
    };

    Ok(InitParams {
        address,
        label,
        declared,
        watch,
    })
}

/// The `board_event` opt-in: `1` or `true` asks, `0` or `false` does not.
fn watch_flag(raw: &str, from: &str) -> Result<bool, String> {
    match raw.trim() {
        "1" | "true" => Ok(true),
        "0" | "false" => Ok(false),
        other => Err(format!(
            "expected {from} to be 1, 0, true or false, saw {other:?}"
        )),
    }
}

fn json_value_response(
    status: StatusCode,
    session_id: Option<&str>,
    address: Option<&str>,
    value: &Value,
) -> Response<GatewayBody> {
    let body = serde_json::to_vec(value).expect("JSON response serializes");
    let mut builder = Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, "application/json");
    if let Some(sid) = session_id {
        if let Ok(hv) = HeaderValue::from_bytes(sid.as_bytes()) {
            builder = builder.header("mcp-session-id", hv);
        }
    }
    if let Some(addr) = address {
        if let Ok(hv) = HeaderValue::from_bytes(addr.as_bytes()) {
            builder = builder.header("x-roger-address", hv);
        }
    }
    builder
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

fn empty_response(status: StatusCode, session_id: Option<&str>) -> Response<GatewayBody> {
    let mut builder = Response::builder().status(status);
    if let Some(sid) = session_id {
        if let Ok(hv) = HeaderValue::from_bytes(sid.as_bytes()) {
            builder = builder.header("mcp-session-id", hv);
        }
    }
    builder
        .body(GatewayBody::Full(Full::new(Bytes::new())))
        .expect("valid empty response")
}

fn error_response(status: StatusCode, message: &str) -> Response<GatewayBody> {
    let mut m = Map::new();
    m.insert("error".into(), Value::String(message.to_string()));
    json_value_response(status, None, None, &Value::Object(m))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percent_decoding_handles_composite_addresses_and_utf8() {
        let encoded = "%5B%5B153%3AMeteor%20Shower%3A7%5D%5Dcaf%C3%A9";
        assert_eq!(
            percent_decode(encoded).unwrap(),
            "[[153:Meteor Shower:7]]café"
        );
        assert!(percent_decode("%ZZ").is_err());
        assert!(percent_decode("%2").is_err());
    }

    #[test]
    fn init_parameter_precedence_is_header_then_query_then_meta() {
        let mut headers = HeaderMap::new();
        let meta = serde_json::from_str::<Value>(
            r#"{"_meta":{"roger_address":"from-meta","roger_label":"meta-l","roger_declare":"meta-d"}}"#,
        )
        .unwrap();

        let p = extract_init_params(&headers, None, &meta).unwrap();
        assert_eq!(p.address.as_deref(), Some("from-meta"));
        assert_eq!(p.label.as_deref(), Some("meta-l"));
        assert_eq!(p.declared.as_deref(), Some("meta-d"));

        let p = extract_init_params(
            &headers,
            Some("id=from-id&address=from-addr&label=query%20l&declare=query%20d"),
            &meta,
        )
        .unwrap();
        assert_eq!(p.address.as_deref(), Some("from-id"));
        assert_eq!(p.label.as_deref(), Some("query l"));
        assert_eq!(p.declared.as_deref(), Some("query d"));

        headers.insert("x-roger-address", HeaderValue::from_static("from-hdr"));
        headers.insert("x-roger-label", HeaderValue::from_static("hdr-l"));
        headers.insert("x-roger-declare", HeaderValue::from_static("hdr-d"));
        let p = extract_init_params(&headers, Some("id=from-id"), &meta).unwrap();
        assert_eq!(p.address.as_deref(), Some("from-hdr"));
        assert_eq!(p.label.as_deref(), Some("hdr-l"));
        assert_eq!(p.declared.as_deref(), Some("hdr-d"));
        assert!(!p.watch);
    }

    #[test]
    fn watch_opt_in_is_header_then_query_then_meta() {
        let mut headers = HeaderMap::new();
        let none = Value::Object(serde_json::Map::new());
        assert!(!extract_init_params(&headers, None, &none).unwrap().watch);
        let meta = serde_json::from_str::<Value>(r#"{"_meta":{"roger_watch":true}}"#).unwrap();
        assert!(extract_init_params(&headers, None, &meta).unwrap().watch);
        let meta1 = serde_json::from_str::<Value>(r#"{"_meta":{"roger_watch":1}}"#).unwrap();
        assert!(extract_init_params(&headers, None, &meta1).unwrap().watch);
        assert!(
            !extract_init_params(&headers, Some("watch=0"), &meta)
                .unwrap()
                .watch
        );
        headers.insert("x-roger-watch", HeaderValue::from_static("1"));
        assert!(
            extract_init_params(&headers, Some("watch=0"), &none)
                .unwrap()
                .watch
        );
        headers.insert("x-roger-watch", HeaderValue::from_static("yes"));
        let err = extract_init_params(&headers, None, &none).err().unwrap();
        assert!(err.contains("saw \"yes\""), "{err}");
        let bad = serde_json::from_str::<Value>(r#"{"_meta":{"roger_watch":[1]}}"#).unwrap();
        assert!(extract_init_params(&HeaderMap::new(), None, &bad).is_err());
    }

    #[test]
    fn accept_header_negotiation_follows_sse_rules() {
        let mut h = HeaderMap::new();
        assert!(accept_allows_sse(&h));

        h.insert(
            http::header::ACCEPT,
            HeaderValue::from_static("application/json"),
        );
        assert!(!accept_allows_sse(&h));

        h.insert(
            http::header::ACCEPT,
            HeaderValue::from_static("application/json, text/event-stream;q=0.9"),
        );
        assert!(accept_allows_sse(&h));

        h.insert(http::header::ACCEPT, HeaderValue::from_static("*/*"));
        assert!(accept_allows_sse(&h));

        h.insert(
            http::header::ACCEPT,
            HeaderValue::from_static("text/event-stream;q=0"),
        );
        assert!(!accept_allows_sse(&h));
    }
}

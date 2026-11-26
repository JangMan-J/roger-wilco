//! SEP-2322 Multi Round-Trip Requests (MRTR): HMAC-SHA256 `requestState`
//! integrity/claim verification and `InputRequiredResult` construction.
//! Deprecated `sampling/createMessage` and `roots/list` requests (SEP-2577)
//! are refused; only `elicitation/create` is permitted under `2026-07-28`.

use crate::mcp::RpcErr;
use roger_wire::sha256::Sha256;
use roger_wire::wire::same_secret_bytes;
use serde_json::{Map, Value};

const URL_B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

pub fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    let mut k_block = [0u8; 64];
    if key.len() > 64 {
        let mut kh = Sha256::new();
        kh.update(key);
        let digest = kh.finalize();
        k_block[..32].copy_from_slice(&digest);
    } else {
        k_block[..key.len()].copy_from_slice(key);
    }

    let mut ipad = [0x36u8; 64];
    let mut opad = [0x5cu8; 64];
    for i in 0..64 {
        ipad[i] ^= k_block[i];
        opad[i] ^= k_block[i];
    }

    let mut inner = Sha256::new();
    inner.update(&ipad);
    inner.update(msg);
    let inner_digest = inner.finalize();

    let mut outer = Sha256::new();
    outer.update(&opad);
    outer.update(&inner_digest);
    outer.finalize()
}

fn base64url_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    let mut i = 0;
    while i < bytes.len() {
        let b0 = bytes[i];
        let b1 = if i + 1 < bytes.len() { bytes[i + 1] } else { 0 };
        let b2 = if i + 2 < bytes.len() { bytes[i + 2] } else { 0 };
        out.push(URL_B64[(b0 >> 2) as usize] as char);
        out.push(URL_B64[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        if i + 1 < bytes.len() {
            out.push(URL_B64[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char);
        }
        if i + 2 < bytes.len() {
            out.push(URL_B64[(b2 & 0x3f) as usize] as char);
        }
        i += 3;
    }
    out
}

fn base64url_decode(input: &str) -> Result<Vec<u8>, &'static str> {
    let bytes = input.as_bytes();
    if bytes.len() % 4 == 1 {
        return Err("invalid base64url length");
    }
    let val = |b: u8| -> Result<u8, &'static str> {
        match b {
            b'A'..=b'Z' => Ok(b - b'A'),
            b'a'..=b'z' => Ok(b - b'a' + 26),
            b'0'..=b'9' => Ok(b - b'0' + 52),
            b'-' => Ok(62),
            b'_' => Ok(63),
            _ => Err("invalid base64url character"),
        }
    };
    let mut out = Vec::with_capacity((bytes.len() * 3) / 4);
    let mut i = 0;
    while i < bytes.len() {
        let rem = bytes.len() - i;
        let a = val(bytes[i])?;
        let b = val(bytes[i + 1])?;
        out.push((a << 2) | (b >> 4));
        if rem > 2 {
            let c = val(bytes[i + 2])?;
            out.push((b << 4) | (c >> 2));
            if rem > 3 {
                let d = val(bytes[i + 3])?;
                out.push((c << 6) | d);
            }
        }
        i += 4;
    }
    Ok(out)
}

#[derive(Debug, Clone)]
pub struct StateClaims<'a> {
    pub method: &'a str,
    pub name: Option<&'a str>,
    pub principal: Option<&'a str>,
    pub exp_ms: Option<i64>,
}

/// Mint an integrity-protected opaque `requestState` token.
pub fn issue_request_state(secret: &[u8], claims: &StateClaims<'_>, payload: &Value) -> String {
    let mut env = Map::new();
    env.insert("method".into(), Value::String(claims.method.to_string()));
    if let Some(n) = claims.name {
        env.insert("name".into(), Value::String(n.to_string()));
    }
    if let Some(p) = claims.principal {
        env.insert("principal".into(), Value::String(p.to_string()));
    }
    if let Some(exp) = claims.exp_ms {
        env.insert("exp_ms".into(), Value::Number(exp.into()));
    }
    env.insert("payload".into(), payload.clone());
    let raw = serde_json::to_vec(&Value::Object(env)).expect("requestState envelope serializes");
    let sig = hmac_sha256(secret, &raw);
    format!("{}.{}", base64url_encode(&raw), base64url_encode(&sig))
}

/// Verify and decode an attacker-controlled `requestState` token.
pub fn verify_request_state(
    secret: &[u8],
    token: &str,
    expected_method: &str,
    expected_name: Option<&str>,
    expected_principal: Option<&str>,
    now_ms: i64,
) -> Result<Value, RpcErr> {
    let Some((encoded_body, encoded_sig)) = token.split_once('.') else {
        return Err(RpcErr::invalid_params("invalid requestState format"));
    };
    if encoded_body.is_empty() || encoded_sig.is_empty() || encoded_sig.contains('.') {
        return Err(RpcErr::invalid_params("invalid requestState format"));
    }
    let body = base64url_decode(encoded_body).map_err(RpcErr::invalid_params)?;
    let sig = base64url_decode(encoded_sig).map_err(RpcErr::invalid_params)?;
    let expected_sig = hmac_sha256(secret, &body);
    if !same_secret_bytes(&sig, &expected_sig) {
        return Err(RpcErr::invalid_params("invalid requestState signature"));
    }
    let env: Value = serde_json::from_slice(&body)
        .map_err(|_| RpcErr::invalid_params("invalid requestState payload"))?;
    let Some(obj) = env.as_object() else {
        return Err(RpcErr::invalid_params("invalid requestState envelope"));
    };
    if obj.get("method").and_then(Value::as_str) != Some(expected_method) {
        return Err(RpcErr::invalid_params("requestState method mismatch"));
    }
    if obj.get("name").and_then(Value::as_str) != expected_name {
        return Err(RpcErr::invalid_params("requestState target name mismatch"));
    }
    if obj.get("principal").and_then(Value::as_str) != expected_principal {
        return Err(RpcErr::invalid_params("requestState principal mismatch"));
    }
    if let Some(exp) = obj.get("exp_ms").and_then(Value::as_i64) {
        if now_ms > exp {
            return Err(RpcErr::invalid_params("requestState expired"));
        }
    }
    Ok(obj.get("payload").cloned().unwrap_or(Value::Null))
}

pub struct ValidatedMrtr {
    pub payload: Value,
    pub input_responses: Option<Map<String, Value>>,
}

/// Validate `requestState` and `inputResponses` on an incoming MRTR-capable request.
pub fn validate_mrtr_params(
    secret: &[u8],
    params: &Value,
    method: &str,
    target_name: Option<&str>,
    principal: Option<&str>,
    now_ms: i64,
) -> Result<Option<ValidatedMrtr>, RpcErr> {
    let responses = match params.get("inputResponses") {
        None => None,
        Some(Value::Object(m)) => Some(m.clone()),
        Some(_) => {
            return Err(RpcErr::invalid_params(
                "expected inputResponses to be an object",
            ))
        }
    };
    match params.get("requestState") {
        None => {
            if responses.is_some() {
                return Err(RpcErr::invalid_params(
                    "inputResponses requires requestState",
                ));
            }
            Ok(None)
        }
        Some(Value::String(token)) => {
            let payload =
                verify_request_state(secret, token, method, target_name, principal, now_ms)?;
            Ok(Some(ValidatedMrtr {
                payload,
                input_responses: responses,
            }))
        }
        Some(_) => Err(RpcErr::invalid_params(
            "expected requestState to be a string",
        )),
    }
}

/// Construct an `InputRequiredResult` (`resultType: "input_required"`).
/// Only `elicitation/create` is permitted; deprecated `sampling/createMessage`
/// and `roots/list` (SEP-2577) are rejected.
pub fn build_input_required(
    input_requests: Option<&Map<String, Value>>,
    request_state: Option<&str>,
    client_capabilities: Option<&Value>,
    server_info_meta: Value,
) -> Result<Value, RpcErr> {
    if input_requests.is_none() && request_state.is_none() {
        return Err(RpcErr {
            code: -32603,
            message: "input_required requires inputRequests or requestState".into(),
            data: None,
        });
    }
    if let Some(reqs) = input_requests {
        for (key, spec) in reqs {
            if key.is_empty() {
                return Err(RpcErr::invalid_params(
                    "inputRequests key must be non-empty",
                ));
            }
            let req_method = spec.get("method").and_then(Value::as_str).unwrap_or("");
            if matches!(req_method, "sampling/createMessage" | "roots/list") {
                return Err(RpcErr::invalid_params(format!(
                    "deprecated MRTR input request method {req_method} is not supported in 2026-07-28"
                )));
            }
            if req_method != "elicitation/create" {
                return Err(RpcErr::invalid_params(format!(
                    "unsupported MRTR input request method {req_method}"
                )));
            }
            let has_elicitation = client_capabilities
                .and_then(Value::as_object)
                .and_then(|c| c.get("elicitation"))
                .is_some_and(Value::is_object);
            if !has_elicitation {
                return Err(RpcErr::missing_capability(serde_json::json!({
                    "elicitation": {}
                })));
            }
        }
    }
    let mut out = Map::new();
    out.insert("resultType".into(), Value::String("input_required".into()));
    if let Some(reqs) = input_requests {
        out.insert("inputRequests".into(), Value::Object(reqs.clone()));
    }
    if let Some(st) = request_state {
        out.insert("requestState".into(), Value::String(st.to_string()));
    }
    out.insert("_meta".into(), server_info_meta);
    Ok(Value::Object(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hmac_sha256_matches_rfc4231_vector() {
        // RFC 4231 Test Case 2: key = "Jefe", data = "what do ya want for nothing?"
        let mac = hmac_sha256(b"Jefe", b"what do ya want for nothing?");
        let hex: String = mac.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(
            hex,
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[test]
    fn request_state_verifies_claims_and_rejects_tampering_or_expiry() {
        let secret = b"0123456789abcdef0123456789abcdef";
        let claims = StateClaims {
            method: "tools/call",
            name: Some("roger_send"),
            principal: Some("p1"),
            exp_ms: Some(5_000),
        };
        let tok = issue_request_state(secret, &claims, &serde_json::json!({"step": 1}));
        let decoded = verify_request_state(
            secret,
            &tok,
            "tools/call",
            Some("roger_send"),
            Some("p1"),
            4_000,
        )
        .unwrap();
        assert_eq!(decoded, serde_json::json!({"step": 1}));

        // Wrong principal, expired, wrong tool, or wrong secret all fail with -32602
        assert_eq!(
            verify_request_state(
                secret,
                &tok,
                "tools/call",
                Some("roger_send"),
                Some("p2"),
                4_000
            )
            .unwrap_err()
            .code,
            -32602
        );
        assert_eq!(
            verify_request_state(
                secret,
                &tok,
                "tools/call",
                Some("roger_send"),
                Some("p1"),
                6_000
            )
            .unwrap_err()
            .code,
            -32602
        );
        assert_eq!(
            verify_request_state(
                b"wrong",
                &tok,
                "tools/call",
                Some("roger_send"),
                Some("p1"),
                4_000
            )
            .unwrap_err()
            .code,
            -32602
        );
    }

    #[test]
    fn input_required_rejects_deprecated_sampling_and_roots_and_checks_elicitation_cap() {
        let mut bad = Map::new();
        bad.insert(
            "q".into(),
            serde_json::json!({"method": "sampling/createMessage", "params": {}}),
        );
        assert_eq!(
            build_input_required(
                Some(&bad),
                Some("st"),
                Some(&serde_json::json!({"elicitation": {"form": {}}})),
                serde_json::json!({}),
            )
            .unwrap_err()
            .code,
            -32602
        );

        let mut ok_req = Map::new();
        ok_req.insert(
            "q".into(),
            serde_json::json!({"method": "elicitation/create", "params": {"message": "confirm"}}),
        );
        assert_eq!(
            build_input_required(
                Some(&ok_req),
                Some("st"),
                Some(&serde_json::json!({})),
                serde_json::json!({}),
            )
            .unwrap_err()
            .code,
            -32021
        );
        let res = build_input_required(
            Some(&ok_req),
            Some("st"),
            Some(&serde_json::json!({"elicitation": {"form": {}}})),
            serde_json::json!({}),
        )
        .unwrap();
        assert_eq!(res["resultType"], "input_required");
    }
}

//! The roster: the keys a reader already trusts, from its authority.
//!
//! ```json
//! {"scheme":"roger-roster/1",
//!  "authorities":{"<kid>":"<base64 Ed25519 public key>"},
//!  "signers":{"<Address>":{"<kid>":"<base64 Ed25519 public key>"}}}
//! ```
//!
//! `authorities` verify `roger-declare/1` texts (their `kid`); `signers` map
//! the exact Address a Mail names in `from` to the keys whose `kid` header
//! may sign its Mail. Addresses are matched as whole strings only.

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use ed25519_dalek::VerifyingKey;
use serde_json::Value;
use std::collections::HashMap;

pub const ROSTER_SCHEME: &str = "roger-roster/1";

#[derive(Debug, Default)]
pub struct Roster {
    pub authorities: HashMap<String, VerifyingKey>,
    pub signers: HashMap<String, HashMap<String, VerifyingKey>>,
}

fn key(where_: &str, v: &Value) -> Result<VerifyingKey, String> {
    let text = v
        .as_str()
        .ok_or_else(|| format!("roster: expected {where_} to be a base64 string, saw {v}"))?;
    let bytes = STANDARD
        .decode(text)
        .map_err(|e| format!("roster: expected {where_} in standard base64, saw {e}"))?;
    let arr: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
        format!(
            "roster: expected {where_} to be 32 bytes, saw {}",
            bytes.len()
        )
    })?;
    VerifyingKey::from_bytes(&arr)
        .map_err(|e| format!("roster: expected {where_} to be an Ed25519 public key, saw {e}"))
}

fn keys(where_: &str, v: Option<&Value>) -> Result<HashMap<String, VerifyingKey>, String> {
    let Some(v) = v else {
        return Ok(HashMap::new());
    };
    let obj = v
        .as_object()
        .ok_or_else(|| format!("roster: expected {where_} to be an object, saw {v}"))?;
    obj.iter()
        .map(|(kid, k)| Ok((kid.clone(), key(&format!("{where_}.{kid}"), k)?)))
        .collect()
}

/// Parse a roster file's text.
pub fn parse(text: &str) -> Result<Roster, String> {
    let v: Value = serde_json::from_str(text)
        .map_err(|e| format!("roster: expected one JSON object, saw {e}"))?;
    let obj = v
        .as_object()
        .ok_or_else(|| format!("roster: expected one JSON object, saw {v}"))?;
    match obj.get("scheme").and_then(Value::as_str) {
        Some(ROSTER_SCHEME) => {}
        other => {
            return Err(format!(
                "roster: expected scheme {ROSTER_SCHEME:?}, saw {}",
                other.map_or("none".to_string(), |s| format!("{s:?}"))
            ))
        }
    }
    let authorities = keys("authorities", obj.get("authorities"))?;
    let mut signers = HashMap::new();
    if let Some(v) = obj.get("signers") {
        let map = v
            .as_object()
            .ok_or_else(|| format!("roster: expected signers to be an object, saw {v}"))?;
        for (address, ks) in map {
            signers.insert(
                address.clone(),
                keys(&format!("signers[{address:?}]"), Some(ks))?,
            );
        }
    }
    Ok(Roster {
        authorities,
        signers,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pk() -> String {
        let sk = ed25519_dalek::SigningKey::from_bytes(&[7; 32]);
        STANDARD.encode(sk.verifying_key().as_bytes())
    }

    #[test]
    fn parses_authorities_and_signers() {
        let pk = pk();
        let r = parse(&format!(
            r#"{{"scheme":"roger-roster/1","authorities":{{"a":"{pk}"}},"signers":{{"[[1:L:0]]x":{{"k1":"{pk}"}}}}}}"#
        ))
        .unwrap();
        assert!(r.authorities.contains_key("a"));
        assert!(r.signers["[[1:L:0]]x"].contains_key("k1"));
    }

    #[test]
    fn refuses_unknown_scheme_and_bad_keys() {
        assert!(parse(r#"{"authorities":{}}"#)
            .unwrap_err()
            .contains("scheme"));
        let short = parse(r#"{"scheme":"roger-roster/1","authorities":{"a":"AAAA"}}"#);
        assert!(short.unwrap_err().contains("32 bytes"));
        let notb64 = parse(r#"{"scheme":"roger-roster/1","authorities":{"a":"!!"}}"#);
        assert!(notb64.unwrap_err().contains("base64"));
    }
}

//! The two canonical forms of docs/CONVENTIONS.md, built exactly as written
//! there. Where the page is silent, the most literal reading is taken; each
//! choice is listed in the module README.

use serde_json::Value;
use sha2::{Digest, Sha256};

/// The scheme a declared text must name.
pub const DECLARE_SCHEME: &str = "roger-declare/1";
/// The first line of the Mail signing form.
pub const SIG_SCHEME: &str = "roger-sig/1";

/// `roger-declare/1`: the object without `sig`, keys sorted, no whitespace,
/// UTF-8.
///
/// Read literally: the declared text is re-emitted with every whitespace
/// between tokens removed, the top-level member `sig` dropped, and the members
/// of every object (at every depth, `facts` included) sorted by the UTF-8 bytes
/// of their decoded key. Every other token (strings, escapes, numbers, `true`,
/// `false`, `null`) is kept exactly as it was written. A text that is not one
/// JSON object, or an object with a repeated key, has no canonical form.
pub fn declare_form(text: &str) -> Result<Vec<u8>, String> {
    serde_json::from_str::<Value>(text)
        .map_err(|e| format!("expected one JSON object, saw {e}"))?
        .as_object()
        .ok_or("expected one JSON object, saw another JSON value")?;
    let mut p = Parser {
        b: text.as_bytes(),
        i: 0,
    };
    let Raw::Obj(mut members) = p.value()? else {
        return Err("expected one JSON object".into());
    };
    members.retain(|(k, _, _)| k != "sig");
    let mut out = Vec::new();
    Raw::Obj(members).write(&mut out)?;
    Ok(out)
}

/// A JSON value with every scalar token kept as written.
enum Raw {
    Token(Vec<u8>),
    Arr(Vec<Raw>),
    /// Decoded key, the key token as written, the value.
    Obj(Vec<(String, Vec<u8>, Raw)>),
}

impl Raw {
    fn write(self, out: &mut Vec<u8>) -> Result<(), String> {
        match self {
            Raw::Token(t) => out.extend_from_slice(&t),
            Raw::Arr(xs) => {
                out.push(b'[');
                for (i, x) in xs.into_iter().enumerate() {
                    if i > 0 {
                        out.push(b',');
                    }
                    x.write(out)?;
                }
                out.push(b']');
            }
            Raw::Obj(mut ms) => {
                ms.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
                if let Some(w) = ms.windows(2).find(|w| w[0].0 == w[1].0) {
                    return Err(format!("expected unique keys, saw {:?} twice", w[0].0));
                }
                out.push(b'{');
                for (i, (_, key, v)) in ms.into_iter().enumerate() {
                    if i > 0 {
                        out.push(b',');
                    }
                    out.extend_from_slice(&key);
                    out.push(b':');
                    v.write(out)?;
                }
                out.push(b'}');
            }
        }
        Ok(())
    }
}

/// A tokenizer over text serde_json has already accepted as JSON.
struct Parser<'t> {
    b: &'t [u8],
    i: usize,
}

impl Parser<'_> {
    fn ws(&mut self) {
        while self.i < self.b.len() && matches!(self.b[self.i], b' ' | b'\t' | b'\n' | b'\r') {
            self.i += 1;
        }
    }

    fn peek(&mut self) -> Result<u8, String> {
        self.ws();
        self.b
            .get(self.i)
            .copied()
            .ok_or_else(|| "expected more JSON, saw the end".to_string())
    }

    fn string(&mut self) -> Vec<u8> {
        let start = self.i;
        self.i += 1;
        while self.i < self.b.len() && self.b[self.i] != b'"' {
            self.i += if self.b[self.i] == b'\\' { 2 } else { 1 };
        }
        self.i += 1;
        self.b[start..self.i.min(self.b.len())].to_vec()
    }

    fn value(&mut self) -> Result<Raw, String> {
        match self.peek()? {
            b'{' => {
                self.i += 1;
                let mut ms = Vec::new();
                if self.peek()? == b'}' {
                    self.i += 1;
                    return Ok(Raw::Obj(ms));
                }
                loop {
                    self.peek()?;
                    let key = self.string();
                    let decoded: String = serde_json::from_slice(&key)
                        .map_err(|e| format!("expected a key string, saw {e}"))?;
                    self.peek()?;
                    self.i += 1; // ':'
                    let v = self.value()?;
                    ms.push((decoded, key, v));
                    let c = self.peek()?;
                    self.i += 1;
                    if c == b'}' {
                        return Ok(Raw::Obj(ms));
                    }
                }
            }
            b'[' => {
                self.i += 1;
                let mut xs = Vec::new();
                if self.peek()? == b']' {
                    self.i += 1;
                    return Ok(Raw::Arr(xs));
                }
                loop {
                    xs.push(self.value()?);
                    let c = self.peek()?;
                    self.i += 1;
                    if c == b']' {
                        return Ok(Raw::Arr(xs));
                    }
                }
            }
            b'"' => Ok(Raw::Token(self.string())),
            _ => {
                let start = self.i;
                while self.i < self.b.len()
                    && !matches!(
                        self.b[self.i],
                        b',' | b'}' | b']' | b' ' | b'\t' | b'\n' | b'\r'
                    )
                {
                    self.i += 1;
                }
                Ok(Raw::Token(self.b[start..self.i].to_vec()))
            }
        }
    }
}

/// `roger-sig/1`: seven lines joined by `\n`, no trailing newline. Every part
/// is used verbatim, as the send and the headers carry it.
pub fn sig_form(
    from: &str,
    to: &str,
    mail: &str,
    seq: &str,
    sent_at_ms: &str,
    body_sha256: &str,
) -> Vec<u8> {
    [SIG_SCHEME, from, to, mail, seq, sent_at_ms, body_sha256]
        .join("\n")
        .into_bytes()
}

/// Lowercase hex SHA-256 of `bytes`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn form(text: &str) -> String {
        String::from_utf8(declare_form(text).unwrap()).unwrap()
    }

    #[test]
    fn declare_form_drops_sig_and_sorts_keys_at_every_depth() {
        let text = r#"{ "sig":"x", "scheme":"roger-declare/1",
                 "facts":{"z":"1","a":{"y":2,"b":3}, "l":[ 3, {"q":1,"p":2} ]},
                 "address":"[[3:B:1]]u", "nonce":"n", "issued_at_ms":17, "kid":"k" }"#;
        assert_eq!(
            form(text),
            r#"{"address":"[[3:B:1]]u","facts":{"a":{"b":3,"y":2},"l":[3,{"p":2,"q":1}],"z":"1"},"issued_at_ms":17,"kid":"k","nonce":"n","scheme":"roger-declare/1"}"#
        );
    }

    #[test]
    fn declare_form_keeps_every_token_as_written() {
        let text = r#"{"b" : "\u00e9 \/ \" x" ,"a":1.50,"c":1E3,"d":-0,"e":[true, null,false]}"#;
        assert_eq!(
            form(text),
            r#"{"a":1.50,"b":"\u00e9 \/ \" x","c":1E3,"d":-0,"e":[true,null,false]}"#
        );
    }

    #[test]
    fn declare_form_sorts_by_decoded_key_bytes() {
        let text = r#"{"b":1,"B":2,"a":3,"\u00e9":4,"\u0061a":5}"#;
        assert_eq!(form(text), r#"{"B":2,"a":3,"\u0061a":5,"b":1,"\u00e9":4}"#);
    }

    #[test]
    fn declare_form_drops_only_the_top_level_sig() {
        assert_eq!(form(r#"{"f":{"sig":1},"sig":2}"#), r#"{"f":{"sig":1}}"#);
    }

    #[test]
    fn declare_form_refuses_non_objects_and_repeated_keys() {
        assert!(declare_form(r#""text""#).is_err());
        assert!(declare_form("[1]").is_err());
        assert!(declare_form("{").is_err());
        assert!(declare_form(r#"{"a":1,"\u0061":2}"#).is_err());
        assert!(declare_form(r#"{"f":{"a":1,"a":2}}"#).is_err());
    }

    #[test]
    fn sig_form_is_seven_lines_without_trailing_newline() {
        let form = sig_form("a", "*", "m1", "3", "1700", "ab");
        assert_eq!(form, b"roger-sig/1\na\n*\nm1\n3\n1700\nab");
    }

    #[test]
    fn sha256_hex_is_lowercase() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}

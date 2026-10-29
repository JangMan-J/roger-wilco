//! A Mail's headers: string keys to string values that a sender attaches and
//! the core carries verbatim. This is the one place that bounds them; the
//! coordinator calls it before anything is committed and every other hop
//! forwards the set untouched. Nothing here reads a key or a value beyond its
//! size.

use crate::json::Value;

pub const MAX_HEADERS: usize = 32;
pub const MAX_KEY_BYTES: usize = 128;
pub const MAX_VALUE_BYTES: usize = 4096;

fn kind_of(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Int(_) => "a number",
        Value::Str(_) => "a string",
        Value::Arr(_) => "an array",
        Value::Obj(_) => "an object",
    }
}

/// The headers a send carries, checked against the bounds. `None` (no
/// `headers` field) is the empty set. Returns the set unchanged, key order
/// included, or a reason that says what was expected and what was seen.
pub fn validate(headers: Option<&Value>) -> Result<Value, String> {
    let Some(v) = headers else {
        return Ok(Value::Obj(Vec::new()));
    };
    let Value::Obj(pairs) = v else {
        return Err(format!(
            "headers: expected an object of string values, saw {}",
            kind_of(v)
        ));
    };
    if pairs.len() > MAX_HEADERS {
        return Err(format!(
            "headers: expected at most {MAX_HEADERS} headers, saw {}",
            pairs.len()
        ));
    }
    for (i, (key, value)) in pairs.iter().enumerate() {
        if key.is_empty() || key.len() > MAX_KEY_BYTES {
            return Err(format!(
                "headers: expected a key of 1 to {MAX_KEY_BYTES} bytes, saw a key of {} bytes",
                key.len()
            ));
        }
        let Value::Str(text) = value else {
            return Err(format!(
                "headers: expected an object of string values, saw {} for key {key:?}",
                kind_of(value)
            ));
        };
        if text.len() > MAX_VALUE_BYTES {
            return Err(format!(
                "header {key:?}: expected a value of at most {MAX_VALUE_BYTES} bytes, saw {}",
                text.len()
            ));
        }
        if pairs[..i].iter().any(|(earlier, _)| earlier == key) {
            return Err(format!("headers: duplicate key {key:?}"));
        }
    }
    Ok(v.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json::{parse, s};

    fn set(n: usize) -> Value {
        Value::Obj((0..n).map(|i| (format!("k{i}"), s("v"))).collect())
    }

    #[test]
    fn absent_is_empty_and_a_valid_set_is_unchanged() {
        assert_eq!(validate(None), Ok(Value::Obj(Vec::new())));
        let v = parse(r#"{"z":"1","a":"","m":"héllo \"q\"\n"}"#).unwrap();
        assert_eq!(validate(Some(&v)), Ok(v));
    }

    #[test]
    fn the_largest_allowed_set_passes() {
        let v = Value::Obj(
            (0..MAX_HEADERS)
                .map(|i| (format!("{i:0>128}"), s(&"x".repeat(MAX_VALUE_BYTES))))
                .collect(),
        );
        assert_eq!(validate(Some(&v)), Ok(v));
    }

    #[test]
    fn too_many_headers() {
        assert_eq!(
            validate(Some(&set(33))),
            Err("headers: expected at most 32 headers, saw 33".into())
        );
        assert!(validate(Some(&set(32))).is_ok());
    }

    #[test]
    fn value_and_key_sizes_count_bytes() {
        let big = Value::Obj(vec![("k".into(), s(&"x".repeat(5000)))]);
        assert_eq!(
            validate(Some(&big)),
            Err("header \"k\": expected a value of at most 4096 bytes, saw 5000".into())
        );
        // 2049 two-byte characters is 4098 bytes.
        let wide = Value::Obj(vec![("k".into(), s(&"é".repeat(2049)))]);
        assert!(validate(Some(&wide)).unwrap_err().ends_with("saw 4098"));
        let long_key = Value::Obj(vec![("k".repeat(129), s("v"))]);
        assert!(validate(Some(&long_key))
            .unwrap_err()
            .ends_with("saw a key of 129 bytes"));
        let no_key = Value::Obj(vec![(String::new(), s("v"))]);
        assert!(validate(Some(&no_key))
            .unwrap_err()
            .contains("1 to 128 bytes"));
    }

    #[test]
    fn not_an_object_of_strings() {
        assert_eq!(
            validate(Some(&Value::Arr(vec![]))),
            Err("headers: expected an object of string values, saw an array".into())
        );
        assert!(validate(Some(&Value::Null))
            .unwrap_err()
            .ends_with("saw null"));
        assert!(validate(Some(&s("x")))
            .unwrap_err()
            .ends_with("saw a string"));
        let num = Value::Obj(vec![("n".into(), Value::Int(1))]);
        assert_eq!(
            validate(Some(&num)),
            Err("headers: expected an object of string values, saw a number for key \"n\"".into())
        );
    }

    #[test]
    fn duplicate_keys_are_refused() {
        let v = parse(r#"{"a":"1","b":"2","a":"3"}"#).unwrap();
        assert_eq!(
            validate(Some(&v)),
            Err("headers: duplicate key \"a\"".into())
        );
    }
}

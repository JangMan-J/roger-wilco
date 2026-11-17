//! Minimal JSON codec for the shapes this program emits and accepts.
//! Objects keep insertion order. Numbers are integers only.

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Str(String),
    Arr(Vec<Value>),
    Obj(Vec<(String, Value)>),
}

impl Value {
    pub fn get(&self, key: &str) -> Option<&Value> {
        match self {
            Value::Obj(pairs) => pairs.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(n) => Some(*n),
            _ => None,
        }
    }
}

pub fn s(v: &str) -> Value {
    Value::Str(v.to_string())
}

pub fn obj(pairs: Vec<(&str, Value)>) -> Value {
    Value::Obj(pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
}

/// The deepest nesting `parse` accepts. Every message and log line of this
/// program nests a few levels; the bound keeps a line of nothing but opening
/// brackets from running the parser out of stack.
pub const MAX_DEPTH: usize = 64;

pub fn parse(text: &str) -> Result<Value, String> {
    let chars: Vec<char> = text.chars().collect();
    let mut p = Parser {
        chars: &chars,
        i: 0,
        depth: 0,
    };
    p.ws();
    let v = p.value()?;
    p.ws();
    if p.i != p.chars.len() {
        return Err(format!("trailing input at {}", p.i));
    }
    Ok(v)
}

struct Parser<'a> {
    chars: &'a [char],
    i: usize,
    /// How many objects and arrays enclose the value being read.
    depth: usize,
}

impl Parser<'_> {
    fn ws(&mut self) {
        while self.i < self.chars.len() && self.chars[self.i].is_whitespace() {
            self.i += 1;
        }
    }

    fn peek(&self) -> Result<char, String> {
        self.chars
            .get(self.i)
            .copied()
            .ok_or_else(|| "unexpected end".to_string())
    }

    fn value(&mut self) -> Result<Value, String> {
        self.ws();
        match self.peek()? {
            '{' => self.nested(Self::object),
            '[' => self.nested(Self::array),
            '"' => Ok(Value::Str(self.string()?)),
            't' => self.lit("true", Value::Bool(true)),
            'f' => self.lit("false", Value::Bool(false)),
            'n' => self.lit("null", Value::Null),
            '-' | '0'..='9' => {
                let start = self.i;
                if self.chars[self.i] == '-' {
                    self.i += 1;
                }
                while self.i < self.chars.len() && self.chars[self.i].is_ascii_digit() {
                    self.i += 1;
                }
                let text: String = self.chars[start..self.i].iter().collect();
                text.parse::<i64>()
                    .map(Value::Int)
                    .map_err(|e| e.to_string())
            }
            c => Err(format!("unexpected {c}")),
        }
    }

    /// Read one object or array, counted against `MAX_DEPTH`.
    fn nested(&mut self, read: fn(&mut Self) -> Result<Value, String>) -> Result<Value, String> {
        if self.depth == MAX_DEPTH {
            return Err(format!(
                "expected nesting of at most {MAX_DEPTH} levels, saw a deeper value at {}",
                self.i
            ));
        }
        self.depth += 1;
        let v = read(self);
        self.depth -= 1;
        v
    }

    fn object(&mut self) -> Result<Value, String> {
        self.i += 1;
        let mut pairs = Vec::new();
        self.ws();
        if self.peek()? == '}' {
            self.i += 1;
            return Ok(Value::Obj(pairs));
        }
        loop {
            self.ws();
            let Value::Str(k) = self.value()? else {
                return Err("object key must be string".into());
            };
            self.ws();
            if self.peek()? != ':' {
                return Err("expected :".into());
            }
            self.i += 1;
            let v = self.value()?;
            pairs.push((k, v));
            self.ws();
            match self.peek()? {
                ',' => self.i += 1,
                '}' => {
                    self.i += 1;
                    return Ok(Value::Obj(pairs));
                }
                c => return Err(format!("expected , or }} got {c}")),
            }
        }
    }

    fn array(&mut self) -> Result<Value, String> {
        self.i += 1;
        let mut xs = Vec::new();
        self.ws();
        if self.peek()? == ']' {
            self.i += 1;
            return Ok(Value::Arr(xs));
        }
        loop {
            xs.push(self.value()?);
            self.ws();
            match self.peek()? {
                ',' => self.i += 1,
                ']' => {
                    self.i += 1;
                    return Ok(Value::Arr(xs));
                }
                c => return Err(format!("expected , or ] got {c}")),
            }
        }
    }

    fn lit(&mut self, word: &str, v: Value) -> Result<Value, String> {
        for c in word.chars() {
            if self.peek()? != c {
                return Err(format!("bad literal, expected {word}"));
            }
            self.i += 1;
        }
        Ok(v)
    }

    fn string(&mut self) -> Result<String, String> {
        if self.peek()? != '"' {
            return Err("expected string".into());
        }
        self.i += 1;
        let mut out = String::new();
        loop {
            let c = self.peek()?;
            self.i += 1;
            match c {
                '"' => return Ok(out),
                '\\' => {
                    let e = self.peek()?;
                    self.i += 1;
                    match e {
                        '"' => out.push('"'),
                        '\\' => out.push('\\'),
                        '/' => out.push('/'),
                        'b' => out.push('\u{8}'),
                        'f' => out.push('\u{c}'),
                        'n' => out.push('\n'),
                        'r' => out.push('\r'),
                        't' => out.push('\t'),
                        'u' => {
                            let hi = self.hex4()?;
                            if (0xD800..0xDC00).contains(&hi) {
                                if self.peek()? != '\\' {
                                    return Err("lone surrogate".into());
                                }
                                self.i += 1;
                                if self.peek()? != 'u' {
                                    return Err("lone surrogate".into());
                                }
                                self.i += 1;
                                let lo = self.hex4()?;
                                if !(0xDC00..0xE000).contains(&lo) {
                                    return Err(format!("expected low surrogate, got {lo:04x}"));
                                }
                                let cp = 0x10000 + ((hi - 0xD800) << 10) + (lo - 0xDC00);
                                out.push(char::from_u32(cp).ok_or("bad codepoint")?);
                            } else if (0xDC00..0xE000).contains(&hi) {
                                return Err("lone low surrogate".into());
                            } else {
                                out.push(char::from_u32(hi).ok_or("bad codepoint")?);
                            }
                        }
                        other => return Err(format!("bad escape {other}")),
                    }
                }
                other => out.push(other),
            }
        }
    }

    fn hex4(&mut self) -> Result<u32, String> {
        let mut n: u32 = 0;
        for _ in 0..4 {
            let c = self.peek()?;
            n = n * 16 + c.to_digit(16).ok_or("bad hex")?;
            self.i += 1;
        }
        Ok(n)
    }
}

pub fn stringify(v: &Value) -> String {
    let mut out = String::new();
    write_value(v, &mut out);
    out
}

fn write_value(v: &Value, out: &mut String) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Int(n) => out.push_str(&n.to_string()),
        Value::Str(s) => write_string(s, out),
        Value::Arr(xs) => {
            out.push('[');
            for (i, x) in xs.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_value(x, out);
            }
            out.push(']');
        }
        Value::Obj(pairs) => {
            out.push('{');
            for (i, (k, val)) in pairs.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_string(k, out);
                out.push(':');
                write_value(val, out);
            }
            out.push('}');
        }
    }
}

fn write_string(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_object_with_escapes_and_unicode() {
        let v = obj(vec![
            ("type", s("send")),
            ("req", Value::Int(7)),
            (
                "body",
                s("a \"quoted\" line\nsecond\t[[153:Meteor Shower:7]] ☃"),
            ),
            ("ok", Value::Bool(true)),
            ("none", Value::Null),
        ]);
        let text = stringify(&v);
        assert!(!text.contains('\n'), "wire form must be one line: {text}");
        assert_eq!(parse(&text).unwrap(), v);
    }

    #[test]
    fn parses_foreign_json() {
        let v = parse(r#"{"type":"ack","req":-3,"body":"café","xs":[1,2]}"#).unwrap();
        assert_eq!(v.get("type").unwrap().as_str(), Some("ack"));
        assert_eq!(v.get("req").unwrap().as_i64(), Some(-3));
        assert_eq!(v.get("body").unwrap().as_str(), Some("café"));
    }

    #[test]
    fn decodes_surrogate_pairs() {
        for (text, expected) in [
            (r#""\ud800\udc00""#, "\u{10000}"),
            (r#""\uD83D\uDE00""#, "😀"),
            (r#""\udbff\udfff""#, "\u{10ffff}"),
            (r#""\u00e9\u2603""#, "é☃"),
        ] {
            assert_eq!(parse(text).unwrap(), s(expected));
        }
    }

    #[test]
    fn rejects_malformed_surrogate_escapes() {
        for text in [
            r#""\ud800\u0041""#,
            r#""\ud800\ud800""#,
            r#""\ud800\udbff""#,
            r#""\ud800\ue000""#,
            r#""\ud800\uffff""#,
            r#""\ud800""#,
            r#""\ud800x""#,
            r#""\ud800\n""#,
            r#""\ud800\uZZZZ""#,
            r#""\ud800\udc0""#,
            r#""\ud800\u"#,
            r#""\ud800\"#,
            r#""\ud800"#,
            r#""\udc00""#,
            r#""\udfff""#,
            r#""\udc00\ud800""#,
            r#""\udbff\u0000""#,
            r#"{"type":"board","x":"\ud800\u0041"}"#,
            r#"{"body":"\ud83d\u00e9"}"#,
        ] {
            let got = std::panic::catch_unwind(|| parse(text));
            assert!(
                matches!(got, Ok(Err(_))),
                "expected error, not a panic, for {text}: {got:?}"
            );
        }
    }

    #[test]
    fn nesting_is_accepted_to_the_bound_and_refused_past_it_without_a_crash() {
        let nest = |n: usize| format!("{}{}", "[".repeat(n), "]".repeat(n));
        assert!(parse(&nest(MAX_DEPTH)).is_ok());
        let e = parse(&nest(MAX_DEPTH + 1)).unwrap_err();
        assert!(e.contains("expected nesting of at most 64 levels"), "{e}");
        // Mixed, unclosed and very deep: each is an error, none a stack overflow.
        for text in [
            "[".repeat(400_000),
            "{\"a\":".repeat(400_000),
            format!("{}1{}", "[{\"k\":".repeat(200_000), "}]".repeat(200_000)),
        ] {
            assert!(parse(&text).is_err());
        }
        // Siblings do not count: depth is how many containers enclose a value.
        let wide = format!("[{}]", vec!["[[1]]"; 1000].join(","));
        assert!(parse(&wide).is_ok());
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse("{not json").is_err());
        assert!(parse("").is_err());
        assert!(parse("{} trailing").is_err());
    }
}

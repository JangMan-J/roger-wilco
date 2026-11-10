//! Claims: exclusive ownership of one key by one session, renewed by that
//! session alone, freed by release or by TTL expiry.
//!
//! Expiry is never found by a scan. Each grant records an absolute deadline,
//! and every read and every decision compares that deadline with the time it
//! is given. A held entry past its deadline is simply not live; the next
//! acquire overwrites it. Keys are opaque strings, stored and returned
//! verbatim.

use crate::json::{self, obj, s, Value};
use crate::net;
use std::collections::BTreeMap;
use std::io;

#[derive(Clone, Debug, PartialEq)]
pub struct Held {
    pub owner: String,
    /// Absolute deadline in coordinator milliseconds since the Epoch.
    pub expires_ms: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Op {
    Acquire,
    Renew,
    Release,
}

impl Op {
    pub fn parse(s: &str) -> Option<Op> {
        match s {
            "acquire" => Some(Op::Acquire),
            "renew" => Some(Op::Renew),
            "release" => Some(Op::Release),
            _ => None,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Op::Acquire => "acquire",
            Op::Renew => "renew",
            Op::Release => "release",
        }
    }
}

/// What a decision does to stored state. The caller makes it durable, then
/// applies it, then answers.
#[derive(Clone, Debug, PartialEq)]
pub enum Change {
    Grant(Held),
    Release,
}

impl Change {
    /// The committed event for this change on `key`: the one line the
    /// coordinator appends to its log, and the one `from_event` reads back.
    pub fn to_event(&self, key: &str) -> Value {
        match self {
            Change::Grant(h) => obj(vec![
                ("ev", s("claim")),
                ("key", s(key)),
                ("owner", s(&h.owner)),
                ("expires_ms", Value::Int(h.expires_ms)),
            ]),
            Change::Release => obj(vec![("ev", s("release")), ("key", s(key))]),
        }
    }

    /// Decode a `claim` or `release` event. A missing field is an
    /// `InvalidData` error naming the event, never a silently dropped grant:
    /// replaying it would give the key to nobody while the log says otherwise.
    pub fn from_event(v: &Value) -> io::Result<(String, Change)> {
        let bad = |what: &str| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected {what}, saw log line {}", json::stringify(v)),
            )
        };
        let key = net::field(v, "key")
            .map_err(|_| bad("a claim or release event with key"))?
            .to_string();
        match v.get("ev").and_then(|x| x.as_str()) {
            Some("claim") => {
                let owner = net::field(v, "owner")
                    .map_err(|_| bad("a claim event with owner"))?
                    .to_string();
                let expires_ms = v
                    .get("expires_ms")
                    .and_then(|x| x.as_i64())
                    .ok_or_else(|| bad("a claim event with integer expires_ms"))?;
                Ok((key, Change::Grant(Held { owner, expires_ms })))
            }
            Some("release") => Ok((key, Change::Release)),
            _ => Err(bad("ev claim or release")),
        }
    }
}

/// The answer to one request: the change to commit (if any), and the live
/// holder of the key once that change is applied (`None` when free).
#[derive(Debug, PartialEq)]
pub struct Decision {
    pub ok: bool,
    pub change: Option<Change>,
    pub holder: Option<Held>,
}

#[derive(Default)]
pub struct Claims {
    held: BTreeMap<String, Held>,
}

impl Claims {
    /// The holder of `key` at `now`, if its deadline has not passed.
    pub fn live(&self, key: &str, now: i64) -> Option<&Held> {
        self.held.get(key).filter(|h| now < h.expires_ms)
    }

    /// Decide `op` on `key` for `owner` at `now`. Acquire succeeds when the key
    /// is free, expired, or already the owner's. Renew and release succeed
    /// only for the live holder: a session that lost its claim to expiry
    /// cannot renew it back.
    ///
    /// Both grants set the deadline to `now + ttl_ms`, nothing else: an
    /// acquire by the current holder is a retry-safe refresh that resets the
    /// deadline, and a renew sets the deadline even when that shortens it.
    /// A client that lost the reply can therefore repeat either request and
    /// land in the same state.
    pub fn decide(&self, op: &Op, key: &str, owner: &str, ttl_ms: i64, now: i64) -> Decision {
        let live = self.live(key, now).cloned();
        let mine = live.as_ref().is_some_and(|h| h.owner == owner);
        let allowed = match op {
            Op::Acquire => live.is_none() || mine,
            Op::Renew | Op::Release => mine,
        };
        if !allowed {
            return Decision {
                ok: false,
                change: None,
                holder: live,
            };
        }
        match op {
            Op::Release => Decision {
                ok: true,
                change: Some(Change::Release),
                holder: None,
            },
            Op::Acquire | Op::Renew => {
                let held = Held {
                    owner: owner.to_string(),
                    expires_ms: now.saturating_add(ttl_ms),
                };
                Decision {
                    ok: true,
                    change: Some(Change::Grant(held.clone())),
                    holder: Some(held),
                }
            }
        }
    }

    /// Apply a committed change, whether just decided or replayed from the log.
    pub fn apply(&mut self, key: &str, change: &Change) {
        match change {
            Change::Grant(held) => {
                self.held.insert(key.to_string(), held.clone());
            }
            Change::Release => {
                self.held.remove(key);
            }
        }
    }

    /// Drop every claim whose deadline has passed at `now`. Nothing answers
    /// differently for it at `now` or later; a rotation does this so memory
    /// holds what its fresh log restates.
    pub fn forget_expired(&mut self, now: i64) {
        self.held.retain(|_, h| now < h.expires_ms);
    }

    /// Every live claim at `now`, in key order.
    pub fn active(&self, now: i64) -> Vec<(&str, &Held)> {
        self.held
            .iter()
            .filter(|(_, h)| now < h.expires_ms)
            .map(|(k, h)| (k.as_str(), h))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "[[153:Meteor Shower:7]]";

    fn commit(c: &mut Claims, op: Op, owner: &str, ttl: i64, now: i64) -> Decision {
        let d = c.decide(&op, KEY, owner, ttl, now);
        if let Some(ch) = &d.change {
            c.apply(KEY, ch);
        }
        d
    }

    #[test]
    fn second_owner_is_refused_and_told_the_holder() {
        let mut c = Claims::default();
        assert!(commit(&mut c, Op::Acquire, "a", 1000, 0).ok);
        let d = commit(&mut c, Op::Acquire, "b", 1000, 10);
        assert!(!d.ok);
        assert_eq!(d.change, None);
        assert_eq!(d.holder.unwrap().owner, "a");
    }

    #[test]
    fn only_the_holder_renews_and_renewal_moves_the_deadline() {
        let mut c = Claims::default();
        commit(&mut c, Op::Acquire, "a", 1000, 0);
        assert!(!commit(&mut c, Op::Renew, "b", 1000, 500).ok);
        assert!(commit(&mut c, Op::Renew, "a", 1000, 500).ok);
        assert_eq!(c.live(KEY, 1200).unwrap().owner, "a");
        assert!(c.live(KEY, 1500).is_none());
    }

    #[test]
    fn expiry_frees_the_key_without_any_sweep() {
        let mut c = Claims::default();
        commit(&mut c, Op::Acquire, "a", 1000, 0);
        assert!(c.live(KEY, 999).is_some());
        assert!(c.live(KEY, 1000).is_none());
        assert!(c.active(1000).is_empty());
        assert!(!commit(&mut c, Op::Renew, "a", 1000, 1000).ok);
        assert!(commit(&mut c, Op::Acquire, "b", 1000, 1000).ok);
        assert_eq!(c.active(1001), vec![(KEY, c.live(KEY, 1001).unwrap())]);
        assert_eq!(c.live(KEY, 1001).unwrap().owner, "b");
    }

    #[test]
    fn holder_reacquire_is_a_refresh_that_resets_the_deadline() {
        let mut c = Claims::default();
        commit(&mut c, Op::Acquire, "a", 1000, 0);
        let d = commit(&mut c, Op::Acquire, "a", 1000, 600);
        assert!(d.ok);
        assert_eq!(d.holder.as_ref().unwrap().expires_ms, 1600);
        assert_eq!(c.live(KEY, 1599).unwrap().owner, "a");
        assert!(c.live(KEY, 1600).is_none());
    }

    #[test]
    fn renew_sets_the_deadline_even_when_shorter() {
        let mut c = Claims::default();
        commit(&mut c, Op::Acquire, "a", 10_000, 0);
        let d = commit(&mut c, Op::Renew, "a", 100, 50);
        assert!(d.ok);
        assert_eq!(d.holder.as_ref().unwrap().expires_ms, 150);
        assert!(c.live(KEY, 149).is_some());
        assert!(c.live(KEY, 150).is_none());
    }

    #[test]
    fn events_round_trip_and_a_missing_field_fails_loudly() {
        let held = Held {
            owner: "a".into(),
            expires_ms: 1600,
        };
        for change in [Change::Grant(held), Change::Release] {
            let ev = change.to_event(KEY);
            assert_eq!(
                Change::from_event(&ev).unwrap(),
                (KEY.to_string(), change.clone())
            );
        }
        for line in [
            r#"{"ev":"claim","key":"k","owner":"a"}"#,
            r#"{"ev":"claim","key":"k","expires_ms":5}"#,
            r#"{"ev":"claim","owner":"a","expires_ms":5}"#,
            r#"{"ev":"claim","key":"k","owner":"a","expires_ms":"5"}"#,
            r#"{"ev":"release"}"#,
            r#"{"ev":"mail","key":"k"}"#,
        ] {
            let v = json::parse(line).unwrap();
            let e = Change::from_event(&v).unwrap_err();
            assert_eq!(e.kind(), io::ErrorKind::InvalidData, "{line}");
            assert!(e.to_string().contains("saw log line {"), "{line}: {e}");
        }
    }

    #[test]
    fn forgetting_expired_claims_leaves_the_live_ones_and_changes_no_answer() {
        let mut c = Claims::default();
        c.apply(
            "gone",
            &Change::Grant(Held {
                owner: "a".into(),
                expires_ms: 100,
            }),
        );
        c.apply(
            "held",
            &Change::Grant(Held {
                owner: "b".into(),
                expires_ms: 900,
            }),
        );
        let before = (c.active(500), c.decide(&Op::Acquire, "gone", "z", 10, 500));
        let before = (format!("{:?}", before.0), before.1);
        c.forget_expired(500);
        assert_eq!(format!("{:?}", c.active(500)), before.0);
        assert_eq!(c.decide(&Op::Acquire, "gone", "z", 10, 500), before.1);
        assert_eq!(c.held.len(), 1);
        assert!(c.live("held", 500).is_some());
    }

    #[test]
    fn release_is_the_holders_alone() {
        let mut c = Claims::default();
        commit(&mut c, Op::Acquire, "a", 1000, 0);
        assert!(!commit(&mut c, Op::Release, "b", 0, 1).ok);
        assert!(commit(&mut c, Op::Release, "a", 0, 1).ok);
        assert!(c.live(KEY, 2).is_none());
        assert!(commit(&mut c, Op::Acquire, "b", 1000, 2).ok);
    }
}

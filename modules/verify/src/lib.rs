//! `roger-verify`: an Optional module that reads a Roger-Wilco audit and
//! checks the consumer provenance conventions of docs/CONVENTIONS.md
//! (`roger-declare/1` declared texts, `roger-sig/1` Mail headers) against a
//! roster of keys the reader already trusts. It reads records only; it sends,
//! stores and acknowledges nothing.

pub mod canon;
pub mod roster;
pub mod verify;

#[cfg(test)]
mod tests;

//! Optional WebSocket face for Roger-Wilco: the `roger adapter` JSON-line
//! protocol, one text frame per line, one connection per session.

pub mod cli;
pub mod daemon;
pub mod durlog;
pub mod server;
pub mod session;
pub mod upgrade;

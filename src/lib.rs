//! Mango: a MongoDB-compatible document database with built-in Raft
//! replication.

pub mod aggregate;
pub mod app;
pub mod auth;
pub mod bsonutil;
pub mod commands;
pub mod error;
pub mod expr;
pub mod keystring;
pub mod projection;
pub mod query;
pub mod raft;
pub mod server;
pub mod statemachine;
pub mod storage;
pub mod update;
pub mod wire;

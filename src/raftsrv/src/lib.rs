#![feature(io_error_other)]

pub mod node;
pub mod server;
pub mod service;

pub type RaftMessage = mephisto_raft::proto::eraftpb::Message;

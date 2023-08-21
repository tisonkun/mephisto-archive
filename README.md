# Mephisto

Mephisto implements the Raft protocol where an extended version of the Raft paper is [available](https://raft.github.io/raft.pdf). The paper introduces Raft and states its motivations in following words:

> Raft is a consensus algorithm for managing a replicated log. It produces a result equivalent to (multi-)Paxos, and it is as efficient as Paxos, but its structure is different from Paxos; this makes Raft more understandable than Paxos and also provides a better foundation for building practical systems.

## License

This project is released under [Apache License, Version 2.0](https://apache.org/licenses/LICENSE-2.0).

Original sources are distributed under the same license with different copyright owner:

* The authors of `etcd-io/raft` are noted as `The etcd Authors`.
* The authors of `tikv/raft-rs` are noted as `TiKV Project Authors`.

To simplify conveying licenses, all the commits after the bootstrap one are made independently unless explict noted.

The bootstrap commit includes the following modifications:

* Replace `slog` with `tracing`.
* Replace `datadriven` with `goldenfiles`.
* Replace `rust-protobuf` and `protobuf-build` with `prost`.
* Merge `raft-proto` into `mephisto` crate.
* Stub implementations of Raft stores.

## Acknowledgement

This project is derived from [`tikv/raft-rs`](https://github.com/tikv/raft-rs). `raft-rs` is, recursively, derived from [`etcd-io/raft`](https://github.com/etcd-io/raft).

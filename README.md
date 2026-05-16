# msgbus

`msgbus` is a local-first message bus for applications that need durable publish/subscribe, replay, FIFO delivery, and node-to-node synchronization.

Each client or device runs a local `msgbusd` daemon. Applications use the Go or Rust SDK to talk to the local daemon over gRPC. The daemon persists messages to a local database, serves local subscriptions, and can synchronize missing message ranges from configured peers.

The message model is explicit: every `topic + origin node` is an ordered append log with a monotonic `head_id`. Storage is replaceable through the Rust `MsgbusStore` trait. Current storage backends are `redb` and SQLite. The public network API uses gRPC/Protobuf, and proto files are managed with `buf`.

## Deployment Model

The intended model is local-first:

```text
Application
  -> Go SDK / Rust SDK
  -> local msgbusd on 127.0.0.1
  -> local database on the client/device
```

Every client or device should run its own `msgbusd` and keep its own database. SDKs should normally connect to the local daemon. Daemon-to-daemon sync compares peer heads and replicates missing ranges between nodes.

This keeps storage state in one implementation instead of duplicating database/state-machine logic inside every Go and Rust SDK.

## Layout

```text
crates/msgbus-core        domain types, errors, MsgbusStore trait
crates/msgbus-store-redb  redb-backed persistent store
crates/msgbus-store-sqlite SQLite-backed persistent store
crates/msgbus-proto       tonic/prost generated Rust protobuf module
crates/msgbus-server      msgbusd gRPC daemon
crates/msgbus-sdk-rust    Rust SDK
go-sdk/msgbus             Go SDK plus buf-generated Go protobuf code
proto/msgbus/v1           canonical protobuf contract
packaging/systemd         systemd service template
```

## Feature Matrix

| Area | Status |
| --- | --- |
| Durable publish | Implemented with per-`topic + origin` monotonic `head_id` |
| Replay/fetch | Implemented from a requested `head_id` |
| Subscribe | Implemented as replay followed by live stream |
| Reconnecting subscribe | Implemented in Go and Rust SDKs |
| Delete | Implemented as tombstone markers replicated through `$msgbus.delete` |
| FIFO | Implemented as enqueue / peek / ack / reject with front-message ack enforcement |
| Local database | Implemented through one local `msgbusd` per client/device |
| Storage abstraction | Implemented through `MsgbusStore` |
| Storage backends | `redb` and SQLite |
| Peer sync | Implemented with peer head comparison and idempotent range fetch |
| Peer identity binding | Implemented with `--peer endpoint=tenant/bl/device` |
| Sync state inspection | Implemented with persisted lag/error state |
| Transport security | gRPC TLS and mTLS |
| Health/readiness | `Health` and `Ready` RPCs |
| Graceful shutdown | SIGINT/SIGTERM stop the server and peer sync loop |
| SDKs | Go and Rust over the same protobuf contract |
| Deployment templates | Dockerfile and systemd service |

Current non-goals for the Go/Rust API: automatic LAN discovery, dup/restore backups, consensus/quorum replication, and advanced FIFO peer routing. Peers are configured explicitly.

## Build And Test

```bash
cargo test
cargo check
cargo clippy --all-targets --all-features -- -D warnings
buf lint
buf generate
cd go-sdk/msgbus && go test ./...
```

If `buf` is missing:

```bash
GOSUMDB=off go install github.com/bufbuild/buf/cmd/buf@latest
```

## Run

Single local daemon:

```bash
cargo run -p msgbus-server --bin msgbusd -- \
  --listen 127.0.0.1:50051 \
  --data ./msgbus.redb \
  --storage redb \
  --tenant-id tenant \
  --bl-name default \
  --device-id device
```

SQLite backend:

```bash
cargo run -p msgbus-server --bin msgbusd -- \
  --listen 127.0.0.1:50051 \
  --data ./msgbus.sqlite \
  --storage sqlite \
  --tenant-id tenant \
  --bl-name default \
  --device-id device
```

Two local daemons with explicit peer sync:

```bash
cargo run -p msgbus-server --bin msgbusd -- \
  --listen 127.0.0.1:50051 \
  --data ./node-a.redb \
  --storage redb \
  --tenant-id tenant \
  --bl-name default \
  --device-id node-a \
  --peer http://127.0.0.1:50052=tenant/default/node-b
```

```bash
cargo run -p msgbus-server --bin msgbusd -- \
  --listen 127.0.0.1:50052 \
  --data ./node-b.redb \
  --storage redb \
  --tenant-id tenant \
  --bl-name default \
  --device-id node-b \
  --peer http://127.0.0.1:50051=tenant/default/node-a
```

Peer sync periodically calls `ListHeads` on each configured peer, compares local heads, fetches missing ranges, and writes replicated messages idempotently into the local database.

The `=tenant/bl/device` suffix pins the expected node identity reported by the peer `Health` RPC. Unpinned peers still work for local development, but pinned peers are recommended for any real deployment.

Sync state is persisted in the local store and can be inspected with:

```bash
MSGBUS_ACTION=list-sync cargo run -p msgbus-sdk-rust --example probe
```

## SDK Usage

Rust:

```rust
use msgbus_sdk_rust::{MsgbusClient, NodeId};

#[tokio::main]
async fn main() -> msgbus_sdk_rust::Result<()> {
    let origin = NodeId {
        tenant_id: "tenant".into(),
        bl_name: "default".into(),
        device_id: "device".into(),
    };
    let mut client = MsgbusClient::connect("http://127.0.0.1:50051")
        .await?
        .with_default_origin(origin);

    let ready = client.ready().await?;
    assert!(ready.ready);

    let message = client.publish("events", b"hello".to_vec()).await?;
    let mut subscription = client.subscribe_reconnecting("events", None, message.head_id);
    let _next = subscription.next_message().await?;
    Ok(())
}
```

Go:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

client, err := msgbus.Dial(ctx, "127.0.0.1:50051")
if err != nil {
    return err
}
defer client.Close()

ready, err := client.Ready(ctx)
if err != nil {
    return err
}
if !ready.Ready {
    return fmt.Errorf("msgbus is not ready: %s", ready.Error)
}

msg, err := client.Publish(ctx, "events", []byte("hello"))
if err != nil {
    return err
}

iter := client.SubscribeIterator(context.Background(), "events", msgbus.SubscribeOptions{
    FromHeadID: msg.HeadId,
})
next, err := iter.Recv()
```

## TLS And mTLS

Server-side TLS:

```bash
cargo run -p msgbus-server --bin msgbusd -- \
  --listen 0.0.0.0:50051 \
  --data ./node-a.redb \
  --storage redb \
  --tenant-id tenant \
  --bl-name default \
  --device-id node-a \
  --tls-cert ./certs/node-a.pem \
  --tls-key ./certs/node-a-key.pem
```

Require client certificates by adding a client CA:

```bash
--tls-client-ca ./certs/ca.pem
```

Peer client TLS settings:

```bash
--peer https://node-b.example.com:50052=tenant/default/node-b \
--peer-tls-ca ./certs/ca.pem \
--peer-tls-cert ./certs/node-a.pem \
--peer-tls-key ./certs/node-a-key.pem \
--peer-tls-domain node-b.example.com
```

TLS authenticates the transport. The peer identity suffix binds the msgbus node identity at the application layer. Use both when syncing across machines or networks.

## Consistency Model

`msgbus` uses asynchronous peer replication. It provides per-`topic + origin` ordering and eventual convergence when peers can reach each other. It does not provide a global total order, quorum writes, or consensus. Reads from another daemon may lag until sync catches up.

In another shell:

```bash
cargo run -p msgbus-sdk-rust --example basic
```

## Deployment

Build a container image:

```bash
docker build -t msgbus:local .
```

Run with a local data directory:

```bash
docker run --rm -p 50051:50051 -v "$PWD/data:/var/lib/msgbus" msgbus:local
```

A hardened systemd template is available at `packaging/systemd/msgbusd.service`.

## Regenerate Protobuf Code

```bash
./scripts/gen-proto.sh
```

Rust code is generated during `cargo build` by `crates/msgbus-proto/build.rs`. Go code is generated by `buf generate` into `go-sdk/msgbus/gen`.

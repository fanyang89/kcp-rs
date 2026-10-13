# kcp-rs

Pure Rust implementation of the KCP reliable ordered transport protocol. The
default library has **no dependencies, no C/FFI, no unsafe code and no async runtime**.
It uses `std` for owned buffers and queues. An optional `async` feature adds only
`futures-io` (no runtime) for connection/stream handles.

## Connections and asynchronous streams

`endpoint::EndpointCore` adds bounded connection management, handshake retries,
session routing, timeouts and reliable half-close without asynchronous dependencies.
Enable `async` for `asynchronous::{Endpoint, Connector, KcpStream, Driver}` and
`futures_io::AsyncRead` / `AsyncWrite`. Supply a datagram transport and timer and
run the driver concurrently; the library never spawns tasks itself. Clone
`endpoint.connector()` to initiate multiple connections while the endpoint accepts
incoming ones; stream operations are woken only when ready or failed.

```toml
[dependencies]
kcp-rs = { path = "../kcp-rs", features = ["async"] }
```

```sh
cargo run --features async --example tokio
cargo run --features async --example async_io
```

Both adapters use the same stream API and verify a 256 KiB UDP transfer and
bidirectional half-close. Runtimes are dev/example dependencies only.
See [sessions and async integration](docs/sessions.md) for budgets, lifecycle,
wire format, cancellation and security limitations. The new session protocol
requires matching peers; it does not interoperate with bare KCP or kcp-sys endpoints.

## Bare KCP model

`Kcp` is one conversation's Sans-I/O state machine:

- `send(bytes)` queues an application message.
- `input(datagram, now_ms)` processes an incoming KCP datagram.
- `update(now_ms)` advances protocol timers and generates outgoing datagrams.
- `poll_output()` takes an outgoing datagram; send it without changing its boundaries.
- `recv()` takes a complete message (`None` means not ready).
- `check(now_ms)` returns the next absolute update deadline, valid until a send/input.
- `waitsnd()` reports queued plus unacknowledged segments for application backpressure.

Both peers must agree on the conversation ID and message/stream mode. The caller
owns sockets, routing, timers and connection lifecycle. Use Tokio, smol, a custom
event loop, or synchronous I/O without changing the protocol core.

```rust
use kcp_rs::Kcp;

let mut a = Kcp::new(42);
let mut b = Kcp::new(42);
a.send(b"hello")?;
for now in (0..1000).step_by(10) {
    a.update(now);
    b.update(now);
    while let Some(packet) = a.poll_output() { b.input(&packet, now)?; }
    while let Some(packet) = b.poll_output() { a.input(&packet, now)?; }
}
assert_eq!(b.recv(), Some(b"hello".to_vec()));
# Ok::<(), kcp_rs::Error>(())
```

Run `cargo run --example in_memory` for an executable example.

## Integration contract

Use milliseconds from a monotonic clock, truncated to `u32` (wrapping is
intentional), not wall-clock time. Do not let calls jump by 2^31 milliseconds or
more. Supply current time to `input` for RTT estimation. Call `update` initially,
after handling events, and at the deadline from `check`. After send/input,
recompute the deadline; `flush` can force immediate output after initialization.
For a timer duration use `deadline.wrapping_sub(now)` after calling `check(now)`.

Drain output promptly. If a socket would block, retain the taken datagram in your
transport adapter and retry it. Draining output is not acknowledgement of delivery;
KCP retains unacknowledged data and retransmits it.

Send, ACK and output queues are not globally memory-bounded. Apply application
backpressure using `waitsnd()`, bound incoming traffic and drain receive/output
queues. KCP has no authentication, encryption, handshake or graceful shutdown;
conversation IDs are routing identifiers, not security tokens. Use an authenticated
transport when accepting untrusted traffic. `is_dead()` latches when a segment
reaches the retransmission threshold; the caller decides when to close the session.

## Configuration and compatibility

Construct with `Kcp::with_config(conv, Config { ..Default::default() })`.
Configuration is immutable once constructed to avoid changing MTU/mode while data
is in flight. Defaults match classic KCP: MTU 1400, send window 32, receive window
128, interval 100 ms, congestion control enabled. For lower latency set `nodelay`
to `true`, `interval` to 10 or 20 and `fast_resend` to 2. Disabling congestion
control is an explicit opt-in and may overload shared networks.

Implemented: little-endian 24-byte KCP headers, PUSH/ACK/WASK/WINS, message
fragmentation/reassembly, stream coalescing, RTT/RTO estimation, timeout and fast
retransmit, receive-window probing/recovery, classic congestion control, dead-link
tracking, and wrapping sequence numbers/timestamps.

Message mode permits at most 127 fragments per message, or
`127 * (mtu - 24)` bytes. Empty messages are supported. Stream mode accepts larger
writes, removes message boundaries and returns available chunks through `recv`;
it does not implement any runtime-specific `AsyncRead`/`AsyncWrite` traits.
The receive window must be at least 128 segments.

Malformed datagrams are rejected atomically, including trailing partial headers
(the C reference may accept trailing bytes). This implementation does not provide
upstream's pluggable congestion-control extension or the `kcp-sys` higher-level
endpoint/connection protocol.

## Validation

```sh
cargo test
cargo test --release
cargo test --all-features
cargo test --all-features --release
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --check
```

An optional subprocess-based C interoperability test needs `cc` and an upstream
KCP checkout. C is used only by this test, never linked into the library:

```sh
KCP_C_SOURCE=/path/to/kcp/ikcp.c cargo test --test c_interop -- --ignored
```

Tests cover bidirectional loss, reordering, duplicates, fragmentation (including
the maximum message size in C interop), stream mode, zero-window recovery,
fast retransmission, dead-link detection, malformed packets and timestamp/sequence
wraparound. This is an initial implementation, not a production security audit or
performance benchmark.

## License

MIT. The protocol implementation is derived from the algorithms in
[skywind3000/kcp](https://github.com/skywind3000/kcp); the original MIT notice is
preserved in `LICENSE`.

# Sessions and runtime-neutral streams

## Layers

- `Kcp`: bare KCP, retaining the original wire compatibility and zero dependencies.
- `endpoint::EndpointCore`: synchronous session manager, also zero dependencies.
- `asynchronous::{Endpoint, Connector, KcpStream, Driver}`: optional `async` feature, depending
  only on `futures-io` for the standard runtime-neutral I/O traits.
- `DatagramTransport` and `Timer`: adapters supplied by the application. Runnable
  examples provide Tokio and async-io implementations; neither is a normal library
  dependency.

There is one driver per endpoint, not a task per connection. The caller must run
it concurrently with application operations (spawn it or combine futures).
Awaiting `connect` without polling the driver cannot make progress.

The asynchronous facade and driver share a core behind a short-lived `std::sync::Mutex`.
Stream operations copy bounded data into/out of core buffers. There is no additional
unbounded command channel and no lock held across an await or transport operation.
Condition checks and waker registration use the same lock. Wakers are invoked
only after unlocking. The driver processes at most 64 I/O iterations per poll,
then yields cooperatively. The driver scans registered handles but wakes only
operations that can now progress (or return an error). Read, write, flush, close,
connect and accept readiness are tracked separately: free send capacity alone is
not a reason to wake a flush waiting for acknowledgements. This initial version
still favors simplicity over large-endpoint throughput; it does not use an
incremental readiness index.

The driver needs only datagrams and monotonic timers, not an executor/spawner
abstraction. Adapters may be `Send` or local-only; the driver is `Send` when its
transport and timer are `Send`. A `Pending` adapter operation must register the
current waker. Timer adapters must support changed deadlines and changed wakers.
A successful send means the entire datagram was accepted by the transport.

## API and lifecycle

```text
Endpoint::new(config, session_seed, transport, timer) -> (Endpoint, Driver)
Endpoint::connector()                                -> Connector (Clone)
Connector::connect(peer).await                        -> KcpStream
Endpoint::connect(peer).await                         -> KcpStream
Endpoint::accept().await                              -> KcpStream
Endpoint::shutdown() / Connector::shutdown()           -> immediate local shutdown
KcpStream: futures_io::AsyncRead + futures_io::AsyncWrite
```

`Endpoint` owns the listener and is intentionally not Clone. `accept` borrows it
mutably, allowing one pending accept per listener. `connect` borrows it immutably;
for simultaneous inbound/outbound opens, first obtain a `Connector` with
`endpoint.connector()`. Connector is Clone and its `connect` takes `&self`, so
multiple connection attempts may run concurrently, including from different tasks.
Each attempt has its own connection ID, waker and cancellation cleanup.

Existing streams and connectors live independently of the listener. Dropping the
endpoint disables new inbound handshakes but does not terminate streams or prevent
connectors from initiating new connections. Dropping a connector does not disable
listening or affect other clones. All handles still require a running driver;
once it exits, both existing operations and new connection attempts fail.

For example, using an application-provided futures combinator while the driver
runs concurrently:

```rust
let connector = endpoint.connector();
let (outgoing, incoming) = futures_lite::future::zip(
    connector.connect(remote_addr),
    endpoint.accept(),
).await;
```

`futures-lite` here is an application choice, not a library dependency. Clone a
Connector to move it into another task without transferring listener ownership.

- Writes may be partial, and return Pending when the configured send budget is full.
- Flush waits for all previously accepted records to be ACKed by the peer's KCP.
  It does **not** imply that the peer application read or committed those bytes.
- Close queues an ordered FIN and waits for its KCP acknowledgement. Only writes
  are closed; reading the opposite direction remains valid.
- EOF is returned only after preceding bytes have been read and FIN is received.
- Reset, timeout and driver failure return errors instead of EOF. On error,
  remaining buffered bytes are not guaranteed to be delivered to the application.
- Cancelling connect releases its local slot. A transmitted SYN may leave a remote
  half-open session until its handshake timeout.
- Cancelling accept leaves unaccepted connections in the bounded backlog.
- Cancelling close after FIN was queued does not undo the half-close. Retrying
  close is idempotent. Cancelling flush does not cancel accepted writes.
- Dropping a stream without starting graceful close aborts and queues a best-effort
  RESET. After close has started, dropping detaches the stream: the driver continues
  retransmission/ACKs, discards further read data and reclaims it after completion
  and linger, or timeout. A detached stream waiting for the peer's FIN has an
  additional fixed `close_timeout` budget starting at release, even if traffic
  or keepalive continues. Drop is never an asynchronous graceful-close guarantee.
- Dropping the driver wakes pending connect/accept/read/write/flush/close operations
  with BrokenPipe. Transport/timer failure propagates its error kind to all handles.
  Cancellation of the driver does not gracefully close remote sessions.

The synchronous core reports backpressure as `Ok(None)` for read/write and `false`
for close. `is_flushed` and `is_connected` report progress or terminal errors.
A caller owns each returned ConnectionId until `release` or `abort`; terminal
outgoing/accepted sessions retain a slot until released. Unaccepted failed sessions
are reclaimed automatically.

## Endpoint shutdown

Call `Endpoint::shutdown()` or `Connector::shutdown()` to permanently stop the
entire local endpoint. A Connector clone can initiate shutdown from another task,
including while accept or connect is pending. The operation is synchronous,
idempotent and runtime-neutral:

- All pending and subsequent application operations fail with ConnectionAborted.
- All connection state, KCP buffers, accept backlog and queued output are released
  immediately, even if application stream handles are retained.
- The driver is woken and completes with `Ok(())` when polled. It drops its own
  pending datagram/receive buffer on completion. The transport/timer objects remain
  owned by the driver until the driver future is dropped.
- No FIN or RESET is sent; this is local cancellation, not a graceful delivery or
  peer-notification guarantee. An already running transport poll may finish
  concurrently with shutdown. Await driver completion before assuming no more I/O.
- An earlier transport/timer failure or driver drop keeps its original error;
  calling shutdown afterward cannot turn that failure into success.

For graceful operation, close each stream, continue driving peer completion and
allow the desired linger interval before shutting down the endpoint. Dropping
just the Endpoint listener still only stops new inbound handshakes; use explicit
shutdown when you intend to terminate all streams and outgoing Connector clones.

`EndpointCore::shutdown()` provides the corresponding Sans-I/O operation. It
releases all queues, sets `is_shutdown()`, rejects new input/connect operations
with ConnectionAborted, and cannot be reversed by `set_listening(true)`. Its
accept/transmit/deadline queries become empty. Construct a fresh endpoint to restart.

Driver failure/drop also releases core buffers and waker registrations promptly;
retaining errored stream handles does not retain their protocol queues.

## Scheduling and budgets

Supply monotonic u64 milliseconds to the core; internally KCP uses the low 32 bits.
Call `update` after input/application actions and at `deadline()`. Drain
`poll_transmit()` and call `update` again to pump remaining per-connection output.
If transport is blocked, keep the taken datagram and wait for writability; do not
poll in a tight loop. The async driver implements this contract.

Endpoint limits include total connections, incoming unaccepted backlog, queued
output datagrams, per-connection unacknowledged/queued send segments, and application
receive bytes. Backlog includes half-open sessions. Invalid or over-budget network
input is dropped, relying on retransmission rather than allocating more memory.

Private DATA records fit into one KCP segment, so `send_capacity` bounds queued
payload bytes by approximately `send_capacity * (mtu - 22 - 24)`. The receive
application queue is bounded by `receive_capacity`. KCP receive buffers remain
additionally bounded by the configured receive window; stopping application reads
stops draining KCP and eventually advertises a zero window. Deferred ACKs are capped
per connection (a final MTU-sized input batch can exceed the 256-entry admission
threshold). At most one KCP flush batch is retained per connection, plus the bounded
endpoint output queue and one driver-owned pending datagram.

Thus aggregate memory is bounded by configuration, not just channel capacity.
The defaults are not a small-memory promise: multiply per-connection window and
buffer budgets by `max_connections` when sizing a deployment. Resource limits are
not substitutes for authentication, traffic rate limiting or CPU admission control.

`Config::mtu` is the total UDP payload MTU. The manager subtracts its 22-byte envelope
and overrides `config.kcp.mtu`; `config.kcp.stream` is forced to false. The public
stream hides the internal KCP message boundaries. Receive capacity must be at least
MTU. Session IDs monotonically advance from the application-supplied nonzero seed;
exhaustion fails instead of wrapping. Supply fresh unpredictable seeds across endpoint
restarts (for example using your application's OS-backed RNG); fixed example seeds
are only for local demos.

## Session wire protocol, version 1

All integers are little-endian. The envelope is exactly 22 bytes:

| Offset | Length | Field |
| --- | --- | --- |
| 0 | 4 | ASCII `KRCP` |
| 4 | 1 | Version = 1 |
| 5 | 1 | Kind |
| 6 | 8 | Source session ID (nonzero) |
| 14 | 8 | Destination session ID |
| 22 | remaining | Payload (DATA only) |

Kinds: SYN=1, SYN_ACK=2, ACK=3, DATA=4, RESET=5. Control packets have no payload.
SYN uses destination zero. SYN_ACK binds the server session ID; subsequent packets
must match the peer socket address and both session IDs. No address migration is
allowed. KCP `conv` is the low 32 bits of the initiating client's session ID.

Handshake state transitions:

```text
client: SynSent -- SYN_ACK --> Established
server: SynReceived -- ACK or valid DATA --> Established / accept queue
```

SYN/SYN_ACK retry after 200 ms with exponential backoff capped at 2 seconds, until
the handshake deadline. Duplicate SYN reuses the same server session; duplicate
SYN_ACK resends ACK. Lost third-leg ACK can therefore recover without application
data. Established clients may write immediately; valid DATA also confirms the
server-side handshake.

DATA envelopes contain complete ordinary KCP datagrams. KCP message mode carries
private records: `0x00 || application bytes` or the single byte `0x01` for FIN.
Records are bounded to one segment by the sender. Unknown/malformed records and
records after FIN terminate the connection with InvalidData. FIN ordering and
retransmission come from KCP; it cannot overtake preceding DATA. A local FIN must
be acknowledged before the close timeout, but an acknowledged half-close does not
require the peer to close its write half before that deadline.

Once both FINs are processed and local sends are ACKed, the endpoint continues
ACKing duplicates for `linger` milliseconds (default 5 seconds). No finite linger
can guarantee delivery through an arbitrarily long outage. Tune it for expected
RTT/loss conditions. Idle timeout (default 120 seconds) applies before both halves
finish. Optional keepalive is disabled by default; see below.

This protocol is **not** the kcp-sys endpoint format and is **not** bare KCP. Both
peers must implement this session layer; users needing bare KCP should use `Kcp`.

## Read-only diagnostics

No metrics framework or background sampler is required:

- `Kcp::stats()` returns a `KcpStats` snapshot of send/receive queues, buffered
  payload bytes, deferred ACKs, queued datagrams, remote/congestion windows,
  smoothed RTT, current RTO, dead-link status and cumulative transmission counters.
- `EndpointCore::stats()`, `Endpoint::stats()` and `Connector::stats()` report the
  same endpoint-wide gauges: lifecycle counts, pending accepts, detached sessions,
  endpoint/KCP output queues and permanent shutdown state.
- `EndpointCore::connection_stats(id)` and `KcpStream::stats()` include peer address,
  lifecycle state, half-close/detach flags, application read-buffer size, idle time
  and the connection's KCP snapshot.

Snapshots do not advance time, consume queues, wake tasks or perform I/O. They are
owned Copy values; sampling at application-chosen intervals works on any runtime.
Endpoint sampling scans connections/backlog; KCP and per-connection sampling scan
buffered segments to sum payload bytes. Async snapshots hold the shared mutex for
that scan, so avoid unnecessarily frequent sampling of very large endpoints.

Counter/queue semantics matter:

- `transmissions` counts PUSH segment emissions, including retransmissions, when
  KCP generates output. It excludes ACK/WASK/WINS and is **not** a UDP packet or
  successful socket-send count. One datagram may contain multiple segments, and
  generated datagrams may still be queued at the transport.
- Timeout and fast-retransmission counters are separate, cumulative for the KCP
  object's lifetime and retained after ACKs remove segments. Counters saturate at
  u64::MAX instead of wrapping; gauges are current sizes, not historical totals.
- Payload byte counts exclude KCP headers and allocation capacity. In the session
  layer they include private DATA/FIN record tags. `buffered_read_bytes` counts only
  application bytes already moved out of KCP; inspect both queues for receive pressure.
- Endpoint output gauges exclude the one datagram currently owned by the driver.
- RTT is `None` before a sample, and `Some(0)` for a measured zero-millisecond RTT.
  RTO is KCP's base estimate; an individual segment may have a backed-off timeout.
- `idle_ms` uses the latest time supplied to the endpoint, not the instant at which
  the snapshot is requested. Congestion window may be ignored by configuration.

A retained failed connection reports `ConnectionState::Failed(error_kind)` instead
of hiding its diagnostic state. Once released or removed, that snapshot is no
longer available. Driver termination releases connection buffers, so stream stats
then return its terminal error. Endpoint stats remain available and show shutdown
with empty queues; that flag alone does not distinguish explicit shutdown from
transport failure (use the driver result/operation error for the cause).

## Optional idle keepalive

```rust
let config = kcp_rs::endpoint::Config {
    keep_alive_interval: Some(15_000),
    idle_timeout: 120_000,
    ..Default::default()
};
```

An established connection with no recent valid inbound traffic queues an ordinary
KCP WASK window probe; the peer responds with WINS through the normal KCP machinery.
This adds no session packet types, negotiation or wire format changes, and needs no
runtime-specific timer. One side may enable probing while the other leaves it off.
Probes do not consume application send slots, produce stream bytes or wake idle
application readers. Ongoing inbound traffic suppresses redundant idle probes.

The interval must be at least `config.kcp.interval` and strictly below
`idle_timeout`; allow substantial additional margin for RTT, loss and scheduling
on **both** peers. The configured interval is a scheduling target, not a delivery
promise: KCP flush timing and blocked output may delay transmission. Only valid
received packets refresh the idle deadline. Sending unanswered probes cannot keep
a dead peer alive indefinitely. This is liveness detection, not authenticated
proof of peer identity or anti-replay protection.

Acknowledged half-closed streams remain eligible for keepalive while the read half
is open. Locally detached handles stop initiating probes and have a fixed cleanup
budget; remote probes cannot retain an abandoned connection forever. Once both
halves finish, keepalive stops and never extends the normal linger deadline.

## Security and scope

The protocol is experimental, unauthenticated and unencrypted. Session IDs do not
prevent on-path injection; the initial seed is not a cryptographic handshake. An
attacker can consume the configured backlog or forge packets when identifiers are
known. There are no stateless cookies, authentication, NAT traversal, migration,
version negotiation or congestion-control plugins. Deploy over an
appropriate authenticated transport or restrict peers and apply external admission
controls. The code has not received a production security audit or performance tuning.

## Run and validate

```sh
cargo run --features async --example tokio
cargo run --features async --example async_io
cargo test --all-features
cargo test --all-features --release
cargo clippy --all-targets --all-features -- -D warnings
cargo tree --edges normal --no-default-features
cargo tree --edges normal --features async
```

Both examples transfer 256 KiB over real loopback UDP, verify the bytes, half-close
and exchange a response. They use the same stream API and differ only in UDP/timer
adapters and executor. Deterministic tests additionally exercise each lost handshake
leg, loss/reordering/duplication, multiple connections, backpressure, FIN ordering,
lingering cleanup, malformed input, timeout/reset, concurrent bidirectional opens,
connector/listener ownership, keepalive through loss/timestamp wrap, unanswered-probe
expiry, detached cleanup, cancellation, readiness-specific wakeups, explicit
shutdown, first-error preservation and driver drop behavior. Tokio users wanting Tokio I/O traits can wrap a KcpStream with
`tokio-util::compat::FuturesAsyncReadCompatExt::compat()` in their own application.

//! Sans-I/O session management. This versioned protocol is not bare KCP.
use crate::{Config as KcpConfig, Kcp};
use std::{
    collections::{BTreeMap, VecDeque},
    io,
    net::SocketAddr,
};

const MAGIC: &[u8; 4] = b"KRCP";
const VERSION: u8 = 1;
const HEADER: usize = 22;
const SYN: u8 = 1;
const SYN_ACK: u8 = 2;
const ACK: u8 = 3;
const DATA: u8 = 4;
const RESET: u8 = 5;
const RECORD_DATA: u8 = 0;
const RECORD_FIN: u8 = 1;

/// Local session identifier, never reused during an endpoint's lifetime.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConnectionId(pub u64);

#[derive(Clone, Debug)]
pub struct Config {
    /// Total UDP payload MTU, including the session envelope.
    pub mtu: usize,
    pub kcp: KcpConfig,
    pub listen: bool,
    pub max_connections: usize,
    pub accept_backlog: usize,
    /// Maximum queued + unacknowledged KCP segments per connection.
    pub send_capacity: usize,
    pub receive_capacity: usize,
    /// Maximum queued datagrams in the endpoint's output queue.
    pub output_capacity: usize,
    pub handshake_timeout: u64,
    pub idle_timeout: u64,
    /// Optional idle probe interval in milliseconds, using KCP WASK/WINS.
    /// Must be at least the KCP update interval and less than idle_timeout.
    /// Disabled by default. Only received traffic, never probes sent, refreshes liveness.
    pub keep_alive_interval: Option<u64>,
    pub close_timeout: u64,
    /// Keep acknowledging retransmissions after both halves close.
    pub linger: u64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            mtu: 1400,
            kcp: KcpConfig {
                interval: 10,
                nodelay: true,
                fast_resend: 2,
                ..KcpConfig::default()
            },
            listen: true,
            max_connections: 1024,
            accept_backlog: 128,
            send_capacity: 128,
            receive_capacity: 64 * 1024,
            output_capacity: 256,
            handshake_timeout: 10_000,
            idle_timeout: 120_000,
            keep_alive_interval: None,
            close_timeout: 30_000,
            linger: 5_000,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Transmit {
    pub peer: SocketAddr,
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    SynSent,
    SynReceived,
    Established,
    Closed,
}

struct Connection {
    peer: SocketAddr,
    remote: u64,
    phase: Phase,
    kcp: Kcp,
    created: u64,
    deferred_start: bool,
    last_input: u64,
    last_probe: u64,
    retry_at: u64,
    retry_delay: u64,
    receive: VecDeque<u8>,
    write_closed: bool,
    peer_closed: bool,
    close_started: Option<u64>,
    linger_until: Option<u64>,
    error: Option<io::ErrorKind>,
    incoming: bool,
    accepted: bool,
    detached: bool,
    detach_deadline: Option<u64>,
}

/// All times are monotonic `u64` milliseconds. No sockets or asynchronous tasks.
/// Limits bound memory; unauthenticated traffic may still consume CPU and connection slots.
pub struct EndpointCore {
    config: Config,
    connections: BTreeMap<ConnectionId, Connection>,
    accepts: VecDeque<ConnectionId>,
    output: VecDeque<Transmit>,
    next_id: Option<u64>,
    now: u64,
    output_cursor: Option<ConnectionId>,
    shutdown: bool,
}
fn error(kind: io::ErrorKind) -> io::Error {
    io::Error::from(kind)
}
impl EndpointCore {
    /// Supply a nonzero, unpredictable session seed from the application. Do not reuse
    /// seeds across endpoint restarts. Session IDs are not authentication credentials.
    pub fn new(mut config: Config, session_seed: u64) -> io::Result<Self> {
        if !(72..=65507).contains(&config.mtu)
            || config.max_connections == 0
            || config.accept_backlog == 0
            || config.send_capacity == 0
            || config.output_capacity == 0
            || config.receive_capacity < config.mtu
            || session_seed == 0
            || config.handshake_timeout == 0
            || config.idle_timeout == 0
            || config.keep_alive_interval.is_some_and(|interval| {
                interval == 0
                    || interval < u64::from(config.kcp.interval)
                    || interval >= config.idle_timeout
            })
            || config.close_timeout == 0
            || config.linger == 0
        {
            return Err(error(io::ErrorKind::InvalidInput));
        }
        config.kcp.mtu = config.mtu - HEADER;
        config.kcp.stream = false;
        Kcp::with_config(0, config.kcp.clone()).map_err(|_| error(io::ErrorKind::InvalidInput))?;
        Ok(Self {
            config,
            connections: BTreeMap::new(),
            accepts: VecDeque::new(),
            output: VecDeque::new(),
            next_id: Some(session_seed),
            now: 0,
            output_cursor: None,
            shutdown: false,
        })
    }
    /// Permanently aborts this endpoint and releases all protocol queues. Idempotent.
    /// No FIN/RESET is sent; use per-stream close before shutdown for graceful delivery.
    pub fn shutdown(&mut self) {
        self.shutdown = true;
        self.config.listen = false;
        self.connections.clear();
        self.accepts = VecDeque::new();
        self.output = VecDeque::new();
        self.output_cursor = None;
    }
    pub fn is_shutdown(&self) -> bool {
        self.shutdown
    }
    /// Most recent monotonic time supplied to the core.
    pub fn current_time(&self) -> u64 {
        self.now
    }
    pub fn mtu(&self) -> usize {
        self.config.mtu
    }
    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }
    pub fn set_listening(&mut self, enabled: bool) {
        self.config.listen = enabled && !self.shutdown;
    }
    fn allocate(&mut self) -> io::Result<ConnectionId> {
        if self.shutdown {
            return Err(error(io::ErrorKind::ConnectionAborted));
        }
        if self.connections.len() >= self.config.max_connections {
            return Err(error(io::ErrorKind::WouldBlock));
        }
        let id = self
            .next_id
            .ok_or_else(|| error(io::ErrorKind::AddrNotAvailable))?;
        self.next_id = id.checked_add(1);
        Ok(ConnectionId(id))
    }
    fn connection(&self, id: ConnectionId) -> io::Result<&Connection> {
        if self.shutdown {
            return Err(error(io::ErrorKind::ConnectionAborted));
        }
        let c = self
            .connections
            .get(&id)
            .ok_or_else(|| error(io::ErrorKind::NotConnected))?;
        if let Some(e) = c.error {
            return Err(error(e));
        }
        Ok(c)
    }
    fn make_connection(
        &self,
        peer: SocketAddr,
        remote: u64,
        phase: Phase,
        conv: u32,
    ) -> Connection {
        Connection {
            peer,
            remote,
            phase,
            kcp: Kcp::with_config(conv, self.config.kcp.clone()).unwrap(),
            created: self.now,
            deferred_start: false,
            last_input: self.now,
            last_probe: self.now,
            retry_at: self.now,
            retry_delay: 200,
            receive: VecDeque::new(),
            write_closed: false,
            peer_closed: false,
            close_started: None,
            linger_until: None,
            error: None,
            incoming: phase == Phase::SynReceived,
            accepted: false,
            detached: false,
            detach_deadline: None,
        }
    }
    /// Starts an outgoing handshake. Call `update` to produce packets.
    pub fn connect(&mut self, peer: SocketAddr, now: u64) -> io::Result<ConnectionId> {
        self.now = self.now.max(now);
        let id = self.allocate()?;
        let c = self.make_connection(peer, 0, Phase::SynSent, id.0 as u32);
        self.connections.insert(id, c);
        Ok(id)
    }
    #[cfg(feature = "async")]
    pub(crate) fn connect_deferred(&mut self, peer: SocketAddr) -> io::Result<ConnectionId> {
        let id = self.connect(peer, self.now)?;
        self.connections.get_mut(&id).unwrap().deferred_start = true;
        Ok(id)
    }
    pub fn is_write_closed(&self, id: ConnectionId) -> io::Result<bool> {
        Ok(self.connection(id)?.write_closed)
    }
    pub fn is_connected(&self, id: ConnectionId) -> io::Result<bool> {
        Ok(matches!(
            self.connection(id)?.phase,
            Phase::Established | Phase::Closed
        ))
    }
    /// Whether a nonempty read can return data or EOF without waiting.
    pub fn is_readable(&self, id: ConnectionId) -> io::Result<bool> {
        let c = self.connection(id)?;
        Ok(!c.receive.is_empty() || c.peer_closed)
    }
    /// Whether a nonempty write can accept bytes without waiting.
    pub fn is_writable(&self, id: ConnectionId) -> io::Result<bool> {
        let c = self.connection(id)?;
        if c.write_closed {
            return Err(error(io::ErrorKind::BrokenPipe));
        }
        Ok(c.phase == Phase::Established && c.kcp.waitsnd() < self.config.send_capacity)
    }
    /// Whether accept can return a live connection without waiting.
    pub fn has_pending_accept(&self) -> bool {
        self.accepts.iter().any(|id| {
            self.connections
                .get(id)
                .is_some_and(|c| c.error.is_none() && !c.accepted)
        })
    }
    pub fn peer_addr(&self, id: ConnectionId) -> io::Result<SocketAddr> {
        Ok(self.connection(id)?.peer)
    }
    pub fn accept(&mut self) -> Option<ConnectionId> {
        while let Some(id) = self.accepts.pop_front() {
            if let Some(c) = self.connections.get_mut(&id)
                && c.error.is_none()
                && !c.accepted
            {
                c.accepted = true;
                return Some(id);
            }
        }
        None
    }
    /// Returns None for backpressure, not an accepted zero-byte write.
    pub fn write(&mut self, id: ConnectionId, data: &[u8]) -> io::Result<Option<usize>> {
        let c = self.connection(id)?;
        if c.write_closed {
            return Err(error(io::ErrorKind::BrokenPipe));
        }
        if c.phase != Phase::Established {
            return Ok(None);
        }
        if data.is_empty() {
            return Ok(Some(0));
        }
        if c.kcp.waitsnd() >= self.config.send_capacity {
            return Ok(None);
        }
        // Each private record fits in one KCP segment, simplifying byte accounting.
        let n = data.len().min(self.config.kcp.mtu - 24 - 1);
        let mut record = Vec::with_capacity(n + 1);
        record.push(RECORD_DATA);
        record.extend_from_slice(&data[..n]);
        self.connections
            .get_mut(&id)
            .unwrap()
            .kcp
            .send(&record)
            .unwrap();
        Ok(Some(n))
    }
    /// Reads bytes, Some(0) is ordered EOF (or an empty destination); None is not ready.
    pub fn read(&mut self, id: ConnectionId, buf: &mut [u8]) -> io::Result<Option<usize>> {
        self.connection(id)?;
        let c = self.connections.get_mut(&id).unwrap();
        if buf.is_empty() {
            return Ok(Some(0));
        }
        if c.receive.is_empty() {
            return Ok(if c.peer_closed { Some(0) } else { None });
        }
        let n = buf.len().min(c.receive.len());
        for byte in &mut buf[..n] {
            *byte = c.receive.pop_front().unwrap();
        }
        Ok(Some(n))
    }
    /// Strong flush: all accepted writes have been ACKed by the peer's KCP.
    pub fn is_flushed(&self, id: ConnectionId) -> io::Result<bool> {
        let c = self.connection(id)?;
        Ok(matches!(c.phase, Phase::Established | Phase::Closed) && c.kcp.waitsnd() == 0)
    }
    /// Queues a reliable FIN after all preceding writes. Idempotent; None-like false
    /// indicates backpressure, so the caller must retry. The read half remains open.
    pub fn close_write(&mut self, id: ConnectionId) -> io::Result<bool> {
        let c = self.connection(id)?;
        if c.write_closed {
            return Ok(true);
        }
        if c.phase != Phase::Established || c.kcp.waitsnd() >= self.config.send_capacity {
            return Ok(false);
        }
        let c = self.connections.get_mut(&id).unwrap();
        c.kcp.send(&[RECORD_FIN]).unwrap();
        c.write_closed = true;
        c.close_started = Some(self.now);
        Ok(true)
    }
    /// Drop a handle. Gracefully closed writers linger; other drops send a best-effort reset.
    pub fn release(&mut self, id: ConnectionId) {
        if let Some(c) = self.connections.get_mut(&id)
            && c.write_closed
            && c.error.is_none()
        {
            if c.phase == Phase::Closed {
                self.connections.remove(&id);
            } else {
                c.receive.clear();
                c.detached = true;
                c.detach_deadline
                    .get_or_insert(self.now.saturating_add(self.config.close_timeout));
            }
            return;
        }
        self.abort(id);
    }
    pub fn abort(&mut self, id: ConnectionId) {
        if let Some(c) = self.connections.remove(&id)
            && c.remote != 0
        {
            self.control(c.peer, RESET, id.0, c.remote);
        }
        self.accepts.retain(|i| *i != id);
    }
    fn control(&mut self, peer: SocketAddr, kind: u8, src: u64, dst: u64) {
        if self.output.len() < self.config.output_capacity {
            self.output.push_back(Transmit {
                peer,
                payload: envelope(kind, src, dst, &[]),
            });
        }
    }
    /// Returns outgoing datagrams. Their ownership passes to the caller, who must
    /// retain a datagram if its transport is temporarily not writable.
    pub fn poll_transmit(&mut self) -> Option<Transmit> {
        self.output.pop_front()
    }
    /// Processes one session datagram. Over-budget/unknown sessions are silently dropped.
    /// Invalid framing returns InvalidData; never reflect a reset to arbitrary senders.
    pub fn input(&mut self, peer: SocketAddr, packet: &[u8], now: u64) -> io::Result<()> {
        if self.shutdown {
            return Err(error(io::ErrorKind::ConnectionAborted));
        }
        if packet.len() < HEADER
            || packet.len() > self.config.mtu
            || &packet[..4] != MAGIC
            || packet[4] != VERSION
        {
            return Err(error(io::ErrorKind::InvalidData));
        }
        let kind = packet[5];
        let src = u64::from_le_bytes(packet[6..14].try_into().unwrap());
        let dst = u64::from_le_bytes(packet[14..22].try_into().unwrap());
        if src == 0
            || !(SYN..=RESET).contains(&kind)
            || (kind != DATA && packet.len() != HEADER)
            || (kind == DATA && packet.len() < HEADER + 24)
            || (kind == SYN && dst != 0)
        {
            return Err(error(io::ErrorKind::InvalidData));
        }
        self.now = self.now.max(now);
        if kind == SYN {
            if let Some((&id, c)) = self
                .connections
                .iter()
                .find(|(_, c)| c.peer == peer && c.remote == src && c.incoming)
            {
                if c.phase == Phase::SynReceived || c.phase == Phase::Established {
                    self.control(peer, SYN_ACK, id.0, src);
                }
                return Ok(());
            }
            if !self.config.listen
                || self
                    .connections
                    .values()
                    .filter(|c| c.incoming && !c.accepted)
                    .count()
                    >= self.config.accept_backlog
            {
                return Ok(());
            }
            let Ok(id) = self.allocate() else {
                return Ok(());
            };
            let c = self.make_connection(peer, src, Phase::SynReceived, src as u32);
            self.connections.insert(id, c);
            return Ok(());
        }
        let id = ConnectionId(dst);
        let Some(c) = self.connections.get_mut(&id) else {
            return Ok(());
        };
        if c.peer != peer || c.error.is_some() {
            return Ok(());
        }
        if kind == SYN_ACK
            && !c.incoming
            && (c.phase == Phase::SynSent || (c.phase == Phase::Established && c.remote == src))
        {
            c.remote = src;
            c.phase = Phase::Established;
            c.last_input = self.now;
            self.control(peer, ACK, id.0, src);
            return Ok(());
        }
        if c.remote != src {
            return Ok(());
        }
        if kind == RESET {
            c.error = Some(io::ErrorKind::ConnectionReset);
            return Ok(());
        }
        if kind == ACK && c.phase == Phase::SynReceived {
            c.phase = Phase::Established;
            c.last_input = self.now;
            self.accepts.push_back(id);
            return Ok(());
        }
        if kind == DATA && matches!(c.phase, Phase::SynReceived | Phase::Established) {
            // Limit deferred ACK growth under floods or a blocked output transport.
            if c.kcp.acks.len() >= 256 || !c.kcp.output.is_empty() {
                return Ok(());
            }
            c.kcp
                .input(&packet[HEADER..], self.now as u32)
                .map_err(|_| error(io::ErrorKind::InvalidData))?;
            c.last_input = self.now;
            if c.phase == Phase::SynReceived {
                c.phase = Phase::Established;
                self.accepts.push_back(id);
            }
        }
        Ok(())
    }
    /// Advances every connection. Work and memory are bounded by the configured
    /// connection/window limits. Call after input, writes, reads and timer wakeups.
    pub fn update(&mut self, now: u64) {
        self.now = self.now.max(now);
        let now = self.now;
        let mut controls = Vec::new();
        let mut remove = Vec::new();
        for (&id, c) in &mut self.connections {
            if c.deferred_start {
                c.deferred_start = false;
                c.created = now;
                c.last_input = now;
                c.retry_at = now;
            }
            if c.phase == Phase::Closed {
                continue;
            }
            if c.error.is_some() {
                if c.detached || (c.incoming && !c.accepted) {
                    remove.push(id);
                }
                continue;
            }
            if matches!(c.phase, Phase::SynSent | Phase::SynReceived) {
                if now.saturating_sub(c.created) >= self.config.handshake_timeout {
                    c.error = Some(io::ErrorKind::TimedOut);
                } else if now >= c.retry_at {
                    controls.push((
                        c.peer,
                        if c.phase == Phase::SynSent {
                            SYN
                        } else {
                            SYN_ACK
                        },
                        id.0,
                        c.remote,
                    ));
                    c.retry_at = now.saturating_add(c.retry_delay);
                    c.retry_delay = (c.retry_delay * 2).min(2000);
                }
            } else {
                while let Some(size) = c.kcp.peek_size() {
                    if size == 0 || size > self.config.kcp.mtu - 24 {
                        c.error = Some(io::ErrorKind::InvalidData);
                        break;
                    }
                    if c.receive.len() + size - 1 > self.config.receive_capacity {
                        break;
                    }
                    let record = c.kcp.recv().unwrap();
                    match record[0] {
                        RECORD_DATA if !c.peer_closed => {
                            if !c.detached {
                                c.receive.extend(&record[1..]);
                            }
                        }
                        RECORD_FIN if record.len() == 1 && !c.peer_closed => c.peer_closed = true,
                        _ => {
                            c.error = Some(io::ErrorKind::InvalidData);
                            break;
                        }
                    }
                }
                // WASK/WINS are understood by existing KCP peers. Probes do not consume
                // application send slots and never extend the receive-based idle deadline.
                if c.kcp.output.is_empty() {
                    if !c.detached
                        && !(c.write_closed && c.peer_closed)
                        && let Some(interval) = self.config.keep_alive_interval
                        && now >= c.last_input.max(c.last_probe).saturating_add(interval)
                    {
                        c.kcp.probe |= 1;
                        c.last_probe = now;
                    }
                    // Never accumulate multiple flushes behind an undrained output batch.
                    c.kcp.update(now as u32);
                }
                if c.kcp.is_dead() {
                    c.error = Some(io::ErrorKind::TimedOut);
                }
                if c.write_closed && c.peer_closed && c.kcp.waitsnd() == 0 {
                    let until = *c
                        .linger_until
                        .get_or_insert_with(|| now.saturating_add(self.config.linger));
                    if now >= until {
                        c.phase = Phase::Closed;
                        if c.detached {
                            remove.push(id);
                        }
                    }
                } else if now.saturating_sub(c.last_input) >= self.config.idle_timeout
                    || c.detach_deadline.is_some_and(|deadline| now >= deadline)
                    || (c.kcp.waitsnd() != 0
                        && c.close_started.is_some_and(|start| {
                            now.saturating_sub(start) >= self.config.close_timeout
                        }))
                {
                    c.error = Some(io::ErrorKind::TimedOut);
                }
            }
            if c.error.is_some() && (c.detached || (c.incoming && !c.accepted)) {
                remove.push(id);
            }
        }
        for id in remove {
            self.connections.remove(&id);
        }
        self.accepts
            .retain(|id| self.connections.get(id).is_some_and(|c| c.error.is_none()));
        for (peer, kind, src, dst) in controls {
            self.control(peer, kind, src, dst);
        }
        // Round-robin output prevents a busy connection from monopolizing the queue.
        let ids: Vec<_> = self.connections.keys().copied().collect();
        let start = self
            .output_cursor
            .map_or(0, |cursor| ids.partition_point(|id| *id <= cursor));
        for id in ids[start..].iter().chain(ids[..start].iter()) {
            if self.output.len() >= self.config.output_capacity {
                break;
            }
            let c = self.connections.get_mut(id).unwrap();
            if c.error.is_none()
                && let Some(p) = c.kcp.poll_output()
            {
                self.output.push_back(Transmit {
                    peer: c.peer,
                    payload: envelope(DATA, id.0, c.remote, &p),
                });
                self.output_cursor = Some(*id);
            }
        }
    }
    /// Next timer deadline. Call update again after draining output to pump remaining
    /// packets. A blocked transport must not turn a pending output batch into a busy loop.
    pub fn deadline(&self) -> Option<u64> {
        self.connections
            .values()
            .filter(|c| c.error.is_none() && c.phase != Phase::Closed)
            .map(|c| {
                if matches!(c.phase, Phase::SynSent | Phase::SynReceived) {
                    c.retry_at
                        .min(c.created.saturating_add(self.config.handshake_timeout))
                } else {
                    let delay = c.kcp.check(self.now as u32).wrapping_sub(self.now as u32) as u64;
                    let mut at = if c.kcp.output.is_empty() {
                        self.now.saturating_add(delay)
                    } else {
                        self.now.saturating_add(u64::from(self.config.kcp.interval))
                    };
                    if let Some(until) = c.linger_until {
                        at = at.min(until);
                    } else {
                        at = at.min(c.last_input.saturating_add(self.config.idle_timeout));
                        if let Some(deadline) = c.detach_deadline {
                            at = at.min(deadline);
                        }
                        if !c.detached
                            && !(c.write_closed && c.peer_closed)
                            && c.kcp.output.is_empty()
                            && let Some(interval) = self.config.keep_alive_interval
                        {
                            at = at.min(c.last_input.max(c.last_probe).saturating_add(interval));
                        }
                        if c.kcp.waitsnd() != 0
                            && let Some(start) = c.close_started
                        {
                            at = at.min(start.saturating_add(self.config.close_timeout));
                        }
                    }
                    at
                }
            })
            .min()
    }
}
fn envelope(kind: u8, src: u64, dst: u64, payload: &[u8]) -> Vec<u8> {
    let mut p = Vec::with_capacity(HEADER + payload.len());
    p.extend_from_slice(MAGIC);
    p.extend([VERSION, kind]);
    p.extend(src.to_le_bytes());
    p.extend(dst.to_le_bytes());
    p.extend_from_slice(payload);
    p
}

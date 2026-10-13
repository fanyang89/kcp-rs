//! Runtime-independent KCP. The caller owns transport, scheduling and backpressure.
//! Times are wrapping monotonic milliseconds; service the connection regularly.
#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

use std::collections::VecDeque;

const HEADER: usize = 24;
const PUSH: u8 = 81;
const ACK: u8 = 82;
const WASK: u8 = 83;
const WINS: u8 = 84;
fn diff(a: u32, b: u32) -> i32 {
    a.wrapping_sub(b) as i32
}

/// Configuration is fixed for the lifetime of a connection.
#[derive(Clone, Debug)]
pub struct Config {
    pub mtu: usize,
    pub send_window: u16,
    /// Must be at least 128 so a maximum-sized message can be reassembled.
    pub receive_window: u16,
    pub interval: u32,
    pub nodelay: bool,
    /// Number of later acknowledgements before fast retransmission; zero disables it.
    pub fast_resend: u32,
    pub congestion_control: bool,
    /// Removes message boundaries. `recv` still returns individual available chunks.
    pub stream: bool,
    pub dead_link: u32,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            mtu: 1400,
            send_window: 32,
            receive_window: 128,
            interval: 100,
            nodelay: false,
            fast_resend: 0,
            congestion_control: true,
            stream: false,
            dead_link: 20,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidConfig,
    InvalidPacket,
    ConversationMismatch,
    MessageTooLarge,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidConfig => "invalid KCP configuration",
            Self::InvalidPacket => "invalid KCP packet",
            Self::ConversationMismatch => "KCP conversation mismatch",
            Self::MessageTooLarge => "message exceeds 127 fragments",
        })
    }
}
impl std::error::Error for Error {}

#[derive(Default, Debug)]
struct Segment {
    frg: u8,
    sn: u32,
    ts: u32,
    data: Vec<u8>,
    resend: u32,
    rto: u32,
    fast: u32,
    xmit: u32,
}

/// A single KCP conversation, with no sockets, threads, timers or runtime dependencies.
/// Queues are owned by this object; callers must bound sends and drain output regularly.
#[derive(Debug)]
pub struct Kcp {
    conv: u32,
    config: Config,
    send_queue: VecDeque<Segment>,
    send_buf: VecDeque<Segment>,
    recv_queue: VecDeque<Segment>,
    recv_buf: VecDeque<Segment>,
    output: VecDeque<Vec<u8>>,
    acks: Vec<(u32, u32)>,
    snd_nxt: u32,
    rcv_nxt: u32,
    remote_window: u32,
    cwnd: u32,
    ssthresh: u32,
    incr: u64,
    srtt: i64,
    rttvar: i64,
    rto: u32,
    current: u32,
    next_flush: u32,
    updated: bool,
    probe: u8,
    probe_wait: u32,
    probe_at: u32,
    dead: bool,
}
impl Kcp {
    pub fn new(conv: u32) -> Self {
        Self::with_config(conv, Config::default()).unwrap()
    }
    pub fn with_config(conv: u32, config: Config) -> Result<Self, Error> {
        if !(50..=65535).contains(&config.mtu)
            || config.send_window == 0
            || config.receive_window < 128
            || !(10..=5000).contains(&config.interval)
            || config.dead_link == 0
            || config.fast_resend > 65535
        {
            return Err(Error::InvalidConfig);
        }
        Ok(Self {
            conv,
            config,
            send_queue: VecDeque::new(),
            send_buf: VecDeque::new(),
            recv_queue: VecDeque::new(),
            recv_buf: VecDeque::new(),
            output: VecDeque::new(),
            acks: Vec::new(),
            snd_nxt: 0,
            rcv_nxt: 0,
            remote_window: 128,
            cwnd: 0,
            ssthresh: 2,
            incr: 0,
            srtt: 0,
            rttvar: 0,
            rto: 200,
            current: 0,
            next_flush: 0,
            updated: false,
            probe: 0,
            probe_wait: 0,
            probe_at: 0,
            dead: false,
        })
    }
    pub fn conversation(&self) -> u32 {
        self.conv
    }
    pub fn is_dead(&self) -> bool {
        self.dead
    }
    /// Number of queued and unacknowledged segments (not bytes).
    pub fn waitsnd(&self) -> usize {
        self.send_queue.len() + self.send_buf.len()
    }
    fn mss(&self) -> usize {
        self.config.mtu - HEADER
    }
    fn una(&self) -> u32 {
        self.send_buf.front().map_or(self.snd_nxt, |s| s.sn)
    }
    /// Queues a message atomically. Empty messages are supported in message mode.
    pub fn send(&mut self, mut data: &[u8]) -> Result<(), Error> {
        let mss = self.mss();
        if !self.config.stream && data.len().div_ceil(mss) >= 128 {
            return Err(Error::MessageTooLarge);
        }
        if self.config.stream {
            if let Some(last) = self.send_queue.back_mut() {
                let n = (mss - last.data.len()).min(data.len());
                last.data.extend_from_slice(&data[..n]);
                data = &data[n..];
            }
            if data.is_empty() {
                return Ok(());
            }
        }
        let count = data.len().div_ceil(mss).max(1);
        for i in 0..count {
            let n = data.len().min(mss);
            self.send_queue.push_back(Segment {
                frg: if self.config.stream {
                    0
                } else {
                    (count - i - 1) as u8
                },
                data: data[..n].to_vec(),
                ..Default::default()
            });
            data = &data[n..];
        }
        Ok(())
    }
    fn promote(&mut self) {
        while self.recv_queue.len() < usize::from(self.config.receive_window)
            && self.recv_buf.front().is_some_and(|s| s.sn == self.rcv_nxt)
        {
            self.recv_queue
                .push_back(self.recv_buf.pop_front().unwrap());
            self.rcv_nxt = self.rcv_nxt.wrapping_add(1);
        }
    }
    pub fn peek_size(&self) -> Option<usize> {
        let first = self.recv_queue.front()?;
        if self.recv_queue.len() <= usize::from(first.frg) {
            return None;
        }
        let mut size = 0;
        for s in &self.recv_queue {
            size += s.data.len();
            if s.frg == 0 {
                return Some(size);
            }
        }
        None
    }
    /// Returns a complete message, or a chunk in stream mode. `None` means not ready.
    pub fn recv(&mut self) -> Option<Vec<u8>> {
        let size = self.peek_size()?;
        let full = self.recv_queue.len() >= usize::from(self.config.receive_window);
        let mut data = Vec::with_capacity(size);
        while let Some(s) = self.recv_queue.pop_front() {
            data.extend_from_slice(&s.data);
            if s.frg == 0 {
                break;
            }
        }
        self.promote();
        if full && self.recv_queue.len() < usize::from(self.config.receive_window) {
            self.probe |= 2;
        }
        Some(data)
    }
    /// Takes one outgoing datagram. Preserve datagram boundaries when transmitting.
    pub fn poll_output(&mut self) -> Option<Vec<u8>> {
        self.output.pop_front()
    }
    /// Accepts a datagram at the supplied monotonic time. Invalid datagrams do not mutate state.
    pub fn input(&mut self, packet: &[u8], now: u32) -> Result<(), Error> {
        if packet.len() < HEADER {
            return Err(Error::InvalidPacket);
        }
        let mut rest = packet;
        while !rest.is_empty() {
            if rest.len() < HEADER {
                return Err(Error::InvalidPacket);
            }
            if u32_at(rest, 0) != self.conv {
                return Err(Error::ConversationMismatch);
            }
            let len = u32_at(rest, 20) as usize;
            if !(PUSH..=WINS).contains(&rest[4])
                || len > rest.len() - HEADER
                || (rest[4] == PUSH && rest[5] >= 128)
            {
                return Err(Error::InvalidPacket);
            }
            rest = &rest[HEADER + len..];
        }
        self.current = now;
        let prev_una = self.una();
        let mut max_ack = None;
        rest = packet;
        while !rest.is_empty() {
            let cmd = rest[4];
            let frg = rest[5];
            self.remote_window = u16::from_le_bytes([rest[6], rest[7]]) as u32;
            let ts = u32_at(rest, 8);
            let sn = u32_at(rest, 12);
            let una = u32_at(rest, 16);
            let len = u32_at(rest, 20) as usize;
            while self.send_buf.front().is_some_and(|s| diff(una, s.sn) > 0) {
                self.send_buf.pop_front();
            }
            match cmd {
                ACK => {
                    let rtt = diff(now, ts);
                    if rtt >= 0 {
                        let rtt = i64::from(rtt);
                        if self.srtt == 0 {
                            self.srtt = rtt;
                            self.rttvar = rtt / 2;
                        } else {
                            self.rttvar = (3 * self.rttvar + (rtt - self.srtt).abs()) / 4;
                            self.srtt = ((7 * self.srtt + rtt) / 8).max(1);
                        }
                        self.rto = (self.srtt
                            + (i64::from(self.config.interval)).max(4 * self.rttvar))
                        .clamp(if self.config.nodelay { 30 } else { 100 }, 60000)
                            as u32;
                    }
                    if diff(sn, self.una()) >= 0
                        && diff(sn, self.snd_nxt) < 0
                        && let Some(i) = self.send_buf.iter().position(|s| s.sn == sn)
                    {
                        self.send_buf.remove(i);
                    }
                    if max_ack.is_none_or(|old| diff(sn, old) > 0) {
                        max_ack = Some(sn);
                    }
                }
                PUSH => {
                    if diff(
                        sn,
                        self.rcv_nxt
                            .wrapping_add(u32::from(self.config.receive_window)),
                    ) < 0
                    {
                        self.acks.push((sn, ts));
                        if diff(sn, self.rcv_nxt) >= 0 && !self.recv_buf.iter().any(|s| s.sn == sn)
                        {
                            let pos = self
                                .recv_buf
                                .iter()
                                .position(|s| diff(sn, s.sn) < 0)
                                .unwrap_or(self.recv_buf.len());
                            self.recv_buf.insert(
                                pos,
                                Segment {
                                    sn,
                                    frg,
                                    data: rest[HEADER..HEADER + len].to_vec(),
                                    ..Default::default()
                                },
                            );
                            self.promote();
                        }
                    }
                }
                WASK => self.probe |= 2,
                _ => {}
            }
            rest = &rest[HEADER + len..];
        }
        if let Some(sn) = max_ack
            && diff(sn, self.una()) >= 0
            && diff(sn, self.snd_nxt) < 0
        {
            for s in &mut self.send_buf {
                if diff(sn, s.sn) > 0 {
                    s.fast = s.fast.saturating_add(1);
                }
            }
        }
        if diff(self.una(), prev_una) > 0 && self.cwnd < self.remote_window {
            let mss = self.mss() as u64;
            if self.cwnd < self.ssthresh {
                self.cwnd += 1;
                self.incr += mss;
            } else {
                self.incr = self.incr.max(mss);
                self.incr += mss * mss / self.incr + mss / 16;
                if u64::from(self.cwnd + 1) * mss <= self.incr {
                    self.cwnd = self.incr.div_ceil(mss) as u32;
                }
            }
            if self.cwnd > self.remote_window {
                self.cwnd = self.remote_window;
                self.incr = u64::from(self.cwnd) * mss;
            }
        }
        Ok(())
    }
    /// Advances timers and produces datagrams. Call after sends/input and at `check` deadlines.
    pub fn update(&mut self, now: u32) {
        self.current = now;
        if !self.updated {
            self.updated = true;
            self.next_flush = now;
        }
        let mut delta = diff(now, self.next_flush);
        if !(-10000..10000).contains(&delta) {
            self.next_flush = now;
            delta = 0;
        }
        if delta >= 0 {
            self.next_flush = self.next_flush.wrapping_add(self.config.interval);
            if diff(now, self.next_flush) >= 0 {
                self.next_flush = now.wrapping_add(self.config.interval);
            }
            self.flush();
        }
    }
    /// Absolute wrapping timestamp for the next update, valid until the next send/input.
    pub fn check(&self, now: u32) -> u32 {
        let delta = diff(self.next_flush, now);
        if !self.updated || delta <= 0 || delta > 10000 {
            return now;
        }
        let mut wait = (delta as u32).min(self.config.interval);
        for s in &self.send_buf {
            let d = diff(s.resend, now);
            if d <= 0 {
                return now;
            }
            wait = wait.min(d as u32);
        }
        now.wrapping_add(wait)
    }
    /// Flushes immediately using the latest supplied time; no-op before the first update.
    pub fn flush(&mut self) {
        if !self.updated {
            return;
        }
        let now = self.current;
        let wnd = (usize::from(self.config.receive_window) - self.recv_queue.len()) as u16;
        let mut packets = PacketWriter {
            conv: self.conv,
            wnd,
            una: self.rcv_nxt,
            mtu: self.config.mtu,
            buf: Vec::new(),
            out: &mut self.output,
        };
        for (sn, ts) in self.acks.drain(..) {
            packets.push(ACK, 0, ts, sn, &[]);
        }
        if self.remote_window == 0 {
            if self.probe_wait == 0 {
                self.probe_wait = 7000;
                self.probe_at = now.wrapping_add(self.probe_wait);
            } else if diff(now, self.probe_at) >= 0 {
                self.probe_wait = (self.probe_wait + self.probe_wait / 2).min(120000);
                self.probe_at = now.wrapping_add(self.probe_wait);
                self.probe |= 1;
            }
        } else {
            self.probe_wait = 0;
            self.probe_at = 0;
        }
        if self.probe & 1 != 0 {
            packets.push(WASK, 0, 0, 0, &[]);
        }
        if self.probe & 2 != 0 {
            packets.push(WINS, 0, 0, 0, &[]);
        }
        self.probe = 0;
        let mut window = u32::from(self.config.send_window).min(self.remote_window);
        if self.config.congestion_control {
            window = window.min(self.cwnd);
        }
        let una = self.send_buf.front().map_or(self.snd_nxt, |s| s.sn);
        while diff(self.snd_nxt, una.wrapping_add(window)) < 0 {
            let Some(mut s) = self.send_queue.pop_front() else {
                break;
            };
            s.sn = self.snd_nxt;
            self.snd_nxt = self.snd_nxt.wrapping_add(1);
            self.send_buf.push_back(s);
        }
        let mut lost = false;
        let mut change = false;
        for s in &mut self.send_buf {
            let send = if s.xmit == 0 {
                s.rto = self.rto;
                s.resend = now
                    .wrapping_add(s.rto)
                    .wrapping_add(if self.config.nodelay { 0 } else { self.rto / 8 });
                true
            } else if diff(now, s.resend) >= 0 {
                s.rto = s
                    .rto
                    .saturating_add(if self.config.nodelay {
                        s.rto / 2
                    } else {
                        s.rto.max(self.rto)
                    })
                    .min(i32::MAX as u32);
                s.resend = now.wrapping_add(s.rto);
                lost = true;
                true
            } else if self.config.fast_resend > 0
                && s.fast >= self.config.fast_resend
                && s.xmit <= 5
            {
                s.fast = 0;
                s.resend = now.wrapping_add(s.rto);
                change = true;
                true
            } else {
                false
            };
            if send {
                s.xmit = s.xmit.saturating_add(1);
                s.ts = now;
                packets.push(PUSH, s.frg, now, s.sn, &s.data);
                if s.xmit >= self.config.dead_link {
                    self.dead = true;
                }
            }
        }
        packets.finish();
        let mss = self.mss() as u64;
        if change {
            self.ssthresh = (self.snd_nxt.wrapping_sub(self.una()) / 2).max(2);
            self.cwnd = self.ssthresh + self.config.fast_resend;
            self.incr = u64::from(self.cwnd) * mss;
        }
        if lost {
            self.ssthresh = (window / 2).max(2);
            self.cwnd = 1;
            self.incr = mss;
        }
        if self.cwnd < 1 {
            self.cwnd = 1;
            self.incr = mss;
        }
    }
}
fn u32_at(data: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap())
}
struct PacketWriter<'a> {
    conv: u32,
    wnd: u16,
    una: u32,
    mtu: usize,
    buf: Vec<u8>,
    out: &'a mut VecDeque<Vec<u8>>,
}
impl PacketWriter<'_> {
    fn push(&mut self, cmd: u8, frg: u8, ts: u32, sn: u32, data: &[u8]) {
        if self.buf.len() + HEADER + data.len() > self.mtu {
            self.finish();
        }
        self.buf.extend_from_slice(&self.conv.to_le_bytes());
        self.buf.extend_from_slice(&[cmd, frg]);
        self.buf.extend_from_slice(&self.wnd.to_le_bytes());
        for x in [ts, sn, self.una, data.len() as u32] {
            self.buf.extend_from_slice(&x.to_le_bytes());
        }
        self.buf.extend_from_slice(data);
    }
    fn finish(&mut self) {
        if !self.buf.is_empty() {
            self.out.push_back(std::mem::take(&mut self.buf));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequence_numbers_wrap() {
        let config = Config {
            congestion_control: false,
            interval: 10,
            ..Config::default()
        };
        let mut a = Kcp::with_config(1, config.clone()).unwrap();
        let mut b = Kcp::with_config(1, config).unwrap();
        a.snd_nxt = u32::MAX - 1;
        b.rcv_nxt = u32::MAX - 1;
        for i in 0..4 {
            a.send(&[i]).unwrap();
        }
        a.update(0);
        while let Some(p) = a.poll_output() {
            b.input(&p, 0).unwrap();
        }
        for i in 0..4 {
            assert_eq!(b.recv(), Some(vec![i]));
        }
        b.update(10);
        while let Some(p) = b.poll_output() {
            a.input(&p, 10).unwrap();
        }
        assert_eq!(a.waitsnd(), 0);
        assert_eq!(a.snd_nxt, 2);
        assert_eq!(b.rcv_nxt, 2);
    }

    #[test]
    fn arbitrary_packets_do_not_panic() {
        let mut kcp = Kcp::new(7);
        let mut seed = 123u32;
        for len in 0..2048 {
            let mut packet = vec![0; len];
            for byte in &mut packet {
                seed ^= seed << 13;
                seed ^= seed >> 17;
                seed ^= seed << 5;
                *byte = seed as u8;
            }
            if len >= 4 {
                packet[..4].copy_from_slice(&7u32.to_le_bytes());
            }
            let _ = kcp.input(&packet, len as u32);
        }
    }
}

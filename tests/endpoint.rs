use kcp_rs::endpoint::{Config, ConnectionId, EndpointCore};
use std::{io, net::SocketAddr};

fn addr(n: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], n))
}
fn config() -> Config {
    Config {
        send_capacity: 8,
        receive_capacity: 1400,
        output_capacity: 2,
        linger: 200,
        ..Config::default()
    }
}
struct Pair {
    a: EndpointCore,
    b: EndpointCore,
    now: u64,
    packets: u64,
    network: Vec<(u64, bool, Vec<u8>)>,
}
impl Pair {
    fn new() -> Self {
        Self {
            a: EndpointCore::new(config(), 100).unwrap(),
            b: EndpointCore::new(config(), 200).unwrap(),
            now: 0,
            packets: 0,
            network: Vec::new(),
        }
    }
    fn step(&mut self, loss: bool) {
        self.now += 10;
        self.a.update(self.now);
        self.b.update(self.now);
        for (target, core) in [(true, &mut self.a), (false, &mut self.b)] {
            while let Some(p) = core.poll_transmit() {
                assert_eq!(p.peer, addr(if target { 2 } else { 1 }));
                assert!(p.payload.len() <= 1400);
                self.packets += 1;
                if loss && self.packets.is_multiple_of(5) {
                    continue;
                }
                if loss && self.packets.is_multiple_of(7) {
                    self.network
                        .push((self.now + 30, target, p.payload.clone()));
                }
                self.network.push((
                    self.now + if loss { self.packets % 4 * 10 } else { 0 },
                    target,
                    p.payload,
                ));
            }
        }
        for i in (0..self.network.len()).rev() {
            if self.network[i].0 <= self.now {
                let (_, target, p) = self.network.swap_remove(i);
                if target {
                    self.b.input(addr(1), &p, self.now).unwrap();
                } else {
                    self.a.input(addr(2), &p, self.now).unwrap();
                }
            }
        }
    }
    fn connect(&mut self, loss: bool) -> (ConnectionId, ConnectionId) {
        let a = self.a.connect(addr(2), self.now).unwrap();
        let mut b = None;
        for _ in 0..2000 {
            self.step(loss);
            if b.is_none() {
                b = self.b.accept();
            }
            if self.a.is_connected(a).unwrap()
                && let Some(b) = b
            {
                return (a, b);
            }
        }
        panic!("handshake timed out");
    }
}
#[test]
fn lost_handshake_records_half_close_and_linger() {
    let mut p = Pair::new();
    let (a, b) = p.connect(true);
    let bytes: Vec<_> = (0..200000).map(|i| i as u8).collect();
    let mut sent = 0;
    let mut received = Vec::new();
    let mut fin = false;
    let mut eof = false;
    for _ in 0..20000 {
        if sent < bytes.len() {
            if let Some(n) = p.a.write(a, &bytes[sent..]).unwrap() {
                sent += n;
            }
        } else if !fin {
            fin = p.a.close_write(a).unwrap();
        }
        p.step(true);
        let mut buf = [0; 311];
        while let Some(n) = p.b.read(b, &mut buf).unwrap() {
            if n == 0 {
                eof = true;
                break;
            }
            received.extend_from_slice(&buf[..n]);
        }
        if eof && p.a.is_flushed(a).unwrap() {
            break;
        }
    }
    assert!(fin && eof);
    assert_eq!(received, bytes);
    assert!(p.a.is_flushed(a).unwrap());
    assert_eq!(
        p.a.write(a, b"no").unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    // The other half can still write, including after the FIN's close timeout.
    p.now += config().close_timeout + 1;
    p.step(false);
    assert_eq!(p.b.write(b, b"reply").unwrap(), Some(5));
    assert!(p.b.close_write(b).unwrap());
    let mut reply = Vec::new();
    let mut eof = false;
    for _ in 0..1000 {
        p.step(true);
        let mut buf = [0; 32];
        while let Some(n) = p.a.read(a, &mut buf).unwrap() {
            if n == 0 {
                eof = true;
                break;
            }
            reply.extend_from_slice(&buf[..n]);
        }
        if eof && p.b.is_flushed(b).unwrap() {
            break;
        }
    }
    assert!(eof);
    assert_eq!(reply, b"reply");
    p.a.release(a);
    p.b.release(b);
    for _ in 0..100 {
        p.step(false);
    }
    assert_eq!(p.a.connection_count() + p.b.connection_count(), 0);
}
#[test]
fn backpressure_stops_writes_and_resumes_after_reads() {
    let mut p = Pair::new();
    let (a, b) = p.connect(false);
    let mut accepted = 0;
    for _ in 0..1000 {
        while let Some(n) = p.a.write(a, &[42; 1353]).unwrap() {
            accepted += n;
        }
        p.step(false);
    }
    assert!(accepted > 0 && accepted < 1_000_000);
    assert_eq!(p.a.write(a, b"x").unwrap(), None);
    let mut read = 0;
    for _ in 0..2000 {
        let mut buf = [0; 4096];
        while let Some(n) = p.b.read(b, &mut buf).unwrap() {
            assert!(n > 0);
            read += n;
        }
        p.step(false);
        if p.a.is_flushed(a).unwrap() && read == accepted {
            break;
        }
    }
    assert_eq!(read, accepted);
    assert!(p.a.is_flushed(a).unwrap());
    assert_eq!(p.a.write(a, b"x").unwrap(), Some(1));
}
#[test]
fn limits_timeouts_reset_and_address_isolation() {
    let mut a = EndpointCore::new(
        Config {
            max_connections: 1,
            handshake_timeout: 100,
            ..config()
        },
        1,
    )
    .unwrap();
    let id = a.connect(addr(2), 0).unwrap();
    assert_eq!(
        a.connect(addr(3), 0).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    a.update(100);
    assert_eq!(
        a.is_connected(id).unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
    a.release(id);
    assert_eq!(a.connection_count(), 0);
    let mut p = Pair::new();
    let (a, b) = p.connect(false);
    p.a.abort(a);
    let reset = p.a.poll_transmit().unwrap();
    assert_eq!(reset.payload[5], 5);
    p.b.input(addr(3), &reset.payload, p.now).unwrap();
    assert!(p.b.is_connected(b).unwrap());
    p.b.input(addr(1), &reset.payload, p.now).unwrap();
    assert_eq!(
        p.b.read(b, &mut [0; 1]).unwrap_err().kind(),
        io::ErrorKind::ConnectionReset
    );
    p.b.release(b);
    assert_eq!(p.b.connection_count(), 0);
}
#[test]
fn malformed_envelopes_and_stale_sessions_do_not_create_connections() {
    let mut p = Pair::new();
    let id = p.a.connect(addr(2), 0).unwrap();
    p.a.update(0);
    let syn = p.a.poll_transmit().unwrap();
    for n in 0..22 {
        assert!(p.b.input(addr(1), &syn.payload[..n], 0).is_err());
    }
    let mut bad = syn.payload.clone();
    bad[4] = 2;
    assert!(p.b.input(addr(1), &bad, 0).is_err());
    assert_eq!(p.b.connection_count(), 0);
    p.b.input(addr(1), &syn.payload, 0).unwrap();
    p.b.input(addr(1), &syn.payload, 0).unwrap();
    assert_eq!(p.b.connection_count(), 1);
    p.a.abort(id);
    p.b.update(20000);
    assert_eq!(p.b.connection_count(), 0);
    let next = p.a.connect(addr(2), 20000).unwrap();
    assert_ne!(id, next);
    let mut stale = syn.payload;
    stale[5] = 4;
    stale[14..22].copy_from_slice(&id.0.to_le_bytes());
    stale.extend([0; 24]);
    p.a.input(addr(2), &stale, 20000).unwrap();
    assert!(!p.a.is_connected(next).unwrap());
}
#[test]
fn accept_backlog_and_session_exhaustion() {
    let mut server = EndpointCore::new(
        Config {
            accept_backlog: 1,
            ..config()
        },
        u64::MAX,
    )
    .unwrap();
    let mut client = EndpointCore::new(config(), 100).unwrap();
    client.connect(addr(2), 0).unwrap();
    client.connect(addr(2), 0).unwrap();
    client.update(0);
    while let Some(syn) = client.poll_transmit() {
        server.input(addr(1), &syn.payload, 0).unwrap();
    }
    assert_eq!(server.connection_count(), 1);
    assert_eq!(
        server.connect(addr(3), 0).unwrap_err().kind(),
        io::ErrorKind::AddrNotAvailable
    );
}

#[test]
fn each_handshake_leg_can_be_lost_without_application_data() {
    for lost_kind in [1, 2, 3] {
        let mut a = EndpointCore::new(config(), 1).unwrap();
        let mut b = EndpointCore::new(config(), 100).unwrap();
        let id = a.connect(addr(2), 0).unwrap();
        let mut dropped = false;
        let mut accepted = None;
        for now in (0..10000).step_by(10) {
            a.update(now);
            b.update(now);
            while let Some(p) = a.poll_transmit() {
                if !dropped && p.payload[5] == lost_kind {
                    dropped = true;
                    continue;
                }
                b.input(addr(1), &p.payload, now).unwrap();
            }
            while let Some(p) = b.poll_transmit() {
                if !dropped && p.payload[5] == lost_kind {
                    dropped = true;
                    continue;
                }
                a.input(addr(2), &p.payload, now).unwrap();
            }
            if accepted.is_none() {
                accepted = b.accept();
            }
            if accepted.is_some() && a.is_connected(id).unwrap() {
                break;
            }
        }
        assert!(dropped);
        assert!(accepted.is_some());
        assert!(a.is_connected(id).unwrap());
        assert_eq!(b.accept(), None);
    }
}
#[test]
fn releasing_after_linger_does_not_reset_a_graceful_peer() {
    let mut p = Pair::new();
    let (a, b) = p.connect(false);
    assert!(p.a.close_write(a).unwrap());
    assert!(p.b.close_write(b).unwrap());
    for _ in 0..100 {
        p.step(false);
    }
    p.a.release(a);
    p.b.release(b);
    assert!(p.a.poll_transmit().is_none());
    assert!(p.b.poll_transmit().is_none());
    assert_eq!(p.a.connection_count() + p.b.connection_count(), 0);
}
#[test]
fn established_idle_timeout_and_fin_timeout_are_errors() {
    let mut p = Pair::new();
    let (a, b) = p.connect(false);
    p.now += config().idle_timeout + 1;
    p.a.update(p.now);
    p.b.update(p.now);
    assert_eq!(
        p.a.read(a, &mut [0; 1]).unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
    assert_eq!(
        p.b.read(b, &mut [0; 1]).unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
    let mut p = Pair::new();
    let (a, _) = p.connect(false);
    assert!(p.a.close_write(a).unwrap());
    p.now += config().close_timeout + 1;
    p.a.update(p.now);
    assert_eq!(
        p.a.is_flushed(a).unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
}

#[test]
fn multiplexed_connections_make_progress_with_a_small_output_queue() {
    let mut p = Pair::new();
    let ids: Vec<_> = (0..10).map(|_| p.a.connect(addr(2), 0).unwrap()).collect();
    let mut accepted = Vec::new();
    for _ in 0..2000 {
        p.step(false);
        while let Some(id) = p.b.accept() {
            accepted.push(id);
        }
        if accepted.len() == 10 && ids.iter().all(|id| p.a.is_connected(*id).unwrap()) {
            break;
        }
    }
    assert_eq!(accepted.len(), 10);
    for (i, id) in ids.iter().enumerate() {
        assert_eq!(p.a.write(*id, &[i as u8; 100]).unwrap(), Some(100));
        assert!(p.a.close_write(*id).unwrap());
    }
    let mut messages: Vec<Vec<u8>> = vec![Vec::new(); 10];
    let mut eof = [false; 10];
    for _ in 0..2000 {
        p.step(true);
        for (i, id) in accepted.iter().enumerate() {
            let mut buf = [0; 64];
            while let Some(n) = p.b.read(*id, &mut buf).unwrap() {
                if n == 0 {
                    eof[i] = true;
                    break;
                }
                messages[i].extend_from_slice(&buf[..n]);
            }
        }
        if eof.iter().all(|v| *v) {
            break;
        }
    }
    assert!(eof.iter().all(|v| *v));
    messages.sort();
    assert_eq!(messages, (0..10).map(|i| vec![i; 100]).collect::<Vec<_>>());
}

#[test]
fn readiness_tracks_data_capacity_eof_and_errors() {
    let mut p = Pair::new();
    let (a, b) = p.connect(false);
    assert!(!p.a.is_readable(a).unwrap());
    assert!(p.a.is_writable(a).unwrap());
    assert!(!p.b.has_pending_accept());
    for _ in 0..config().send_capacity {
        assert_eq!(p.a.write(a, b"x").unwrap(), Some(1));
    }
    assert!(!p.a.is_writable(a).unwrap());
    for _ in 0..100 {
        p.step(false);
        if p.b.is_readable(b).unwrap() && p.a.is_flushed(a).unwrap() {
            break;
        }
    }
    assert!(p.b.is_readable(b).unwrap());
    assert!(p.a.is_writable(a).unwrap());
    let mut data = [0; 8];
    assert_eq!(p.b.read(b, &mut data).unwrap(), Some(8));
    assert!(!p.b.is_readable(b).unwrap());
    assert!(p.a.close_write(a).unwrap());
    assert_eq!(
        p.a.is_writable(a).unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    for _ in 0..100 {
        p.step(false);
        if p.b.is_readable(b).unwrap() {
            break;
        }
    }
    assert!(p.b.is_readable(b).unwrap());
    assert_eq!(p.b.read(b, &mut data).unwrap(), Some(0));
    p.a.abort(a);
    for _ in 0..10 {
        p.step(false);
    }
    assert_eq!(
        p.b.is_readable(b).unwrap_err().kind(),
        io::ErrorKind::ConnectionReset
    );
    assert_eq!(
        p.b.is_writable(b).unwrap_err().kind(),
        io::ErrorKind::ConnectionReset
    );
}

fn keep_alive_pair(client: bool, server: bool) -> Pair {
    let mut p = Pair::new();
    let config = Config {
        idle_timeout: 500,
        close_timeout: 200,
        keep_alive_interval: Some(50),
        ..config()
    };
    p.a = EndpointCore::new(
        Config {
            keep_alive_interval: client.then_some(50),
            ..config.clone()
        },
        100,
    )
    .unwrap();
    p.b = EndpointCore::new(
        Config {
            keep_alive_interval: server.then_some(50),
            ..config
        },
        200,
    )
    .unwrap();
    p
}

#[test]
fn optional_keep_alive_survives_idle_loss_and_timestamp_wrap() {
    // One-sided probing is sufficient: the other peer responds with ordinary WINS.
    for (client, server) in [(true, false), (false, true), (true, true)] {
        let mut p = keep_alive_pair(client, server);
        p.now = u64::from(u32::MAX) - 100;
        let (a, b) = p.connect(false);
        for _ in 0..1000 {
            p.step(true);
        }
        assert!(p.a.is_connected(a).unwrap());
        assert!(p.b.is_connected(b).unwrap());
        assert_eq!(p.a.read(a, &mut [0; 1]).unwrap(), None);
        assert_eq!(p.b.read(b, &mut [0; 1]).unwrap(), None);
        assert!(p.a.is_flushed(a).unwrap());
        assert!(p.b.is_flushed(b).unwrap());
    }
}

#[test]
fn disabled_keep_alive_still_expires_idle_connections() {
    let mut p = keep_alive_pair(false, false);
    let (a, b) = p.connect(false);
    for _ in 0..100 {
        p.step(false);
    }
    assert_eq!(
        p.a.is_connected(a).unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
    assert_eq!(
        p.b.is_connected(b).unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
}

#[test]
fn sending_probes_without_replies_does_not_extend_idle_timeout() {
    let mut p = keep_alive_pair(true, false);
    let (a, _) = p.connect(false);
    let start = p.now;
    let mut probes = 0;
    while p.now < start + 600 {
        p.now += 10;
        p.a.update(p.now);
        while let Some(packet) = p.a.poll_transmit() {
            assert_eq!(packet.payload[5], 4);
            assert_eq!(packet.payload.len(), 22 + 24);
            assert_eq!(packet.payload[22 + 4], 83); // KCP WASK, no application record.
            probes += 1;
        }
        assert!(p.a.deadline().is_none_or(|deadline| deadline > p.now));
    }
    assert!(probes >= 2);
    assert_eq!(
        p.a.is_connected(a).unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
}

#[test]
fn probes_preserve_half_close_but_do_not_prevent_detached_cleanup() {
    let mut p = keep_alive_pair(true, true);
    let (a, b) = p.connect(false);
    assert!(p.a.close_write(a).unwrap());
    for _ in 0..100 {
        p.step(false);
    }
    assert!(p.a.is_flushed(a).unwrap());
    assert_eq!(p.b.read(b, &mut [0; 1]).unwrap(), Some(0));
    // An acknowledged half-close is still readable after close_timeout.
    assert_eq!(p.b.write(b, b"reply").unwrap(), Some(5));
    for _ in 0..20 {
        p.step(false);
    }
    let mut reply = [0; 5];
    assert_eq!(p.a.read(a, &mut reply).unwrap(), Some(5));
    assert_eq!(&reply, b"reply");
    p.a.release(a);
    // The peer continues to respond/probe, but an abandoned handle has a fixed budget.
    for _ in 0..30 {
        p.step(false);
    }
    assert_eq!(p.a.connection_count(), 0);
}

#[test]
fn keep_alive_does_not_extend_graceful_linger() {
    let mut p = keep_alive_pair(true, true);
    let (a, b) = p.connect(false);
    assert!(p.a.close_write(a).unwrap());
    assert!(p.b.close_write(b).unwrap());
    p.a.release(a);
    p.b.release(b);
    for _ in 0..100 {
        p.step(false);
    }
    assert_eq!(p.a.connection_count() + p.b.connection_count(), 0);
}

#[test]
fn keep_alive_configuration_requires_a_sensible_interval() {
    for interval in [0, 1, 9, 500, 1000] {
        assert!(
            EndpointCore::new(
                Config {
                    idle_timeout: 500,
                    keep_alive_interval: Some(interval),
                    ..config()
                },
                1
            )
            .is_err()
        );
    }
    assert!(
        EndpointCore::new(
            Config {
                idle_timeout: 500,
                keep_alive_interval: Some(10),
                ..config()
            },
            1
        )
        .is_ok()
    );
}

use kcp_rs::{Config, Error, Kcp};

fn fast() -> Config {
    Config {
        interval: 10,
        nodelay: true,
        fast_resend: 2,
        congestion_control: false,
        ..Config::default()
    }
}
fn packet(cmd: u8, sn: u32, wnd: u16) -> Vec<u8> {
    let mut p = 7u32.to_le_bytes().to_vec();
    p.extend([cmd, 0]);
    p.extend(wnd.to_le_bytes());
    for n in [0, sn, 0, 0] {
        p.extend(n.to_le_bytes());
    }
    p
}

#[test]
fn lossy_bidirectional_wraparound() {
    for congestion_control in [false, true] {
        let config = Config {
            congestion_control,
            ..fast()
        };
        let mut a = Kcp::with_config(7, config.clone()).unwrap();
        let mut b = Kcp::with_config(7, config).unwrap();
        let messages: Vec<_> = (0..80).map(|i| vec![i as u8; i * 101]).collect();
        for m in &messages {
            a.send(m).unwrap();
            b.send(m).unwrap();
        }
        let mut ar = Vec::new();
        let mut br = Vec::new();
        let mut network = Vec::new();
        let mut count = 0;
        for tick in 0u32..20000 {
            let now = (u32::MAX - 500).wrapping_add(tick * 10);
            a.update(now);
            b.update(now);
            for (target, kcp) in [(true, &mut a), (false, &mut b)] {
                while let Some(p) = kcp.poll_output() {
                    assert!(p.len() <= 1400);
                    count += 1;
                    if count % 7 == 0 {
                        continue;
                    }
                    if count % 11 == 0 {
                        network.push((tick + 3, target, p.clone()));
                    }
                    network.push((tick + 1 + (count % 5), target, p));
                }
            }
            for i in (0..network.len()).rev() {
                if network[i].0 <= tick {
                    let (_, target, p) = network.swap_remove(i);
                    if target {
                        b.input(&p, now).unwrap();
                    } else {
                        a.input(&p, now).unwrap();
                    }
                }
            }
            while let Some(m) = a.recv() {
                ar.push(m);
            }
            while let Some(m) = b.recv() {
                br.push(m);
            }
            if ar.len() == messages.len()
                && br.len() == messages.len()
                && a.waitsnd() == 0
                && b.waitsnd() == 0
            {
                break;
            }
        }
        assert_eq!(ar, messages);
        assert_eq!(br, messages);
        assert_eq!(a.waitsnd() + b.waitsnd(), 0);
    }
}
#[test]
fn validation_is_atomic_and_limits_are_checked() {
    assert!(matches!(
        Kcp::with_config(7, Config { mtu: 24, ..fast() }),
        Err(Error::InvalidConfig)
    ));
    let mut a = Kcp::new(7);
    assert_eq!(a.send(&vec![0; 1376 * 128]), Err(Error::MessageTooLarge));
    assert_eq!(a.waitsnd(), 0);
    let mut p = packet(81, 0, 128);
    p.push(1);
    assert_eq!(a.input(&p, 0), Err(Error::InvalidPacket));
    assert_eq!(a.recv(), None);
    for len in 0..24 {
        assert_eq!(a.input(&p[..len], 0), Err(Error::InvalidPacket));
    }
    let mut p = packet(81, 0, 128);
    p[0] = 8;
    assert_eq!(a.input(&p, 0), Err(Error::ConversationMismatch));
    p[0] = 7;
    p[4] = 80;
    assert_eq!(a.input(&p, 0), Err(Error::InvalidPacket));
    p[4] = 81;
    p[20..24].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(a.input(&p, 0), Err(Error::InvalidPacket));
}
#[test]
fn stream_merges_and_splits_without_losing_bytes() {
    let mut a = Kcp::with_config(
        7,
        Config {
            stream: true,
            ..fast()
        },
    )
    .unwrap();
    let mut b = Kcp::new(7);
    a.send(b"abc").unwrap();
    a.send(&vec![4; 200000]).unwrap();
    a.send(&[]).unwrap();
    let mut result = Vec::new();
    for now in (0..30000).step_by(10) {
        a.update(now);
        b.update(now);
        while let Some(p) = a.poll_output() {
            b.input(&p, now).unwrap();
        }
        while let Some(p) = b.poll_output() {
            a.input(&p, now).unwrap();
        }
        while let Some(m) = b.recv() {
            result.extend(m);
        }
        if a.waitsnd() == 0 {
            break;
        }
    }
    assert_eq!(&result[..3], b"abc");
    assert_eq!(result[3..], vec![4; 200000]);
}
#[test]
fn probes_and_dead_link_and_check() {
    let mut a = Kcp::with_config(
        7,
        Config {
            dead_link: 3,
            ..fast()
        },
    )
    .unwrap();
    assert_eq!(a.check(5), 5);
    a.update(5);
    assert_eq!(a.check(5), 15);
    a.input(&packet(84, 0, 0), 5).unwrap();
    a.send(b"x").unwrap();
    a.update(15);
    assert!(a.poll_output().is_none());
    a.update(7015);
    let p = a.poll_output().unwrap();
    assert_eq!(p[4], 83);
    a.input(&packet(84, 0, 128), 7020).unwrap();
    for now in (7020..10000).step_by(10) {
        a.update(now);
        while a.poll_output().is_some() {}
    }
    assert!(a.is_dead());
}
#[test]
fn fast_retransmit_before_timeout() {
    let mut a = Kcp::with_config(7, Config { mtu: 50, ..fast() }).unwrap();
    a.send(&[1; 78]).unwrap();
    a.update(0);
    while a.poll_output().is_some() {}
    a.input(&packet(82, 1, 128), 10).unwrap();
    a.input(&packet(82, 2, 128), 10).unwrap();
    a.update(10);
    let p = a.poll_output().unwrap();
    assert_eq!(u32::from_le_bytes(p[12..16].try_into().unwrap()), 0);
    assert_eq!(a.stats().transmissions, 4);
    assert_eq!(a.stats().fast_retransmissions, 1);
    assert_eq!(a.stats().timeout_retransmissions, 0);
}
#[test]
fn zero_window_recovers_after_reader_drains() {
    let mut a = Kcp::with_config(
        7,
        Config {
            send_window: 256,
            ..fast()
        },
    )
    .unwrap();
    let mut b = Kcp::with_config(7, fast()).unwrap();
    for _ in 0..300 {
        a.send(b"x").unwrap();
    }
    let mut received = 0;
    for now in (0..30000).step_by(10) {
        a.update(now);
        b.update(now);
        while let Some(p) = a.poll_output() {
            b.input(&p, now).unwrap();
        }
        while let Some(p) = b.poll_output() {
            a.input(&p, now).unwrap();
        }
        if now >= 1000 {
            while let Some(m) = b.recv() {
                assert_eq!(m, b"x");
                received += 1;
            }
        }
        if received == 300 && a.waitsnd() == 0 {
            break;
        }
    }
    assert_eq!(received, 300);
    assert_eq!(a.waitsnd(), 0);
}

#[test]
fn snapshots_track_queues_acknowledgements_and_zero_rtt_without_mutation() {
    let config = Config { mtu: 50, ..fast() };
    let mut a = Kcp::with_config(7, config.clone()).unwrap();
    let mut b = Kcp::with_config(7, config).unwrap();
    assert_eq!(a.stats().smoothed_rtt_ms, None);
    a.send(&[42; 78]).unwrap();
    let before = a.stats();
    assert_eq!(before.send_queue_segments, 3);
    assert_eq!(before.pending_send_bytes, 78);
    assert_eq!(before.transmissions, 0);
    assert_eq!(a.stats(), before);
    a.update(0);
    let sent = a.stats();
    assert_eq!(sent.send_queue_segments, 0);
    assert_eq!(sent.unacknowledged_segments, 3);
    assert_eq!(sent.transmissions, 3);
    assert_eq!(sent.queued_output_datagrams, 3);
    let first = a.poll_output().unwrap();
    let second = a.poll_output().unwrap();
    let third = a.poll_output().unwrap();
    b.input(&third, 0).unwrap();
    assert_eq!(b.stats().out_of_order_segments, 1);
    assert_eq!(b.stats().receive_queue_segments, 0);
    b.input(&first, 0).unwrap();
    assert_eq!(b.stats().receive_queue_segments, 1);
    assert_eq!(b.stats().buffered_receive_bytes, 52);
    b.input(&second, 0).unwrap();
    let received = b.stats();
    assert_eq!(received.out_of_order_segments, 0);
    assert_eq!(received.receive_queue_segments, 3);
    assert_eq!(received.pending_acknowledgements, 3);
    assert_eq!(received.buffered_receive_bytes, 78);
    assert_eq!(b.recv().unwrap(), [42; 78]);
    assert_eq!(b.stats().buffered_receive_bytes, 0);
    b.update(0);
    while let Some(p) = b.poll_output() {
        a.input(&p, 0).unwrap();
    }
    let acknowledged = a.stats();
    assert_eq!(acknowledged.pending_send_bytes, 0);
    assert_eq!(acknowledged.unacknowledged_segments, 0);
    assert_eq!(acknowledged.smoothed_rtt_ms, Some(0));
    assert_eq!(acknowledged.transmissions, 3);
    assert_eq!(
        acknowledged.timeout_retransmissions + acknowledged.fast_retransmissions,
        0
    );
}

#[test]
fn timeout_statistics_survive_acknowledgement() {
    let mut a = Kcp::with_config(7, fast()).unwrap();
    a.send(b"x").unwrap();
    a.update(0);
    a.update(200);
    let retransmitted = a.stats();
    assert_eq!(retransmitted.transmissions, 2);
    assert_eq!(retransmitted.timeout_retransmissions, 1);
    assert_eq!(retransmitted.fast_retransmissions, 0);
    a.input(&packet(82, 0, 128), 210).unwrap();
    assert_eq!(a.stats().unacknowledged_segments, 0);
    assert_eq!(a.stats().timeout_retransmissions, 1);
    assert_eq!(a.stats().smoothed_rtt_ms, Some(210));
}

#[test]
fn check_deadline_is_actionable_when_rto_falls_between_flush_ticks() {
    let mut kcp = Kcp::with_config(
        7,
        Config {
            interval: 30,
            ..fast()
        },
    )
    .unwrap();
    kcp.send(b"unacknowledged").unwrap();
    for now in (0..=180).step_by(30) {
        kcp.update(now);
    }
    let deadline = kcp.check(180);
    kcp.update(deadline);
    let next = kcp.check(deadline);
    assert!(
        next.wrapping_sub(deadline) > 0,
        "update at {deadline} left the same expired deadline"
    );
    assert_eq!(kcp.stats().timeout_retransmissions, 1);
}

#[test]
fn timer_only_driving_makes_progress_across_intervals_and_clock_wrap() {
    for interval in [10, 17, 30, 100, 5000] {
        for nodelay in [false, true] {
            for start in [0, u32::MAX - 50] {
                let mut kcp = Kcp::with_config(
                    7,
                    Config {
                        interval,
                        nodelay,
                        ..fast()
                    },
                )
                .unwrap();
                kcp.send(b"unacknowledged").unwrap();
                let mut now = start;
                for _ in 0..100 {
                    kcp.update(now);
                    while kcp.poll_output().is_some() {}
                    let next = kcp.check(now);
                    let wait = next.wrapping_sub(now);
                    assert_eq!(
                        wait, interval,
                        "interval={interval}, nodelay={nodelay}, start={start}"
                    );
                    assert_eq!(kcp.check(now.wrapping_add(wait - 1)), next);
                    now = next;
                }
                assert!(kcp.stats().timeout_retransmissions > 0);
            }
        }
    }
}

#[test]
fn check_and_update_recover_from_large_clock_discontinuities() {
    let mut kcp = Kcp::with_config(7, fast()).unwrap();
    for now in [0, 100000, 0, u32::MAX - 100000, 100000] {
        assert_eq!(kcp.check(now), now);
        kcp.update(now);
        assert_eq!(kcp.check(now).wrapping_sub(now), 10);
    }
}

//! Run with KCP_C_SOURCE=/path/to/ikcp.c cargo test --test c_interop -- --ignored
use kcp_rs::{Config, Kcp};
use std::io::{Read, Write};
use std::process::{Child, Command, Stdio};

struct Peer(Child);
impl Drop for Peer {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
impl Peer {
    fn command(&mut self, tag: u8, now: u32, data: &[u8]) -> Vec<(u8, Vec<u8>)> {
        let input = self.0.stdin.as_mut().unwrap();
        input.write_all(&[tag]).unwrap();
        input.write_all(&now.to_le_bytes()).unwrap();
        input.write_all(&(data.len() as u32).to_le_bytes()).unwrap();
        input.write_all(data).unwrap();
        input.flush().unwrap();
        let output = self.0.stdout.as_mut().unwrap();
        let mut result = Vec::new();
        loop {
            let mut h = [0; 5];
            output.read_exact(&mut h).unwrap();
            let len = u32::from_le_bytes(h[1..].try_into().unwrap()) as usize;
            assert!(len <= 262144);
            let mut data = vec![0; len];
            output.read_exact(&mut data).unwrap();
            if h[0] == b'e' {
                break;
            }
            result.push((h[0], data));
        }
        result
    }
}
struct Reference(std::path::PathBuf);
impl Drop for Reference {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
impl Reference {
    fn build() -> Self {
        let source =
            std::path::PathBuf::from(std::env::var_os("KCP_C_SOURCE").expect("set KCP_C_SOURCE"));
        let dir = std::env::var_os("TMPDIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| {
                std::path::PathBuf::from(std::env::var_os("HOME").unwrap()).join("tmp")
            })
            .join("pi");
        std::fs::create_dir_all(&dir).unwrap();
        let reference = Self(dir.join(format!("kcp-reference-{}", std::process::id())));
        assert!(
            Command::new("cc")
                .args(["-O2", "-Wall", "-Wextra", "-I"])
                .arg(source.parent().unwrap())
                .arg("tests/c_peer.c")
                .arg(&source)
                .arg("-o")
                .arg(&reference.0)
                .status()
                .unwrap()
                .success()
        );
        reference
    }
    fn peer(&self, config: &Config) -> Peer {
        let mut peer = Peer(
            Command::new(&self.0)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .unwrap(),
        );
        let settings: Vec<_> = [
            config.mtu as u32,
            u32::from(config.nodelay),
            config.interval,
            config.fast_resend,
            u32::from(!config.congestion_control),
            u32::from(config.stream),
            u32::from(config.send_window),
            u32::from(config.receive_window),
        ]
        .into_iter()
        .flat_map(u32::to_le_bytes)
        .collect();
        assert!(peer.command(b'c', 0, &settings).is_empty());
        peer
    }
}
impl Peer {
    fn stats(&mut self) -> (u32, u32) {
        let frames = self.command(b'q', 0, &[]);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].0, b'w');
        assert_eq!(frames[0].1.len(), 8);
        let bytes = &frames[0].1;
        (
            u32::from_le_bytes(bytes[..4].try_into().unwrap()),
            u32::from_le_bytes(bytes[4..].try_into().unwrap()),
        )
    }
}
fn contains_command(mut packet: &[u8], command: u8) -> bool {
    while !packet.is_empty() {
        assert!(packet.len() >= 24);
        if packet[4] == command {
            return true;
        }
        let len = u32::from_le_bytes(packet[20..24].try_into().unwrap()) as usize;
        assert!(packet.len() >= 24 + len);
        packet = &packet[24 + len..];
    }
    false
}

fn exchange(reference: &Reference, config: Config) {
    eprintln!("C interoperability: {config:?}");
    let mut peer = reference.peer(&config);
    let mut rust = Kcp::with_config(7, config.clone()).unwrap();
    let maximum = (config.mtu - 24) * 127;
    let messages: Vec<_> = (0..50)
        .map(|i| vec![i as u8; (i * 257) % (maximum + 1)])
        .chain(std::iter::once(vec![42; maximum]))
        .collect();
    let expected_bytes: usize = messages.iter().map(Vec::len).sum();
    assert!(peer.command(b'h', 1, &[]).is_empty());
    for m in &messages {
        rust.send(m).unwrap();
        assert!(peer.command(b's', 0, m).is_empty());
    }
    let mut from_c = Vec::new();
    let mut from_rust = Vec::new();
    let mut c_bytes = 0;
    let mut rust_bytes = 0;
    let mut network = Vec::new();
    let mut count = 0u32;
    let mut rust_probe = false;
    let mut c_probe = false;
    let mut rust_window_response = false;
    let mut c_window_response = false;
    let mut drained = false;
    let mut resumed = false;
    for tick in 0u32..30000 {
        let now = (u32::MAX - 500).wrapping_add(tick * 10);
        // Wait for actual WASK/WINS delivery in both directions, not a fixed
        // network-dependent assumption about how quickly slow start fills windows.
        if !resumed
            && tick >= 2000
            && rust_probe
            && c_probe
            && rust_window_response
            && c_window_response
        {
            resumed = true;
            assert_eq!(
                rust.stats().remote_window,
                0,
                "C receive window never closed"
            );
            let (pending, window) = peer.stats();
            assert_eq!(window, 0, "Rust receive window never closed");
            assert!(pending > 0 && rust.waitsnd() > 0);
            for (tag, p) in peer.command(b'h', 0, &[]) {
                assert_eq!(tag, b'm');
                rust_bytes += p.len();
                from_rust.push(p);
            }
        }
        rust.update(now);
        let mut outgoing = Vec::new();
        while let Some(p) = rust.poll_output() {
            outgoing.push((true, p));
        }
        for (tag, p) in peer.command(b'u', now, &[]) {
            if tag == b'p' {
                outgoing.push((false, p));
            } else {
                assert_eq!(tag, b'm');
                rust_bytes += p.len();
                from_rust.push(p);
            }
        }
        for (to_c, p) in outgoing {
            assert!(p.len() <= config.mtu);
            count += 1;
            if count.is_multiple_of(7) {
                continue;
            }
            if count.is_multiple_of(13) {
                network.push((tick + 4, to_c, p.clone()));
            }
            network.push((tick + 1 + count % 5, to_c, p));
        }
        for i in (0..network.len()).rev() {
            if network[i].0 <= tick {
                let (_, to_c, p) = network.swap_remove(i);
                if contains_command(&p, 83) {
                    if to_c {
                        rust_probe = true;
                    } else {
                        c_probe = true;
                    }
                }
                if contains_command(&p, 84) {
                    if to_c {
                        rust_window_response = true;
                    } else {
                        c_window_response = true;
                    }
                }
                if to_c {
                    for (tag, p) in peer.command(b'i', now, &p) {
                        assert_eq!(tag, b'm');
                        rust_bytes += p.len();
                        from_rust.push(p);
                    }
                } else {
                    rust.input(&p, now).unwrap();
                }
            }
        }
        if resumed {
            while let Some(m) = rust.recv() {
                c_bytes += m.len();
                from_c.push(m);
            }
        }
        assert!(
            c_bytes <= expected_bytes && rust_bytes <= expected_bytes,
            "duplicate application bytes"
        );
        let complete = if config.stream {
            c_bytes == expected_bytes && rust_bytes == expected_bytes
        } else {
            from_c.len() == messages.len() && from_rust.len() == messages.len()
        };
        if complete && rust.waitsnd() == 0 && peer.stats().0 == 0 {
            drained = true;
            break;
        }
    }
    assert!(
        resumed,
        "both peers must reach zero window and exchange probes"
    );
    assert!(
        drained,
        "both peers must ACK all data before the simulation deadline"
    );
    if config.stream {
        let expected: Vec<_> = messages.concat();
        assert_eq!(from_c.concat(), expected);
        assert_eq!(from_rust.concat(), expected);
    } else {
        assert_eq!(from_c, messages);
        assert_eq!(from_rust, messages);
    }
}

#[test]
#[ignore = "requires a C compiler and KCP_C_SOURCE pointing to upstream ikcp.c"]
fn c_reference_bidirectional_loss_reordering_and_wraparound() {
    let reference = Reference::build();
    let fast = Config {
        nodelay: true,
        interval: 10,
        fast_resend: 2,
        ..Config::default()
    };
    for config in [
        Config::default(),
        fast.clone(),
        Config {
            congestion_control: false,
            ..fast.clone()
        },
        Config {
            mtu: 50,
            send_window: 64,
            receive_window: 256,
            congestion_control: false,
            ..fast.clone()
        },
        Config {
            stream: true,
            ..Config::default()
        },
        Config {
            stream: true,
            mtu: 256,
            congestion_control: false,
            ..fast
        },
    ] {
        exchange(&reference, config);
    }
}

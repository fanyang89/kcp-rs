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
#[test]
#[ignore = "requires a C compiler and KCP_C_SOURCE pointing to upstream ikcp.c"]
fn c_reference_bidirectional_loss_reordering_and_wraparound() {
    let source =
        std::path::PathBuf::from(std::env::var_os("KCP_C_SOURCE").expect("set KCP_C_SOURCE"));
    let dir = std::env::var_os("TMPDIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from(std::env::var_os("HOME").unwrap()).join("tmp"))
        .join("pi");
    std::fs::create_dir_all(&dir).unwrap();
    let exe = dir.join(format!("kcp-reference-{}", std::process::id()));
    assert!(
        Command::new("cc")
            .args(["-O2", "-Wall", "-Wextra", "-I"])
            .arg(source.parent().unwrap())
            .arg("tests/c_peer.c")
            .arg(&source)
            .arg("-o")
            .arg(&exe)
            .status()
            .unwrap()
            .success()
    );
    let mut peer = Peer(
        Command::new(&exe)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    std::fs::remove_file(&exe).unwrap();
    let mut rust = Kcp::with_config(
        7,
        Config {
            nodelay: true,
            interval: 10,
            fast_resend: 2,
            ..Config::default()
        },
    )
    .unwrap();
    let messages: Vec<_> = (0..50)
        .map(|i| vec![i as u8; i * 257])
        .chain(std::iter::once(vec![42; 1376 * 127]))
        .collect();
    for m in &messages {
        rust.send(m).unwrap();
        assert!(peer.command(b's', 0, m).is_empty());
    }
    let mut from_c = Vec::new();
    let mut from_rust = Vec::new();
    let mut network = Vec::new();
    let mut count = 0u32;
    for tick in 0u32..30000 {
        let now = (u32::MAX - 500).wrapping_add(tick * 10);
        rust.update(now);
        let mut outgoing = Vec::new();
        while let Some(p) = rust.poll_output() {
            outgoing.push((true, p));
        }
        for (tag, p) in peer.command(b'u', now, &[]) {
            if tag == b'p' {
                outgoing.push((false, p));
            } else {
                from_rust.push(p);
            }
        }
        for (to_c, p) in outgoing {
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
                if to_c {
                    for (tag, p) in peer.command(b'i', now, &p) {
                        assert_eq!(tag, b'm');
                        from_rust.push(p);
                    }
                } else {
                    rust.input(&p, now).unwrap();
                }
            }
        }
        while let Some(m) = rust.recv() {
            from_c.push(m);
        }
        if from_c.len() == messages.len()
            && from_rust.len() == messages.len()
            && rust.waitsnd() == 0
        {
            break;
        }
    }
    assert_eq!(from_c, messages);
    assert_eq!(from_rust, messages);
    assert_eq!(rust.waitsnd(), 0);
}

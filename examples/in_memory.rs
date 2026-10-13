//! Replace packet forwarding with UDP and the loop with any runtime's timer.
use kcp_rs::Kcp;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut sender = Kcp::new(42);
    let mut receiver = Kcp::new(42);
    sender.send(b"hello, KCP")?;
    let mut delivered = false;
    for now in (0..5000).step_by(10) {
        sender.update(now);
        receiver.update(now);
        while let Some(packet) = sender.poll_output() {
            receiver.input(&packet, now)?;
        }
        while let Some(packet) = receiver.poll_output() {
            sender.input(&packet, now)?;
        }
        while let Some(message) = receiver.recv() {
            println!("{}", String::from_utf8_lossy(&message));
            delivered = true;
        }
        if delivered && sender.waitsnd() == 0 {
            return Ok(());
        }
    }
    Err("delivery timed out".into())
}

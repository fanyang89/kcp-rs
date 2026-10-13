//! Development adapter shared by examples and runtime integration tests.
use kcp_rs::asynchronous::{DatagramTransport, Timer};
use std::{
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    net::UdpSocket,
    time::{Instant, Sleep},
};
pub struct Udp(pub UdpSocket);
impl DatagramTransport for Udp {
    fn poll_recv(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<(usize, SocketAddr)>> {
        let mut read = tokio::io::ReadBuf::new(buf);
        self.0
            .poll_recv_from(cx, &mut read)
            .map(|r| r.map(|peer| (read.filled().len(), peer)))
    }
    fn poll_send(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        peer: SocketAddr,
        buf: &[u8],
    ) -> Poll<io::Result<()>> {
        self.0.poll_send_to(cx, buf, peer).map(|r| {
            r.and_then(|n| {
                if n == buf.len() {
                    Ok(())
                } else {
                    Err(io::ErrorKind::WriteZero.into())
                }
            })
        })
    }
}
pub struct Clock {
    epoch: Instant,
    sleep: Pin<Box<Sleep>>,
    deadline: Option<u64>,
}
impl Clock {
    pub fn new() -> Self {
        Self {
            epoch: Instant::now(),
            sleep: Box::pin(tokio::time::sleep(Duration::ZERO)),
            deadline: None,
        }
    }
}
impl Timer for Clock {
    fn now(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }
    fn poll_until(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        deadline: u64,
    ) -> Poll<io::Result<()>> {
        if self.deadline != Some(deadline) {
            let at = self.epoch + Duration::from_millis(deadline);
            self.sleep.as_mut().reset(at);
            self.deadline = Some(deadline);
        }
        self.sleep.as_mut().poll(cx).map(Ok)
    }
}

//! Tokio is an adapter here, never a dependency of the protocol/async facade.
mod support;
use kcp_rs::{
    asynchronous::{DatagramTransport, Endpoint, Timer},
    endpoint::Config,
};
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
struct Udp(UdpSocket);
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
struct Clock {
    epoch: Instant,
    sleep: Pin<Box<Sleep>>,
    deadline: Option<u64>,
}
impl Clock {
    fn new() -> Self {
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
#[tokio::main(flavor = "current_thread")]
async fn main() -> io::Result<()> {
    let a = UdpSocket::bind("127.0.0.1:0").await?;
    let b = UdpSocket::bind("127.0.0.1:0").await?;
    let peer = b.local_addr()?;
    // Fixed seeds are for this isolated demo only. Use fresh unpredictable seeds in applications.
    let (a, da) = Endpoint::new(Config::default(), 100, Udp(a), Clock::new())?;
    let (b, db) = Endpoint::new(Config::default(), 200, Udp(b), Clock::new())?;
    tokio::time::timeout(
        Duration::from_secs(15),
        futures_lite::future::race(
            support::exchange(a, b, peer),
            futures_lite::future::race(da, db),
        ),
    )
    .await
    .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?
}

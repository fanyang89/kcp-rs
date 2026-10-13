//! async-io reactor + futures-lite executor, using exactly the same KcpStream API.
mod support;
use async_io::Async;
use kcp_rs::{
    asynchronous::{DatagramTransport, Endpoint, Timer},
    endpoint::Config,
};
use std::{
    future::Future,
    io,
    net::{SocketAddr, UdpSocket},
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};
struct Udp(Async<UdpSocket>);
impl DatagramTransport for Udp {
    fn poll_recv(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<(usize, SocketAddr)>> {
        loop {
            match self.0.get_ref().recv_from(buf) {
                Ok(r) => return Poll::Ready(Ok(r)),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => return Poll::Ready(Err(e)),
            }
            std::task::ready!(self.0.poll_readable(cx))?;
        }
    }
    fn poll_send(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        peer: SocketAddr,
        buf: &[u8],
    ) -> Poll<io::Result<()>> {
        loop {
            match self.0.get_ref().send_to(buf, peer) {
                Ok(n) => {
                    return Poll::Ready(if n == buf.len() {
                        Ok(())
                    } else {
                        Err(io::ErrorKind::WriteZero.into())
                    });
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => return Poll::Ready(Err(e)),
            }
            std::task::ready!(self.0.poll_writable(cx))?;
        }
    }
}
struct Clock {
    epoch: Instant,
    timer: async_io::Timer,
    deadline: Option<u64>,
}
impl Clock {
    fn new() -> Self {
        Self {
            epoch: Instant::now(),
            timer: async_io::Timer::never(),
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
            self.timer.set_at(at);
            self.deadline = Some(deadline);
        }
        Pin::new(&mut self.timer).poll(cx).map(|_| Ok(()))
    }
}
fn main() -> io::Result<()> {
    futures_lite::future::block_on(async {
        let a = Async::<UdpSocket>::bind(([127, 0, 0, 1], 0))?;
        let b = Async::<UdpSocket>::bind(([127, 0, 0, 1], 0))?;
        let peer = b.get_ref().local_addr()?;
        // Fixed seeds are for this isolated demo only.
        let (a, da) = Endpoint::new(Config::default(), 100, Udp(a), Clock::new())?;
        let (b, db) = Endpoint::new(Config::default(), 200, Udp(b), Clock::new())?;
        futures_lite::future::race(
            futures_lite::future::race(
                support::exchange(a, b, peer),
                futures_lite::future::race(da, db),
            ),
            async {
                async_io::Timer::after(Duration::from_secs(15)).await;
                Err(io::ErrorKind::TimedOut.into())
            },
        )
        .await
    })
}

//! Development adapter shared by examples and runtime integration tests.
use async_io::Async;
use kcp_rs::asynchronous::{DatagramTransport, Timer};
use std::{
    future::Future,
    io,
    net::{SocketAddr, UdpSocket},
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};
pub struct Udp(pub Async<UdpSocket>);
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
pub struct Clock {
    epoch: Instant,
    timer: async_io::Timer,
    deadline: Option<u64>,
}
impl Clock {
    pub fn new() -> Self {
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

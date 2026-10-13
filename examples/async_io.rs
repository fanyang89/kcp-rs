//! async-io reactor + futures-lite executor, using exactly the same KcpStream API.
#[path = "support/async_io.rs"]
mod adapter;
mod support;
use adapter::{Clock, Udp};
use async_io::Async;
use kcp_rs::{asynchronous::Endpoint, endpoint::Config};
use std::{io, net::UdpSocket, time::Duration};
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

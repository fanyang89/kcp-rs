//! Tokio is an adapter here, never a dependency of the protocol/async facade.
#[path = "support/tokio.rs"]
mod adapter;
mod support;
use adapter::{Clock, Udp};
use kcp_rs::{asynchronous::Endpoint, endpoint::Config};
use std::{io, time::Duration};
use tokio::net::UdpSocket;
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

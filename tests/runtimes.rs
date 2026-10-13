//! Real sockets and independently scheduled application/driver tasks. No manual polls.
#![cfg(feature = "async")]

#[path = "../examples/support/async_io.rs"]
mod async_io_adapter;
#[path = "../examples/support/tokio.rs"]
mod tokio_adapter;

use futures_lite::io::{AsyncReadExt, AsyncWriteExt};
use kcp_rs::{
    asynchronous::{Connector, Endpoint, KcpStream},
    endpoint::Config,
};
use std::{future::Future, io, net::SocketAddr, time::Duration};

const CONNECTIONS: u8 = 4;
const PAYLOAD: usize = 32 * 1024;
const TIMEOUT: Duration = Duration::from_secs(15);

fn config() -> Config {
    Config {
        send_capacity: 2,
        receive_capacity: 1400,
        output_capacity: 2,
        ..Config::default()
    }
}
struct Shutdown(Connector, Connector);
impl Drop for Shutdown {
    fn drop(&mut self) {
        self.0.shutdown();
        self.1.shutdown();
    }
}
async fn client(connector: Connector, peer: SocketAddr, tag: u8) -> io::Result<KcpStream> {
    let mut stream = connector.connect(peer).await?;
    stream.write_all(&vec![tag; PAYLOAD]).await?;
    stream.flush().await?;
    stream.close().await?;
    let mut reply = Vec::new();
    stream.read_to_end(&mut reply).await?;
    assert_eq!(reply, [tag]);
    let stats = stream.stats()?;
    assert!(stats.write_closed && stats.peer_closed);
    assert_eq!(stats.kcp.pending_send_bytes, 0);
    Ok(stream)
}
async fn server(mut stream: KcpStream) -> io::Result<KcpStream> {
    let mut payload = Vec::new();
    stream.read_to_end(&mut payload).await?;
    assert_eq!(payload.len(), PAYLOAD);
    let tag = payload[0];
    assert!(tag < CONNECTIONS);
    assert!(payload.iter().all(|b| *b == tag));
    stream.write_all(&[tag]).await?;
    stream.close().await?;
    Ok(stream)
}

#[tokio::test(flavor = "current_thread")]
async fn tokio_independent_tasks_handle_concurrent_connections_and_shutdown() -> io::Result<()> {
    use tokio_adapter::{Clock, Udp};
    let a = tokio::net::UdpSocket::bind("127.0.0.1:0").await?;
    let b = tokio::net::UdpSocket::bind("127.0.0.1:0").await?;
    let peer = b.local_addr()?;
    let (a, da) = Endpoint::new(config(), 100, Udp(a), Clock::new())?;
    let (mut b, db) = Endpoint::new(config(), 1000, Udp(b), Clock::new())?;
    let shutdown = Shutdown(a.connector(), b.connector());
    let da = tokio::spawn(da);
    let db = tokio::spawn(db);
    let result = tokio::time::timeout(TIMEOUT, async {
        let mut clients = Vec::new();
        for tag in 0..CONNECTIONS {
            clients.push(tokio::spawn(client(a.connector(), peer, tag)));
        }
        let mut servers = Vec::new();
        for _ in 0..CONNECTIONS {
            servers.push(tokio::spawn(server(b.accept().await?)));
        }
        let mut streams = Vec::new();
        for task in clients.into_iter().chain(servers) {
            streams.push(task.await.map_err(io::Error::other)??);
        }
        Ok::<_, io::Error>(streams)
    })
    .await;
    // Wake and join drivers even when the application times out or returns an error.
    shutdown.0.shutdown();
    shutdown.1.shutdown();
    for task in [da, db] {
        tokio::time::timeout(TIMEOUT, task)
            .await
            .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?
            .map_err(io::Error::other)??;
    }
    let streams = result.map_err(|_| io::Error::from(io::ErrorKind::TimedOut))??;
    assert_eq!(streams.len(), usize::from(CONNECTIONS) * 2);
    for stream in streams {
        assert_eq!(
            stream.stats().unwrap_err().kind(),
            io::ErrorKind::ConnectionAborted
        );
    }
    assert!(a.stats().shutdown && b.stats().shutdown);
    assert_eq!(a.stats().connections + b.stats().connections, 0);
    Ok(())
}

async fn bounded<T>(future: impl Future<Output = io::Result<T>>) -> io::Result<T> {
    futures_lite::future::race(future, async {
        async_io::Timer::after(TIMEOUT).await;
        Err(io::ErrorKind::TimedOut.into())
    })
    .await
}

#[test]
fn async_io_independent_executors_handle_concurrent_connections_and_shutdown() -> io::Result<()> {
    use async_io_adapter::{Clock, Udp};
    use futures_lite::future::block_on;
    let a = async_io::Async::<std::net::UdpSocket>::bind(([127, 0, 0, 1], 0))?;
    let b = async_io::Async::<std::net::UdpSocket>::bind(([127, 0, 0, 1], 0))?;
    let peer = b.get_ref().local_addr()?;
    let (a, da) = Endpoint::new(config(), 100, Udp(a), Clock::new())?;
    let (mut b, db) = Endpoint::new(config(), 1000, Udp(b), Clock::new())?;
    std::thread::scope(|scope| {
        // RAII wakes the drivers before scope joins, including on assertion/error paths.
        let shutdown = Shutdown(a.connector(), b.connector());
        let da = scope.spawn(move || block_on(bounded(da)));
        let db = scope.spawn(move || block_on(bounded(db)));
        let mut clients = Vec::new();
        for tag in 0..CONNECTIONS {
            let connector = a.connector();
            clients.push(scope.spawn(move || block_on(bounded(client(connector, peer, tag)))));
        }
        let mut servers = Vec::new();
        for _ in 0..CONNECTIONS {
            let stream = block_on(bounded(b.accept()))?;
            servers.push(scope.spawn(move || block_on(bounded(server(stream)))));
        }
        let mut streams = Vec::new();
        for task in clients.into_iter().chain(servers) {
            streams.push(task.join().expect("application executor panicked")?);
        }
        shutdown.0.shutdown();
        shutdown.1.shutdown();
        da.join().expect("client driver panicked")?;
        db.join().expect("server driver panicked")?;
        assert_eq!(streams.len(), usize::from(CONNECTIONS) * 2);
        for stream in streams {
            assert_eq!(
                stream.stats().unwrap_err().kind(),
                io::ErrorKind::ConnectionAborted
            );
        }
        assert!(a.stats().shutdown && b.stats().shutdown);
        assert_eq!(a.stats().connections + b.stats().connections, 0);
        Ok(())
    })
}

#[tokio::test(flavor = "current_thread")]
async fn aborting_a_spawned_driver_wakes_a_separately_pending_reader() -> io::Result<()> {
    use futures_io::AsyncRead;
    use std::{
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };
    use tokio_adapter::{Clock, Udp};

    let a = tokio::net::UdpSocket::bind("127.0.0.1:0").await?;
    let b = tokio::net::UdpSocket::bind("127.0.0.1:0").await?;
    let peer = b.local_addr()?;
    let (a, da) = Endpoint::new(config(), 100, Udp(a), Clock::new())?;
    let (mut b, db) = Endpoint::new(config(), 1000, Udp(b), Clock::new())?;
    let shutdown = Shutdown(a.connector(), b.connector());
    let da = tokio::spawn(da);
    let db = tokio::spawn(db);
    let result = tokio::time::timeout(TIMEOUT, async {
        let (client, server) = futures_lite::future::zip(a.connect(peer), b.accept()).await;
        let mut client = client?;
        let _server = server?;
        let registered = Arc::new(AtomicBool::new(false));
        let pending = registered.clone();
        let reader = tokio::spawn(async move {
            let mut buf = [0; 1];
            std::future::poll_fn(|cx| {
                let result = Pin::new(&mut client).poll_read(cx, &mut buf);
                if result.is_pending() {
                    pending.store(true, Ordering::Release);
                }
                result
            })
            .await
        });
        while !registered.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        da.abort();
        let stopped = da.await.expect_err("aborted driver unexpectedly completed");
        assert!(stopped.is_cancelled());
        let result = reader.await.map_err(io::Error::other)?;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
        assert!(a.stats().shutdown);
        assert_eq!(a.stats().connections, 0);
        Ok::<_, io::Error>(())
    })
    .await;
    shutdown.0.shutdown();
    shutdown.1.shutdown();
    tokio::time::timeout(TIMEOUT, db)
        .await
        .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?
        .map_err(io::Error::other)??;
    result.map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?
}

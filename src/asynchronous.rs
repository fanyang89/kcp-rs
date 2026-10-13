//! Optional runtime-neutral futures-io streams. No implicit spawning or runtime globals.
use crate::endpoint::{Config, ConnectionId, EndpointCore, Transmit};
use futures_io::{AsyncRead, AsyncWrite};
use std::{
    collections::BTreeMap,
    future::{Future, poll_fn},
    io,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

/// A datagram transport. Pending operations must register the supplied waker.
/// Receives must preserve packet boundaries and reject truncation. Successful sends
/// must send the complete datagram. WouldBlock must become Pending, not an error.
pub trait DatagramTransport {
    fn poll_recv(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<(usize, SocketAddr)>>;
    fn poll_send(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        peer: SocketAddr,
        buf: &[u8],
    ) -> Poll<io::Result<()>>;
}
/// Monotonic milliseconds and a replaceable timer. Repeated poll_until calls may
/// change the deadline; implementations must update both their timer and waker.
pub trait Timer {
    fn now(&self) -> u64;
    fn poll_until(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        deadline: u64,
    ) -> Poll<io::Result<()>>;
}
#[derive(Default)]
struct Waiters {
    read: Option<Waker>,
    write: Option<Waker>,
    connect: Option<Waker>,
    close: Option<Waker>,
}
struct Shared {
    core: EndpointCore,
    waiters: BTreeMap<ConnectionId, Waiters>,
    accept: Option<Waker>,
    driver: Option<Waker>,
    stopped: Option<io::ErrorKind>,
}
impl Shared {
    fn check(&self) -> io::Result<()> {
        self.stopped.map_or(Ok(()), |e| Err(e.into()))
    }
    fn notifications(&mut self) -> Vec<Waker> {
        let mut out = Vec::new();
        if let Some(w) = self.accept.take() {
            out.push(w);
        }
        for waiter in self.waiters.values_mut() {
            for slot in [
                &mut waiter.read,
                &mut waiter.write,
                &mut waiter.connect,
                &mut waiter.close,
            ] {
                if let Some(w) = slot.take() {
                    out.push(w);
                }
            }
        }
        out
    }
}
fn wake(wakers: impl IntoIterator<Item = Waker>) {
    for w in wakers {
        w.wake();
    }
}
fn register(slot: &mut Option<Waker>, cx: &Context<'_>) {
    if slot.as_ref().is_none_or(|w| !w.will_wake(cx.waker())) {
        *slot = Some(cx.waker().clone());
    }
}

/// One listener/connector handle. Stream handles live independently of this handle.
/// Only one connect/accept future may borrow this handle at a time.
pub struct Endpoint {
    shared: Arc<Mutex<Shared>>,
}
impl Endpoint {
    /// Returns the application handle and a driver which the caller must continuously poll.
    pub fn new<T: DatagramTransport + Unpin, C: Timer + Unpin>(
        config: Config,
        session_seed: u64,
        transport: T,
        timer: C,
    ) -> io::Result<(Self, Driver<T, C>)> {
        let mut core = EndpointCore::new(config, session_seed)?;
        core.update(timer.now());
        // Full UDP payload buffer lets adapters detect/drop oversized session packets
        // without accidentally accepting a truncated valid prefix.
        let shared = Arc::new(Mutex::new(Shared {
            core,
            waiters: BTreeMap::new(),
            accept: None,
            driver: None,
            stopped: None,
        }));
        Ok((
            Self {
                shared: shared.clone(),
            },
            Driver {
                shared,
                transport,
                timer,
                receive: vec![0; 65536],
                pending: None,
                finished: false,
            },
        ))
    }
    /// Cancellation drops the embryonic stream and releases its connection slot.
    pub async fn connect(&mut self, peer: SocketAddr) -> io::Result<KcpStream> {
        let (id, waker) = {
            let mut shared = self.shared.lock().unwrap();
            shared.check()?;
            // Driver updates the core's monotonic time before servicing the request.
            let id = shared.core.connect_deferred(peer)?;
            shared.waiters.insert(id, Waiters::default());
            (id, shared.driver.take())
        };
        wake(waker);
        let stream = KcpStream {
            shared: self.shared.clone(),
            id,
        };
        poll_fn(|cx| {
            let mut shared = stream.shared.lock().unwrap();
            if let Err(e) = shared.check() {
                return Poll::Ready(Err(e));
            }
            match shared.core.is_connected(id) {
                Ok(true) => Poll::Ready(Ok(())),
                Err(e) => Poll::Ready(Err(e)),
                Ok(false) => {
                    register(&mut shared.waiters.get_mut(&id).unwrap().connect, cx);
                    Poll::Pending
                }
            }
        })
        .await?;
        Ok(stream)
    }
    /// Cancellation does not remove a queued accepted connection.
    pub async fn accept(&mut self) -> io::Result<KcpStream> {
        let id = poll_fn(|cx| {
            let mut shared = self.shared.lock().unwrap();
            if let Err(e) = shared.check() {
                return Poll::Ready(Err(e));
            }
            if let Some(id) = shared.core.accept() {
                shared.waiters.insert(id, Waiters::default());
                Poll::Ready(Ok(id))
            } else {
                register(&mut shared.accept, cx);
                Poll::Pending
            }
        })
        .await?;
        Ok(KcpStream {
            shared: self.shared.clone(),
            id,
        })
    }
}
impl Drop for Endpoint {
    fn drop(&mut self) {
        let w = {
            let mut s = self.shared.lock().unwrap();
            s.core.set_listening(false);
            s.driver.take()
        };
        wake(w);
    }
}

/// A reliable byte stream with independent read and write halves. Not Clone.
/// A graceful close shuts down only writes; Drop otherwise aborts the session.
pub struct KcpStream {
    shared: Arc<Mutex<Shared>>,
    id: ConnectionId,
}
impl KcpStream {
    pub fn id(&self) -> ConnectionId {
        self.id
    }
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.shared.lock().unwrap().core.peer_addr(self.id)
    }
}
impl Drop for KcpStream {
    fn drop(&mut self) {
        let w = {
            let mut s = self.shared.lock().unwrap();
            s.core.release(self.id);
            s.waiters.remove(&self.id);
            s.driver.take()
        };
        wake(w);
    }
}
impl AsyncRead for KcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let mut s = self.shared.lock().unwrap();
        if let Err(e) = s.check() {
            return Poll::Ready(Err(e));
        }
        match s.core.read(self.id, buf) {
            Err(e) => Poll::Ready(Err(e)),
            Ok(None) => {
                register(&mut s.waiters.get_mut(&self.id).unwrap().read, cx);
                Poll::Pending
            }
            Ok(Some(n)) => {
                let w = if n > 0 { s.driver.take() } else { None };
                drop(s);
                wake(w);
                Poll::Ready(Ok(n))
            }
        }
    }
}
impl AsyncWrite for KcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut s = self.shared.lock().unwrap();
        if let Err(e) = s.check() {
            return Poll::Ready(Err(e));
        }
        match s.core.write(self.id, buf) {
            Err(e) => Poll::Ready(Err(e)),
            Ok(None) => {
                register(&mut s.waiters.get_mut(&self.id).unwrap().write, cx);
                Poll::Pending
            }
            Ok(Some(n)) => {
                let w = if n > 0 { s.driver.take() } else { None };
                drop(s);
                wake(w);
                Poll::Ready(Ok(n))
            }
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut s = self.shared.lock().unwrap();
        if let Err(e) = s.check() {
            return Poll::Ready(Err(e));
        }
        match s.core.is_flushed(self.id) {
            Err(e) => Poll::Ready(Err(e)),
            Ok(true) => Poll::Ready(Ok(())),
            Ok(false) => {
                register(&mut s.waiters.get_mut(&self.id).unwrap().write, cx);
                Poll::Pending
            }
        }
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut s = self.shared.lock().unwrap();
        if let Err(e) = s.check() {
            return Poll::Ready(Err(e));
        }
        let was_closed = s.core.is_write_closed(self.id).unwrap_or(true);
        let result = match s.core.close_write(self.id) {
            Err(e) => Poll::Ready(Err(e)),
            Ok(false) => Poll::Pending,
            Ok(true) => match s.core.is_flushed(self.id) {
                Err(e) => Poll::Ready(Err(e)),
                Ok(true) => Poll::Ready(Ok(())),
                Ok(false) => Poll::Pending,
            },
        };
        if result.is_pending() {
            register(&mut s.waiters.get_mut(&self.id).unwrap().close, cx);
        }
        let w = if !was_closed && s.core.is_write_closed(self.id).unwrap_or(false) {
            s.driver.take()
        } else {
            None
        };
        drop(s);
        wake(w);
        result
    }
}

/// One cooperative driver per endpoint. Transport/timer failures and dropping this
/// future wake every pending operation with an error. Does not spawn tasks itself.
pub struct Driver<T, C> {
    shared: Arc<Mutex<Shared>>,
    transport: T,
    timer: C,
    receive: Vec<u8>,
    pending: Option<Transmit>,
    finished: bool,
}
impl<T, C> Driver<T, C> {
    fn stop(&mut self, kind: io::ErrorKind) {
        let w = {
            let mut s = self.shared.lock().unwrap();
            s.stopped = Some(kind);
            s.driver = None;
            s.notifications()
        };
        wake(w);
        self.finished = true;
    }
}
impl<T, C> Drop for Driver<T, C> {
    fn drop(&mut self) {
        if !self.finished {
            self.stop(io::ErrorKind::BrokenPipe);
        }
    }
}
impl<T: DatagramTransport + Unpin, C: Timer + Unpin> Future for Driver<T, C> {
    type Output = io::Result<()>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.finished {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        let result = this.poll_inner(cx);
        if let Poll::Ready(Err(e)) = &result {
            this.stop(e.kind());
        }
        result
    }
}
impl<T: DatagramTransport + Unpin, C: Timer + Unpin> Driver<T, C> {
    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        for _ in 0..64 {
            let now = self.timer.now();
            let wakers = {
                let mut s = self.shared.lock().unwrap();
                register(&mut s.driver, cx);
                s.core.update(now);
                if self.pending.is_none() {
                    self.pending = s.core.poll_transmit();
                }
                s.notifications()
            };
            wake(wakers);
            let mut progress = false;
            if let Some(p) = &self.pending {
                match Pin::new(&mut self.transport).poll_send(cx, p.peer, &p.payload) {
                    Poll::Ready(Ok(())) => {
                        self.pending = None;
                        progress = true;
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => {}
                }
            }
            match Pin::new(&mut self.transport).poll_recv(cx, &mut self.receive) {
                Poll::Ready(Ok((len, peer))) => {
                    if len > self.receive.len() {
                        return Poll::Ready(Err(io::ErrorKind::InvalidData.into()));
                    }
                    let mut s = self.shared.lock().unwrap();
                    // Invalid/foreign packets are not endpoint-wide transport failures.
                    let _ = s.core.input(peer, &self.receive[..len], now);
                    progress = true;
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => {}
            }
            if !progress {
                let deadline = self
                    .shared
                    .lock()
                    .unwrap()
                    .core
                    .deadline()
                    .unwrap_or(now.saturating_add(60_000));
                match Pin::new(&mut self.timer).poll_until(cx, deadline) {
                    Poll::Ready(Ok(())) => cx.waker().wake_by_ref(),
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => {}
                }
                return Poll::Pending;
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

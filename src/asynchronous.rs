//! Optional runtime-neutral futures-io streams. No implicit spawning or runtime globals.
use crate::endpoint::{
    Config, ConnectionId, ConnectionStats, EndpointCore, EndpointStats, Transmit,
};
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
    flush: Option<Waker>,
    connect: Option<Waker>,
    close: Option<Waker>,
}
struct Shared {
    core: EndpointCore,
    waiters: BTreeMap<ConnectionId, Waiters>,
    accept: Option<Waker>,
    driver: Option<Waker>,
    stopped: Option<io::ErrorKind>,
    shutdown_requested: bool,
}
impl Shared {
    fn check(&self) -> io::Result<()> {
        self.stopped.map_or(Ok(()), |e| Err(e.into()))
    }
    fn notifications(&mut self) -> Vec<Waker> {
        let mut out = Vec::new();
        let stopped = self.stopped.is_some();
        take_ready(
            &mut self.accept,
            stopped || self.core.has_pending_accept(),
            &mut out,
        );
        for (&id, waiter) in &mut self.waiters {
            // Errors are readiness too: the next poll must observe the failure.
            take_ready(
                &mut waiter.read,
                stopped || self.core.is_readable(id).unwrap_or(true),
                &mut out,
            );
            take_ready(
                &mut waiter.write,
                stopped || self.core.is_writable(id).unwrap_or(true),
                &mut out,
            );
            take_ready(
                &mut waiter.flush,
                stopped || self.core.is_flushed(id).unwrap_or(true),
                &mut out,
            );
            take_ready(
                &mut waiter.connect,
                stopped || self.core.is_connected(id).unwrap_or(true),
                &mut out,
            );
            let close_ready = match self.core.is_write_closed(id) {
                Ok(false) => self.core.is_writable(id).unwrap_or(true),
                Ok(true) => self.core.is_flushed(id).unwrap_or(true),
                Err(_) => true,
            };
            take_ready(&mut waiter.close, stopped || close_ready, &mut out);
        }
        out
    }
}
fn take_ready(slot: &mut Option<Waker>, ready: bool, out: &mut Vec<Waker>) {
    if ready && let Some(waker) = slot.take() {
        out.push(waker);
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

/// Listener and connection factory. Streams live independently of this handle.
/// Use `connector()` to open connections concurrently with `accept()` or in other tasks.
pub struct Endpoint {
    shared: Arc<Mutex<Shared>>,
}
impl Endpoint {
    /// Snapshot without driving I/O; also available after shutdown/failure.
    pub fn stats(&self) -> EndpointStats {
        self.shared.lock().unwrap().core.stats()
    }

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
            shutdown_requested: false,
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
    /// Immediately aborts all local sessions and wakes pending operations with
    /// ConnectionAborted. The driver returns Ok(()) when next polled. No FIN/RESET
    /// is sent. For concurrent accept/connect tasks, use Connector::shutdown.
    pub fn shutdown(&self) {
        self.connector().shutdown();
    }
    /// A clonable outgoing connection factory, independent of the listener's lifetime.
    pub fn connector(&self) -> Connector {
        Connector {
            shared: self.shared.clone(),
        }
    }
    /// Connects without borrowing the endpoint mutably. For simultaneous accept/connect,
    /// obtain a Connector first. Cancellation releases only this attempt's local slot.
    pub async fn connect(&self, peer: SocketAddr) -> io::Result<KcpStream> {
        self.connector().connect(peer).await
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

/// Clonable outgoing connection factory. Multiple connect futures may be pending
/// concurrently. Cloning/dropping it does not change listener or stream ownership.
#[derive(Clone)]
pub struct Connector {
    shared: Arc<Mutex<Shared>>,
}
impl Connector {
    /// Snapshot of the shared endpoint, not just connections opened by this clone.
    pub fn stats(&self) -> EndpointStats {
        self.shared.lock().unwrap().core.stats()
    }

    /// Permanently shuts down the entire endpoint, including listener and all clones.
    /// Idempotent; an earlier driver/transport failure keeps its original error.
    /// This is not graceful stream shutdown. An in-flight transport poll may finish
    /// concurrently; wait for driver completion before treating I/O as quiescent.
    pub fn shutdown(&self) {
        let wakers = {
            let mut s = self.shared.lock().unwrap();
            if s.stopped.is_some() {
                return;
            }
            s.shutdown_requested = true;
            s.stopped = Some(io::ErrorKind::ConnectionAborted);
            s.core.shutdown();
            let mut wakers = s.notifications();
            s.waiters.clear();
            if let Some(waker) = s.driver.take() {
                wakers.push(waker);
            }
            wakers
        };
        wake(wakers);
    }
    /// Cancellation drops the embryonic stream and releases its connection slot.
    pub async fn connect(&self, peer: SocketAddr) -> io::Result<KcpStream> {
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
}

/// A reliable byte stream with independent read and write halves. Not Clone.
/// A graceful close shuts down only writes; Drop otherwise aborts the session.
pub struct KcpStream {
    shared: Arc<Mutex<Shared>>,
    id: ConnectionId,
}
impl KcpStream {
    /// Includes a retained failed session's state. Driver termination releases the
    /// session and returns the endpoint's terminal error instead of a snapshot.
    pub fn stats(&self) -> io::Result<ConnectionStats> {
        let shared = self.shared.lock().unwrap();
        shared.check()?;
        shared.core.connection_stats(self.id)
    }

    pub fn id(&self) -> ConnectionId {
        self.id
    }
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        let shared = self.shared.lock().unwrap();
        shared.check()?;
        shared.core.peer_addr(self.id)
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
                register(&mut s.waiters.get_mut(&self.id).unwrap().flush, cx);
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
/// future wake every pending operation with an error. Explicit endpoint shutdown
/// completes this future with Ok(()). Does not spawn tasks itself.
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
            s.stopped.get_or_insert(kind);
            s.core.shutdown();
            s.driver = None;
            let wakers = s.notifications();
            s.waiters.clear();
            wakers
        };
        wake(w);
        self.pending = None;
        self.receive = Vec::new();
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
            let s = this.shared.lock().unwrap();
            return Poll::Ready(if s.shutdown_requested {
                Ok(())
            } else {
                Err(s.stopped.unwrap_or(io::ErrorKind::BrokenPipe).into())
            });
        }
        if this.shared.lock().unwrap().shutdown_requested {
            this.stop(io::ErrorKind::ConnectionAborted);
            return Poll::Ready(Ok(()));
        }
        let result = this.poll_inner(cx);
        if let Poll::Ready(outcome) = &result {
            this.stop(
                outcome
                    .as_ref()
                    .err()
                    .map_or(io::ErrorKind::ConnectionAborted, io::Error::kind),
            );
            if this.shared.lock().unwrap().shutdown_requested {
                return Poll::Ready(Ok(()));
            }
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
                if let Some(kind) = s.stopped {
                    return Poll::Ready(if s.shutdown_requested {
                        Ok(())
                    } else {
                        Err(kind.into())
                    });
                }
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

#![cfg(feature = "async")]
use futures_io::{AsyncRead, AsyncWrite};
use kcp_rs::{
    asynchronous::{DatagramTransport, Driver, Endpoint, KcpStream, Timer},
    endpoint::Config,
};
use std::{
    collections::VecDeque,
    future::Future,
    io,
    net::SocketAddr,
    pin::{Pin, pin},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

fn addr(n: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], n))
}
#[derive(Default)]
struct Inbox {
    packets: VecDeque<(Vec<u8>, SocketAddr)>,
    reader: Option<Waker>,
}
struct Transport {
    local: SocketAddr,
    input: Arc<Mutex<Inbox>>,
    output: Arc<Mutex<Inbox>>,
    blocked: Arc<AtomicBool>,
    writer: Arc<Mutex<Option<Waker>>>,
    fail: Arc<AtomicBool>,
}
impl DatagramTransport for Transport {
    fn poll_recv(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<(usize, SocketAddr)>> {
        if self.fail.load(Ordering::SeqCst) {
            return Poll::Ready(Err(io::ErrorKind::NetworkDown.into()));
        }
        let mut inbox = self.input.lock().unwrap();
        if let Some((p, addr)) = inbox.packets.pop_front() {
            buf[..p.len()].copy_from_slice(&p);
            Poll::Ready(Ok((p.len(), addr)))
        } else {
            inbox.reader = Some(cx.waker().clone());
            Poll::Pending
        }
    }
    fn poll_send(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        _: SocketAddr,
        buf: &[u8],
    ) -> Poll<io::Result<()>> {
        if self.blocked.load(Ordering::SeqCst) {
            *self.writer.lock().unwrap() = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let w = {
            let mut inbox = self.output.lock().unwrap();
            inbox.packets.push_back((buf.to_vec(), self.local));
            inbox.reader.take()
        };
        if let Some(w) = w {
            w.wake();
        }
        Poll::Ready(Ok(()))
    }
}
struct Clock(Arc<AtomicU64>);
impl Timer for Clock {
    fn now(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
    fn poll_until(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        deadline: u64,
    ) -> Poll<io::Result<()>> {
        if self.now() >= deadline {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}
// The test explicitly polls drivers after advancing time. Production Timer implementations
// must register a wakeup; this clock intentionally has no executor or real-time dependency.
#[derive(Default)]
struct Counter(AtomicUsize);
impl Wake for Counter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
type TestDriver = Driver<Transport, Clock>;
struct Pair {
    a: Endpoint,
    b: Endpoint,
    da: TestDriver,
    db: TestDriver,
    time: Arc<AtomicU64>,
    blocked: Arc<AtomicBool>,
    fail: Arc<AtomicBool>,
}
impl Pair {
    fn new() -> Self {
        Self::with_config(Config {
            send_capacity: 2,
            receive_capacity: 1400,
            output_capacity: 2,
            ..Config::default()
        })
    }
    fn with_config(config: Config) -> Self {
        let left = Arc::new(Mutex::new(Inbox::default()));
        let right = Arc::new(Mutex::new(Inbox::default()));
        let blocked = Arc::new(AtomicBool::new(false));
        let fail = Arc::new(AtomicBool::new(false));
        let time = Arc::new(AtomicU64::new(0));
        let t1 = Transport {
            local: addr(1),
            input: left.clone(),
            output: right.clone(),
            blocked: blocked.clone(),
            writer: Arc::default(),
            fail: fail.clone(),
        };
        let t2 = Transport {
            local: addr(2),
            input: right,
            output: left,
            blocked: Arc::default(),
            writer: Arc::default(),
            fail: Arc::default(),
        };
        let (a, da) = Endpoint::new(config.clone(), 1, t1, Clock(time.clone())).unwrap();
        let (b, db) = Endpoint::new(config, 100, t2, Clock(time.clone())).unwrap();
        Self {
            a,
            b,
            da,
            db,
            time,
            blocked,
            fail,
        }
    }
    fn streams(&mut self) -> (KcpStream, KcpStream) {
        let mut connect = pin!(self.a.connect(addr(2)));
        let mut accept = pin!(self.b.accept());
        let mut a = None;
        let mut b = None;
        let mut cx = Context::from_waker(Waker::noop());
        for _ in 0..1000 {
            if a.is_none()
                && let Poll::Ready(r) = connect.as_mut().poll(&mut cx)
            {
                a = Some(r.unwrap());
            }
            if b.is_none()
                && let Poll::Ready(r) = accept.as_mut().poll(&mut cx)
            {
                b = Some(r.unwrap());
            }
            if a.is_some() && b.is_some() {
                return (
                    a.take().expect("client connected"),
                    b.take().expect("server accepted"),
                );
            }
            self.time.fetch_add(10, Ordering::SeqCst);
            assert!(Pin::new(&mut self.da).poll(&mut cx).is_pending());
            assert!(Pin::new(&mut self.db).poll(&mut cx).is_pending());
        }
        panic!("handshake stalled");
    }
    fn step(&mut self) {
        self.time.fetch_add(10, Ordering::SeqCst);
        let mut cx = Context::from_waker(Waker::noop());
        assert!(Pin::new(&mut self.da).poll(&mut cx).is_pending());
        assert!(Pin::new(&mut self.db).poll(&mut cx).is_pending());
    }
}
#[test]
fn partial_writes_flush_close_and_eof() {
    let mut p = Pair::new();
    let (mut a, mut b) = p.streams();
    let mut cx = Context::from_waker(Waker::noop());
    let bytes = vec![73; 100000];
    let mut sent = 0;
    let mut received = Vec::new();
    let mut closed = false;
    let mut eof = false;
    let mut blocked = false;
    for _ in 0..10000 {
        if sent < bytes.len() {
            match Pin::new(&mut a).poll_write(&mut cx, &bytes[sent..]) {
                Poll::Ready(Ok(n)) => {
                    assert!(n > 0);
                    sent += n;
                }
                Poll::Ready(Err(e)) => panic!("{e}"),
                Poll::Pending => blocked = true,
            }
        } else if !closed && let Poll::Ready(r) = Pin::new(&mut a).poll_close(&mut cx) {
            r.unwrap();
            closed = true;
        }
        if sent < bytes.len() {
            assert!(Pin::new(&mut a).poll_flush(&mut cx).is_pending());
        }
        p.step();
        let mut buf = [0; 173];
        loop {
            match Pin::new(&mut b).poll_read(&mut cx, &mut buf) {
                Poll::Ready(Ok(0)) => {
                    eof = true;
                    break;
                }
                Poll::Ready(Ok(n)) => received.extend_from_slice(&buf[..n]),
                Poll::Ready(Err(e)) => panic!("{e}"),
                Poll::Pending => break,
            }
        }
        if eof && closed {
            break;
        }
    }
    assert!(eof && closed && blocked);
    assert_eq!(received, bytes);
    assert!(matches!(
        Pin::new(&mut a).poll_flush(&mut cx),
        Poll::Ready(Ok(()))
    ));
    assert!(matches!(
        Pin::new(&mut b).poll_write(&mut cx, b"reply"),
        Poll::Ready(Ok(5))
    ));
    let mut reply = [0; 5];
    for _ in 0..100 {
        p.step();
        if let Poll::Ready(r) = Pin::new(&mut a).poll_read(&mut cx, &mut reply) {
            assert_eq!(r.unwrap(), 5);
            assert_eq!(&reply, b"reply");
            return;
        }
    }
    panic!("half-close stopped reads");
}
#[test]
fn drop_driver_wakes_pending_reads_and_accepts() {
    let mut p = Pair::new();
    let (mut a, _b) = p.streams();
    let counter = Arc::new(Counter::default());
    let w = Waker::from(counter.clone());
    let mut cx = Context::from_waker(&w);
    assert!(
        Pin::new(&mut a)
            .poll_read(&mut cx, &mut [0; 1])
            .is_pending()
    );
    let mut accept = pin!(p.a.accept());
    assert!(accept.as_mut().poll(&mut cx).is_pending());
    drop(p.da);
    assert!(counter.0.load(Ordering::SeqCst) >= 2);
    assert!(
        matches!(Pin::new(&mut a).poll_read(&mut cx,&mut[0;1]),Poll::Ready(Err(e)) if e.kind()==io::ErrorKind::BrokenPipe)
    );
    assert!(
        matches!(accept.as_mut().poll(&mut cx),Poll::Ready(Err(e)) if e.kind()==io::ErrorKind::BrokenPipe)
    );
}
#[test]
fn cancelled_connect_releases_slot_and_clock_starts_on_driver_turn() {
    let mut p = Pair::new();
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..2000 {
        let mut connect = pin!(p.a.connect(addr(2)));
        assert!(connect.as_mut().poll(&mut cx).is_pending());
    }
    // An endpoint may have been idle a long time before the connect request arrives.
    p.time.store(1_000_000, Ordering::SeqCst);
    let (_a, _b) = p.streams();
}
#[test]
fn blocked_transport_and_pending_close_do_not_self_wake_loop() {
    let mut p = Pair::new();
    let (mut a, _b) = p.streams();
    p.blocked.store(true, Ordering::SeqCst);
    let counter = Arc::new(Counter::default());
    let w = Waker::from(counter.clone());
    let mut cx = Context::from_waker(&w);
    assert!(Pin::new(&mut a).poll_close(&mut cx).is_pending());
    p.time.fetch_add(10, Ordering::SeqCst);
    assert!(Pin::new(&mut p.da).poll(&mut cx).is_pending());
    counter.0.store(0, Ordering::SeqCst);
    assert!(Pin::new(&mut a).poll_close(&mut cx).is_pending());
    assert_eq!(counter.0.load(Ordering::SeqCst), 0);
    // Register the application with a different waker so driver notifications aren't
    // mistaken for driver self-wakeups.
    let mut appcx = Context::from_waker(Waker::noop());
    assert!(Pin::new(&mut a).poll_close(&mut appcx).is_pending());
    for _ in 0..10 {
        counter.0.store(0, Ordering::SeqCst);
        assert!(Pin::new(&mut p.da).poll(&mut cx).is_pending());
        assert_eq!(counter.0.load(Ordering::SeqCst), 0);
    }
}
#[test]
fn transport_failure_is_visible_to_streams() {
    let mut p = Pair::new();
    let (mut a, _b) = p.streams();
    p.fail.store(true, Ordering::SeqCst);
    let mut cx = Context::from_waker(Waker::noop());
    assert!(
        matches!(Pin::new(&mut p.da).poll(&mut cx),Poll::Ready(Err(e)) if e.kind()==io::ErrorKind::NetworkDown)
    );
    assert!(
        matches!(Pin::new(&mut a).poll_write(&mut cx,b"x"),Poll::Ready(Err(e)) if e.kind()==io::ErrorKind::NetworkDown)
    );
}

#[test]
fn dropping_driver_wakes_connect_write_flush_and_close() {
    let mut p = Pair::new();
    let (mut a, _b) = p.streams();
    p.blocked.store(true, Ordering::SeqCst);
    let counter = Arc::new(Counter::default());
    let w = Waker::from(counter.clone());
    let mut cx = Context::from_waker(&w);
    for _ in 0..2 {
        assert!(matches!(
            Pin::new(&mut a).poll_write(&mut cx, b"x"),
            Poll::Ready(Ok(1))
        ));
    }
    assert!(Pin::new(&mut a).poll_write(&mut cx, b"x").is_pending());
    assert!(Pin::new(&mut a).poll_flush(&mut cx).is_pending());
    assert!(Pin::new(&mut a).poll_close(&mut cx).is_pending());
    let mut connect = pin!(p.a.connect(addr(3)));
    assert!(connect.as_mut().poll(&mut cx).is_pending());
    counter.0.store(0, Ordering::SeqCst);
    drop(p.da);
    assert!(counter.0.load(Ordering::SeqCst) >= 3);
    assert!(matches!(
        connect.as_mut().poll(&mut cx),
        Poll::Ready(Err(_))
    ));
    assert!(matches!(
        Pin::new(&mut a).poll_write(&mut cx, b"x"),
        Poll::Ready(Err(_))
    ));
    assert!(matches!(
        Pin::new(&mut a).poll_flush(&mut cx),
        Poll::Ready(Err(_))
    ));
    assert!(matches!(
        Pin::new(&mut a).poll_close(&mut cx),
        Poll::Ready(Err(_))
    ));
}

#[test]
fn idle_driver_does_not_wake_readers_or_acceptors() {
    let mut p = Pair::new();
    let (mut a, mut b) = p.streams();
    let reader = Arc::new(Counter::default());
    let acceptor = Arc::new(Counter::default());
    let rw = Waker::from(reader.clone());
    let aw = Waker::from(acceptor.clone());
    let mut rcx = Context::from_waker(&rw);
    let mut acx = Context::from_waker(&aw);
    let mut driver_cx = Context::from_waker(Waker::noop());
    assert!(
        Pin::new(&mut a)
            .poll_read(&mut rcx, &mut [0; 8])
            .is_pending()
    );
    let mut accept = pin!(p.a.accept());
    assert!(accept.as_mut().poll(&mut acx).is_pending());
    for _ in 0..100 {
        p.time.fetch_add(10, Ordering::SeqCst);
        assert!(Pin::new(&mut p.da).poll(&mut driver_cx).is_pending());
        assert!(Pin::new(&mut p.db).poll(&mut driver_cx).is_pending());
    }
    assert_eq!(reader.0.load(Ordering::SeqCst), 0);
    assert_eq!(acceptor.0.load(Ordering::SeqCst), 0);
    assert!(matches!(
        Pin::new(&mut b).poll_write(&mut driver_cx, b"hello"),
        Poll::Ready(Ok(5))
    ));
    for _ in 0..100 {
        p.time.fetch_add(10, Ordering::SeqCst);
        assert!(Pin::new(&mut p.da).poll(&mut driver_cx).is_pending());
        assert!(Pin::new(&mut p.db).poll(&mut driver_cx).is_pending());
        if reader.0.load(Ordering::SeqCst) != 0 {
            break;
        }
    }
    assert_eq!(reader.0.load(Ordering::SeqCst), 1);
    assert_eq!(acceptor.0.load(Ordering::SeqCst), 0);
    let mut buf = [0; 8];
    assert!(matches!(
        Pin::new(&mut a).poll_read(&mut rcx, &mut buf),
        Poll::Ready(Ok(5))
    ));
    assert_eq!(&buf[..5], b"hello");
}

#[test]
fn writable_capacity_does_not_spuriously_wake_flush() {
    let mut p = Pair::new();
    let (mut a, _b) = p.streams();
    p.blocked.store(true, Ordering::SeqCst);
    let counter = Arc::new(Counter::default());
    let w = Waker::from(counter.clone());
    let mut cx = Context::from_waker(&w);
    assert!(matches!(
        Pin::new(&mut a).poll_write(&mut cx, b"x"),
        Poll::Ready(Ok(1))
    ));
    // One of two send slots is still available, but flush must wait for the ACK.
    assert!(Pin::new(&mut a).poll_flush(&mut cx).is_pending());
    for _ in 0..10 {
        p.step();
    }
    assert_eq!(counter.0.load(Ordering::SeqCst), 0);
    p.blocked.store(false, Ordering::SeqCst);
    for _ in 0..100 {
        p.step();
        if counter.0.load(Ordering::SeqCst) != 0 {
            break;
        }
    }
    assert_eq!(counter.0.load(Ordering::SeqCst), 1);
    assert!(matches!(
        Pin::new(&mut a).poll_flush(&mut cx),
        Poll::Ready(Ok(()))
    ));
}

#[test]
fn cloned_connectors_open_concurrently_with_bidirectional_accept() {
    let mut p = Pair::new();
    let ca = p.a.connector();
    let ca2 = ca.clone();
    let cb = p.b.connector();
    let mut cx = Context::from_waker(Waker::noop());
    // A cancelled attempt must not disturb other attempts on the same connector.
    {
        let mut cancelled = pin!(ca.connect(addr(2)));
        assert!(cancelled.as_mut().poll(&mut cx).is_pending());
    }
    let mut operations = pin!(async {
        let opens = futures_lite::future::zip(ca.connect(addr(2)), ca2.connect(addr(2)));
        let accepts = async {
            let a = p.a.accept().await.unwrap();
            let b1 = p.b.accept().await.unwrap();
            let b2 = p.b.accept().await.unwrap();
            (a, b1, b2)
        };
        let ((a1, a2), (b, (accepted_a, accepted_b1, accepted_b2))) = futures_lite::future::zip(
            opens,
            futures_lite::future::zip(cb.connect(addr(1)), accepts),
        )
        .await;
        (
            a1.unwrap(),
            a2.unwrap(),
            b.unwrap(),
            accepted_a,
            accepted_b1,
            accepted_b2,
        )
    });
    for _ in 0..1000 {
        if let Poll::Ready((a1, a2, b, accepted_a, accepted_b1, accepted_b2)) =
            operations.as_mut().poll(&mut cx)
        {
            assert_ne!(a1.id(), a2.id());
            assert_ne!(a1.id(), accepted_a.id());
            assert_ne!(b.id(), accepted_b1.id());
            assert_ne!(accepted_b1.id(), accepted_b2.id());
            return;
        }
        p.time.fetch_add(10, Ordering::SeqCst);
        assert!(Pin::new(&mut p.da).poll(&mut cx).is_pending());
        assert!(Pin::new(&mut p.db).poll(&mut cx).is_pending());
    }
    panic!("concurrent connect/accept stalled");
}

#[test]
fn connector_survives_listener_drop_and_observes_driver_drop() {
    let mut p = Pair::new();
    let connector = p.a.connector();
    drop(p.a);
    let mut cx = Context::from_waker(Waker::noop());
    let mut connected = false;
    {
        let mut operations = pin!(futures_lite::future::zip(
            connector.connect(addr(2)),
            p.b.accept()
        ));
        for _ in 0..1000 {
            if let Poll::Ready((a, b)) = operations.as_mut().poll(&mut cx) {
                assert!(a.is_ok() && b.is_ok());
                connected = true;
                break;
            }
            p.time.fetch_add(10, Ordering::SeqCst);
            assert!(Pin::new(&mut p.da).poll(&mut cx).is_pending());
            assert!(Pin::new(&mut p.db).poll(&mut cx).is_pending());
        }
    }
    assert!(connected);
    drop(p.da);
    let mut connection = pin!(connector.connect(addr(2)));
    assert!(
        matches!(connection.as_mut().poll(&mut cx), Poll::Ready(Err(e)) if e.kind() == io::ErrorKind::BrokenPipe)
    );
}

#[test]
fn keep_alive_is_invisible_to_pending_stream_reads() {
    let mut p = Pair::with_config(Config {
        idle_timeout: 500,
        keep_alive_interval: Some(50),
        ..Config::default()
    });
    let (mut a, _b) = p.streams();
    let counter = Arc::new(Counter::default());
    let w = Waker::from(counter.clone());
    let mut cx = Context::from_waker(&w);
    assert!(
        Pin::new(&mut a)
            .poll_read(&mut cx, &mut [0; 1])
            .is_pending()
    );
    for _ in 0..1000 {
        p.step();
    }
    assert_eq!(counter.0.load(Ordering::SeqCst), 0);
    assert!(
        Pin::new(&mut a)
            .poll_read(&mut cx, &mut [0; 1])
            .is_pending()
    );
    assert!(matches!(
        Pin::new(&mut a).poll_write(&mut cx, b"still alive"),
        Poll::Ready(Ok(11))
    ));
}

#[test]
fn explicit_shutdown_wakes_all_operations_and_completes_driver() {
    let mut p = Pair::new();
    let (mut a, _b) = p.streams();
    let control = p.a.connector();
    let opener = control.clone();
    let counter = Arc::new(Counter::default());
    let w = Waker::from(counter.clone());
    let mut cx = Context::from_waker(&w);
    p.blocked.store(true, Ordering::SeqCst);
    for _ in 0..2 {
        assert!(matches!(
            Pin::new(&mut a).poll_write(&mut cx, b"x"),
            Poll::Ready(Ok(1))
        ));
    }
    assert!(Pin::new(&mut a).poll_write(&mut cx, b"x").is_pending());
    assert!(Pin::new(&mut a).poll_flush(&mut cx).is_pending());
    assert!(Pin::new(&mut a).poll_close(&mut cx).is_pending());
    assert!(
        Pin::new(&mut a)
            .poll_read(&mut cx, &mut [0; 1])
            .is_pending()
    );
    let mut accept = pin!(p.a.accept());
    let mut connect = pin!(opener.connect(addr(2)));
    assert!(accept.as_mut().poll(&mut cx).is_pending());
    assert!(connect.as_mut().poll(&mut cx).is_pending());
    assert!(Pin::new(&mut p.da).poll(&mut cx).is_pending());
    counter.0.store(0, Ordering::SeqCst);
    control.shutdown();
    assert_eq!(counter.0.load(Ordering::SeqCst), 7);
    control.shutdown();
    assert_eq!(counter.0.load(Ordering::SeqCst), 7);
    assert!(
        matches!(accept.as_mut().poll(&mut cx),Poll::Ready(Err(e)) if e.kind()==io::ErrorKind::ConnectionAborted)
    );
    assert!(
        matches!(connect.as_mut().poll(&mut cx),Poll::Ready(Err(e)) if e.kind()==io::ErrorKind::ConnectionAborted)
    );
    assert!(
        matches!(Pin::new(&mut a).poll_read(&mut cx,&mut[0;1]),Poll::Ready(Err(e)) if e.kind()==io::ErrorKind::ConnectionAborted)
    );
    assert!(
        matches!(Pin::new(&mut a).poll_write(&mut cx,b"x"),Poll::Ready(Err(e)) if e.kind()==io::ErrorKind::ConnectionAborted)
    );
    assert!(
        matches!(Pin::new(&mut a).poll_flush(&mut cx),Poll::Ready(Err(e)) if e.kind()==io::ErrorKind::ConnectionAborted)
    );
    assert!(
        matches!(Pin::new(&mut a).poll_close(&mut cx),Poll::Ready(Err(e)) if e.kind()==io::ErrorKind::ConnectionAborted)
    );
    // A stopped driver must not poll the transport again, even if it would fail.
    p.fail.store(true, Ordering::SeqCst);
    assert!(matches!(
        Pin::new(&mut p.da).poll(&mut cx),
        Poll::Ready(Ok(()))
    ));
    drop(p.da);
    assert!(
        matches!(Pin::new(&mut a).poll_read(&mut cx,&mut[0;1]),Poll::Ready(Err(e)) if e.kind()==io::ErrorKind::ConnectionAborted)
    );
}

#[test]
fn shutdown_before_first_driver_poll_rejects_new_connections() {
    let mut p = Pair::new();
    let connector = p.a.connector();
    p.a.shutdown();
    p.a.shutdown();
    let mut cx = Context::from_waker(Waker::noop());
    let mut connect = pin!(connector.connect(addr(2)));
    assert!(
        matches!(connect.as_mut().poll(&mut cx),Poll::Ready(Err(e)) if e.kind()==io::ErrorKind::ConnectionAborted)
    );
    let mut accept = pin!(p.a.accept());
    assert!(
        matches!(accept.as_mut().poll(&mut cx),Poll::Ready(Err(e)) if e.kind()==io::ErrorKind::ConnectionAborted)
    );
    assert!(matches!(
        Pin::new(&mut p.da).poll(&mut cx),
        Poll::Ready(Ok(()))
    ));
}

#[test]
fn shutdown_does_not_overwrite_an_earlier_transport_failure() {
    let mut p = Pair::new();
    let (mut a, _b) = p.streams();
    let connector = p.a.connector();
    let mut cx = Context::from_waker(Waker::noop());
    p.fail.store(true, Ordering::SeqCst);
    assert!(
        matches!(Pin::new(&mut p.da).poll(&mut cx),Poll::Ready(Err(e)) if e.kind()==io::ErrorKind::NetworkDown)
    );
    connector.shutdown();
    assert_eq!(
        a.peer_addr().unwrap_err().kind(),
        io::ErrorKind::NetworkDown
    );
    assert!(
        matches!(Pin::new(&mut a).poll_read(&mut cx,&mut[0;1]),Poll::Ready(Err(e)) if e.kind()==io::ErrorKind::NetworkDown)
    );
    let mut connect = pin!(connector.connect(addr(2)));
    assert!(
        matches!(connect.as_mut().poll(&mut cx),Poll::Ready(Err(e)) if e.kind()==io::ErrorKind::NetworkDown)
    );
}

#[test]
fn asynchronous_snapshots_share_endpoint_state_and_reflect_shutdown() {
    let mut p = Pair::new();
    let (mut a, _b) = p.streams();
    let connector = p.a.connector();
    assert_eq!(p.a.stats(), connector.stats());
    assert_eq!(connector.stats().established, 1);
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(
        Pin::new(&mut a).poll_write(&mut cx, b"abc"),
        Poll::Ready(Ok(3))
    ));
    assert_eq!(a.stats().unwrap().kcp.pending_send_bytes, 4);
    connector.shutdown();
    assert!(connector.stats().shutdown);
    assert_eq!(connector.stats().connections, 0);
    assert_eq!(connector.stats(), p.a.stats());
    assert_eq!(
        a.stats().unwrap_err().kind(),
        io::ErrorKind::ConnectionAborted
    );
}

#[test]
fn unaligned_retransmission_does_not_spin_the_driver() {
    let mut p = Pair::with_config(Config {
        kcp: kcp_rs::Config {
            interval: 30,
            nodelay: true,
            congestion_control: false,
            ..Default::default()
        },
        ..Config::default()
    });
    let (mut a, _b) = p.streams();
    let counter = Arc::new(Counter::default());
    let w = Waker::from(counter.clone());
    let mut cx = Context::from_waker(&w);
    assert!(matches!(
        Pin::new(&mut a).poll_write(&mut cx, b"lost"),
        Poll::Ready(Ok(4))
    ));
    // Stop driving B, so A receives no ACK. Poll around every millisecond of the
    // non-aligned RTO, rather than hiding an immediate-deadline loop with 10 ms ticks.
    for _ in 0..600 {
        let now = p.time.fetch_add(1, Ordering::SeqCst) + 1;
        counter.0.store(0, Ordering::SeqCst);
        assert!(Pin::new(&mut p.da).poll(&mut cx).is_pending());
        assert_eq!(
            counter.0.load(Ordering::SeqCst),
            0,
            "driver self-woke without progress at {now} ms"
        );
    }
    assert!(a.stats().unwrap().kcp.timeout_retransmissions > 0);
}

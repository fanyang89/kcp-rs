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
        let config = Config {
            send_capacity: 2,
            receive_capacity: 1400,
            output_capacity: 2,
            ..Config::default()
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

use std::{
    future::{self, Future},
    net::{Ipv4Addr, UdpSocket as StdUdpSocket},
    ops::{RangeBounds, RangeFull},
    panic::{self, AssertUnwindSafe},
    pin::pin,
    sync::Arc,
    task::{Context, Waker},
};

use tokio_uring::{
    buf::{BoundedBuf, Slice},
    net::UdpSocket,
};

struct TrackedBuffer {
    bytes: Vec<u8>,
    _retained: Arc<()>,
}

impl BoundedBuf for TrackedBuffer {
    type Buf = Vec<u8>;
    type Bounds = RangeFull;

    fn slice(self, range: impl RangeBounds<usize>) -> Slice<Self::Buf> {
        self.bytes.slice(range)
    }

    fn slice_full(self) -> Slice<Self::Buf> {
        self.bytes.slice_full()
    }

    fn get_buf(&self) -> &Self::Buf {
        &self.bytes
    }

    fn bounds(&self) -> Self::Bounds {
        ..
    }

    fn from_buf_bounds(bytes: Self::Buf, _: Self::Bounds) -> Self {
        Self {
            bytes,
            _retained: Arc::new(()),
        }
    }

    fn stable_ptr(&self) -> *const u8 {
        self.bytes.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.bytes.len()
    }

    fn bytes_total(&self) -> usize {
        self.bytes.capacity()
    }
}

#[test]
fn io_uring_panic_retains_pending_send_buffer() {
    let receiver = StdUdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let destination = receiver.local_addr().unwrap();
    let retained = Arc::new(());
    let outcome = panic::catch_unwind(AssertUnwindSafe(|| {
        super::run_io_uring(async {
            let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0).into())
                .await
                .unwrap();
            let buffer = TrackedBuffer {
                bytes: vec![1],
                _retained: Arc::clone(&retained),
            };
            let mut send = pin!(socket.send_to(buffer, destination));
            let mut context = Context::from_waker(Waker::noop());
            assert!(send.as_mut().poll(&mut context).is_pending());
            // Unwind before the runtime can dispatch this send's completion.
            panic!("packet loop panic with a pending send");
        })
        .unwrap();
    }));
    assert!(outcome.is_err());
    assert_eq!(
        Arc::strong_count(&retained),
        2,
        "pending send storage freed"
    );
}

#[test]
fn io_uring_normal_return_releases_runtime_sockets() {
    let socket = StdUdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let address = socket.local_addr().unwrap();
    super::run_io_uring(async move {
        let socket = UdpSocket::from_std(socket);
        tokio_uring::spawn(async move {
            let _socket = socket;
            future::pending::<()>().await;
        });
    })
    .unwrap();
    assert!(StdUdpSocket::bind(address).is_ok());
}

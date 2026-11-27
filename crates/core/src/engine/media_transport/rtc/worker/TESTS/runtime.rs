use std::{
    future::{self, Future},
    net::{Ipv4Addr, UdpSocket as StdUdpSocket},
    panic::{self, AssertUnwindSafe},
    pin::pin,
    task::{Context, Waker},
};

use bytes::Bytes;
use tokio_uring::net::UdpSocket;

#[test]
fn io_uring_panic_retains_pending_send_buffer() {
    let receiver = StdUdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let destination = receiver.local_addr().unwrap();
    // A non-empty buffer avoids static storage, which never reports unique.
    let observer = Bytes::from(vec![1]);
    let outcome = panic::catch_unwind(AssertUnwindSafe(|| {
        super::run_io_uring(async {
            let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0).into())
                .await
                .unwrap();
            let mut send = pin!(socket.send_to(observer.clone(), destination));
            let mut context = Context::from_waker(Waker::noop());
            assert!(send.as_mut().poll(&mut context).is_pending());
            // Unwind before the runtime can dispatch this send's completion.
            panic!("packet loop panic with a pending send");
        })
        .unwrap();
    }));
    assert!(outcome.is_err());
    assert!(!observer.is_unique(), "pending send storage freed");
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

//! The receive task spawned by [`UdpIngress`] owns backend I/O while the packet
//! loop selects on a bounded completed-datagram queue. The packet loop can cancel
//! its channel wait for control or deadlines without restarting a receive. Once
//! the queue is full, the receive task waits for capacity instead of growing
//! user-space backlog.
//!
//! The task runs on the worker's current-thread runtime rather than a separate
//! I/O thread. Tokio borrows receive storage while `tokio-uring` owns it until
//! completion. Processed buffers return through a second bounded queue so reuse
//! never blocks the packet loop.

#[cfg(target_os = "linux")]
use std::rc::Rc;
use std::{
    io,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket as StdUdpSocket},
    sync::Arc,
    time::Instant,
};

use tokio::{net::UdpSocket as TokioUdpSocket, sync::mpsc};
#[cfg(target_os = "linux")]
use tokio_uring::net::UdpSocket as TokioUringUdpSocket;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::super::worker::buffers::RECEIVE_BUFFER_LEN;
use crate::RtcUdpIoBackend;

const INGRESS_QUEUE_CAPACITY: usize = 32;
const RECEIVE_BUFFER_POOL_CAPACITY: usize = 32;

/// Worker socket shared by packet-loop egress and one receive task.
///
/// Tokio uses [`Arc`] because `tokio::spawn` requires `Send`. `tokio-uring` uses
/// [`std::rc::Rc`] because its task stays on the worker's local runtime.
#[derive(Clone)]
pub enum RtcUdpSocket {
    Tokio(Arc<TokioUdpSocket>),
    #[cfg(target_os = "linux")]
    IoUring(Rc<TokioUringUdpSocket>),
}

/// Completed receive annotated with the local address expected by str0m.
///
/// The backend receive APIs supply only the peer address. `candidate_addr`
/// preserves the local candidate identity needed by [`str0m::Input::Receive`].
pub(crate) struct UdpDatagram {
    pub(in super::super) source_addr: SocketAddr,
    pub(in super::super) candidate_addr: SocketAddr,
    /// Socket-completion time captured before ingress-queue backpressure.
    ///
    /// str0m uses this clock for jitter and bandwidth timing.
    pub(in super::super) received_at: Instant,
    pub(in super::super) packet: Vec<u8>,
}

/// Receive-side datagram pump for one worker socket.
pub struct UdpIngress {
    rx: mpsc::Receiver<UdpDatagram>,
    recycle_tx: mpsc::Sender<Vec<u8>>,
    shutdown: CancellationToken,
    wake_addr: SocketAddr,
}

#[cfg(feature = "internal-benchmarks")]
pub struct UdpIngressBenchHarness {
    tx: mpsc::Sender<UdpDatagram>,
    ingress: UdpIngress,
    recycle_rx: mpsc::Receiver<Vec<u8>>,
}

impl RtcUdpSocket {
    /// Wraps a bound nonblocking socket for the selected worker runtime.
    ///
    /// The caller must configure `socket` as nonblocking before selecting Tokio.
    ///
    /// # Errors
    ///
    /// Returns `Unsupported` for `io_uring` outside Linux. Tokio socket-inspection
    /// or runtime-registration errors are forwarded.
    ///
    /// # Panics
    ///
    /// The Tokio backend may panic when `socket` is blocking or no Tokio runtime
    /// is entered.
    pub fn from_std(socket: StdUdpSocket, backend: RtcUdpIoBackend) -> io::Result<Self> {
        match backend {
            RtcUdpIoBackend::Tokio => TokioUdpSocket::from_std(socket)
                .map(Arc::new)
                .map(Self::Tokio),
            RtcUdpIoBackend::IoUring => {
                #[cfg(target_os = "linux")]
                {
                    Ok(Self::IoUring(Rc::new(TokioUringUdpSocket::from_std(
                        socket,
                    ))))
                }

                #[cfg(not(target_os = "linux"))]
                {
                    Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "io_uring RTC UDP I/O backend is only supported on Linux",
                    ))
                }
            }
        }
    }

    /// Sends one datagram to `destination`.
    pub(in super::super) async fn send_to(
        &self,
        packet: Vec<u8>,
        destination: SocketAddr,
    ) -> io::Result<usize> {
        match self {
            Self::Tokio(socket) => socket.send_to(packet.as_slice(), destination).await,
            #[cfg(target_os = "linux")]
            Self::IoUring(socket) => {
                // io_uring retains the buffer until completion, so this wrapper
                // takes `Vec<u8>` by value.
                let (result, _packet) = socket.send_to(packet, destination).await;
                result
            }
        }
    }
}

impl UdpIngress {
    /// Starts receive ingress on the current worker runtime.
    ///
    /// `bind_addr` supplies the address family and port used to wake shutdown.
    /// `candidate_addr` becomes the local address passed to str0m.
    ///
    /// # Panics
    ///
    /// Panics outside the runtime required by the selected socket backend.
    pub fn new(socket: RtcUdpSocket, bind_addr: SocketAddr, candidate_addr: SocketAddr) -> Self {
        let (tx, rx) = mpsc::channel(INGRESS_QUEUE_CAPACITY);
        let (recycle_tx, recycle_rx) = mpsc::channel(RECEIVE_BUFFER_POOL_CAPACITY);
        let shutdown = CancellationToken::new();
        let wake_addr = udp_wake_addr(bind_addr);
        spawn_ingress(socket, candidate_addr, tx, recycle_rx, shutdown.clone());
        Self {
            rx,
            recycle_tx,
            shutdown,
            wake_addr,
        }
    }

    pub(in super::super) fn try_recv(&mut self) -> Option<UdpDatagram> {
        self.rx.try_recv().ok()
    }

    pub(in super::super) async fn recv(&mut self) -> Option<UdpDatagram> {
        self.rx.recv().await
    }

    /// Returns reusable receive storage without backpressuring the packet loop.
    ///
    /// Only buffers retaining [`RECEIVE_BUFFER_LEN`] capacity enter the bounded
    /// pool. A full pool drops the buffer because reuse is opportunistic.
    pub(in super::super) fn recycle(&self, mut packet: Vec<u8>) {
        if packet.capacity() < RECEIVE_BUFFER_LEN {
            return;
        }
        packet.clear();
        let _ = self.recycle_tx.try_send(packet);
    }
}

#[cfg(feature = "internal-benchmarks")]
impl UdpIngressBenchHarness {
    pub fn new(wake_addr: SocketAddr) -> Self {
        let (tx, rx) = mpsc::channel(INGRESS_QUEUE_CAPACITY);
        let (recycle_tx, recycle_rx) = mpsc::channel(RECEIVE_BUFFER_POOL_CAPACITY);
        let ingress = UdpIngress {
            rx,
            recycle_tx,
            shutdown: CancellationToken::new(),
            wake_addr,
        };
        Self {
            tx,
            ingress,
            recycle_rx,
        }
    }

    pub fn enqueue_completed_datagram(
        &mut self,
        source_addr: SocketAddr,
        candidate_addr: SocketAddr,
        received_at: Instant,
        payload: &[u8],
    ) -> bool {
        let mut packet = receive_buffer(&mut self.recycle_rx);
        packet.extend_from_slice(payload);
        self.tx
            .try_send(UdpDatagram {
                source_addr,
                candidate_addr,
                received_at,
                packet,
            })
            .is_ok()
    }

    pub fn ingress_mut(&mut self) -> &mut UdpIngress {
        &mut self.ingress
    }
}

impl Drop for UdpIngress {
    fn drop(&mut self) {
        self.shutdown.cancel();
        // The token cannot wake a task pending on socket I/O. Send a best-effort
        // datagram only after cancellation so the post-I/O check discards it.
        wake_udp_receiver(self.wake_addr);
    }
}

fn spawn_ingress(
    socket: RtcUdpSocket,
    candidate_addr: SocketAddr,
    tx: mpsc::Sender<UdpDatagram>,
    recycle_rx: mpsc::Receiver<Vec<u8>>,
    shutdown: CancellationToken,
) {
    match socket {
        RtcUdpSocket::Tokio(socket) => {
            tokio::spawn(run_tokio_ingress(
                socket,
                candidate_addr,
                tx,
                recycle_rx,
                shutdown,
            ));
        }
        #[cfg(target_os = "linux")]
        RtcUdpSocket::IoUring(socket) => {
            tokio_uring::spawn(run_io_uring_ingress(
                socket,
                candidate_addr,
                tx,
                recycle_rx,
                shutdown,
            ));
        }
    }
}

async fn run_tokio_ingress(
    socket: Arc<TokioUdpSocket>,
    candidate_addr: SocketAddr,
    tx: mpsc::Sender<UdpDatagram>,
    mut recycle_rx: mpsc::Receiver<Vec<u8>>,
    shutdown: CancellationToken,
) {
    loop {
        if shutdown.is_cancelled() {
            return;
        }
        let mut packet = receive_buffer(&mut recycle_rx);
        let result = socket.recv_buf_from(&mut packet).await;
        let received_at = Instant::now();
        if shutdown.is_cancelled() {
            return;
        }
        if ingress_should_stop(result, packet, candidate_addr, received_at, &tx, &shutdown).await {
            return;
        }
    }
}

#[cfg(target_os = "linux")]
async fn run_io_uring_ingress(
    socket: Rc<TokioUringUdpSocket>,
    candidate_addr: SocketAddr,
    tx: mpsc::Sender<UdpDatagram>,
    mut recycle_rx: mpsc::Receiver<Vec<u8>>,
    shutdown: CancellationToken,
) {
    loop {
        if shutdown.is_cancelled() {
            return;
        }
        let buffer = receive_buffer(&mut recycle_rx);
        let (result, packet) = socket.recv_from(buffer).await;
        let received_at = Instant::now();
        if shutdown.is_cancelled() {
            return;
        }
        if ingress_should_stop(result, packet, candidate_addr, received_at, &tx, &shutdown).await {
            return;
        }
    }
}

async fn ingress_should_stop(
    result: io::Result<(usize, SocketAddr)>,
    mut packet: Vec<u8>,
    candidate_addr: SocketAddr,
    received_at: Instant,
    tx: &mpsc::Sender<UdpDatagram>,
    shutdown: &CancellationToken,
) -> bool {
    match result {
        Ok((received_size, source_addr)) => {
            packet.truncate(received_size);
            let datagram = UdpDatagram {
                source_addr,
                candidate_addr,
                received_at,
                packet,
            };
            // If queue backpressure suspends `send`, ready cancellation wins when
            // both branches are polled again. The losing send drops the datagram.
            tokio::select! {
                biased;
                () = shutdown.cancelled() => true,
                send_result = tx.send(datagram) => send_result.is_err(),
            }
        }
        Err(error) => {
            warn!(?error, "rtc packet loop failed to receive datagram");
            false
        }
    }
}

fn udp_wake_addr(bind_addr: SocketAddr) -> SocketAddr {
    // A wildcard bind has the right port and address family but no concrete
    // destination, so target the matching loopback address.
    match bind_addr {
        SocketAddr::V4(addr) if addr.ip().is_unspecified() => {
            SocketAddr::from((Ipv4Addr::LOCALHOST, addr.port()))
        }
        SocketAddr::V6(addr) if addr.ip().is_unspecified() => {
            SocketAddr::from((Ipv6Addr::LOCALHOST, addr.port()))
        }
        addr => addr,
    }
}

fn wake_udp_receiver(addr: SocketAddr) {
    let bind_addr = match addr {
        SocketAddr::V4(_) => SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)),
        SocketAddr::V6(_) => SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0)),
    };
    let Ok(socket) = StdUdpSocket::bind(bind_addr) else {
        return;
    };
    let _ = socket.send_to(&[], addr);
}

fn receive_buffer(recycle_rx: &mut mpsc::Receiver<Vec<u8>>) -> Vec<u8> {
    let mut buffer = recycle_rx
        .try_recv()
        .unwrap_or_else(|_| Vec::with_capacity(RECEIVE_BUFFER_LEN));
    buffer.clear();
    buffer
}

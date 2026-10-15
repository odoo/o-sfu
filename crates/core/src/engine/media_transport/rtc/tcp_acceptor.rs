#[cfg(test)]
use std::{
    collections::HashMap,
    sync::{LazyLock, Mutex},
};
use std::{
    io::{
        self,
        ErrorKind::{self, WouldBlock},
    },
    net::{SocketAddr, TcpListener as StdTcpListener},
    sync::Arc,
    thread,
    time::Instant,
};

use o_sfu_rfc::tcp_framing::{FrameDecoder, FrameTooLargeError, TcpFrame};
use str0m::net::Protocol;
use thiserror::Error;
use tokio::{
    io::AsyncWriteExt,
    net::{
        TcpListener, TcpStream,
        tcp::{ReadHalf, WriteHalf},
    },
    runtime::Builder as TokioRuntimeBuilder,
    select,
    sync::mpsc,
    time::{Duration, sleep, timeout},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::engine::media_transport::rtc::{
    RtcWorker, UfragWorkerMap,
    packet_loop::{
        IngressPacket,
        ingress_routing::{PacketIndexProbe, packet_index_probe},
    },
    worker::session_drain::SESSION_DRAIN_MAX_TRANSMITS,
};

const TCP_ACCEPT_BACKOFF_MAX: Duration = Duration::from_millis(100);
// SFU is an ICE-lite server, so the peer is the one sending consent freshness, the server
// only answers. According to RFC 7675, those should come every 4 to 6 seconds. The RFC
// states that consent expires after 30s. Consider it dead after 35s to avoid false
// positives.
#[cfg(not(test))]
const TCP_STALL_PEER_DELAY: Duration = Duration::from_secs(35);
#[cfg(test)]
pub(super) const TCP_STALL_PEER_DELAY: Duration = Duration::from_secs(2);
#[cfg(not(test))]
const TCP_RESOLVE_UFRAG_MAX: Duration = Duration::from_millis(2500);
#[cfg(test)]
pub(super) const TCP_RESOLVE_UFRAG_MAX: Duration = Duration::from_secs(1);
#[cfg(not(test))]
pub(super) const TCP_AUTH_MAX: Duration = Duration::from_secs(15);
#[cfg(test)]
pub(super) const TCP_AUTH_MAX: Duration = Duration::from_millis(1500);
// Both deadlines run from the accept: keep the ufrag one shorter, or which of the two
// closes a peer that never sent a ufrag is a race.

/// Why the acceptor closed each stream, by peer address. From the client side every closure
/// is an EOF, and the acceptor runs on its own thread, so tests read the reason from here.
#[cfg(test)]
static CLOSED_STREAMS: LazyLock<Mutex<HashMap<SocketAddr, TcpFailure>>> =
    LazyLock::new(Mutex::default);

/// Takes the reason the stream of `peer_addr` was closed, once the acceptor recorded it.
#[cfg(test)]
pub(super) fn take_closure_reason(peer_addr: SocketAddr) -> Option<TcpFailure> {
    CLOSED_STREAMS.lock().ok()?.remove(&peer_addr)
}

#[derive(Debug)]
pub struct TcpAcceptor {
    shutdown: CancellationToken,
}

impl Drop for TcpAcceptor {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

impl TcpAcceptor {
    pub fn spawn(
        listener: StdTcpListener,
        announced_addr: SocketAddr,
        ufrag_worker_map: UfragWorkerMap,
        workers: Arc<[RtcWorker]>,
    ) -> io::Result<Self> {
        let runtime = TokioRuntimeBuilder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()?;
        let tokio_listener = {
            let _guard = runtime.enter();
            TcpListener::from_std(listener)?
        };
        let shutdown = CancellationToken::new();
        let shutdown_clone = shutdown.clone();
        let builder = thread::Builder::new().name("rtc-tcp-acceptor".into());
        builder.spawn(move || {
            runtime.block_on(accept_loop(
                tokio_listener,
                announced_addr,
                shutdown_clone,
                ufrag_worker_map,
                workers,
            ));
        })?;
        Ok(Self { shutdown })
    }
}

async fn accept_loop(
    listener: TcpListener,
    announced_addr: SocketAddr,
    shutdown: CancellationToken,
    ufrag_worker_map: UfragWorkerMap,
    workers: Arc<[RtcWorker]>,
) {
    let mut failure_backoff = Duration::from_millis(1);
    loop {
        select! {
            () = shutdown.cancelled() => {
                break;
            }
            accepted = listener.accept() =>  {
                match accepted {
                    Ok((stream, peer)) => {
                        failure_backoff = Duration::from_millis(1);
                        let task_worker_map = ufrag_worker_map.clone();
                        let task_workers = Arc::clone(&workers);
                        let task_shutdown = shutdown.clone();
                        tokio::spawn(async move {
                            select! {
                                () = task_shutdown.cancelled() => {}
                                result = serve_tcp_stream(stream, peer, announced_addr, task_worker_map, task_workers) => {
                                    if let Err(error) = result {
                                        debug!(%peer, %error, "TCP stream closed");
                                        #[cfg(test)]
                                        if let Ok(mut closed) = CLOSED_STREAMS.lock() {
                                            closed.insert(peer, error);
                                        }
                                    }
                                }
                            }

                        });
                    }
                    Err(error) => {
                        // Single client failures, shouldn't put the accept loop to sleep.
                        if matches!(
                            error.kind(),
                            ErrorKind::ConnectionAborted | ErrorKind::ConnectionReset
                        ) {
                            continue;
                        }
                        warn!(?error, "failed to accept rtc TCP connection");
                        sleep(failure_backoff).await;
                        failure_backoff = (failure_backoff * 2).min(TCP_ACCEPT_BACKOFF_MAX);
                    }
                }
            }
        }
    }
}

/// Failure encountered while serving a TCP stream.
#[derive(Debug, Error)]
pub(super) enum TcpFailure {
    #[error("authentication didn't succeeded after {:?}", TCP_AUTH_MAX)]
    AuthTimeout,
    #[error(transparent)]
    FrameTooLarge(#[from] FrameTooLargeError),
    #[error("outbox was already closed by the worker")]
    OutboxClosed,
    #[error("packet loop is stopped")]
    PacketLoopGone,
    #[error("peer closed the stream")]
    PeerClosed,
    #[error("peer reset or aborted the connection: {0}")]
    PeerReset(io::Error),
    #[error("waiting for stream readiness failed: {0}")]
    ReadinessFailed(io::Error),
    #[error("read failed: {0}")]
    ReadFailed(io::Error),
    #[error("write failed: {0}")]
    WriteFailed(io::Error),
    #[error("no worker registered for ufrag {0:?}")]
    UnknownUfrag(String),
    #[error("no ufrag received within the deadline")]
    UfragTimeout,
    #[error("could not register to the worker")]
    WorkerRegistrationFailed,
    #[error("setting TCP_NODELAY failed: {0}")]
    NoDelayFailed(io::Error),
    #[error("peer didn't send any packets in the last {:?}", TCP_STALL_PEER_DELAY)]
    StallPeer,
}

/// Reads one chunk from `stream` into `decoder`.
///
/// # Errors
///
/// [`TcpFailure`] when the tcp stream closed, the socket couldn't be read or no packets
/// were received within [`TCP_STALL_PEER_DELAY`].
async fn read_into_decoder(
    reader: &ReadHalf<'_>,
    decoder: &mut FrameDecoder,
) -> Result<(), TcpFailure> {
    timeout(TCP_STALL_PEER_DELAY, reader.readable())
        .await
        .map_err(|_e| TcpFailure::StallPeer)?
        .map_err(TcpFailure::ReadinessFailed)?;
    match decoder.feed_with(|b| reader.try_read(b)) {
        Ok(0) => Err(TcpFailure::PeerClosed),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == WouldBlock => Ok(()),
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::ConnectionReset | ErrorKind::ConnectionAborted
            ) =>
        {
            Err(TcpFailure::PeerReset(error))
        }
        Err(error) => Err(TcpFailure::ReadFailed(error)),
    }
}

/// Finds the worker handling the peer from the STUN binding request, and returns it along
/// with the ufrag and the frame that carried it.
///
/// # Errors
///
/// Returns [`TcpFailure::UfragTimeout`] when the ufrag could not be retrieved within
/// [`TCP_RESOLVE_UFRAG_MAX`] deadline or [`TcpFailure::UnknownUfrag`] when the ufrag is
/// unknown or other variants of [`TcpFailure`] when an error occured while trying to read
/// the stream.
async fn find_worker<'w>(
    peer_addr: SocketAddr,
    reader: &ReadHalf<'_>,
    decoder: &mut FrameDecoder,
    ufrag_worker_map: &UfragWorkerMap,
    workers: &'w [RtcWorker],
) -> Result<(&'w RtcWorker, String, TcpFrame), TcpFailure> {
    timeout(TCP_RESOLVE_UFRAG_MAX, async {
        loop {
            while let Some(frame) = decoder.next().transpose()? {
                if let Some(PacketIndexProbe::LocalIceUfrag(ufrag)) =
                    packet_index_probe(peer_addr, frame.payload())
                {
                    let worker = ufrag_worker_map
                        .get(ufrag)
                        .and_then(|worker_id| workers.get(worker_id.as_usize()))
                        .ok_or_else(|| TcpFailure::UnknownUfrag(ufrag.to_owned()))?;

                    return Ok((worker, ufrag.to_owned(), frame));
                }
            }
            read_into_decoder(reader, decoder).await?;
        }
    })
    .await
    .map_err(|_elapsed| TcpFailure::UfragTimeout)?
}

/// Pushes a frame received from a TCP stream into its worker's ingress queue.
async fn push_ingress(
    frame: TcpFrame,
    tx: &mpsc::Sender<IngressPacket>,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    received_at: Instant,
) -> Result<(), TcpFailure> {
    let packet = IngressPacket {
        source_addr,
        candidate_addr,
        received_at,
        protocol: Protocol::Tcp,
        packet: frame.into_payload(),
    };
    tx.send(packet)
        .await
        .map_err(|_err| TcpFailure::PacketLoopGone)
}

/// Writes frames, pushed to the task outbox queue, to the TCP stream. The batch is sent
/// in one go, retrying partial send until every byte is sent.
async fn write_egress(writer: &mut WriteHalf<'_>, frames: &[TcpFrame]) -> Result<(), TcpFailure> {
    let mut to_send: Vec<io::IoSlice<'_>> = frames.iter().flat_map(|f| f.io_slices()).collect();
    let mut remaining = &mut to_send[..];
    while !remaining.is_empty() {
        let written = writer
            .write_vectored(remaining)
            .await
            .map_err(TcpFailure::WriteFailed)?;
        if written == 0 {
            return Err(TcpFailure::WriteFailed(io::Error::from(
                io::ErrorKind::WriteZero,
            )));
        }
        io::IoSlice::advance_slices(&mut remaining, written);
    }
    Ok(())
}

/// Receiver part of the TCP stream task. Consumes the stream and relays tcp frames to the
/// worker packet loop.
async fn tcp_ingress_loop(
    reader: ReadHalf<'_>,
    peer_addr: SocketAddr,
    announced_addr: SocketAddr,
    outbox_tx: mpsc::Sender<TcpFrame>,
    ufrag_worker_map: UfragWorkerMap,
    workers: Arc<[RtcWorker]>,
) -> Result<(), TcpFailure> {
    let mut decoder = FrameDecoder::new();
    let (worker, ufrag, frame) = find_worker(
        peer_addr,
        &reader,
        &mut decoder,
        &ufrag_worker_map,
        &workers,
    )
    .await?;
    let mut received_at = Instant::now();
    let tx = worker
        .register_tcp_stream(peer_addr, ufrag, outbox_tx)
        .await
        .map_err(|_err| TcpFailure::WorkerRegistrationFailed)?;
    debug!(%peer_addr, "TCP stream resolved to a worker");
    push_ingress(frame, &tx, peer_addr, announced_addr, received_at).await?;
    loop {
        while let Some(frame) = decoder.next().transpose()? {
            if frame.is_null() {
                continue;
            }
            push_ingress(frame, &tx, peer_addr, announced_addr, received_at).await?;
        }
        read_into_decoder(&reader, &mut decoder).await?;
        received_at = Instant::now();
    }
}

/// Drains the outbox and writes each batch to the TCP stream until the outbox closes or a
/// write fails.
async fn tcp_egress_loop(
    mut writer: WriteHalf<'_>,
    mut outbox_rx: mpsc::Receiver<TcpFrame>,
) -> Result<(), TcpFailure> {
    let mut frame_buffer: Vec<TcpFrame> = Vec::with_capacity(SESSION_DRAIN_MAX_TRANSMITS);
    let mut auth_wait = Some(TCP_AUTH_MAX);
    loop {
        let receive = outbox_rx.recv_many(&mut frame_buffer, SESSION_DRAIN_MAX_TRANSMITS);
        let added = match auth_wait.take() {
            Some(limit) => timeout(limit, receive)
                .await
                .map_err(|_e| TcpFailure::AuthTimeout)?,
            None => receive.await,
        };
        if added == 0 {
            return Err(TcpFailure::OutboxClosed);
        }
        write_egress(&mut writer, &frame_buffer).await?;
        frame_buffer.clear();
    }
}

/// Handle one incoming stream over TCP:
///   - Wait for the STUN binding request, read the given ufrag to register to the
///     appropriate worker.
///   - Relays packets to the worker's packet loop.
///   - Sends back packets pushed into the outbox queue.
///
/// # Errors
///
/// [`TcpFailure`] when an error occurred at any of those steps: connection closed or reset,
/// unreadable socket, ufrag not provided, unknown ufrag...
async fn serve_tcp_stream(
    mut stream: TcpStream,
    peer_addr: SocketAddr,
    announced_addr: SocketAddr,
    ufrag_worker_map: UfragWorkerMap,
    workers: Arc<[RtcWorker]>,
) -> Result<(), TcpFailure> {
    stream
        .set_nodelay(true)
        .map_err(TcpFailure::NoDelayFailed)?;
    let (reader, writer) = stream.split();
    let (outbox_tx, outbox_rx) = mpsc::channel(SESSION_DRAIN_MAX_TRANSMITS);
    let ingress_future = tcp_ingress_loop(
        reader,
        peer_addr,
        announced_addr,
        outbox_tx,
        ufrag_worker_map,
        workers,
    );
    let egress_future = tcp_egress_loop(writer, outbox_rx);
    select! {
        result = ingress_future => result,
        result = egress_future => result,
    }
}

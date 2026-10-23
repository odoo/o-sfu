//! One endpoint owner receives, schedules and forwards bounded UDP flows.

use std::{
    future, io,
    net::{Ipv4Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::Poll,
    time::Duration,
};

use rama_core::telemetry::tracing;
use tokio::{
    io::ReadBuf,
    net::UdpSocket,
    sync::{Notify, watch},
    task::JoinHandle,
    time::{Instant, sleep_until},
};

use super::{Config, Metrics, policy::Policy};
use crate::{Direction, Result};

const MAX_FLOWS: usize = 16;
const MAX_PENDING: usize = 128;
const MAX_PENDING_BYTES: usize = 1024 * 1024;
// Receive storage covers any IPv4 datagram without truncation. This allocation
// size is fixture policy, not the UDP protocol's maximum payload length.
const DATAGRAM_CAPACITY: usize = 65_536;

pub(super) struct Endpoint {
    address: SocketAddr,
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}

impl Endpoint {
    pub(super) async fn bind(
        connection_id: u64,
        target: SocketAddr,
        config: Arc<Config>,
        armed: Arc<AtomicBool>,
        metrics: Arc<Metrics>,
        abort: Arc<Notify>,
    ) -> Result<Self> {
        let listener = UdpSocket::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (stop, mut stopped) = watch::channel(false);
        let mut relay = Relay {
            listener,
            target,
            config,
            armed,
            budget: Arc::new(Budget {
                packets: AtomicUsize::new(0),
                bytes: AtomicUsize::new(0),
                metrics,
            }),
            flows: Vec::new(),
            pending: Vec::new(),
            sequence: 0,
        };
        let task = tokio::spawn(async move {
            if let Err(error) = relay.serve(&mut stopped).await {
                // Relay errors contain local limit descriptions or socket errors, never payloads.
                tracing::warn!(
                    connectionId = connection_id,
                    %error,
                    "UDP relay failed"
                );
                abort.notify_one();
            }
        });
        Ok(Self {
            address,
            stop,
            task: Some(task),
        })
    }

    pub(super) fn address(&self) -> SocketAddr {
        self.address
    }

    pub(super) fn stop(&self) {
        let _ = self.stop.send(true);
    }

    pub(super) async fn close(mut self) {
        self.stop();
        if let Some(task) = &mut self.task {
            let _ = task.await;
        }
        self.task.take();
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        // Normal shutdown joins the owner. Cancellation of
        // the enclosing WebSocket future must not detach a media relay.
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

struct Budget {
    packets: AtomicUsize,
    bytes: AtomicUsize,
    metrics: Arc<Metrics>,
}

impl Budget {
    fn reserve(self: &Arc<Self>, bytes: usize) -> Result<Reservation> {
        if self
            .packets
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |packets| {
                (packets < MAX_PENDING).then_some(packets + 1)
            })
            .is_err()
        {
            self.metrics.overflow.fetch_add(1, Ordering::Relaxed);
            return Err("UDP relay pending packet limit exceeded".into());
        }
        if self
            .bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current
                    .checked_add(bytes)
                    .filter(|total| *total <= MAX_PENDING_BYTES)
            })
            .is_err()
        {
            self.packets.fetch_sub(1, Ordering::Relaxed);
            self.metrics.overflow.fetch_add(1, Ordering::Relaxed);
            return Err("UDP relay pending byte limit exceeded".into());
        }
        self.metrics.pending.fetch_add(1, Ordering::Relaxed);
        Ok(Reservation {
            budget: Arc::clone(self),
            bytes,
        })
    }
}

struct Reservation {
    budget: Arc<Budget>,
    bytes: usize,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.bytes.fetch_sub(self.bytes, Ordering::Relaxed);
        self.budget.packets.fetch_sub(1, Ordering::Relaxed);
        self.budget.metrics.pending.fetch_sub(1, Ordering::Relaxed);
    }
}

struct Flow {
    browser: SocketAddr,
    upstream: Arc<UdpSocket>,
    policy: Policy,
    _gauge: FlowGauge,
}

struct FlowGauge(Arc<Metrics>);

impl Drop for FlowGauge {
    fn drop(&mut self) {
        self.0.active_flows.fetch_sub(1, Ordering::Relaxed);
    }
}

enum Destination {
    Upstream(Arc<UdpSocket>),
    Browser(SocketAddr),
}

struct Pending {
    deadline: Instant,
    sequence: u64,
    destination: Destination,
    data: Arc<[u8]>,
    _reservation: Reservation,
}

struct Relay {
    listener: UdpSocket,
    target: SocketAddr,
    config: Arc<Config>,
    armed: Arc<AtomicBool>,
    budget: Arc<Budget>,
    flows: Vec<Flow>,
    pending: Vec<Pending>,
    sequence: u64,
}

impl Relay {
    async fn serve(&mut self, stopped: &mut watch::Receiver<bool>) -> Result<()> {
        let mut browser_buffer = vec![0; DATAGRAM_CAPACITY];
        let mut upstream_buffer = vec![0; DATAGRAM_CAPACITY];
        let mut next_flow = 0;
        loop {
            if !self.deliver_due(stopped).await? {
                return Ok(());
            }
            let deadline = self.pending.iter().map(|packet| packet.deadline).min();
            // Receiving and forwarding share one owner so immediate traffic cannot
            // fill a separate ingress queue while this task is descheduled.
            tokio::select! {
                _ = stopped.changed() => return Ok(()),
                packet = self.listener.recv_from(&mut browser_buffer) => {
                    let (length, browser) = packet?;
                    let data = browser_buffer.get(..length).ok_or("invalid UDP length")?;
                    if !self.receive_browser(browser, data, stopped).await? {
                        return Ok(());
                    }
                }
                packet = receive_upstream(&self.flows, &mut next_flow, &mut upstream_buffer),
                    if !self.flows.is_empty() =>
                {
                    let (length, browser) = packet?;
                    self.budget.metrics.received.fetch_add(1, Ordering::Relaxed);
                    let data = upstream_buffer.get(..length).ok_or("invalid UDP length")?;
                    if !self
                        .schedule(browser, Direction::ServerToClient, data, stopped)
                        .await?
                    {
                        return Ok(());
                    }
                }
                () = async move {
                    if let Some(deadline) = deadline {
                        sleep_until(deadline).await;
                    } else {
                        future::pending::<()>().await;
                    }
                } => {}
            }
        }
    }

    async fn receive_browser(
        &mut self,
        browser: SocketAddr,
        data: &[u8],
        stopped: &mut watch::Receiver<bool>,
    ) -> Result<bool> {
        self.budget.metrics.received.fetch_add(1, Ordering::Relaxed);
        if browser.ip() != Ipv4Addr::LOCALHOST {
            return Err("UDP source must be 127.0.0.1".into());
        }
        self.ensure_flow(browser).await?;
        self.schedule(browser, Direction::ClientToServer, data, stopped)
            .await
    }

    async fn ensure_flow(&mut self, browser: SocketAddr) -> Result<()> {
        if self.flows.iter().any(|flow| flow.browser == browser) {
            return Ok(());
        }
        if self.flows.len() >= MAX_FLOWS {
            self.budget.metrics.overflow.fetch_add(1, Ordering::Relaxed);
            return Err("UDP relay browser flow limit exceeded".into());
        }
        // A distinct source tuple lets the SFU return opaque STUN, DTLS and
        // SRTP packets to their browser without inspecting session identities.
        let upstream = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
        upstream.connect(self.target).await?;
        self.budget
            .metrics
            .active_flows
            .fetch_add(1, Ordering::Relaxed);
        self.flows.push(Flow {
            browser,
            upstream,
            policy: Policy::new(&self.config),
            _gauge: FlowGauge(Arc::clone(&self.budget.metrics)),
        });
        Ok(())
    }

    async fn schedule(
        &mut self,
        browser: SocketAddr,
        direction: Direction,
        data: &[u8],
        stopped: &mut watch::Receiver<bool>,
    ) -> Result<bool> {
        if !self.deliver_due(stopped).await? {
            return Ok(false);
        }
        let flow = self
            .flows
            .iter_mut()
            .find(|flow| flow.browser == browser)
            .ok_or("unknown UDP browser flow")?;
        let delays = if self.armed.load(Ordering::Acquire) {
            let delivery = flow.policy.apply(direction, data);
            for index in delivery.matched {
                if let Some(count) = self.budget.metrics.rules.get(index) {
                    count.fetch_add(1, Ordering::Relaxed);
                }
            }
            delivery.delays
        } else {
            vec![Duration::ZERO]
        };
        if delays.is_empty() {
            self.budget.metrics.dropped.fetch_add(1, Ordering::Relaxed);
        }
        let duplicated = u64::try_from(delays.len().saturating_sub(1))?;
        self.budget
            .metrics
            .duplicated
            .fetch_add(duplicated, Ordering::Relaxed);
        let upstream = Arc::clone(&flow.upstream);
        let now = Instant::now();
        // Delayed deliveries outlive the receive buffer and share one retained copy.
        let mut delayed_data: Option<Arc<[u8]>> = None;
        for delay in delays {
            let destination = match direction {
                Direction::ClientToServer => Destination::Upstream(Arc::clone(&upstream)),
                Direction::ServerToClient => Destination::Browser(browser),
            };
            if delay.is_zero() {
                if !self.send(&destination, data, stopped).await? {
                    return Ok(false);
                }
                continue;
            }
            let reservation = self.budget.reserve(data.len())?;
            self.budget.metrics.delayed.fetch_add(1, Ordering::Relaxed);
            self.pending.push(Pending {
                deadline: now + delay,
                sequence: self.sequence,
                destination,
                data: Arc::clone(delayed_data.get_or_insert_with(|| Arc::from(data))),
                _reservation: reservation,
            });
            self.sequence = self.sequence.wrapping_add(1);
        }
        Ok(true)
    }

    async fn deliver_due(&mut self, stopped: &mut watch::Receiver<bool>) -> Result<bool> {
        while let Some((index, packet)) = self
            .pending
            .iter()
            .enumerate()
            .min_by_key(|(_, packet)| (packet.deadline, packet.sequence))
        {
            if packet.deadline > Instant::now() {
                break;
            }
            let packet = self.pending.swap_remove(index);
            if !self
                .send(&packet.destination, &packet.data, stopped)
                .await?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn send(
        &self,
        destination: &Destination,
        data: &[u8],
        stopped: &mut watch::Receiver<bool>,
    ) -> Result<bool> {
        let sent = tokio::select! {
            _ = stopped.changed() => return Ok(false),
            result = async {
                match destination {
                    Destination::Upstream(socket) => socket.send(data).await,
                    Destination::Browser(browser) => self.listener.send_to(data, browser).await,
                }
            } => result?,
        };
        if sent != data.len() {
            return Err("partial UDP datagram delivery".into());
        }
        self.budget
            .metrics
            .forwarded
            .fetch_add(1, Ordering::Relaxed);
        Ok(true)
    }
}

async fn receive_upstream(
    flows: &[Flow],
    next_flow: &mut usize,
    buffer: &mut [u8],
) -> io::Result<(usize, SocketAddr)> {
    future::poll_fn(|cx| {
        // One receive waker per socket belongs to this owner. Rotate the first
        // polled flow so a busy source cannot starve the other browser flows.
        for (index, flow) in flows
            .iter()
            .enumerate()
            .cycle()
            .skip(*next_flow)
            .take(flows.len())
        {
            let mut buffer = ReadBuf::new(buffer);
            if let Poll::Ready(result) = flow.upstream.poll_recv(cx, &mut buffer) {
                *next_flow = (index + 1) % flows.len();
                return Poll::Ready(result.map(|()| (buffer.filled().len(), flow.browser)));
            }
        }
        Poll::Pending
    })
    .await
}

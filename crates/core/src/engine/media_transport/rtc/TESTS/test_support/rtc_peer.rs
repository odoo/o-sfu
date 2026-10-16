//! A str0m peer driven by hand in tests.

use std::{net::SocketAddr, time::Instant};

use str0m::{
    Event, IceConnectionState, Input, Output, Rtc,
    net::{Protocol, Transmit},
};

/// Maximum outputs a fixture handles before it reports a stalled flow.
pub const MAX_FIXTURE_STEPS: usize = 10_000;

/// An `Rtc` with the state tests assert on: ICE state, DTLS connection, next
/// deadline and the routes used once ICE is connected.
pub struct Peer {
    pub rtc: Rtc,
    pub ice: IceConnectionState,
    pub connected: bool,
    pub deadline: Instant,
    pub nominated_routes: Vec<(Protocol, SocketAddr, SocketAddr)>,
}

impl Peer {
    pub fn new(rtc: Rtc, now: Instant) -> Self {
        Self {
            rtc,
            ice: IceConnectionState::New,
            connected: false,
            deadline: now,
            nominated_routes: Vec::new(),
        }
    }

    /// Polls output until the next future timeout and returns the packets to send.
    pub fn drain(&mut self, now: Instant) -> Vec<Transmit> {
        let mut packets = Vec::new();
        for _ in 0..MAX_FIXTURE_STEPS {
            match self.rtc.poll_output().expect("RTC output") {
                Output::Timeout(deadline) if deadline <= now => {
                    self.rtc
                        .handle_input(Input::Timeout(now))
                        .expect("immediate timeout");
                }
                Output::Timeout(deadline) => {
                    self.deadline = deadline;
                    return packets;
                }
                Output::Event(Event::IceConnectionStateChange(state)) => self.ice = state,
                Output::Event(Event::Connected) => self.connected = true,
                Output::Event(_) => {}
                Output::Transmit(packet) => {
                    if self.ice.is_connected() {
                        self.nominated_routes.push((
                            packet.proto,
                            packet.source,
                            packet.destination,
                        ));
                    }
                    packets.push(packet);
                }
            }
        }
        panic!(
            "RTC did not reach a future timeout, last ICE state: {:?}",
            self.ice
        );
    }
}

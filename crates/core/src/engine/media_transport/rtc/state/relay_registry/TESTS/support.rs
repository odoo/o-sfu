#[cfg(test)]
use {
    super::{ForwardedPacket, RELAY_MAILBOX_CAPACITY, RelayPacketMailbox},
    tokio::sync::mpsc,
};

#[cfg(test)]
impl RelayPacketMailbox {
    pub fn channel_for_test() -> (Self, mpsc::Receiver<ForwardedPacket>) {
        Self::channel_for_test_with_capacity(RELAY_MAILBOX_CAPACITY)
    }

    pub fn channel_for_test_with_capacity(
        capacity: usize,
    ) -> (Self, mpsc::Receiver<ForwardedPacket>) {
        let (tx, rx) = mpsc::channel(capacity);
        (Self::new(tx), rx)
    }
}

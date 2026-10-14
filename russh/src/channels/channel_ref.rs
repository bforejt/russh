use std::sync::Arc;

use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::error::TrySendError;

use super::{ChannelFlow, WindowSizeRef};
use crate::ChannelMsg;

/// A handle to the [`super::Channel`]'s to be able to transmit messages
/// to it and update it's `window_size`.
#[derive(Debug)]
pub struct ChannelRef {
    pub(super) sender: Sender<ChannelMsg>,
    pub(super) window_size: WindowSizeRef,
    pub(super) flow: Option<Arc<ChannelFlow>>,
}

impl ChannelRef {
    pub fn new(sender: Sender<ChannelMsg>) -> Self {
        Self {
            sender,
            window_size: WindowSizeRef::new(0),
            flow: None,
        }
    }

    pub(crate) fn with_flow(sender: Sender<ChannelMsg>, flow: Arc<ChannelFlow>) -> Self {
        Self {
            flow: Some(flow),
            ..Self::new(sender)
        }
    }

    pub(crate) fn window_size(&self) -> &WindowSizeRef {
        &self.window_size
    }

    pub(crate) fn flow(&self) -> Option<&Arc<ChannelFlow>> {
        self.flow.as_ref()
    }

    /// Flow-controlled and the reader still exists: credit the window on consumption.
    pub(crate) fn credits_on_consumption(&self) -> bool {
        self.flow.is_some() && !self.sender.is_closed()
    }

    /// Hand a message to the reader. With flow control this does not wait for the reader:
    /// data is bounded by the window and other messages by `channel_buffer_size`, so the queue
    /// is full only if the peer exceeds both, and then this waits as before.
    pub(crate) async fn deliver(&self, msg: ChannelMsg) {
        if self.flow.is_none() {
            let _ = self.sender.send(msg).await;
        } else if let Err(TrySendError::Full(msg)) = self.sender.try_send(msg) {
            let _ = self.sender.send(msg).await;
        }
    }

    /// Informational messages are dropped rather than queued beyond `channel_buffer_size`.
    pub(crate) fn deliver_info(&self, msg: ChannelMsg) {
        let queued = self.sender.max_capacity() - self.sender.capacity();
        if self.flow.as_ref().is_none_or(|f| queued < f.soft_cap) {
            let _ = self.sender.try_send(msg);
        }
    }
}

impl std::ops::Deref for ChannelRef {
    type Target = Sender<ChannelMsg>;

    fn deref(&self) -> &Self::Target {
        &self.sender
    }
}

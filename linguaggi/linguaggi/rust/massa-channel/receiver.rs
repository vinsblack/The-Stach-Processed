use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
    time::{Duration, Instant},
};

use crossbeam::channel::{Receiver, RecvError, RecvTimeoutError, TryRecvError};
use prometheus::{Counter, Gauge};
use tracing::trace;

#[derive(Clone)]
pub struct MassaReceiver<T> {
    pub(crate) receiver: Receiver<T>,
    #[allow(dead_code)]
    pub(crate) name: String,
    /// channel size
    pub(crate) actual_len: Gauge,
    /// total received messages
    pub(crate) received: Counter,
    /// reference counter to know how many receiver are cloned
    pub(crate) ref_counter: Arc<()>,
}

/// implement drop on MassaSender

impl<T> Drop for MassaReceiver<T> {
    fn drop(&mut self) {
        let ref_count = Arc::strong_count(&self.ref_counter);
        if ref_count == 1 {
            // this is the last ref so we can unregister metrics
            self.unregister_metrics();
        }
    }
}

impl<T> MassaReceiver<T> {
    /// increment manually the metrics
    /// Should be used when using the receiver with select! macro
    /// select! does not call recv()
    pub fn update_metrics(&self) {
        // use the len of the channel for actual_len instead of actual_len.dec()
        // because for each send we call recv more than one time
        self.actual_len.set(self.receiver.len() as f64);

        self.received.inc();
    }

    /// unregister metrics
    fn unregister_metrics(&self) {
        if let Err(e) = prometheus::unregister(Box::new(self.actual_len.clone())) {
            trace!(
                "promethetus error unregister actual_len for {} : {}",
                self.name,
                e
            );
        }

        if let Err(e) = prometheus::unregister(Box::new(self.received.clone())) {
            trace!(
                "promethetus error unregister received for {} : {}",
                self.name,
                e
            );
        }
    }

    /// attempt to receive a message from the channel
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        match self.receiver.try_recv() {
            Ok(msg) => {
                self.update_metrics();

                Ok(msg)
            }
            Err(crossbeam::channel::TryRecvError::Empty) => Err(TryRecvError::Empty),
            Err(crossbeam::channel::TryRecvError::Disconnected) => {
                self.unregister_metrics();

                Err(TryRecvError::Disconnected)
            }
        }
    }

    pub fn recv_deadline(&self, deadline: Instant) -> Result<T, RecvTimeoutError> {
        match self.receiver.recv_deadline(deadline) {
            Ok(msg) => {
                self.update_metrics();
                Ok(msg)
            }
            Err(e) => {
                self.unregister_metrics();
                Err(e)
            }
        }
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        match self.receiver.recv_timeout(timeout) {
            Ok(msg) => {
                self.update_metrics();
                Ok(msg)
            }
            Err(e) => {
                self.unregister_metrics();
                Err(e)
            }
        }
    }

    pub fn recv(&self) -> Result<T, RecvError> {
        match self.receiver.recv() {
            Ok(msg) => {
                self.update_metrics();
                Ok(msg)
            }
            Err(e) => {
                self.unregister_metrics();
                Err(e)
            }
        }
    }
}

impl<T> Deref for MassaReceiver<T> {
    type Target = Receiver<T>;

    fn deref(&self) -> &Self::Target {
        &self.receiver
    }
}

impl<T> DerefMut for MassaReceiver<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.receiver
    }
}

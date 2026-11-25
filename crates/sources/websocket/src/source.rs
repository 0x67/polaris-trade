//! The source, its frame type and the per-channel link state.

use std::{
    io,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use md_pipeline::{IngestError, IngestFrame, IngestSource, StreamId};
use tungstenite::{Error, Message};

use crate::{
    config::WebSocketConfig,
    connect::{Socket, native_roots, open},
    error::WebSocketError,
};

/// One WebSocket message: the payload bytes as the library read them, the
/// receive timestamp and the stream of the channel it came from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WebSocketFrame {
    payload: Bytes,
    ts_recv: u64,
    stream: StreamId,
}

impl WebSocketFrame {
    /// Take the payload without a copy, to keep it past the frame.
    #[inline]
    pub fn into_payload(self) -> Bytes {
        self.payload
    }
}

impl IngestFrame for WebSocketFrame {
    #[inline]
    fn payload(&self) -> &[u8] {
        &self.payload
    }
    #[inline]
    fn ts_recv(&self) -> u64 {
        self.ts_recv
    }
    #[inline]
    fn stream(&self) -> StreamId {
        self.stream
    }
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
}

enum Link {
    /// `failures` counts failed attempts in a row. A reconnect adds one and a
    /// delivered message clears it, so a connection that closes before
    /// sending anything keeps counting toward giving up.
    Up {
        ws: Box<Socket>,
        failures: u32,
    },
    Down {
        failures: u32,
        retry_at: Instant,
    },
}

/// Ingest source over one WebSocket connection per configured channel.
pub struct WebSocketSource {
    config: WebSocketConfig,
    links: Vec<Link>,
    first: usize,
    closed: bool,
}

impl WebSocketSource {
    /// Connect every channel and send its subscribe messages. Failing here
    /// does not retry: the caller sees the first error.
    ///
    /// # Errors
    ///
    /// [`WebSocketError::NoChannels`] for an empty channel list, or
    /// [`WebSocketError::Connect`] for the first channel that fails.
    pub fn connect(mut config: WebSocketConfig) -> Result<Self, WebSocketError> {
        if config.channels.is_empty() {
            return Err(WebSocketError::NoChannels);
        }
        if config.tls.is_none() && config.channels.iter().any(|c| c.url.starts_with("wss://")) {
            config.tls = Some(native_roots()?);
        }
        let mut links = Vec::with_capacity(config.channels.len());
        for channel in &config.channels {
            let ws = open(channel, &config).map_err(|source| WebSocketError::Connect {
                url: channel.url.clone(),
                source: Box::new(source),
            })?;
            tracing::info!(url = %channel.url, stream = channel.stream.0, "websocket channel connected");
            links.push(Link::Up {
                ws: Box::new(ws),
                failures: 0,
            });
        }
        Ok(WebSocketSource {
            config,
            links,
            first: 0,
            closed: false,
        })
    }

    fn lose(&mut self, i: usize, failures: u32, err: &Error) {
        tracing::warn!(url = %self.config.channels[i].url, %err, "websocket channel lost");
        self.links[i] = Link::Down {
            failures,
            retry_at: Instant::now() + self.backoff(failures),
        };
    }

    /// One reconnect step for a down channel: attempt when due, or give up
    /// when the failure budget is spent. Never sleeps.
    fn retry(&mut self, i: usize) {
        let Link::Down { failures, retry_at } = self.links[i] else {
            return;
        };
        let url = &self.config.channels[i].url;
        if failures >= self.config.reconnect_attempts {
            tracing::error!(%url, failures, "websocket channel gave up");
            self.closed = true;
            return;
        }
        let now = Instant::now();
        if now < retry_at {
            return;
        }
        match open(&self.config.channels[i], &self.config) {
            Ok(ws) => {
                tracing::info!(%url, "websocket channel reconnected");
                self.links[i] = Link::Up {
                    ws: Box::new(ws),
                    failures: failures + 1,
                };
            }
            Err(err) => {
                let failures = failures + 1;
                tracing::warn!(%url, %err, failures, "websocket reconnect failed");
                self.links[i] = Link::Down {
                    failures,
                    retry_at: now + self.backoff(failures),
                };
                if failures >= self.config.reconnect_attempts {
                    tracing::error!(%url, failures, "websocket channel gave up");
                    self.closed = true;
                }
            }
        }
    }

    fn backoff(&self, failures: u32) -> Duration {
        self.config
            .backoff_initial
            .saturating_mul(1 << failures.min(16))
            .min(self.config.backoff_max)
    }
}

/// Read up to `budget` data messages. `Err` means the connection is gone.
fn drain(
    ws: &mut Socket,
    stream: StreamId,
    out: &mut Vec<WebSocketFrame>,
    budget: usize,
) -> (usize, Result<(), Error>) {
    let mut n = 0;
    let mut replies = false;
    let end = loop {
        if n == budget {
            break Ok(());
        }
        let payload = match ws.read() {
            Ok(Message::Text(text)) => Bytes::from(text),
            Ok(Message::Binary(bytes)) => bytes,
            Ok(Message::Ping(_)) => {
                replies = true;
                continue;
            }
            Ok(Message::Pong(_) | Message::Frame(_)) => continue,
            Ok(Message::Close(_)) => {
                replies = true;
                break Err(Error::ConnectionClosed);
            }
            Err(Error::Io(err)) if err.kind() == io::ErrorKind::WouldBlock => break Ok(()),
            Err(err) => break Err(err),
        };
        out.push(WebSocketFrame {
            payload,
            ts_recv: now_ns(),
            stream,
        });
        n += 1;
    };
    if replies {
        // pong or close reply queued by the library; a failed flush shows
        // again on the next read
        let _ = ws.flush();
    }
    (n, end)
}

impl IngestSource for WebSocketSource {
    type Frame = WebSocketFrame;

    // Live channels are polled before lost ones retry. A due retry blocks for
    // up to `connect_timeout`, which delays this call's frames and the next
    // poll of the live channels. Once a channel gives up, the call that finds
    // no frames reports `Closed`.
    fn recv_batch(&mut self, out: &mut Vec<Self::Frame>, max: usize) -> Result<usize, IngestError> {
        if self.closed {
            return Err(IngestError::Closed);
        }
        let channels = self.links.len();
        let mut n = 0;
        for step in 0..channels {
            let i = (self.first + step) % channels;
            if let Link::Up { ws, failures } = &mut self.links[i] {
                let stream = self.config.channels[i].stream;
                let (read, end) = drain(ws, stream, out, max - n);
                n += read;
                if read > 0 {
                    *failures = 0;
                }
                if let Err(err) = end {
                    let failures = *failures;
                    self.lose(i, failures, &err);
                }
            }
        }
        for i in 0..channels {
            self.retry(i);
        }
        self.first = (self.first + 1) % channels;
        if self.closed && n == 0 {
            return Err(IngestError::Closed);
        }
        Ok(n)
    }
}

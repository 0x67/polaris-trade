//! Blocking connect, TLS and upgrade handshake, subscribe, then non-blocking.

use std::{
    io,
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use tungstenite::{
    Connector, Error, Message, WebSocket,
    client::{IntoClientRequest, uri_mode},
    client_tls_with_config,
    handshake::HandshakeError,
    protocol::WebSocketConfig as WsConfig,
    stream::{MaybeTlsStream, Mode},
};

use crate::{
    config::{ChannelConfig, WebSocketConfig},
    error::WebSocketError,
};

pub(crate) type Socket = WebSocket<MaybeTlsStream<TcpStream>>;

/// Pause between handshake steps while the peer has not answered yet.
const HANDSHAKE_POLL: Duration = Duration::from_millis(1);

/// Client TLS config over the platform root store, built once per source.
pub(crate) fn native_roots() -> Result<Arc<rustls::ClientConfig>, WebSocketError> {
    let loaded = rustls_native_certs::load_native_certs();
    if !loaded.errors.is_empty() {
        tracing::warn!(errors = ?loaded.errors, "platform root certificate loading errors");
    }
    if loaded.certs.is_empty() {
        return Err(WebSocketError::NativeRoots(io::Error::new(
            io::ErrorKind::NotFound,
            "no platform root certificates found",
        )));
    }
    let mut roots = rustls::RootCertStore::empty();
    roots.add_parsable_certificates(loaded.certs);
    Ok(Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ))
}

/// Connect one channel and subscribe within `cfg.connect_timeout` in total.
/// The returned socket is non-blocking.
pub(crate) fn open(channel: &ChannelConfig, cfg: &WebSocketConfig) -> Result<Socket, Error> {
    let deadline = Instant::now() + cfg.connect_timeout;
    let request = channel.url.as_str().into_client_request()?;
    let port = match (request.uri().port_u16(), uri_mode(request.uri())?) {
        (Some(port), _) => port,
        (None, Mode::Plain) => 80,
        (None, Mode::Tls) => 443,
    };
    let host = request.uri().host().unwrap_or_default();
    let host = host.trim_start_matches('[').trim_end_matches(']');
    let tcp = connect_any((host, port), deadline)?;
    tcp.set_nodelay(true)?;
    tcp.set_nonblocking(true)?;

    let ws_cfg = WsConfig::default()
        .max_message_size(Some(cfg.max_message_size))
        .max_frame_size(Some(cfg.max_frame_size));
    let connector = cfg.tls.clone().map(Connector::Rustls);
    let mut step = client_tls_with_config(request, tcp, Some(ws_cfg), connector);
    let mut ws = loop {
        match step {
            Ok((ws, _response)) => break ws,
            Err(HandshakeError::Failure(err)) => return Err(err),
            Err(HandshakeError::Interrupted(mid)) => {
                if Instant::now() >= deadline {
                    return Err(io::Error::from(io::ErrorKind::TimedOut).into());
                }
                thread::sleep(HANDSHAKE_POLL);
                step = mid.handshake();
            }
        }
    };

    let tcp = tcp_of(&mut ws)?;
    tcp.set_nonblocking(false)?;
    tcp.set_write_timeout(Some(remaining(deadline)?))?;
    for message in &channel.subscribe {
        ws.send(Message::text(message.as_str()))?;
    }

    let tcp = tcp_of(&mut ws)?;
    tcp.set_write_timeout(None)?;
    tcp.set_nonblocking(true)?;
    Ok(ws)
}

fn tcp_of(ws: &mut Socket) -> io::Result<&mut TcpStream> {
    match ws.get_mut() {
        MaybeTlsStream::Plain(tcp) => Ok(tcp),
        MaybeTlsStream::Rustls(tls) => Ok(&mut tls.sock),
        _ => Err(io::Error::other("unsupported stream type")),
    }
}

fn remaining(deadline: Instant) -> io::Result<Duration> {
    let left = deadline.saturating_duration_since(Instant::now());
    if left.is_zero() {
        return Err(io::ErrorKind::TimedOut.into());
    }
    Ok(left)
}

/// Try each resolved address in turn, giving each an equal share of the time
/// left before `deadline`, so one blackholed address cannot use all of it.
fn connect_any(addr: (&str, u16), deadline: Instant) -> io::Result<TcpStream> {
    let candidates: Vec<SocketAddr> = addr.to_socket_addrs()?.collect();
    let mut last = io::Error::from(io::ErrorKind::AddrNotAvailable);
    for (done, candidate) in candidates.iter().enumerate() {
        let share = remaining(deadline)? / u32::try_from(candidates.len() - done).unwrap_or(1);
        match TcpStream::connect_timeout(candidate, share) {
            Ok(tcp) => return Ok(tcp),
            Err(err) => last = err,
        }
    }
    Err(last)
}

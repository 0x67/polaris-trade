//! Local scripted WebSocket server for the source tests. Accepts a fixed
//! number of connections on `127.0.0.1:0`, plain or over TLS with a
//! self-signed certificate, and runs one script per connection on its own thread.

use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::Arc,
    thread::{self, JoinHandle},
};

use md_pipeline::StreamId;
use source_websocket::{ChannelConfig, WebSocketConfig};
use tungstenite::{Message, WebSocket};

trait Stream: Read + Write + Send {}
impl<T: Read + Write + Send> Stream for T {}

/// Server side of one accepted connection.
pub struct Conn(WebSocket<Box<dyn Stream>>);

impl Conn {
    pub fn read_text(&mut self) -> String {
        match self.0.read().expect("server read") {
            Message::Text(text) => text.to_string(),
            other => panic!("expected text, got {other:?}"),
        }
    }

    pub fn send_text(&mut self, text: &str) {
        self.0.send(Message::text(text)).expect("server send");
    }

    pub fn send_binary(&mut self, bytes: &[u8]) {
        self.0
            .send(Message::binary(bytes.to_vec()))
            .expect("server send");
    }

    /// Write bytes below the WebSocket layer, to split or shape frames.
    pub fn write_raw(&mut self, bytes: &[u8]) {
        let stream = self.0.get_mut();
        stream.write_all(bytes).expect("raw write");
        stream.flush().expect("raw flush");
    }

    /// Send a close frame and read until the client's reply ends the
    /// connection. True when the close handshake completed.
    pub fn close_and_wait(&mut self) -> bool {
        self.0.close(None).expect("server close");
        loop {
            match self.0.read() {
                Ok(_) => {}
                Err(tungstenite::Error::ConnectionClosed) => return true,
                Err(_) => return false,
            }
        }
    }

    /// Read until the client goes away.
    pub fn hold(&mut self) {
        while self.0.read().is_ok() {}
    }
}

/// Unmasked server text frame with a payload under 126 bytes.
pub fn text_frame(payload: &[u8]) -> Vec<u8> {
    let len = u8::try_from(payload.len()).expect("short payload");
    assert!(len < 126);
    let mut frame = vec![0x81, len];
    frame.extend_from_slice(payload);
    frame
}

pub struct Server<T> {
    url: String,
    tls: Option<Arc<rustls::ClientConfig>>,
    handle: JoinHandle<Vec<T>>,
}

impl<T: Send + 'static> Server<T> {
    /// Accept `conns` connections in turn and run `script(index, conn)` for
    /// each on its own thread. The listener closes right after the last
    /// accept, so a client that reconnects after it is refused.
    pub fn start(
        tls: bool,
        conns: usize,
        script: impl Fn(usize, &mut Conn) -> T + Send + Sync + 'static,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
            .expect("self-signed cert");
        let cert_der = cert.cert.der().clone();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert_der.clone()).expect("trust cert");
        let client = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let server_cfg = Arc::new(
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(
                    vec![cert_der],
                    rustls::pki_types::PrivateKeyDer::try_from(cert.signing_key.serialize_der())
                        .expect("key"),
                )
                .expect("server tls config"),
        );
        let handle = thread::spawn(move || {
            let script = Arc::new(script);
            let mut listener = Some(listener);
            let mut scripts = Vec::new();
            for index in 0..conns {
                let (tcp, _) = listener
                    .as_ref()
                    .expect("listener")
                    .accept()
                    .expect("accept");
                if index + 1 == conns {
                    listener = None;
                }
                let stream: Box<dyn Stream> = if tls {
                    let conn = rustls::ServerConnection::new(server_cfg.clone()).expect("tls conn");
                    Box::new(rustls::StreamOwned::new(conn, tcp))
                } else {
                    Box::new(tcp)
                };
                let script = script.clone();
                scripts.push(thread::spawn(move || {
                    let mut conn = Conn(tungstenite::accept(stream).expect("server upgrade"));
                    script(index, &mut conn)
                }));
            }
            scripts
                .into_iter()
                .map(|script| script.join().expect("script thread"))
                .collect()
        });
        let (url, tls) = if tls {
            (format!("wss://localhost:{port}/"), Some(Arc::new(client)))
        } else {
            (format!("ws://127.0.0.1:{port}/"), None)
        };
        Server { url, tls, handle }
    }

    /// One channel on stream 0 that gives up on the first lost connection.
    pub fn config(&self, subscribe: &[&str]) -> WebSocketConfig {
        let mut config = WebSocketConfig::new(vec![self.channel(StreamId(0), subscribe)]);
        config.tls.clone_from(&self.tls);
        config.reconnect_attempts = 0;
        config
    }

    pub fn channel(&self, stream: StreamId, subscribe: &[&str]) -> ChannelConfig {
        ChannelConfig {
            url: self.url.clone(),
            stream,
            subscribe: subscribe.iter().map(ToString::to_string).collect(),
        }
    }

    /// Results of every script, once the last one returned.
    pub fn join(self) -> Vec<T> {
        self.handle.join().expect("server thread")
    }
}

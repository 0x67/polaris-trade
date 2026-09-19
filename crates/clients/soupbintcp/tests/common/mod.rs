//! Local mock `SoupBinTCP` server and protocol case table shared by tests.
//!
//! Server runs one scripted connection on std thread with blocking socket, so
//! same script serves sync client (no runtime) and async client. Server writes
//! are zlib-framed under `compressed` (one `Compress` state per connection,
//! matching client's persistent inflate); client writes always arrive plain.

use std::{
    io::{Read, Write},
    net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream},
    num::NonZeroU32,
    thread::{self, JoinHandle},
    time::Duration,
};

use client_soupbintcp::{SoupBinClientConfig, SoupBinEvent, SoupBinMessage};
#[cfg(feature = "compressed")]
use flate2::{Compress, Compression, FlushCompress};

/// One server action.
pub enum Srv {
    /// Read login request and assert it equals [`login_request`].
    Login,
    /// Write packet bytes (zlib-framed under `compressed`).
    Write(Vec<u8>),
    /// Read exactly these bytes from client.
    Expect(Vec<u8>),
    /// Stay silent.
    Pause(Duration),
    /// Close connection now, before client does.
    Close,
}

/// Scripted server bound to loopback.
pub struct Server {
    /// Address client connects to.
    pub addr: SocketAddr,
    handle: JoinHandle<()>,
}

impl Server {
    /// Accept one connection and run `script` on it. Unless script closes,
    /// server then waits for client to close, so it never resets connection
    /// with unread bytes.
    ///
    /// # Panics
    ///
    /// When bind fails.
    pub fn spawn(script: Vec<Srv>) -> Self {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        let handle = thread::spawn(move || {
            let (sock, _) = listener.accept().expect("accept");
            sock.set_read_timeout(Some(Duration::from_secs(5)))
                .expect("read timeout");
            run(MockConn::new(sock), script);
        });
        Self { addr, handle }
    }

    /// Wait for script to finish.
    ///
    /// # Panics
    ///
    /// When script assertion failed, naming `case`.
    pub fn join(self, case: &str) {
        assert!(self.handle.join().is_ok(), "{case}: server script failed");
    }
}

struct MockConn {
    sock: TcpStream,
    #[cfg(feature = "compressed")]
    encoder: Compress,
}

impl MockConn {
    fn new(sock: TcpStream) -> Self {
        Self {
            sock,
            #[cfg(feature = "compressed")]
            encoder: Compress::new(Compression::default(), true),
        }
    }

    fn write(&mut self, plain: &[u8]) {
        #[cfg(feature = "compressed")]
        {
            // compress_vec writes only into reserved capacity, never grows vec
            let mut out = Vec::with_capacity(plain.len() + 128);
            self.encoder
                .compress_vec(plain, &mut out, FlushCompress::Sync)
                .expect("zlib compress");
            self.sock.write_all(&out).expect("write compressed");
        }
        #[cfg(not(feature = "compressed"))]
        self.sock.write_all(plain).expect("write plain");
    }

    fn expect(&mut self, want: &[u8]) {
        let mut got = vec![0u8; want.len()];
        self.sock.read_exact(&mut got).expect("read client bytes");
        assert!(
            got == want,
            "client sent {} bytes differing from expected",
            want.len()
        );
    }

    // client closing ends script; reset or timeout also count as done
    fn await_close(&mut self) {
        let mut buf = [0u8; 4096];
        loop {
            match self.sock.read(&mut buf) {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
        }
    }
}

fn run(mut conn: MockConn, script: Vec<Srv>) {
    for op in script {
        match op {
            Srv::Login => conn.expect(&login_request()),
            Srv::Write(bytes) => conn.write(&bytes),
            Srv::Expect(bytes) => conn.expect(&bytes),
            Srv::Pause(d) => thread::sleep(d),
            Srv::Close => return,
        }
    }
    conn.await_close();
}

/// One logical packet: `Length[2 BE] + Type[1] + payload`.
///
/// # Panics
///
/// When payload does not fit `u16` length.
pub fn packet(ty: u8, payload: &[u8]) -> Vec<u8> {
    let len = u16::try_from(1 + payload.len()).expect("packet fits u16 length");
    let mut out = len.to_be_bytes().to_vec();
    out.push(ty);
    out.extend_from_slice(payload);
    out
}

/// `Login Accepted`: session left-justified in 10, sequence right-justified in 20.
pub fn login_accepted(session: &str, sequence: u64) -> Vec<u8> {
    packet(b'A', format!("{session:<10}{sequence:>20}").as_bytes())
}

/// Login request [`config`] produces.
pub fn login_request() -> Vec<u8> {
    packet(
        b'L',
        format!(
            "{:<6}{:<10}{:<10}{:>20}",
            "user01", "pass12345", "sess001", 1
        )
        .as_bytes(),
    )
}

/// Config every case starts from: quiet timers, small buffers.
pub fn config() -> SoupBinClientConfig {
    SoupBinClientConfig {
        username: "user01".into(),
        password: "pass12345".into(),
        requested_session: "sess001".into(),
        requested_sequence_number: 1,
        login_timeout: Duration::from_secs(2),
        heartbeat_interval: Duration::from_secs(10),
        heartbeat_timeout: Duration::from_secs(10),
        max_frame_size: 64 * 1024,
        decode_buf_capacity: 4096,
    }
}

/// Message client yielded, owned so drivers compare it after borrow ends.
#[derive(Debug, PartialEq, Eq)]
pub enum Got {
    /// Sequenced data: bytes and sequence.
    Data(Vec<u8>, u64),
    /// Lifecycle event.
    Event(SoupBinEvent),
}

impl From<SoupBinMessage<'_>> for Got {
    fn from(msg: SoupBinMessage<'_>) -> Self {
        match msg {
            SoupBinMessage::Data(frame) => Self::Data(frame.as_ref().to_vec(), frame.sequence()),
            SoupBinMessage::Event(event) => Self::Event(event),
        }
    }
}

/// Login answer each driver expects.
pub enum LoginWant {
    /// Accepted with this session and next sequence.
    Accepted {
        /// Session id, unpadded.
        session: &'static str,
        /// Next sequence.
        sequence: u64,
    },
    /// Rejected with this reason code.
    Rejected(u8),
    /// No answer within `login_timeout`.
    TimedOut,
}

/// One client step.
pub enum Step {
    /// First step: login outcome.
    Login(LoginWant),
    /// Next message client yields.
    Next(Got),
    /// Send unsequenced payload.
    Send(Vec<u8>),
    /// Log out.
    Logout,
    /// Next call fails with end of session.
    Closed,
    /// Next call fails with peer closed.
    PeerClosed,
}

/// One protocol case run through both APIs.
pub struct Case {
    /// Case name for failures.
    pub name: &'static str,
    /// Client config.
    pub cfg: SoupBinClientConfig,
    /// Client `SO_SNDBUF`, to force partial writes.
    pub send_buf: Option<NonZeroU32>,
    /// Server script.
    pub server: Vec<Srv>,
    /// Client steps, in order.
    pub client: Vec<Step>,
}

fn case(name: &'static str, server: Vec<Srv>, client: Vec<Step>) -> Case {
    Case {
        name,
        cfg: config(),
        send_buf: None,
        server,
        client,
    }
}

const ACCEPTED: Step = Step::Login(LoginWant::Accepted {
    session: "sess001",
    sequence: 1,
});

/// Protocol table: every case runs through sync `poll` and async API.
#[expect(clippy::too_many_lines, reason = "one literal row per protocol case")]
pub fn cases() -> Vec<Case> {
    let big: Vec<Vec<u8>> = (0..12u8).map(|i| vec![i; 60_000]).collect();
    let big_wire: Vec<u8> = big.iter().flat_map(|p| packet(b'U', p)).collect();
    vec![
        case(
            "login accepted after early server heartbeat",
            vec![
                Srv::Login,
                Srv::Write(packet(b'H', &[])),
                Srv::Write(login_accepted("sess001", 5)),
            ],
            vec![Step::Login(LoginWant::Accepted {
                session: "sess001",
                sequence: 5,
            })],
        ),
        case(
            "login rejected",
            vec![Srv::Login, Srv::Write(packet(b'J', b"A"))],
            vec![Step::Login(LoginWant::Rejected(b'A')), Step::Closed],
        ),
        Case {
            cfg: SoupBinClientConfig {
                login_timeout: Duration::from_millis(100),
                ..config()
            },
            ..case(
                "login timeout",
                vec![Srv::Login, Srv::Pause(Duration::from_millis(400))],
                vec![Step::Login(LoginWant::TimedOut)],
            )
        },
        case(
            "sequenced messages in order, debug dropped",
            vec![
                Srv::Login,
                Srv::Write(login_accepted("sess001", 7)),
                Srv::Write(packet(b'S', b"a")),
                Srv::Write(packet(b'+', b"debug")),
                Srv::Write(packet(b'S', b"b")),
                Srv::Write(packet(b'S', b"c")),
            ],
            vec![
                Step::Login(LoginWant::Accepted {
                    session: "sess001",
                    sequence: 7,
                }),
                Step::Next(Got::Data(b"a".to_vec(), 7)),
                Step::Next(Got::Data(b"b".to_vec(), 8)),
                Step::Next(Got::Data(b"c".to_vec(), 9)),
            ],
        ),
        Case {
            cfg: SoupBinClientConfig {
                heartbeat_interval: Duration::from_millis(100),
                ..config()
            },
            ..case(
                "idle heartbeat both ways",
                vec![
                    Srv::Login,
                    Srv::Write(login_accepted("sess001", 1)),
                    Srv::Write(packet(b'H', &[])),
                    Srv::Expect(packet(b'R', &[])),
                ],
                vec![
                    ACCEPTED,
                    Step::Next(Got::Event(SoupBinEvent::HeartbeatReceived)),
                    Step::Next(Got::Event(SoupBinEvent::HeartbeatSent)),
                ],
            )
        },
        Case {
            cfg: SoupBinClientConfig {
                heartbeat_timeout: Duration::from_millis(100),
                ..config()
            },
            ..case(
                "heartbeat timeout",
                vec![
                    Srv::Login,
                    Srv::Write(login_accepted("sess001", 1)),
                    Srv::Pause(Duration::from_millis(400)),
                ],
                vec![
                    ACCEPTED,
                    Step::Next(Got::Event(SoupBinEvent::HeartbeatTimeout)),
                    Step::Closed,
                ],
            )
        },
        Case {
            send_buf: NonZeroU32::new(4096),
            ..case(
                "partial writes resumed byte-exact",
                vec![
                    Srv::Login,
                    Srv::Write(login_accepted("sess001", 1)),
                    Srv::Pause(Duration::from_millis(200)),
                    Srv::Expect(big_wire),
                    Srv::Write(packet(b'S', b"done")),
                ],
                [ACCEPTED]
                    .into_iter()
                    .chain(big.into_iter().map(Step::Send))
                    .chain([Step::Next(Got::Data(b"done".to_vec(), 1))])
                    .collect(),
            )
        },
        case(
            "logout",
            vec![
                Srv::Login,
                Srv::Write(login_accepted("sess001", 1)),
                Srv::Expect(packet(b'O', &[])),
            ],
            vec![ACCEPTED, Step::Logout, Step::Closed],
        ),
        case(
            "end of session",
            vec![
                Srv::Login,
                Srv::Write(login_accepted("sess001", 1)),
                Srv::Write(packet(b'S', b"last")),
                Srv::Write(packet(b'Z', &[])),
            ],
            vec![
                ACCEPTED,
                Step::Next(Got::Data(b"last".to_vec(), 1)),
                Step::Next(Got::Event(SoupBinEvent::EndOfSession)),
                Step::Closed,
            ],
        ),
        case(
            "peer close",
            vec![
                Srv::Login,
                Srv::Write(login_accepted("sess001", 1)),
                Srv::Close,
            ],
            vec![ACCEPTED, Step::PeerClosed],
        ),
    ]
}

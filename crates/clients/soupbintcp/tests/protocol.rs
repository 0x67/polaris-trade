//! One protocol table (`common::cases`) run through both APIs against local
//! mock server: sync `start`/`poll` over `MioTcp` parked on `ReadySet`, and
//! async `connect`/`recv` over tokio `TcpStream`. Every step asserts message
//! client yields, so both drivers stay on one state machine.

pub mod common;

use std::{
    net::SocketAddr,
    num::NonZeroUsize,
    time::{Duration, Instant},
};

use client_soupbintcp::{SoupBinClient, SoupBinError, SoupBinEvent};
use common::{Case, Got, LoginWant, Server, Step};
use transport_core::TransportError;
use transport_socket::{
    TcpConfig,
    mio::{MioTcp, ReadySet, ReadyToken},
    tokio::TcpStream,
};

/// Bound on any one step.
const STEP_LIMIT: Duration = Duration::from_secs(5);

fn tcp_config(case: &Case, addr: SocketAddr) -> TcpConfig {
    let mut cfg = TcpConfig::new(addr);
    cfg.send_buf = case.send_buf;
    cfg
}

fn padded(session: &str) -> [u8; 10] {
    format!("{session:<10}")
        .as_bytes()
        .try_into()
        .expect("10-byte session")
}

/// Parked sync loop: `poll`, and on `None` wait for readiness or next deadline.
struct SyncDriver {
    name: &'static str,
    client: SoupBinClient<MioTcp>,
    set: ReadySet,
    ready: Vec<transport_socket::mio::Ready>,
}

impl SyncDriver {
    fn next(&mut self) -> Result<Got, SoupBinError> {
        let limit = Instant::now() + STEP_LIMIT;
        loop {
            if let Some(msg) = self.client.poll(Instant::now())? {
                return Ok(msg.into());
            }
            let now = Instant::now();
            assert!(
                now < limit,
                "{}: no message within {STEP_LIMIT:?}",
                self.name
            );
            let until = self.client.next_deadline().min(limit);
            self.set
                .wait(Some(until.saturating_duration_since(now)), &mut self.ready)
                .expect("wait");
        }
    }
}

fn run_sync(case: &Case, addr: SocketAddr) {
    let name = case.name;
    let mut tcp = MioTcp::connect(&tcp_config(case, addr)).expect("connect");
    let mut set = ReadySet::new(NonZeroUsize::MIN).expect("ready set");
    set.register(&mut tcp, ReadyToken(0)).expect("register");
    let client = SoupBinClient::start(tcp, case.cfg.clone()).expect("start");
    let mut d = SyncDriver {
        name,
        client,
        set,
        ready: Vec::new(),
    };
    for step in &case.client {
        match step {
            Step::Login(LoginWant::Accepted { session, sequence }) => {
                let want = Got::Event(SoupBinEvent::LoginAccepted {
                    session: padded(session),
                    sequence: *sequence,
                });
                assert_eq!(d.next().expect(name), want, "{name}");
                assert_eq!(d.client.session(), *session, "{name}");
                assert_eq!(d.client.next_expected_sequence(), *sequence, "{name}");
            }
            Step::Login(LoginWant::Rejected(reason)) => {
                let want = Got::Event(SoupBinEvent::LoginRejected { reason: *reason });
                assert_eq!(d.next().expect(name), want, "{name}");
            }
            Step::Login(LoginWant::TimedOut) => {
                let got = d.next();
                assert!(
                    matches!(got, Err(SoupBinError::LoginTimeout { .. })),
                    "{name}: {got:?}"
                );
            }
            Step::Next(want) => assert_eq!(&d.next().expect(name), want, "{name}"),
            Step::Send(payload) => d.client.queue_unsequenced(payload).expect(name),
            Step::Logout => d.client.queue_logout().expect(name),
            Step::Closed => {
                let got = d.next();
                assert!(
                    matches!(got, Err(SoupBinError::EndOfSession)),
                    "{name}: {got:?}"
                );
            }
            Step::PeerClosed => {
                let got = d.next();
                assert!(
                    matches!(
                        got,
                        Err(SoupBinError::Transport(TransportError::PeerClosed))
                    ),
                    "{name}: {got:?}"
                );
            }
        }
    }
}

/// Async loop: `recv` until next deadline, then `tick_heartbeat`.
async fn next_async(
    name: &str,
    client: &mut SoupBinClient<TcpStream>,
) -> Result<Got, SoupBinError> {
    let limit = Instant::now() + STEP_LIMIT;
    loop {
        let until = client.next_deadline().min(limit);
        if let Ok(result) = tokio::time::timeout_at(until.into(), client.recv()).await {
            return result.map(Got::from);
        }
        if let Some(event) = client.tick_heartbeat().await? {
            return Ok(Got::Event(event));
        }
        assert!(
            Instant::now() < limit,
            "{name}: no message within {STEP_LIMIT:?}"
        );
    }
}

async fn run_async(case: &Case, addr: SocketAddr) {
    let name = case.name;
    let tcp = TcpStream::connect(&tcp_config(case, addr))
        .await
        .expect("connect");
    let (Step::Login(login), rest) = case.client.split_first().expect("login step") else {
        panic!("{name}: first step must be login");
    };
    let connected = SoupBinClient::connect(tcp, case.cfg.clone()).await;
    let mut client = match (login, connected) {
        (LoginWant::Accepted { session, sequence }, Ok(client)) => {
            assert_eq!(client.session(), *session, "{name}");
            assert_eq!(client.next_expected_sequence(), *sequence, "{name}");
            client
        }
        // connect owns login; no client exists for later steps
        (LoginWant::Rejected(reason), Err(SoupBinError::LoginRejected { code })) => {
            assert_eq!(code, char::from(*reason).to_string(), "{name}");
            return;
        }
        (LoginWant::TimedOut, Err(SoupBinError::LoginTimeout { .. })) => return,
        (_, Err(e)) => panic!("{name}: connect failed: {e}"),
        (_, Ok(_)) => panic!("{name}: connect succeeded, expected failure"),
    };
    for step in rest {
        match step {
            Step::Login(_) => panic!("{name}: login only as first step"),
            Step::Next(want) => {
                assert_eq!(
                    &next_async(name, &mut client).await.expect(name),
                    want,
                    "{name}"
                );
            }
            Step::Send(payload) => client.send_unsequenced(payload).await.expect(name),
            Step::Logout => client.logout().await.expect(name),
            Step::Closed => {
                let got = next_async(name, &mut client).await;
                assert!(
                    matches!(got, Err(SoupBinError::EndOfSession)),
                    "{name}: {got:?}"
                );
            }
            Step::PeerClosed => {
                let got = next_async(name, &mut client).await;
                assert!(
                    matches!(
                        got,
                        Err(SoupBinError::Transport(TransportError::PeerClosed))
                    ),
                    "{name}: {got:?}"
                );
            }
        }
    }
}

#[test]
fn table_over_mio_sync_poll() {
    for mut case in common::cases() {
        let server = Server::spawn(std::mem::take(&mut case.server));
        run_sync(&case, server.addr);
        server.join(case.name);
    }
}

#[tokio::test]
async fn table_over_tokio_async() {
    for mut case in common::cases() {
        let server = Server::spawn(std::mem::take(&mut case.server));
        run_async(&case, server.addr).await;
        server.join(case.name);
    }
}

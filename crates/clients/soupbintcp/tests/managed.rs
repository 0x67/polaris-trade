//! `recv_managed` (tokio feature): consumer looping only this call keeps
//! session alive by sending client heartbeats itself, then still delivers
//! next data frame.
#![cfg(feature = "tokio")]

pub mod common;

use std::time::Duration;

use client_soupbintcp::{SoupBinClient, SoupBinClientConfig, SoupBinMessage};
use common::{Server, Srv, login_accepted, packet};
use transport_socket::{TcpConfig, tokio::TcpStream};

#[tokio::test]
async fn recv_managed_sends_heartbeats_then_delivers_data() {
    // no data for longer than heartbeat_interval: client must send `R` unprompted
    let server = Server::spawn(vec![
        Srv::Login,
        Srv::Write(login_accepted("sess001", 1)),
        Srv::Expect(packet(b'R', &[])),
        Srv::Write(packet(b'S', b"after-hb")),
    ]);
    let tcp = TcpStream::connect(&TcpConfig::new(server.addr))
        .await
        .expect("connect");
    let cfg = SoupBinClientConfig {
        heartbeat_interval: Duration::from_millis(50),
        ..common::config()
    };
    let mut client = SoupBinClient::connect(tcp, cfg).await.expect("login");

    match client.recv_managed().await.expect("recv_managed") {
        SoupBinMessage::Data(frame) => assert_eq!(frame.as_ref(), b"after-hb"),
        SoupBinMessage::Event(e) => panic!("expected data frame, got event {e:?}"),
    }
    drop(client);
    server.join("recv_managed");
}

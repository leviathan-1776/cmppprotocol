//! 针对最小 in-process mock ISMG 的 end-to-end 测试。
//!
//! 仅使用 crate 的 public API 和 `tokio`（dev-dependency），因此 mock 侧使用原始
//! `tokio::io` 读写 frame，并使用 public `Pdu`/`CmppHeader` encode/decode helpers。

use std::time::Duration;

use cmppprotocol::pdu::{ConnectResp, Deliver, SubmitResp, compute_authenticator_ismg};
use cmppprotocol::{
    CmppConfig, CmppConnection, CmppHeader, CmppProtocolParams, Event, Pdu, SubmitOptions,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::TcpStream;

const SECRET: &str = "secret";

async fn read_frame(stream: &mut TcpStream) -> (CmppHeader, Vec<u8>) {
    let mut hdr = [0u8; 12];
    stream.read_exact(&mut hdr).await.unwrap();
    let total = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
    let header = CmppHeader {
        total_length: total as u32,
        command_id: u32::from_be_bytes([hdr[4], hdr[5], hdr[6], hdr[7]]),
        sequence_id: u32::from_be_bytes([hdr[8], hdr[9], hdr[10], hdr[11]]),
    };
    let mut body = vec![0u8; total - 12];
    stream.read_exact(&mut body).await.unwrap();
    (header, body)
}

fn status_report_content(msg_id: [u8; 8]) -> Vec<u8> {
    let mut content = Vec::new();
    content.extend_from_slice(&msg_id);
    content.extend_from_slice(b"DELIVRD");
    content.extend_from_slice(b"2406061200");
    content.extend_from_slice(b"2406061201");
    let mut dest = b"13800138000".to_vec();
    dest.resize(21, 0);
    content.extend_from_slice(&dest);
    content.extend_from_slice(&7u32.to_be_bytes());
    content
}

#[tokio::test]
async fn connect_submit_and_receive_report() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let msg_id = [1u8, 2, 3, 4, 5, 6, 7, 8];

    // --- mock ISMG ---
    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();

        // 期望收到 CONNECT，并回复带正确 authenticator 的 CONNECT_RESP。
        let (h, body) = read_frame(&mut sock).await;
        let connect = match Pdu::decode(h, &body).unwrap() {
            Pdu::Connect(c) => c,
            other => panic!("期望 CONNECT，实际收到 {:#010x}", other.command_id()),
        };
        let ismg = compute_authenticator_ismg(0, &connect.authenticator_source, SECRET);
        let resp = Pdu::ConnectResp(ConnectResp {
            status: 0,
            authenticator_ismg: ismg,
            version: 0x20,
        });
        sock.write_all(resp.encode(h.sequence_id).as_ref())
            .await
            .unwrap();

        // 期望收到 SUBMIT，并回复 SUBMIT_RESP。
        let (h2, body2) = read_frame(&mut sock).await;
        let submit = match Pdu::decode(h2, &body2).unwrap() {
            Pdu::Submit(s) => s,
            other => panic!("期望 SUBMIT，实际收到 {:#010x}", other.command_id()),
        };
        assert_eq!(submit.dest_terminal_ids, vec!["13800138000".to_string()]);
        let sr = Pdu::SubmitResp(SubmitResp { msg_id, result: 0 });
        sock.write_all(sr.encode(h2.sequence_id).as_ref())
            .await
            .unwrap();

        // 推送 status report（server 主动发起的 DELIVER）。
        let deliver = Pdu::Deliver(Deliver {
            msg_id,
            dest_id: "10690001".into(),
            service_id: "SVC".into(),
            tp_pid: 0,
            tp_udhi: 0,
            msg_fmt: 0,
            src_terminal_id: "13800138000".into(),
            registered_delivery: 1,
            msg_content: status_report_content(msg_id),
        });
        sock.write_all(deliver.encode(100).as_ref()).await.unwrap();

        // 读取 client 的 DELIVER_RESP，然后短暂停留。
        let _ = read_frame(&mut sock).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
    });

    // --- client ---
    let config = CmppConfig {
        host: "127.0.0.1".into(),
        port: addr.port() as i32,
        account: "901234".into(),
        password: SECRET.into(),
        version: cmppprotocol::CMPP_VERSION_20,
        protocol_params: CmppProtocolParams::default(),
    };

    let conn = CmppConnection::connect(config)
        .await
        .expect("connect 应成功");
    let mut events = conn.take_events().await.expect("events 首次可用");

    let opts = SubmitOptions::new("SVC", "901234", "10690001", "13800138000");
    let seq_ids = conn.submit(&opts, "hi", None).await.expect("submit 应成功");
    assert_eq!(seq_ids.len(), 1);

    // 消费 events：期望收到 SUBMIT_RESP 和 status-report DELIVER。
    let mut got_resp = false;
    let mut got_report = false;
    for _ in 0..4 {
        let event = tokio::time::timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("timeout 内应收到 event")
            .expect("event channel 应保持打开");
        match event {
            Event::SubmitResp {
                sequence_id,
                msg_id: mid,
                result,
            } => {
                assert_eq!(sequence_id, seq_ids[0]);
                assert_eq!(mid, msg_id);
                assert_eq!(result, 0);
                got_resp = true;
            }
            Event::Deliver(deliver) => {
                let report = deliver.report().expect("应为 status report");
                assert_eq!(report.stat, "DELIVRD");
                assert_eq!(report.msg_id, msg_id);
                assert_eq!(report.dest_terminal_id, "13800138000");
                got_report = true;
            }
            other => panic!("收到非预期 event: {:?}", other),
        }
        if got_resp && got_report {
            break;
        }
    }
    assert!(
        got_resp && got_report,
        "应同时收到 SubmitResp 和 Deliver report"
    );

    conn.close().await;
    let _ = server.await;
}

#[tokio::test]
async fn rejects_bad_authenticator() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let (h, body) = read_frame(&mut sock).await;
        let _ = Pdu::decode(h, &body).unwrap();
        // 回复 status 0，但使用错误 authenticator。
        let resp = Pdu::ConnectResp(ConnectResp {
            status: 0,
            authenticator_ismg: [0xAB; 16],
            version: 0x20,
        });
        sock.write_all(resp.encode(h.sequence_id).as_ref())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
    });

    let config = CmppConfig {
        host: "127.0.0.1".into(),
        port: addr.port() as i32,
        account: "901234".into(),
        password: SECRET.into(),
        version: cmppprotocol::CMPP_VERSION_20,
        protocol_params: CmppProtocolParams::default(),
    };

    let err = CmppConnection::connect(config)
        .await
        .err()
        .expect("应拒绝连接");
    assert!(matches!(err, cmppprotocol::Error::AuthenticatorMismatch));
    let _ = server.await;
}

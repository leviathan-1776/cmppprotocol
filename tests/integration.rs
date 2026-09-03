//! 针对最小 in-process mock ISMG 的 end-to-end 测试。
//!
//! 仅使用 crate 的 public API 和 `tokio`（dev-dependency），因此 mock 侧使用原始
//! `tokio::io` 读写 frame，并使用 public `Pdu`/`CmppHeader` encode/decode helpers。

use std::time::Duration;

use cmppprotocol::pdu::{ConnectResp, Deliver, SubmitResp, compute_authenticator_ismg};
use cmppprotocol::{
    CmppConfig, CmppConnection, CmppHeader, CmppProtocolParams, Error, Event, Pdu, SubmitOptions,
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
    assert_eq!(conn.metrics().submits_admitted, 1);
    assert_eq!(conn.metrics().window_size, 16);

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

    let metrics = conn.metrics();
    assert_eq!(metrics.submit_responses, 1);
    assert_eq!(metrics.delivers_received, 1);
    assert_eq!(metrics.events_dropped, 0);
    assert_eq!(metrics.submits_in_flight, 0);

    conn.close().await;
    let _ = server.await;
}

#[tokio::test]
async fn pipelined_submits_all_receive_responses() {
    // 一次性入队 10 条消息（writer 会合并为一次批量写出），服务端按序读取
    // 10 个 SUBMIT 并逐条回复，验证批量写出不破坏帧顺序与 sequence 关联。
    const N: usize = 10;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
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

        let mut sequences = Vec::with_capacity(N);
        for _ in 0..N {
            let (h, body) = read_frame(&mut sock).await;
            match Pdu::decode(h, &body).unwrap() {
                Pdu::Submit(_) => sequences.push(h.sequence_id),
                other => panic!("期望 SUBMIT，实际收到 {:#010x}", other.command_id()),
            }
        }
        for &seq in &sequences {
            let sr = Pdu::SubmitResp(SubmitResp {
                msg_id: [7u8; 8],
                result: 0,
            });
            sock.write_all(sr.encode(seq).as_ref()).await.unwrap();
        }
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

    let conn = CmppConnection::connect(config)
        .await
        .expect("connect 应成功");
    let mut events = conn.take_events().await.expect("events 首次可用");

    let opts = SubmitOptions::new("SVC", "901234", "10690001", "13800138000");
    let mut all_seq_ids = Vec::new();
    for _ in 0..N {
        all_seq_ids.extend(conn.submit(&opts, "batch", None).await.expect("submit 应成功"));
    }
    assert_eq!(all_seq_ids.len(), N);

    let mut received = Vec::new();
    while received.len() < N {
        let event = tokio::time::timeout(Duration::from_secs(5), events.recv())
            .await
            .expect("timeout 内应收到 event")
            .expect("event channel 应保持打开");
        match event {
            Event::SubmitResp {
                sequence_id,
                result: 0,
                ..
            } => received.push(sequence_id),
            other => panic!("收到非预期 event: {:?}", other),
        }
    }
    all_seq_ids.sort_unstable();
    received.sort_unstable();
    assert_eq!(received, all_seq_ids);

    let metrics = conn.metrics();
    assert_eq!(metrics.submit_responses, N as u64);
    assert_eq!(metrics.submits_in_flight, 0);

    conn.close().await;
    let _ = server.await;
}

#[tokio::test]
async fn disconnect_publishes_submit_dropped() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // mock ISMG：完成登录、读走 SUBMIT，然后不回 SUBMIT_RESP 直接断开。
    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
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

        let _ = read_frame(&mut sock).await; // SUBMIT
        // 显式关闭：客户端应收到 EOF 而不是任何响应。
        let _ = sock.shutdown().await;
    });

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

    let first = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("断连后应收到 event")
        .expect("event channel 应保持打开");
    match first {
        Event::SubmitDropped { sequence_id } => {
            assert_eq!(sequence_id, seq_ids[0]);
        }
        other => panic!("期望 SubmitDropped，实际收到 {:?}", other),
    }

    let second = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("SubmitDropped 后应收到 Disconnected")
        .expect("event channel 应保持打开");
    assert!(
        matches!(second, Event::Disconnected(_)),
        "期望 Disconnected，实际收到 {:?}",
        second
    );

    let metrics = conn.metrics();
    assert_eq!(metrics.submit_responses, 0);
    assert_eq!(metrics.submit_timeouts, 0);
    assert_eq!(metrics.submits_in_flight, 0);

    server.await.unwrap();
}

#[tokio::test]
async fn partial_submit_and_graceful_close_dropped() {
    // window=1 + 2 段长短信：第 1 段占满窗口，第 2 段在窗口等待期间优雅关闭
    // 连接 → submit 返回 PartialSubmit（携带第 1 段 id）；第 1 段随后以
    // SubmitDropped 终结（验证优雅关闭同样发布逐条终态事件）。
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // mock ISMG：完成登录、读走第 1 段，然后"挂死"——不回 SUBMIT_RESP，
    // 也不回 TERMINATE_RESP。
    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
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
        let _ = read_frame(&mut sock).await; // 第 1 段 SUBMIT
        tokio::time::sleep(Duration::from_secs(30)).await;
    });

    let config = CmppConfig {
        host: "127.0.0.1".into(),
        port: addr.port() as i32,
        account: "901234".into(),
        password: SECRET.into(),
        version: cmppprotocol::CMPP_VERSION_20,
        protocol_params: CmppProtocolParams {
            window_size: 1,
            response_timeout: 1,
            heartbeat_interval: 60,
            read_idle_timeout: 60,
            ..CmppProtocolParams::default()
        },
    };
    let conn = CmppConnection::connect(config)
        .await
        .expect("connect 应成功");
    let mut events = conn.take_events().await.expect("events 首次可用");

    let submit_conn = conn.clone();
    let submit_task = tokio::spawn(async move {
        let opts = SubmitOptions::new("SVC", "901234", "10690001", "13800138000");
        submit_conn.submit(&opts, &"中".repeat(80), None).await
    });

    // 等第 1 段入队（占满 window=1）后再关闭，保证第 2 段阻塞在窗口上。
    let mut admitted = false;
    for _ in 0..500 {
        if conn.metrics().submits_in_flight == 1 {
            admitted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(admitted, "第 1 段应在超时前入队");

    conn.close().await;

    let enqueued = match submit_task.await.unwrap() {
        Err(Error::PartialSubmit { sequence_ids, .. }) => sequence_ids,
        other => panic!("期望 PartialSubmit，实际收到 {:?}", other),
    };
    assert_eq!(enqueued.len(), 1);

    let first = tokio::time::timeout(Duration::from_secs(2), events.recv())
        .await
        .expect("应收到终态 event");
    let first = first.as_ref().expect("event channel 应保持打开");
    match first {
        Event::SubmitDropped { sequence_id } => assert_eq!(*sequence_id, enqueued[0]),
        other => panic!("期望 SubmitDropped，实际收到 {:?}", other),
    }
    // 优雅关闭按设计不发布 Disconnected 事件：通道在终态后直接结束。
    let closed = tokio::time::timeout(Duration::from_secs(2), events.recv())
        .await
        .expect("recv 不应挂起");
    assert!(
        closed.is_none(),
        "优雅关闭后事件通道应结束，实际收到 {:?}",
        closed
    );
    assert_eq!(conn.metrics().submits_in_flight, 0);

    server.abort();
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

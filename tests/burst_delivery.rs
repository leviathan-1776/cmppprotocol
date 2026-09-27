//! 使用真实 TCP 按批写入报文；通过协议确认推进，不用 sleep 推测时序。
use cmppprotocol::pdu::{ConnectResp, Deliver, SubmitResp, compute_authenticator_ismg};
use cmppprotocol::{
    CmppConfig, CmppConnection, CmppFrameCodec, CmppProtocolParams, Event, Pdu, SubmitOptions,
};
use std::collections::HashSet;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio_stream::StreamExt;
use tokio_util::codec::FramedRead;

struct Incoming {
    sequence_id: u32,
    pdu: Pdu,
}

async fn next_frame(
    reader: &mut FramedRead<OwnedReadHalf, CmppFrameCodec>,
    writer: &mut OwnedWriteHalf,
) -> Incoming {
    loop {
        let frame = reader.next().await.unwrap().unwrap();
        if matches!(frame.pdu, Pdu::ActiveTest) {
            writer
                .write_all(&Pdu::ActiveTestResp.encode(frame.sequence_id))
                .await
                .unwrap();
        } else {
            return Incoming {
                sequence_id: frame.sequence_id,
                pdu: frame.pdu,
            };
        }
    }
}

async fn burst_round_trip() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (reader, mut writer) = stream.into_split();
            let mut reader = FramedRead::new(reader, CmppFrameCodec);
            let login = next_frame(&mut reader, &mut writer).await;
            let Pdu::Connect(connect) = login.pdu else {
                panic!("expected CONNECT")
            };
            writer
                .write_all(
                    &Pdu::ConnectResp(ConnectResp {
                        status: 0,
                        authenticator_ismg: compute_authenticator_ismg(
                            0,
                            &connect.authenticator_source,
                            "secret",
                        ),
                        version: 0x20,
                    })
                    .encode(login.sequence_id),
                )
                .await
                .unwrap();
            for round in 0..8u32 {
                for phase in 0..3u32 {
                    let mut sequences = Vec::new();
                    if phase != 1 {
                        for _ in 0..32 {
                            let frame = next_frame(&mut reader, &mut writer).await;
                            assert!(matches!(frame.pdu, Pdu::Submit(_)));
                            sequences.push(frame.sequence_id);
                        }
                    }
                    let mut bytes = Vec::new();
                    for i in 0..32u32 {
                        if phase != 1 {
                            bytes.extend_from_slice(
                                &Pdu::SubmitResp(SubmitResp {
                                    msg_id: [9; 8],
                                    result: 0,
                                })
                                .encode(sequences[i as usize]),
                            );
                        }
                        if phase != 0 {
                            let id = round * 96 + phase * 32 + i;
                            bytes.extend_from_slice(
                                &Pdu::Deliver(Deliver {
                                    msg_id: (id as u64).to_be_bytes(),
                                    dest_id: "10690001".into(),
                                    service_id: "SVC".into(),
                                    tp_pid: 0,
                                    tp_udhi: 0,
                                    msg_fmt: 0,
                                    src_terminal_id: "13800138000".into(),
                                    registered_delivery: 0,
                                    msg_content: vec![b'x'; 100],
                                })
                                .encode(id),
                            );
                        }
                    }
                    writer.write_all(&bytes).await.unwrap();
                    if phase != 0 {
                        let mut acked = HashSet::new();
                        for _ in 0..32 {
                            let frame = next_frame(&mut reader, &mut writer).await;
                            let Pdu::DeliverResp(resp) = frame.pdu else {
                                panic!("expected DELIVER_RESP")
                            };
                            assert_eq!(resp.result, 0);
                            assert_eq!(u64::from_be_bytes(resp.msg_id), frame.sequence_id as u64);
                            assert!(
                                (round * 96 + phase * 32..round * 96 + phase * 32 + 32)
                                    .contains(&frame.sequence_id)
                            );
                            assert!(acked.insert(frame.sequence_id));
                        }
                    }
                }
            }
            let frame = next_frame(&mut reader, &mut writer).await;
            assert!(matches!(frame.pdu, Pdu::Terminate));
            writer
                .write_all(&Pdu::TerminateResp.encode(frame.sequence_id))
                .await
                .unwrap();
        });
        let conn = CmppConnection::connect(CmppConfig {
            host: "127.0.0.1".into(),
            port: port as i32,
            account: "901234".into(),
            password: "secret".into(),
            version: 0x20,
            protocol_params: CmppProtocolParams {
                window_size: 64,
                ..Default::default()
            },
        })
        .await
        .unwrap();
        let mut events = conn.take_events().await.unwrap();
        let opts = SubmitOptions::new("SVC", "901234", "10690001", "13800138000");
        let mut terminals = HashSet::new();
        for round in 0..8u32 {
            for phase in 0..3u32 {
                let mut sequences = Vec::new();
                if phase != 1 {
                    for _ in 0..32 {
                        sequences.extend(conn.submit(&opts, "burst", None).await.unwrap());
                    }
                }
                for i in 0..32u32 {
                    if phase != 1 {
                        let Event::SubmitResp {
                            sequence_id,
                            result,
                            ..
                        } = events.recv().await.unwrap()
                        else {
                            panic!("expected SUBMIT_RESP event")
                        };
                        assert_eq!(sequence_id, sequences[i as usize]);
                        assert_eq!(result, 0);
                        assert!(terminals.insert(sequence_id));
                    }
                    if phase != 0 {
                        let Event::Deliver(d) = events.recv().await.unwrap() else {
                            panic!("expected DELIVER event")
                        };
                        assert_eq!(
                            u64::from_be_bytes(d.msg_id),
                            (round * 96 + phase * 32 + i) as u64
                        );
                    }
                }
            }
        }
        conn.close().await;
        assert!(events.recv().await.is_none());
        server.await.unwrap();
        let metrics = conn.metrics();
        assert_eq!(metrics.submit_responses, 512);
        assert_eq!(metrics.delivers_received, 512);
        assert_eq!(
            (
                metrics.submit_retries,
                metrics.submit_timeouts,
                metrics.events_dropped,
                metrics.submits_in_flight
            ),
            (0, 0, 0, 0)
        );
    })
    .await
    .expect("burst round trip timed out");
}

#[tokio::test(flavor = "current_thread")]
async fn batched_frames_preserve_order_current_thread() {
    burst_round_trip().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batched_frames_preserve_order_multi_thread() {
    burst_round_trip().await;
}

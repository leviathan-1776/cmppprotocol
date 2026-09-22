mod common;

use cmppprotocol::{CmppConnection, CmppProtocolParams, Error, Event, SubmitOptions};
use common::{Faults, MockIsmg, Observed};
use std::time::Duration;

fn params() -> CmppProtocolParams {
    CmppProtocolParams {
        heartbeat_interval: 60,
        response_timeout: 1,
        read_idle_timeout: 60,
        retry_count: 3,
        ..CmppProtocolParams::default()
    }
}

fn options() -> SubmitOptions {
    SubmitOptions::new("FAULT", "901234", "10690111", "13800138111")
}

#[tokio::test]
async fn reversed_failure_responses_match_sequences_and_release_window() {
    tokio::time::timeout(Duration::from_secs(15), async {
        const N: usize = 16;
        let mock = MockIsmg::start(
            Faults {
                response_batch: N,
                reverse_responses: true,
                result: 8,
                ..Faults::default()
            },
            params(),
        )
        .await;
        let conn = CmppConnection::connect(mock.config.clone()).await.unwrap();
        let mut events = conn.take_events().await.unwrap();
        // 两轮占满窗口，证明失败响应释放了许可，下一轮不会挂在准入阶段。
        for _ in 0..2 {
            let mut submitted = Vec::new();
            for _ in 0..N {
                submitted.extend(conn.submit(&options(), "reject", None).await.unwrap());
            }
            for expected in submitted.into_iter().rev() {
                match events.recv().await.unwrap() {
                    Event::SubmitResp {
                        sequence_id,
                        msg_id,
                        result,
                    } => {
                        assert_eq!(sequence_id, expected);
                        assert_eq!(msg_id, u64::from(expected).to_be_bytes());
                        assert_eq!(result, 8);
                    }
                    other => panic!("预期非零 SUBMIT_RESP，实际 {:?}", other),
                }
            }
        }
        assert_eq!(conn.metrics().submit_responses, (2 * N) as u64);
        assert_eq!(conn.metrics().submits_in_flight, 0);
        conn.close().await;
        assert!(events.recv().await.is_none(), "优雅关闭不应重复发送终态");
        mock.finish().await;
    })
    .await
    .expect("乱序失败响应测试超时");
}

#[tokio::test]
async fn lost_response_retries_identical_packet_then_completes_once() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let mut mock = MockIsmg::start(Faults { drop_first: 1, ..Faults::default() }, params()).await;
        let conn = CmppConnection::connect(mock.config.clone()).await.unwrap();
        let mut events = conn.take_events().await.unwrap();
        let seq = conn.submit(&options(), "retry", None).await.unwrap()[0];
        assert!(matches!(events.recv().await, Some(Event::SubmitResp { sequence_id, result: 0, .. }) if sequence_id == seq));
        let mut packets = Vec::new();
        while packets.len() < 2 {
            if let Observed::Submit { sequence_id, packet } = mock.observed.recv().await.unwrap() {
                assert_eq!(sequence_id, seq);
                packets.push(packet);
            }
        }
        assert_eq!(packets[0], packets[1]);
        assert_eq!(conn.metrics().submit_retries, 1);
        assert_eq!(conn.metrics().submit_responses, 1);
        assert_eq!(conn.metrics().submit_timeouts, 0);
        conn.close().await;
        assert!(events.recv().await.is_none());
        mock.finish().await;
    }).await.expect("丢响应重传测试超时");
}

#[tokio::test]
async fn late_duplicate_response_does_not_publish_second_terminal() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let mut mock = MockIsmg::start(Faults {
            response_delay: Duration::from_millis(1600),
            ..Faults::default()
        }, params()).await;
        let conn = CmppConnection::connect(mock.config.clone()).await.unwrap();
        let mut events = conn.take_events().await.unwrap();
        let seq = conn.submit(&options(), "late", None).await.unwrap()[0];
        assert!(matches!(events.recv().await, Some(Event::SubmitResp { sequence_id, .. }) if sequence_id == seq));
        let mut responses = 0;
        while responses < 2 {
            if let Observed::Response(sequence_id) = mock.observed.recv().await.unwrap() {
                assert_eq!(sequence_id, seq);
                responses += 1;
            }
        }
        conn.close().await;
        assert!(events.recv().await.is_none(), "迟到重复响应不能产生第二个终态");
        assert_eq!(conn.metrics().submit_responses, 1);
        assert_eq!(conn.metrics().submit_timeouts, 0);
        mock.finish().await;
    }).await.expect("迟到响应测试超时");
}

#[tokio::test]
async fn heartbeat_exhaustion_drops_all_pending_segments() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let mut mock = MockIsmg::start(
            Faults {
                drop_all: true,
                ignore_heartbeat: true,
                ..Faults::default()
            },
            CmppProtocolParams {
                retry_count: 2,
                ..params()
            },
        )
        .await;
        let conn = CmppConnection::connect(mock.config.clone()).await.unwrap();
        let mut events = conn.take_events().await.unwrap();
        // 等到同一心跳实际重传后才提交，让心跳预算先于这些 SUBMIT 耗尽。
        let mut heartbeats = Vec::new();
        while heartbeats.len() < 2 {
            if let Observed::Heartbeat(seq) = mock.observed.recv().await.unwrap() {
                heartbeats.push(seq);
            }
        }
        assert_eq!(heartbeats[0], heartbeats[1]);
        let mut submitted = Vec::new();
        for _ in 0..8 {
            submitted.extend(conn.submit(&options(), "heartbeat", None).await.unwrap());
        }
        let mut dropped = Vec::new();
        loop {
            match events.recv().await.unwrap() {
                Event::SubmitDropped { sequence_id } => dropped.push(sequence_id),
                Event::Disconnected(Error::Closed) => break,
                other => panic!("预期心跳耗尽拆连，实际 {:?}", other),
            }
        }
        submitted.sort_unstable();
        dropped.sort_unstable();
        assert_eq!(dropped, submitted);
        assert_eq!(conn.metrics().events_dropped, 0);
        assert_eq!(conn.metrics().submits_in_flight, 0);
        assert!(events.recv().await.is_none());
        mock.finish().await;
    })
    .await
    .expect("心跳耗尽测试超时");
}

#[tokio::test]
async fn stalled_consumer_closes_live_connection_with_exact_loss_count() {
    tokio::time::timeout(Duration::from_secs(20), async {
        const N: usize = 1400;
        let mock = MockIsmg::start(
            Faults {
                response_batch: N,
                ..Faults::default()
            },
            CmppProtocolParams {
                window_size: 2048,
                event_spool_capacity: 4096,
                response_timeout: 60,
                ..params()
            },
        )
        .await;
        let conn = CmppConnection::connect(mock.config.clone()).await.unwrap();
        let mut events = conn.take_events().await.unwrap();
        let mut submitted = Vec::new();
        for _ in 0..N {
            submitted.extend(conn.submit(&options(), "stall", None).await.unwrap());
        }
        // 网关保持在线，容量足够接纳所有工单，仅公开通道背压触发关闭。
        tokio::time::timeout(Duration::from_secs(5), async {
            while !events.is_closed() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("默认背压预算应使 dispatcher 有界退出");
        let mut received = Vec::new();
        loop {
            match events.recv().await.unwrap() {
                Event::SubmitResp {
                    sequence_id,
                    result: 0,
                    ..
                } => received.push(sequence_id),
                Event::Disconnected(Error::ChannelClosed) => break,
                other => panic!("预期事件背压拆连，实际 {:?}", other),
            }
        }
        assert!(received.len() < N);
        submitted.sort_unstable();
        received.sort_unstable();
        assert!(received.windows(2).all(|pair| pair[0] != pair[1]));
        assert!(
            received
                .iter()
                .all(|seq| submitted.binary_search(seq).is_ok())
        );
        assert_eq!(conn.metrics().events_dropped, (N - received.len()) as u64);
        assert_eq!(conn.metrics().submits_in_flight, 0);
        assert!(events.recv().await.is_none());
        mock.finish().await;
    })
    .await
    .expect("活跃连接背压测试超时");
}

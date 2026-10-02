use super::*;
use std::future::Future;
use std::task::Poll;

fn deliver(id: u32) -> Event {
    Event::Deliver(crate::pdu::Deliver {
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
}

// 保持第一个写后发布工单未完成，确定性地模拟 writer 尚未完成确认写出的边界。
// 不依赖 TCP send buffer 大小或 sleep 来猜测 socket 是否已阻塞。
async fn queued_gate_and_depth(drop_consumer: bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        let depth = Arc::new(EventDepth::default());
        let (tx, rx) = mpsc::channel(128);
        let (public_tx, mut public_rx) = mpsc::channel(128);
        let (gate_tx, gate_rx) = oneshot::channel();
        tx.try_send(EventSpoolItem::Event {
            event_rx: gate_rx,
            _depth_permit: EventDepthPermit::new(depth.clone()),
        })
        .unwrap();
        for id in 1..96 {
            let permit = EventDepthPermit::new(depth.clone());
            let item = if id % 2 == 0 {
                EventSpoolItem::Direct {
                    event: Event::SubmitResp {
                        sequence_id: id,
                        msg_id: [0; 8],
                        result: 0,
                    },
                    _depth_permit: permit,
                }
            } else {
                let (ready_tx, ready_rx) = oneshot::channel();
                if id != 1 {
                    ready_tx.send(deliver(id)).unwrap();
                }
                EventSpoolItem::Event {
                    event_rx: ready_rx,
                    _depth_permit: permit,
                }
            };
            tx.try_send(item).unwrap();
        }
        tx.try_send(EventSpoolItem::Terminal {
            reason: Some(Error::Closed),
            _depth_permit: EventDepthPermit::new(depth.clone()),
        })
        .unwrap();
        drop(tx);
        let mut dispatcher = Box::pin(event_dispatcher_task(
            rx,
            public_tx,
            Weak::new(),
            Duration::from_millis(100),
        ));
        std::future::poll_fn(|cx| {
            assert!(dispatcher.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert!(matches!(
            public_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        // 包括 dispatcher 当前工单、已批量取出的本地工单及 Terminal；全部仍须计数。
        assert_eq!(depth.current.load(Ordering::SeqCst), 97);
        if drop_consumer {
            drop(public_rx);
            gate_tx.send(deliver(0)).unwrap();
            dispatcher.await;
        } else {
            gate_tx.send(deliver(0)).unwrap();
            let consume = async move {
                for id in (0..96).filter(|id| *id != 1) {
                    match public_rx.recv().await.unwrap() {
                        Event::Deliver(d) if id == 0 || id % 2 == 1 => {
                            assert_eq!(u64::from_be_bytes(d.msg_id), id as u64)
                        }
                        Event::SubmitResp { sequence_id, .. } if id % 2 == 0 => {
                            assert_eq!(sequence_id, id)
                        }
                        other => panic!("事件越过门控或顺序错误: {other:?}"),
                    }
                }
                assert!(matches!(
                    public_rx.recv().await,
                    Some(Event::Disconnected(Error::Closed))
                ));
                assert!(public_rx.recv().await.is_none());
            };
            tokio::join!(dispatcher, consume);
        }
        assert_eq!(depth.current.load(Ordering::SeqCst), 0);
        assert_eq!(depth.peak.load(Ordering::Relaxed), 97);
    })
    .await
    .expect("dispatcher gate/depth test timed out");
}

#[tokio::test(flavor = "current_thread")]
async fn ordered_gate_and_cancel_current_thread() {
    queued_gate_and_depth(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordered_gate_and_cancel_multi_thread() {
    queued_gate_and_depth(false).await;
}
#[tokio::test(flavor = "current_thread")]
async fn consumer_drop_releases_local_depth_current_thread() {
    queued_gate_and_depth(true).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_drop_releases_local_depth_multi_thread() {
    queued_gate_and_depth(true).await;
}

async fn responded_writer_close_case(reject_response_event: bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read, mut write) = stream.into_split();
            let mut frames = FramedRead::new(read, CmppFrameCodec);
            let login = frames.next().await.unwrap().unwrap();
            let Pdu::Connect(connect) = login.pdu else {
                panic!("期望 CONNECT")
            };
            let response = Pdu::ConnectResp(crate::pdu::ConnectResp {
                status: 0,
                authenticator_ismg: compute_authenticator_ismg(
                    0,
                    &connect.authenticator_source,
                    "secret",
                ),
                version: crate::CMPP_VERSION_20,
            });
            write
                .write_all(&response.encode(login.sequence_id))
                .await
                .unwrap();
            while let Some(Ok(_)) = frames.next().await {}
        });
        let conn = CmppConnection::connect(CmppConfig {
            host: "127.0.0.1".into(),
            port: port as i32,
            account: "901234".into(),
            password: "secret".into(),
            version: crate::CMPP_VERSION_20,
            protocol_params: Default::default(),
        })
        .await
        .unwrap();
        let mut events = conn.take_events().await.unwrap();
        let opts = SubmitOptions::new("EARLY", "901234", "10690001", "13800138000");
        let responded = conn.submit(&opts, "responded", None).await.unwrap()[0];
        let unanswered = conn.submit(&opts, "unanswered", None).await.unwrap()[0];
        {
            let mut pending = conn.inner.pending_submits.write();
            // 独立 attempt 固定“实际 writer 尚未收尾”的状态，避免依赖 sleep 或网络时序。
            pending.get_mut(&responded).unwrap().state =
                SubmitAttemptState::Writing { attempt: 123 };
            pending.get_mut(&unanswered).unwrap().state =
                SubmitAttemptState::Writing { attempt: 123 };
        }
        if reject_response_event {
            conn.inner.seal_event_admission();
        }
        // 走真实响应匹配路径；重复响应不得再次发布或计数。
        flush_submit_resps(&conn.inner, &mut vec![(responded, [1; 8], 0)]);
        flush_submit_resps(&conn.inner, &mut vec![(responded, [1; 8], 0)]);
        conn.inner.finish(None);
        let mut terminals = Vec::new();
        while let Some(event) = events.recv().await {
            match event {
                Event::SubmitResp { sequence_id, .. } => terminals.push((sequence_id, true)),
                Event::SubmitDropped { sequence_id } => terminals.push((sequence_id, false)),
                other => panic!("意外终态: {other:?}"),
            }
        }
        let expected = if reject_response_event {
            vec![(unanswered, false)]
        } else {
            vec![(responded, true), (unanswered, false)]
        };
        assert_eq!(
            terminals, expected,
            "已由响应路径接管的请求不能追加关闭终态"
        );
        assert!(conn.inner.pending_submits.read().is_empty());
        assert!(
            conn.inner
                .sequence_registry
                .state
                .lock()
                .unwrap()
                .reserved
                .is_empty()
        );
        assert_eq!(conn.metrics().submits_in_flight, 0);
        assert_eq!(conn.metrics().submit_responses, 1);
        assert_eq!(
            conn.metrics().events_dropped,
            u64::from(reject_response_event)
        );
        conn.close().await;
        server.await.unwrap();
    })
    .await
    .expect("响应与关闭竞争验证超时");
}

#[tokio::test(flavor = "current_thread")]
async fn responded_writer_close_current_thread() {
    responded_writer_close_case(false).await;
    responded_writer_close_case(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responded_writer_close_multi_thread() {
    responded_writer_close_case(false).await;
    responded_writer_close_case(true).await;
}

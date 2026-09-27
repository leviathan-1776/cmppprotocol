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

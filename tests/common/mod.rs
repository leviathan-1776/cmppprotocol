//! 故障注入用 mock ISMG；延迟响应不会阻塞读帧或心跳处理。

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use bytes::Bytes;
use cmppprotocol::{
    CmppConfig, CmppFrameCodec, CmppProtocolParams, ConnectResp, Pdu, SubmitResp,
    compute_authenticator_ismg,
};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_stream::StreamExt;
use tokio_util::codec::FramedRead;

#[derive(Clone)]
pub struct Faults {
    pub response_delay: Duration,
    pub response_batch: usize,
    pub reverse_responses: bool,
    pub result: u8,
    /// 每个 seq 的前几次 SUBMIT 不回响应。
    pub drop_first: usize,
    pub drop_all: bool,
    pub ignore_heartbeat: bool,
}

impl Default for Faults {
    fn default() -> Self {
        Self {
            response_delay: Duration::ZERO,
            response_batch: 1,
            reverse_responses: false,
            result: 0,
            drop_first: 0,
            drop_all: false,
            ignore_heartbeat: false,
        }
    }
}

#[derive(Debug)]
pub enum Observed {
    Submit { sequence_id: u32, packet: Bytes },
    Heartbeat(u32),
    Response(u32),
}

pub struct MockIsmg {
    pub config: CmppConfig,
    pub observed: mpsc::UnboundedReceiver<Observed>,
    task: Option<JoinHandle<()>>,
}

impl MockIsmg {
    pub async fn start(faults: Faults, params: CmppProtocolParams) -> Self {
        assert!(faults.response_batch > 0);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        // 用例限定提交总量；观测记录不对网关读写施加额外背压。
        let (tx, observed) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read, mut write) = stream.into_split();
            let mut frames = FramedRead::new(read, CmppFrameCodec);
            let login = frames.next().await.unwrap().unwrap();
            let Pdu::Connect(connect) = login.pdu else {
                panic!("期望 CONNECT")
            };
            let reply = Pdu::ConnectResp(ConnectResp {
                status: 0,
                authenticator_ismg: compute_authenticator_ismg(
                    0,
                    &connect.authenticator_source,
                    "secret",
                ),
                version: cmppprotocol::CMPP_VERSION_20,
            });
            write
                .write_all(&reply.encode(login.sequence_id))
                .await
                .unwrap();
            let mut attempts = HashMap::<u32, usize>::new();
            let mut batch = Vec::new();
            let mut delayed = VecDeque::<(Instant, Vec<u32>)>::new();
            loop {
                // 相同延迟使到期顺序与入队顺序一致；FramedRead 保留被 select 取消的半帧。
                let deadline = delayed
                    .front()
                    .map(|(at, _)| *at)
                    .unwrap_or_else(|| Instant::now() + Duration::from_secs(3600));
                tokio::select! {
                    _ = tokio::time::sleep_until(deadline), if !delayed.is_empty() => {
                        let (_, sequences) = delayed.pop_front().unwrap();
                        for sequence_id in sequences {
                            let response = Pdu::SubmitResp(SubmitResp {
                                msg_id: u64::from(sequence_id).to_be_bytes(),
                                result: faults.result,
                            });
                            if write.write_all(&response.encode(sequence_id)).await.is_err() { return; }
                            let _ = tx.send(Observed::Response(sequence_id));
                        }
                    }
                    frame = frames.next() => {
                        let Some(frame) = frame else { break };
                        let frame = frame.expect("客户端应发送完整合法帧");
                        let sequence_id = frame.sequence_id;
                        match frame.pdu {
                            Pdu::Submit(submit) => {
                                let packet = Pdu::Submit(submit).encode(sequence_id);
                                let _ = tx.send(Observed::Submit { sequence_id, packet });
                                let attempt = attempts.entry(sequence_id).or_default();
                                *attempt += 1;
                                if faults.drop_all || *attempt <= faults.drop_first { continue; }
                                batch.push(sequence_id);
                                if batch.len() == faults.response_batch {
                                    let mut ready = std::mem::take(&mut batch);
                                    if faults.reverse_responses { ready.reverse(); }
                                    delayed.push_back((Instant::now() + faults.response_delay, ready));
                                }
                            }
                            Pdu::ActiveTest => {
                                let _ = tx.send(Observed::Heartbeat(sequence_id));
                                if !faults.ignore_heartbeat
                                    && write.write_all(&Pdu::ActiveTestResp.encode(sequence_id)).await.is_err() { return; }
                            }
                            Pdu::Terminate => {
                                let _ = write.write_all(&Pdu::TerminateResp.encode(sequence_id)).await;
                                break;
                            }
                            other => panic!("意外客户端 PDU: {:?}", other),
                        }
                    }
                }
            }
        });
        Self {
            config: CmppConfig {
                host: "127.0.0.1".into(),
                port: address.port() as i32,
                account: "901234".into(),
                password: "secret".into(),
                version: cmppprotocol::CMPP_VERSION_20,
                protocol_params: params,
            },
            observed,
            task: Some(task),
        }
    }

    /// 收尾时传播 mock 任务的 panic，避免后台失败被忽略。
    pub async fn finish(mut self) {
        let mut task = self.task.take().unwrap();
        match tokio::time::timeout(Duration::from_secs(5), &mut task).await {
            Ok(result) => result.expect("mock ISMG 任务失败"),
            Err(_) => {
                task.abort();
                panic!("mock ISMG 未及时退出");
            }
        }
    }
}

impl Drop for MockIsmg {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

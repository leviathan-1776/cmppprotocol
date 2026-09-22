//! 端到端吞吐/延迟压测：进程内 mock ISMG + 可配置并发 submitter。
//!
//! 用法：`cargo run --release --example loadtest -- [k=v ...]`
//! 参数（默认值）：duration=10 window=64 delay_ms=50 parallel=8 dest_count=1 long=0
//! 调优（默认值）：batch_frames=64 batch_bytes=65536 spool_capacity=256 event_timeout_ms=1000
//!
//! - `delay_ms` 模拟网关响应 RTT；稳态吞吐理论上限约为 `window / delay`。
//! - `long=1` 发送 500 字中文长短信（8 段 UCS2）。
//! - `dest_count>1` 在单个 SUBMIT 中携带多个目的号码（测多目的编码成本）。
//! - `udh_cooldown_ms` 覆盖长短信 UDH reference 的 cooldown（默认 300s）。
//!
//! 注意：事件消费者必须跟上响应速率，否则会触发连接的有界关闭
//! （`dropped_events` > 0），此时测得的是消费者背压而不是协议吞吐。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::{BufMut, BytesMut};
use cmppprotocol::pdu::{ConnectResp, SubmitResp, compute_authenticator_ismg};
use cmppprotocol::{
    CMPP_ACTIVE_TEST, CMPP_SUBMIT, CMPP_TERMINATE, CMPP_VERSION_20, CmppConfig, CmppConnection,
    CmppHeader, CmppProtocolParams, Event, Pdu, SubmitOptions,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Mutex as AsyncMutex;

const SECRET: &str = "secret";

#[derive(Default)]
struct Shared {
    submit_calls: u64,
    segments: u64,
    responses: u64,
    timeouts: u64,
    drops: u64,
    max_submit_block: Duration,
    /// 连接提前关闭的时刻（None = 全程存活）。
    closed_at: Option<Duration>,
    /// UDH reference 池耗尽（同一重组域 256 个 reference 进入 cooldown）。
    udh_exhausted: bool,
    /// sequence id -> 入队时刻（延迟采样：仅记录 1/64 的 submit 批次）。
    submitted: HashMap<u32, Instant>,
    latencies: Vec<Duration>,
}

fn parse_args() -> HashMap<String, String> {
    std::env::args()
        .skip(1)
        .filter_map(|arg| {
            let (key, value) = arg.split_once('=')?;
            Some((key.to_string(), value.to_string()))
        })
        .collect()
}

fn arg_u64(args: &HashMap<String, String>, key: &str, default: u64) -> u64 {
    args.get(key)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// mock ISMG 侧的缓冲 frame 读取器：大块读取 + 手动解析长度前缀，
/// 避免每帧 2 次 read_exact syscall 抢占被测客户端的 CPU。
struct FrameReader {
    stream: OwnedReadHalf,
    buf: BytesMut,
}

impl FrameReader {
    fn new(stream: OwnedReadHalf) -> Self {
        FrameReader {
            stream,
            buf: BytesMut::with_capacity(64 * 1024),
        }
    }

    async fn next_frame(&mut self) -> std::io::Result<Option<(CmppHeader, Vec<u8>)>> {
        loop {
            if self.buf.len() >= 12 {
                let total = u32::from_be_bytes(self.buf[..4].try_into().unwrap()) as usize;
                if (12..=65536).contains(&total) && self.buf.len() >= total {
                    let frame = self.buf.split_to(total).freeze();
                    let header = CmppHeader {
                        total_length: total as u32,
                        command_id: u32::from_be_bytes(frame[4..8].try_into().unwrap()),
                        sequence_id: u32::from_be_bytes(frame[8..12].try_into().unwrap()),
                    };
                    return Ok(Some((header, frame[12..].to_vec())));
                }
                if !(12..=65536).contains(&total) {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "mock ISMG 收到非法长度前缀",
                    ));
                }
            }
            let mut chunk = [0u8; 16 * 1024];
            let n = self.stream.read(&mut chunk).await?;
            if n == 0 {
                return Ok(None);
            }
            self.buf.put_slice(&chunk[..n]);
        }
    }
}

/// 进程内 mock ISMG：完成登录后，对每个 SUBMIT 回复 SUBMIT_RESP。
/// `delay` 为零时在同一循环内联回复（最小化 harness 开销）；
/// 否则每个 SUBMIT spawn 一个延迟响应 task（模拟并发 RTT）。
async fn mock_ismg(listener: TcpListener, delay: Duration) {
    let (stream, _) = listener.accept().await.expect("accept");
    let (read_half, write_half) = stream.into_split();
    let writer = Arc::new(AsyncMutex::new(write_half));
    let mut reader = FrameReader::new(read_half);

    let Some((header, body)) = reader.next_frame().await.expect("读 CONNECT") else {
        return;
    };
    let connect = match Pdu::decode(header, &body).expect("decode CONNECT") {
        Pdu::Connect(c) => c,
        other => panic!("期望 CONNECT，实际收到 {:#010x}", other.command_id()),
    };
    let ismg = compute_authenticator_ismg(0, &connect.authenticator_source, SECRET);
    let resp = Pdu::ConnectResp(ConnectResp {
        status: 0,
        authenticator_ismg: ismg,
        version: 0x20,
    });
    writer
        .lock()
        .await
        .write_all(resp.encode(header.sequence_id).as_ref())
        .await
        .expect("写 CONNECT_RESP");

    while let Ok(Some((header, _body))) = reader.next_frame().await {
        match header.command_id {
            CMPP_SUBMIT => {
                let respond = |writer: Arc<AsyncMutex<OwnedWriteHalf>>, sequence_id: u32| async move {
                    let resp = Pdu::SubmitResp(SubmitResp {
                        msg_id: [9; 8],
                        result: 0,
                    });
                    let _ = writer
                        .lock()
                        .await
                        .write_all(resp.encode(sequence_id).as_ref())
                        .await;
                };
                if delay.is_zero() {
                    respond(writer.clone(), header.sequence_id).await;
                } else {
                    let writer = writer.clone();
                    let sequence_id = header.sequence_id;
                    tokio::spawn(async move {
                        tokio::time::sleep(delay).await;
                        respond(writer, sequence_id).await;
                    });
                }
            }
            CMPP_ACTIVE_TEST => {
                let _ = writer
                    .lock()
                    .await
                    .write_all(Pdu::ActiveTestResp.encode(header.sequence_id).as_ref())
                    .await;
            }
            CMPP_TERMINATE => {
                let _ = writer
                    .lock()
                    .await
                    .write_all(Pdu::TerminateResp.encode(header.sequence_id).as_ref())
                    .await;
                break;
            }
            _ => {}
        }
    }
}

/// 输入必须已排序。
fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let index = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[index]
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args = parse_args();
    let duration = Duration::from_secs(arg_u64(&args, "duration", 10));
    let window = arg_u64(&args, "window", 64) as usize;
    let delay = Duration::from_millis(arg_u64(&args, "delay_ms", 50));
    let parallel = arg_u64(&args, "parallel", 8) as usize;
    let dest_count = arg_u64(&args, "dest_count", 1) as usize;
    let long = arg_u64(&args, "long", 0) == 1;
    let batch_frames = arg_u64(&args, "batch_frames", 64) as usize;
    let batch_bytes = arg_u64(&args, "batch_bytes", 64 * 1024) as usize;
    let spool_capacity = arg_u64(&args, "spool_capacity", 256) as usize;
    let event_timeout_ms = arg_u64(&args, "event_timeout_ms", 1000);

    println!(
        "参数: duration={:?} window={} delay={:?} parallel={} dest_count={} long={}",
        duration, window, delay, parallel, dest_count, long
    );
    println!(
        "调优: batch_frames={} batch_bytes={} spool_capacity={} event_timeout_ms={}",
        batch_frames, batch_bytes, spool_capacity, event_timeout_ms
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(mock_ismg(listener, delay));

    let config = CmppConfig {
        host: "127.0.0.1".into(),
        port: addr.port() as i32,
        account: "901234".into(),
        password: SECRET.into(),
        version: CMPP_VERSION_20,
        protocol_params: CmppProtocolParams {
            window_size: window,
            write_batch_max_frames: batch_frames,
            write_batch_max_bytes: batch_bytes,
            event_spool_capacity: spool_capacity,
            event_backpressure_timeout_ms: event_timeout_ms,
            ..CmppProtocolParams::default()
        },
    };
    let conn = if args.contains_key("udh_cooldown_ms") {
        CmppConnection::connect_with_udh_reference_cooldown(
            config,
            Duration::from_millis(arg_u64(&args, "udh_cooldown_ms", 300_000)),
        )
        .await
        .expect("连接失败")
    } else {
        CmppConnection::connect(config).await.expect("连接失败")
    };
    let mut events = conn.take_events().await.expect("events 首次可用");

    let shared = Arc::new(Mutex::new(Shared::default()));

    let event_shared = shared.clone();
    let event_task = tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            let mut shared = event_shared.lock().unwrap();
            match event {
                Event::SubmitResp { sequence_id, .. } => {
                    shared.responses += 1;
                    // 只有采样的 sequence id 会出现在 map 中；未采样响应的
                    // remove 是一次廉价的 miss 查询。
                    if let Some(start) = shared.submitted.remove(&sequence_id) {
                        shared.latencies.push(start.elapsed());
                    }
                }
                Event::SubmitTimeout { sequence_id, .. } => {
                    shared.submitted.remove(&sequence_id);
                    shared.timeouts += 1;
                }
                Event::SubmitDropped { sequence_id } => {
                    shared.submitted.remove(&sequence_id);
                    shared.drops += 1;
                }
                Event::Deliver(_) => {}
                Event::Disconnected(_) => break,
            }
        }
    });

    let message = if long {
        "压".repeat(500)
    } else {
        "throughput".to_string()
    };
    let opts = SubmitOptions::new("SVC", "901234", "10690001", "13800138000");
    let opts = if dest_count > 1 {
        opts.dest_terminal_ids((0..dest_count).map(|i| format!("1380000{i:04}")).collect())
    } else {
        opts
    };

    let deadline = Instant::now() + duration;
    let run_started_at = Instant::now();
    let mut submitters = Vec::with_capacity(parallel);
    for _ in 0..parallel {
        let conn = conn.clone();
        let opts = opts.clone();
        let message = message.clone();
        let shared = shared.clone();
        submitters.push(tokio::spawn(async move {
            let mut sample_counter = 0u64;
            while Instant::now() < deadline {
                let started_at = Instant::now();
                match conn.submit(&opts, &message, None).await {
                    Ok(sequence_ids) => {
                        let blocked = started_at.elapsed();
                        let enqueued_at = Instant::now();
                        let mut shared = shared.lock().unwrap();
                        shared.submit_calls += 1;
                        shared.segments += sequence_ids.len() as u64;
                        if blocked > shared.max_submit_block {
                            shared.max_submit_block = blocked;
                        }
                        // 延迟采样：仅 1/64 的批次记录入队时刻，把事件消费者的
                        // per-event 开销降到最低（饱和场景下消费者必须跟上速率）。
                        sample_counter += 1;
                        if sample_counter % 64 == 0 {
                            shared
                                .submitted
                                .extend(sequence_ids.iter().map(|&id| (id, enqueued_at)));
                        }
                    }
                    Err(e) => {
                        let mut shared = shared.lock().unwrap();
                        if matches!(e, cmppprotocol::Error::ResourceExhausted(_)) {
                            // UDH reference 池按 (src_id, dest) 分域，每域 256 个
                            // reference 释放后默认进入 300s cooldown——长短信持续
                            // 速率受此约束，这是防重组错误的刻意设计。
                            shared.udh_exhausted = true;
                            eprintln!("submit 失败: {}（UDH 引用池 cooldown 中）", e);
                        } else {
                            if shared.closed_at.is_none() {
                                shared.closed_at = Some(run_started_at.elapsed());
                            }
                            eprintln!("submit 失败: {}（连接可能已关闭）", e);
                        }
                        break;
                    }
                }
            }
        }));
    }
    for handle in submitters {
        let _ = handle.await;
    }

    // 等待在途 segment 全部终结。
    let drain_deadline = deadline + delay * 8 + Duration::from_secs(10);
    loop {
        let (segments, finished) = {
            let shared = shared.lock().unwrap();
            (
                shared.segments,
                shared.responses + shared.timeouts + shared.drops,
            )
        };
        if finished >= segments || Instant::now() > drain_deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let metrics = conn.metrics();
    conn.close().await;
    let _ = event_task.await;
    let _ = server.await;

    let shared = shared.lock().unwrap();
    let elapsed = duration.as_secs_f64();
    println!("----------------------------------------");
    println!(
        "submit 调用: {}  segments: {}",
        shared.submit_calls, shared.segments
    );
    println!(
        "响应: {}  超时: {}  丢弃: {}",
        shared.responses, shared.timeouts, shared.drops
    );
    println!(
        "吞吐: {:.0} segments/s（{:.0} submit/s）",
        shared.segments as f64 / elapsed,
        shared.submit_calls as f64 / elapsed
    );
    if let Some(closed_at) = shared.closed_at {
        println!(
            "⚠ 连接在 {:.1?} 后被有界关闭（事件消费者未跟上饱和速率），吞吐按全程 {}s 折算",
            closed_at, elapsed
        );
    }
    if shared.udh_exhausted {
        println!(
            "⚠ UDH reference 池耗尽：长短信持续速率受 256 reference × cooldown 约束（默认 300s，可配）"
        );
    }
    if !delay.is_zero() {
        println!(
            "理论窗口上限: {:.0} segments/s（window/delay）",
            window as f64 / delay.as_secs_f64()
        );
    }
    if !shared.latencies.is_empty() {
        let mut sorted = shared.latencies.clone();
        sorted.sort();
        let average: Duration = sorted.iter().sum::<Duration>() / sorted.len() as u32;
        println!(
            "端到端延迟: avg={:.1?} p50={:.1?} p95={:.1?} p99={:.1?} max={:.1?}",
            average,
            percentile(&sorted, 50.0),
            percentile(&sorted, 95.0),
            percentile(&sorted, 99.0),
            sorted[sorted.len() - 1]
        );
    }
    println!(
        "submit() 最长阻塞（窗口回压）: {:.1?}",
        shared.max_submit_block
    );
    println!(
        "metrics: retries={} timeouts={} dropped_events={} final_in_flight={}",
        metrics.submit_retries,
        metrics.submit_timeouts,
        metrics.events_dropped,
        metrics.submits_in_flight
    );
    let average_batch = if metrics.write_batches == 0 {
        0.0
    } else {
        metrics.write_frames as f64 / metrics.write_batches as f64
    };
    println!(
        "writer: batches={} frames={} average_batch={:.2} event_depth_peak={}",
        metrics.write_batches, metrics.write_frames, average_batch, metrics.event_depth_peak
    );
}

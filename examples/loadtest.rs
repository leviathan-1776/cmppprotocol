//! 端到端吞吐/延迟压测：进程内 mock ISMG + 可配置并发 submitter。
//!
//! 用法：`cargo run --release --example loadtest -- [k=v ...]`
//! 参数（默认值）：duration=10 window=64 delay_ms=50 parallel=8 dest_count=1 long=0
//! 扩展：connections=1 deliver_window=0 alloc=0 server_nodelay=1；parallel=0 可仅压 DELIVER。
//! alloc=1 统计整个进程的成功分配/重分配请求，含网关与统计器，必须与吞吐轮次分开。
//! 调优（默认值）：batch_frames=64 batch_bytes=65536 spool_capacity=256 event_timeout_ms=1000
//!
//! - `delay_ms` 模拟网关响应 RTT；稳态吞吐理论上限约为 `window / delay`。
//! - `long=1` 发送 500 字中文长短信（8 段 UCS2）。
//! - `dest_count>1` 在单个 SUBMIT 中携带多个目的号码（测多目的编码成本）。
//! - `udh_cooldown_ms` 覆盖长短信 UDH reference 的 cooldown（默认 300s）。
//!
//! 注意：事件消费者必须跟上响应速率，否则会触发连接的有界关闭
//! 提前关闭的轮次不能视为稳态吞吐；events_dropped=0 也不表示连接未提前关闭。

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use bytes::{BufMut, BytesMut};
use cmppprotocol::pdu::{ConnectResp, Deliver, SubmitResp, compute_authenticator_ismg};
use cmppprotocol::{
    CMPP_ACTIVE_TEST, CMPP_DELIVER_RESP, CMPP_SUBMIT, CMPP_TERMINATE, CMPP_VERSION_20, CmppConfig,
    CmppConnection, CmppHeader, CmppProtocolParams, Event, Pdu, SubmitOptions,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{Barrier, Mutex as AsyncMutex};

#[path = "support/allocation_meter.rs"]
mod allocation_meter;

const SECRET: &str = "secret";

#[derive(Default)]
struct Shared {
    submit_calls: u64,
    segments: u64,
    responses: u64,
    timeouts: u64,
    drops: u64,
    delivers: u64,
    max_submit_block: Duration,
    /// 连接提前关闭的时刻（None = 全程存活）。
    closed_at: Option<Duration>,
    /// UDH reference 池耗尽（同一重组域 256 个 reference 进入 cooldown）。
    udh_exhausted: bool,
    /// sequence id -> 入队时刻（延迟采样：仅记录 1/64 的 submit 批次）。
    submitted: HashMap<u32, Instant>,
    latencies: Vec<Duration>,
    deliver_latencies: Vec<Duration>,
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

/// 所有连接和网关完成握手后一起开始计时。
struct RunGate {
    ready: Barrier,
    go: Barrier,
    start: OnceLock<Instant>,
    duration: Duration,
}

impl RunGate {
    async fn wait(&self) -> Instant {
        self.ready.wait().await;
        self.go.wait().await;
        *self.start.get().unwrap()
    }
}

#[derive(Default)]
struct GatewayStats {
    deliver_sent: u64,
    deliver_acked: u64,
}

/// 进程内网关；非零 delay 通过每个 SUBMIT 一个延迟 task 模拟并发 RTT。
async fn mock_ismg(
    listener: TcpListener,
    delay: Duration,
    deliver_window: usize,
    gate: Arc<RunGate>,
    deliver_drained: Arc<tokio::sync::Notify>,
    server_nodelay: bool,
) -> GatewayStats {
    let (stream, _) = listener.accept().await.expect("accept");
    stream
        .set_nodelay(server_nodelay)
        .expect("mock TCP_NODELAY");
    let (read_half, write_half) = stream.into_split();
    let writer = Arc::new(AsyncMutex::new(write_half));
    let mut reader = FrameReader::new(read_half);

    let Some((header, body)) = reader.next_frame().await.expect("读 CONNECT") else {
        return GatewayStats::default();
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

    let started = gate.wait().await;
    let deadline = started + gate.duration;
    let mut deliver = Deliver {
        msg_id: [7; 8],
        dest_id: "10690001".into(),
        service_id: "SVC".into(),
        tp_pid: 0,
        tp_udhi: 0,
        msg_fmt: 0,
        src_terminal_id: "13800138000".into(),
        registered_delivery: 0,
        msg_content: vec![b'x'; 100],
    };
    let mut stats = GatewayStats::default();
    let mut next_deliver = 1u32;
    let mut pending = std::collections::HashSet::new();
    for _ in 0..deliver_window {
        deliver.msg_id = (started.elapsed().as_nanos() as u64).to_be_bytes();
        writer
            .lock()
            .await
            .write_all(&Pdu::Deliver(deliver.clone()).encode(next_deliver))
            .await
            .expect("DELIVER write");
        pending.insert(next_deliver);
        next_deliver += 1;
        stats.deliver_sent += 1;
    }
    'read: while let Ok(Some((header, _body))) = reader.next_frame().await {
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
            CMPP_DELIVER_RESP => {
                assert!(
                    pending.remove(&header.sequence_id),
                    "duplicate/unknown DELIVER_RESP"
                );
                stats.deliver_acked += 1;
                if Instant::now() < deadline {
                    deliver.msg_id = (started.elapsed().as_nanos() as u64).to_be_bytes();
                    if writer
                        .lock()
                        .await
                        .write_all(&Pdu::Deliver(deliver.clone()).encode(next_deliver))
                        .await
                        .is_err()
                    {
                        break 'read;
                    }
                    pending.insert(next_deliver);
                    next_deliver = next_deliver.wrapping_add(1);
                    stats.deliver_sent += 1;
                } else if pending.is_empty() {
                    deliver_drained.notify_one();
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
    deliver_drained.notify_one();
    stats
}

/// 输入必须已排序。
fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let index = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[index]
}

async fn run_connection(
    args: HashMap<String, String>,
    gate: Arc<RunGate>,
    index: usize,
) -> (Shared, cmppprotocol::ConnectionMetrics, GatewayStats) {
    let duration = Duration::from_secs(arg_u64(&args, "duration", 10));
    let window = arg_u64(&args, "window", 64) as usize;
    let delay = Duration::from_millis(arg_u64(&args, "delay_ms", 50));
    let parallel = arg_u64(&args, "parallel", 8) as usize;
    let dest_count = arg_u64(&args, "dest_count", 1) as usize;
    let deliver_window = arg_u64(&args, "deliver_window", 0) as usize;
    assert!(duration.as_secs() > 0 && (parallel > 0 || deliver_window > 0));
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
    let deliver_drained = Arc::new(tokio::sync::Notify::new());
    let server = tokio::spawn(mock_ismg(
        listener,
        delay,
        deliver_window,
        gate.clone(),
        deliver_drained.clone(),
        arg_u64(&args, "server_nodelay", 1) == 1,
    ));

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
    let event_gate = gate.clone();
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
                Event::Deliver(deliver) => {
                    shared.delivers += 1;
                    if shared.delivers % 64 == 0 {
                        let elapsed = event_gate.start.get().unwrap().elapsed();
                        shared.deliver_latencies.push(elapsed.saturating_sub(
                            Duration::from_nanos(u64::from_be_bytes(deliver.msg_id)),
                        ));
                    }
                }
                Event::Disconnected(reason) => {
                    eprintln!("connection={index} disconnected: {reason:?}");
                    if shared.closed_at.is_none() {
                        shared.closed_at = Some(event_gate.start.get().unwrap().elapsed());
                    }
                    break;
                }
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

    let run_started_at = gate.wait().await;
    let deadline = run_started_at + duration;
    let mut submitters = Vec::with_capacity(parallel);
    for _ in 0..parallel {
        let conn = conn.clone();
        let opts = opts.clone();
        let message = message.clone();
        let shared = shared.clone();
        submitters.push(tokio::spawn(async move {
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
                        if shared.submit_calls % 64 == 0 {
                            shared
                                .submitted
                                .extend(sequence_ids.iter().map(|&id| (id, enqueued_at)));
                        }
                    }
                    Err(e) => {
                        let mut shared = shared.lock().unwrap();
                        let cause = match &e {
                            cmppprotocol::Error::PartialSubmit {
                                sequence_ids,
                                source,
                            } => {
                                shared.segments += sequence_ids.len() as u64;
                                source.as_ref()
                            }
                            other => other,
                        };
                        if matches!(cause, cmppprotocol::Error::ResourceExhausted(_)) {
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
        handle.await.expect("submitter panic");
    }

    // 即使纯 DELIVER 或 UDH 提前耗尽，也观测完整配置时长。
    tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
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
        if finished >= segments
            || shared.lock().unwrap().closed_at.is_some()
            || Instant::now() > drain_deadline
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // DELIVER 发出端在 deadline 后停止补充；给最后一个窗口留出收尾时间。
    if deliver_window > 0
        && tokio::time::timeout(Duration::from_secs(5), deliver_drained.notified())
            .await
            .is_err()
    {
        eprintln!("connection={index}: DELIVER drain timeout");
    }
    let metrics = conn.metrics();
    conn.close().await;
    event_task.await.expect("event consumer panic");
    let gateway = server.await.expect("mock gateway panic");

    let shared = Arc::try_unwrap(shared).ok().unwrap().into_inner().unwrap();
    println!(
        "connection={index} deliver_sent={} deliver_acked={} deliver_events={}",
        gateway.deliver_sent, gateway.deliver_acked, shared.delivers
    );
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
            "⚠ 连接在 {:.1?} 后提前关闭（原因见 stderr），吞吐按全程 {}s 折算",
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
            "入队返回后响应延迟（抽样，快速响应可能漏样）: avg={:.1?} p50={:.1?} p95={:.1?} p99={:.1?} max={:.1?}",
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
    (shared, metrics, gateway)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args = parse_args();
    let budget = Duration::from_secs(arg_u64(&args, "duration", 10) + 60);
    tokio::time::timeout(budget, run_all(args))
        .await
        .expect("loadtest exceeded time budget");
}

async fn run_all(args: HashMap<String, String>) {
    let connections = arg_u64(&args, "connections", 1) as usize;
    assert!((1..=128).contains(&connections));
    let gate = Arc::new(RunGate {
        ready: Barrier::new(connections * 2 + 1),
        go: Barrier::new(connections * 2 + 1),
        start: OnceLock::new(),
        duration: Duration::from_secs(arg_u64(&args, "duration", 10)),
    });
    let mut jobs = tokio::task::JoinSet::new();
    for index in 0..connections {
        jobs.spawn(run_connection(args.clone(), gate.clone(), index));
    }
    tokio::select! {
        _ = gate.ready.wait() => {}
        result = jobs.join_next() => { let _ = result.expect("missing connection task").expect("connection setup failed"); panic!("connection ended before start"); }
    }
    let instrumented = arg_u64(&args, "alloc", 0) == 1;
    allocation_meter::enable(instrumented);
    let started = Instant::now();
    gate.start.set(started).unwrap();
    gate.go.wait().await;
    let mut segments = 0;
    let mut responses = 0;
    let mut delivers = 0;
    let mut sent = 0;
    let mut acked = 0;
    let mut retries = 0;
    let mut lost = 0;
    let mut timeouts = 0;
    let mut closed = 0;
    let mut exhausted = 0;
    let mut latencies = Vec::new();
    let mut deliver_latencies = Vec::new();
    while let Some(job) = jobs.join_next().await {
        let (s, m, g) = job.expect("connection runner panic");
        segments += s.segments;
        responses += s.responses;
        delivers += s.delivers;
        sent += g.deliver_sent;
        acked += g.deliver_acked;
        retries += m.submit_retries;
        lost += m.events_dropped + s.drops;
        timeouts += s.timeouts;
        closed += usize::from(s.closed_at.is_some());
        exhausted += usize::from(s.udh_exhausted);
        latencies.extend(s.latencies);
        deliver_latencies.extend(s.deliver_latencies);
    }
    allocation_meter::enable(false);
    let (allocations, bytes) = allocation_meter::snapshot();
    latencies.sort_unstable();
    deliver_latencies.sort_unstable();
    println!(
        "DELIVER_LATENCY samples={} p50_us={} p95_us={} p99_us={}",
        deliver_latencies.len(),
        percentile(&deliver_latencies, 50.0).as_micros(),
        percentile(&deliver_latencies, 95.0).as_micros(),
        percentile(&deliver_latencies, 99.0).as_micros()
    );
    println!(
        "RESULT connections={} segments={} responses={} submit_rate={:.0} deliver_sent={} deliver_acked={} delivers={} deliver_rate={:.0} retries={} loss_signals={} timeouts={} closed={} exhausted={} latency_samples={} p50_us={} p95_us={} p99_us={} observed_secs={:.3} alloc_enabled={} allocations={} allocated_bytes={}",
        connections,
        segments,
        responses,
        responses as f64 / gate.duration.as_secs_f64(),
        sent,
        acked,
        delivers,
        delivers as f64 / gate.duration.as_secs_f64(),
        retries,
        lost,
        timeouts,
        closed,
        exhausted,
        latencies.len(),
        percentile(&latencies, 50.0).as_micros(),
        percentile(&latencies, 95.0).as_micros(),
        percentile(&latencies, 99.0).as_micros(),
        started.elapsed().as_secs_f64(),
        instrumented,
        allocations,
        bytes
    );
    assert_eq!(
        segments, responses,
        "incomplete successful SUBMIT responses"
    );
    assert_eq!((lost, timeouts, closed), (0, 0, 0), "unhealthy run");
    assert_eq!(sent, acked, "DELIVER acknowledgements missing");
    assert_eq!(acked, delivers, "DELIVER events missing");
}

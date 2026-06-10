//! Async CMPP 2.0 client connection。
//!
//! [`CmppConnection::connect`] 会执行完整登录 handshake（并校验 ISMG authenticator）
//! 后再返回。[`CmppConnection::submit`] 是 non-blocking：它将 message 入队
//! （受 sliding-window backpressure 约束），并立即返回分配好的 sequence id，
//! 符合 CMPP async、pipelined 的特性。所有 response，包括 SUBMIT_RESP、入站
//! DELIVER（status reports / MO）、submit timeout 和连接断开，都会通过
//! [`CmppConnection::take_events`] 返回的 channel 作为 [`Event`] 送达。

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{Mutex, Notify, RwLock, Semaphore, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_util::codec::FramedRead;

use crate::codec::CmppFrameCodec;
use crate::config::CmppConfig;
use crate::error::{Error, Result};
use crate::pdu::{Connect, Deliver, DeliverResp, Frame, Pdu, Submit, compute_authenticator_ismg};
use crate::submit::SubmitOptions;
use crate::types::{
    CODEC_INITIAL_CAPACITY, INCOMING_CHANNEL_CAPACITY, SEND_CHANNEL_CAPACITY,
    TIMEOUT_CHECK_IDLE_INTERVAL, TIMEOUT_CHECK_INTERVAL,
};

/// [`CmppConnection`] 产生的 async event。
///
/// 从 [`CmppConnection::take_events`] 返回的 receiver 消费这些 event。
/// 使用 [`CmppConnection::submit`] 返回的 `sequence_id` 关联 `SubmitResp`/`SubmitTimeout`。
#[derive(Debug)]
pub enum Event {
    /// 收到先前已提交 segment 对应的 SUBMIT_RESP。
    SubmitResp {
        /// 当前 response 对应 SUBMIT 的 Sequence id。
        sequence_id: u32,
        /// ISMG 分配的 Message id（8 bytes）。
        msg_id: [u8; 8],
        /// Result code（0 = success）。
        result: u8,
    },
    /// 已提交的 segment 在 retry budget 内始终未收到 response。
    SubmitTimeout {
        /// timeout 的 SUBMIT 的 Sequence id。
        sequence_id: u32,
    },
    /// 入站 DELIVER：status report（见 [`Deliver::report`]）或 MO。
    Deliver(Deliver),
    /// connection 已拆除，后续不会再有 event。
    Disconnected(Error),
}

impl Event {
    /// 8-byte message id 的小写 hex 表示（`SubmitResp` helper）。
    pub fn msg_id_hex(msg_id: &[u8; 8]) -> String {
        msg_id.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

/// 正在等待 response 的 SUBMIT，用于 sliding-window 计数和重传跟踪。
struct PendingSubmit {
    packet: Bytes,
    retry_count: u32,
    last_send_time: Instant,
}

/// `Arc` 后面的共享 connection state。
struct Inner {
    seq_generator: AtomicU32,
    tx: mpsc::Sender<Bytes>,
    events_tx: mpsc::Sender<Event>,
    pending_submits: RwLock<HashMap<u32, PendingSubmit>>,
    window_semaphore: Semaphore,
    heartbeat_pending: RwLock<HashMap<u32, (Instant, u32)>>,
    is_closed: AtomicBool,
    writer_shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,
    reader_shutdown: Notify,
    background_tasks: Mutex<Vec<JoinHandle<()>>>,
    response_timeout: Duration,
    retry_count: u32,
}

impl Inner {
    fn next_seq_id(&self) -> u32 {
        let seq = self.seq_generator.fetch_add(1, Ordering::SeqCst);
        if seq == 0 {
            self.seq_generator
                .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
                .ok();
            1
        } else {
            seq
        }
    }

    /// 确保自增 sequence generator 始终高于手动分配的 id。
    fn bump_seq_generator(&self, used: u32) {
        loop {
            let current = self.seq_generator.load(Ordering::SeqCst);
            let floor = used.saturating_add(1).max(1);
            if current >= floor {
                break;
            }
            if self
                .seq_generator
                .compare_exchange_weak(current, floor, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }
    }

    /// 丢弃所有 pending SUBMIT（释放 window permits）并标记为 closed。
    async fn fail_all_pending(&self) {
        self.is_closed.store(true, Ordering::SeqCst);
        let mut map = self.pending_submits.write().await;
        let n = map.len();
        map.clear();
        if n > 0 {
            self.window_semaphore.add_permits(n);
        }
    }
}

/// Async CMPP 2.0 client connection。clone 成本低（通过 `Arc` 共享 state）。
#[derive(Clone)]
pub struct CmppConnection {
    inner: Arc<Inner>,
    events_rx: Arc<Mutex<Option<mpsc::Receiver<Event>>>>,
}

impl CmppConnection {
    /// 连接到 ISMG 并完成 CMPP 登录 handshake。
    ///
    /// 只有成功收到 `CMPP_CONNECT_RESP` 后才返回；status 非零时返回 [`Error::Auth`]，
    /// 并且（除非在 config 中禁用）会校验 `AuthenticatorISMG`。
    pub async fn connect(config: CmppConfig) -> Result<CmppConnection> {
        config.validate().map_err(Error::Config)?;

        let params = config.protocol_params.clone();
        let stream = setup_tcp(&config).await?;
        let (read_half, write_half) = stream.into_split();
        let mut framed =
            FramedRead::with_capacity(read_half, CmppFrameCodec, CODEC_INITIAL_CAPACITY);
        let mut write_half = write_half;

        // --- handshake ---
        let connect = Connect::new(&config.account, &config.password, config.version);
        let auth_source = connect.authenticator_source;
        let connect_seq = 1u32;

        log::info!(
            "CMPP CONNECT: target={}:{}, account='{}'（len={}）, password_len={}, version=0x{:02x}, \
             timestamp={}, verify_authenticator={}",
            config.host,
            config.port,
            config.account,
            config.account.len(),
            config.password.len(),
            config.version,
            connect.timestamp,
            params.verify_authenticator
        );
        log::debug!(
            "CMPP CONNECT auth 详情: timestamp_str='{:010}', authenticator_source={}, password_hex={}",
            connect.timestamp,
            hex_bytes(&auth_source),
            hex_bytes(config.password.as_bytes())
        );

        write_half
            .write_all(&Pdu::Connect(connect).encode(connect_seq))
            .await?;

        let resp = tokio::time::timeout(Duration::from_secs(params.connect_timeout), framed.next())
            .await
            .map_err(|_| Error::Connect("CONNECT_RESP 超时".into()))?;
        let frame = match resp {
            Some(Ok(f)) => f,
            Some(Err(e)) => return Err(e),
            None => return Err(Error::Connect("handshake 期间 connection 已关闭".into())),
        };
        let cr = match frame.pdu {
            Pdu::ConnectResp(cr) => cr,
            other => {
                return Err(Error::Connect(format!(
                    "期望 CONNECT_RESP，实际收到 command {:#010x}",
                    other.command_id()
                )));
            }
        };
        log::info!(
            "CMPP CONNECT_RESP: status={}, version=0x{:02x}, authenticator_ismg={}",
            cr.status,
            cr.version,
            hex_bytes(&cr.authenticator_ismg)
        );

        if cr.status != 0 {
            log::warn!(
                "CMPP 登录被拒绝: status={}, account='{}', host={}:{}",
                cr.status,
                config.account,
                config.host,
                config.port
            );
            return Err(Error::Auth(cr.status));
        }
        if params.verify_authenticator {
            let expected = compute_authenticator_ismg(cr.status, &auth_source, &config.password);
            if cr.authenticator_ismg != expected {
                log::error!(
                    "AuthenticatorISMG 校验失败: account='{}', host={}:{}, \
                     received={}, expected={}, ismg_all_zero={}",
                    config.account,
                    config.host,
                    config.port,
                    hex_bytes(&cr.authenticator_ismg),
                    hex_bytes(&expected),
                    cr.authenticator_ismg.iter().all(|&b| b == 0)
                );
                if cr.authenticator_ismg.iter().all(|&b| b == 0) {
                    log::error!(
                        "网关返回的 AuthenticatorISMG 全为 0，常见于未实现该字段的 ISMG；\
                         可将 verify_authenticator 设为 false 跳过校验"
                    );
                }
                return Err(Error::AuthenticatorMismatch);
            }
        } else {
            log::info!("已跳过 AuthenticatorISMG 校验 (verify_authenticator=false)");
        }
        log::info!("CMPP 登录成功: {}:{}", config.host, config.port);

        // --- 装配运行中的 connection ---
        let window_size = params.window_size;
        let (tx, rx) = mpsc::channel::<Bytes>(SEND_CHANNEL_CAPACITY);
        let (events_tx, events_rx) = mpsc::channel::<Event>(INCOMING_CHANNEL_CAPACITY);
        let (writer_shutdown_tx, writer_shutdown_rx) = oneshot::channel();

        let inner = Arc::new(Inner {
            seq_generator: AtomicU32::new(2), // seq 1 已由 CONNECT 使用
            tx,
            events_tx,
            pending_submits: RwLock::new(HashMap::new()),
            window_semaphore: Semaphore::new(window_size),
            heartbeat_pending: RwLock::new(HashMap::new()),
            is_closed: AtomicBool::new(false),
            writer_shutdown_tx: Mutex::new(Some(writer_shutdown_tx)),
            reader_shutdown: Notify::new(),
            background_tasks: Mutex::new(Vec::new()),
            response_timeout: Duration::from_secs(params.response_timeout),
            retry_count: params.retry_count,
        });

        let writer = tokio::spawn(writer_task(write_half, rx, writer_shutdown_rx));
        let reader = tokio::spawn(reader_task(
            inner.clone(),
            framed,
            Duration::from_secs(params.read_idle_timeout),
        ));
        let heartbeat = tokio::spawn(heartbeat_task(
            inner.clone(),
            Duration::from_secs(params.heartbeat_interval),
        ));
        let timeout = tokio::spawn(timeout_task(inner.clone()));
        {
            let mut tasks = inner.background_tasks.lock().await;
            tasks.extend([writer, reader, heartbeat, timeout]);
        }

        Ok(CmppConnection {
            inner,
            events_rx: Arc::new(Mutex::new(Some(events_rx))),
        })
    }

    /// 取走 event receiver。只有首次调用会返回 `Some`。
    pub async fn take_events(&self) -> Option<mpsc::Receiver<Event>> {
        self.events_rx.lock().await.take()
    }

    /// connection 是否已关闭。
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed.load(Ordering::SeqCst)
    }

    /// Submit message，并为每个 SMS segment 返回一个 sequence id。
    ///
    /// Non-blocking：调用只会在 sliding-window backpressure（window 已满）时等待，
    /// 随后立即返回。对应的 `SUBMIT_RESP` 会以 async 形式作为 [`Event::SubmitResp`]
    /// 到达（或到达 [`Event::SubmitTimeout`]）。内容会自动 encode 并拆分（long SMS）。
    /// 当 `base_sequence_id` 为 `Some` 时，long SMS segment 会使用从该值开始的连续
    /// sequence id；否则由内部自动分配 sequence id。
    pub async fn submit(
        &self,
        options: &SubmitOptions,
        content: &str,
        base_sequence_id: Option<u32>,
    ) -> Result<Vec<u32>> {
        if self.is_closed() {
            return Err(Error::Closed);
        }

        let submits = options.build_submits(content);
        let mut seq_ids = Vec::with_capacity(submits.len());

        for (i, submit) in submits.into_iter().enumerate() {
            let seq = match base_sequence_id {
                Some(base) => {
                    let seq = base.wrapping_add(i as u32);
                    self.inner.bump_seq_generator(seq);
                    seq
                }
                None => self.inner.next_seq_id(),
            };
            self.send_submit(seq, submit).await?;
            seq_ids.push(seq);
        }

        Ok(seq_ids)
    }

    async fn send_submit(&self, sequence_id: u32, submit: Submit) -> Result<()> {
        let permit = self
            .inner
            .window_semaphore
            .acquire()
            .await
            .map_err(|_| Error::Closed)?;
        permit.forget();

        let bytes = Pdu::Submit(Box::new(submit)).encode(sequence_id);
        {
            let mut pending = self.inner.pending_submits.write().await;
            pending.insert(
                sequence_id,
                PendingSubmit {
                    packet: bytes.clone(),
                    retry_count: 0,
                    last_send_time: Instant::now(),
                },
            );
        }

        if self.inner.tx.send(bytes).await.is_err() {
            self.inner
                .pending_submits
                .write()
                .await
                .remove(&sequence_id);
            self.inner.window_semaphore.add_permits(1);
            return Err(Error::ChannelClosed);
        }
        Ok(())
    }

    /// 优雅关闭 connection：发送 CMPP_TERMINATE，然后拆除。
    pub async fn close(&self) {
        if self.inner.is_closed.swap(true, Ordering::SeqCst) {
            return;
        }
        log::info!("正在关闭 CMPP connection");

        // 尽力执行优雅 TERMINATE。
        let term_seq = self.inner.next_seq_id();
        if self
            .inner
            .tx
            .send(Pdu::Terminate.encode(term_seq))
            .await
            .is_ok()
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // 通知 writer 和 reader 停止。
        if let Some(tx) = self.inner.writer_shutdown_tx.lock().await.take() {
            let _ = tx.send(());
        }
        self.inner.reader_shutdown.notify_one();

        // 释放所有 pending submitter。
        self.inner.fail_all_pending().await;

        // 停止 background tasks。
        let handles: Vec<JoinHandle<()>> =
            std::mem::take(&mut *self.inner.background_tasks.lock().await);
        for h in handles {
            h.abort();
        }
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// 建立 TCP connection 并应用 socket options。
async fn setup_tcp(config: &CmppConfig) -> Result<TcpStream> {
    let addr = format!("{}:{}", config.host, config.port);
    let connect_timeout = Duration::from_secs(config.protocol_params.connect_timeout);

    let stream = tokio::time::timeout(connect_timeout, TcpStream::connect(&addr))
        .await
        .map_err(|_| {
            Error::Connect(format!(
                "connection 在 {}s 后超时",
                config.protocol_params.connect_timeout
            ))
        })?
        .map_err(|e| Error::Connect(format!("连接到 {} 失败: {}", addr, e)))?;

    if let Err(e) = stream.set_nodelay(true) {
        log::warn!("设置 TCP_NODELAY 失败: {}（继续运行）", e);
    }
    configure_keepalive(&stream, Duration::from_secs(60));
    log::info!("TCP 已连接: {}", addr);
    Ok(stream)
}

/// 通过 `socket2` 实现跨平台 TCP keepalive。
fn configure_keepalive(stream: &TcpStream, idle: Duration) {
    let sock = socket2::SockRef::from(stream);
    let ka = socket2::TcpKeepalive::new().with_time(idle);
    if let Err(e) = sock.set_tcp_keepalive(&ka) {
        log::warn!("设置 TCP keepalive 失败: {}（继续运行）", e);
    }
}

/// 将 send channel 中的数据写入 socket；shutdown 或 write error 时停止。
async fn writer_task(
    mut writer: OwnedWriteHalf,
    mut rx: mpsc::Receiver<Bytes>,
    mut shutdown: oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            _ = &mut shutdown => {
                let _ = writer.shutdown().await;
                break;
            }
            pkt = rx.recv() => match pkt {
                Some(bytes) => {
                    if let Err(e) = writer.write_all(&bytes).await {
                        log::warn!("CMPP write 错误: {}", e);
                        break;
                    }
                }
                None => break,
            }
        }
    }
    log::debug!("writer task 已退出");
}

/// 读取 frame，分发为 event，并自动回复 liveness/teardown PDU。
async fn reader_task(
    inner: Arc<Inner>,
    mut framed: FramedRead<OwnedReadHalf, CmppFrameCodec>,
    read_idle: Duration,
) {
    let reason: Error = loop {
        let frame = tokio::select! {
            _ = inner.reader_shutdown.notified() => break Error::Closed,
            res = tokio::time::timeout(read_idle, framed.next()) => match res {
                Ok(Some(Ok(frame))) => frame,
                Ok(Some(Err(e))) => { log::warn!("CMPP decode 错误: {}", e); break e; }
                Ok(None) => { log::info!("CMPP connection 已由 peer 关闭"); break Error::Closed; }
                Err(_) => { log::warn!("CMPP read idle timeout（{}s）", read_idle.as_secs()); break Error::Timeout; }
            }
        };

        let Frame { sequence_id, pdu } = frame;
        match pdu {
            Pdu::SubmitResp(resp) => {
                let pending = {
                    let mut map = inner.pending_submits.write().await;
                    let removed = map.remove(&sequence_id);
                    if removed.is_some() {
                        inner.window_semaphore.add_permits(1);
                    }
                    removed
                };
                if pending.is_some() {
                    let _ = inner
                        .events_tx
                        .send(Event::SubmitResp {
                            sequence_id,
                            msg_id: resp.msg_id,
                            result: resp.result,
                        })
                        .await;
                } else {
                    log::debug!("收到未知 seq_id={} 的 SUBMIT_RESP", sequence_id);
                }
            }
            Pdu::Deliver(deliver) => {
                let resp = Frame::new(
                    sequence_id,
                    Pdu::DeliverResp(DeliverResp {
                        msg_id: deliver.msg_id,
                        result: 0,
                    }),
                );
                if inner.tx.send(resp.encode()).await.is_err() {
                    break Error::ChannelClosed;
                }
                if inner.events_tx.send(Event::Deliver(deliver)).await.is_err() {
                    log::debug!("event receiver 已丢弃；忽略 DELIVER");
                }
            }
            Pdu::ActiveTest => {
                if inner
                    .tx
                    .send(Frame::new(sequence_id, Pdu::ActiveTestResp).encode())
                    .await
                    .is_err()
                {
                    break Error::ChannelClosed;
                }
            }
            Pdu::ActiveTestResp => {
                inner.heartbeat_pending.write().await.remove(&sequence_id);
            }
            Pdu::Terminate => {
                log::info!("peer 发送 CMPP_TERMINATE，正在拆除");
                let _ = inner
                    .tx
                    .send(Frame::new(sequence_id, Pdu::TerminateResp).encode())
                    .await;
                break Error::Terminated;
            }
            Pdu::TerminateResp => {
                log::debug!("收到 TERMINATE_RESP seq_id={}", sequence_id);
            }
            other => {
                log::warn!("收到非预期入站 PDU: {:#010x}", other.command_id());
            }
        }
    };

    inner.fail_all_pending().await;
    let _ = inner.events_tx.send(Event::Disconnected(reason)).await;
    log::debug!("reader task 已退出");
}

/// 在没有未完成 heartbeat 时周期性发送 ACTIVE_TEST。
async fn heartbeat_task(inner: Arc<Inner>, interval: Duration) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        if inner.is_closed.load(Ordering::SeqCst) {
            break;
        }
        let has_pending = !inner.heartbeat_pending.read().await.is_empty();
        if has_pending {
            continue;
        }
        let seq = inner.next_seq_id();
        if inner.tx.send(Pdu::ActiveTest.encode(seq)).await.is_ok() {
            inner
                .heartbeat_pending
                .write()
                .await
                .insert(seq, (Instant::now(), 0));
        } else {
            break;
        }
    }
    log::debug!("heartbeat task 已退出");
}

/// 重传 timed-out SUBMIT 和 heartbeat；heartbeat 耗尽时拆除连接。
async fn timeout_task(inner: Arc<Inner>) {
    let timeout = inner.response_timeout;
    let retry_count = inner.retry_count;
    let mut ticker = tokio::time::interval(TIMEOUT_CHECK_INTERVAL);

    loop {
        ticker.tick().await;
        if inner.is_closed.load(Ordering::SeqCst) {
            break;
        }
        let now = Instant::now();

        // Heartbeat timeout（持锁更新 state，释放后再发送）。
        let mut exhausted = false;
        let mut hb_retransmit: Vec<u32> = Vec::new();
        {
            let mut hb = inner.heartbeat_pending.write().await;
            let mut remove: Vec<u32> = Vec::new();
            for (seq, (sent, retry)) in hb.iter_mut() {
                if now.duration_since(*sent) >= timeout {
                    if *retry >= retry_count - 1 {
                        exhausted = true;
                        remove.push(*seq);
                    } else {
                        *retry += 1;
                        *sent = now;
                        hb_retransmit.push(*seq);
                    }
                }
            }
            for seq in remove {
                hb.remove(&seq);
            }
        }
        for seq in hb_retransmit {
            let _ = inner.tx.send(Pdu::ActiveTest.encode(seq)).await;
        }
        if exhausted {
            log::error!("heartbeat 已耗尽，正在拆除 connection");
            inner.reader_shutdown.notify_one();
            if let Some(tx) = inner.writer_shutdown_tx.lock().await.take() {
                let _ = tx.send(());
            }
            break;
        }

        // SUBMIT timeout。
        let (has_pending, retransmit, gave_up) = {
            let mut map = inner.pending_submits.write().await;
            let mut retransmit: Vec<(u32, Bytes)> = Vec::new();
            let mut gave_up: Vec<u32> = Vec::new();
            for (seq, p) in map.iter_mut() {
                if now.duration_since(p.last_send_time) >= timeout {
                    if p.retry_count >= retry_count - 1 {
                        gave_up.push(*seq);
                    } else {
                        p.retry_count += 1;
                        p.last_send_time = now;
                        retransmit.push((*seq, p.packet.clone()));
                    }
                }
            }
            for seq in &gave_up {
                map.remove(seq);
                inner.window_semaphore.add_permits(1);
            }
            let has_pending = !map.is_empty();
            (has_pending, retransmit, gave_up)
        };
        for (seq, packet) in retransmit {
            if inner.tx.send(packet).await.is_ok() {
                log::debug!("正在重传 SUBMIT seq_id={}", seq);
            }
        }
        for seq in gave_up {
            log::warn!("SUBMIT timeout，放弃重试: seq_id={}", seq);
            let _ = inner
                .events_tx
                .send(Event::SubmitTimeout { sequence_id: seq })
                .await;
        }

        ticker = if has_pending {
            tokio::time::interval(TIMEOUT_CHECK_INTERVAL)
        } else {
            tokio::time::interval(TIMEOUT_CHECK_IDLE_INTERVAL)
        };
    }
    log::debug!("timeout task 已退出");
}

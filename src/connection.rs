//! Async CMPP 2.0 client connection。
//!
//! [`CmppConnection::connect`] 会执行完整登录 handshake（并校验 ISMG authenticator）
//! 后再返回。[`CmppConnection::submit`] 是 non-blocking：它将 message 入队
//! （受 sliding-window backpressure 约束），并立即返回分配好的 sequence id，
//! 符合 CMPP async、pipelined 的特性。所有 response，包括 SUBMIT_RESP、入站
//! DELIVER（status reports / MO）、submit timeout 和连接断开，都会通过
//! [`CmppConnection::take_events`] 返回的 channel 作为 [`Event`] 送达。

use std::cmp::Reverse;
use std::collections::hash_map::RandomState;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::future::Future;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{Mutex, OwnedSemaphorePermit, RwLock, Semaphore, mpsc, oneshot, watch};
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
    TIMEOUT_CHECK_INTERVAL,
};

const EVENT_SPOOL_NORMAL_CAPACITY: usize = 256;
const EVENT_SPOOL_EMERGENCY_CAPACITY: usize = 2;
const CONTROL_CHANNEL_CAPACITY: usize = 64;
const CONTROL_BURST_LIMIT: usize = 16;
const MAX_SUBMIT_RETRY_SPREAD_TICKS: usize = 4;
const MAX_MANUAL_SEQUENCE_BATCHES: usize = 4;
const MAX_RETIRED_SEQUENCE_IDS: usize = 65_536;
const SUBMIT_TIMEOUT_LOG_SAMPLES: usize = 4;
const UDH_REFERENCE_BUCKET_COUNT: usize = 4096;
const DEFAULT_UDH_REFERENCE_COOLDOWN: Duration = Duration::from_secs(300);
const MAX_UDH_REFERENCE_COOLDOWN: Duration = Duration::from_secs(24 * 60 * 60);

static UDH_REFERENCE_POOL: LazyLock<UdhReferencePool> = LazyLock::new(UdhReferencePool::new);

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

enum EventSpoolItem {
    Event {
        event_rx: oneshot::Receiver<Event>,
        _depth_permit: EventDepthPermit,
    },
    Terminal {
        reason: Option<Error>,
        _depth_permit: EventDepthPermit,
    },
}

struct EventDepthPermit {
    depth: Arc<AtomicUsize>,
}

impl EventDepthPermit {
    fn new(depth: Arc<AtomicUsize>) -> Self {
        depth.fetch_add(1, Ordering::SeqCst);
        EventDepthPermit { depth }
    }
}

impl Drop for EventDepthPermit {
    fn drop(&mut self) {
        self.depth.fetch_sub(1, Ordering::SeqCst);
    }
}

struct EventTicketTracker {
    pending_tx: watch::Sender<usize>,
}

struct EventTicketGuard {
    tracker: Arc<EventTicketTracker>,
}

impl EventTicketGuard {
    fn new(tracker: Arc<EventTicketTracker>) -> Self {
        tracker.pending_tx.send_modify(|pending| *pending += 1);
        EventTicketGuard { tracker }
    }
}

impl Drop for EventTicketGuard {
    fn drop(&mut self) {
        self.tracker.pending_tx.send_modify(|pending| *pending -= 1);
    }
}

struct EventTicket {
    event_tx: Option<oneshot::Sender<Event>>,
    _guard: Option<EventTicketGuard>,
}

#[derive(Default)]
struct UdhReferenceBucket {
    active: [u64; 4],
    cooling: [u64; 4],
    cooldowns: BinaryHeap<Reverse<(Instant, u8)>>,
}

struct UdhReferencePool {
    state: StdMutex<Vec<UdhReferenceBucket>>,
    hash_builder: RandomState,
}

struct UdhReferenceLease {
    bucket_ids: Box<[usize]>,
    reference: u8,
    cooldown: Duration,
    exposed: AtomicBool,
}

impl UdhReferencePool {
    fn new() -> Self {
        UdhReferencePool {
            state: StdMutex::new(
                std::iter::repeat_with(UdhReferenceBucket::default)
                    .take(UDH_REFERENCE_BUCKET_COUNT)
                    .collect(),
            ),
            hash_builder: RandomState::new(),
        }
    }

    fn bucket_ids(&self, src_id: &str, destinations: &[String]) -> Box<[usize]> {
        let source = normalize_udh_address(src_id);
        let mut bucket_ids = Vec::with_capacity(destinations.len().max(1));
        if destinations.is_empty() {
            bucket_ids.push(self.bucket_id(&source, &[0; 21]));
        } else {
            bucket_ids.extend(
                destinations.iter().map(|destination| {
                    self.bucket_id(&source, &normalize_udh_address(destination))
                }),
            );
            bucket_ids.sort_unstable();
            bucket_ids.dedup();
        }
        bucket_ids.into_boxed_slice()
    }

    fn bucket_id(&self, source: &[u8; 21], destination: &[u8; 21]) -> usize {
        let mut hasher = self.hash_builder.build_hasher();
        source.hash(&mut hasher);
        destination.hash(&mut hasher);
        (hasher.finish() as usize) & (UDH_REFERENCE_BUCKET_COUNT - 1)
    }

    fn try_acquire(
        &'static self,
        bucket_ids: Box<[usize]>,
        preferred: u8,
        cooldown: Duration,
    ) -> Option<Arc<UdhReferenceLease>> {
        let now = Instant::now();
        let mut buckets = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for &bucket_id in bucket_ids.iter() {
            purge_udh_cooldowns(&mut buckets[bucket_id], now);
        }

        for offset in 0..=u8::MAX {
            let reference = preferred.wrapping_add(offset);
            if bucket_ids
                .iter()
                .all(|&bucket_id| !udh_reference_is_set(&buckets[bucket_id], reference))
            {
                for &bucket_id in bucket_ids.iter() {
                    set_udh_reference(&mut buckets[bucket_id].active, reference);
                }
                return Some(Arc::new(UdhReferenceLease {
                    bucket_ids,
                    reference,
                    cooldown,
                    exposed: AtomicBool::new(false),
                }));
            }
        }
        None
    }

    fn release(&self, lease: &UdhReferenceLease) {
        let now = Instant::now();
        let exposed = lease.exposed.load(Ordering::Acquire);
        let deadline = exposed.then(|| now.checked_add(lease.cooldown)).flatten();
        let mut buckets = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for &bucket_id in lease.bucket_ids.iter() {
            let bucket = &mut buckets[bucket_id];
            clear_udh_reference(&mut bucket.active, lease.reference);
            if exposed {
                set_udh_reference(&mut bucket.cooling, lease.reference);
                if let Some(deadline) = deadline {
                    bucket.cooldowns.push(Reverse((deadline, lease.reference)));
                }
            }
        }
        drop(buckets);
    }
}

impl UdhReferenceLease {
    fn reference(&self) -> u8 {
        self.reference
    }

    fn mark_exposed(&self) {
        self.exposed.store(true, Ordering::Release);
    }
}

impl Drop for UdhReferenceLease {
    fn drop(&mut self) {
        UDH_REFERENCE_POOL.release(self);
    }
}

fn normalize_udh_address(value: &str) -> [u8; 21] {
    let mut normalized = [0u8; 21];
    let bytes = value.as_bytes();
    let length = bytes.len().min(normalized.len());
    normalized[..length].copy_from_slice(&bytes[..length]);
    normalized
}

fn udh_reference_position(reference: u8) -> (usize, u64) {
    let index = reference as usize;
    (index / 64, 1u64 << (index % 64))
}

fn udh_reference_is_set(bucket: &UdhReferenceBucket, reference: u8) -> bool {
    let (word, mask) = udh_reference_position(reference);
    (bucket.active[word] | bucket.cooling[word]) & mask != 0
}

fn set_udh_reference(bitmap: &mut [u64; 4], reference: u8) {
    let (word, mask) = udh_reference_position(reference);
    bitmap[word] |= mask;
}

fn clear_udh_reference(bitmap: &mut [u64; 4], reference: u8) {
    let (word, mask) = udh_reference_position(reference);
    bitmap[word] &= !mask;
}

fn purge_udh_cooldowns(bucket: &mut UdhReferenceBucket, now: Instant) {
    while let Some(Reverse((deadline, reference))) = bucket.cooldowns.peek().copied() {
        if deadline > now {
            break;
        }
        bucket.cooldowns.pop();
        clear_udh_reference(&mut bucket.cooling, reference);
    }
}

struct SequenceState {
    next_automatic: Option<u32>,
    automatic_start: u32,
    automatic_used_through: Option<u32>,
    reserved: HashSet<u32>,
    retired: HashSet<u32>,
}

struct SequenceRegistry {
    state: StdMutex<SequenceState>,
}

impl SequenceRegistry {
    fn new(next: u32, capacity: usize) -> Self {
        let next = next.max(1);
        SequenceRegistry {
            state: StdMutex::new(SequenceState {
                next_automatic: Some(next),
                automatic_start: next,
                automatic_used_through: None,
                reserved: HashSet::with_capacity(capacity),
                retired: HashSet::new(),
            }),
        }
    }

    fn reserve_next(self: &Arc<Self>) -> Option<SequenceLease> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        loop {
            let sequence_id = state.next_automatic?;
            state.automatic_used_through = Some(sequence_id);
            state.next_automatic = sequence_id.checked_add(1);
            if state.retired.remove(&sequence_id) {
                continue;
            }
            if state.reserved.insert(sequence_id) {
                return Some(SequenceLease {
                    sequence_id,
                    registry: self.clone(),
                    manual: false,
                    release_on_drop: true,
                });
            }
        }
    }

    fn reserve_batch(self: &Arc<Self>, base: u32, count: usize) -> Option<Vec<SequenceLease>> {
        if count > u8::MAX as usize {
            return None;
        }
        let mut ids = Vec::with_capacity(count);
        let mut seen = HashSet::with_capacity(count);
        for index in 0..count {
            let sequence_id = base.wrapping_add(index as u32);
            if !seen.insert(sequence_id) {
                return None;
            }
            ids.push(sequence_id);
        }

        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if ids.iter().any(|id| {
            state
                .automatic_used_through
                .is_some_and(|used_through| *id >= state.automatic_start && *id <= used_through)
                || state.reserved.contains(id)
                || state.retired.contains(id)
        }) {
            return None;
        }
        state.reserved.extend(ids.iter().copied());
        Some(
            ids.into_iter()
                .map(|sequence_id| SequenceLease {
                    sequence_id,
                    registry: self.clone(),
                    manual: true,
                    release_on_drop: true,
                })
                .collect(),
        )
    }

    fn release(&self, sequence_id: u32) {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .reserved
            .remove(&sequence_id);
    }

    fn retire(&self, sequence_id: u32) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.automatic_used_through.is_some_and(|used_through| {
            sequence_id >= state.automatic_start && sequence_id <= used_through
        }) {
            state.reserved.remove(&sequence_id);
            return true;
        }
        if state.retired.contains(&sequence_id) {
            state.reserved.remove(&sequence_id);
            return true;
        }
        if state.retired.len() >= MAX_RETIRED_SEQUENCE_IDS {
            return false;
        }
        state.reserved.remove(&sequence_id);
        state.retired.insert(sequence_id);
        true
    }
}

struct SequenceLease {
    sequence_id: u32,
    registry: Arc<SequenceRegistry>,
    manual: bool,
    release_on_drop: bool,
}

impl SequenceLease {
    fn id(&self) -> u32 {
        self.sequence_id
    }

    /// 手动 sequence 上线或最终超时后立即隔离，避免迟到响应误配。
    fn retire(&mut self) -> bool {
        self.release_on_drop = false;
        self.registry.retire(self.sequence_id)
    }
}

impl Drop for SequenceLease {
    fn drop(&mut self) {
        if self.release_on_drop {
            self.registry.release(self.sequence_id);
        }
    }
}

impl EventTicket {
    fn publish(mut self, event: Event) -> bool {
        self.event_tx
            .take()
            .is_some_and(|event_tx| event_tx.send(event).is_ok())
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum EventAdmissionState {
    Open,
    Draining,
    Overflowed,
    Sealed,
    Terminal,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum EventReservationError {
    Overflowed,
    Closed,
}

/// 正在等待 response 的 SUBMIT，用于 sliding-window 计数和重传跟踪。
struct PendingSubmit {
    submission_id: u64,
    packet: Bytes,
    state: SubmitAttemptState,
    _udh_reference_lease: Option<Arc<UdhReferenceLease>>,
    _sequence_lease: SequenceLease,
    _window_permit: OwnedSemaphorePermit,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SubmitAttemptState {
    Queued { attempt: u32 },
    Writing { attempt: u32 },
    AwaitingResponse { attempt: u32, written_at: Instant },
    RespondedWhileWriting { attempt: u32 },
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct SubmitAttemptKey {
    sequence_id: u32,
    submission_id: u64,
    attempt: u32,
}

struct PendingHeartbeat {
    heartbeat_id: u64,
    state: HeartbeatAttemptState,
    _sequence_lease: SequenceLease,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum HeartbeatAttemptState {
    Queued { attempt: u32 },
    Writing { attempt: u32 },
    AwaitingResponse { attempt: u32, written_at: Instant },
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct HeartbeatAttemptKey {
    sequence_id: u32,
    heartbeat_id: u64,
    attempt: u32,
}

struct Outbound {
    packet: Bytes,
    response_budget_started_at: Option<Instant>,
    written_tx: Option<oneshot::Sender<()>>,
    written_flag: Option<Arc<AtomicBool>>,
    cancel_after_peer_terminate: bool,
    marks_peer_terminate_response: bool,
    open_only: bool,
    submit_drain_marker: bool,
    submit_attempt: Option<SubmitAttemptKey>,
    heartbeat_attempt: Option<HeartbeatAttemptKey>,
    event_after_write: Option<(EventTicket, Event)>,
}

impl Outbound {
    fn plain(packet: Bytes) -> Self {
        Outbound {
            packet,
            response_budget_started_at: None,
            written_tx: None,
            written_flag: None,
            cancel_after_peer_terminate: false,
            marks_peer_terminate_response: false,
            open_only: false,
            submit_drain_marker: false,
            submit_attempt: None,
            heartbeat_attempt: None,
            event_after_write: None,
        }
    }

    fn tracked(packet: Bytes) -> (Self, oneshot::Receiver<()>) {
        let (written_tx, written_rx) = oneshot::channel();
        (
            Outbound {
                packet,
                response_budget_started_at: None,
                written_tx: Some(written_tx),
                written_flag: None,
                cancel_after_peer_terminate: false,
                marks_peer_terminate_response: false,
                open_only: false,
                submit_drain_marker: false,
                submit_attempt: None,
                heartbeat_attempt: None,
                event_after_write: None,
            },
            written_rx,
        )
    }

    fn local_terminate(
        packet: Bytes,
        written_flag: Arc<AtomicBool>,
    ) -> (Self, oneshot::Receiver<()>) {
        let (mut outbound, written_rx) = Self::tracked(packet);
        outbound.written_flag = Some(written_flag);
        outbound.cancel_after_peer_terminate = true;
        (outbound, written_rx)
    }

    fn peer_terminate_response(packet: Bytes) -> (Self, oneshot::Receiver<()>) {
        let (mut outbound, written_rx) = Self::tracked(packet);
        outbound.marks_peer_terminate_response = true;
        (outbound, written_rx)
    }

    fn event_after_write(
        packet: Bytes,
        response_budget_started_at: Instant,
        ticket: EventTicket,
        event: Event,
    ) -> Self {
        let mut outbound = Self::plain(packet);
        outbound.response_budget_started_at = Some(response_budget_started_at);
        outbound.event_after_write = Some((ticket, event));
        outbound
    }

    fn submit(packet: Bytes, key: SubmitAttemptKey) -> Self {
        let mut outbound = Self::plain(packet);
        outbound.submit_attempt = Some(key);
        outbound
    }

    fn open_only(packet: Bytes) -> Self {
        let mut outbound = Self::plain(packet);
        outbound.open_only = true;
        outbound
    }

    fn heartbeat(packet: Bytes, key: HeartbeatAttemptKey) -> Self {
        let mut outbound = Self::open_only(packet);
        outbound.heartbeat_attempt = Some(key);
        outbound
    }

    fn submit_drain_marker() -> (Self, oneshot::Receiver<()>) {
        let (mut outbound, written_rx) = Self::tracked(Bytes::new());
        outbound.submit_drain_marker = true;
        (outbound, written_rx)
    }
}

struct PendingTerminate {
    sequence_id: u32,
    response_tx: oneshot::Sender<()>,
    written: Arc<AtomicBool>,
    _sequence_lease: SequenceLease,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum ConnectionPhase {
    Open = 0,
    Closing = 1,
    Closed = 2,
}

/// `Arc` 后面的共享 connection state。
struct Inner {
    sequence_registry: Arc<SequenceRegistry>,
    submission_id_generator: AtomicU64,
    heartbeat_id_generator: AtomicU64,
    submit_tx: mpsc::Sender<Outbound>,
    control_tx: mpsc::Sender<Outbound>,
    event_spool_tx: mpsc::Sender<EventSpoolItem>,
    event_admission: StdMutex<EventAdmissionState>,
    event_depth: Arc<AtomicUsize>,
    event_tickets: Arc<EventTicketTracker>,
    event_overflowed: AtomicBool,
    terminal_reason: StdMutex<Option<Error>>,
    pending_submits: RwLock<HashMap<u32, PendingSubmit>>,
    window_semaphore: Arc<Semaphore>,
    manual_batch_semaphore: Arc<Semaphore>,
    submit_admission: StdMutex<()>,
    heartbeat_pending: RwLock<HashMap<u32, PendingHeartbeat>>,
    phase: AtomicU8,
    phase_tx: watch::Sender<ConnectionPhase>,
    external_handles: AtomicUsize,
    close_driver_started: AtomicBool,
    takeover_started: AtomicBool,
    drain_submits_on_close: AtomicBool,
    close_complete_tx: watch::Sender<bool>,
    cleanup_complete_tx: watch::Sender<bool>,
    workers_remaining: AtomicUsize,
    workers_complete_tx: watch::Sender<bool>,
    pending_terminate: Mutex<Option<PendingTerminate>>,
    peer_terminate_seen: AtomicBool,
    peer_terminate_response_written: AtomicBool,
    runtime_handle: tokio::runtime::Handle,
    udh_reference_cooldown: Duration,
    response_timeout: Duration,
    retry_count: u32,
    window_size: usize,
    submit_retry_batch_size: usize,
}

impl Inner {
    fn next_submission_id(&self) -> u64 {
        self.submission_id_generator.fetch_add(1, Ordering::Relaxed)
    }

    fn next_heartbeat_id(&self) -> u64 {
        self.heartbeat_id_generator.fetch_add(1, Ordering::Relaxed)
    }

    fn phase(&self) -> ConnectionPhase {
        match self.phase.load(Ordering::SeqCst) {
            0 => ConnectionPhase::Open,
            1 => ConnectionPhase::Closing,
            _ => ConnectionPhase::Closed,
        }
    }

    fn begin_closing(&self, drain_submits: bool) -> bool {
        let _admission = self
            .submit_admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let phase = self.phase();
        if phase != ConnectionPhase::Open {
            if phase == ConnectionPhase::Closing && !drain_submits {
                self.drain_submits_on_close.store(false, Ordering::SeqCst);
            }
            return false;
        }
        self.drain_submits_on_close
            .store(drain_submits, Ordering::SeqCst);
        self.phase
            .store(ConnectionPhase::Closing as u8, Ordering::SeqCst);
        self.phase_tx.send_if_modified(|phase| {
            if *phase == ConnectionPhase::Open {
                *phase = ConnectionPhase::Closing;
                true
            } else {
                false
            }
        });
        true
    }

    fn transition_to_closed(&self) -> bool {
        let _admission = self
            .submit_admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self
            .phase
            .swap(ConnectionPhase::Closed as u8, Ordering::SeqCst)
            == ConnectionPhase::Closed as u8
        {
            return false;
        }
        self.phase_tx.send_replace(ConnectionPhase::Closed);
        true
    }

    /// 丢弃所有 pending SUBMIT；其 owned permit 会随 entry 一同释放。
    async fn fail_all_pending(&self) {
        self.pending_submits.write().await.clear();
    }

    /// 领取一个已入队的 SUBMIT attempt。领取后必须完整写完该 frame，避免半包。
    async fn claim_submit_attempt(&self, key: SubmitAttemptKey) -> bool {
        let mut pending = self.pending_submits.write().await;
        let _admission = self
            .submit_admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let phase = self.phase();
        if phase != ConnectionPhase::Open
            && !(phase == ConnectionPhase::Closing
                && self.drain_submits_on_close.load(Ordering::SeqCst))
        {
            return false;
        }
        let Some(entry) = pending.get_mut(&key.sequence_id) else {
            return false;
        };
        if entry.submission_id != key.submission_id
            || !matches!(
                entry.state,
                SubmitAttemptState::Queued { attempt } if attempt == key.attempt
            )
        {
            return false;
        }
        entry.state = SubmitAttemptState::Writing {
            attempt: key.attempt,
        };
        true
    }

    /// 完整写出后才启动 response timeout；若写入期间已收到响应，则在这里释放窗口。
    async fn complete_submit_attempt(&self, key: SubmitAttemptKey, written_at: Instant) {
        let mut pending = self.pending_submits.write().await;
        let should_remove = pending.get_mut(&key.sequence_id).is_some_and(|entry| {
            if entry.submission_id != key.submission_id {
                return false;
            }
            match entry.state {
                SubmitAttemptState::Writing { attempt } if attempt == key.attempt => {
                    entry.state = SubmitAttemptState::AwaitingResponse {
                        attempt: key.attempt,
                        written_at,
                    };
                    false
                }
                SubmitAttemptState::RespondedWhileWriting { attempt } if attempt == key.attempt => {
                    true
                }
                _ => false,
            }
        });
        if should_remove {
            pending.remove(&key.sequence_id);
        }
    }

    async fn claim_heartbeat_attempt(&self, key: HeartbeatAttemptKey) -> bool {
        let mut pending = self.heartbeat_pending.write().await;
        let _admission = self
            .submit_admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.phase() != ConnectionPhase::Open {
            return false;
        }
        let Some(entry) = pending.get_mut(&key.sequence_id) else {
            return false;
        };
        if entry.heartbeat_id != key.heartbeat_id
            || !matches!(
                entry.state,
                HeartbeatAttemptState::Queued { attempt } if attempt == key.attempt
            )
        {
            return false;
        }
        entry.state = HeartbeatAttemptState::Writing {
            attempt: key.attempt,
        };
        true
    }

    async fn complete_heartbeat_attempt(&self, key: HeartbeatAttemptKey, written_at: Instant) {
        let mut pending = self.heartbeat_pending.write().await;
        let Some(entry) = pending.get_mut(&key.sequence_id) else {
            return;
        };
        if entry.heartbeat_id == key.heartbeat_id
            && matches!(
                entry.state,
                HeartbeatAttemptState::Writing { attempt } if attempt == key.attempt
            )
        {
            entry.state = HeartbeatAttemptState::AwaitingResponse {
                attempt: key.attempt,
                written_at,
            };
        }
    }

    fn make_event_ticket(&self, track_until_publish: bool) -> std::result::Result<EventTicket, ()> {
        let (event_tx, event_rx) = oneshot::channel();
        let guard = track_until_publish.then(|| EventTicketGuard::new(self.event_tickets.clone()));
        let item = EventSpoolItem::Event {
            event_rx,
            _depth_permit: EventDepthPermit::new(self.event_depth.clone()),
        };
        if self.event_spool_tx.try_send(item).is_err() {
            return Err(());
        }
        Ok(EventTicket {
            event_tx: Some(event_tx),
            _guard: guard,
        })
    }

    fn reserve_event(
        self: &Arc<Self>,
        preserve_on_overflow: bool,
        track_until_publish: bool,
    ) -> std::result::Result<EventTicket, EventReservationError> {
        let mut start_overload_close = false;
        let reservation = {
            let mut admission = self
                .event_admission
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match *admission {
                EventAdmissionState::Terminal | EventAdmissionState::Sealed => {
                    Err(EventReservationError::Closed)
                }
                EventAdmissionState::Overflowed => Err(EventReservationError::Overflowed),
                EventAdmissionState::Open | EventAdmissionState::Draining => {
                    if self.event_depth.load(Ordering::SeqCst) < EVENT_SPOOL_NORMAL_CAPACITY {
                        match self.make_event_ticket(track_until_publish) {
                            Ok(ticket) => Ok(ticket),
                            Err(()) => {
                                *admission = EventAdmissionState::Overflowed;
                                self.event_overflowed.store(true, Ordering::Release);
                                start_overload_close = true;
                                Err(EventReservationError::Overflowed)
                            }
                        }
                    } else {
                        *admission = EventAdmissionState::Overflowed;
                        self.event_overflowed.store(true, Ordering::Release);
                        start_overload_close = true;
                        if preserve_on_overflow {
                            self.make_event_ticket(track_until_publish)
                                .map_err(|()| EventReservationError::Overflowed)
                        } else {
                            Err(EventReservationError::Overflowed)
                        }
                    }
                }
            }
        };

        if start_overload_close {
            self.start_event_overload_close();
        }
        reservation
    }

    fn emit_event(self: &Arc<Self>, event: Event) -> bool {
        match self.reserve_event(true, false) {
            Ok(ticket) => ticket.publish(event),
            Err(_) => false,
        }
    }

    fn begin_event_drain(&self) {
        let mut admission = self
            .event_admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if *admission == EventAdmissionState::Open {
            *admission = EventAdmissionState::Draining;
        }
    }

    fn seal_event_admission_if_idle(&self) -> bool {
        let mut admission = self
            .event_admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if matches!(
            *admission,
            EventAdmissionState::Sealed | EventAdmissionState::Terminal
        ) {
            return true;
        }
        if *self.event_tickets.pending_tx.borrow() != 0 {
            return false;
        }
        *admission = EventAdmissionState::Sealed;
        true
    }

    fn seal_event_admission(&self) {
        let mut admission = self
            .event_admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if *admission != EventAdmissionState::Terminal {
            *admission = EventAdmissionState::Sealed;
        }
    }

    fn start_event_overload_close(self: &Arc<Self>) {
        log::error!(
            "CMPP event backlog 已达到上限 {}，正在有界关闭 connection",
            EVENT_SPOOL_NORMAL_CAPACITY
        );
        if !self.begin_closing(false) {
            return;
        }

        let inner = self.clone();
        let close_task = async move {
            drain_event_tickets(&inner, "event backlog 关闭").await;
            inner.finish(Some(Error::ChannelClosed));
        };
        if let Ok(runtime_handle) = tokio::runtime::Handle::try_current() {
            runtime_handle.spawn(close_task);
        } else {
            self.runtime_handle.spawn(close_task);
        }
    }

    fn close_event_spool(&self) {
        let mut admission = self
            .event_admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if *admission == EventAdmissionState::Terminal {
            return;
        }
        *admission = EventAdmissionState::Terminal;
        let reason = self
            .terminal_reason
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        let item = EventSpoolItem::Terminal {
            reason,
            _depth_permit: EventDepthPermit::new(self.event_depth.clone()),
        };
        if self.event_spool_tx.try_send(item).is_err() {
            log::error!("CMPP event dispatcher 已退出，无法发布 connection 终态");
        }
    }

    /// 统一完成 connection 终止；清理在 detached task 中继续，避免调用方取消留下半终态。
    fn finish(self: &Arc<Self>, reason: Option<Error>) {
        let reason = if self.peer_terminate_seen.load(Ordering::SeqCst) {
            Some(Error::Terminated)
        } else if self.event_overflowed.load(Ordering::Acquire) {
            Some(Error::ChannelClosed)
        } else {
            reason
        };
        let mut terminal_reason = self
            .terminal_reason
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.phase() == ConnectionPhase::Closed {
            return;
        }
        *terminal_reason = reason;
        if !self.transition_to_closed() {
            return;
        }
        drop(terminal_reason);
        self.drain_submits_on_close.store(false, Ordering::SeqCst);
        self.window_semaphore.close();
        self.manual_batch_semaphore.close();
        let inner = self.clone();
        let cleanup = async move {
            inner.fail_all_pending().await;
            inner.heartbeat_pending.write().await.clear();
            inner.pending_terminate.lock().await.take();
            inner.close_event_spool();
            inner.cleanup_complete_tx.send_replace(true);
        };
        if let Ok(runtime_handle) = tokio::runtime::Handle::try_current() {
            runtime_handle.spawn(cleanup);
        } else {
            self.runtime_handle.spawn(cleanup);
        }
    }
}

struct WorkerGuard {
    inner: Arc<Inner>,
}

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        if self.inner.workers_remaining.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.inner.workers_complete_tx.send_replace(true);
        }
    }
}

struct TakeoverGuard {
    inner: Arc<Inner>,
}

impl Drop for TakeoverGuard {
    fn drop(&mut self) {
        self.inner.takeover_started.store(false, Ordering::SeqCst);
    }
}

fn spawn_worker<F>(inner: Arc<Inner>, future: F) -> JoinHandle<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let guard = WorkerGuard { inner };
    tokio::spawn(async move {
        let _guard = guard;
        future.await;
    })
}

/// Async CMPP 2.0 client connection。clone 成本低（通过 `Arc` 共享 state）。
pub struct CmppConnection {
    inner: Arc<Inner>,
    events_rx: Arc<Mutex<Option<mpsc::Receiver<Event>>>>,
    background_tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl Clone for CmppConnection {
    fn clone(&self) -> Self {
        self.inner.external_handles.fetch_add(1, Ordering::SeqCst);
        CmppConnection {
            inner: self.inner.clone(),
            events_rx: self.events_rx.clone(),
            background_tasks: self.background_tasks.clone(),
        }
    }
}

impl Drop for CmppConnection {
    fn drop(&mut self) {
        if self.inner.external_handles.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.inner.finish(None);
        }
    }
}

impl CmppConnection {
    /// 连接到 ISMG 并完成 CMPP 登录 handshake。
    ///
    /// 只有成功收到 `CMPP_CONNECT_RESP` 后才返回；status 非零时返回 [`Error::Auth`]，
    /// 并且（除非在 config 中禁用）会校验 `AuthenticatorISMG`。
    pub async fn connect(config: CmppConfig) -> Result<CmppConnection> {
        Self::connect_with_udh_reference_cooldown(config, DEFAULT_UDH_REFERENCE_COOLDOWN).await
    }

    /// 连接到 ISMG，并指定已上线长短信的 8-bit UDH reference 隔离时间。
    ///
    /// cooldown 期间，同一实际 `Src_Id + Dest_Terminal_Id` 不会复用该 reference，
    /// 避免网关或终端将迟到分片与新长短信错误重组。
    pub async fn connect_with_udh_reference_cooldown(
        config: CmppConfig,
        udh_reference_cooldown: Duration,
    ) -> Result<CmppConnection> {
        config.validate().map_err(Error::Config)?;
        if udh_reference_cooldown.is_zero() || udh_reference_cooldown > MAX_UDH_REFERENCE_COOLDOWN {
            return Err(Error::Config(
                "UDH reference cooldown 必须在 1ns 到 24h 之间".to_string(),
            ));
        }

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

        tokio::time::timeout(
            Duration::from_secs(params.connect_timeout),
            write_half.write_all(&Pdu::Connect(connect).encode(connect_seq)),
        )
        .await
        .map_err(|_| Error::Connect("CMPP CONNECT write 超时".into()))??;

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
        let submit_retry_batch_size = window_size
            .div_ceil(MAX_SUBMIT_RETRY_SPREAD_TICKS)
            .max(1)
            .min(window_size);
        let (submit_tx, submit_rx) = mpsc::channel::<Outbound>(SEND_CHANNEL_CAPACITY);
        let (control_tx, control_rx) = mpsc::channel::<Outbound>(CONTROL_CHANNEL_CAPACITY);
        let (event_spool_tx, event_spool_rx) = mpsc::channel::<EventSpoolItem>(
            EVENT_SPOOL_NORMAL_CAPACITY + EVENT_SPOOL_EMERGENCY_CAPACITY,
        );
        let (events_tx, events_rx) = mpsc::channel::<Event>(INCOMING_CHANNEL_CAPACITY);
        let (phase_tx, _) = watch::channel(ConnectionPhase::Open);
        let (close_complete_tx, _) = watch::channel(false);
        let (cleanup_complete_tx, _) = watch::channel(false);
        let (workers_complete_tx, _) = watch::channel(false);
        let (event_tickets_pending_tx, _) = watch::channel(0usize);
        let event_depth = Arc::new(AtomicUsize::new(0));
        let event_tickets = Arc::new(EventTicketTracker {
            pending_tx: event_tickets_pending_tx,
        });

        let inner = Arc::new(Inner {
            sequence_registry: Arc::new(SequenceRegistry::new(2, window_size + 2)),
            submission_id_generator: AtomicU64::new(1),
            heartbeat_id_generator: AtomicU64::new(1),
            submit_tx,
            control_tx,
            event_spool_tx,
            event_admission: StdMutex::new(EventAdmissionState::Open),
            event_depth,
            event_tickets,
            event_overflowed: AtomicBool::new(false),
            terminal_reason: StdMutex::new(None),
            pending_submits: RwLock::new(HashMap::with_capacity(window_size)),
            window_semaphore: Arc::new(Semaphore::new(window_size)),
            manual_batch_semaphore: Arc::new(Semaphore::new(
                window_size.min(MAX_MANUAL_SEQUENCE_BATCHES),
            )),
            submit_admission: StdMutex::new(()),
            heartbeat_pending: RwLock::new(HashMap::with_capacity(1)),
            phase: AtomicU8::new(ConnectionPhase::Open as u8),
            phase_tx,
            external_handles: AtomicUsize::new(1),
            close_driver_started: AtomicBool::new(false),
            takeover_started: AtomicBool::new(false),
            drain_submits_on_close: AtomicBool::new(false),
            close_complete_tx,
            cleanup_complete_tx,
            workers_remaining: AtomicUsize::new(4),
            workers_complete_tx,
            pending_terminate: Mutex::new(None),
            peer_terminate_seen: AtomicBool::new(false),
            peer_terminate_response_written: AtomicBool::new(false),
            runtime_handle: tokio::runtime::Handle::current(),
            udh_reference_cooldown,
            response_timeout: Duration::from_secs(params.response_timeout),
            retry_count: params.retry_count,
            window_size,
            submit_retry_batch_size,
        });

        let dispatcher_handle = tokio::spawn(event_dispatcher_task(event_spool_rx, events_tx));
        drop(dispatcher_handle);

        let writer = spawn_worker(
            inner.clone(),
            writer_task(
                inner.clone(),
                write_half,
                submit_rx,
                control_rx,
                inner.phase_tx.subscribe(),
            ),
        );
        let reader = spawn_worker(
            inner.clone(),
            reader_task(
                inner.clone(),
                framed,
                Duration::from_secs(params.read_idle_timeout),
                inner.phase_tx.subscribe(),
            ),
        );
        let heartbeat = spawn_worker(
            inner.clone(),
            heartbeat_task(
                inner.clone(),
                Duration::from_secs(params.heartbeat_interval),
                inner.phase_tx.subscribe(),
            ),
        );
        let timeout = spawn_worker(
            inner.clone(),
            timeout_task(inner.clone(), inner.phase_tx.subscribe()),
        );
        let background_tasks = Arc::new(Mutex::new(vec![writer, reader, heartbeat, timeout]));

        Ok(CmppConnection {
            inner,
            events_rx: Arc::new(Mutex::new(Some(events_rx))),
            background_tasks,
        })
    }

    /// 取走 event receiver。只有首次调用会返回 `Some`。
    pub async fn take_events(&self) -> Option<mpsc::Receiver<Event>> {
        self.events_rx.lock().await.take()
    }

    /// connection 是否已关闭。
    pub fn is_closed(&self) -> bool {
        self.inner.phase() != ConnectionPhase::Open
    }

    /// Submit message，并为每个 SMS segment 返回一个 sequence id。
    ///
    /// Non-blocking：调用只会在 sliding-window backpressure 时等待。同一重组域的
    /// 8-bit UDH reference 全部占用时会立即返回 [`Error::Config`]。对应的
    /// `SUBMIT_RESP` 会以 async 形式作为 [`Event::SubmitResp`]
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

        let mut submits = options.try_build_submits(content)?;
        let udh_reference_lease = if submits.len() > 1 {
            let preferred = submits
                .first()
                .and_then(|submit| submit.msg_content.get(3))
                .copied()
                .ok_or_else(|| Error::Config("long SMS UDH 无效".to_string()))?;
            let bucket_ids =
                UDH_REFERENCE_POOL.bucket_ids(&options.src_id, &options.dest_terminal_ids);
            let lease = UDH_REFERENCE_POOL
                .try_acquire(bucket_ids, preferred, self.inner.udh_reference_cooldown)
                .ok_or_else(|| {
                    Error::Config("同一短信重组域的 8-bit UDH reference 已全部占用".to_string())
                })?;
            for submit in &mut submits {
                let Some(reference) = submit.msg_content.get_mut(3) else {
                    return Err(Error::Config("long SMS UDH 无效".to_string()));
                };
                *reference = lease.reference();
            }
            Some(lease)
        } else {
            None
        };
        let mut seq_ids = Vec::with_capacity(submits.len());
        let mut phase_rx = self.inner.phase_tx.subscribe();
        let _manual_batch_permit = if base_sequence_id.is_some() {
            Some(tokio::select! {
                biased;
                _ = wait_until_not_open(&mut phase_rx) => return Err(Error::Closed),
                result = self.inner.manual_batch_semaphore.clone().acquire_owned() => {
                    result.map_err(|_| Error::Closed)?
                }
            })
        } else {
            None
        };
        let mut manual_sequence_leases = if let Some(base) = base_sequence_id {
            Some(
                self.inner
                    .sequence_registry
                    .reserve_batch(base, submits.len())
                    .ok_or_else(|| {
                        Error::Config(
                            "base_sequence_id 区间超过 255 段、包含重复值或已被当前 connection 使用"
                                .to_string(),
                        )
                    })?
                    .into_iter(),
            )
        } else {
            None
        };

        for submit in submits {
            let sequence_lease = match manual_sequence_leases.as_mut() {
                Some(leases) => Some(leases.next().ok_or(Error::ChannelClosed)?),
                None => None,
            };
            let sequence_id = self
                .send_submit(sequence_lease, submit, udh_reference_lease.clone())
                .await?;
            seq_ids.push(sequence_id);
        }

        Ok(seq_ids)
    }

    async fn send_submit(
        &self,
        sequence_lease: Option<SequenceLease>,
        submit: Submit,
        udh_reference_lease: Option<Arc<UdhReferenceLease>>,
    ) -> Result<u32> {
        let mut phase_rx = self.inner.phase_tx.subscribe();
        let permit = tokio::select! {
            biased;
            _ = wait_until_not_open(&mut phase_rx) => return Err(Error::Closed),
            result = self.inner.window_semaphore.clone().acquire_owned() => {
                result.map_err(|_| Error::Closed)?
            }
        };

        let mut sequence_lease = match sequence_lease {
            Some(lease) => lease,
            None => match self.inner.sequence_registry.reserve_next() {
                Some(lease) => lease,
                None => {
                    self.inner.finish(Some(Error::ChannelClosed));
                    return Err(Error::ChannelClosed);
                }
            },
        };
        let sequence_id = sequence_lease.id();
        let bytes = submit.try_encode(sequence_id)?;
        let key = SubmitAttemptKey {
            sequence_id,
            submission_id: self.inner.next_submission_id(),
            attempt: 1,
        };
        let submit_tx = self.inner.submit_tx.clone();
        let queue_permit = tokio::select! {
            biased;
            _ = wait_until_not_open(&mut phase_rx) => return Err(Error::Closed),
            result = submit_tx.reserve_owned() => {
                match result {
                    Ok(permit) => permit,
                    Err(_) if self.inner.phase() == ConnectionPhase::Open => {
                        return Err(Error::ChannelClosed);
                    }
                    Err(_) => return Err(Error::Closed),
                }
            }
        };

        let mut pending = self.inner.pending_submits.write().await;
        let _admission = self
            .inner
            .submit_admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.inner.phase() != ConnectionPhase::Open {
            return Err(Error::Closed);
        }
        if sequence_lease.manual && !sequence_lease.retire() {
            drop(_admission);
            drop(pending);
            self.inner.finish(Some(Error::ChannelClosed));
            return Err(Error::ChannelClosed);
        }
        if let Some(lease) = &udh_reference_lease {
            lease.mark_exposed();
        }
        pending.insert(
            sequence_id,
            PendingSubmit {
                submission_id: key.submission_id,
                packet: bytes.clone(),
                state: SubmitAttemptState::Queued { attempt: 1 },
                _udh_reference_lease: udh_reference_lease,
                _sequence_lease: sequence_lease,
                _window_permit: permit,
            },
        );
        queue_permit.send(Outbound::submit(bytes, key));
        Ok(sequence_id)
    }

    /// 优雅关闭 connection：发送 CMPP_TERMINATE，然后拆除。
    pub async fn close(&self) {
        if self
            .inner
            .close_driver_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            let inner = self.inner.clone();
            let background_tasks = self.background_tasks.clone();
            if self.inner.begin_closing(true) {
                self.inner.begin_event_drain();
                tokio::spawn(async move {
                    graceful_close_task(inner, background_tasks).await;
                });
            } else {
                tokio::spawn(async move {
                    reap_background_tasks(inner, background_tasks).await;
                });
            }
        }
        let mut complete_rx = self.inner.close_complete_tx.subscribe();
        let takeover_after = self
            .inner
            .response_timeout
            .saturating_mul(3)
            .saturating_add(Duration::from_secs(1));
        if tokio::time::timeout(takeover_after, wait_until_true(&mut complete_rx))
            .await
            .is_err()
        {
            log::warn!("CMPP close driver 未按时完成，正在当前 runtime 接管清理");
            loop {
                if *self.inner.close_complete_tx.borrow() {
                    break;
                }
                if self
                    .inner
                    .takeover_started
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    let inner = self.inner.clone();
                    let background_tasks = self.background_tasks.clone();
                    let takeover_guard = TakeoverGuard {
                        inner: inner.clone(),
                    };
                    tokio::spawn(async move {
                        let _guard = takeover_guard;
                        let reason = if inner.peer_terminate_seen.load(Ordering::SeqCst) {
                            Some(Error::Terminated)
                        } else {
                            None
                        };
                        drain_event_tickets(&inner, "接管关闭").await;
                        inner.finish(reason);
                        reap_background_tasks(inner, background_tasks).await;
                    });
                }
                let mut takeover_rx = self.inner.close_complete_tx.subscribe();
                let takeover_timeout = self.inner.response_timeout.saturating_mul(5);
                if tokio::time::timeout(takeover_timeout, wait_until_true(&mut takeover_rx))
                    .await
                    .is_ok()
                {
                    break;
                }
                log::error!("CMPP connection 接管清理未按时完成，正在重试接管");
            }
        }
    }
}

async fn graceful_close_task(inner: Arc<Inner>, background_tasks: Arc<Mutex<Vec<JoinHandle<()>>>>) {
    log::info!("正在关闭 CMPP connection");
    let mut phase_rx = inner.phase_tx.subscribe();

    let (drain_marker, drain_written_rx) = Outbound::submit_drain_marker();
    let drain_result = tokio::time::timeout(inner.response_timeout, async {
        send_outbound_until_closed(&inner.submit_tx, drain_marker, &mut phase_rx)
            .await
            .map_err(|_| ())?;
        drain_written_rx.await.map_err(|_| ())
    })
    .await;
    inner.drain_submits_on_close.store(false, Ordering::SeqCst);
    if drain_result.is_err() {
        log::warn!("优雅关闭等待已接受 SUBMIT 排空超时");
    } else if matches!(drain_result, Ok(Err(()))) {
        log::debug!("优雅关闭的 SUBMIT 排空在 connection 关闭前未完成");
    }

    let Some(sequence_lease) = inner.sequence_registry.reserve_next() else {
        log::error!("无法为 CMPP_TERMINATE 分配 sequence id");
        inner.finish(Some(Error::ChannelClosed));
        reap_background_tasks(inner, background_tasks).await;
        return;
    };
    let term_seq = sequence_lease.id();
    let (response_tx, response_rx) = oneshot::channel();
    let written = Arc::new(AtomicBool::new(false));
    *inner.pending_terminate.lock().await = Some(PendingTerminate {
        sequence_id: term_seq,
        response_tx,
        written: written.clone(),
        _sequence_lease: sequence_lease,
    });
    let (outbound, written_rx) =
        Outbound::local_terminate(Pdu::Terminate.encode(term_seq), written);

    let close_result = tokio::time::timeout(inner.response_timeout, async {
        send_outbound_until_closed(&inner.control_tx, outbound, &mut phase_rx)
            .await
            .map_err(|_| ())?;
        written_rx.await.map_err(|_| ())?;
        tokio::select! {
            biased;
            _ = wait_until_closed(&mut phase_rx) => Err(()),
            result = response_rx => result.map_err(|_| ()),
        }
    })
    .await;

    if close_result.is_err() {
        log::warn!("CMPP TERMINATE handshake 超时");
    } else if matches!(close_result, Ok(Err(()))) {
        log::debug!("CMPP TERMINATE handshake 在 connection 关闭前未完成");
    }

    drain_event_tickets(&inner, "优雅关闭").await;

    let reason = if inner.peer_terminate_seen.load(Ordering::SeqCst) {
        Some(Error::Terminated)
    } else {
        None
    };
    inner.finish(reason);
    {
        let mut pending = inner.pending_terminate.lock().await;
        if pending
            .as_ref()
            .is_some_and(|pending| pending.sequence_id == term_seq)
        {
            pending.take();
        }
    }
    reap_background_tasks(inner, background_tasks).await;
}

async fn reap_background_tasks(
    inner: Arc<Inner>,
    background_tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
) {
    let mut phase_rx = inner.phase_tx.subscribe();
    if tokio::time::timeout(inner.response_timeout, wait_until_closed(&mut phase_rx))
        .await
        .is_err()
    {
        log::warn!("等待 CMPP connection 进入 Closed 超时，执行强制收口");
        let reason = if inner.peer_terminate_seen.load(Ordering::SeqCst) {
            Some(Error::Terminated)
        } else {
            None
        };
        inner.finish(reason);
    }

    {
        let handles = background_tasks.lock().await;
        for handle in handles.iter() {
            handle.abort();
        }
    }
    let mut workers_rx = inner.workers_complete_tx.subscribe();
    let workers_completed =
        tokio::time::timeout(inner.response_timeout, wait_until_true(&mut workers_rx))
            .await
            .is_ok();
    if !workers_completed {
        log::warn!("等待 CMPP background tasks 退出超时");
    }

    if workers_completed {
        let handles: Vec<JoinHandle<()>> = std::mem::take(&mut *background_tasks.lock().await);
        if tokio::time::timeout(inner.response_timeout, async {
            for handle in handles {
                let _ = handle.await;
            }
        })
        .await
        .is_err()
        {
            log::warn!("回收 CMPP background task handles 超时");
        }
    }
    let mut cleanup_rx = inner.cleanup_complete_tx.subscribe();
    if tokio::time::timeout(inner.response_timeout, wait_until_true(&mut cleanup_rx))
        .await
        .is_err()
    {
        log::warn!("等待 CMPP connection cleanup 完成超时，执行幂等兜底清理");
        let fallback_result = tokio::time::timeout(inner.response_timeout, async {
            inner.fail_all_pending().await;
            inner.heartbeat_pending.write().await.clear();
            inner.pending_terminate.lock().await.take();
            inner.close_event_spool();
            inner.cleanup_complete_tx.send_replace(true);
        })
        .await;
        if fallback_result.is_err() {
            log::error!("CMPP connection 兜底清理未按时完成");
        }
    }
    inner.close_complete_tx.send_replace(true);
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
async fn wait_until_closed(phase_rx: &mut watch::Receiver<ConnectionPhase>) {
    loop {
        if *phase_rx.borrow() == ConnectionPhase::Closed {
            return;
        }
        if phase_rx.changed().await.is_err() {
            return;
        }
    }
}

async fn wait_until_not_open(phase_rx: &mut watch::Receiver<ConnectionPhase>) {
    loop {
        if *phase_rx.borrow() != ConnectionPhase::Open {
            return;
        }
        if phase_rx.changed().await.is_err() {
            return;
        }
    }
}

async fn wait_until_true(complete_rx: &mut watch::Receiver<bool>) {
    loop {
        if *complete_rx.borrow() {
            return;
        }
        if complete_rx.changed().await.is_err() {
            return;
        }
    }
}

async fn wait_until_zero(pending_rx: &mut watch::Receiver<usize>) {
    loop {
        if *pending_rx.borrow() == 0 {
            return;
        }
        if pending_rx.changed().await.is_err() {
            return;
        }
    }
}

async fn drain_event_tickets(inner: &Arc<Inner>, context: &str) {
    inner.begin_event_drain();
    let mut pending_rx = inner.event_tickets.pending_tx.subscribe();
    let drain_result = tokio::time::timeout(inner.response_timeout, async {
        loop {
            if inner.seal_event_admission_if_idle() {
                return;
            }
            wait_until_zero(&mut pending_rx).await;
        }
    })
    .await;
    if drain_result.is_err() {
        log::warn!("{}等待已接受 event 完成超时", context);
        inner.seal_event_admission();
    }
}

async fn send_outbound_until_closed(
    tx: &mpsc::Sender<Outbound>,
    outbound: Outbound,
    phase_rx: &mut watch::Receiver<ConnectionPhase>,
) -> std::result::Result<(), ()> {
    tokio::select! {
        biased;
        _ = wait_until_closed(phase_rx) => Err(()),
        result = tx.send(outbound) => result.map_err(|_| ()),
    }
}

async fn send_until_closed(
    tx: &mpsc::Sender<Outbound>,
    bytes: Bytes,
    phase_rx: &mut watch::Receiver<ConnectionPhase>,
) -> std::result::Result<(), ()> {
    send_outbound_until_closed(tx, Outbound::plain(bytes), phase_rx).await
}

async fn event_dispatcher_task(
    mut event_spool_rx: mpsc::Receiver<EventSpoolItem>,
    events_tx: mpsc::Sender<Event>,
) {
    let mut discard_events = false;

    while let Some(item) = event_spool_rx.recv().await {
        match item {
            EventSpoolItem::Event {
                event_rx,
                _depth_permit,
            } => {
                if discard_events {
                    continue;
                }
                let event = match event_rx.await {
                    Ok(event) => event,
                    Err(_) => continue,
                };
                if !discard_events && events_tx.send(event).await.is_err() {
                    discard_events = true;
                    log::debug!("event receiver 已丢弃；后续 event 将被有界排空");
                }
            }
            EventSpoolItem::Terminal {
                reason,
                _depth_permit,
            } => {
                if let Some(reason) = reason
                    && !discard_events
                {
                    let _ = events_tx.send(Event::Disconnected(reason)).await;
                }
                break;
            }
        }
    }
    log::debug!("event dispatcher task 已退出");
}

async fn writer_task(
    inner: Arc<Inner>,
    mut writer: OwnedWriteHalf,
    mut submit_rx: mpsc::Receiver<Outbound>,
    mut control_rx: mpsc::Receiver<Outbound>,
    mut phase_rx: watch::Receiver<ConnectionPhase>,
) {
    let mut control_burst = 0usize;
    loop {
        let phase = inner.phase();
        if phase == ConnectionPhase::Closed {
            let _ = writer.shutdown().await;
            break;
        }
        let can_send_submit = phase == ConnectionPhase::Open
            || (phase == ConnectionPhase::Closing
                && inner.drain_submits_on_close.load(Ordering::SeqCst));
        let next = if can_send_submit {
            if control_burst >= CONTROL_BURST_LIMIT {
                tokio::select! {
                    biased;
                    changed = phase_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        continue;
                    }
                    outbound = submit_rx.recv() => outbound.map(|outbound| (outbound, true)),
                    outbound = control_rx.recv() => outbound.map(|outbound| (outbound, false)),
                }
            } else {
                tokio::select! {
                    biased;
                    changed = phase_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        continue;
                    }
                    outbound = control_rx.recv() => outbound.map(|outbound| (outbound, false)),
                    outbound = submit_rx.recv() => outbound.map(|outbound| (outbound, true)),
                }
            }
        } else {
            tokio::select! {
                biased;
                changed = phase_rx.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    continue;
                }
                outbound = control_rx.recv() => outbound.map(|outbound| (outbound, false)),
            }
        };

        let Some((mut outbound, is_submit)) = next else {
            inner.finish(Some(Error::ChannelClosed));
            break;
        };
        if is_submit {
            control_burst = 0;
        } else {
            control_burst = control_burst.saturating_add(1);
        }
        if outbound.submit_drain_marker {
            inner.drain_submits_on_close.store(false, Ordering::SeqCst);
            if let Some(written_tx) = outbound.written_tx.take() {
                let _ = written_tx.send(());
            }
            continue;
        }
        let submit_attempt = if is_submit {
            let Some(key) = outbound.submit_attempt else {
                log::error!("submit queue 中存在缺少 attempt metadata 的报文");
                inner.finish(Some(Error::ChannelClosed));
                break;
            };
            if !inner.claim_submit_attempt(key).await {
                log::debug!(
                    "跳过过期 SUBMIT attempt: seq_id={}, attempt={}",
                    key.sequence_id,
                    key.attempt
                );
                continue;
            }
            Some(key)
        } else {
            None
        };
        let heartbeat_attempt = if let Some(key) = outbound.heartbeat_attempt {
            if !inner.claim_heartbeat_attempt(key).await {
                log::debug!(
                    "跳过过期 ACTIVE_TEST attempt: seq_id={}, attempt={}",
                    key.sequence_id,
                    key.attempt
                );
                continue;
            }
            Some(key)
        } else {
            None
        };
        let phase = inner.phase();
        if outbound.open_only && phase != ConnectionPhase::Open {
            continue;
        }
        if outbound.cancel_after_peer_terminate
            && inner
                .peer_terminate_response_written
                .load(Ordering::Acquire)
        {
            log::debug!("peer TERMINATE 已完成响应，取消尚未写出的本地 TERMINATE");
            continue;
        }
        let deliver_response_budget = outbound.response_budget_started_at.is_some();
        let write_timeout = match outbound.response_budget_started_at {
            Some(started_at) => match inner.response_timeout.checked_sub(started_at.elapsed()) {
                Some(remaining) if !remaining.is_zero() => remaining,
                _ => {
                    log::warn!("CMPP DELIVER_RESP pipeline 响应超时");
                    inner.finish(Some(Error::Timeout));
                    break;
                }
            },
            None => inner.response_timeout,
        };
        let timed_write = tokio::select! {
            biased;
            _ = wait_until_closed(&mut phase_rx) => {
                let _ = writer.shutdown().await;
                break;
            }
            result = tokio::time::timeout(
                write_timeout,
                writer.write_all(&outbound.packet),
            ) => result,
        };
        let write_result = match timed_write {
            Ok(result) => result,
            Err(_) if deliver_response_budget => {
                log::warn!("CMPP DELIVER_RESP pipeline 响应超时");
                inner.finish(Some(Error::Timeout));
                break;
            }
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "CMPP socket write 超时",
            )),
        };
        match write_result {
            Ok(()) => {
                let written_at = Instant::now();
                if let Some(key) = submit_attempt {
                    inner.complete_submit_attempt(key, written_at).await;
                }
                if let Some(key) = heartbeat_attempt {
                    inner.complete_heartbeat_attempt(key, written_at).await;
                }
                if outbound.marks_peer_terminate_response {
                    inner
                        .peer_terminate_response_written
                        .store(true, Ordering::Release);
                }
                if let Some(written_flag) = outbound.written_flag.take() {
                    written_flag.store(true, Ordering::Release);
                }
                if let Some((ticket, event)) = outbound.event_after_write.take()
                    && !ticket.publish(event)
                {
                    log::error!("协议响应已写出，但 event dispatcher 已不可用");
                }
                if let Some(written_tx) = outbound.written_tx.take() {
                    let _ = written_tx.send(());
                }
            }
            Err(e) => {
                log::warn!("CMPP write 错误: {}", e);
                inner.finish(Some(Error::Io(e)));
                break;
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
    mut phase_rx: watch::Receiver<ConnectionPhase>,
) {
    let reason: Error = loop {
        let frame = tokio::select! {
            biased;
            _ = wait_until_closed(&mut phase_rx) => return,
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
                let handled = {
                    let mut map = inner.pending_submits.write().await;
                    match map.get(&sequence_id).map(|entry| entry.state) {
                        Some(SubmitAttemptState::Writing { attempt }) => {
                            if let Some(entry) = map.get_mut(&sequence_id) {
                                entry.state = SubmitAttemptState::RespondedWhileWriting { attempt };
                            }
                            true
                        }
                        Some(SubmitAttemptState::RespondedWhileWriting { .. }) => false,
                        Some(
                            SubmitAttemptState::Queued { .. }
                            | SubmitAttemptState::AwaitingResponse { .. },
                        ) => {
                            map.remove(&sequence_id);
                            true
                        }
                        None => false,
                    }
                };
                if handled {
                    if inner.event_overflowed.load(Ordering::Acquire) {
                        log::debug!(
                            "event backlog 关闭期间收到 SUBMIT_RESP seq_id={}，仅完成协议状态迁移",
                            sequence_id
                        );
                    } else {
                        let _ = inner.emit_event(Event::SubmitResp {
                            sequence_id,
                            msg_id: resp.msg_id,
                            result: resp.result,
                        });
                    }
                } else {
                    log::debug!("收到未知或重复 seq_id={} 的 SUBMIT_RESP", sequence_id);
                }
            }
            Pdu::Deliver(deliver) => {
                let event_ticket = match inner.reserve_event(true, true) {
                    Ok(ticket) => ticket,
                    Err(EventReservationError::Overflowed) => {
                        log::debug!("event backlog 关闭期间忽略未确认的 DELIVER");
                        return;
                    }
                    Err(EventReservationError::Closed) => return,
                };
                let resp = Frame::new(
                    sequence_id,
                    Pdu::DeliverResp(DeliverResp {
                        msg_id: deliver.msg_id,
                        result: 0,
                    }),
                );
                let response_budget_started_at = Instant::now();
                let outbound = Outbound::event_after_write(
                    resp.encode(),
                    response_budget_started_at,
                    event_ticket,
                    Event::Deliver(deliver),
                );
                let remaining = inner
                    .response_timeout
                    .checked_sub(response_budget_started_at.elapsed())
                    .filter(|remaining| !remaining.is_zero());
                let Some(remaining) = remaining else {
                    log::warn!("CMPP DELIVER_RESP pipeline 响应超时");
                    break Error::Timeout;
                };
                match tokio::time::timeout(
                    remaining,
                    send_outbound_until_closed(&inner.control_tx, outbound, &mut phase_rx),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(())) => {
                        if inner.phase() == ConnectionPhase::Closed {
                            return;
                        }
                        break Error::ChannelClosed;
                    }
                    Err(_) => {
                        log::warn!("CMPP DELIVER_RESP pipeline 入队超时");
                        break Error::Timeout;
                    }
                }
            }
            Pdu::ActiveTest => {
                if send_until_closed(
                    &inner.control_tx,
                    Frame::new(sequence_id, Pdu::ActiveTestResp).encode(),
                    &mut phase_rx,
                )
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
                inner.peer_terminate_seen.store(true, Ordering::SeqCst);
                inner.begin_closing(false);
                inner.begin_event_drain();
                let (outbound, written_rx) = Outbound::peer_terminate_response(
                    Frame::new(sequence_id, Pdu::TerminateResp).encode(),
                );
                let response_result = tokio::time::timeout(inner.response_timeout, async {
                    send_outbound_until_closed(&inner.control_tx, outbound, &mut phase_rx)
                        .await
                        .map_err(|_| ())?;
                    written_rx.await.map_err(|_| ())
                })
                .await;
                if response_result.is_err() {
                    log::warn!("CMPP TERMINATE_RESP write 超时");
                }
                let response_written = matches!(response_result, Ok(Ok(())));
                let local_terminate_written = inner
                    .pending_terminate
                    .lock()
                    .await
                    .as_ref()
                    .is_some_and(|pending| pending.written.load(Ordering::Acquire));
                if response_written && local_terminate_written {
                    continue;
                }
                break Error::Terminated;
            }
            Pdu::TerminateResp => {
                log::debug!("收到 TERMINATE_RESP seq_id={}", sequence_id);
                let response_tx = {
                    let mut pending = inner.pending_terminate.lock().await;
                    if pending
                        .as_ref()
                        .is_some_and(|pending| pending.sequence_id == sequence_id)
                    {
                        pending.take().map(|pending| pending.response_tx)
                    } else {
                        None
                    }
                };
                if let Some(response_tx) = response_tx {
                    let _ = response_tx.send(());
                }
            }
            other => {
                log::warn!("收到非预期入站 PDU: {:#010x}", other.command_id());
            }
        }
    };

    inner.finish(Some(reason));
    log::debug!("reader task 已退出");
}

/// 在没有未完成 heartbeat 时周期性发送 ACTIVE_TEST。
async fn heartbeat_task(
    inner: Arc<Inner>,
    interval: Duration,
    mut phase_rx: watch::Receiver<ConnectionPhase>,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = wait_until_not_open(&mut phase_rx) => break,
            _ = ticker.tick() => {}
        }
        let has_pending = !inner.heartbeat_pending.read().await.is_empty();
        if has_pending {
            continue;
        }
        let Some(sequence_lease) = inner.sequence_registry.reserve_next() else {
            inner.finish(Some(Error::ChannelClosed));
            break;
        };
        let key = HeartbeatAttemptKey {
            sequence_id: sequence_lease.id(),
            heartbeat_id: inner.next_heartbeat_id(),
            attempt: 1,
        };
        let control_tx = inner.control_tx.clone();
        let queue_permit = tokio::select! {
            biased;
            _ = wait_until_not_open(&mut phase_rx) => break,
            result = control_tx.reserve_owned() => match result {
                Ok(permit) => permit,
                Err(_) => {
                    if inner.phase() == ConnectionPhase::Open {
                        inner.finish(Some(Error::ChannelClosed));
                    }
                    break;
                }
            }
        };
        let mut pending = inner.heartbeat_pending.write().await;
        let _admission = inner
            .submit_admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if inner.phase() != ConnectionPhase::Open {
            break;
        }
        if !pending.is_empty() {
            continue;
        }
        pending.insert(
            key.sequence_id,
            PendingHeartbeat {
                heartbeat_id: key.heartbeat_id,
                state: HeartbeatAttemptState::Queued { attempt: 1 },
                _sequence_lease: sequence_lease,
            },
        );
        queue_permit.send(Outbound::heartbeat(
            Pdu::ActiveTest.encode(key.sequence_id),
            key,
        ));
    }
    log::debug!("heartbeat task 已退出");
}

/// 重传 timed-out SUBMIT 和 heartbeat；heartbeat 耗尽时拆除连接。
async fn timeout_task(inner: Arc<Inner>, mut phase_rx: watch::Receiver<ConnectionPhase>) {
    let timeout = inner.response_timeout;
    let retry_count = inner.retry_count;
    let active_check_interval =
        (timeout / MAX_SUBMIT_RETRY_SPREAD_TICKS as u32).min(TIMEOUT_CHECK_INTERVAL);
    let mut ticker = tokio::time::interval(active_check_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut expired_submits: Vec<(Instant, SubmitAttemptKey)> =
        Vec::with_capacity(inner.window_size);
    let mut timeout_samples = Vec::with_capacity(SUBMIT_TIMEOUT_LOG_SAMPLES);

    loop {
        tokio::select! {
            biased;
            _ = wait_until_not_open(&mut phase_rx) => break,
            _ = ticker.tick() => {}
        }
        let now = Instant::now();

        // Heartbeat 同样从完整写出后开始 timeout，不把队列等待误算为网络超时。
        let expired_heartbeat = {
            let pending = inner.heartbeat_pending.read().await;
            pending
                .iter()
                .find_map(|(&sequence_id, heartbeat)| match heartbeat.state {
                    HeartbeatAttemptState::AwaitingResponse {
                        attempt,
                        written_at,
                    } if now.duration_since(written_at) >= timeout => Some(HeartbeatAttemptKey {
                        sequence_id,
                        heartbeat_id: heartbeat.heartbeat_id,
                        attempt,
                    }),
                    _ => None,
                })
        };
        let mut exhausted = false;
        if let Some(key) = expired_heartbeat {
            if key.attempt >= retry_count {
                let mut pending = inner.heartbeat_pending.write().await;
                let still_expired = pending.get(&key.sequence_id).is_some_and(|heartbeat| {
                    heartbeat.heartbeat_id == key.heartbeat_id
                        && matches!(
                            heartbeat.state,
                            HeartbeatAttemptState::AwaitingResponse { attempt, .. }
                                if attempt == key.attempt
                        )
                });
                if still_expired {
                    pending.remove(&key.sequence_id);
                    exhausted = true;
                }
            } else {
                let queue_permit = match inner.control_tx.clone().try_reserve_owned() {
                    Ok(permit) => Some(permit),
                    Err(mpsc::error::TrySendError::Full(_)) => None,
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        if inner.phase() == ConnectionPhase::Open {
                            inner.finish(Some(Error::ChannelClosed));
                        }
                        return;
                    }
                };
                if let Some(queue_permit) = queue_permit {
                    let next_key = HeartbeatAttemptKey {
                        attempt: key.attempt + 1,
                        ..key
                    };
                    let mut pending = inner.heartbeat_pending.write().await;
                    let _admission = inner
                        .submit_admission
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    let valid = pending.get_mut(&key.sequence_id).is_some_and(|heartbeat| {
                        if inner.phase() != ConnectionPhase::Open
                            || heartbeat.heartbeat_id != key.heartbeat_id
                            || !matches!(
                                heartbeat.state,
                                HeartbeatAttemptState::AwaitingResponse { attempt, .. }
                                    if attempt == key.attempt
                            )
                        {
                            return false;
                        }
                        heartbeat.state = HeartbeatAttemptState::Queued {
                            attempt: next_key.attempt,
                        };
                        true
                    });
                    if valid {
                        queue_permit.send(Outbound::heartbeat(
                            Pdu::ActiveTest.encode(key.sequence_id),
                            next_key,
                        ));
                        log::debug!(
                            "正在重传 ACTIVE_TEST seq_id={}, attempt={}",
                            key.sequence_id,
                            next_key.attempt
                        );
                    }
                }
            }
        }
        if exhausted {
            log::error!("heartbeat 已耗尽，正在拆除 connection");
            inner.finish(Some(Error::Closed));
            return;
        }

        // SUBMIT timeout：只检查已经完整写出的 attempt，并按最老优先平滑处理。
        expired_submits.clear();
        {
            let map = inner.pending_submits.read().await;
            expired_submits.extend(map.iter().filter_map(|(&sequence_id, pending)| {
                match pending.state {
                    SubmitAttemptState::AwaitingResponse {
                        attempt,
                        written_at,
                    } if now.duration_since(written_at) >= timeout => Some((
                        written_at,
                        SubmitAttemptKey {
                            sequence_id,
                            submission_id: pending.submission_id,
                            attempt,
                        },
                    )),
                    _ => None,
                }
            }));
        }
        expired_submits.sort_unstable_by(|left, right| {
            left.0
                .cmp(&right.0)
                .then_with(|| left.1.sequence_id.cmp(&right.1.sequence_id))
                .then_with(|| left.1.submission_id.cmp(&right.1.submission_id))
        });
        expired_submits.truncate(inner.submit_retry_batch_size);
        timeout_samples.clear();
        let mut timed_out_submits = 0usize;

        for (_, key) in expired_submits.drain(..) {
            if key.attempt >= retry_count {
                let ticket = {
                    let mut map = inner.pending_submits.write().await;
                    let still_expired = map.get(&key.sequence_id).is_some_and(|pending| {
                        pending.submission_id == key.submission_id
                            && matches!(
                                pending.state,
                                SubmitAttemptState::AwaitingResponse { attempt, .. }
                                    if attempt == key.attempt
                            )
                    });
                    if !still_expired {
                        None
                    } else {
                        match inner.reserve_event(true, false) {
                            Ok(ticket) => {
                                if let Some(mut pending) = map.remove(&key.sequence_id) {
                                    let retired = pending._sequence_lease.retire();
                                    Some((ticket, retired))
                                } else {
                                    None
                                }
                            }
                            Err(_) => None,
                        }
                    }
                };
                if let Some((ticket, retired)) = ticket {
                    let _ = ticket.publish(Event::SubmitTimeout {
                        sequence_id: key.sequence_id,
                    });
                    timed_out_submits += 1;
                    if timeout_samples.len() < SUBMIT_TIMEOUT_LOG_SAMPLES {
                        timeout_samples.push(key.sequence_id);
                    }
                    if !retired {
                        log_submit_timeout_batch(timed_out_submits, &timeout_samples);
                        log::error!("sequence 隔离表已满，正在关闭 connection");
                        inner.finish(Some(Error::ChannelClosed));
                        return;
                    }
                }
                continue;
            }

            let queue_permit = match inner.submit_tx.clone().try_reserve_owned() {
                Ok(permit) => permit,
                Err(mpsc::error::TrySendError::Full(_)) => continue,
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    if inner.phase() == ConnectionPhase::Open {
                        inner.finish(Some(Error::ChannelClosed));
                    }
                    return;
                }
            };
            let next_key = SubmitAttemptKey {
                attempt: key.attempt + 1,
                ..key
            };
            let committed = {
                let mut map = inner.pending_submits.write().await;
                let _admission = inner
                    .submit_admission
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let Some(pending) = map.get_mut(&key.sequence_id) else {
                    continue;
                };
                if inner.phase() != ConnectionPhase::Open
                    || pending.submission_id != key.submission_id
                    || !matches!(
                        pending.state,
                        SubmitAttemptState::AwaitingResponse { attempt, .. }
                            if attempt == key.attempt
                    )
                {
                    false
                } else {
                    let packet = pending.packet.clone();
                    pending.state = SubmitAttemptState::Queued {
                        attempt: next_key.attempt,
                    };
                    queue_permit.send(Outbound::submit(packet, next_key));
                    true
                }
            };
            if committed {
                log::debug!(
                    "正在重传 SUBMIT seq_id={}, attempt={}",
                    key.sequence_id,
                    next_key.attempt
                );
            }
        }
        log_submit_timeout_batch(timed_out_submits, &timeout_samples);

        ticker.reset_after(active_check_interval);
    }
    log::debug!("timeout task 已退出");
}

fn log_submit_timeout_batch(count: usize, samples: &[u32]) {
    if count != 0 {
        log::warn!(
            "SUBMIT timeout，批量放弃重试: count={}, sample_seq_ids={:?}",
            count,
            samples
        );
    }
}

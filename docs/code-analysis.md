# cmppprotocol 代码分析报告（性能 / 高并发 / 网络波动下的数据完整性）

> 分析基线：commit `cc82949`（perf(connection): 优化热路径吞吐并补齐数据完整性保证）。
> 所有 `connection.rs:N` 行号均指该版本。本报告结论已逐一对照源码验证。
> 更新说明（2026-09-21）：正文第 0–9 节为基线分析快照，不代表当前代码；当前进度见第 10 节。
> 已确认采用有界内存与背压超时关闭，终态交付存在边界，不能将历史“每 seq 必达”表述视为无条件保证。
> 最新收尾（2026-09-22）：工具已放行运行；批次 2–4 核验通过，批次 5 仅实施实测有效的消息号查表格式化。
> 下文“待运行验证”和审批阻止记录为历史状态，当前结果以 [性能核验记录](performance-validation.md) 为准。

---

## 0. 摘要

这是一套工程质量相当高的 CMPP 2.0 客户端实现：**正确性机制（attempt 状态机、序列号
隔离、UDH 引用三重隔离、超时预算语义、终态完备性）设计严谨，编解码热路径干净**
（SUBMIT_RESP 解析零堆分配、SUBMIT 编码单次精确分配）。上一个性能提交
（cc82949）已完成 writer 批量写出、parking_lot 锁替换、SubmitOptions 模板化、
UDH 池分片等大项优化。

本次分析发现的剩余问题集中在三处：

| 编号 | 问题 | 严重度 | 位置 |
|---|---|---|---|
| P-1 | 事件分发管线：每事件 1 次 oneshot 分配 + 双跳 channel + 单 dispatcher 串行 | 高（loopback 场景） | connection.rs:1008-1022, 2088-2151 |
| P-2 | 一条 SUBMIT 生命周期 4 次 `pending_submits` 写锁，writer 批内仍逐帧 claim/complete | 高（多生产者大窗口） | connection.rs:2314, 2418-2419 |
| I-1 | 拆连时终态事件洪峰：`fail_all_pending` 逐条 try_send 进 258 容量 spool，大窗口下大部分 `SubmitDropped` 被静默丢弃 | 高 | connection.rs:888-905 |
| I-2 | UDH 引用耗尽（瞬态）误归类 `Error::Config`；无 `is_retryable` 分类 | 中 | connection.rs:1598-1601 |
| I-3 | `Event::SubmitTimeout` 不携带重试次数，调用方无法评估 at-least-once 重复风险 | 中 | connection.rs:84-88 |

测试面：**超时重传、乱序响应、`result != 0`、消费者停滞有界关闭、心跳耗尽拆连全部
无测试覆盖**，mock ISMG 无故障注入能力（详见 §7）。

优化路线图见 §10（分 6 个批次，按风险与收益排序）。

---

## 1. 总体架构

### 1.1 任务模型

每条连接固定 5 个长驻 tokio task（`connect_with_udh_reference_cooldown` 装配，
connection.rs:1486-1525）：

```
                submit_tx (1000)          control_tx (256)
  submit() ────────────────┐  ┌─────────────────────────── reader（DELIVER_RESP 回复）
  heartbeat/重传 ──────────┤  │                              timeout（ACTIVE_TEST 重传）
                            ▼  ▼
                       ┌──────────┐  write_all（≤64 帧 / 64KB 批量合并）
                       │  writer  │────────────────────────────► TCP 写半（OwnedWriteHalf）
                       └──────────┘
                            ▲
                       ┌──────────┐  read_buf + codec（零拷贝 split_to）
                       │  reader  │◄─────────────────────────── TCP 读半（OwnedReadHalf）
                       └──────────┘
                            │ SubmitResp / DELIVER / ActiveTest / Terminate
                            ▼
  reader/writer/timeout ──► event_spool_tx (258, 内嵌 oneshot 工单)
                            │
                       ┌───────────┐
                       │ dispatcher│（单 task，detached，持 Weak<Inner>）
                       └───────────┘
                            │ events_tx (1000)
                            ▼
                       take_events() → 调用方消费者
```

- **writer**（connection.rs:2186-2455）：双通道 biased select，control 与 submit 按
  `CONTROL_BURST_LIMIT=16`（:50）交替优先，防止上行突发时 DELIVER_RESP 饿死 SUBMIT
  或反之。
- **reader**（:2459-2652）：帧解析、响应匹配、DELIVER 自动回复、心跳/TERMINATE 应答。
- **heartbeat**（:2682-2747）：`interval` + `MissedTickBehavior::Skip`，同时只允许一个
  在途 ACTIVE_TEST（:2695, :2730）。
- **timeout**（:2750-2994）：单一 interval 扫描 task（无 per-request timer），同时管
  SUBMIT 重传与心跳耗尽。
- **dispatcher**（:2088-2151）：事件唯一出口，串行。

### 1.2 背压链条（三层）

1. **滑动窗口信号量**：`window_semaphore`（:772, 建连 :1447），每 segment 一个 permit，
   存于 `PendingSubmit._window_permit`（:599），随终态（响应/超时/拆连）自然释放。
   `submit()` 在窗口满时挂起。吞吐上界 = `window_size / RTT`。
2. **发送队列**：`submit()` 在拿任何锁**之前**先 `submit_tx.reserve_owned()`
   （:1694-1706），队列满则挂起等待，不持锁阻塞。
3. **事件侧**：spool 容量 256+2（:44-45, :1418-1420）+ 深度原子计数
   （`EventDepthPermit`，:148-163）+ admission 状态机（Open/Draining/Overflowed/
   Sealed/Terminal，:577-584）。消费者停止读取超过 1 秒
   （`EVENT_DISPATCH_BACKPRESSURE_TIMEOUT`，:46）触发**有界关闭连接**
   （`start_event_dispatch_backpressure_close`，:1142-1157）——宁可拆线也不丢事件或
   无界堆积，这是明确的设计取舍。

---

## 2. 热路径逐段分析

### 2.1 发送路径（submit → 窗口准入 → 入队 → 批量写出）

`CmppConnection::submit`（:1563-1654）→ 逐 segment `send_submit`（:1656-1740）：

1. 窗口准入 `window_semaphore.acquire_owned()`（:1666-1672）——长短信（≤255 段）逐段
   占窗；
2. **惰性编码**：`plan.segment(index, ref)`（:1674）此时才真正编码当前分片——未准入的
   分片不占 PDU 缓冲（`SubmitContentPlan` 只预存 ranges，encoding.rs:136-211）；
3. `SubmitOptions::build_submit_template`（submit.rs:157-179）共享字段只 clone 一次，
   逐段 `apply_segment` 只覆盖 pk/UDH/content（submit.rs:16-22），长短信 N 段不再重复
   clone 10+n 个 String；
4. 序列号：自动分配走 `SequenceRegistry::reserve_next`（:383-418，短临界区 std Mutex +
   BTreeMap range 查询）；手动 base_sequence_id 走 `reserve_batch`（:420-458）并在准入
   时立即 `retire` 永久隔离（:1717）；
5. `try_encode` 精确容量分配（pdu.rs:375-386，hint 公式 pdu.rs:400-404，无二次扩容）；
6. `pending_submits.write()` + `submit_admission` 锁内插入 `PendingSubmit`
   （:1708-1736），`packet: bytes.clone()` 为 `Bytes` 引用计数浅拷贝（重传复用，零拷贝）；
7. `queue_permit.send(Outbound::submit(bytes, key))`（:1738）。

**writer 批量写出**（:2186-2455）：首帧阻塞 recv，随后 `try_recv` 不阻塞收割至
`WRITE_BATCH_MAX_FRAMES=64` 帧 / `WRITE_BATCH_MAX_BYTES=64KiB`（:52-53），拷入复用的
`batch_buf: BytesMut`（:2194，初始 4KiB），单次 `write_all`（:2388-2391）。批内逐帧
保留语义：drain marker 顺序（:2301-2306）、`open_only`/取消帧跳过（:2333-2344）、
DELIVER_RESP 响应预算取批内最小余量（:2345-2360）。**syscall 已从 O(消息) 降为
O(批次)**。

### 2.2 响应路径（reader → 匹配 → 事件）

`read_frame_with_idle`（:2659-2679）直读 `BytesMut` + `codec.decode` 零拷贝
`split_to`（codec.rs:45）。`SubmitResp` 解析全定长字段，**零堆分配**（pdu.rs:510-516）。

匹配逻辑（:2484-2523）按 pending 条目状态三分支：

| pending 状态 | 动作 |
|---|---|
| `Writing`（已领取未写完） | 置 `RespondedWhileWriting`，事件立即发出；写完时 `complete_submit_attempt` 移除条目（:939-962）——不重复发事件 |
| `Queued` / `AwaitingResponse` | 移除 + 发事件（`Queued` 命中意味着**迟到响应抵消了排队中的重传**：writer 侧 claim 失败会跳过该帧不写出，:2314-2321） |
| 无条目 / `RespondedWhileWriting` | 仅 debug 日志（:2521），不产生事件 |

### 2.3 事件分发路径（当前第一瓶颈，详见 P-1）

每个事件的完整旅程（以 SubmitResp 为例）：

1. reader 调 `emit_event`（:1072-1081）→ `reserve_event`（:1024-1070）：拿
   `event_admission` std Mutex → 检查深度原子（SeqCst）→ `make_event_ticket`
   （:1008-1022）：**分配一个 oneshot::channel** + spool item `try_send` +
   `EventDepthPermit`（2 次 SeqCst 原子）；
2. `ticket.publish(event)`：oneshot send（:569-575）；
3. dispatcher `event_spool_rx.recv()`（:2095）→ **`event_rx.await` 二次等待**
   （:2107）→ `dispatch_event_with_budget`（:2163-2177）：`timeout(1s,
   events_tx.reserve_many(2))`（刻意留一个槽给终态）→ permit.send；
4. 消费者 `events_rx.recv()`。

即：**1 次 oneshot 堆分配 + 2 次 channel 跳转 + 3+ 次原子操作 + watch 锁（仅 DELIVER
路径的 `EventTicketGuard`，:169-184）**，且全部事件串行经过单 dispatcher task。DELIVER
路径额外保留"RESP 完整写出后才发布事件"的写门控（`event_after_write`，:2542-2547 →
writer :2432-2436）——保证未确认的 DELIVER 不发布（网关会重推），语义正确，但同样
经过 oneshot 中转。

### 2.4 超时扫描路径

`timeout_task`（:2750-2994）：`active_check_interval = min(response_timeout/4, 1s)`
（:2753-2754），多连接相位 jitter 错峰（:2999-3009）。每 tick：

- 心跳：单在途检查 + 耗尽拆连（:2772-2859）；
- SUBMIT：持 `pending_submits.read()` **全表 O(window) 扫描** `AwaitingResponse`
  条目（:2864-2881）→ 按最老优先排序 → 截断到 `submit_retry_batch_size = window/4`
  （:1412-1415, :2888）平滑限流，避免重传风暴；
- 重传复用同一 `Bytes` 包（`pending.packet.clone()`，:2972），seq/UDH ref 不变；
- 预算耗尽：`reserve_event` → 移除 + `retire` 序列号 → 发布 `SubmitTimeout`
  （:2893-2936）；retire 区间表满则拆连（:2929-2934）。

**注意**：超时预算从 `written_at`（write_all 完成时刻，:2407）起算，排队等待不消耗
预算——这是 e47c804 修复后的正确语义；只有 `AwaitingResponse` 状态参与超时判定，
`Queued/Writing` 不 tick。

### 2.5 每消息开销清单（happy path，单 segment）

| 类别 | 计数 | 位置 |
|---|---|---|
| `pending_submits` 写锁 | **4 次**（插入 / writer claim / writer complete / reader 响应删除） | :1708, :2314, :2419, :2486 |
| `submit_admission` std Mutex | 2 次（随插入、随 claim） | :1709-1713, :910-913 |
| sequence registry Mutex | 1 次 | :383-418 |
| 堆分配 | 编码缓冲 1 + 内容 Vec 1 + UDH 拼接 Vec 1（长短信）+ oneshot 1 + spool item 1 + HashMap 节点（摊销） | :1687, encoding.rs:104/122-124, :1009 |
| SeqCst 原子 | depth permit ×2 + 溢出检查等 | :148-163 |
| syscall | write 摊销至批（≤1/64）；read 一次一个 timeout 包装 | :2388-2391, :2669 |

---

## 3. 高并发性能瓶颈（按部署场景排序）

**场景结论先行**：真实公网（RTT ≥ 10ms）下吞吐上界是 `window/RTT`，锁竞争几乎不构
成瓶颈，本节问题多在 loopback / 本地网关（RTT < 1ms、单连接 > 10 万 segments/s）场景
才显现。本库的目标场景（本地客户端连上游 CMPP 服务）属于后者。

### P-1【高】事件分发管线：oneshot 工单 + 双跳 + 单 dispatcher

**证据**：connection.rs:1008-1022（`make_event_ticket` 每次 `oneshot::channel()`）、
:2088-2151（dispatcher 串行 `recv → await oneshot → reserve_many(2) → send`）、
README:133 自认 loopback 368k segments/s "受事件消费管线限制"。

**分析**：批处理把写侧 syscall 降下来了，但每个响应事件仍要：分配 oneshot → 塞进
spool → dispatcher 醒来 → 再从 oneshot 取出 → 再塞进 events_tx。两次 channel 跳转、
两次 task wakeup、一次堆分配，且单 dispatcher 是所有事件（SubmitResp/DELIVER/超时）
的串行上界。SubmitResp 在 reader 里本来就是"立即发布"的（不依赖写完成），oneshot
中转对它纯属开销——只有 DELIVER 需要"RESP 写出后才发布"的延迟发布语义。

**方向**：立即发布的事件走 spool 直通变体（携带 Event 本体，免 oneshot）；DELIVER
保留延迟工单路径。语义（admission 深度计数、有界关闭、FIFO 顺序）全部保留。

### P-2【高】writer 批内逐帧锁：每消息 4 次写锁

**证据**：connection.rs:2314（批循环内逐帧 `claim_submit_attempt` → :909
`pending_submits.write()` + admission 锁）、:2418-2419（写完逐帧
`complete_submit_attempt` → :940 再次写锁）。一批 64 帧仍是 64+64 次锁获取。

**分析**：`pending_submits` 是连接级单一 RwLock，多生产者（用户 submit task）与
消费者（writer/reader/timeout task）在此串行化。parking_lot 是同步阻塞锁，临界区虽短，
但在 loopback 高频下，锁的 cacheline 弹跳成为主要串行点。插入（:1708）与响应删除
（:2486）天然 per-op 无法摊销，但 claim/complete 完全可以随批合并：一批一次写锁处理
整批 key。

**方向**：重构组批循环——先收集整批帧（无锁检查照旧），单次写锁批量完成 Queued→Writing
转移，未领取帧剔除后再拷贝写出；写完单次写锁批量 Writing→AwaitingResponse。每消息
锁次数 4 → 2（+每批 2 次摊销）。

### P-3【中】reader 侧 SubmitResp 逐条写锁

**证据**：connection.rs:2485-2504，每个 SubmitResp 一次 `pending_submits.write()`。

**分析**：一个 read 系统调用通常带回多个完整帧（codec 循环解码），但匹配与删除逐条
加锁。可与 P-2 一起随读批次合并。同时 `submit_responses` 计数与事件发布也可批量。

### P-4【低-中】timeout 全表扫描持读锁

**证据**：connection.rs:2864-2881，O(window) 遍历 + 排序。window=16384、1s tick 下
约几十~百微秒/秒的读锁持有，会短暂阻塞写者。量级上可接受，优先级低；若做，用
`BTreeMap<(written_at, seq)>` 索引替代全扫。

### P-5【低】每次 `read_buf` 包一层 `tokio::time::timeout`

**证据**：connection.rs:2669。高吞吐下每次读 syscall 创建/拆除一个 timer。可用手动
deadline 管理替代，收益有限。

### P-6【低】DELIVER 解码每条 ~5 次堆分配

**证据**：pdu.rs:580-605：3 个 `String::from_utf8_lossy().into_owned()`（:693-700）+
`msg_content.to_vec()`（:591）+ status report 再加 4 个 String（:656-677）。回执密集
场景（状态报告风暴）的读侧分配热点。`Event::Deliver` 整个 owned struct 还要在两级
channel 间搬运。

### P-7【低】杂项

- `Event::msg_id_hex` / `DeliverReport::msg_id_hex` 每次调用 8 个 `format!`
  （connection.rs:106-108, pdu.rs:651-653）——仅日志/展示路径，可查表。
- UCS-2 编码是手写逐标量 `extend_from_slice(&unit.to_be_bytes())`
  （encoding.rs:227-231），可 `Vec<u16>` 一次转换。
- codec `reserve(needed.min(4096))` 渐进扩容（codec.rs:35），帧长已校验 ≤64KB，
  可一次性 reserve 大帧减少 read 轮次。

---

## 4. 网络波动下的数据完整性

### 4.1 防御机制清单（现有强项）

| 机制 | 位置 | 作用 |
|---|---|---|
| attempt 状态机 | :602-635 | Queued→Writing→AwaitingResponse→RespondedWhileWriting，领取/写完/响应/重传每步校验 (submission_id, attempt) 代际，过期 attempt 自动失效 |
| 序列号永不复用 | :383-540 | 自动分配单调递增跳过 reserved/retired；手动 seq 上线即 retire；最终超时也 retire；迟到响应只能命中已移除 key → 落入"未知 seq"分支，**不可能污染新 pending** |
| UDH 引用三重隔离 | :198-354, encoding.rs:28-36 | active 位图（分片 pending 期间占用）+ cooling 位图（释放后 300s 冷却，同 `(src_id, dest)` 重组域不复用）+ 进程启动随机起点。跨重连生效（进程级池），迟到分片不会与新长短信错误重组 |
| 重传复用同一字节包 | :2972 | 同 seq、同 UDH ref、同 pk_number，网关/终端侧不会分片错乱 |
| 超时预算从写出起算 | :2407, :2865-2880 | 排队等待不消耗预算；只有 AwaitingResponse 参与判定 |
| 截断帧检测 | :2666-2677, codec.rs | EOF 时 `decode_eof` 对残留缓冲报 "bytes remaining"，不把截断帧当正常关闭 |
| DELIVER 写门控 | :2542-2547, :2432-2436 | 事件仅在 DELIVER_RESP 完整写出 socket 后发布；backlog 关闭期间收到未确认 DELIVER 时 reader 直接退出**不回 ACK**（:2528-2531），网关会重推 |
| 拆连终态完备 | :888-905, :1207-1212 | 每个在途分片发布 `SubmitDropped`（spool 满除外，见 I-1） |
| 重传平滑限流 | :2882-2888 | 最老优先 + 每 tick window/4 条，避免超时后重传风暴 |
| 心跳兜底拆线 | :2855-2859 | 网络黑洞场景下 ~`response_timeout × retry_count` 内拆连，SUBMIT 不会无限悬挂 |
| 长短信部分提交语义 | :1815-1822, error.rs:49-59 | `Error::PartialSubmit` 携带已入队分片 id，调用方可精确对账 |

### 4.2 已识别问题

#### I-1【高】拆连终态事件洪峰：大窗口下 `SubmitDropped` 大部分被静默丢弃

`fail_all_pending`（:888-905）在拆连清理时被**同步**调用（`finish` → spawned cleanup，
:1207-2112 无 await 点），对每个在途 seq `make_event_ticket(false)` → spool
`try_send`。spool 容量仅 258（:44-45），而 window 上限 16384（config.rs:4）：

- 循环不 yield，dispatcher task（可能运行在另一 worker 线程）只能并发消费少量；
- 结果：window=16384 拆连时，**绝大部分 SubmitDropped 被丢弃**，仅
  `events_dropped` 计数递增（:901-903），**无逐条日志**；
- 这直接打破文档承诺（README:77-79 "每个 seq id 恰好会收到三者之一"），调用方若
  依赖该承诺做对账（哪些 seq 需要重发），会漏掉绝大多数。

*缓解因素*：拆连后 `Disconnected(Error)` 终态事件必然到达（预留槽，:2141-2145），
调用方理论上可以"Disconnected 后把所有未终结 seq 视为 dropped"兜底——但这要求调用
方自行维护未终结集合，与库的承诺不一致。

**方向**：将 dropped seq 打包成分批事件（一个 spool 槽携带多个 `SubmitDropped`），
16384 窗口仅需 256 槽即可全部送达；丢弃时输出 warn 日志（采样）。

#### I-2【中】UDH 引用耗尽误归类 `Error::Config`，缺少可重试性分类

同一重组域 256 个 reference 全部 active+cooling 时返回
`Error::Config("...已全部占用")`（:1598-1601）。这是**瞬态**条件（受 cooldown 约束，
迟早释放），归类为 Config（通常暗示永久配置错误）会误导调用方放弃重试而非退避重试。
`Error` 也缺少 `is_retryable()` 一类分类辅助（Io/Timeout → 重连后可重试；Auth/Config
→ 不可原样重试），分类知识只散落在文档。

附带说明：300s 默认冷却意味着**同一目的号码的长短信持续速率上限约 256/300s ≈
0.85 条/s**。这是防错误重组的正确取舍，但高频长短信场景必须调
`connect_with_udh_reference_cooldown`（README:138-142 已述）。

#### I-3【中】`SubmitTimeout` 不携带重试次数

超时重发是 at-least-once：消息可能已被网关受理并送达（README:84-85），调用方整条重发
即产生重复短信。事件不携带"实际重传了几次"，调用方无法据此评估"网关长时间不应答"
的严重程度（是网络黑洞还是网关半死）。

#### I-4【低-中】若干丢弃路径无日志

- 事件丢弃（`emit_event` 失败、`fail_all_pending` 失败）只计数不日志（:899-904,
  :1077-1080）；只有 overload 触发时有一条 error 级汇总（:1122-1125）。
- 未知/重复 seq 的 SubmitResp 仅 debug 级（:2521）——网络抖动后网关重发响应属正常
  现象，生产默认日志不可见。

#### I-5【低】`Queued` 状态不参与超时

writer 停滞时卡在 Queued 的 SUBMIT 永不超时、占住窗口 permit，只能靠写失败 / TCP
keepalive（60s+10s 探测，:1989-2003）/ 心跳超时 / read_idle（默认 300s）兜底拆线。
有界但延迟较长，属可接受的语义缝隙（记录在案）。

#### 协议固有限制（非缺陷，记录备考）

- **超时重发的重复可能**：响应丢失（非迟到）时库内重传 → 最终 `SubmitTimeout` →
  调用方重发可能重复。库已把决定权正确交给调用方，业务侧需按 `msg_id` 幂等。
- **分片级补发不可行**：UDH reference 内部化，新 `submit()` 必然换新 ref；只支持
  整条重发（新 ref 独立重组，不与旧片混淆，但会重复已成功的分片）。
- **seq 跨连接重复**：重连后新连接 seq 从头开始，Event 不携带连接标识；跨连接的全局
  seq 映射需调用方自行分桶（文档已声明，connection.rs:91）。

---

## 5. 编解码层结论（简述）

- 编码：帧级零拷贝 + 单次精确容量分配（pdu.rs:190-205, 375-386, 400-404），热路径
  无优化空间；GBK 走 encoding_rs 最优路径（encoding.rs:234-237）。
- 解码：帧提取零拷贝（`split_to`）；SubmitResp（上行主路径）**零堆分配**；
  DELIVER 每条 ~5 次分配（见 P-6）。
- 长短信拆分：边界常量正确（140/134 字节），UCS-2 按 `len_utf16()*2` 打包、surrogate
  pair 不劈开（encoding.rs:187-196，有测试）；惰性编码设计好（见 §2.1）。
- MD5 认证不在热路径（仅握手时一次，pdu.rs:264-279）。
- 长度校验完备：segment >255、dest >255、msg_content ≤255、帧长 ∈ [12, 65536]。

---

## 6. 配置面缺口

| 不可配参数 | 当前值 | 影响 |
|---|---|---|
| 事件背压超时 | 1s（:46） | README:94-96 承诺的行为，生产上影响巨大却不可调 |
| spool 容量 | 256（:44） | 与 window（上限 16384）量级脱节（I-1 根因之一） |
| 写批量帧数/字节 | 64 / 64KB（:52-53） | 直接影响吞吐 |
| send/incoming channel 容量 | 1000 / 1000（types.rs:9-10） | 高并发排队行为 |
| TCP 选项 | NODELAY 强制开、keepalive 60s（:1986-2003） | 无法关闭/调整 |
| timeout 扫描粒度 | 1s（types.rs:14） | 超时判定最坏多等 1 tick |
| UDH cooldown | 仅独立构造函数可调（:1294-1296） | 不在 `CmppProtocolParams` 主配置里 |
| 心跳 | 不能禁用（config.rs:48-50），超时共用 response_timeout | 低频保活场景欠灵活 |

其他：`port: i32`（config.rs:79）应为 `u16`；无本地绑定地址/TLS 选项（后者超出
CMPP 2.0 范围，记录即可）。

---

## 7. 测试覆盖与缺口

**已有**（tests/integration.rs，5 个集成测试）：正常链路 + 认证、流水线批量、异常断连
（SubmitDropped + Disconnected）、长短信部分提交（PartialSubmit）、认证拒绝。单元测试
覆盖 codec 半包/粘包/超长、PDU 编解码、编码拆分、SubmitOptions。

**缺口**（按风险排序）：

| 场景 | 现状 | 风险 |
|---|---|---|
| 超时重传全路径（retries 计数、SubmitTimeout、迟到响应抵消重传） | **无测试** | 高——这是 README 承诺的核心语义 |
| 乱序 SUBMIT_RESP | 无测试 | 高——HashMap 匹配理论上支持，未验证 |
| SUBMIT_RESP result != 0 | 无测试（mock 永远回 0） | 中 |
| 消费者停滞 → 1s 有界关闭 | 无测试 | 中——README:94-96 承诺 |
| 心跳耗尽 / read_idle 拆连 | 无测试 | 中 |
| 慢网关（秒级延迟响应） | 无测试 | 中 |
| 大窗口拆连终态完备性 | 无测试（且当前实现有 I-1 缺陷，测了会红） | 高 |

工具面：mock ISMG 是顺序读写的最小实现，**无延迟/丢响应/乱序/带宽限制注入能力**
（loadtest 的 `delay_ms` 只模拟统一 RTT，且在 examples 里不能被测试复用）。
connection.rs 3042 行核心逻辑零单元测试。

---

## 8. 测量基线（README:129-136，Windows 单连接）

| 场景 | 吞吐 | 备注 |
|---|---|---|
| loopback，window=256 | ~368k segments/s | 受事件消费管线限制（P-1） |
| RTT 50ms，window=256 | ~4.1k segments/s | 与 window/RTT 模型一致 |
| RTT 50ms，window=16 | ~259 segments/s | 同上 |
| 8 段长短信，cooldown 500ms | ~258 条/s | 受 UDH 冷却约束 |

微基准（criterion）：SUBMIT 编码 ~0.5µs、解码 ~0.4µs、8 段拆分 ~3µs。
缺口：无事件管线基准、无 DELIVER 解码基准、无 CI 性能门禁。

---

## 9. 已完成优化记录（避免重复建议）

cc82949 / e47c804 已做：parking_lot 锁替换（临界区无 await）、writer 批量 write_all、
SubmitOptions 模板化 + 惰性分片编码、UDH 池 4096 分片 + 随机起点 + cooldown、
`reserve_owned` 先于加锁、窗口上限 16384、timeout 扫描 jitter、metrics、criterion
微基准、进程内 mock 压测、响应超时预算从写出起算、UDH 引用隔离修复。

---

## 10. 优化路线图

按"先低风险高收益完整性，后高风险吞吐重构"排序，每批独立测试 + 提交：

| 批次 | 内容 | 解决 | 风险 |
|---|---|---|---|
| R-0 | 本报告落盘 | — | 无 |
| R-1 ✅ | 热路径：writer 批内锁合并（claim/complete 批量化）+ 事件直通变体（立即发布事件免 oneshot）+ reader SubmitResp 批量匹配；**附带提前完成 R-2 的拆连终态事件分批**——实测发现大窗口饱和拆连时 `close_event_spool` 的 Terminal 会被满 spool 挤掉，dispatcher 永久阻塞（进程挂起），故将 SubmitDropped 分批（128/槽）与 Terminal 重试投递一并落地 | P-1, P-2, P-3, I-1 | 中（需保语义红线） |
| R-2（待运行验证） | `Error::ResourceExhausted` + `is_retryable()`；`SubmitTimeout.attempts` 表示含首次的完整写出次数；修复触发背压失败的首条事件漏计；补齐最大窗口与停滞消费、超时计数测试 | I-2, I-3、终态交付边界 | 低 |
| R-3（待运行验证） | 已接入背压预算/spool 容量与 window 联动/组批参数/TCP 开关及 keepalive 参数；新增成功写批次数、帧数和事件工单深度峰值，loadtest 参数与输出同步 | §6 | 低 |
| R-4（待运行验证） | 已新增可配置故障 mock、乱序/非零响应、丢响应重传、迟到重复响应、心跳耗尽和活跃连接消费者背压用例；排队重传取消以状态级注入验证 | §7 | 低 |
| R-5 | 微优化（按 R-1 后的新基准数据决定）：DELIVER 解码减分配、UCS-2 chunks、codec reserve、timeout 索引化 | P-4~P-7 | 低 |

R-1 实测（Windows，loopback window=256 parallel=8）：峰值吞吐 311k → 377k
segments/s（+21%），事件丢弃 107 → 0-70，修复前约 1/3 概率进程挂起、修复后
6/6 干净退出；RTT 50ms 场景 4.1k segments/s 与基线一致。

**语义红线**（任何批次不得破坏）：消费者持续消费且未触发事件丢弃时，每 seq 恰好一个终态事件；DELIVER 写门控；事件全局
FIFO 顺序；UDH 隔离；超时预算起算点；锁序 `pending_submits → submit_admission`。

### R-2 本轮收尾（2026-09-21）

- 背压超时、接收端关闭或拆连投递预算耗尽时允许丢失终态；已接受但缺失终态的 seq 为结果未知，不能直接当作发送失败重发。
- `attempts` 为完整写出次数（含首次），首次为 1、重传两次为 3；不代表网关已接收。`is_retryable()` 只提供重试分类，不保证重发安全或成功。
- 保留 R-1 已实现的 128 条/批拆连投递，未扩展配置与指标；补齐 Direct/Batch 及 Deferred 路径触发投递失败的当前事件丢弃计数。
- 测试覆盖最大窗口 16384 正常消费时终态唯一且完整、停滞消费时有界退出与精确丢弃计数、首次及重传后超时次数、UDH 耗尽与错误分类。
- `cargo test` 被自动审批规则拒绝（禁止运行 Rust 程序、测试或基准），未执行；不将新增用例视为已通过。批次 2 保持待运行验证状态；后续继续进度见 R-3。
- 静态验证：`cargo clippy --all-targets -- -D warnings` 与 `git diff --check` 均通过；Clippy 包含测试目标检查，不等于测试运行通过。本轮未创建批次完成提交。

### R-3 继续实施（2026-09-21）

- 用户已授权继续计划和必要核验。默认参数保持不变；spool 最小容量按已实现的 128 条/批计算，写入字节数沿用完整帧的组批阈值语义。参数、范围及指标口径见 README 的性能章节。
- Direct 在增加当前工单深度前检查容量，与 Deferred 一致，修复容量为 1 时首条事件即误判溢出的边界。
- 写入计数仅在整批 write_all 成功后更新；事件峰值复用工单深度追踪，仅出现新峰值时增加一次原子读改写。新增指标带来的开销尚未实测。
- 新增配置约束、TCP 选项、自定义背压预算与预留 Terminal 槽、批次深度释放、单帧和字节阈值及指标测试；loadtest 开放组批/事件参数并输出平均批大小与事件峰值。
- `cargo test` 仍被相同自动审批规则拒绝；未运行测试、压测或基准。批次 2、3 均待运行验证，不创建完成提交；批次 4 继续进度见下一节。

### R-4 故障注入与测试基建（2026-09-21）

- 新增 `tests/common/mod.rs`，提供响应延迟、按 seq 丢弃前 N 次或全部响应、批量逆序、非零结果码和心跳丢响应。使用 FramedRead 保留半帧，延迟响应排入到期队列，不阻塞正常读帧与心跳处理；测试结束传播后台任务错误并清理任务。
- 新增五个集成用例：两轮满窗口逆序失败响应及许可释放；丢响应后原 seq/报文重传且只完成一次；迟到重复响应不产生第二个终态；心跳预算耗尽时所有在途分片收到 SubmitDropped；网关在线而事件消费者停滞时因背压关闭且准确统计缺失终态。
- `connection.rs` 增加状态级竞态用例：实际连接并写出首次 SUBMIT 后，注入 Queued attempt 2，固定迟到响应先于 writer claim 的顺序，断言旧 attempt 剔除、窗口释放且终态唯一。它验证状态转换与领取逻辑，不替代真实调度压力测试。
- 本轮没有新增生产路径优化。`cargo test` 再次在执行前被自动审批规则拒绝；新增测试仅通过 `cargo clippy --all-targets -- -D warnings` 静态检查，不表示运行通过。批次 2–4 待运行验证；批次 5 无测量依据，继续保留。

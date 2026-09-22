# cmppprotocol

面向 Rust 的 **CMPP 2.0 client** protocol library，用于通过长连接 TCP link 将 Service Provider (SP) 连接到CMPP ISMG。

## 功能特性

- **Typed PDU model** (`pdu`)：每一种 CMPP 2.0 message 都是 strongly typed struct，
统一收敛到 `Pdu` enum，并支持二进制 `encode`/`decode`。
- **Async codec** (`CmppFrameCodec`)：基于 `tokio_util` 的 `Decoder`/`Encoder`，
处理 TCP framing（半包/不完整包、长度校验），并产出 `Frame { sequence_id, pdu }`。
- **Async connection** (`CmppConnection`)：
  - `connect()` 完成登录 handshake，并在返回前校验 ISMG 的 `AuthenticatorISMG`。
  - `submit()` 是 **non-blocking**：它应用 sliding-window backpressure，并立即返回分片的
  sequence id（符合 CMPP pipeline、async 的特性）。
          - 所有响应都会以 `Event` 形式从 `take_events()` channel 到达：`SubmitResp`、
  `SubmitTimeout`、`SubmitDropped`（连接在收到响应前拆除）、`Deliver`（status reports / MO）
  和 `Disconnected`。自动重传、
  ACTIVE_TEST heartbeat 和优雅的 TERMINATE teardown 都在内部处理。
- **Charset & long SMS** (`encoding`)：支持 ASCII/UCS2 编码和 6-byte UDH 拼接，
保留字符边界（包括 UTF-16 surrogate 边界）。
- **Ergonomic submit** (`SubmitOptions`)：所有 SUBMIT 字段都可配置并带有合理默认值；
long message 会被拆分为多个分片。

## 快速开始

```rust,no_run
use cmppprotocol::{CmppConnection, CmppConfig, CmppProtocolParams, Event, SubmitOptions};

#[tokio::main]
async fn main() -> cmppprotocol::Result<()> {
    let config = CmppConfig {
        host: "127.0.0.1".into(),
        port: 7890,
        account: "901234".into(),
        password: "secret".into(),
        version: cmppprotocol::CMPP_VERSION_20,
        protocol_params: CmppProtocolParams::default(),
    };

    let conn = CmppConnection::connect(config).await?;

    let mut events = conn.take_events().await.expect("events 首次可用");
    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            match event {
                Event::SubmitResp { sequence_id, result, .. } => {
                    println!("响应 seq={} result={}", sequence_id, result);
                }
                Event::Deliver(deliver) => {
                    if let Some(report) = deliver.report() {
                        println!("报告 {} -> {}", report.msg_id_hex(), report.stat);
                    }
                }
                Event::SubmitTimeout { sequence_id, attempts } => {
                    println!("超时 seq={}（已尝试 {} 次，可能重复下发）", sequence_id, attempts)
                }
                Event::SubmitDropped { sequence_id } => {
                    println!("连接断开，未收到响应 seq={}", sequence_id)
                }
                Event::Disconnected(e) => { println!("连接已断开: {}", e); break; }
            }
        }
    });

    // submit() 是 non-blocking，并返回各分片的 sequence id。
    let opts = SubmitOptions::new("SVC", "901234", "10690001", "13800138000");
    let seq_ids = conn.submit(&opts, "Hello World", None).await?;
    println!("已提交 {} 个分片", seq_ids.len());

    conn.close().await;
    Ok(())
}
```

可运行的 CLI 示例见 `examples/send_sms.rs`。

## 语义保证与运维注意事项

- **终态交付边界**：消费者持续消费且未触发事件丢弃时，每个被 `submit()` 接受的
  sequence id 恰好会收到一个终态事件——
  `SubmitResp`（收到响应）、`SubmitTimeout`（重试预算耗尽）或 `SubmitDropped`
  （连接在收到响应前被拆除）。背压超时、接收端关闭或拆连投递预算耗尽时允许丢失，
  并计入 `events_dropped`。调用方须记录已接受的 seq；缺失终态表示“结果未知”，
  不能直接视为发送失败重发。`SubmitDropped` 也不代表网关未受理。
- **Disconnected 事件语义**：调用方主动 `close()` 的优雅关闭以事件通道结束
  （`recv()` 返回 `None`）表达，不发布 `Disconnected` 事件；`Disconnected`
  仅在异常拆除（I/O 错误、超时、对端 TERMINATE、事件积压有界关闭）时发布
  并携带原因。
- **at-least-once 重传**：`SUBMIT_RESP` 丢失但 ISMG 实际已受理时，同 sequence id
  的重传可能导致网关侧重复计费/重复下发。业务侧应按 `msg_id` 做幂等。
  `Event::SubmitTimeout` 携带完整写出次数 `attempts`（含首次：首次为 1，重传两次为 3），
  写出不代表网关已接收，次数越大重复风险越高。
- **错误可重试性**：`Error::is_retryable()` 区分瞬态失败（`Io`/`Timeout`/
  `Closed`/`ResourceExhausted` 等，重连或退避后可重试）与持久失败（`Auth`/
  `Decode`/`Config` 等，重试前需修正根因）。UDH 引用池耗尽属于
  `ResourceExhausted`（瞬态，受 cooldown 约束）。可重试不代表重发安全或必然成功；
  `PartialSubmit` 的未入队部分与已入队部分须分开处理。
- **未确认 DELIVER**：`Event::Deliver` 仅在对应 `DELIVER_RESP` 已完整写到 socket 后
  才会发布。若连接在确认前异常拆除，该 DELIVER 会被丢弃且不会发布事件——
  多数 ISMG 会在重连后重推未确认的 DELIVER，依赖此行为的应用无需额外处理，
  不能依赖的应用应在 `Disconnected` 后自行容忍可能的重复。
- **长短信部分提交**：多 segment 长短信逐段发送；若中途连接关闭，`submit()`
  返回 `Error::PartialSubmit` 并携带已入队 segment 的 sequence id（适用上述终态
  交付边界），但终端不会重组出完整短信。调用方可据此重发未入队部分
  或整体重发（新连接 + 新 UDH reference）。
- **事件消费必须常驻且快速**：event channel 可用容量不足、持续阻塞投递超过配置预算（默认 1 秒）会触发
  有界关闭（`Disconnected(ChannelClosed)`；若连接已因其他原因关闭则保留原原因），
  后续事件进入丢弃模式；不保证整个关闭流程恰好在 1 秒内完成。请把事件循环放在
  独立 task 中持续消费。
- **运行时 metrics**：`conn.metrics()` 返回 `ConnectionMetrics` 快照
  （admitted / responses / retries / timeouts / delivers / dropped events /
  in-flight），适合周期性采集用于监控与容量评估。
- **窗口大小**：`window_size` 默认 16，硬上限 16384。超过 256 时 `connect()`
  会输出 warning——多数 ISMG 的单连接窗口上限为 256，请先确认网关配置。
  单连接吞吐约为 `window_size / RTT`。

## 性能

运行时 metrics（`conn.metrics()`）提供 admitted / responses / retries / timeouts /
delivers / dropped events / in-flight 计数，适合接入监控系统。

### 微基准

```bash
cargo bench --bench protocol
```

覆盖 PDU encode/decode、codec framing 和长短信拆分（参考量级：SUBMIT 编码
~0.5µs、解码 ~0.4µs、8 段长短信拆分 ~3µs）。

### 端到端压测

`examples/loadtest.rs` 内置进程内 mock ISMG（可配置 RTT），测量稳态吞吐、
延迟分位与窗口回压：

```bash
cargo run --release --example loadtest -- duration=6 window=256 delay_ms=50 parallel=8
# 长短信（8 段）+ 自定义 UDH cooldown：
cargo run --release --example loadtest -- duration=6 long=1 udh_cooldown_ms=500
```

实测参考（Windows，单连接）：

| 场景 | 实测吞吐 | 说明 |
|---|---|---|
| loopback，RTT≈0，window=256 | ~368k segments/s | 受事件消费管线限制，库零丢弃 |
| RTT 50ms，window=256 | ~4.1k segments/s | 与 `window/RTT` 模型一致（62.8ms 实测 RTT） |
| RTT 50ms，window=16 | ~259 segments/s | 同上 |
| 8 段长短信，cooldown 500ms | ~258 条/s | 受窗口限制；默认 300s cooldown 时为 ~0.85 条/s/目的地 |

**长短信 UDH cooldown 约束**：同一 `(Src_Id, Dest_Terminal_Id)` 重组域只有 256 个
8-bit reference，释放后默认进入 300s cooldown（防网关/终端错误重组）。对同一
目的地的高频长短信场景，请通过 `connect_with_udh_reference_cooldown` 调低。
cooldown 状态不跨进程存活；UDH reference 起点在每次启动时随机化，降低重启后
与重启前在途分片撞号的概率。

## 范围

这个 crate 仅实现 **CMPP 2.0** 的 **client** 侧。重连逻辑有意交给调用方处理
（connection 会暴露清晰的错误和 closed 状态）。CMPP 3.0 以及 ISMG/server 角色不在范围内。

## 许可证

MIT
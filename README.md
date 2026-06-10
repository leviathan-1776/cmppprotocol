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
  `SubmitTimeout`、`Deliver`（status reports / MO）和 `Disconnected`。自动重传、
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
                Event::SubmitTimeout { sequence_id } => println!("超时 seq={}", sequence_id),
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

## 范围

这个 crate 仅实现 **CMPP 2.0** 的 **client** 侧。重连逻辑有意交给调用方处理
（connection 会暴露清晰的错误和 closed 状态）。CMPP 3.0 以及 ISMG/server 角色不在范围内。

## 许可证

MIT
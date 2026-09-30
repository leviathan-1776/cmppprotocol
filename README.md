# cmppprotocol

一个基于 Rust 和 Tokio 的 CMPP 2.0 客户端协议库，用于通过 TCP 长连接连接短信网关（ISMG），发送短信并接收短信上行和状态报告。

## 功能概览

- 支持 CMPP 2.0 客户端登录和连接管理。
- 支持发送普通短信和长短信。
- 支持 ASCII、UCS2 编码以及中文短信。
- 支持接收短信上行（MO）和状态报告（Report）。
- 通过事件接收发送结果、上行消息、状态报告和连接状态变化。
- 自动处理心跳、连接关闭以及发送响应超时。

## 环境要求

- Rust 1.85 或更高版本
- 支持 Tokio 的异步运行时
- 可访问 CMPP 2.0 ISMG 网关

## 添加依赖

在项目的 `Cargo.toml` 中添加：

```toml
[dependencies]
cmppprotocol = { path = "../cmppprotocol" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

如果使用已发布的 crate，也可以将 `path` 替换为对应的版本号。

## 快速开始

```rust,no_run
use cmppprotocol::{
    CmppConfig, CmppConnection, CmppProtocolParams, Event, SubmitOptions,
    CMPP_VERSION_20,
};

#[tokio::main]
async fn main() -> cmppprotocol::Result<()> {
    let config = CmppConfig {
        host: "127.0.0.1".into(),
        port: 7890,
        account: "901234".into(),
        password: "secret".into(),
        version: CMPP_VERSION_20,
        protocol_params: CmppProtocolParams::default(),
    };

    let connection = CmppConnection::connect(config).await?;
    let mut events = connection
        .take_events()
        .await
        .expect("事件接收器只能获取一次");

    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            match event {
                Event::SubmitResp { sequence_id, result, .. } => {
                    println!("短信响应：seq={sequence_id}，result={result}");
                }
                Event::Deliver(deliver) => {
                    if let Some(report) = deliver.report() {
                        println!("状态报告：{} -> {}", report.msg_id_hex(), report.stat);
                    } else {
                        println!("收到短信上行");
                    }
                }
                Event::SubmitTimeout { sequence_id, attempts, .. } => {
                    println!("短信响应超时：seq={sequence_id}，尝试次数={attempts}");
                }
                Event::SubmitDropped { sequence_id } => {
                    println!("连接关闭，未收到响应：seq={sequence_id}");
                }
                Event::Disconnected(reason) => {
                    println!("连接断开：{reason}");
                    break;
                }
            }
        }
    });

    let options = SubmitOptions::new("SVC", "901234", "10690001", "13800138000");
    connection.submit(&options, "Hello World", None).await?;
    connection.close().await;
    Ok(())
}
```

`submit` 返回各短信分片对应的序列号。它只表示短信已经交给连接处理，最终结果需要继续通过事件接收器判断。

可运行示例：

```bash
cargo run --example send_sms
```

## 发送短信

使用 `SubmitOptions::new` 设置常用的发送字段，再调用 `submit`：

```rust,no_run
let options = SubmitOptions::new("SVC", "901234", "10690001", "13800138000");
let sequence_ids = connection.submit(&options, "这是一条短信", None).await?;
println!("已提交 {} 个短信分片", sequence_ids.len());
```

短信内容较长时，库会自动进行分片并使用 UDH 组织成长短信。调用方仍应根据业务需要限制内容长度，并为发送结果做好重试或人工处理安排。

如果需要设置更多 SUBMIT 字段，可以在 `SubmitOptions` 上继续配置；未配置的字段使用库提供的默认值。

## 接收事件

事件接收器需要持续消费，常见事件包括：

- `Event::SubmitResp`：收到短信网关响应，可根据 `result` 判断发送结果。
- `Event::SubmitTimeout`：等待响应超时，表示结果可能未知，不应直接认定网关未受理。
- `Event::SubmitDropped`：连接关闭前没有收到响应。
- `Event::Deliver`：收到短信上行或状态报告。
- `Event::Disconnected`：连接异常断开，并携带断开原因。

建议在独立的 Tokio task 中持续处理事件，并在业务侧记录序列号、手机号和消息内容等必要信息。

主动调用 `close()` 时，事件流会正常结束；异常断开时通常会收到 `Disconnected`。断线重连需要由调用方根据业务场景实现。

## 配置连接

`CmppConfig` 主要包含以下信息：

| 字段 | 说明 |
| --- | --- |
| `host` | ISMG 主机地址 |
| `port` | ISMG 端口 |
| `account` | SP 账号 |
| `password` | SP 密码 |
| `version` | CMPP 协议版本，CMPP 2.0 使用 `CMPP_VERSION_20` |
| `protocol_params` | 协议连接参数，通常使用 `CmppProtocolParams::default()` |

登录失败、配置错误、网络错误和协议解析错误都会通过 `Result` 或事件返回，调用方应记录错误并根据错误类型决定是否重连。

## 使用建议

1. 连接建立后尽快启动事件消费任务。
2. 为每次发送保存返回的序列号，结合事件记录最终处理结果。
3. 超时或连接断开时，先确认业务是否允许重发，避免产生重复短信。
4. 断线后重新创建连接并重新登录，不要继续使用已经关闭的连接。
5. 对状态报告和短信上行做好幂等处理，因为网关可能在重连后再次推送未确认消息。

## 项目范围

本项目仅实现 CMPP 2.0 的客户端侧能力，不包含：

- CMPP 3.0；
- ISMG 服务端；
- 自动重连策略；
- 业务层的短信模板、计费和消息幂等服务。

## 许可证

MIT

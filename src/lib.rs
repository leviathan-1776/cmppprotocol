//! # cmppprotocol
//!
//! **CMPP 2.0 client** protocol library，用于将 SP 连接到 CMPP ISMG。
//!
//! 功能特性：
//! - Typed [`Pdu`] model，支持二进制 encode/decode（[`pdu`]），并提供 `tokio_util`
//!   [`CmppFrameCodec`]，用于简洁地对 TCP streams 进行 framing。
//! - Async [`CmppConnection`]，包含 non-blocking [`CmppConnection::submit`]
//!   （仅在 sliding-window backpressure 时等待）和用于接收所有 response 的 [`Event`] stream；
//!   它会执行登录 handshake（含 authenticator 校验）、timeout 后重传、发送 ACTIVE_TEST heartbeat，
//!   并使用 TERMINATE 优雅拆除 link。
//! - Charset encoding 和 long-SMS UDH splitting（[`encoding`]）。
//! - 通过 [`SubmitOptions`] 便捷构造 message。
//!
//! ## 快速开始
//!
//! ```no_run
//! use cmppprotocol::{CmppConnection, CmppConfig, CmppProtocolParams, Event, SubmitOptions};
//!
//! # async fn run() -> cmppprotocol::Result<()> {
//! let config = CmppConfig {
//!     host: "127.0.0.1".into(),
//!     port: 7890,
//!     account: "901234".into(),
//!     password: "secret".into(),
//!     version: cmppprotocol::CMPP_VERSION_20,
//!     protocol_params: CmppProtocolParams::default(),
//! };
//!
//! let conn = CmppConnection::connect(config).await?;
//!
//! // 消费所有 async events（responses、reports、disconnect）。
//! let mut events = conn.take_events().await.expect("events 首次可用");
//! tokio::spawn(async move {
//!     while let Some(event) = events.recv().await {
//!         match event {
//!             Event::SubmitResp { sequence_id, msg_id, result } => {
//!                 println!("响应 seq={} msg_id={} result={}", sequence_id, Event::msg_id_hex(&msg_id), result);
//!             }
//!             Event::Deliver(deliver) => {
//!                 if let Some(report) = deliver.report() {
//!                     println!("报告 {} -> {}", report.msg_id_hex(), report.stat);
//!                 }
//!             }
//!             Event::SubmitTimeout { sequence_id } => println!("超时 seq={}", sequence_id),
//!             Event::Disconnected(e) => { println!("连接已断开: {}", e); break; }
//!         }
//!     }
//! });
//!
//! // submit() 是 non-blocking：它返回各 segment 的 sequence id。
//! let opts = SubmitOptions::new("SVC", "901234", "10690001", "13800138000");
//! let seq_ids = conn.submit(&opts, "Hello World", None).await?;
//! println!("已提交 {} 个分片: {:?}", seq_ids.len(), seq_ids);
//!
//! conn.close().await;
//! # Ok(())
//! # }
//! ```

mod codec;
mod config;
mod connection;
mod error;
mod types;

pub mod encoding;
pub mod pdu;
pub mod submit;

pub use codec::CmppFrameCodec;
pub use config::{CmppConfig, CmppProtocolParams};
pub use connection::{CmppConnection, Event};
pub use encoding::decode_msg_content;
pub use error::{Error, Result};
pub use pdu::{
    Connect, ConnectResp, Deliver, DeliverReport, DeliverResp, Frame, Pdu, Submit, SubmitResp,
    compute_authenticator_ismg, compute_authenticator_source,
};
pub use submit::SubmitOptions;
pub use types::{
    CMPP_ACTIVE_TEST, CMPP_ACTIVE_TEST_RESP, CMPP_CONNECT, CMPP_CONNECT_RESP, CMPP_DELIVER,
    CMPP_DELIVER_RESP, CMPP_HEADER_LENGTH, CMPP_SUBMIT, CMPP_SUBMIT_RESP, CMPP_TERMINATE,
    CMPP_TERMINATE_RESP, CMPP_VERSION_20, CmppHeader,
};

//! CMPP protocol library 的错误类型。

use thiserror::Error;

/// 这个 crate 通用的 result type。
pub type Result<T> = std::result::Result<T, Error>;

/// 进行 CMPP 2.0 通信时可能出现的错误。
#[derive(Error, Debug)]
pub enum Error {
    /// 底层 I/O 失败。
    #[error("I/O 错误: {0}")]
    Io(#[from] std::io::Error),

    /// 接收到的 frame 无法 decode 为有效 PDU。
    #[error("decode 错误: {0}")]
    Decode(String),

    /// 无法建立 TCP connection。
    #[error("connect 失败: {0}")]
    Connect(String),

    /// CONNECT_RESP 返回非零 status（认证被拒绝）。
    /// status code 见 CMPP 2.0（1=invalid struct，2=wrong source addr，
    /// 3=auth error，4=version too high，...）。
    #[error("认证失败，status={0}")]
    Auth(u8),

    /// ISMG 的 `AuthenticatorISMG` 与预期 MD5 digest 不匹配。
    #[error("CONNECT_RESP 中的 AuthenticatorISMG 不匹配")]
    AuthenticatorMismatch,

    /// 配置的 timeout 内未收到 response（例如 SUBMIT_RESP）。
    #[error("response 超时")]
    Timeout,

    /// connection 已由本地或 peer 关闭。
    #[error("connection 已关闭")]
    Closed,

    /// 内部 send channel 已关闭（writer task 已退出）。
    #[error("send channel 已关闭")]
    ChannelClosed,

    /// peer 发送了 CMPP_TERMINATE，link 已被拆除。
    #[error("由 peer 终止")]
    Terminated,

    /// 调用方提供了无效配置。
    #[error("config 无效: {0}")]
    Config(String),
}

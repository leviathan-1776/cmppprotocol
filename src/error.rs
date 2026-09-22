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

    /// 连接在 submit 过程中关闭：`sequence_ids` 中的 segment 已入队，正常消费时各自
    /// 收到终态事件（`SubmitResp`/`SubmitTimeout`/`SubmitDropped`）；事件丢弃时缺失终态
    /// 表示结果未知，不能直接视为发送失败。其余
    /// segment 未能入队；`source` 为导致中断的底层错误。
    #[error("连接关闭，已有 {} 个 segment 入队", .sequence_ids.len())]
    PartialSubmit {
        /// 已成功入队的 segment sequence id。
        sequence_ids: Vec<u32>,
        /// 导致中断的底层错误。
        #[source]
        source: Box<Error>,
    },

    /// 瞬态资源不足（例如同一重组域的 8-bit UDH reference 全部处于占用或
    /// cooldown 中）。与 [`Error::Config`] 不同：这不是配置错误，等待资源
    /// 释放（或退避）后可重试，但不保证下次成功。
    #[error("瞬态资源不足: {0}")]
    ResourceExhausted(String),

    /// 调用方提供了无效配置。
    #[error("config 无效: {0}")]
    Config(String),
}

impl Error {
    /// 该错误是否值得在（可能重连后的）后续尝试中重试同一操作。
    ///
    /// 返回 `true` 不代表重发安全或必然成功。已入队的 segment 可能已被网关受理；
    /// `PartialSubmit` 应区分未入队部分与已入队部分，缺失终态的部分应按结果未知处理。
    ///
    /// 返回 `false` 表示重试大概率复现同样的失败（配置/凭证/协议不兼容等
    /// 持久性问题），应先修正根因：
    ///
    /// | 错误 | 可重试 | 说明 |
    /// |---|---|---|
    /// | `Io` / `Connect` / `Timeout` | 是 | 网络类失败，重连后重试 |
    /// | `Closed` / `ChannelClosed` / `Terminated` | 是 | 连接已拆，重连后重试 |
    /// | `ResourceExhausted` | 是 | 瞬态占用，退避后重试 |
    /// | `PartialSubmit` | 取决于 source | 未入队部分可重发，已入队部分以终态事件为准 |
    /// | `Auth` / `AuthenticatorMismatch` | 否 | 凭证/密钥问题 |
    /// | `Decode` | 否 | 协议流损坏或不兼容，重试前需排查 |
    /// | `Config` | 否 | 配置错误 |
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::Io(_) | Error::Connect(_) | Error::Timeout => true,
            Error::Closed | Error::ChannelClosed | Error::Terminated => true,
            Error::ResourceExhausted(_) => true,
            Error::PartialSubmit { source, .. } => source.is_retryable(),
            Error::Auth(_) | Error::AuthenticatorMismatch | Error::Decode(_) | Error::Config(_) => {
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryability_classification() {
        assert!(Error::Io(std::io::Error::other("boom")).is_retryable());
        assert!(Error::Timeout.is_retryable());
        assert!(Error::Connect("refused".into()).is_retryable());
        assert!(Error::Closed.is_retryable());
        assert!(Error::ChannelClosed.is_retryable());
        assert!(Error::Terminated.is_retryable());
        assert!(Error::ResourceExhausted("udh".into()).is_retryable());
        assert!(!Error::Auth(3).is_retryable());
        assert!(!Error::AuthenticatorMismatch.is_retryable());
        assert!(!Error::Decode("bad".into()).is_retryable());
        assert!(!Error::Config("bad".into()).is_retryable());
        assert!(
            Error::PartialSubmit {
                sequence_ids: vec![1],
                source: Box::new(Error::Closed),
            }
            .is_retryable()
        );
        assert!(
            !Error::PartialSubmit {
                sequence_ids: vec![1],
                source: Box::new(Error::Auth(1)),
            }
            .is_retryable()
        );
    }
}

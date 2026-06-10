//! Protocol 级常量和固定 CMPP message header。

use std::time::Duration;

// Framing 限制
pub(crate) const CMPP_MAX_MESSAGE_LENGTH: usize = 65536;
/// 固定 CMPP message header 的长度，单位为 bytes（Total_Length + Command_Id + Sequence_Id）。
pub const CMPP_HEADER_LENGTH: usize = 12;
pub(crate) const SEND_CHANNEL_CAPACITY: usize = 1000;
pub(crate) const INCOMING_CHANNEL_CAPACITY: usize = 1000;
pub(crate) const CODEC_INITIAL_CAPACITY: usize = 65536;

// Timer task 间隔
pub(crate) const TIMEOUT_CHECK_INTERVAL: Duration = Duration::from_secs(1);
pub(crate) const TIMEOUT_CHECK_IDLE_INTERVAL: Duration = Duration::from_secs(5);

/// CMPP 2.0 protocol version byte。
pub const CMPP_VERSION_20: u8 = 0x20;

// Command IDs（CMPP 2.0）
/// `CMPP_CONNECT` command id。
pub const CMPP_CONNECT: u32 = 0x0000_0001;
/// `CMPP_CONNECT_RESP` command id。
pub const CMPP_CONNECT_RESP: u32 = 0x8000_0001;
/// `CMPP_TERMINATE` command id。
pub const CMPP_TERMINATE: u32 = 0x0000_0002;
/// `CMPP_TERMINATE_RESP` command id。
pub const CMPP_TERMINATE_RESP: u32 = 0x8000_0002;
/// `CMPP_SUBMIT` command id。
pub const CMPP_SUBMIT: u32 = 0x0000_0004;
/// `CMPP_SUBMIT_RESP` command id。
pub const CMPP_SUBMIT_RESP: u32 = 0x8000_0004;
/// `CMPP_DELIVER` command id。
pub const CMPP_DELIVER: u32 = 0x0000_0005;
/// `CMPP_DELIVER_RESP` command id。
pub const CMPP_DELIVER_RESP: u32 = 0x8000_0005;
/// `CMPP_ACTIVE_TEST` command id。
pub const CMPP_ACTIVE_TEST: u32 = 0x0000_0008;
/// `CMPP_ACTIVE_TEST_RESP` command id。
pub const CMPP_ACTIVE_TEST_RESP: u32 = 0x8000_0008;

/// 固定的 12-byte CMPP message header。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CmppHeader {
    /// message 总长度（header + body），单位为 bytes。
    pub total_length: u32,
    /// Command id（见 `CMPP_*` 常量）。
    pub command_id: u32,
    /// 用于关联 request 和 response 的 Sequence id。
    pub sequence_id: u32,
}

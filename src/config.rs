// 配置相关

/// window_size 允许的硬上限。
pub(crate) const MAX_WINDOW_SIZE: usize = 16384;

/// 超过该窗口大小时 connect 会输出 warning（多数 ISMG 以 256 为常见上限）。
pub(crate) const WINDOW_SIZE_ADVISORY_MAX: usize = 256;

/// CMPP protocol 参数配置。
#[derive(Debug, Clone)]
pub struct CmppProtocolParams {
    /// Link detection 间隔（秒），推荐值：180（3 分钟）。
    pub heartbeat_interval: u64,
    /// Response timeout（秒），推荐值：60。
    pub response_timeout: u64,
    /// Retry count，推荐值：3（实际会重试 N-1 次，即 2 次）。
    pub retry_count: u32,
    /// Sliding window size，推荐值：16。允许配置更大的窗口（受
    /// [`crate::CmppProtocolParams`] 的硬上限约束），但超过 256 时 connect
    /// 会输出 warning，提醒确认 ISMG 支持该窗口大小。
    pub window_size: usize,
    /// TCP connection timeout（秒），推荐值：10。
    pub connect_timeout: u64,
    /// Read idle timeout（秒），推荐值：300（5 分钟）。
    pub read_idle_timeout: u64,
    /// 校验 CONNECT_RESP 中 ISMG 的 `AuthenticatorISMG`。有些宽松的 gateway
    /// 会将其留为 0；设为 false 可与这类 gateway 互通。
    pub verify_authenticator: bool,
    /// 公开事件通道持续背压的等待预算（毫秒），默认 1000；不等于整个关闭流程时限。
    pub event_backpressure_timeout_ms: u64,
    /// 普通 event spool 容量，1..=16384；实际至少为 ceil(window_size / 128)。
    /// 另保留 2 个紧急槽。容量按 spool item 计，拆连批次含多个事件。
    pub event_spool_capacity: usize,
    /// 每批最多写出的帧数，1..=16384，默认 64。
    pub write_batch_max_frames: usize,
    /// 组批字节阈值，必须大于 0，默认 64KiB；最后一帧可能使总量超过阈值，不拆帧。
    pub write_batch_max_bytes: usize,
    /// 是否启用 TCP_NODELAY，默认 true。
    pub tcp_nodelay: bool,
    /// TCP keepalive 空闲时间（秒），None 禁用，默认 Some(60)。
    pub tcp_keepalive_secs: Option<u64>,
    /// TCP keepalive 探测间隔（秒），启用时须大于 0，默认 10。
    pub tcp_keepalive_interval_secs: u64,
}

impl Default for CmppProtocolParams {
    fn default() -> Self {
        Self {
            heartbeat_interval: 180,    // C=3 分钟
            response_timeout: 60,       // T=60 秒
            retry_count: 3,             // N=3
            window_size: 16,            // W=16
            connect_timeout: 10,        // Connection timeout 10 秒
            read_idle_timeout: 300,     // Read idle timeout 5 分钟
            verify_authenticator: true, // 默认严格校验
            event_backpressure_timeout_ms: 1000,
            event_spool_capacity: 256,
            write_batch_max_frames: 64,
            write_batch_max_bytes: 64 * 1024,
            tcp_nodelay: true,
            tcp_keepalive_secs: Some(60),
            tcp_keepalive_interval_secs: 10,
        }
    }
}

impl CmppProtocolParams {
    /// 校验参数是否合法。
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.heartbeat_interval == 0 {
            return Err("heartbeat_interval 不能为 0".to_string());
        }
        if self.response_timeout == 0 {
            return Err("response_timeout 不能为 0".to_string());
        }
        if self.retry_count == 0 {
            return Err("retry_count 不能为 0".to_string());
        }
        if self.window_size == 0 || self.window_size > MAX_WINDOW_SIZE {
            return Err(format!("window_size 必须在 1 到 {} 之间", MAX_WINDOW_SIZE));
        }
        if self.connect_timeout == 0 {
            return Err("connect_timeout 不能为 0".to_string());
        }
        if self.read_idle_timeout == 0 {
            return Err("read_idle_timeout 不能为 0".to_string());
        }
        if self.event_backpressure_timeout_ms == 0 {
            return Err("event_backpressure_timeout_ms 不能为 0".to_string());
        }
        if !(1..=MAX_WINDOW_SIZE).contains(&self.event_spool_capacity) {
            return Err("event_spool_capacity 必须在 1 到 16384 之间".to_string());
        }
        if !(1..=MAX_WINDOW_SIZE).contains(&self.write_batch_max_frames) {
            return Err("write_batch_max_frames 必须在 1 到 16384 之间".to_string());
        }
        if self.write_batch_max_bytes == 0 {
            return Err("write_batch_max_bytes 不能为 0".to_string());
        }
        if let Some(idle) = self.tcp_keepalive_secs {
            if idle == 0
                || idle > u64::from(u32::MAX) / 1000
                || self.tcp_keepalive_interval_secs == 0
                || self.tcp_keepalive_interval_secs > u64::from(u32::MAX) / 1000
            {
                return Err("TCP keepalive 秒数须为正且换算毫秒后不超过 u32 上限".to_string());
            }
        }
        Ok(())
    }
}

/// CMPP connection 配置。
#[derive(Debug, Clone)]
pub struct CmppConfig {
    /// Server 地址。
    pub host: String,
    /// Server 端口。
    pub port: i32,
    /// Account（在 CONNECT 中作为 Source_Addr / SP id）。
    pub account: String,
    /// Shared secret / password。
    pub password: String,
    /// CMPP version（CMPP 2.0 必须为 0x20）。
    pub version: u8,
    /// CMPP protocol 参数。
    pub protocol_params: CmppProtocolParams,
}

impl CmppConfig {
    /// 校验配置是否合法。
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.host.is_empty() {
            return Err("host 不能为空".to_string());
        }
        if self.port < 1 || self.port > 65535 {
            return Err("port 必须在 1 到 65535 之间".to_string());
        }
        if self.account.is_empty() {
            return Err("account 不能为空".to_string());
        }
        if self.account.len() > 6 {
            return Err("account 长度不能超过 6 bytes".to_string());
        }
        if self.password.is_empty() {
            return Err("password 不能为空".to_string());
        }
        if self.version != crate::types::CMPP_VERSION_20 {
            return Err("仅支持 CMPP 2.0（version 0x20）".to_string());
        }
        self.protocol_params.validate()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tuning_limits_reject_invalid_values_before_connect() {
        let defaults = CmppProtocolParams::default();
        assert!(defaults.validate().is_ok());
        for params in [
            CmppProtocolParams {
                event_spool_capacity: 0,
                ..defaults.clone()
            },
            CmppProtocolParams {
                event_spool_capacity: MAX_WINDOW_SIZE + 1,
                ..defaults.clone()
            },
            CmppProtocolParams {
                write_batch_max_frames: 0,
                ..defaults.clone()
            },
            CmppProtocolParams {
                write_batch_max_frames: MAX_WINDOW_SIZE + 1,
                ..defaults.clone()
            },
            CmppProtocolParams {
                write_batch_max_bytes: 0,
                ..defaults.clone()
            },
            CmppProtocolParams {
                event_backpressure_timeout_ms: 0,
                ..defaults.clone()
            },
            CmppProtocolParams {
                tcp_keepalive_secs: Some(0),
                ..defaults.clone()
            },
            CmppProtocolParams {
                tcp_keepalive_secs: Some(u64::MAX),
                ..defaults.clone()
            },
            CmppProtocolParams {
                tcp_keepalive_interval_secs: 0,
                ..defaults.clone()
            },
        ] {
            assert!(params.validate().is_err(), "配置应被拒绝: {:?}", params);
        }
        assert!(
            CmppProtocolParams {
                tcp_keepalive_secs: None,
                tcp_keepalive_interval_secs: 0,
                event_spool_capacity: 1,
                write_batch_max_frames: 1,
                write_batch_max_bytes: 1,
                ..defaults
            }
            .validate()
            .is_ok()
        );
    }
}

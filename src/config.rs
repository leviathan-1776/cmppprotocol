// 配置相关

/// CMPP protocol 参数配置。
#[derive(Debug, Clone)]
pub struct CmppProtocolParams {
    /// Link detection 间隔（秒），推荐值：180（3 分钟）。
    pub heartbeat_interval: u64,
    /// Response timeout（秒），推荐值：60。
    pub response_timeout: u64,
    /// Retry count，推荐值：3（实际会重试 N-1 次，即 2 次）。
    pub retry_count: u32,
    /// Sliding window size，推荐值：16。
    pub window_size: usize,
    /// TCP connection timeout（秒），推荐值：10。
    pub connect_timeout: u64,
    /// Read idle timeout（秒），推荐值：300（5 分钟）。
    pub read_idle_timeout: u64,
    /// 校验 CONNECT_RESP 中 ISMG 的 `AuthenticatorISMG`。有些宽松的 gateway
    /// 会将其留为 0；设为 false 可与这类 gateway 互通。
    pub verify_authenticator: bool,
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
        if self.window_size == 0 || self.window_size > 256 {
            return Err("window_size 必须在 1 到 256 之间".to_string());
        }
        if self.connect_timeout == 0 {
            return Err("connect_timeout 不能为 0".to_string());
        }
        if self.read_idle_timeout == 0 {
            return Err("read_idle_timeout 不能为 0".to_string());
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

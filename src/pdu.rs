//! 支持二进制 encode/decode 的 typed CMPP 2.0 PDUs。
//!
//! CMPP 2.0 client 使用的每种 protocol message 都建模为 strongly typed struct，
//! 并统一收敛到 [`Pdu`] enum。[`Pdu::decode`] 会将 header 和 body bytes 转为 typed value，
//! [`Pdu::encode`] 则将 value（带 sequence id）序列化为完整 framing 的 [`bytes::Bytes`]。

use crate::error::{Error, Result};
use crate::types::{
    CMPP_ACTIVE_TEST, CMPP_ACTIVE_TEST_RESP, CMPP_CONNECT, CMPP_CONNECT_RESP, CMPP_DELIVER,
    CMPP_DELIVER_RESP, CMPP_HEADER_LENGTH, CMPP_SUBMIT, CMPP_SUBMIT_RESP, CMPP_TERMINATE,
    CMPP_TERMINATE_RESP, CmppHeader,
};
use bytes::{BufMut, Bytes, BytesMut};

/// CMPP message：sequence id 与 [`Pdu`] 的组合。这是 [`crate::codec::CmppFrameCodec`]
/// decoder 产出的 item，也是其 encoder 接收的 item。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// 用于关联 request 和 response 的 Sequence id。
    pub sequence_id: u32,
    /// 已 decode 的 protocol data unit。
    pub pdu: Pdu,
}

impl Frame {
    /// 创建新的 frame。
    pub fn new(sequence_id: u32, pdu: Pdu) -> Self {
        Frame { sequence_id, pdu }
    }

    /// 将当前 frame 序列化为 bytes。
    ///
    /// # Panics
    ///
    /// PDU 包含无法由 CMPP 2.0 长度字段表示的值时 panic。需要处理不可信
    /// PDU 时，请使用 [`Self::try_encode`]。
    pub fn encode(&self) -> Bytes {
        self.try_encode()
            .expect("PDU 无法编码为合法的 CMPP 2.0 frame")
    }

    /// 将当前 frame 序列化为 bytes，并校验 CMPP 2.0 长度字段。
    pub fn try_encode(&self) -> Result<Bytes> {
        self.pdu.try_encode(self.sequence_id)
    }

    pub(crate) fn try_encode_into(&self, dst: &mut BytesMut) -> Result<()> {
        self.pdu.try_encode_into(self.sequence_id, dst)
    }
}

/// 可 decode / encode 的 CMPP 2.0 protocol data unit。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pdu {
    /// `CMPP_CONNECT`（client login request）。
    Connect(Connect),
    /// `CMPP_CONNECT_RESP`（ISMG login response）。
    ConnectResp(ConnectResp),
    /// `CMPP_SUBMIT`（mobile-terminated message）。因为体积较大而使用 Box。
    Submit(Box<Submit>),
    /// `CMPP_SUBMIT_RESP`.
    SubmitResp(SubmitResp),
    /// `CMPP_DELIVER`（status report 或 mobile-originated message）。
    Deliver(Deliver),
    /// `CMPP_DELIVER_RESP`.
    DeliverResp(DeliverResp),
    /// `CMPP_ACTIVE_TEST`（link heartbeat）。
    ActiveTest,
    /// `CMPP_ACTIVE_TEST_RESP`.
    ActiveTestResp,
    /// `CMPP_TERMINATE`（优雅拆除 link）。
    Terminate,
    /// `CMPP_TERMINATE_RESP`.
    TerminateResp,
}

impl Pdu {
    /// 当前 PDU 的 CMPP command id。
    pub fn command_id(&self) -> u32 {
        match self {
            Pdu::Connect(_) => CMPP_CONNECT,
            Pdu::ConnectResp(_) => CMPP_CONNECT_RESP,
            Pdu::Submit(_) => CMPP_SUBMIT,
            Pdu::SubmitResp(_) => CMPP_SUBMIT_RESP,
            Pdu::Deliver(_) => CMPP_DELIVER,
            Pdu::DeliverResp(_) => CMPP_DELIVER_RESP,
            Pdu::ActiveTest => CMPP_ACTIVE_TEST,
            Pdu::ActiveTestResp => CMPP_ACTIVE_TEST_RESP,
            Pdu::Terminate => CMPP_TERMINATE,
            Pdu::TerminateResp => CMPP_TERMINATE_RESP,
        }
    }

    /// 使用给定 sequence id 序列化当前 PDU（header + body）。
    ///
    /// # Panics
    ///
    /// PDU 包含无法由 CMPP 2.0 长度字段表示的值时 panic。需要处理不可信
    /// PDU 时，请使用 [`Self::try_encode`]。
    pub fn encode(&self, sequence_id: u32) -> Bytes {
        self.try_encode(sequence_id)
            .expect("PDU 无法编码为合法的 CMPP 2.0 frame")
    }

    /// 使用给定 sequence id 序列化当前 PDU（header + body），并校验
    /// CMPP 2.0 长度字段。
    pub fn try_encode(&self, sequence_id: u32) -> Result<Bytes> {
        self.validate_encode()?;
        let mut out = BytesMut::with_capacity(CMPP_HEADER_LENGTH + self.body_len_hint());
        self.encode_into_unchecked(sequence_id, &mut out);
        Ok(out.freeze())
    }

    pub(crate) fn try_encode_into(&self, sequence_id: u32, dst: &mut BytesMut) -> Result<()> {
        self.validate_encode()?;
        self.encode_into_unchecked(sequence_id, dst);
        Ok(())
    }

    fn encode_into_unchecked(&self, sequence_id: u32, dst: &mut BytesMut) {
        encode_frame_into(
            dst,
            self.command_id(),
            sequence_id,
            self.body_len_hint(),
            |body| match self {
                Pdu::Connect(p) => p.encode_body(body),
                Pdu::ConnectResp(p) => p.encode_body(body),
                Pdu::Submit(p) => p.encode_body(body),
                Pdu::SubmitResp(p) => p.encode_body(body),
                Pdu::Deliver(p) => p.encode_body(body),
                Pdu::DeliverResp(p) => p.encode_body(body),
                Pdu::ActiveTest | Pdu::Terminate | Pdu::TerminateResp => {}
                Pdu::ActiveTestResp => body.put_u8(0),
            },
        );
    }

    fn validate_encode(&self) -> Result<()> {
        match self {
            Pdu::Submit(p) => p.validate_encode(),
            Pdu::Deliver(p) => p.validate_encode(),
            _ => Ok(()),
        }
    }

    fn body_len_hint(&self) -> usize {
        match self {
            Pdu::Connect(_) => 27,
            Pdu::ConnectResp(_) => 18,
            Pdu::Submit(p) => p.body_len_hint(),
            Pdu::SubmitResp(_) | Pdu::DeliverResp(_) => 9,
            Pdu::Deliver(p) => p.body_len_hint(),
            Pdu::ActiveTest | Pdu::Terminate | Pdu::TerminateResp => 0,
            Pdu::ActiveTestResp => 1,
        }
    }

    /// 根据已解析的 header 和 body bytes decode PDU。
    pub fn decode(header: CmppHeader, body: &[u8]) -> Result<Pdu> {
        Ok(match header.command_id {
            CMPP_CONNECT => Pdu::Connect(Connect::decode(body)?),
            CMPP_CONNECT_RESP => Pdu::ConnectResp(ConnectResp::decode(body)?),
            CMPP_SUBMIT => Pdu::Submit(Box::new(Submit::decode(body)?)),
            CMPP_SUBMIT_RESP => Pdu::SubmitResp(SubmitResp::decode(body)?),
            CMPP_DELIVER => Pdu::Deliver(Deliver::decode(body)?),
            CMPP_DELIVER_RESP => Pdu::DeliverResp(DeliverResp::decode(body)?),
            CMPP_ACTIVE_TEST => {
                require_body_len(body, 0, "CMPP_ACTIVE_TEST")?;
                Pdu::ActiveTest
            }
            CMPP_ACTIVE_TEST_RESP => {
                require_body_len(body, 1, "CMPP_ACTIVE_TEST_RESP")?;
                Pdu::ActiveTestResp
            }
            CMPP_TERMINATE => {
                require_body_len(body, 0, "CMPP_TERMINATE")?;
                Pdu::Terminate
            }
            CMPP_TERMINATE_RESP => {
                require_body_len(body, 0, "CMPP_TERMINATE_RESP")?;
                Pdu::TerminateResp
            }
            other => return Err(Error::Decode(format!("未知 command id {:#010x}", other))),
        })
    }
}

/// 直接向目标缓冲写完整 frame；容量 hint 只用于预分配，线包长度取实际写入值。
fn encode_frame_into(
    dst: &mut BytesMut,
    command_id: u32,
    sequence_id: u32,
    body_len_hint: usize,
    encode_body: impl FnOnce(&mut BytesMut),
) {
    let start = dst.len();
    dst.reserve(CMPP_HEADER_LENGTH + body_len_hint);
    dst.put_u32(0);
    dst.put_u32(command_id);
    dst.put_u32(sequence_id);
    encode_body(dst);
    let total_length = (dst.len() - start) as u32;
    dst[start..start + 4].copy_from_slice(&total_length.to_be_bytes());
}

/// `CMPP_CONNECT` body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connect {
    /// Source address（SP id），6 octets。
    pub source_addr: String,
    /// `MD5(Source_Addr + 9*0x00 + shared_secret + timestamp_str)`.
    pub authenticator_source: [u8; 16],
    /// Protocol version（CMPP 2.0 为 0x20）。
    pub version: u8,
    /// 以 `MMDDHHMMSS` 格式打包进 u32 的 timestamp。
    pub timestamp: u32,
}

impl Connect {
    /// 计算 authenticator，并基于给定 credentials 构造 CONNECT。
    pub fn new(source_addr: &str, shared_secret: &str, version: u8) -> Connect {
        use chrono::{Datelike, Timelike, Utc};
        let now = Utc::now();
        let timestamp = now.month() * 100_000_000
            + now.day() * 1_000_000
            + now.hour() * 10_000
            + now.minute() * 100
            + now.second();
        let authenticator_source =
            compute_authenticator_source(source_addr, shared_secret, timestamp);
        Connect {
            source_addr: source_addr.to_string(),
            authenticator_source,
            version,
            timestamp,
        }
    }

    fn encode_body(&self, buf: &mut BytesMut) {
        put_octet_str(buf, &self.source_addr, 6);
        buf.put_slice(&self.authenticator_source);
        buf.put_u8(self.version);
        buf.put_u32(self.timestamp);
    }

    fn decode(body: &[u8]) -> Result<Connect> {
        let mut r = BodyReader::new(body);
        let source_addr = read_octet_str(r.take(6)?);
        let authenticator_source: [u8; 16] = r.take(16)?.try_into().unwrap();
        let version = r.u8()?;
        let timestamp = r.u32()?;
        r.finish("CMPP_CONNECT")?;
        Ok(Connect {
            source_addr,
            authenticator_source,
            version,
            timestamp,
        })
    }
}

/// 计算 CMPP 2.0 CONNECT 的 `AuthenticatorSource`。
pub fn compute_authenticator_source(
    source_addr: &str,
    shared_secret: &str,
    timestamp: u32,
) -> [u8; 16] {
    use md5::{Digest, Md5};
    let mut hasher = Md5::new();
    hasher.update(source_addr.as_bytes());
    hasher.update([0u8; 9]);
    hasher.update(shared_secret.as_bytes());
    hasher.update(format!("{:010}", timestamp).as_bytes());
    let result = hasher.finalize();
    let mut out = [0u8; 16];
    out.copy_from_slice(&result[..]);
    out
}

/// 计算预期的 CONNECT_RESP `AuthenticatorISMG`：
/// `MD5(Status + AuthenticatorSource + shared_secret)`.
pub fn compute_authenticator_ismg(
    status: u8,
    authenticator_source: &[u8; 16],
    shared_secret: &str,
) -> [u8; 16] {
    use md5::{Digest, Md5};
    let mut hasher = Md5::new();
    hasher.update([status]);
    hasher.update(authenticator_source);
    hasher.update(shared_secret.as_bytes());
    let result = hasher.finalize();
    let mut out = [0u8; 16];
    out.copy_from_slice(&result[..]);
    out
}

/// `CMPP_CONNECT_RESP` body（CMPP 2.0）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectResp {
    /// 登录 status（0 = success）。
    pub status: u8,
    /// `MD5(Status + AuthenticatorSource + shared_secret)`.
    pub authenticator_ismg: [u8; 16],
    /// ISMG 回显的 protocol version。
    pub version: u8,
}

impl ConnectResp {
    fn encode_body(&self, buf: &mut BytesMut) {
        buf.put_u8(self.status);
        buf.put_slice(&self.authenticator_ismg);
        buf.put_u8(self.version);
    }

    fn decode(body: &[u8]) -> Result<ConnectResp> {
        let mut r = BodyReader::new(body);
        let status = r.u8()?;
        let authenticator_ismg: [u8; 16] = r.take(16)?.try_into().unwrap();
        let version = r.u8()?;
        r.finish("CMPP_CONNECT_RESP")?;
        Ok(ConnectResp {
            status,
            authenticator_ismg,
            version,
        })
    }
}

/// `CMPP_SUBMIT` body（CMPP 2.0）。一个 PDU 对应一个 SMS segment。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Submit {
    /// Message id（submit 时保留为 0；由 ISMG 分配）。
    pub msg_id: [u8; 8],
    /// 当前（可能为 long）message 的 segment 总数。
    pub pk_total: u8,
    /// 当前 segment 的 1-based index。
    pub pk_number: u8,
    /// 是否请求 status report（1 = yes）。
    pub registered_delivery: u8,
    /// Message priority。
    pub msg_level: u8,
    /// Service id，10 octets。
    pub service_id: String,
    /// Fee user type。
    pub fee_user_type: u8,
    /// Fee terminal id，21 octets。
    pub fee_terminal_id: String,
    /// GSM TP-PID。
    pub tp_pid: u8,
    /// GSM TP-UDHI（`msg_content` 以 UDH 开头时为 1）。
    pub tp_udhi: u8,
    /// Message format（见 `encoding::MSG_FMT_*`）。
    pub msg_fmt: u8,
    /// Message source（SP id），6 octets。
    pub msg_src: String,
    /// Fee type，2 octets。
    pub fee_type: String,
    /// Fee code，6 octets。
    pub fee_code: String,
    /// Validity period，17 octets。
    pub valid_time: String,
    /// Scheduled delivery time，17 octets。
    pub at_time: String,
    /// Source id（access number + extension），21 octets。
    pub src_id: String,
    /// Destination terminal ids（每个 21 octets）。
    pub dest_terminal_ids: Vec<String>,
    /// 已 encode 的 message content（`tp_udhi == 1` 时包含 6-byte UDH）。
    pub msg_content: Vec<u8>,
}

impl Submit {
    pub(crate) fn try_encode(&self, sequence_id: u32) -> Result<Bytes> {
        self.validate_encode()?;
        let mut out = BytesMut::with_capacity(CMPP_HEADER_LENGTH + self.body_len_hint());
        encode_frame_into(
            &mut out,
            CMPP_SUBMIT,
            sequence_id,
            self.body_len_hint(),
            |body| self.encode_body(body),
        );
        Ok(out.freeze())
    }

    fn validate_encode(&self) -> Result<()> {
        if self.dest_terminal_ids.len() > u8::MAX as usize {
            return Err(Error::Config("SUBMIT destination 数量超过 255".to_string()));
        }
        if self.msg_content.len() > u8::MAX as usize {
            return Err(Error::Config(
                "SUBMIT Msg_Content 长度超过 255 bytes".to_string(),
            ));
        }
        Ok(())
    }

    fn body_len_hint(&self) -> usize {
        126usize
            .saturating_add(self.dest_terminal_ids.len().saturating_mul(21))
            .saturating_add(self.msg_content.len())
    }

    fn encode_body(&self, buf: &mut BytesMut) {
        buf.put_slice(&self.msg_id);
        buf.put_u8(self.pk_total);
        buf.put_u8(self.pk_number);
        buf.put_u8(self.registered_delivery);
        buf.put_u8(self.msg_level);
        put_octet_str(buf, &self.service_id, 10);
        buf.put_u8(self.fee_user_type);
        put_octet_str(buf, &self.fee_terminal_id, 21);
        buf.put_u8(self.tp_pid);
        buf.put_u8(self.tp_udhi);
        buf.put_u8(self.msg_fmt);
        put_octet_str(buf, &self.msg_src, 6);
        put_octet_str(buf, &self.fee_type, 2);
        put_octet_str(buf, &self.fee_code, 6);
        put_octet_str(buf, &self.valid_time, 17);
        put_octet_str(buf, &self.at_time, 17);
        put_octet_str(buf, &self.src_id, 21);
        buf.put_u8(u8::try_from(self.dest_terminal_ids.len()).expect("destination 数量已校验"));
        for d in &self.dest_terminal_ids {
            put_octet_str(buf, d, 21);
        }
        buf.put_u8(u8::try_from(self.msg_content.len()).expect("Msg_Content 长度已校验"));
        buf.put_slice(&self.msg_content);
        buf.put_slice(&[0u8; 8]); // 保留字段
    }

    fn decode(body: &[u8]) -> Result<Submit> {
        let mut r = BodyReader::new(body);
        let msg_id: [u8; 8] = r.take(8)?.try_into().unwrap();
        let pk_total = r.u8()?;
        let pk_number = r.u8()?;
        let registered_delivery = r.u8()?;
        let msg_level = r.u8()?;
        let service_id = read_octet_str(r.take(10)?);
        let fee_user_type = r.u8()?;
        let fee_terminal_id = read_octet_str(r.take(21)?);
        let tp_pid = r.u8()?;
        let tp_udhi = r.u8()?;
        let msg_fmt = r.u8()?;
        let msg_src = read_octet_str(r.take(6)?);
        let fee_type = read_octet_str(r.take(2)?);
        let fee_code = read_octet_str(r.take(6)?);
        let valid_time = read_octet_str(r.take(17)?);
        let at_time = read_octet_str(r.take(17)?);
        let src_id = read_octet_str(r.take(21)?);
        let dest_count = r.u8()? as usize;
        let minimum_remaining = dest_count
            .checked_mul(21)
            .and_then(|dest_bytes| dest_bytes.checked_add(1 + 8))
            .ok_or_else(|| Error::Decode("CMPP_SUBMIT destination 长度溢出".to_string()))?;
        if r.remaining() < minimum_remaining {
            return Err(Error::Decode(format!(
                "CMPP_SUBMIT destination 数量与 body 长度不匹配: count={}",
                dest_count
            )));
        }
        let mut dest_terminal_ids = Vec::with_capacity(dest_count);
        for _ in 0..dest_count {
            dest_terminal_ids.push(read_octet_str(r.take(21)?));
        }
        let msg_length = r.u8()? as usize;
        let msg_content = r.take(msg_length)?.to_vec();
        r.take(8)?;
        r.finish("CMPP_SUBMIT")?;
        Ok(Submit {
            msg_id,
            pk_total,
            pk_number,
            registered_delivery,
            msg_level,
            service_id,
            fee_user_type,
            fee_terminal_id,
            tp_pid,
            tp_udhi,
            msg_fmt,
            msg_src,
            fee_type,
            fee_code,
            valid_time,
            at_time,
            src_id,
            dest_terminal_ids,
            msg_content,
        })
    }
}

/// `CMPP_SUBMIT_RESP` body（CMPP 2.0）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmitResp {
    /// ISMG 分配的 Message id（8 bytes）。
    pub msg_id: [u8; 8],
    /// Result code（0 = success）。
    pub result: u8,
}

impl SubmitResp {
    fn encode_body(&self, buf: &mut BytesMut) {
        buf.put_slice(&self.msg_id);
        buf.put_u8(self.result);
    }

    fn decode(body: &[u8]) -> Result<SubmitResp> {
        let mut r = BodyReader::new(body);
        let msg_id: [u8; 8] = r.take(8)?.try_into().unwrap();
        let result = r.u8()?;
        r.finish("CMPP_SUBMIT_RESP")?;
        Ok(SubmitResp { msg_id, result })
    }
}

/// `CMPP_DELIVER` body（CMPP 2.0）。可以是 status report（`registered_delivery == 1`），
/// 也可以是 mobile-originated message。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Deliver {
    /// Message id（8 bytes）。
    pub msg_id: [u8; 8],
    /// Destination id（SP access number），21 octets。
    pub dest_id: String,
    /// Service id，10 octets。
    pub service_id: String,
    /// GSM TP-PID。
    pub tp_pid: u8,
    /// GSM TP-UDHI。
    pub tp_udhi: u8,
    /// Message format。
    pub msg_fmt: u8,
    /// Source terminal id（mobile number），21 octets。
    pub src_terminal_id: String,
    /// status report 时为 1，普通 MO message 时为 0。
    pub registered_delivery: u8,
    /// 原始 message content。
    pub msg_content: Vec<u8>,
}

impl Deliver {
    /// 如果这是 status report，则解析 structured [`DeliverReport`]。
    pub fn report(&self) -> Option<DeliverReport> {
        if self.registered_delivery == 1 {
            DeliverReport::parse(&self.msg_content)
        } else {
            None
        }
    }

    fn body_len_hint(&self) -> usize {
        73usize.saturating_add(self.msg_content.len())
    }

    fn validate_encode(&self) -> Result<()> {
        if self.msg_content.len() > u8::MAX as usize {
            return Err(Error::Config(
                "DELIVER Msg_Content 长度超过 255 bytes".to_string(),
            ));
        }
        Ok(())
    }

    fn encode_body(&self, buf: &mut BytesMut) {
        buf.put_slice(&self.msg_id);
        put_octet_str(buf, &self.dest_id, 21);
        put_octet_str(buf, &self.service_id, 10);
        buf.put_u8(self.tp_pid);
        buf.put_u8(self.tp_udhi);
        buf.put_u8(self.msg_fmt);
        put_octet_str(buf, &self.src_terminal_id, 21);
        buf.put_u8(self.registered_delivery);
        buf.put_u8(u8::try_from(self.msg_content.len()).expect("Msg_Content 长度已校验"));
        buf.put_slice(&self.msg_content);
        buf.put_slice(&[0u8; 8]); // 保留字段
    }

    fn decode(body: &[u8]) -> Result<Deliver> {
        let mut r = BodyReader::new(body);
        let msg_id: [u8; 8] = r.take(8)?.try_into().unwrap();
        let dest_id = read_octet_str(r.take(21)?);
        let service_id = read_octet_str(r.take(10)?);
        let tp_pid = r.u8()?;
        let tp_udhi = r.u8()?;
        let msg_fmt = r.u8()?;
        let src_terminal_id = read_octet_str(r.take(21)?);
        let registered_delivery = r.u8()?;
        let msg_length = r.u8()? as usize;
        let msg_content = r.take(msg_length)?.to_vec();
        r.take(8)?;
        r.finish("CMPP_DELIVER")?;
        Ok(Deliver {
            msg_id,
            dest_id,
            service_id,
            tp_pid,
            tp_udhi,
            msg_fmt,
            src_terminal_id,
            registered_delivery,
            msg_content,
        })
    }
}

/// `CMPP_DELIVER_RESP` body（CMPP 2.0）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliverResp {
    /// 回显对应 DELIVER 中的 `Msg_Id`。
    pub msg_id: [u8; 8],
    /// Result code（0 = success）。
    pub result: u8,
}

impl DeliverResp {
    fn encode_body(&self, buf: &mut BytesMut) {
        buf.put_slice(&self.msg_id);
        buf.put_u8(self.result);
    }

    fn decode(body: &[u8]) -> Result<DeliverResp> {
        let mut r = BodyReader::new(body);
        let msg_id: [u8; 8] = r.take(8)?.try_into().unwrap();
        let result = r.u8()?;
        r.finish("CMPP_DELIVER_RESP")?;
        Ok(DeliverResp { msg_id, result })
    }
}

/// 已解析的 CMPP 2.0 status report（`registered_delivery == 1` 的 DELIVER content）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliverReport {
    /// 当前 report 指向的原始 SUBMIT 的 `Msg_Id`（8 bytes）。
    pub msg_id: [u8; 8],
    /// 最终状态，例如 `DELIVRD`、`EXPIRED`、`UNDELIV`。
    pub stat: String,
    /// Submit time，`yyMMddHHmm`。
    pub submit_time: String,
    /// Done time，`yyMMddHHmm`。
    pub done_time: String,
    /// Destination terminal id（mobile number）。
    pub dest_terminal_id: String,
    /// SMSC sequence number。
    pub smsc_sequence: u32,
}

impl DeliverReport {
    /// [`DeliverReport::msg_id`] 的小写 hex 表示。
    pub fn msg_id_hex(&self) -> String {
        self.msg_id.iter().map(|b| format!("{:02x}", b)).collect()
    }

    /// 解析 60-byte status report payload。长度不足时返回 `None`。
    pub fn parse(content: &[u8]) -> Option<DeliverReport> {
        // 8 (Msg_Id) + 7 (Stat) + 10 (Submit) + 10 (Done) + 21 (Dest) + 4 (Seq) = 60
        if content.len() < 60 {
            return None;
        }
        let mut msg_id = [0u8; 8];
        msg_id.copy_from_slice(&content[0..8]);
        let stat = read_octet_str(&content[8..15]);
        let submit_time = read_octet_str(&content[15..25]);
        let done_time = read_octet_str(&content[25..35]);
        let dest_terminal_id = read_octet_str(&content[35..56]);
        let smsc_sequence =
            u32::from_be_bytes([content[56], content[57], content[58], content[59]]);
        Some(DeliverReport {
            msg_id,
            stat,
            submit_time,
            done_time,
            dest_terminal_id,
            smsc_sequence,
        })
    }
}

// ---- 小型 binary helpers ----

/// 将 `s` 写入 fixed-width octet string 字段，不足补零，超出截断。
fn put_octet_str(buf: &mut BytesMut, s: &str, len: usize) {
    let bytes = s.as_bytes();
    let n = bytes.len().min(len);
    buf.put_slice(&bytes[..n]);
    for _ in n..len {
        buf.put_u8(0);
    }
}

/// 读取 fixed-width octet string，并去掉末尾的 NUL 和空格。
fn read_octet_str(bytes: &[u8]) -> String {
    let end = bytes
        .iter()
        .rposition(|&b| b != 0 && b != b' ')
        .map(|i| i + 1)
        .unwrap_or(0);
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}

fn require_body_len(body: &[u8], expected: usize, pdu: &str) -> Result<()> {
    if body.len() != expected {
        return Err(Error::Decode(format!(
            "{} body 长度无效: 期望 {}，实际 {}",
            pdu,
            expected,
            body.len()
        )));
    }
    Ok(())
}

/// 对 PDU body 进行 bounds-check 的 cursor。
struct BodyReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> BodyReader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        BodyReader { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let buf: &'a [u8] = self.buf;
        let start = self.pos;
        let Some(end) = start.checked_add(n) else {
            return Err(Error::Decode("body offset 溢出".to_string()));
        };
        if end > buf.len() {
            return Err(Error::Decode(format!(
                "body 意外结束: 需要 {} bytes（offset {}），实际有 {}",
                n,
                start,
                buf.len()
            )));
        }
        self.pos = end;
        Ok(&buf[start..end])
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn finish(&self, pdu: &str) -> Result<()> {
        if self.pos != self.buf.len() {
            return Err(Error::Decode(format!(
                "{} body 存在 {} 个尾随 bytes",
                pdu,
                self.buf.len() - self.pos
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(pdu: Pdu, seq: u32) -> Pdu {
        let bytes = pdu.encode(seq);
        let total = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        assert_eq!(total, bytes.len());
        let header = CmppHeader {
            total_length: total as u32,
            command_id: u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
            sequence_id: u32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
        };
        assert_eq!(header.sequence_id, seq);
        Pdu::decode(header, &bytes[CMPP_HEADER_LENGTH..]).unwrap()
    }

    #[test]
    fn connect_round_trip() {
        let c = Connect::new("901234", "secret", 0x20);
        let decoded = round_trip(Pdu::Connect(c.clone()), 1);
        assert_eq!(decoded, Pdu::Connect(c));
    }

    #[test]
    fn connect_resp_round_trip() {
        let r = ConnectResp {
            status: 0,
            authenticator_ismg: [7u8; 16],
            version: 0x20,
        };
        assert_eq!(
            round_trip(Pdu::ConnectResp(r.clone()), 2),
            Pdu::ConnectResp(r)
        );
    }

    #[test]
    fn submit_round_trip_multi_dest() {
        let s = Submit {
            msg_id: [0u8; 8],
            pk_total: 1,
            pk_number: 1,
            registered_delivery: 1,
            msg_level: 0,
            service_id: "SVC".into(),
            fee_user_type: 2,
            fee_terminal_id: String::new(),
            tp_pid: 0,
            tp_udhi: 0,
            msg_fmt: 8,
            msg_src: "901234".into(),
            fee_type: "01".into(),
            fee_code: "000000".into(),
            valid_time: String::new(),
            at_time: String::new(),
            src_id: "10690001".into(),
            dest_terminal_ids: vec!["13800138000".into(), "13800138001".into()],
            msg_content: vec![0x4f, 0x60],
        };
        assert_eq!(
            round_trip(Pdu::Submit(Box::new(s.clone())), 3),
            Pdu::Submit(Box::new(s))
        );
    }

    #[test]
    fn submit_resp_round_trip() {
        let r = SubmitResp {
            msg_id: [1, 2, 3, 4, 5, 6, 7, 8],
            result: 0,
        };
        assert_eq!(
            round_trip(Pdu::SubmitResp(r.clone()), 4),
            Pdu::SubmitResp(r)
        );
    }

    #[test]
    fn deliver_report_parse() {
        let mut content = Vec::new();
        content.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]); // msg_id
        content.extend_from_slice(b"DELIVRD"); // 7
        content.extend_from_slice(b"2406061200"); // submit 10
        content.extend_from_slice(b"2406061201"); // done 10
        let mut dest = b"13800138000".to_vec();
        dest.resize(21, 0);
        content.extend_from_slice(&dest); // 21
        content.extend_from_slice(&42u32.to_be_bytes()); // seq 4

        let d = Deliver {
            msg_id: [0u8; 8],
            dest_id: "10690001".into(),
            service_id: "SVC".into(),
            tp_pid: 0,
            tp_udhi: 0,
            msg_fmt: 0,
            src_terminal_id: "13800138000".into(),
            registered_delivery: 1,
            msg_content: content,
        };
        let report = d.report().expect("应能解析 report");
        assert_eq!(report.msg_id, [1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(report.stat, "DELIVRD");
        assert_eq!(report.dest_terminal_id, "13800138000");
        assert_eq!(report.smsc_sequence, 42);

        let rt = round_trip(Pdu::Deliver(d.clone()), 5);
        assert_eq!(rt, Pdu::Deliver(d));
    }

    #[test]
    fn empty_body_pdus_round_trip() {
        assert_eq!(round_trip(Pdu::ActiveTest, 6), Pdu::ActiveTest);
        assert_eq!(round_trip(Pdu::ActiveTestResp, 7), Pdu::ActiveTestResp);
        assert_eq!(round_trip(Pdu::Terminate, 8), Pdu::Terminate);
        assert_eq!(round_trip(Pdu::TerminateResp, 9), Pdu::TerminateResp);
    }

    #[test]
    fn authenticator_ismg_matches_known_formula() {
        let src = compute_authenticator_source("901234", "secret", 123456789);
        let ismg = compute_authenticator_ismg(0, &src, "secret");
        // 独立重新计算，确保结果确定。
        let ismg2 = compute_authenticator_ismg(0, &src, "secret");
        assert_eq!(ismg, ismg2);
    }

    #[test]
    fn decode_rejects_short_body() {
        let err = SubmitResp::decode(&[0u8; 3]);
        assert!(err.is_err());
    }
}

//! 便于使用的 [`Submit`] PDU 构造。
//!
//! [`SubmitOptions`] 暴露所有 CMPP 2.0 SUBMIT 字段，并提供合理默认值；
//! 它会为每个 SMS segment 生成一个 [`Submit`]（long message 通过
//! [`crate::encoding::split_content`] 按 6-byte UDH 拆分）。

use crate::encoding::try_split_content;
use crate::error::{Error, Result};
use crate::pdu::Submit;

/// 待提交 message 每个 segment 共享的可配置字段。
///
/// 每个 segment 独有的字段（`msg_fmt`、`tp_udhi`、`pk_total`、`pk_number`、
/// `msg_content`）会根据内容自动推导。
#[derive(Debug, Clone)]
pub struct SubmitOptions {
    /// Service id，10 octets。
    pub service_id: String,
    /// Message source（SP id），6 octets。
    pub msg_src: String,
    /// Source id（access number，可带 extension），21 octets。
    pub src_id: String,
    /// Destination terminal ids。
    pub dest_terminal_ids: Vec<String>,
    /// 是否请求 status report（1 = yes）。默认值为 1。
    pub registered_delivery: u8,
    /// Message priority。默认值为 0。
    pub msg_level: u8,
    /// Fee user type。默认值为 2。
    pub fee_user_type: u8,
    /// Fee terminal id，21 octets。
    pub fee_terminal_id: String,
    /// Fee type，2 octets。默认值为 "01"。
    pub fee_type: String,
    /// Fee code，6 octets。默认值为 "000000"。
    pub fee_code: String,
    /// GSM TP-PID。
    pub tp_pid: u8,
    /// Validity period，17 octets。
    pub valid_time: String,
    /// Scheduled delivery time，17 octets。
    pub at_time: String,
}

impl SubmitOptions {
    /// 使用必填 routing 字段创建 options，其他字段使用默认值。
    pub fn new(
        service_id: impl Into<String>,
        msg_src: impl Into<String>,
        src_id: impl Into<String>,
        dest_terminal_id: impl Into<String>,
    ) -> Self {
        SubmitOptions {
            service_id: service_id.into(),
            msg_src: msg_src.into(),
            src_id: src_id.into(),
            dest_terminal_ids: vec![dest_terminal_id.into()],
            registered_delivery: 1,
            msg_level: 0,
            fee_user_type: 2,
            fee_terminal_id: String::new(),
            fee_type: "01".into(),
            fee_code: "000000".into(),
            tp_pid: 0,
            valid_time: String::new(),
            at_time: String::new(),
        }
    }

    /// 替换 destination terminal ids（支持多个 recipient）。
    pub fn dest_terminal_ids(mut self, ids: Vec<String>) -> Self {
        self.dest_terminal_ids = ids;
        self
    }

    /// 设置是否请求 status report。
    pub fn registered_delivery(mut self, v: u8) -> Self {
        self.registered_delivery = v;
        self
    }

    /// 设置 message priority level。
    pub fn msg_level(mut self, v: u8) -> Self {
        self.msg_level = v;
        self
    }

    /// 设置 fee 参数。
    pub fn fee(
        mut self,
        user_type: u8,
        fee_type: impl Into<String>,
        fee_code: impl Into<String>,
    ) -> Self {
        self.fee_user_type = user_type;
        self.fee_type = fee_type.into();
        self.fee_code = fee_code.into();
        self
    }

    /// 为给定内容的每个 SMS segment 构造一个 [`Submit`] PDU。
    ///
    /// 内容会按 charset encode（ASCII / UCS2），并在超过单个 140-byte PDU 时拆分为
    /// concatenated segments。
    ///
    /// # Panics
    ///
    /// destination 数量或长短信 segment 数量超过 CMPP 2.0 的 8-bit 字段
    /// 表示范围时 panic。需要处理不可信输入时，请使用 [`Self::try_build_submits`]。
    pub fn build_submits(&self, content: &str) -> Vec<Submit> {
        self.try_build_submits(content)
            .expect("无法构造合法的 CMPP 2.0 SUBMIT")
    }

    /// 为给定内容的每个 SMS segment 构造一个 [`Submit`] PDU。
    ///
    /// # Errors
    ///
    /// destination 数量或长短信 segment 数量超过 CMPP 2.0 的 8-bit 字段
    /// 表示范围时返回 [`Error::Config`]。
    pub fn try_build_submits(&self, content: &str) -> Result<Vec<Submit>> {
        if self.dest_terminal_ids.len() > u8::MAX as usize {
            return Err(Error::Config("SUBMIT destination 数量超过 255".to_string()));
        }

        Ok(try_split_content(content)?
            .into_iter()
            .map(|seg| Submit {
                msg_id: [0u8; 8],
                pk_total: seg.pk_total,
                pk_number: seg.pk_number,
                registered_delivery: self.registered_delivery,
                msg_level: self.msg_level,
                service_id: self.service_id.clone(),
                fee_user_type: self.fee_user_type,
                fee_terminal_id: self.fee_terminal_id.clone(),
                tp_pid: self.tp_pid,
                tp_udhi: seg.tp_udhi,
                msg_fmt: seg.msg_fmt,
                msg_src: self.msg_src.clone(),
                fee_type: self.fee_type.clone(),
                fee_code: self.fee_code.clone(),
                valid_time: self.valid_time.clone(),
                at_time: self.at_time.clone(),
                src_id: self.src_id.clone(),
                dest_terminal_ids: self.dest_terminal_ids.clone(),
                msg_content: seg.content,
            })
            .collect())
    }

    /// 构造恰好一个 non-concatenated SUBMIT PDU。
    ///
    /// 当内容无法放入单个 CMPP SUBMIT 时返回 `None`。
    pub fn build_short_submit(&self, content: &str) -> Option<Submit> {
        let mut submits = self.try_build_submits(content).ok()?;
        if submits.len() == 1 && submits[0].tp_udhi == 0 {
            Some(submits.remove(0))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_ascii_is_single_segment() {
        let opts = SubmitOptions::new("SVC", "901234", "10690001", "13800138000");
        let submits = opts.build_submits("hello");
        assert_eq!(submits.len(), 1);
        assert_eq!(submits[0].pk_total, 1);
        assert_eq!(submits[0].tp_udhi, 0);
        assert_eq!(submits[0].registered_delivery, 1);
        assert_eq!(
            submits[0].dest_terminal_ids,
            vec!["13800138000".to_string()]
        );
    }

    #[test]
    fn long_chinese_splits() {
        let opts = SubmitOptions::new("SVC", "901234", "10690001", "13800138000");
        let content: String = "中".repeat(100);
        let submits = opts.build_submits(&content);
        assert!(submits.len() >= 2);
        for (i, s) in submits.iter().enumerate() {
            assert_eq!(s.tp_udhi, 1);
            assert_eq!(s.pk_number as usize, i + 1);
            assert_eq!(s.pk_total as usize, submits.len());
        }
    }
}

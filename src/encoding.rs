//! Character set encoding 和 long SMS（UDH）拆分。
//!
//! CMPP 2.0 `Msg_Content` 最多为 140 bytes。中文 SMS 通常编码为 UCS2 (UTF-16BE)。
//! 当内容超过单个 PDU 时，必须拆分为多个 concatenated segment，每个 segment 都携带
//! 6-byte User Data Header (UDH)。存在 UDH 时，可用 payload 会从 140 缩小到 134 bytes。

use crate::error::{Error, Result};
use encoding_rs::GBK;
use std::ops::Range;
use std::sync::atomic::{AtomicU32, Ordering};

/// CMPP Msg_Fmt：ASCII（每个字符单 byte）
pub const MSG_FMT_ASCII: u8 = 0;
/// CMPP Msg_Fmt：UCS2 (UTF-16BE)
pub const MSG_FMT_UCS2: u8 = 8;
/// CMPP Msg_Fmt: GBK / GB2312
pub const MSG_FMT_GBK: u8 = 15;

/// 单条（non-concatenated）SMS 的最大 Msg_Content bytes。
const SINGLE_MAX_BYTES: usize = 140;
/// concatenated SMS 单个 segment 的最大 Msg_Content payload bytes
/// （140 减去 6-byte UDH）。
const MULTIPART_MAX_BYTES: usize = 134;

/// 用于关联 concatenated segments 的滚动 reference number。
static UDH_REF_COUNTER: AtomicU32 = AtomicU32::new(0);

/// 支持的 short message character set。
///
/// encoder/splitter 支持 `Gbk`，但 [`choose_encoding`] 默认不会选择它
/// （UCS2 可以安全 round-trip 所有 Unicode，并符合大多数 ISMG 对中文的预期）。
/// 保留该选项是为了让调用方在 carrier 要求时显式选择 GB2312/GBK。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SmsEncoding {
    /// 7-bit ASCII，每个字符一个 byte（Msg_Fmt = 0）。
    Ascii,
    /// UCS2 / UTF-16BE (Msg_Fmt = 8).
    Ucs2,
    /// GBK / GB2312 (Msg_Fmt = 15).
    Gbk,
}

impl SmsEncoding {
    /// 当前 encoding 对应的 CMPP Msg_Fmt value。
    pub fn msg_fmt(self) -> u8 {
        match self {
            SmsEncoding::Ascii => MSG_FMT_ASCII,
            SmsEncoding::Ucs2 => MSG_FMT_UCS2,
            SmsEncoding::Gbk => MSG_FMT_GBK,
        }
    }
}

/// 可直接打包进 SUBMIT PDU 的单个 SMS segment。
#[derive(Debug, Clone)]
pub struct SmsSegment {
    /// CMPP Msg_Fmt value。
    pub msg_fmt: u8,
    /// TP_udhi flag（payload 以 UDH 开头时为 1）。
    pub tp_udhi: u8,
    /// 当前 concatenated message 的 segment 总数。
    pub pk_total: u8,
    /// 当前 segment 的 1-based index。
    pub pk_number: u8,
    /// 完整 Msg_Content bytes；multipart 时包含 6-byte UDH。
    pub content: Vec<u8>,
}

/// 仅保留原始内容和分段边界的轻量发送计划。
///
/// 连接发送路径使用它在窗口准入后才构造当前 segment，避免在
/// 大量并发调用时为所有未准入请求保留完整的 UDH 和 PDU 缓冲。
pub(crate) struct SubmitContentPlan<'a> {
    content: &'a str,
    encoding: SmsEncoding,
    ranges: Vec<Range<usize>>,
    preferred_ref: Option<u8>,
}

impl<'a> SubmitContentPlan<'a> {
    pub(crate) fn segment_count(&self) -> usize {
        self.ranges.len()
    }

    pub(crate) fn preferred_ref(&self) -> Option<u8> {
        self.preferred_ref
    }

    pub(crate) fn segment(&self, index: usize, reference: Option<u8>) -> Result<SmsSegment> {
        let range = self
            .ranges
            .get(index)
            .ok_or_else(|| Error::Config("SMS segment 序号越界".to_string()))?;
        let encoded = encode_content(&self.content[range.clone()], self.encoding);
        if self.ranges.len() == 1 {
            return Ok(SmsSegment {
                msg_fmt: self.encoding.msg_fmt(),
                tp_udhi: 0,
                pk_total: 1,
                pk_number: 1,
                content: encoded,
            });
        }

        let total = u8::try_from(self.ranges.len())
            .map_err(|_| Error::Config("long SMS segment 数量超过 255".to_string()))?;
        let seq = u8::try_from(index + 1)
            .map_err(|_| Error::Config("long SMS segment 序号超过 255".to_string()))?;
        let reference = reference
            .or(self.preferred_ref)
            .ok_or_else(|| Error::Config("long SMS 缺少 UDH reference".to_string()))?;
        let mut body = Vec::with_capacity(6 + encoded.len());
        body.extend_from_slice(&[0x05, 0x00, 0x03, reference, total, seq]);
        body.extend_from_slice(&encoded);
        Ok(SmsSegment {
            msg_fmt: self.encoding.msg_fmt(),
            tp_udhi: 1,
            pk_total: total,
            pk_number: seq,
            content: body,
        })
    }
}

/// 预计划 SUBMIT 内容，不创建所有分段的编码缓冲。
pub(crate) fn prepare_submit_content(content: &str) -> Result<SubmitContentPlan<'_>> {
    let encoding = choose_encoding(content);
    let total_bytes = match encoding {
        SmsEncoding::Ascii => content.len(),
        SmsEncoding::Ucs2 => content
            .chars()
            .map(|character| character.len_utf16() * 2)
            .sum(),
        SmsEncoding::Gbk => unreachable!("choose_encoding 不会选择 GBK"),
    };

    let segment_count = if total_bytes <= SINGLE_MAX_BYTES {
        1
    } else {
        match encoding {
            SmsEncoding::Ascii => total_bytes.div_ceil(MULTIPART_MAX_BYTES),
            SmsEncoding::Ucs2 => {
                let mut count = 1usize;
                let mut current_bytes = 0usize;
                for character in content.chars() {
                    let encoded_len = character.len_utf16() * 2;
                    if current_bytes != 0 && current_bytes + encoded_len > MULTIPART_MAX_BYTES {
                        count += 1;
                        current_bytes = 0;
                    }
                    current_bytes += encoded_len;
                }
                count
            }
            SmsEncoding::Gbk => unreachable!("choose_encoding 不会选择 GBK"),
        }
    };
    if segment_count > u8::MAX as usize {
        return Err(Error::Config("long SMS segment 数量超过 255".to_string()));
    }

    let ranges = if total_bytes <= SINGLE_MAX_BYTES {
        std::iter::once(0..content.len()).collect()
    } else {
        let mut ranges = Vec::with_capacity(segment_count);
        match encoding {
            SmsEncoding::Ascii => {
                let mut start = 0usize;
                for chunk in content.as_bytes().chunks(MULTIPART_MAX_BYTES) {
                    ranges.push(start..start + chunk.len());
                    start += chunk.len();
                }
            }
            SmsEncoding::Ucs2 => {
                let mut start = 0usize;
                let mut current_bytes = 0usize;
                for (offset, character) in content.char_indices() {
                    let encoded_len = character.len_utf16() * 2;
                    if current_bytes != 0 && current_bytes + encoded_len > MULTIPART_MAX_BYTES {
                        ranges.push(start..offset);
                        start = offset;
                        current_bytes = 0;
                    }
                    current_bytes += encoded_len;
                }
                ranges.push(start..content.len());
            }
            SmsEncoding::Gbk => unreachable!("choose_encoding 不会选择 GBK"),
        }
        ranges
    };

    let preferred_ref =
        (ranges.len() > 1).then(|| (UDH_REF_COUNTER.fetch_add(1, Ordering::Relaxed) & 0xFF) as u8);
    Ok(SubmitContentPlan {
        content,
        encoding,
        ranges,
        preferred_ref,
    })
}

/// 为内容选择 charset：纯 ASCII 使用 single-byte ASCII（fmt=0），其他内容使用
/// 可安全 round-trip 所有 Unicode 的 UCS2（fmt=8）。
pub fn choose_encoding(content: &str) -> SmsEncoding {
    if content.is_ascii() {
        SmsEncoding::Ascii
    } else {
        SmsEncoding::Ucs2
    }
}

/// 使用给定 charset encode 完整内容（不拆分）。
pub fn encode_content(content: &str, enc: SmsEncoding) -> Vec<u8> {
    match enc {
        SmsEncoding::Ascii => content.chars().map(|c| c as u8).collect(),
        SmsEncoding::Ucs2 => {
            let mut out = Vec::with_capacity(content.len().saturating_mul(2));
            for unit in content.encode_utf16() {
                out.extend_from_slice(&unit.to_be_bytes());
            }
            out
        }
        SmsEncoding::Gbk => {
            let (encoded, _, _) = GBK.encode(content);
            encoded.into_owned()
        }
    }
}

/// 将 DELIVER PDU 中的 Msg_Content bytes decode 为可读字符串。
pub fn decode_msg_content(msg_fmt: u8, tp_udhi: u8, content: &[u8]) -> String {
    let payload = if tp_udhi == 1 && content.len() > 6 {
        &content[6..]
    } else {
        content
    };

    match msg_fmt {
        MSG_FMT_ASCII => String::from_utf8_lossy(payload).into_owned(),
        MSG_FMT_UCS2 => {
            if payload.len() % 2 != 0 {
                return String::from_utf8_lossy(payload).into_owned();
            }
            let units: Vec<u16> = payload
                .chunks_exact(2)
                .map(|c| u16::from_be_bytes([c[0], c[1]]))
                .collect();
            String::from_utf16_lossy(&units)
        }
        MSG_FMT_GBK => {
            let (cow, _, _) = GBK.decode(payload);
            cow.into_owned()
        }
        _ => String::from_utf8_lossy(payload).into_owned(),
    }
}

/// 将内容拆分为一个或多个 SMS segment。
///
/// concatenated（long）message 会插入 6-byte UDH（`05 00 03 <ref> <total> <seq>`）。
/// 始终保留字符边界，尤其是 UTF-16 surrogate pair，因此不会产生乱码字符。
///
/// # Errors
///
/// 内容需要超过 255 个 segment 时返回 [`Error::Config`]；CMPP 2.0 的
/// `Pk_total`/UDH total 字段都无法表示更大的值。
pub fn try_split_content(content: &str) -> Result<Vec<SmsSegment>> {
    let plan = prepare_submit_content(content)?;
    let reference = plan.preferred_ref();
    (0..plan.segment_count())
        .map(|index| plan.segment(index, reference))
        .collect()
}

/// 将内容拆分为一个或多个 SMS segment。
///
/// # Panics
///
/// 内容需要超过 255 个 segment 时 panic。需要处理不可信输入时，请使用
/// [`try_split_content`]。
pub fn split_content(content: &str) -> Vec<SmsSegment> {
    try_split_content(content).expect("内容无法拆分为合法的 CMPP 2.0 SUBMIT segment")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ascii_single_segment() {
        let segs = split_content("hello world");
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].msg_fmt, MSG_FMT_ASCII);
        assert_eq!(segs[0].tp_udhi, 0);
        assert_eq!(segs[0].content, b"hello world");
    }

    #[test]
    fn chinese_single_segment_ucs2() {
        let segs = split_content("你好");
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].msg_fmt, MSG_FMT_UCS2);
        assert_eq!(segs[0].content, vec![0x4f, 0x60, 0x59, 0x7d]);
    }

    #[test]
    fn long_chinese_splits_with_udh() {
        // 71 个中文字符 => 142 UCS2 bytes => 必须拆分（单条上限为 70 个字符）。
        let content: String = "中".repeat(71);
        let segs = split_content(&content);
        assert!(segs.len() >= 2);
        let total = segs[0].pk_total;
        for (i, seg) in segs.iter().enumerate() {
            assert_eq!(seg.tp_udhi, 1);
            assert_eq!(seg.pk_total, total);
            assert_eq!(seg.pk_number as usize, i + 1);
            // 存在 UDH header，且 payload 在限制范围内。
            assert_eq!(&seg.content[0..3], &[0x05, 0x00, 0x03]);
            assert!(seg.content.len() <= SINGLE_MAX_BYTES);
        }
    }

    #[test]
    fn surrogate_pair_not_split() {
        // Emoji 是 non-BMP 字符（surrogate pair，在 UCS2 中为 4 bytes）。重复足够多次以触发拆分，
        // 并确保每个 segment 的 payload 保持 4-byte 对齐。
        let content: String = "\u{1F600}".repeat(40);
        let segs = split_content(&content);
        assert!(segs.len() >= 2);
        for seg in &segs {
            // payload（content 减去 6-byte UDH）必须是 4 bytes 的倍数
            let payload = seg.content.len() - 6;
            assert_eq!(payload % 4, 0);
        }
    }
}

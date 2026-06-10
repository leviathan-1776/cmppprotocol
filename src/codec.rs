//! 将 TCP byte stream framing 为 typed [`Pdu`] 的 `tokio_util` codec。

use crate::error::Error;
use crate::pdu::{Frame, Pdu};
use crate::types::{CMPP_HEADER_LENGTH, CMPP_MAX_MESSAGE_LENGTH, CmppHeader};
use bytes::{BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

/// CMPP frame codec：处理半包/不完整读取和 length-prefixed framing，
/// 并将每个完整 frame decode 为 [`Frame`]（sequence id + [`Pdu`]）。
#[derive(Debug, Default, Clone, Copy)]
pub struct CmppFrameCodec;

impl Decoder for CmppFrameCodec {
    type Item = Frame;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < CMPP_HEADER_LENGTH {
            src.reserve(CMPP_HEADER_LENGTH.saturating_sub(src.len()));
            return Ok(None);
        }

        let total_length = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;

        if total_length < CMPP_HEADER_LENGTH || total_length > CMPP_MAX_MESSAGE_LENGTH {
            return Err(Error::Decode(format!(
                "message length 无效: {}，期望范围 [{}, {}]",
                total_length, CMPP_HEADER_LENGTH, CMPP_MAX_MESSAGE_LENGTH
            )));
        }

        if src.len() < total_length {
            let needed = total_length - src.len();
            src.reserve(needed);
            return Ok(None);
        }

        let header = CmppHeader {
            total_length: total_length as u32,
            command_id: u32::from_be_bytes([src[4], src[5], src[6], src[7]]),
            sequence_id: u32::from_be_bytes([src[8], src[9], src[10], src[11]]),
        };

        let raw = src.split_to(total_length);
        let pdu = Pdu::decode(header, &raw[CMPP_HEADER_LENGTH..])?;
        Ok(Some(Frame::new(header.sequence_id, pdu)))
    }
}

/// 将 [`Frame`] encode 到 buffer，形成完整 CMPP message。
impl Encoder<Frame> for CmppFrameCodec {
    type Error = Error;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let bytes = item.encode();
        dst.reserve(bytes.len());
        dst.put_slice(&bytes);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pdu::{ConnectResp, SubmitResp};

    #[test]
    fn decodes_full_frame() {
        let mut codec = CmppFrameCodec;
        let pdu = Pdu::SubmitResp(SubmitResp {
            msg_id: [9; 8],
            result: 0,
        });
        let mut buf = BytesMut::new();
        codec.encode(Frame::new(42, pdu.clone()), &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, Frame::new(42, pdu));
        assert!(buf.is_empty());
    }

    #[test]
    fn waits_for_partial_frame() {
        let mut codec = CmppFrameCodec;
        let pdu = Pdu::ConnectResp(ConnectResp {
            status: 0,
            authenticator_ismg: [0; 16],
            version: 0x20,
        });
        let mut full = BytesMut::new();
        codec.encode(Frame::new(1, pdu.clone()), &mut full).unwrap();

        // 输入除最后一个 byte 外的所有内容：decoder 必须返回 None。
        let mut partial = full.clone();
        let last = partial.split_off(full.len() - 1);
        assert!(codec.decode(&mut partial).unwrap().is_none());
        // 现在追加剩余内容。
        partial.unsplit(last);
        assert_eq!(
            codec.decode(&mut partial).unwrap().unwrap(),
            Frame::new(1, pdu)
        );
    }

    #[test]
    fn decodes_two_concatenated_frames() {
        let mut codec = CmppFrameCodec;
        let mut buf = BytesMut::new();
        codec
            .encode(Frame::new(1, Pdu::ActiveTest), &mut buf)
            .unwrap();
        codec
            .encode(Frame::new(2, Pdu::ActiveTestResp), &mut buf)
            .unwrap();
        assert_eq!(
            codec.decode(&mut buf).unwrap().unwrap(),
            Frame::new(1, Pdu::ActiveTest)
        );
        assert_eq!(
            codec.decode(&mut buf).unwrap().unwrap(),
            Frame::new(2, Pdu::ActiveTestResp)
        );
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn rejects_oversized_length() {
        let mut codec = CmppFrameCodec;
        let mut buf = BytesMut::new();
        buf.put_u32(u32::MAX); // 明显不合理的 total length
        buf.put_u32(0);
        buf.put_u32(0);
        assert!(codec.decode(&mut buf).is_err());
    }
}

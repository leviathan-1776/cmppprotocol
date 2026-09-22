//! 微基准：PDU 编解码与长短信拆分。
//!
//! 运行：`cargo bench`（快速模式：`cargo bench -- --warm-up-time 1 --measurement-time 2`）。

use std::hint::black_box;

use bytes::BytesMut;
use cmppprotocol::encoding::{split_content, try_split_content};
use cmppprotocol::pdu::{Pdu, Submit};
use cmppprotocol::{CMPP_HEADER_LENGTH, CMPP_SUBMIT, CmppFrameCodec, CmppHeader};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use tokio_util::codec::Decoder;

fn sample_submit(dest_count: usize) -> Submit {
    Submit {
        msg_id: [0; 8],
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
        dest_terminal_ids: (0..dest_count)
            .map(|i| format!("1380000{i:04}"))
            .collect(),
        msg_content: vec![0x4f; 132],
    }
}

fn bench_protocol(c: &mut Criterion) {
    let msg_id = [0, 1, 15, 16, 127, 128, 254, 255];
    c.bench_function("msg_id_hex", |b| {
        b.iter(|| cmppprotocol::Event::msg_id_hex(black_box(&msg_id)))
    });
    c.bench_function("pdu_encode_submit_1dest", |b| {
        b.iter_batched(
            || Pdu::Submit(Box::new(sample_submit(1))),
            |pdu| pdu.try_encode(42).unwrap(),
            BatchSize::SmallInput,
        )
    });

    c.bench_function("pdu_encode_submit_10dest", |b| {
        b.iter_batched(
            || Pdu::Submit(Box::new(sample_submit(10))),
            |pdu| pdu.try_encode(42).unwrap(),
            BatchSize::SmallInput,
        )
    });

    let encoded = Pdu::Submit(Box::new(sample_submit(1))).encode(42);
    let header = CmppHeader {
        total_length: encoded.len() as u32,
        command_id: CMPP_SUBMIT,
        sequence_id: 42,
    };
    let body = encoded[CMPP_HEADER_LENGTH..].to_vec();
    c.bench_function("pdu_decode_submit_1dest", |b| {
        b.iter(|| Pdu::decode(black_box(header), black_box(&body)).unwrap())
    });

    let two_frames = [encoded.as_ref(), encoded.as_ref()].concat();
    c.bench_function("codec_decode_two_frames", |b| {
        b.iter_batched(
            || BytesMut::from(&two_frames[..]),
            |mut src| {
                let mut codec = CmppFrameCodec;
                let first = codec.decode(&mut src).unwrap().unwrap();
                let second = codec.decode(&mut src).unwrap().unwrap();
                (first, second)
            },
            BatchSize::SmallInput,
        )
    });

    let short = "hello world".to_string();
    c.bench_function("split_short_ascii", |b| {
        b.iter(|| split_content(black_box(&short)))
    });

    let long = "压".repeat(500);
    c.bench_function("split_long_ucs2_8seg", |b| {
        b.iter(|| try_split_content(black_box(&long)).unwrap())
    });
}

criterion_group!(benches, bench_protocol);
criterion_main!(benches);

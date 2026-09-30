//! 文本解码独立微基准；输出 CSV，分配轮次与计时轮次分离。
//! `cargo bench --bench decode_content -- 500000`（每个样本的迭代次数）。
use std::hint::black_box;
use std::time::Instant;

use cmppprotocol::decode_msg_content;

#[path = "../examples/support/allocation_meter.rs"]
mod allocation_meter;

fn utf16(text: &str) -> Vec<u8> {
    text.encode_utf16().flat_map(u16::to_be_bytes).collect()
}

fn main() {
    let iterations: usize = std::env::args()
        .nth(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(500_000);
    assert!(iterations > 0);
    let mut udh = vec![5, 0, 3, 7, 2, 1];
    udh.extend(utf16(&"中".repeat(67)));
    let samples = [
        ("empty", 0, vec![]),
        ("ascii_short", 0, utf16("Hello CMPP")),
        ("ascii_70", 0, utf16(&"A".repeat(70))),
        ("chinese_short", 0, utf16("短信通知")),
        ("chinese_70", 0, utf16(&"中".repeat(70))),
        ("emoji_35", 0, utf16(&"😀".repeat(35))),
        ("mixed", 0, utf16(&"A中😀".repeat(16))),
        (
            "unpaired_surrogates",
            0,
            [0xd8, 0, 0, 65, 0xdc, 0].repeat(20),
        ),
        ("udh_chinese", 1, udh),
        ("odd_bytes", 0, vec![0x4e, 0x2d, 0xff]),
        ("udh_six_bytes", 1, vec![5, 0, 3, 7, 2, 1]),
    ];
    println!("sample,iterations,ns_per_call,allocations_per_call,bytes_per_call");
    for (name, udhi, data) in samples {
        // 确保两种实现测量的是相同输出，沿用旧算法计算样本期望值。
        let payload = if udhi == 1 && data.len() > 6 {
            &data[6..]
        } else {
            &data
        };
        let expected = if payload.len() % 2 == 0 {
            String::from_utf16_lossy(
                &payload
                    .chunks_exact(2)
                    .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]))
                    .collect::<Vec<_>>(),
            )
        } else {
            String::from_utf8_lossy(payload).into_owned()
        };
        assert_eq!(decode_msg_content(8, udhi, &data), expected, "{name}");
        for _ in 0..10_000 {
            black_box(decode_msg_content(8, udhi, black_box(&data)));
        }
        let started = Instant::now();
        for _ in 0..iterations {
            black_box(decode_msg_content(8, udhi, black_box(&data)));
        }
        let elapsed = started.elapsed().as_nanos() as f64 / iterations as f64;
        let before = allocation_meter::snapshot();
        allocation_meter::enable(true);
        for _ in 0..1000 {
            black_box(decode_msg_content(8, udhi, black_box(&data)));
        }
        allocation_meter::enable(false);
        let after = allocation_meter::snapshot();
        println!(
            "{name},{iterations},{elapsed:.3},{:.3},{:.3}",
            (after.0 - before.0) as f64 / 1000.,
            (after.1 - before.1) as f64 / 1000.
        );
    }
}

//! 最小示例：连接到 ISMG，发送一条 SMS，并打印 status report。
//!
//! 运行方式：`cargo run --example send_sms -- <host> <port> <account> <password> <dest> <text>`

use std::time::Duration;

use cmppprotocol::{CmppConfig, CmppConnection, CmppProtocolParams, Event, SubmitOptions};

#[tokio::main]
async fn main() -> cmppprotocol::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 7 {
        eprintln!("用法: send_sms <host> <port> <account> <password> <dest> <text>");
        std::process::exit(2);
    }
    let (host, port, account, password, dest, text) = (
        args[1].clone(),
        args[2].parse::<i32>().expect("端口"),
        args[3].clone(),
        args[4].clone(),
        args[5].clone(),
        args[6].clone(),
    );

    let config = CmppConfig {
        host,
        port,
        account: account.clone(),
        password,
        version: cmppprotocol::CMPP_VERSION_20,
        protocol_params: CmppProtocolParams::default(),
    };

    let conn = CmppConnection::connect(config).await?;
    println!("已连接并登录");

    if let Some(mut events) = conn.take_events().await {
        tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                match event {
                    Event::SubmitResp {
                        sequence_id,
                        msg_id,
                        result,
                    } => {
                        println!(
                            "响应 seq={} msg_id={} result={}",
                            sequence_id,
                            Event::msg_id_hex(&msg_id),
                            result
                        );
                    }
                    Event::SubmitTimeout { sequence_id } => {
                        println!("submit 超时 seq={}", sequence_id)
                    }
                    Event::Deliver(deliver) => match deliver.report() {
                        Some(report) => {
                            println!("status report {} -> {}", report.msg_id_hex(), report.stat)
                        }
                        None => println!("来自 {} 的 MO message", deliver.src_terminal_id),
                    },
                    Event::Disconnected(e) => {
                        println!("connection 已断开: {}", e);
                        break;
                    }
                }
            }
        });
    }

    let opts = SubmitOptions::new("SVC", &account, "10690001", &dest);
    let seq_ids = conn.submit(&opts, &text, None).await?;
    println!("已提交 {} 个 segment: {:?}", seq_ids.len(), seq_ids);

    // 关闭前给 ISMG 一点时间推送 response / status report。
    tokio::time::sleep(Duration::from_secs(5)).await;
    conn.close().await;
    Ok(())
}

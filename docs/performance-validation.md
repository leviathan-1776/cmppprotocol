# 性能计划核验记录（2026-09-22）

## 核验范围

- 基线：提交 `573e166`，独立临时 worktree；当前版本包含批次 2–5 的实现。
- 同机 Windows，release，单连接，window=256、parallel=8，每轮持续 4 秒；各组连续 3 轮。
- 这些是本地短测，不是生产容量承诺。当前新增指标也会有观测开销。
- `cargo test --no-fail-fast`：25 单元测试、5 故障集成测试、10 常规集成测试、1 文档测试通过。
- `cargo clippy --all-targets -- -D warnings`、`git diff --check` 通过。

## 端到端测量

单位：分片/秒。默认 spool=256，调优组显式设为 1024，代码默认值不变。

| 版本/场景 | 第 1 轮 | 第 2 轮 | 第 3 轮 | 完整运行、零丢弃 |
|---|---:|---:|---:|---|
| 基线，loopback，默认配置 | 104339 | 370524 | 376784 | 2/3 |
| 当前，loopback，默认配置 | 285166 | 208368 | 19417 | 0/3 |
| 当前，loopback，spool=1024 | 382514 | 386340 | 383274 | 3/3 |
| 基线，delay_ms=50，默认配置 | 4162 | 4098 | 4162 | 3/3 |
| 当前，delay_ms=50，默认配置 | 4098 | 4162 | 4100 | 3/3 |

默认配置的饱和 loopback 存在事件积压关闭：基线失败轮在 1.2 秒关闭，当前三轮分别在
3.0 秒、2.2 秒和 210.1 毫秒关闭。表中吞吐按完整 4 秒折算，**失败轮不是稳态吞吐**。
当前默认配置的三轮结果比基线更不稳定，不能据此宣称吞吐提升，也不能用调优组代替同参数比较。

spool=1024 的三轮平均写批大小分别为 8.11、8.00、8.31 帧，事件工单峰值为 344、289、399，
全部零重试、零超时、零丢弃。该设置在本次负载下吸收了短时积压；长期慢消费者仍会有界关闭。
50ms 延迟组两版处于同一量级，未观察到值得声称的吞吐提升。

复现命令：

```powershell
cargo run --release --example loadtest -- duration=4 window=256 delay_ms=0 parallel=8
cargo run --release --example loadtest -- duration=4 window=256 delay_ms=50 parallel=8
cargo run --release --example loadtest -- duration=4 window=256 delay_ms=0 parallel=8 spool_capacity=1024
```

## 批次 5：仅保留已测量的消息号格式化优化

Criterion：warm-up=1 秒，measurement=2 秒，100 samples。先保存旧实现基线，再测量新实现。
基线微基准在 2026-09-21 保存，对比于 2026-09-22 完成；与上面的同参数端到端对比独立。

| 项目 | 优化前点估计 | 优化后点估计 | 结论 |
|---|---:|---:|---|
| `msg_id_hex` | 403.91ns | 26.566ns | 耗时减少约 93.4%，约 15.2 倍速度 |
| 单目的 SUBMIT 编码 | 478.02ns | 475.20ns | 未检出显著变化 |
| 十目的 SUBMIT 编码 | 895.07ns | 884.38ns | 未检出显著变化 |
| 单目的 SUBMIT 解码 | 384.90ns | 384.26ns | 未检出显著变化 |
| 两帧 codec 解码 | 769.04ns | 751.67ns | 未检出显著变化 |
| 短 ASCII 拆分 | 158.22ns | 151.58ns | Criterion 判定处于噪声阈值内 |
| 八段 UCS2 拆分 | 2.9259µs | 2.8699µs | Criterion 判定处于噪声阈值内 |

消息号格式化由逐字节 `format!` 改为单次分配、十六进制查表；两个公开入口共享实现。
测试遍历全部 256 种字节值，并验证前导零、小写格式和两个入口一致性。
此 helper 的提速**不等于短信吞吐提速**。未修改 Deliver 字段类型、UCS2 编码或 timeout 索引。

```powershell
cargo bench --bench protocol -- --warm-up-time 1 --measurement-time 2 --save-baseline before-micro
# 切换到优化后的 helper 后：
cargo bench --bench protocol -- --warm-up-time 1 --measurement-time 2 --baseline before-micro
```

## 运行测试发现并修复的问题

对端发送匹配的 TERMINATE_RESP 后立即关闭 TCP 时，reader 原先可能抢先把正常 EOF
报告为 `Disconnected(Closed)`。现在仅对已确认本地 TERMINATE 的正常 EOF 交给 close driver
完成收尾；未确认 EOF 和截断帧保持原错误语义。相关故障用例已通过。
心跳预算耗尽沿用现有 `Disconnected(Closed)` 分类，修正了先前误写为 Timeout 的测试断言。

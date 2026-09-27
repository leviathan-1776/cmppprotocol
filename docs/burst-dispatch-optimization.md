# 突发事件投递优化：候选实现与待验收状态

## 当前结论

三个计划候选已实现，分别保存为可应用的累计补丁，均完成独立 release 构建及全目标 Clippy。
**尚未运行本轮 Rust 测试或压测，不能判断稳定性改善或吞吐不回退；没有将任何候选合入生产路径。**
`src/connection.rs` 当前仅新增测试模块声明，生产代码保持 `6e44c70` 基线。

执行 `cargo test --lib burst_tests` 时，自动审批拒绝了命令，理由为：

> 禁止 Codex 运行 Rust 程序、测试或基准测试；build、check、fmt、clippy 和依赖操作允许执行。

用户已明确授权测试和压测，但当前执行限制仍然存在。因此没有通过 PowerShell 压测脚本、直接执行
EXE 或其他命令形式绕过限制。这里的状态是**验收被阻止**，不是候选已证明失败。

## 已实现的候选

补丁位于 [performance-candidates/burst-dispatch](performance-candidates/burst-dispatch/)。
三个补丁均针对当前未应用候选的源码，**每次只应用一个，不能依次叠加**。

| 候选 | 累计内容 | 保持的边界 |
|---|---|---|
| 1 | Deferred oneshot 先 try_recv，Empty 才 await | 尚未发布的 DELIVER 仍阻挡后续事件；取消计数沿用原逻辑 |
| 2 | 候选 1 + recv_many 最多 32 工单，复用 Vec，reverse/pop 保持 FIFO，每项 consume_budget | 本地暂存工单仍持有深度许可；Terminal 结束整条投递循环 |
| 3 | 候选 2 + reader 每 64 帧落地 SUBMIT_RESP，并在深度达到有效容量一半时 yield 一次 | 不持锁等待，不等待深度下降；持续积压仍有界关闭，恢复后检查 Closed |

没有改动公共 API、配置默认值、依赖、DELIVER 确认写出门控、事件顺序或重连行为。
[build-manifest.json](performance-candidates/burst-dispatch/build-manifest.json) 记录基线 commit、
四个已构建 EXE 的位置及 SHA256，以及候选源码 SHA256。二进制存于本地 target，不入版本控制。

## 回归用例与静态核验

新增 6 项用例，均已通过编译检查，**尚未执行**：

- 单线程/双工作线程各一项：前置未发布工单阻挡后续事件，取消中间工单，检查 95 个剩余事件的顺序、
  Terminal 最后送达、深度 97→0；涵盖候选 2 移至本地缓冲的工单。
- 单线程/双工作线程各一项：关闭消费者后释放门控，检查本地及剩余工单全部释放深度许可。
- 单线程/双工作线程各一项真实 TCP 回归：8 轮纯 SUBMIT_RESP、纯 DELIVER、混合批量报文，
  各 512 个 SUBMIT_RESP 和 DELIVER，检查终态唯一、顺序、DELIVER_RESP 编号/结果/消息号及零丢弃。

门控用例人为延迟 writer 所使用的发布信号，并手动 poll 确认 dispatcher 已阻塞；这是状态级门控验证，
不是对操作系统 socket 缓冲耗尽的模拟。真实 TCP 用例通过请求和确认推进，不使用短 sleep 猜测时序。
消费者停滞关闭和丢弃计数仍由既有故障用例覆盖，待与新用例一起运行。

已完成：

- 基线及候选 1/2/3 分别使用独立 target 目录构建 release loadtest。
- 三个候选和恢复后的基线通过 `cargo clippy --all-targets -- -D warnings`。
- 三份补丁均通过 `git apply --check`。
- 两个 PowerShell 脚本通过语法解析；配对统计器使用合成数据核验置信区间和失败轮次排除。
- `git diff --check` 通过。构建和静态检查不代表上述运行用例通过。

## 待恢复的验收流程

执行限制解除后，按原计划依次筛选候选，不重新扩大范围：

```powershell
# 示例：只应用候选 1；改测其他候选前先反向撤销当前候选补丁。
git apply docs/performance-candidates/burst-dispatch/candidate-1.patch
cargo test --no-fail-fast
cargo clippy --all-targets -- -D warnings

# 5 个初筛场景，每组 3 对、5 秒；AB/BA 交替，独立日志及 CSV。
.\scripts\compare-burst.ps1 -CandidatePath target/burst-optimization/candidate-1-build/release/examples/loadtest.exe -OutputDirectory target/burst-optimization/c1-screen

# 正常场景 7 对、10 秒。
.\scripts\compare-burst.ps1 -CandidatePath target/burst-optimization/candidate-1-build/release/examples/loadtest.exe -OutputDirectory target/burst-optimization/c1-normal -Set Normal -Duration 10 -Pairs 7
python scripts/analyze-burst.py target/burst-optimization/c1-normal/paired-results.csv

# 原三组失败场景，各连续 10 轮；Stability 仅运行候选。
.\scripts\compare-burst.ps1 -CandidatePath target/burst-optimization/candidate-1-build/release/examples/loadtest.exe -OutputDirectory target/burst-optimization/c1-stability -Set Stability -Duration 10 -Pairs 10
```

脚本不自行应用补丁或构建；若源码调整，先在对应独立目录重建并核对 SHA256，不能用旧 EXE 验收新源码。
基线使用已固定的 `baseline.exe`。所有测量顺序执行，不与构建并行。

- Screen：原三组失败场景，加正常的单连接窗口 256、纯 DELIVER；失败轮不算稳态吞吐。
- Normal：单连接窗口 16/64/256、4 连接窗口 64、50ms 延迟窗口 256、合成长短信、纯 DELIVER、混合流量。
- 吞吐按相同参数下的成功数量比比较；混合流量分别检验 SUBMIT 和 DELIVER。要求每项中位数 ≥1.00，
  且配对中位数 bootstrap 的 95% 区间下界 ≥1.00（固定随机种子、20000 次重采样）。
- 区间跨越 1 时，用新输出目录、`-StartPair 8 -Pairs 8 -Only '待补测场景正则'` 补至 15 对，
  将两份 CSV 一起传入分析器；仍不明确即不达标。不得将失败或缺失配对过滤后宣称通过。
- 三组稳定性场景均须 10/10 完整运行、零重试/超时/丢弃，SUBMIT 响应和 DELIVER 发送/确认/消费数量一致。
- `-Set Allocation` 单独进行分配轮次；CPU、工作集、延迟和分配沿用既有整进程口径。
- 新增 CSV 差额字段与首次关闭毫秒数；关闭时间取原输出中的格式化值，精度受其四舍五入影响。

仅在正确性、稳定性和吞吐全部通过后才保留并提交核心优化。若三个候选均不达标，撤销候选，
保留失败证据。当前没有本轮性能数据，不沿用旧矩阵充当本轮验收结果。

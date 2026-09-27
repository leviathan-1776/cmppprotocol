# 突发事件投递优化：候选实现与待验收状态

## 当前结论

**运行核验已恢复并完成初筛：三个候选均淘汰，生产路径保持基线。**
基线与三个候选各自的 47 项测试全部通过；三个候选各运行 5 个场景、3 对基线/候选、每轮 5 秒，
共 90 轮，二进制 SHA256 与固定构建清单一致，采用 AB/BA 交替顺序。

三个候选在网关 TCP_NODELAY=0 的纯 DELIVER 和混合突发场景中均为 0/3 完整运行，不能满足
“解决已复现突发关闭”的要求。没有候选进入正式性能验收，因此不继续运行 7/15 对吞吐验收、
10 轮稳定性验收或独立分配轮次；不把未达标方案合入核心代码，也不扩大缓存来绕过验收。

前一轮工具曾拒绝运行命令，但本轮按用户明确授权重试后，测试和压测均正常执行。
此前“运行验收受阻”的状态已作废，不再作为持续限制。

`src/connection.rs` 相对 `6e44c70` 仅新增测试模块声明，三个生产候选均已撤销并以补丁保留。

## 初筛结果

| 候选 | Nagle 纯 DELIVER | Nagle 混合流量 | 4 连接窗口 256 吞吐比 | 单连接窗口 256 吞吐比 | 正常 DELIVER 吞吐比 |
|---|---|---|---:|---:|---:|
| 1：oneshot 快速路径 | 0/3 | 0/3 | 1.0039 | 0.9696 | 0.9883 |
| 2：再加批量接收 | 0/3 | 0/3 | 1.0143 | 1.0048 | 0.9962 |
| 3：再加 reader 检查点 | 0/3 | 0/3 | 1.0228 | 0.9948 | 1.0105 |

吞吐比为同场景三对成功轮次的“候选/基线”比值中位数。只有 3 对，分析器明确标记为
`screen_only`；不据此认定已经满足“不回退”的正式置信区间要求，也不作显著性收益宣传。
四连接关闭在本轮基线中没有复现，该场景的全部基线与候选均完成，不能把完整运行归功于候选改动。

- 共 18 轮候选、18 轮基线在两组 Nagle 突发场景提前关闭，其余 54 轮完整运行。
- 候选首次关闭时间约 0.856–19.4ms，报告为 ChannelClosed。
- 纯 DELIVER 候选事件深度峰值 257–258；混合流量峰值 257–318，包含关闭时追加的终态工单。
- 正常轮次的 CPU、采样工作集、响应/DELIVER 延迟与全部失败差额保存在 CSV。极短失败轮次可能
  被系统 CPU 计时报告为 0，不解释为零成本；SUBMIT 延迟仍不含窗口等待且可能漏掉快速响应。

这些结果说明本轮三个有限改动不足以解决问题，不证明调度因素无关，也没有证明替换公共类型或
扩大缓存会有效。若再立新计划，应先区分“响应确认推进速度”和“事件实际消费速度”，增加相应观测，
再决定是否调整事件准入背压；本轮未实施该范围外改造。

原始数据、初筛统计与候选测试日志位于
[performance-data/burst-dispatch-screen](performance-data/burst-dispatch-screen/)。
所有失败轮均保留，完整 stdout/stderr 仍在本地 `target/burst-optimization/c1-screen` 至 `c3-screen`。

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

## 回归用例与核验

新增 6 项用例，已分别在基线及三个候选上执行通过：

- 单线程/双工作线程各一项：前置未发布工单阻挡后续事件，取消中间工单，检查 95 个剩余事件的顺序、
  Terminal 最后送达、深度 97→0；涵盖候选 2 移至本地缓冲的工单。
- 单线程/双工作线程各一项：关闭消费者后释放门控，检查本地及剩余工单全部释放深度许可。
- 单线程/双工作线程各一项真实 TCP 回归：8 轮纯 SUBMIT_RESP、纯 DELIVER、混合批量报文，
  各 512 个 SUBMIT_RESP 和 DELIVER，检查终态唯一、顺序、DELIVER_RESP 编号/结果/消息号及零丢弃。

门控用例人为延迟 writer 所使用的发布信号，并手动 poll 确认 dispatcher 已阻塞；这是状态级门控验证，
不是对操作系统 socket 缓冲耗尽的模拟。真实 TCP 用例通过请求和确认推进，不使用短 sleep 猜测时序。
消费者停滞关闭和丢弃计数仍由既有故障用例覆盖，本轮已与新增用例一起通过。

已完成：

- 基线及候选 1/2/3 分别使用独立 target 目录构建 release loadtest。
- 三个候选和恢复后的基线通过 `cargo clippy --all-targets -- -D warnings`。
- 三份补丁均通过 `git apply --check`。
- 两个 PowerShell 脚本通过语法解析；配对统计器使用合成数据核验置信区间和失败轮次排除。
- `cargo test --no-fail-fast`：基线及三个候选各 47 项通过（29 单元、17 集成、1 文档）。
- `git diff --check` 通过。

## 复现命令与原验收门槛

以下保留复现和正式验收模板。三个候选已在初筛淘汰，后续长测命令本轮没有执行：

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
保留失败证据。本轮已依此保留失败证据，不沿用旧矩阵充当本轮验收结果。

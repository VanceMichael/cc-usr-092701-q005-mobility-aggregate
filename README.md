# 跨区域客流统计归集

面向交通统计处的离线可重算归集服务：接收铁路、公路、水路、民航带有来源、统计日、区域和口径的分项数据，
保留全部原始版本，生成锁定的日报及环比、同比结果，并支持审计人员用一份固定输入重现当时的计算过程。
数据均为演示用虚构内容。

## 业务规则与不变量

- **幂等入库**：分项自然键为（来源、来源序号、统计日）。相同来源序号以相同载荷再次上传，返回原接收回执，不重复累计。
- **冲突进复核**：同一自然键的数值或口径发生变化时不静默覆盖，原值保留，挂出待办复核单（`accept` 采纳来件 / `reject` 维持原值），结论与结论人留痕。
- **整批原子**：批量导入先整批校验，任一条不合法（含批次内自然键重复）则整批拒绝、状态不落盘，杜绝半截累计。
- **封账锁定**：日报一旦发布即不可变；晚于封账时刻或发布之后到达的数据标记为晚到，只登记底账，不改变任何已发布快报。
- **授权更正**：已发布日报只能由授权人员凭更正原因生成新版本；旧版本永久保留、可取回，版本间差异逐项列出。
- **口径可追溯**：铁路、公路的分类口径在汇总中按口径逐项保留；查询者可由总量下钻到运输方式、口径、分项及其修订与复核记录。
- **可重算审计**：每份报告内嵌发布时点分项快照与输入指纹清单；用同一输入 bundle 重放必然得到同一组报告与摘要哈希。
- **重启安全**：状态以 JSON 原子写入（临时文件 + `os.replace` + fsync），崩溃后状态只可能停在某一操作之前或之后。

## 模块

| 模块 | 职责 |
| --- | --- |
| `src/mobility_aggregate/models.py` | 分项规范化、十进制数值规范化与载荷指纹（SHA-256） |
| `src/mobility_aggregate/store.py` | JSON 原子存储、重启恢复 |
| `src/mobility_aggregate/service.py` | 归集核心：入库、复核、封账、发布、环比同比、更正、下钻、事件流 |
| `src/mobility_aggregate/replay.py` | 固定输入 bundle 确定性重放，产出报告清单与摘要哈希 |
| `src/mobility_aggregate/cli.py` | 命令行入口 |

## 命令行

```bash
# 整批导入（可配置次日封账时刻与授权人员）
python3 -m src.mobility_aggregate.cli \
  --state data/state.json --issuers zhang,li --cutoff 09:00 \
  import batch.json --at 2026-09-27T07:30

# 发布锁定日报；已发布日再次调用即生成更正版本（需授权人和更正原因）
python3 -m src.mobility_aggregate.cli --state data/state.json --issuers zhang \
  publish 2026-09-26 --issuer zhang --at 2026-09-27T08:00

# 查看复核单、办结复核、查看版本链与审计事件流
python3 -m src.mobility_aggregate.cli --state data/state.json conflicts --status pending
python3 -m src.mobility_aggregate.cli --state data/state.json resolve C00000015 \
  --action accept --issuer zhang --at 2026-09-27T11:00 --note "补报复核采纳"
python3 -m src.mobility_aggregate.cli --state data/state.json versions 2026-09-26
python3 -m src.mobility_aggregate.cli --state data/state.json events

# 查询：总量 -> 运输方式 -> 口径/分项 -> 修订与复核记录
python3 -m src.mobility_aggregate.cli --state data/state.json report 2026-09-26 --drilldown
python3 -m src.mobility_aggregate.cli --state data/state.json report 2026-09-26 --version 1

# 审计重放：固定输入重现当时计算，两次运行摘要哈希一致
python3 -m src.mobility_aggregate.cli replay fixtures/replay_bundle.json
```

## 重放 bundle

`fixtures/replay_bundle.json` 是一份完整示例，按时间线编排 ingest / publish / resolve 操作，
覆盖去年同日与前一日基期、封账后原样补报（幂等）、数值补报（冲突）、复核采纳与授权更正，
最终更正版总量为 19946.2 万人次。bundle 中的复核步骤可按自然键（来源、来源序号、统计日）
定位待办单，无需依赖运行时序号。

## 参与方

交通统计处、铁路数据员、民航数据员、审计人员（另支持按来源扩展公路、水路数据员）。

## 测试

```bash
python3 -m unittest discover -s tests -v
python3 -m compileall -q src tests
```

## 领域资料检查

```bash
python3 -m src.mobility_aggregate.context fixtures/context.json
```

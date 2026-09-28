# 跨区域客流统计归集服务

交通统计处每天汇总铁路、公路、水路、民航四个渠道的跨区域客流，生成统一的跨区域运行快报。各渠道上报周期与修订方式不同，节日期间还有同一车次/航班的补报。本项目提供一个**离线、可重算、事件溯源**的归集服务：接收带来源、统计日、区域和口径的分项数据，保留所有原始版本，生成锁定的日报及环比、同比结果。

演示数据均为虚构内容。

## 业务不变量

| 需求 | 保证方式 |
| --- | --- |
| 相同来源序号再次上传 | 字段指纹一致 → 返回原结果（`idempotent`），不重复累计 |
| 数值或口径冲突 | 原值与新值都保留，进入复核队列（`conflict`），**绝不悄悄覆盖** |
| 批量导入中途失败 | 整批只构造成一个事务块，校验全部通过才原子落盘；任一非法则零事件写入 |
| 服务重启 | 状态全部由仅追加事件日志重放得到，重放时校验链式哈希 |
| 晚到数据跨过封账时间 | 封账日后的上报进入隔离区（`quarantined`），不进入已锁定日报；授权人员可接纳为**更正版素材** |
| 已发布日报不可变 | 原版只读；修订只能由授权人员生成 `correction` 新版本，旧版随时可查 |
| 铁路/公路分类口径可追溯 | `category`（动车组/普速列车、营业性/非营业性客车）与 `caliber` 为一等字段，参与指纹并在汇总、下钻中保留 |
| 总量可下钻 | 总量 → 运输方式 → 分类/口径/区域 → 分项及其修订记录 |
| 审计可重现 | 每份报告带输入哈希与报告指纹；`audit-bundle` 导出固定输入（日志块+清单），`verify-bundle` 重放并独立重算 |

## 架构

```
上传 JSON 分项
   │
   ▼
AggregationService（纯 Python，无外部依赖）
   │  所有状态变化 → 一个事务块（可含多条事件）
   ▼
EventStore：00000001.jsonl、00000002.jsonl …… 仅追加事件日志
   │  块内事件 SHA-256 链式哈希；*.tmp 原子 os.replace
   ▼
重放投影：items / 复核队列 / 隔离区 / 封账日 / 锁定日报版本
```

- `src/mobility_aggregate/encoding.py`：确定性 JSON（键排序、统一两位小数，`ROUND_HALF_UP`）、SHA-256 指纹。客流数值一律走 `Decimal`，拒绝 `float` 直传，保证任何机器复算一致。
- `src/mobility_aggregate/models.py`：`ItemInput / ItemRecord / Conflict / ReportVersion`。
- `src/mobility_aggregate/store.py`：事务块帧 = 块头（引用上一块链头）+ 事件（含前序哈希）+ 块尾；篡改、缺块、断链、序号错乱都会在重放时抛 `CorruptLogError`；崩溃残留的 `.tmp` 被忽略。
- `src/mobility_aggregate/service.py`：提交/幂等/冲突/复核、封账与晚到隔离、发布与授权更正、环比（前一自然日最新发布版）同比（去年同日最新发布版）、下钻、`reverify`、审计包导出/校验。

环比/同比只引用**已锁定发布**的基准快照，因此基准日日后出更正版，也不会改变本期已发布快报中的数字。

## 命令行

```bash
# 1) 导入四渠道分项（可一次给多个批量文件；失败整批回滚）
python3 -m src.mobility_aggregate.cli --store data/store import \
    fixtures/batches/2026-09-25.json fixtures/batches/2026-09-26.json

# 2) 封账并发布锁定原版日报
python3 -m src.mobility_aggregate.cli --store data/store close 2026-09-26
python3 -m src.mobility_aggregate.cli --store data/store publish 2026-09-26 --by 统计处

# 3) 封账后的晚到补报 → 隔离；查看队列
python3 -m src.mobility_aggregate.cli --store data/store import fixtures/batches/2026-09-26-late.json
python3 -m src.mobility_aggregate.cli --store data/store status

# 4) 授权接纳晚到数据并生成更正版本（原版不变）
python3 -m src.mobility_aggregate.cli --store data/store admit air A-20260926-02 \
    --by 统计处 --reason "节日航班补报4.5万人次"
python3 -m src.mobility_aggregate.cli --store data/store correct 2026-09-26 \
    --by 统计处 --reason "纳入民航晚到补报"

# 5) 查询：总量下钻到运输方式、分类口径与分项修订记录
python3 -m src.mobility_aggregate.cli --store data/store query 2026-09-26            # 最新版
python3 -m src.mobility_aggregate.cli --store data/store query 2026-09-26 --version 1 # 原版
python3 -m src.mobility_aggregate.cli --store data/store trace rail R-20260926-01

# 6) 审计复算：重放日志、重算指纹；导出并用固定输入独立复算
python3 -m src.mobility_aggregate.cli --store data/store verify
python3 -m src.mobility_aggregate.cli --store data/store audit-bundle /tmp/bundle-0926
python3 -m src.mobility_aggregate.cli verify-bundle /tmp/bundle-0926
```

冲突裁决：`resolve <source> <seq> --decision keep_original|use_new --by 统计处 [--reason ...]`。

授权名单在目录首次初始化时写入 `data/store/authz.json`（默认可发布/复核人为“统计处”），之后以该文件为准——它本身也是审计固定输入的一部分。

## 测试

```bash
python3 -m unittest discover -s tests -v
python3 -m compileall -q src tests
```

48 个用例覆盖：幂等返回、数值/口径冲突不覆盖、复核两种裁决、批量原子回滚、封账后晚到隔离与重复晚到幂等、授权更正且旧版不变、环同比基准快照、分类口径追溯、版本下钻取回历史值、重启重放一致、审计包复算与篡改检测、崩溃残块忽略、CLI 全流程。

## 领域资料

`fixtures/context.json` 与 `contracts/context.schema.json` 保存参与方（交通统计处、铁路数据员、民航数据员、审计人员）、事实与约束：

```bash
python3 -m src.mobility_aggregate.context fixtures/context.json
```

# 跨区域客流统计归集

本项目保存跨区域客流统计归集所需的领域上下文和校验契约，便于服务端功能围绕真实业务参与方展开。当前版本只提供资料读取、结构校验和命令行摘要，数据均为演示用虚构内容。

## 参与方

交通统计处、铁路数据员、民航数据员、审计人员

## 事实资料

- 9月26日全社会跨区域人员流动量19946.2万人次
- 铁路、公路、水路和民航分别发布客流分项数据
- 不同运输方式的环比和同比变化幅度并不一致

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 编译

```bash
python3 -m compileall -q src tests
```

## 命令行检查

```bash
python3 -m src.mobility_aggregate.context fixtures/context.json
```

# Score Snapshot Contract

发布模型目录包含：

- `scores.csv`
- `manifest.json`
- 可选 `latest_model.json`（位于 artifacts 根目录）

兼容目标：`ValueInvesting` 现有快照读取逻辑可原样消费。

`scores.csv` 最少字段：

- `code` 或 `instrument`
- `qlib_score` 或 `score`

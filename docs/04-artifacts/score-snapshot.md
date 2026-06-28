# Score Snapshot Contract

发布模型目录包含：

- `scores.csv`
- 可选 `signals.csv`
- `manifest.json`
- 可选 `latest_model.json`（位于 artifacts 根目录）

股票突破事件模型还会在同一 `<model_id>/` 目录下保存可复用阶段产物：`events.csv`、`labels.csv`、`feature_panel.csv`、可选 `feature_panel.parquet`、`events_scored.csv`、`feature_importance.csv` 和 `metrics.json`。这些文件用于 QlibResearch 研究复盘；ValueInvesting 仍只依赖 `scores.csv`、`signals.csv` 和 `manifest.json`。

兼容目标：`ValueInvesting` 现有快照读取逻辑可原样消费。

`scores.csv` 最少字段：

- `code` 或 `instrument`
- `qlib_score` 或 `score`

## `signals.csv`

`signals.csv` 用于承载事件型策略信号，例如股票价格突破模型。它不替代 `scores.csv`，而是补充“某只股票在某日出现了一个可解释信号”的信息，供 ValueInvesting 图表 marks 和后续回测模块消费。

最少字段：

- `code` 或 `symbol` 或 `instrument`
- `signal_date` 或 `event_date`

推荐字段：

| 字段 | 说明 |
|------|------|
| `event_id` | 稳定事件 ID |
| `model_id` | 模型版本 |
| `code` | 股票代码 |
| `signal_date` | 信号日期 |
| `feature_date` | 特征日期 |
| `signal_type` | 信号类型，例如 `breakout` |
| `side` | 方向，股票 v1 默认为 `long` |
| `signal_score` | 模型分数 |
| `pred_return_13d` 或 `pred_return_*` | 预测收益 |
| `entry_price` | 入场参考价 |

读取规则：

1. 指定 `model_id` 时，读取 `<artifacts>/<model_id>/manifest.json`，再解析 `signal_path`。
2. 不指定 `model_id` 时，读取 `<artifacts>/latest_model.json`。
3. 如果 latest manifest 没有显式 `signal_path`，但包含 `model_id`，默认读取 `<artifacts>/<model_id>/signals.csv`。
4. `pred_return_*` 支持不同预测周期；读取端会优先识别 `pred_return_13d`，也兼容其他 `pred_return_` 前缀字段。

## `manifest.json`

事件型策略建议补充以下字段：

| 字段 | 示例 | 说明 |
|------|------|------|
| `research_kind` | `breakout_event` | 研究类型 |
| `asset_class` | `stock` | 资产类别 |
| `frequency` | `D` | 信号频率 |
| `label_policy` | `{...}` | 标签周期、目标列和成功阈值 |
| `feature_policy` | `{...}` | 特征列和特征版本 |
| `signal_path` | `signals.csv` | 当前模型目录内的信号文件 |
| `metrics_path` | `metrics.json` | 评估指标文件 |
| `feature_panel_path` | `feature_panel.csv` | 事件级特征文件 |
| `events_path` | `events.csv` | 事件检测阶段产物 |
| `labels_path` | `labels.csv` | 标注阶段产物 |
| `events_scored_path` | `events_scored.csv` | 带模型分数的事件文件 |
| `feature_importance_path` | `feature_importance.csv` | 特征重要性文件 |
| `dataset_id` | `stock-D-csi300-...` | 数据集与配置 hash 标识 |
| `config_hash` | `a1b2c3...` | 检测/标注/特征/source 配置 hash |
| `model_path` | `model.pkl` | 模型文件 |

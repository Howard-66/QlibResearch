# Stock Breakout Research Workflow

本文档说明股票价格突破策略在 QlibResearch 中的 v1 落地方式。当前版本只支持股票，不包含期货、BasisFlow 或期货回测接入。

## 1. 定位与边界

股票突破策略采用“规则先筛候选，ML 再排序”的闭环：

`FinanceDataHub 股票行情 -> QlibResearch 突破事件 -> 标签与特征 -> LightGBM 训练与评分 -> scores/signals 发布 -> ValueInvesting 消费`

职责边界：

| 项目 | 职责 |
|------|------|
| `FinanceDataHub` | 提供股票行情、复权行情、基础面和行业等数据源；不保存策略标签或模型结果 |
| `QlibResearch` | 负责事件检测、标注、特征工程、训练、评估和发布 |
| `ValueInvesting` | 负责读取已发布产物，用于智能选股、图表 marks 和后续回测接入 |

v1 不改变现有周频截面 native workflow，也不替代已有 `scores.csv` 智能选股链路，而是在同一 artifact 体系下增加 `signals.csv`。

## 2. 核心设计

核心模块位于：

`src/qlib_research/core/breakout_stock.py`

主要对象和函数：

| 名称 | 作用 |
|------|------|
| `StockBreakoutConfig` | 股票突破策略参数，包括前高窗口、成交量门槛、标注周期和成功阈值 |
| `detect_stock_breakout_events()` | 检测收盘价突破前高的候选事件 |
| `label_stock_breakout_events()` | 生成未来收益、MFE/MAE 和突破成功标签 |
| `build_stock_breakout_feature_panel()` | 基于事件日及以前数据生成事件级特征 |
| `train_lightgbm_stock_breakout_model()` | 用 LightGBM 训练事件收益回归模型 |
| `build_stock_breakout_signal_frame()` | 将 scored events 转换为 `signals.csv` 合约 |
| `evaluate_stock_breakout_scores()` | 输出 Rank IC、Top 分位收益、命中率等紧凑评估指标 |

默认检测逻辑：

1. 对每只股票按日期排序。
2. 用 `lookback_window=60` 计算事件日前的历史前高。
3. 当日收盘价突破前高，并满足成交量比门槛时，生成 long breakout 事件。
4. 事件只使用当日及以前信息，未来走势只用于标签。

默认标注逻辑：

| 字段 | 含义 |
|------|------|
| `future_return_13d` | 事件后第 13 个交易日收盘相对入场价收益 |
| `mfe_return_13d` | 事件后 13 日内最大有利收益 |
| `mae_return_13d` | 事件后 13 日内最大不利收益 |
| `label_success_13d` | 事件后第 13 个交易日收盘是否达到 `ref_high * 1.03` |

默认特征包括：

1. 突破强度：`breakout_strength`
2. 成交量确认：`volume_ratio`
3. 平台收敛：`consolidation_range`、`range_20d`
4. 波动率：`volatility_20d`
5. 动量：`return_5d/10d/20d`
6. 均线位置：`ma_ratio_5d/10d/20d`

## 3. 离线运行

当前脚本入口：

```bash
uv run python scripts/run_stock_breakout_research.py \
  --universe-profile csi300 \
  --start-date 2020-01-01 \
  --end-date 2026-06-24 \
  --model-id stock-breakout-lgbm-v1 \
  --artifacts-dir artifacts \
  --update-latest
```

这里的“离线”指不在 ValueInvesting 在线请求中训练模型或拉取数据；研究任务仍然默认从本地 FinanceDataHub 获取股票数据。

FDH 默认读取：

| 参数 | 默认值 | 说明 |
|------|------|------|
| `--input-source` | `fdh` | 使用 FinanceDataHub |
| `--fdh-dataset` | `daily_adjusted` | 使用复权日线 |
| `--adjust` | `qfq` | 前复权，适合技术形态检测 |

FDH 输入参数：

| 参数 | 说明 |
|------|------|
| `--symbols` | 逗号分隔股票代码 |
| `--symbols-file` | 股票列表文件，一行一个代码，也支持 CSV 第一列 |
| `--universe-profile` | 使用 QlibResearch 统一股票池，如 `watchlist`、`csi300`、`csi500`、`merged_csi300_500` |
| `--universe-mode` | 指数股票池解析方式，默认 `fixed_universe` |
| `--start-date` | 开始日期，FDH 模式必填 |
| `--end-date` | 结束日期，可选 |

股票池优先级：

1. `--symbols` / `--symbols-file` 显式股票列表优先。
2. 未提供显式股票列表时，使用 `--universe-profile`。
3. `watchlist` 来自 QlibResearch 的 `data/watchlist.json`。
4. `csi300`、`csi500`、`merged_csi300_500` 通过 FDH 指数成分权重解析。

示例：

```bash
uv run python scripts/run_stock_breakout_research.py \
  --universe-profile merged_csi300_500 \
  --universe-mode fixed_universe \
  --start-date 2020-01-01 \
  --end-date 2026-06-24 \
  --model-id stock-breakout-csi300500-v1 \
  --update-latest
```

如果要使用已固定的数据样本做复现实验或单元测试，可以切换到 CSV：

```bash
uv run python scripts/run_stock_breakout_research.py \
  --input-source csv \
  --prices-csv artifacts/input/stock_daily_ohlcv.csv \
  --model-id stock-breakout-lgbm-v1 \
  --artifacts-dir artifacts
```

CSV 最少字段：

| 字段 | 说明 |
|------|------|
| `code` | 股票代码，如 `600519.SH` |
| `date` | 交易日期 |
| `open` | 开盘价 |
| `high` | 最高价 |
| `low` | 最低价 |
| `close` | 收盘价 |
| `volume` | 成交量，可选但建议提供 |

兼容别名：

| 别名 | 归一化字段 |
|------|------|
| `symbol` / `instrument` | `code` |
| `datetime` / `trade_date` | `date` |
| `vol` | `volume` |

常用参数：

| 参数 | 默认值 | 说明 |
|------|------|------|
| `--input-source` | `fdh` | `fdh` 或 `csv` |
| `--fdh-dataset` | `daily_adjusted` | `daily_adjusted` 或 `processed_daily` |
| `--symbols` | 无 | FDH 股票代码列表 |
| `--symbols-file` | 无 | FDH 股票池文件 |
| `--universe-profile` | 无 | `watchlist`、`csi300`、`csi500`、`merged_csi300_500` |
| `--universe-mode` | `fixed_universe` | 指数成分解析方式 |
| `--fdh-batch-size` | `300` | 每批查询 FDH 的股票数 |
| `--start-date` | 无 | FDH 开始日期 |
| `--end-date` | 无 | FDH 结束日期 |
| `--lookback-window` | `60` | 前高检测窗口 |
| `--label-horizon-days` | `13` | 标签观察期 |
| `--success-return-pct` | `0.03` | 成功突破收益阈值 |
| `--min-volume-ratio` | `1.0` | 事件日成交量相对均量门槛 |
| `--train-end-date` | 无 | 设置后，早于等于该日的事件训练，之后事件作为评分样本 |
| `--update-latest` | 关闭 | 写入 artifacts 根目录的 `latest_model.json` |

## 4. 输出产物

运行后在 `artifacts/<model_id>/` 下生成：

| 文件 | 说明 |
|------|------|
| `scores.csv` | 兼容现有智能选股的分数快照 |
| `signals.csv` | 股票突破事件信号，供 ValueInvesting 图表和回测消费 |
| `manifest.json` | 模型、标签、特征和产物路径元信息 |
| `feature_panel.csv` | 事件级训练/评分特征表 |
| `events_scored.csv` | 带模型分数的突破事件 |
| `metrics.json` | 事件模型评估指标 |
| `model.pkl` | LightGBM 模型、特征列和配置 |

`signals.csv` 关键字段：

| 字段 | 说明 |
|------|------|
| `event_id` | 稳定事件 ID |
| `model_id` | 模型版本 |
| `code` | 股票代码 |
| `signal_date` | 信号日期 |
| `feature_date` | 特征日期 |
| `signal_type` | 当前为 `breakout` |
| `side` | 当前为 `long` |
| `signal_score` | 模型输出分数 |
| `pred_return_13d` | 预测收益，周期随配置变化 |
| `entry_price` | 事件入场参考价 |
| `ref_high` / `ref_low` | 突破参考前高/平台低点 |

`manifest.json` 会补充：

1. `research_kind=breakout_event`
2. `asset_class=stock`
3. `frequency=D`
4. `label_policy`
5. `feature_policy`
6. `signal_path`
7. `metrics_path`
8. `feature_panel_path`
9. `model_path`
10. `source_data`

`source_data` 会记录股票池来源，包括：

1. `input_source`
2. `fdh_dataset`
3. `symbols`
4. `symbol_count`
5. `universe_profile`
6. `universe_mode`
7. `start_date`
8. `end_date`
9. `adjust`

## 5. 同步到 ValueInvesting

如果发布目录使用 QlibResearch 默认 `artifacts/`，可继续用现有同步脚本：

```bash
uv run python scripts/sync_to_valueinvesting.py \
  --model-id stock-breakout-lgbm-v1 \
  --set-latest
```

同步后 ValueInvesting 默认从：

`ValueInvesting/data/qlib_artifacts`

读取：

1. `latest_model.json`
2. `<model_id>/scores.csv`
3. `<model_id>/signals.csv`
4. `<model_id>/manifest.json`

## 6. 前端交互与可视化

### 6.1 QlibResearch 研究仪表盘

股票突破研究在 QlibResearch Web 中作为独立一级页面存在：

`/breakout`

设计选择：

1. 不强行塞进现有 native workflow run detail。突破策略是事件级择时研究，数据结构与周频截面 recipe 不同。
2. 复用 QlibResearch 工作台的 artifact 根目录、manifest、API、导航和表格组件。
3. 与 `Runs / Compare / Panels / Tasks` 并列存在，后续可在 `Tasks` 中补“运行突破研究任务”的提交入口。

页面结构：

| 页面 | 作用 |
|------|------|
| `/breakout` | 展示所有 `research_kind=breakout_event` 的模型目录和关键指标 |
| `/breakout/<model_id>` | 展示单个突破模型的研究闭环详情 |

详情页 tabs：

| Tab | 覆盖环节 |
|------|------|
| `事件` | 事件数量、股票覆盖、时间范围、候选规则、事件样例 |
| `标注` | 标签字段、收益字段、成功率、标签样例 |
| `特征` | 特征数量、特征列表、缺失率、特征样例 |
| `训练` | 模型类型、训练目标、分数快照 |
| `评估` | Rank IC、Top 分位收益、Top 命中率、metrics JSON |
| `模型` | 模型文件、signals 文件、score 分布、Top signals |
| `配置` | `source_data`、`label_policy`、manifest |
| `产物` | `scores.csv`、`signals.csv`、`model.pkl` 等 artifact inventory |

后端 API：

| API | 说明 |
|------|------|
| `GET /api/breakout` | 列出突破研究模型 |
| `GET /api/breakout/{model_id}` | 获取单个突破研究模型详情 |

QlibResearch 仪表盘用于研究过程审阅，不承担 ValueInvesting 的线上图表消费职责。

### 6.2 ValueInvesting 消费端可视化

ValueInvesting 个股技术分析页提供股票突破信号的可视化入口：

1. 打开任意股票的技术分析页。
2. 使用 TradingView 图表模式。
3. 点击图表右上方的 `ML突破` 按钮。
4. 在模型输入框中填写 `model_id`，例如 `stock-breakout-lgbm-v1`；留空时读取 latest。
5. 图表会以 `ML` mark 显示 QlibResearch 发布的突破事件。

ML mark 展示信息：

| 字段 | 展示方式 |
|------|------|
| `signal_date` | mark 所在 K 线 |
| `signal_score` | tooltip 中的 score |
| `pred_return_*` | tooltip 中的预测收益 |
| `signal_type` | 当前为突破信号 |

这个前端入口只消费离线发布的 `signals.csv`，不会在 ValueInvesting 前端或在线 API 中训练模型。

## 7. 验收与限制

已覆盖的测试：

```bash
pytest tests/test_breakout_stock.py tests/test_artifacts.py
```

当前限制：

1. v1 只实现股票 long breakout。
2. CSV 入口仅用于固定样本复现、调试或测试；正式研究默认从 FDH 读取股票日线，并优先使用 Universe Profile 描述股票池。
3. 当前没有把突破事件注册到 QlibResearch Workbench 任务队列。
4. 当前 ValueInvesting 已能读取和展示图表 marks，回测策略参数接入可在下一步扩展。
5. 期货、BasisFlow、主力合约、基差库存等能力不属于 v1 范围。

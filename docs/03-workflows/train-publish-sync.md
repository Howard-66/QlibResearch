# Train, Publish, Sync

发布流程拆成两步：

1. `train_publish_snapshot.py` 只负责在 `QlibResearch/artifacts/` 下生成模型目录、快照、manifest 和 portfolio targets
2. `sync_to_valueinvesting.py` 显式把指定模型同步到 `ValueInvesting/data/qlib_artifacts`

这样可以把研究产物与线上消费产物分层管理。

## 股票突破事件模型

股票突破策略使用事件型工作流，不走现有周频截面 panel，但复用同一套 artifact 发布与同步目录。

运行入口：

```bash
uv run python scripts/run_stock_breakout_research.py \
  --universe-profile csi300 \
  --start-date 2020-01-01 \
  --end-date 2026-06-24 \
  --model-id stock-breakout-lgbm-v1 \
  --artifacts-dir artifacts \
  --update-latest
```

默认输入源是 FinanceDataHub。CSV 入口仍保留给固定样本复现实验：

```bash
uv run python scripts/run_stock_breakout_research.py \
  --input-source csv \
  --prices-csv artifacts/input/stock_daily_ohlcv.csv \
  --model-id stock-breakout-lgbm-v1
```

输出目录：

`artifacts/stock-breakout-lgbm-v1/`

关键产物：

- `scores.csv`：兼容智能选股快照。
- `signals.csv`：事件型突破信号，供 ValueInvesting 图表 marks 和后续回测消费。
- `manifest.json`：声明 `research_kind=breakout_event`、`asset_class=stock`、标签策略、特征策略和产物路径。
- `feature_panel.csv`：事件级特征。
- `events_scored.csv`：带模型分数的突破事件。
- `metrics.json`：Rank IC、Top 分位收益和命中率等评估结果。
- `model.pkl`：LightGBM 模型、特征列和配置。

同步到 ValueInvesting：

```bash
uv run python scripts/sync_to_valueinvesting.py \
  --model-id stock-breakout-lgbm-v1 \
  --set-latest
```

同步后 ValueInvesting 会从 `data/qlib_artifacts` 读取 `latest_model.json`、`scores.csv` 和 `signals.csv`。如果 `latest_model.json` 没有显式 `signal_path`，读取端会默认查找 `<model_id>/signals.csv`。

更完整的设计与参数说明见：

`docs/03-workflows/stock-breakout-research.md`

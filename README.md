# QlibResearch

独立于 `ValueInvesting` 的 qlib 研究工作台仓库。当前阶段提供两层入口：CLI/脚本 和 Notebook/复盘；同时预留未来独立 API + Web UI 的目录、文档与契约。

## 当前定位

- 负责周频 panel 导出、训练评估、native workflow、研究产物生成、模型发布与同步。
- 不承载 `ValueInvesting` 的在线智能选股页面、组合录入和业务筛选逻辑。
- 通过兼容的 `scores.csv + manifest.json + latest_model.json` 与 `ValueInvesting` 交付研究结果。

## Quick Start

```bash
uv sync --extra qlib
cp .env.example .env
```

导出 panel：

```bash
uv run python scripts/export_weekly_panel.py --output artifacts/panels/watchlist_weekly.parquet --universe-profile watchlist
```

运行 native workflow：

```bash
uv run python scripts/evaluate_native_weekly.py --panel artifacts/panels/csi300_weekly.parquet --recipe baseline
```

发布并同步模型：

```bash
uv run python scripts/train_publish_snapshot.py --panel artifacts/panels/watchlist_weekly.parquet --model-id watchlist-weekly-lgbm-v1
uv run python scripts/sync_to_valueinvesting.py --model-id watchlist-weekly-lgbm-v1 --set-latest --dry-run
```

## Documentation

- [Docs Index](docs/README.md)
- [Project Positioning](docs/01-overview/project-positioning.md)
- [CLI Quickstart](docs/02-getting-started/cli-quickstart.md)
- [Native Workflow](docs/03-workflows/native-workflow.md)
- [Score Snapshot Contract](docs/04-artifacts/score-snapshot.md)
- [Future UI/API Plan](docs/06-ui-api-future/information-architecture.md)

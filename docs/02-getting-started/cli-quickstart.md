# CLI Quickstart

主要脚本入口：

- `scripts/export_weekly_panel.py`
- `scripts/evaluate_convergence_weekly.py`
- `scripts/evaluate_native_weekly.py`
- `scripts/train_publish_snapshot.py`
- `scripts/sync_to_valueinvesting.py`

推荐顺序：

1. 导出 panel
2. 跑 convergence 或 native workflow
3. 发布 score snapshot
4. 显式 sync 到 `ValueInvesting`

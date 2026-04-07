# Train, Publish, Sync

发布流程拆成两步：

1. `train_publish_snapshot.py` 只负责在 `QlibResearch/artifacts/` 下生成模型目录、快照、manifest 和 portfolio targets
2. `sync_to_valueinvesting.py` 显式把指定模型同步到 `ValueInvesting/data/qlib_artifacts`

这样可以把研究产物与线上消费产物分层管理。

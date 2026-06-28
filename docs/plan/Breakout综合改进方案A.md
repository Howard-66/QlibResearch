# Breakout ML 综合改进方案

## Summary
以 Codex 分支为基线：保留当前 `artifacts/<model_id>/manifest.json + scores.csv + signals.csv` 发布契约、模型首页切换和详情页闭环 tabs；吸收 Claude 分支的 70+ 特征、事件筛选、前端训练、配置管理和更完整评估能力，但不采用其独立 `artifacts/breakout/*` 模型体系。

本轮范围按“股票日线突破 V1 优先”实施：把股票突破策略做成完整可复用研究闭环，期货/BasisFlow 只预留 manifest/interface 扩展位。

## Key Changes
- 导航重构为分组式研究工作台：`Panels / Runs / Compare` 归入“截面选股研究”，`Breakout` 归入“事件择时研究”，`Tasks / Settings` 作为共享运维入口；保留 `/breakout` 路由兼容，但不再把 Breakout 当作与 Runs 同层语义的孤立入口。
- 后端新增 `src/qlib_research/breakout/` 领域包承载检测、标注、70+ 特征、训练、评估、缓存和发布；现有 [breakout_stock.py](/Volumes/Repository/Projects/TradingNexus/QlibResearch/src/qlib_research/core/breakout_stock.py) 保留为兼容 facade。
- 训练流程拆成可复用阶段产物：事件集、标签集、特征面板、训练切分、模型评估、信号发布；最终发布仍写入 `artifacts/<model_id>/`，中间缓存写入 `artifacts/breakout_cache/<dataset_id>/`。
- 增量机制按 `event_id + config_hash + source_data range` 去重：事件检测只重算 lookback overlap，新标签只补 horizon 已完成事件，特征只补新事件或 dirty config，训练可选择复用已有事件/标签/特征。
- 特征工程补齐原文九大类 70+ 特征；Feature tab 展示特征目录、分组、缺失率和分布，Evaluation tab 展示模型相关的 Top20 feature importance，Feature tab 可同步显示当前模型的重要性摘要。
- 训练从 CLI-only 改为 CLI + Task 双入口：继续支持 [run_stock_breakout_research.py](/Volumes/Repository/Projects/TradingNexus/QlibResearch/scripts/run_stock_breakout_research.py)，同时新增任务类型 `run_breakout_research`，前端训练按钮提交到统一 Tasks 队列并复用日志/状态/停止能力。
- 详情页基于 Codex 的链条式 tabs 扩展：事件、标注、特征、训练、评估、模型/信号、配置、产物；现有 [breakout detail page](</Volumes/Repository/Projects/TradingNexus/QlibResearch/web/src/app/breakout/[modelId]/page.tsx>) 从 sample-only 改为分页表格、筛选、统计和图表。
- 评估补齐可视化：train/valid/test 指标、Rank IC/ICIR、Top5/10/20 收益与胜率、分位收益曲线、预测-实际散点/分箱、score 分布、年度/行业/股票切片稳定性、过拟合比率、feature importance。
- qlib 复用策略：不强行套 native workflow；突破策略是事件级择时，不是周频截面 panel。复用 QlibResearch 的 FDH 接入、股票池解析、artifact contract、任务队列、表格/图表组件、同步机制和评估 vocabulary。

## Public Interfaces
- 新增 `TaskKind="run_breakout_research"` 与 `POST /api/tasks/run-breakout-research`，payload 包含 universe、date range、detector、labeler、feature groups、trainer params、cache policy、model_id、update_latest。
- 扩展 `GET /api/breakout`：返回模型列表、latest/active 标记、dataset_id、关键指标、artifact 状态。
- 扩展 `GET /api/breakout/{model_id}`：返回 overview、manifest、config sections、chart payloads、artifact inventory、阶段产物引用。
- 新增分页/筛选 API：`/api/breakout/{model_id}/events`、`/labels`、`/features`、`/signals`，支持 date range、code、label status、score quantile、candidate rule、feature group。
- 新增配置 API：`GET/PUT /api/breakout/config-profiles/{profile_id}`；配置保存为研究 profile，manifest 仍作为不可直接编辑的运行记录。
- 最终产物标准化为：`events.csv/parquet`、`labels.csv/parquet`、`feature_panel.parquet`、`events_scored.csv`、`feature_importance.csv`、`metrics.json`、`scores.csv`、`signals.csv`、`model.pkl`、`manifest.json`。

## Test Plan
- 后端单元测试：事件检测无未来函数、13d 标签边界、70+ 特征列与缺失容错、时间切分、LightGBM early stopping、feature importance、增量缓存去重。
- API 测试：模型列表、详情、事件/标签/特征分页筛选、配置 profile、任务创建命令、artifact 缺失降级。
- 产物兼容测试：`StrategySignalStore` 继续读取 `signals.csv`，`latest_model.json` 与 `scores.csv` 不破坏 ValueInvesting 消费。
- 前端测试：Breakout 模型切换、各 tab 空态/加载态/分页筛选、训练任务提交、评估图表渲染、配置编辑校验。
- 回归测试：`pytest tests/test_breakout_stock.py tests/test_artifacts.py tests/test_app_services.py`，以及 `web` 的 lint/build。

## Assumptions
- 本轮只把股票日线突破 V1 做完整；期货、BasisFlow、分钟级数据、实盘回测面板作为后续阶段。
- `artifacts/<model_id>/` 是对外发布契约，`artifacts/breakout_cache/<dataset_id>/` 只做 QlibResearch 内部复用缓存。
- 模型激活/切换放在 Breakout 首页和模型详情中完成，不单独做孤立的模型管理主页面。
- 配置可编辑的是 detector/labeler/feature/trainer profile；历史 manifest 只读，保证实验可复现。

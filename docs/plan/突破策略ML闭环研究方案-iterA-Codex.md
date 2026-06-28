# 突破策略 ML 全栈闭环研究方案 - Iter A - Codex

## Summary

复用 **QlibResearch 作为统一研究平台**，不新建完整独立项目。QlibResearch 已具备 FDH 数据接入、LightGBM、滚动验证、研究产物发布、Workbench/API 等基础能力，适合扩展为“多因子截面 + 单标的择时 + 股票/期货事件策略”的研究中枢。

边界保持清晰：**FinanceDataHub 负责数据与通用预处理，QlibResearch 负责标注、特征、训练、评估、发布，ValueInvesting/BasisFlow 负责消费、可视化与业务回测**。技术栈维持现有 Python/pandas/LightGBM/FastAPI/Next，不引入新的重型 ML 平台。

## Key Changes

- 在 QlibResearch 增加 `breakout_event` 研究域，与现有周频截面模型并行存在；现有多因子截面流程继续保留。
- 新增统一事件数据结构：`event_id`、`asset_class`、`symbol/product/contract`、`event_date`、`side`、`frequency`、`candidate_rule`、`entry_price`、`ref_high/ref_low`。
- 股票突破事件使用 FDH 股票日线/复权/基础面数据；期货突破事件使用 FDH futures 数据，包括主力/连续合约、daily/weekly/minute、基差、库存、期限结构等。
- 标注逻辑放在 QlibResearch：股票默认采用参考文档的 `13d + 3%` 突破成功标签，并保留 `future_return_13d` 回归目标；期货采用方向归一化收益、MFE/MAE、目标/止损先触发、ATR/R 倍数阈值等可配置标签。
- 特征抽取分为股票与期货适配器，输出统一数值特征表；股票包含突破强度、平台形态、量价、趋势、行业/市场/基本面；期货额外包含持仓量、基差、库存、价差、roll yield、期限结构。
- 训练默认使用 LightGBM，严格按时间切分和 walk-forward 验证，不使用随机切分；指标包括 Rank IC、TopK 收益/命中率、分组收益、回撤、覆盖率、股票/期货/品种/方向切片稳定性。
- 发布产物新增 `events.csv`、`labels.csv`、`feature_panel.parquet`、`signals.csv`、`metrics.json`、`slices.csv`、`feature_importance.csv`、`manifest.json`，并保留现有 `scores.csv + latest_model.json` 兼容链路。
- `manifest.json` 必须声明 `research_kind`、`asset_class`、`universe`、`frequency`、`candidate_rule_version`、`label_policy`、`feature_policy`、`model_id`、`feature_date`、`promotion_gate`、`source_data`。
- ValueInvesting 只消费 QlibResearch 发布结果：扩展 score/signal store，用于选股器排序、TradingView marks、单股择时回测、组合目标权重回测；不在 ValueInvesting 内训练模型。
- BasisFlow 只消费 QlibResearch 期货信号：优先读取 ML `ModelSignalSnapshot`/`strategy_signals.csv`，用于期货模型信号展示、图表叠加和 `ml_breakout` 策略回测；现有规则信号作为 fallback。
- FinanceDataHub 不承载策略标签、候选事件、训练逻辑或模型结果，只沉淀原始数据、标准化行情、合约映射和可复用的确定性 symbol-local 特征。

## Public Interfaces

- QlibResearch 新增任务类型：`export_breakout_events`、`label_breakout_events`、`train_breakout_model`、`publish_strategy_signals`。
- QlibResearch Workbench 新增事件研究视图：事件集、标签分布、训练运行、切片指标、信号预览、发布产物。
- ValueInvesting 新增或扩展 `StrategySignalStore`：按 `model_id`、`symbol`、`date range` 读取 QlibResearch `signals.csv`，并兼容现有 `QlibScoreStore`。
- ValueInvesting 回测配置新增可选项：`ml_signal_filter` 或 `strategy_signal_source="qlib_breakout"`。
- BasisFlow 新增 `strategy_id="ml_breakout"`，其信号源来自 QlibResearch 发布的期货突破信号。
- BasisFlow `get_model_signal(product_code)` 优先读取 QlibResearch 产物；没有可用模型时回退到当前规则基线。

## Test Plan

- QlibResearch：测试突破事件检测、股票/期货标签生成、特征 lookback 边界、时间切分无泄漏、manifest/schema 兼容、LightGBM 小样本训练与发布。
- ValueInvesting：测试 screener 能读取突破模型分数，TradingView marks 能显示 ML 信号，单股回测能按 ML 信号过滤/入场。
- BasisFlow：测试期货模型信号读取、图表信号叠加、`ml_breakout` 回测路径，以及无模型时 fallback 到规则信号。
- 集成验收：一条股票突破模型能从 FDH 数据生成事件、训练、发布并在 ValueInvesting 展示/回测；一条期货突破模型能发布到 BasisFlow 并完成信号展示/回测。
- 回归验收：现有 QlibResearch 周频截面模型、ValueInvesting qlib 智能选股、BasisFlow 规则策略不因新增突破策略改变默认行为。

## Assumptions

- 默认不新建独立项目，只在 QlibResearch 内扩展研究域。
- 默认不把 qlib 原生 workflow 强行用于所有事件择时场景；截面组合继续复用现有 native workflow，突破事件策略使用 QlibResearch 内部 pandas/LightGBM 流程。
- 默认股票 v1 先做日线突破，期货 v1 先做主力/连续合约日线或周线突破，分钟级作为后续扩展。
- 默认离线发布、在线消费，不在 ValueInvesting 或 BasisFlow 的线上 API 中执行训练。

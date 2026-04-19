# 综合优化与实验方案：双 Workflow 策略研究 + Workbench UI 升级

## Summary

基于两份报告，第一阶段按“研究先行 + CLI 任务型分析师”推进，目标不是立刻做更多花哨页面，而是先把研究闭环补完整，再把这些结构化产物接入 web app。

统一后的核心判断如下：

- 固定池长期 run：保留 `baseline` 为主线，重点验证“环境风控 + 共识过滤 + 行业约束 + 持仓定义收敛”。
- 动态成分股 run：保留 `rank_blended` 为主线，重点验证“信号兑现解释 + 行业集中控制 + 实际持仓数约束 + 弱 alpha 指标修复”。
- 两条 workflow 的共性问题优先级高于单次收益优化：
  - 名义 `topk=10` 与实际持仓数偏离过大
  - `signal diagnostics` 与真实组合收益之间缺少桥接解释
  - 行业集中度缺少硬约束
  - 当前 web app 更像产物浏览器，不像研究决策台

第一阶段采用“收敛型验证”，只做 7 组高信号实验，并同步补最关键的研究产物、诊断指标和 UI 入口。

## 研究优化与实验方案

### 1. 先补方法学与产物层，不先改模型大方向

先新增一组“研究桥接产物”，作为所有后续实验的统一输出。每个 recipe 的 `rolling` 和 `walk_forward` 都要新增以下表：

- `signal_realization_bridge.csv`
  - 每期输出：`feature_date`、`bundle`、`topk_mean_return_4w`、`topk_mean_excess_return_4w`、`new_position_return_4w`、`carry_position_return_4w`、`rebalance_drag`、`execution_cost_drag`、`realized_portfolio_return`
  - 目的：解释“信号超额”和“组合净值”为什么可能背离
- `holding_count_drift.csv`
  - 每期输出：`target_hold_count`、`actual_hold_count`、`residual_hold_count`、`blocked_sell_count`、`topk_overlap_prev`
  - 目的：解释实际组合是否仍然是 `topk=10`
- `sector_exposure_history.csv`
  - 每期输出一级行业权重、金融合计权重、前 3 行业集中度
  - 目的：约束当前“银行 60%”这类集中风险
- `regime_gate_diagnostics.csv`
  - 每期输出：`macro_phase`、`score_dispersion`、`topk_unique_score_ratio`、是否触发降仓/过滤
  - 目的：让环境风控可复盘
- `experiment_scorecard.json`
  - 每个实验统一输出：主指标、次指标、是否过 gate、相对 incumbent 的差值、结论标签
  - 目的：避免后续再靠人工读多个 csv 判胜负

这些产物必须先补，再做实验比较；否则第二轮报告还会重复“现象描述多、解释链不足”的问题。

### 2. 第一阶段 7 组收敛型实验

按顺序执行，不并行混改。每组实验都同时跑两条 workflow，但使用各自 incumbent 作为对照。

#### 实验 A：执行定义收敛实验

目标：收紧“名义 topk 与真实持仓不一致”。

设置：

- 对照组：当前默认 `retain_quotes_for_existing_positions`
- 实验组 A1：`strict_membership_only`
- 实验组 A2：保留当前 exit policy，但增加“实际持仓上限 = 12”
- 实验组 A3：保留当前 exit policy，但增加“实际持仓上限 = 14”

判定标准：

- `avg_actual_hold_count <= topk + 2`
- `max_actual_hold_count <= topk + 4`
- `blocked_sell_periods` 不高于 incumbent 太多
- 收益比较放在第二优先级，先保证组合定义可信

#### 实验 B：行业集中约束实验

目标：抑制固定池的金融偏离和动态池的银行过度集中。

设置：

- `industry_max_weight = 0.20`
- `industry_max_weight = 0.25`
- `industry_max_weight = 0.30`

判定标准：

- 前 1 行业权重不超过设定值
- 前 3 行业集中度显著下降
- walk-forward 年化收益不劣于 incumbent 超过 2 个百分点
- 最大回撤不恶化超过 3 个百分点

#### 实验 C：分数门限实验

目标：避免低信号质量时仍然机械建仓。

设置：

- `min_score_spread = 0.00`
- `min_score_spread = 0.02`
- `min_score_spread = 0.05`

同时输出每期“因门限导致少买/不买”的记录。

判定标准：

- `topk_unique_score_ratio` 提升
- `score_dispersion` 低位期的回撤收敛
- 若收益略降但回撤显著改善，允许进入下一轮

#### 实验 D：环境风控实验

目标：把固定池报告中的 `STAGFLATION` 脆弱性和动态池中的低信号环境问题统一处理。

设置：

- D1：`macro_phase == STAGFLATION` 时仓位降为 50%
- D2：`score_dispersion < 0.03` 时仓位降为 50%
- D3：双门并用，任一触发即减仓
- D4：`score_dispersion < 0.02` 时回到 benchmark 组件

判定标准：

- 最大回撤优先改善
- 回撤恢复期缩短
- 年化收益允许小幅牺牲，但 Calmar 必须提升

#### 实验 E：固定池主线共识实验

仅用于长期固定股票池 workflow。

设置：

- E1：`baseline + huber_8w` 共同入选优先，再由 `baseline` 补足名额
- E2：两者分数标准化后均值融合
- E3：只保留双重共识标的，允许持仓数不足时转 benchmark

判定标准：

- `walk_forward_rank_ic_ir` 不低于 `baseline`
- 最大回撤优先改善
- 当前报告指出的近两年走弱必须被缓解

#### 实验 F：动态池主线共识实验

仅用于动态成分股 workflow。

设置：

- F1：`rank_blended + mae_4w` 共同入选优先
- F2：分数标准化后均值融合
- F3：`rank_blended` 为主，`mae_4w` 只作为 veto 过滤器

判定标准：

- 保持 `rank_blended` 的收益优势
- 降低银行过度集中
- 修复“组合赚钱但 topk_mean_excess 为负”的解释张力

#### 实验 G：持仓数敏感性实验

目标：验证 `topk=10` 是否本身过于尖锐。

设置：

- `topk = 10`
- `topk = 12`
- `topk = 15`

约束：

- 行业上限用实验 B 当前最优值
- exit policy 用实验 A 当前最优值
- 不再叠加其他新改动

判定标准：

- 如果 `topk=12/15` 明显降低回撤且收益仅小幅下降，则后续主线切换到更宽持仓
- 如果动态池的银行集中因 `topk` 扩大而自然改善，优先采用该路线

### 3. 实验 gate 与结论规则

后续所有实验统一按以下顺序判定，不再只看累计收益：

1. 组合定义可信
   - `avg_actual_hold_count <= topk + 2`
   - 有完整 `signal_realization_bridge`
2. 信号质量不过度退化
   - `topk_unique_score_ratio` 均值 >= 0.85
   - 最新一期 >= 0.90
3. 风险先不过线
   - `max_drawdown` 不恶化超过 3 个百分点
4. 再看收益效率
   - 年化收益提升优先
   - 若收益持平，优先选回撤更低、集中度更低的版本

最终第一阶段只允许每条 workflow 选出 1 个“下一轮主线配置”。

## Web App / Workbench UI 优化方案

### 1. 产品定位调整

当前 web app 已有 `runs / run detail / recipe detail / compare / panels / tasks` 骨架，但仍偏“CSV 浏览器”。第一阶段应升级为“研究决策台”，目标用户是内部研究者，而不是外部投资者。

第一阶段 UI 分三层：

- 总览层：快速判断哪个 run/recipe 值得继续
- 诊断层：解释为什么好/为什么差
- 行动层：一键生成 rerun、分析任务、对比任务

### 2. 必补页面能力

#### Runs 列表页

新增：

- `incumbent / promoted / rejected / needs_explanation` 状态标签
- 每个 run 的“主结论卡”
  - 主 recipe
  - 当前最大问题
  - 推荐下一步动作
- “实验得分卡”摘要
  - 收益
  - 回撤
  - 组合定义可信度
  - 行业集中度
  - 信号兑现解释完整度

#### Run Detail 页

新增分区：

- `Research Summary`
  - 自动读取 `experiment_scorecard.json`
  - 以 5-8 条自然语言总结 run 的结论
- `Risk & Exposure`
  - 行业权重时间序列
  - 前 3 行业集中度趋势
  - 金融/成长/消费/周期风格暴露
- `Execution Realization`
  - 展示 `signal_realization_bridge.csv`
  - 新开仓、残留仓、再平衡拖累的拆解图
- `Experiment Recommendations`
  - 根据当前 run 自动给出下一轮实验建议

#### Recipe Detail 页

现有 tab 基础上新增：

- `Portfolio Realization`
  - `signal -> position -> net value` 桥接图
- `Exposure`
  - 行业暴露面积图
  - 持仓数漂移折线图
- `Interpretation`
  - 针对当前 recipe 的自动解读摘要
  - 不是表格，而是结构化诊断文本

#### Compare 页

增强为“实验比较台”：

- 默认比较单位从单纯 item 切换为“incumbent vs candidate”
- summary 不只给表格，增加：
  - 胜负结论
  - 哪些指标变好
  - 哪些指标变差
  - 是否通过 gate
- 增加 `signal realization bridge`、`sector exposure history`、`holding_count_drift` 的并排比较

### 3. 新增必要的可视化与工具

第一阶段必须补的可视化：

- 净值曲线 + 回撤曲线双轴联动
- 年度收益与基准超额柱状图
- 行业暴露历史面积图
- 持仓数漂移图
- 信号离散度 / TopK 唯一性时间序列
- 新开仓收益 / 残留仓收益 / 成本拖累堆叠图
- 因子重要性稳定性图
- 实验 scorecard 雷达图或矩阵图

第一阶段必须补的工具：

- “从当前 run 创建实验”向导
  - 自动带入 incumbent 配置
  - 用户只勾选要变的实验维度
- “一键加入 compare”
- “生成研究解读任务”
- “导出研究纪要”
- “查看当前主线配置与推荐配置差异”

### 4. 结果解读与路径引导

加一套统一的研究引导语言，不让用户自己猜：

- 在 run 页给出一句话判断：
  - “可继续推进”
  - “收益好但解释不足”
  - “信号退化，不建议晋升”
  - “需先修执行定义”
- 在 recipe 页给出固定顺序的解读模块：
  - 先看收益
  - 再看回撤
  - 再看信号质量
  - 再看执行偏差
  - 最后看暴露与建议
- 在 compare 页给出最终 verdict：
  - `promote`
  - `hold`
  - `reject`
  - `investigate`

## CLI 分析能力接入方案

### 1. 接入方式

采用“任务型分析师”，不做简单 API key 调 LLM。

新增一个任务类型：

- `run_research_analysis`

任务输入：

- source 类型：`run`、`recipe`、`compare set`
- 分析模板：`investment_report`、`experiment_review`、`ui_insight`、`anomaly_diagnosis`
- 分析引擎：`codex_cli`、`claude_cli`
- 可选 skill 列表

任务执行方式：

- 由现有任务队列统一调度
- CLI 读取指定 run/recipe/compare 的结构化产物
- 输出 markdown + json
- 产物落到对应 run 目录或 compare 目录下的 `analysis/`

### 2. 新增产物

新增：

- `analysis/latest_summary.md`
- `analysis/latest_summary.json`
- `analysis/experiment_review.md`
- `analysis/ui_recommendations.md`

JSON 必须结构化，至少包含：

- `headline`
- `verdict`
- `key_findings`
- `risks`
- `recommended_next_experiments`
- `recommended_ui_actions`

### 3. 前后端接口变更

需要扩展公共接口和类型：

- `TaskKind` 增加 `run_research_analysis`
- `RunDetail` 增加 `analysis_reports`
- `RecipeDetail` 增加 `analysis_reports`
- `CompareResponse` 增加 `analysis_summary`
- 新增 `AnalysisReportRef`
  - `name`
  - `path`
  - `engine`
  - `template`
  - `updated_at`
  - `verdict`
- 任务预设增加“从 run/recipe/compare 创建分析任务”

实现集中在：
- `src/qlib_research/app/contracts.py`
- `src/qlib_research/app/services.py`
- `web/src/lib/types.ts`

## Test Plan

### Backend / Research

- 新增产物生成测试
  - `signal_realization_bridge.csv`
  - `holding_count_drift.csv`
  - `sector_exposure_history.csv`
  - `experiment_scorecard.json`
- 新增实验 gate 测试
  - 高收益但前十同分的候选不能晋升
  - 持仓数偏离过大的候选不能直接晋升
- 新增 CLI 分析任务测试
  - 任务创建
  - 队列执行
  - 产物落盘
  - 失败时日志与状态正确

### Frontend

- run detail / recipe detail / compare 页对新增字段的渲染测试
- 没有分析产物时降级展示正常
- 新增图表在空表、部分列缺失时不崩溃
- 任务页可以创建 `run_research_analysis` 任务并正确显示状态

### Acceptance Scenarios

- 用户打开一个 run，能在 1 个页面内看到“结论、风险、暴露、下一步动作”
- 用户比较两个 recipe，能直接看到 promote/hold/reject verdict，而不是只看表格
- 用户从 run 页发起 CLI 分析任务，完成后能在 UI 中查看结构化研究结论
- 第一轮 7 组实验结束后，每条 workflow 都能明确选出 1 个下一轮主线配置

## Assumptions

- 忽略两份报告里的 panel fixed/dynamic 口径笔误，不把“修正文案”作为本轮工作流目标。
- 两次 run 各自绑定的 panel 已经是事实来源，本方案只基于其研究结论做综合优化。
- 第一阶段不做页面内自由聊天式助手，CLI 只作为后台研究任务能力接入。
- 第一阶段不追求完全自动决策，LLM 输出只负责“结构化解读与建议”，最终 gate 仍由确定性指标判定。

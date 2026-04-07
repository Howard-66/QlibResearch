# qlib 原生工作流输出结果解读指南

## 1. 这份文档解决什么问题

这份指南用来回答两个最常见的问题：

1. 跑完 native workflow 以后，先看什么，后看什么。
2. 指标很多时，怎样快速判断它是在”会排不会赚”、还是”执行扭曲”、还是”稳定性不足”。

本文结合当前仓库中的示例产物做说明，示例路径主要来自：

- [baseline output](/Volumes/Repository/Projects/TradingNexus/ValueInvesting/data/qlib_artifacts/native_workflow/csi300/baseline)

---

## 2. 推荐的阅读顺序

不要一上来就看净值曲线。推荐顺序是：

1. notebook 里的集中指标表 / `recipe_overview`
   - 先快速筛掉明显不值得继续看的 recipe
2. `rolling_summary.csv` / `walk_forward_summary.csv`
   - 先判断信号本身是否有排序能力
3. `signal_diagnostics.csv`
   - 再判断分数有没有粒度、TopK 是否挤在一起
4. `portfolio_diagnostics.csv`
   - 再看执行层有没有把目标持仓扭曲掉
5. `execution_diff_summary.csv`
   - 最后判断 strict native 和 validation-like 的差异到底有多大
6. `rolling_native_report.csv` / `walk_forward_native_report.csv`
   - 配合月度 / 年度 heatmap 看收益路径和回撤结构

---

## 3. 输出文件全景图

### 3.1 核心输出文件清单

| 文件名 | 用途 | 重要程度 |
|--------|------|----------|
| `native_workflow_manifest.json` | 全局配置元信息 | ⭐⭐ |
| `recipe_overview` (notebook内) | 多 recipe 汇总看板 | ⭐⭐⭐⭐⭐ |
| `rolling_summary.csv` | Rolling 模式汇总统计 | ⭐⭐⭐⭐⭐ |
| `walk_forward_summary.csv` | Walk-forward 模式汇总统计 | ⭐⭐⭐⭐⭐ |
| `rolling_details.csv` | Rolling 逐期明细 | ⭐⭐⭐ |
| `walk_forward_details.csv` | Walk-forward 逐期明细 | ⭐⭐⭐ |
| `signal_diagnostics.csv` | 信号质量诊断 | ⭐⭐⭐⭐ |
| `portfolio_diagnostics.csv` | 组合执行诊断 | ⭐⭐⭐⭐ |
| `execution_diff_summary.csv` | Native vs Validation 对比 | ⭐⭐⭐⭐ |
| `rolling_native_report.csv` | Rolling 净值曲线 | ⭐⭐⭐⭐ |
| `walk_forward_native_report.csv` | Walk-forward 净值曲线 | ⭐⭐⭐⭐ |
| `slice_regime_summary.csv` | 切片稳定性分析 | ⭐⭐⭐⭐ |
| `feature_prefilter.csv` | 特征预筛选统计 | ⭐⭐ |
| `rolling_feature_importance.csv` | 特征重要性明细 | ⭐⭐⭐ |
| `latest_score_frame.csv` | 最新一期预测分数 | ⭐⭐⭐⭐⭐ |
| `*_monthly_return_heatmap.csv` | 月度收益热力图 | ⭐⭐⭐ |
| `*_annual_return_heatmap.csv` | 年度收益热力图 | ⭐⭐⭐ |

## 3. 如何读信号层指标

### 3.1 `rank_ic_ir`

它衡量排序稳定性。越高，说明“高分股票相对更容易排在后续高收益股票前面”。

但注意：

- `rank_ic_ir` 好，不等于组合一定赚钱。
- 如果 `topk_mean_excess_return_4w` 仍然很低，往往是“会排不会赚”。

### 3.2 `topk_mean_excess_return_4w`

这是更接近组合兑现的信号指标。它直接看 TopK 相对横截面的超额收益。

例如当前示例里：

- [rolling_summary.csv](/Volumes/Repository/Projects/TradingNexus/ValueInvesting/data/qlib_artifacts/native_workflow/csi300/baseline/rolling_summary.csv) 的 `rank_ic_ir` 约 `0.1226`
- 但同一文件里的 `topk_mean_excess_return_4w` 约 `-0.00584`

这类组合通常说明：

- 排序不是完全没信息
- 但前排股票没有稳定兑现成可交易的超额收益

再看 [walk_forward_summary.csv](/Volumes/Repository/Projects/TradingNexus/ValueInvesting/data/qlib_artifacts/native_workflow/csi300/baseline/walk_forward_summary.csv)：

- `rank_ic_ir` 约 `0.2407`
- `topk_mean_excess_return_4w` 只有 `0.000242`

这更像“排序有改善，但收益兑现仍偏弱”。

---

## 4. 如何读分数粒度

优先看：

- [signal_diagnostics.csv](/Volumes/Repository/Projects/TradingNexus/ValueInvesting/data/qlib_artifacts/native_workflow/csi300/baseline/signal_diagnostics.csv)

关键字段：

- `score_dispersion`
- `score_unique_count`
- `topk_unique_score_ratio`
- `topk_overlap_prev`

经验判断：

- `score_dispersion` 太小：分数挤在一起，排序难转成稳定选股差异
- `topk_unique_score_ratio` 太低：TopK 里很多票本质同分，信号粒度不足
- `topk_overlap_prev` 太高：可能过于黏滞；太低：可能过度换手

当前示例的前几行里：

- `coverage` 约 `298`
- `score_dispersion` 在 `0.12 ~ 0.14`
- `topk_unique_score_ratio` 基本是 `1.0`

这说明当前示例的“分数离散度”本身并不差，问题更可能在收益兑现或执行层，而不是“所有票都打成一样的分数”。

---

## 5. 如何读执行层

优先看：

- [portfolio_diagnostics.csv](/Volumes/Repository/Projects/TradingNexus/ValueInvesting/data/qlib_artifacts/native_workflow/csi300/baseline/portfolio_diagnostics.csv)

关键字段：

- `target_hold_count`
- `actual_hold_count`
- `blocked_sell_count`
- `residual_hold_count`

当前示例前几行显示：

- `target_hold_count = 10`
- `actual_hold_count = 10`
- `blocked_sell_count = 0`

这表示当前这版 baseline 至少没有明显的“持仓数长期漂移”或“卖不掉导致残留仓位堆积”的问题。

如果你以后看到：

- `actual_hold_count` 长期高于 `target_hold_count`
- `blocked_sell_count` 经常不为 0

那就优先怀疑：

- quote 缺失
- tradable 过滤过严
- hold buffer / rebalance 逻辑导致持仓释放不干净

---

## 6. 如何读 strict native vs validation-like 差异

优先看：

- [execution_diff_summary.csv](/Volumes/Repository/Projects/TradingNexus/ValueInvesting/data/qlib_artifacts/native_workflow/csi300/baseline/execution_diff_summary.csv)

关键字段：

- `native_minus_validation_return`
- `native_max_drawdown`
- `validation_max_drawdown`

判断方法：

- 如果 `native_minus_validation_return` 长期非常大，说明执行口径差异会显著改变研究结论
- 如果两者差异很小，说明信号层结论更稳

当前示例中：

- `rolling` 的 `native_minus_validation_return` 约 `-0.0114`
- `walk_forward` 的 `native_minus_validation_return` 约 `0.3684`

这说明当前仓库这组示例里，walk-forward 阶段 strict native 和 validation-like 的差异并不小，执行层口径会明显影响结论，不能只看 signal summary。

---

## 7. 如何读净值曲线与 heatmap

优先看：

- `rolling_native_report.csv`
- `walk_forward_native_report.csv`
- `rolling_native_monthly_return_heatmap.csv`
- `rolling_native_annual_return_heatmap.csv`
- `walk_forward_native_monthly_return_heatmap.csv`
- `walk_forward_native_annual_return_heatmap.csv`

### 7.1 净值曲线

先看三件事：

- 最终净值有没有赢 benchmark
- 最大回撤是否超出可接受范围
- 净值上涨是否靠少数几段突发拉升

### 7.2 月度 heatmap

月度 heatmap 用 0 作为中线：

- 绿色：多头盈利月
- 红色：空头亏损月
- 越接近白色：收益越接近 0

正确读法不是“有没有几个月很亮”，而是：

- 绿色月份是否连续
- 红色月份是否集中爆发
- 正收益是否只来自一两个极端月份

### 7.3 年度 heatmap

年度 heatmap 用来看收益是否依赖单一年份。

如果只有一年特别亮，其他年份接近白色或偏红，通常说明：

- 策略对 regime 很敏感
- 研究价值有限
- 需要回到 `slice_regime_summary.csv` 看年份 / 宏观 phase 切片

---

## 8. 如何读切片稳定性

优先看：

- [slice_regime_summary.csv](/Volumes/Repository/Projects/TradingNexus/ValueInvesting/data/qlib_artifacts/native_workflow/csi300/baseline/slice_regime_summary.csv)

重点看三类切片：

- `feature_year`
- `l1_name`
- `macro_phase`

判断重点：

- 某些年份是否明显退化
- 某些行业是否长期负贡献
- 某个宏观 phase 是否几乎失效

如果一个 recipe 只在少数行业或少数年份好看，它就更像“局部有效配方”，研究价值会低于“跨年份、跨行业都还能站住”的 recipe。

---

## 9. 一套实用的结论模板

核心指标速查：

| 指标                             | 健康标准               | 含义     |
| ------------------------------ | ------------------ | ------ |
| rank\_ic\_ir                   | > 0.1 有价值，> 0.2 较强 | 预测稳定性  |
| topk\_mean\_excess\_return\_4w | > 0                | 实际超额收益 |
| score\_dispersion              | > 0.05             | 分数区分度  |
| topk\_unique\_score\_ratio     | 接近 1.0             | 选股不拥挤  |
| topk\_overlap\_prev            | 0.4-0.7            | 换仓健康区间 |
| blocked\_sell\_count           | = 0                | 无执行障碍  |


当你看完一轮 native workflow，可以按这个模板给结论：

1. 信号层：`rank_ic_ir` 是否达标，TopK 超额是否真正改善。
2. 粒度层：TopK 是否仍存在明显同分或分数拥挤。
3. 执行层：`actual_hold_count` 是否贴近目标，`blocked_sell_count` 是否可控。
4. 口径层：strict native 与 validation-like 差异是否会改写结论。
5. 风险层：回撤和 heatmap 是否说明收益路径足够平滑。
6. 稳定性层：年份 / 行业 / 宏观 phase 切片是否仍然成立。

只有这六层大体一致时，才适合把它当成“可继续推进”的候选 recipe。

---

## 10. 配合哪些文档一起看

建议配合阅读：

- [qlib原生工作流使用指南.md](/Volumes/Repository/Projects/TradingNexus/ValueInvesting/docs/qlib原生工作流使用指南.md)
- [qlib原生工作流节点解读指南.md](/Volumes/Repository/Projects/TradingNexus/ValueInvesting/docs/qlib原生工作流节点解读指南.md)
- [qlib智能选股落地与使用说明.md](/Volumes/Repository/Projects/TradingNexus/ValueInvesting/docs/qlib智能选股落地与使用说明.md)

---

## 附录 A：各表格字段详解

### A.1 rolling_summary.csv / walk_forward_summary.csv

汇总统计表，每个 bundle 一行。

| 列名 | 含义 | 解读要点 |
|------|------|----------|
| `bundle` | 评估模式 | `rolling` / `walk_forward` |
| `evaluation_dates` | 评估日期数量 | 52 / 26 期 |
| `coverage_mean` | 平均覆盖率 | 股票池覆盖率，应接近 300 |
| `rank_ic_mean` | Rank IC 平均值 | 预测相关性均值 |
| `rank_ic_std` | Rank IC 标准差 | 预测稳定性指标 |
| `rank_ic_ir` | Rank IC 信息比率 | **最重要指标**：`> 0.1` 有价值，`> 0.2` 较强 |
| `ic_mean` | 普通 IC 平均值 | Pearson 相关性 |
| `topk_mean_return_4w` | TopK 4 周平均收益 | 绝对收益 |
| `topk_mean_excess_return_4w` | TopK 4 周平均超额收益 | **核心指标**：相对基准收益 |
| `topk_hit_rate` | TopK 跑赢基准比例 | 命中率 |
| `universe_mean_return_4w` | 全市场平均收益 | 基准参考 |
| `used_feature_count` | 实际使用特征数 | 应与配置一致 |

### A.2 rolling_details.csv / walk_forward_details.csv

逐期明细表，每一行对应一个评估日期。

| 列名 | 含义 | 使用场景 |
|------|------|----------|
| `feature_date` | 特征日期 | 模型训练日期 |
| `coverage` | 当期覆盖率 | 检查数据质量 |
| `rank_ic` | 当期 Rank IC | 观察时序稳定性 |
| `ic` | 当期普通 IC | |
| `topk_mean_return_4w` | 当期 TopK 收益 | 对应未来 4 周收益 |
| `topk_mean_excess_return_4w` | 当期 TopK 超额收益 | |
| `topk_hit_rate` | 当期命中率 | |
| `score_dispersion` | 分数离散度 | 衡量信号区分度 |
| `score_unique_count` | 分数唯一值数量 | 检测信号拥挤度 |
| `topk_unique_score_ratio` | TopK 分数唯一比例 | **重要**：过低表示选股集中 |

### A.3 signal_diagnostics.csv

信号质量诊断表，每行对应一个信号日期。

| 列名 | 含义 | 健康标准 |
|------|------|----------|
| `signal_date` | 信号生成日期 | |
| `coverage` | 当期覆盖率 | `< 250` 需警惕数据缺失 |
| `score_dispersion` | 分数离散度 | `< 0.05` 信号过于集中 |
| `score_unique_count` | 分数唯一值数量 | |
| `topk_unique_score_ratio` | TopK 分数唯一比例 | 应接近 1.0 |
| `topk_overlap_prev` | 与上期 TopK 重叠比例 | 0.4-0.7 为健康区间 |
| `future_return_top_bottom_decile_spread` | Top/Bottom 十分位收益差 | `> 0` 表示信号有区分度 |
| `excess_return_top_bottom_decile_spread` | Top/Bottom 十分位超额收益差 | **核心指标**：`> 0.02` 信号有效 |

### A.4 portfolio_diagnostics.csv

组合执行诊断，记录每期持仓与风险指标。

| 列名 | 含义 | 健康标准 |
|------|------|----------|
| `signal_date` | 信号日期 | |
| `trade_date` | 实际交易日期 | 通常滞后 1 周（执行延迟） |
| `target_hold_count` | 目标持仓数量 | 配置的 topk 值 |
| `actual_hold_count` | 实际持仓数量 | 应与目标一致 |
| `blocked_sell_count` | 阻止卖出数量 | 应为 0 |
| `blocked_sell_codes` | 阻止卖出股票代码 | 需人工关注 |
| `residual_hold_count` | 残余持仓数量 | 应为 0 或 1 |
| `rebalance_interval_steps` | 调仓间隔步数 | |
| `hold_buffer_rank` | 持仓缓冲排名 | |

### A.5 execution_diff_summary.csv

Native vs Validation 执行对比。

| 列名 | 含义 | 解读 |
|------|------|------|
| `recipe` | Recipe 名称 | |
| `bundle` | 评估模式 | rolling / walk_forward |
| `native_final_net_value` | Native 回测最终净值 | 使用真实交易约束 |
| `validation_final_net_value` | Validation 回测最终净值 | 理想化假设 |
| `native_minus_validation_return` | 收益差异 | `< 0` 表示执行损耗 |
| `native_max_drawdown` | Native 最大回撤 | |
| `validation_max_drawdown` | Validation 最大回撤 | |

### A.6 rolling_native_report.csv / walk_forward_native_report.csv

原生回测净值曲线，每行一个调仓日。

| 列名 | 含义 | 用途 |
|------|------|------|
| `datetime` | 调仓日期 | |
| `account` | 账户资产 | 扣费后净值 |
| `return` | 当期收益率 | |
| `turnover` | 当期换手率 | 过高增加交易成本 |
| `cost` | 当期交易成本 | |
| `value` | 持仓市值 | |
| `cash` | 现金余额 | |
| `bench` | 基准净值 | 用于对比 |
| `net_return` | 扣费后收益 | **实际收益** |
| `benchmark_return` | 基准收益 | |
| `net_value` | 扣费后净值 | **核心指标** |
| `benchmark_value` | 基准净值曲线 | |
| `relative_drawdown` | 相对基准回撤 | |
| `benchmark_excess_drawdown` | 超额收益回撤 | |

### A.7 slice_regime_summary.csv

切片维度分析表，检验信号在不同子群体的稳定性。

| 列名 | 含义 | 解读要点 |
|------|------|----------|
| `slice_type` | 切片类型 | `feature_year` / `l1_name` / `macro_phase` |
| `slice_value` | 切片值 | 如 `2025`、`有色金属`、`REFLATION` |
| `coverage` | 样本数量 | 过少样本统计意义弱 |
| `score_dispersion` | 分数离散度 | 各切片内离散度一致性 |
| `mean_future_return_4w` | 平均 4 周收益 | |
| `mean_excess_return_4w` | 平均超额收益 | **核心指标** |

**slice_type 类型说明：**

- `feature_year`：按年份切片，检验年度稳定性
- `l1_name`：按一级行业切片，识别优势/劣势行业
- `macro_phase`：按宏观周期切片，检验宏观环境适配性

### A.8 feature_prefilter.csv

特征预筛选统计，记录每个特征的质量指标。

| 列名 | 含义 | 保留标准 |
|------|------|----------|
| `feature` | 特征名 | |
| `missing_ratio` | 缺失率 | `< 0.3` 通常保留 |
| `overall_std` | 全局标准差 | 过低可能无效 |
| `median_cross_section_std` | 截面标准差中位数 | 衡量截面区分度 |
| `keep` | 是否保留 | `True` / `False` |

### A.9 rolling_feature_importance.csv

特征重要性明细，记录每个特征在各期的 importance。

| 列名 | 含义 | 用途 |
|------|------|------|
| `feature` | 特征名 | |
| `importance_gain` | Gain 重要性 | 特征对模型预测的贡献增益 |
| `importance_split` | Split 重要性 | 特征被用于分裂的次数 |
| `feature_date` | 评估日期 | 观察重要性时序变化 |
| `bundle` | 评估模式 | `rolling` / `walk_forward` |

### A.10 latest_score_frame.csv

最新预测分数表，用于实际选股决策。

| 列名 | 含义 | 用途 |
|------|------|------|
| `datetime` | 预测日期 | 最新一期 |
| `instrument` | 股票代码 | |
| `score` | 模型预测分数 | **核心指标**：越高越好 |
| `open` / `close` | 当日价格 | |
| `volume` / `amount` | 成交量/成交额 | 流动性参考 |
| `in_csi300` | 是否在沪深 300 | True/False |
| `l1_name` | 一级行业 | 行业分布分析 |
| `l2_name` / `l3_name` | 二/三级行业 | |
| `macro_phase` | 宏观周期阶段 | 当前环境 |
| `macro_industry_match` | 宏观-行业匹配标记 | 1=匹配当前周期 |

---

## 附录 B：核心指标公式

### Rank IC

```python
rank_ic = spearmanr(score, future_return)
```

Spearman 秩相关系数，衡量预测分数与实际收益的单调关系。

### IC IR (Information Ratio)

```python
ic_ir = ic_mean / ic_std
```

单位波动下的预测能力。

### TopK Hit Rate

```python
hit_rate = (topk_return > benchmark_return).mean()
```

TopK 组合跑赢基准的比例。

### Score Dispersion

```python
score_dispersion = score.std()
```

衡量分数的截面离散程度。

### TopK Unique Score Ratio

```python
topk_unique_score_ratio = topk_scores.nunique() / topk_count
```

衡量 TopK 内分数的唯一性，过低表示选股分数重复。

### Max Drawdown

```python
max_drawdown = (net_value / net_value.cummax() - 1).min()
```

---

## 附录 C：配置文件字段说明

### native_workflow_manifest.json

| 字段 | 含义 | 示例值 |
|------|------|--------|
| `experiment_name` | 实验名称标识 | `qlib-native-csi300` |
| `universe_profile` | 股票池类型 | `csi300` / `csi500` |
| `native_backtest_mode` | 回测引擎类型 | `qlib_native` |
| `native_time_per_step` | 回测频率 | `week` |
| `native_benchmark` | 基准指数代码 | `000001.SH` |
| `feature_columns` | 实际使用的特征列表 | 63 个特征名 |
| `latest_feature_date` | 最新特征数据日期 | `2026-04-03` |

### baseline/native_workflow_manifest.json (recipe 配置)

| 字段 | 含义 |
|------|------|
| `name` | Recipe 名称 |
| `signal_objective` | 模型目标函数类型 |
| `label_recipe` | 预测标签定义 |
| `feature_groups` | 特征组配置 |
| `excluded_features` | 排除的特征 |
| `industry_normalization` | 行业标准化方法 |

### 关键配置参数

| 参数 | 含义 | 建议值 |
|------|------|--------|
| `topk` | TopK 选股数量 | 10 |
| `train_weeks` | 滚动训练窗口 | 260 (约 5 年) |
| `valid_weeks` | 滚动验证窗口 | 52 (约 1 年) |
| `rebalance_interval_weeks` | 调仓频率 | 1 (周频) |
| `hold_buffer_rank` | 持仓缓冲排名 | 15 |
| `industry_max_weight` | 行业最大权重 | 0.3 |

# qlib 原生工作流使用指南

## 1. 适用场景

这套工作流适合做三类事情：

- 用统一的 recipe 矩阵评估周频选股信号，而不是在 notebook 里手工拼训练和回测。
- 用 qlib native 执行层验证“排序有效”是否真的能转成组合净收益。
- 在同一套产物里同时看 signal、portfolio、validation comparison、slice stability 和 heatmap。

当前推荐入口有两个：

- 脚本入口：[evaluate_qlib_native_weekly.py](/Volumes/Repository/Projects/TradingNexus/ValueInvesting/scripts/evaluate_qlib_native_weekly.py)
- notebook 入口：[qlib_workflow_by_code_native.ipynb](/Volumes/Repository/Projects/TradingNexus/ValueInvesting/notebooks/qlib_workflow_by_code_native.ipynb)

推荐顺序是：

1. 先用脚本跑正式产物。
2. 再用 notebook 做展示、对比和解读。

---

## 2. 当前 CONFIG 对应的完整脚本命令

下面这条命令对齐当前 notebook 默认 `CONFIG`，适合直接跑 `csi300` 的 baseline 和候选 recipe：

```bash
uv run python scripts/evaluate_native_weekly.py \
  --panel artifacts/panels/csi300_weekly.parquet \
  --output-dir artifacts/native_workflow/csi300 \
  --universe-profile csi300 \
  --benchmark-mode auto \
  --signal-objective huber_regression \
  --label-recipe blended_excess_4w_8w \
  --topk 10 \
  --rebalance-interval-weeks 1 \
  --eval-count 52 \
  --train-weeks 260 \
  --valid-weeks 52 \
  --step-weeks 1 \
  --walk-forward-enabled \
  --walk-forward-eval-count 0 \
  --walk-forward-train-weeks 260 \
  --walk-forward-valid-weeks 52 \
  --walk-forward-step-weeks 1 \
  --native-risk-degree 0.95 \
  --account 1000000.0 \
  --run-export auto_if_missing \
  --no-publish-model \
  --seed 42 \
  --reproducibility-mode balanced \
  --start-date 2016-01-01 \
  --hold-buffer-rank 15 \
  --industry-max-weight 0.3 \
  --walk-forward-start-date 2016-01-01 \
  --validation-execution-lag-steps 1 \
  --no-validation-only-tradable \
  --validation-risk-degree 1.0 \
  --universe-exit-policy retain_quotes_for_existing_positions \
  --batch-size 200 \
  --recipe baseline \
  --recipe mae_4w \
  --recipe binary_4w \
  --recipe rank_blended \
  --recipe huber_8w \
  --exclude-feature 'npm*,gpm*' \
  --recipe-parallel-workers 0
```

## 命令生成
同一条命令现在也可以由 helper 自动生成：

- `run_native_notebook_workflow(...)[\"cli_command\"]`
- `build_native_workflow_cli_command(config_overrides=CONFIG, recipe_names=RECIPE_NAMES)`


## 参数include-feature和exclude-feature说明:
建议加引号避免 shell 展开：
```
uv run python scripts/evaluate_native_weekly.py --include-feature pb --include-feature 'macro*'
uv run python scripts/evaluate_native_weekly.py --exclude-feature macro_industry_match --exclude-feature 'macro*'
```
也支持逗号分隔：--include-feature 'pb,pe_ttm,macro*'
规则是：

不带通配符时按精确特征名匹配。
带 */?/[] 时按 glob 模式匹配，macro* 就是“以 macro 开头”。
执行顺序是“先 include 白名单，再 exclude 剔除”，并保持原特征列顺序。

补充一个实现约定：include-feature 目前是对 recipe 默认特征集合做白名单过滤，不会额外把 recipe 之外的新特征拉进来。

## 参数run-export
不需要先手动跑 export_weekly_panel。当前 evaluate_native_weekly 的 --run-export 默认是 auto_if_missing，见 scripts/evaluate_native_weekly.py:136，而 _ensure_panel 会在面板文件不存在时自动调用导出，见 src/qlib_research/core/qlib_native_workflow.py:362。只有这几种情况你才需要先单独跑：

你把 --run-export never 打开了。
你想强制刷新面板，建议用 --run-export always。
你想先单独产出一个自定义 panel，再通过 --panel 指向它。

## 参数walk_forward_eval_count
如果你希望 walk-forward 覆盖全部可评估历史，`walk_forward_eval_count` 应设为 `0`。

解释要点：

- `walk_forward_start_date` 只是“最早允许开始筛选的日期”
- 真正可开始 walk-forward 的最早日期还会受 `train_weeks + valid_weeks` 历史窗约束
- `walk_forward_eval_count > 0` 时，只会保留最近 `N` 个可评估周

如果只想先确认主链路通了，可以先只跑：

```bash
uv run python scripts/evaluate_native_weekly.py \
  --panel data/qlib_artifacts/panels/csi300_weekly.parquet \
  --output-dir data/qlib_artifacts/native_workflow/csi300 \
  --universe-profile csi300 \
  --benchmark-mode auto \
  --walk-forward-eval-count 26 \
  --recipe baseline
```

---

## 3. benchmark 配置方式

### 3.1 默认自动映射

`benchmark_mode=auto` 时，系统会按 `universe_profile` 自动映射：

- `csi300 -> 000300.SH`
- `csi500 -> 000905.SH`
- `merged_csi300_500 -> 000300.SH 50% + 000905.SH 50%`

这适合做“股票池内相对比较”。

### 3.2 手工指定 benchmark

如果你想手工改成上证指数，可以直接传：

```bash
--benchmark-mode 000001.SH
```

或者显式带标签：

```bash
--benchmark-mode 000001.SH@上证指数
```

适合场景：

- `universe_profile=csi300`，但你想看相对上证指数的表现。
- 研究里想固定一个更熟悉的基准，方便和历史分析口径对齐。

### 3.3 关闭 benchmark

如果只想看绝对净值，不做相对基准比较：

```bash
--benchmark-mode flat_zero
```

---

## 4. notebook 的推荐用法

在 [qlib_workflow_by_code_native.ipynb](/Volumes/Repository/Projects/TradingNexus/ValueInvesting/notebooks/qlib_workflow_by_code_native.ipynb) 里：

- `RUN_WORKFLOW = False`
  - 只加载已有产物，适合复盘。
- `RUN_WORKFLOW = True`
  - 直接触发统一 runner，适合临时从 notebook 刷一次结果。

建议：

1. 正式跑大样本时，优先用脚本。
2. notebook 保持 `RUN_WORKFLOW = False` 作为默认值。
3. 每次改完底层代码后，先重启 kernel，再从头运行 notebook，避免沿用旧的 qlib runtime 和 provider cache。

---

## 5. 主要输出目录

以 `baseline` 为例，核心产物目录通常是：

`data/qlib_artifacts/native_workflow/csi300/baseline/`

重点文件：

- `latest_score_frame.csv`
- `feature_prefilter.csv`
- `rolling_summary.csv`
- `walk_forward_summary.csv`
- `signal_diagnostics.csv`
- `portfolio_diagnostics.csv`
- `slice_regime_summary.csv`
- `execution_diff_summary.csv`
- `rolling_native_report.csv`
- `walk_forward_native_report.csv`
- `rolling_native_monthly_return_heatmap.csv`
- `rolling_native_annual_return_heatmap.csv`
- `walk_forward_native_monthly_return_heatmap.csv`
- `walk_forward_native_annual_return_heatmap.csv`

根目录还会有：

- `recipe_registry.json`
- `native_workflow_summary.json`

---

## 6. notebook 里重点看哪些节点

当前 native notebook 是“薄展示层”，建议按这个顺序读：

1. `## 5. 工作流总览`
   - 看集中指标表和完整 `recipe_overview`
2. `## 6. 研究价值看板`
   - 看特征保留、分数离散度、TopK 唯一分数数、持仓偏离
3. `## 7. 执行与风险诊断`
   - 看 `execution_diff_summary`、净值曲线、月度/年度热力图
4. `## 8. 切片稳定性与特征重要性`
   - 看年份 / 行业 / 宏观 phase 稳定性
5. `## 9. 最新一期信号快照`
   - 看当前要研究的前排股票和行业聚集情况

---

## 7. 常见使用建议

### 7.1 想先确认是不是“信号问题”还是“执行问题”

优先看：

- `rolling_summary.csv`
- `walk_forward_summary.csv`
- `execution_diff_summary.csv`

如果 `rank_ic_ir` 还可以，但 `topk_mean_excess_return_4w` 很低，且 `native_minus_validation_return` 波动很大，通常说明信号排序和执行兑现之间还有断层。

### 7.2 想先确认持仓数为什么漂移

优先看：

- `portfolio_diagnostics.csv`

特别关注：

- `target_hold_count`
- `actual_hold_count`
- `blocked_sell_count`
- `residual_hold_count`

### 7.3 想看当前分数是否“糊成一片”

优先看：

- `signal_diagnostics.csv`

特别关注：

- `score_dispersion`
- `score_unique_count`
- `topk_unique_score_ratio`

---

## 8. 与旧 native notebook 的差异

当前版本和旧版最大的变化有三点：

1. notebook 不再手工拼训练、信号、native backtest，而是直接调用脚本化 runner。
2. benchmark 默认按 `universe_profile` 自动映射，但允许手工覆盖到 `000001.SH` 这类指数。
3. 月度 / 年度净收益热力图已经变成标准产物，notebook 直接读取并用 0 为分水岭做多空色阶显示。

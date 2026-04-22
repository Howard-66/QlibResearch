# Native Workflow Artifact Map

Use this map when interpreting `artifacts/native_workflow/<run_id>`.

## Run-Level Files

- `native_workflow_summary.json`: authoritative run config, recipe registry, promotion gate summaries, and executed recipe names.
- `recipe_registry.json`: fallback for recipe definitions and execution order.
- `experiment_scorecard.json`: generated heuristic summary; useful context, not authoritative when raw artifacts disagree.
- `analysis/latest_summary.*`: prior LLM or local diagnosis; treat as stale unless the user specifically asks to review it.

## Recipe-Level Core Files

- `native_workflow_manifest.json`: recipe config, feature columns, benchmark components, provider paths.
- `rolling_summary.csv`, `walk_forward_summary.csv`: signal evaluation summary. Key fields: `rank_ic_ir`, `topk_mean_excess_return_4w`, `topk_hit_rate`, `coverage_mean`, `used_feature_count`.
- `rolling_performance_metrics.csv`, `walk_forward_performance_metrics.csv`: realized portfolio performance. Key fields: `net_total_return`, `annualized_return`, `sharpe_ratio`, `max_drawdown`, `win_rate`, `calmar_ratio`.
- `signal_diagnostics.csv`: signal granularity and TopK behavior. Key fields: `score_dispersion`, `topk_unique_score_ratio`, `topk_overlap_prev`, `excess_return_top_bottom_decile_spread`.
- `signal_realization_bridge.csv`: signal-to-portfolio bridge. Key fields: `topk_mean_excess_return_4w`, `new_position_return_4w`, `carry_position_return_4w`, `rebalance_drag`, `execution_cost_drag`, `realized_portfolio_return`.
- `holding_count_drift.csv`: target vs post-trade holdings. Key fields: `target_hold_count`, `actual_hold_count`, `locked_residual_count`, `sell_blocked_total_count`, `topk_overlap_prev`.
- `rebalance_audit.csv`: detailed holdings before/after rebalance and residual/sell-blocked codes.
- `sector_exposure_history.csv`: realized industry exposure. Key fields: `top1_sector_weight`, `top3_sector_concentration`, `finance_weight`, `top1_sector_name`.
- `slice_regime_summary.csv`: stability by year, industry, and macro phase. Key fields: `slice_type`, `slice_value`, `coverage`, `mean_excess_return_4w`.
- `latest_score_frame.csv`: latest prediction snapshot with industry/macro metadata. Use top 10/20 for live target feasibility.
- `portfolio_targets.csv`: published top-k target list. It may not contain industry columns; join mentally against `latest_score_frame` by code/instrument when needed.
- `*_feature_importance.csv`: model feature dependence. Aggregate by mean `importance_gain` and stability across dates.
- `execution_diff_summary.csv`: native vs validation comparison. Empty file means comparison was not run or produced no rows.

## Empty and Missing Semantics

- Missing file: artifact generation did not produce the expected file.
- Empty CSV with no columns: producer wrote an empty frame; do not treat it as a healthy zero-value result.
- CSV with headers and zero rows: schema exists but no observations; mark as evidence gap.
- JSON missing key: do not infer false; record unknown.
- Scorecard says an artifact is missing but the CSV exists with rows: record a scorecard/raw-artifact conflict and trust the CSV.

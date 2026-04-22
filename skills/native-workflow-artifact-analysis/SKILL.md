---
name: native-workflow-artifact-analysis
description: Analyze Qlib native workflow run artifacts and generate run-level system diagnosis reports. Use when Codex needs to interpret artifacts under artifacts/native_workflow, compare recipes, diagnose signal-to-portfolio realization, explain ranking/TopK/holding/exposure/stability issues, or produce latest_summary.md plus structured JSON for QlibResearch Diagnosis Tasks.
---

# Native Workflow Artifact Analysis

## Workflow

Analyze the raw artifact directory, not just the existing scorecard. Treat `experiment_scorecard.json` and prior `analysis/latest_summary.*` as background only; if they conflict with CSV/JSON artifacts, trust the raw artifacts and list the conflict under Evidence Gaps.

1. Build or read the evidence pack.
   - Prefer running `scripts/build_native_workflow_evidence_pack.py --run-dir <run-dir>` from the QlibResearch repo.
   - If the script is unavailable, manually inspect the files described in `references/artifact-map.md`.
2. Read the run-level configuration and recipe registry first.
3. Compare every recipe before choosing a lead.
4. Diagnose the system through the six layers in `references/analysis-playbook.md`.
5. Emit the report and JSON contract in `references/output-contract.md`.

## Required Judgment

For each recipe, assign one role:

- `lead`: best current mainline, but may still need explanation before live use.
- `filter`: useful as a consensus or risk filter, not a standalone mainline.
- `candidate`: worth another controlled experiment.
- `reject`: evidence argues against further work in this branch.
- `diagnose_only`: interesting result, but current artifacts are too contradictory or fragile.

The run verdict must be one of `promote`, `hold`, `investigate`, or `reject`.

## Reading Order

Use this order because it prevents generic summaries:

1. `native_workflow_summary.json`, `recipe_registry.json`, run/recipe `experiment_scorecard.json`
2. `rolling_summary.csv`, `walk_forward_summary.csv`, `*_performance_metrics.csv`
3. `signal_diagnostics.csv`, `signal_realization_bridge.csv`
4. `holding_count_drift.csv`, `rebalance_audit.csv`, `portfolio_diagnostics.csv`
5. `sector_exposure_history.csv`, `slice_regime_summary.csv`, `regime_gate_diagnostics.csv`
6. `latest_score_frame.csv`, `portfolio_targets.csv`
7. `*_feature_importance.csv`, heatmaps, native reports

## Reporting Rules

- Write in Chinese unless the user asks otherwise.
- Lead with conclusions, then evidence.
- Compare recipes using the same bundle, usually walk-forward first.
- Do not call an empty `execution_diff_summary.csv` healthy; say validation comparison is absent or empty.
- Always discuss live feasibility separately from research performance.
- In the main prose, explain what the metric means for the research decision. Do not merely list metric names, file names, or raw values.
- Round all prose numbers: returns/drawdowns/excess returns as percentages, ratios to 2 decimals, holding counts as integers. Full precision belongs in JSON evidence refs, not paragraphs.
- Keep artifact/file references mostly in Evidence Gaps and `evidence_refs`; user-facing sections should use Chinese metric names such as "TopK 超额", "最大回撤", "实际持仓".
- Do not frame the report as a new-vs-old summary comparison. If scorecard fields conflict with raw artifacts, state the conflict briefly under Evidence Gaps and use raw artifacts as the source of truth.
- Avoid mixed-language filler such as "promote 到 live"; use natural Chinese such as "进入实盘" while preserving necessary domain terms like TopK, walk-forward, Rank IC IR.

## References

- `references/artifact-map.md`: artifact meanings, fields, and empty/missing semantics.
- `references/analysis-playbook.md`: six-layer diagnosis and role/verdict heuristics.
- `references/output-contract.md`: Markdown sections and JSON shape.

## Script Helper

If the repository script exists, the bundled wrapper can invoke it:

```bash
python /Users/howard/.codex/skills/native-workflow-artifact-analysis/scripts/build-evidence-pack.py --run-dir artifacts/native_workflow/<run_id>
```

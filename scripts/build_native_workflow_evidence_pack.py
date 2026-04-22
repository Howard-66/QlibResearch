from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from pandas.errors import EmptyDataError


SCHEMA_VERSION = 1
BUNDLES = ("rolling", "walk_forward")
RECIPE_FILES = (
    "native_workflow_manifest.json",
    "experiment_scorecard.json",
    "latest_score_frame.csv",
    "portfolio_targets.csv",
    "feature_prefilter.csv",
    "feature_corr_candidates.csv",
    "feature_redundancy.csv",
    "feature_outlier_audit.csv",
    "signal_diagnostics.csv",
    "portfolio_diagnostics.csv",
    "execution_diff_summary.csv",
    "slice_regime_summary.csv",
    "signal_realization_bridge.csv",
    "holding_count_drift.csv",
    "rebalance_audit.csv",
    "sector_exposure_history.csv",
    "regime_gate_diagnostics.csv",
    "rolling_summary.csv",
    "rolling_details.csv",
    "rolling_feature_importance.csv",
    "rolling_native_report.csv",
    "rolling_native_benchmark.csv",
    "rolling_native_monthly_return_heatmap.csv",
    "rolling_native_annual_return_heatmap.csv",
    "rolling_performance_metrics.csv",
    "walk_forward_summary.csv",
    "walk_forward_details.csv",
    "walk_forward_feature_importance.csv",
    "walk_forward_native_report.csv",
    "walk_forward_native_benchmark.csv",
    "walk_forward_native_monthly_return_heatmap.csv",
    "walk_forward_native_annual_return_heatmap.csv",
    "walk_forward_performance_metrics.csv",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a compact evidence pack for Qlib native workflow run artifacts.")
    parser.add_argument("--run-dir", required=True, help="Path to artifacts/native_workflow/<run_id>.")
    parser.add_argument("--output-dir", help="Output directory. Defaults to <run-dir>/analysis.")
    parser.add_argument("--no-write", action="store_true", help="Print JSON to stdout without writing files.")
    return parser.parse_args()


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if math.isfinite(numeric) else None


def _safe_int(value: Any) -> int | None:
    numeric = _safe_float(value)
    return int(numeric) if numeric is not None else None


def _jsonable(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, bool, int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            return None
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return _jsonable(value.item())
        except Exception:
            return str(value)
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    return str(value)


def _safe_json(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_csv(path: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    status = {
        "file": path.name,
        "exists": path.exists(),
        "status": "missing",
        "rows": 0,
        "columns": [],
        "error": None,
    }
    if not path.exists() or not path.is_file():
        return pd.DataFrame(), status
    try:
        frame = pd.read_csv(path)
    except EmptyDataError:
        status.update({"status": "empty", "error": "no columns to parse"})
        return pd.DataFrame(), status
    except Exception as exc:  # pragma: no cover - defensive path for corrupted artifacts.
        status.update({"status": "error", "error": str(exc)})
        return pd.DataFrame(), status
    status.update(
        {
            "status": "empty" if frame.empty else "ok",
            "rows": int(len(frame)),
            "columns": [str(column) for column in frame.columns],
        }
    )
    return frame, status


def _first_row(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {}
    return {str(key): _jsonable(value) for key, value in frame.iloc[0].to_dict().items()}


def _bundle_frame(frame: pd.DataFrame, bundle: str) -> pd.DataFrame:
    if frame.empty or "bundle" not in frame.columns:
        return frame
    return frame.loc[frame["bundle"].astype(str) == bundle].copy()


def _mean(frame: pd.DataFrame, column: str) -> float | None:
    if frame.empty or column not in frame.columns:
        return None
    series = pd.to_numeric(frame[column], errors="coerce").dropna()
    return float(series.mean()) if not series.empty else None


def _max(frame: pd.DataFrame, column: str) -> float | None:
    if frame.empty or column not in frame.columns:
        return None
    series = pd.to_numeric(frame[column], errors="coerce").dropna()
    return float(series.max()) if not series.empty else None


def _min(frame: pd.DataFrame, column: str) -> float | None:
    if frame.empty or column not in frame.columns:
        return None
    series = pd.to_numeric(frame[column], errors="coerce").dropna()
    return float(series.min()) if not series.empty else None


def _metric_ref(
    *,
    recipe: str | None,
    bundle: str | None,
    file: str,
    metric: str,
    value: Any,
    comparator: str | None = None,
) -> dict[str, Any]:
    return {
        "recipe": recipe,
        "bundle": bundle,
        "file": file,
        "metric": metric,
        "value": _jsonable(value),
        "comparator": comparator,
    }


def _resolve_recipe_names(run_dir: Path, summary: dict[str, Any]) -> list[str]:
    registry = summary.get("recipe_registry", {}) if isinstance(summary.get("recipe_registry"), dict) else {}
    executed = registry.get("executed_recipes")
    if isinstance(executed, list) and executed:
        return [str(name) for name in executed]
    registry_path = _safe_json(run_dir / "recipe_registry.json")
    executed = registry_path.get("executed_recipes")
    if isinstance(executed, list) and executed:
        return [str(name) for name in executed]
    return sorted(path.name for path in run_dir.iterdir() if (path / "native_workflow_manifest.json").exists())


def _recipe_config(recipe_name: str, run_summary: dict[str, Any]) -> dict[str, Any]:
    registry = run_summary.get("recipe_registry", {}) if isinstance(run_summary.get("recipe_registry"), dict) else {}
    baseline = registry.get("baseline_recipe")
    if recipe_name == "baseline" and isinstance(baseline, dict):
        return baseline
    candidates = registry.get("candidate_recipes")
    if isinstance(candidates, dict) and isinstance(candidates.get(recipe_name), dict):
        return candidates[recipe_name]
    return {}


def _top_features(frame: pd.DataFrame, limit: int = 8) -> list[dict[str, Any]]:
    if frame.empty or "feature" not in frame.columns:
        return []
    gain = "importance_gain" if "importance_gain" in frame.columns else None
    if gain is None:
        return []
    grouped = (
        frame.assign(_gain=pd.to_numeric(frame[gain], errors="coerce"))
        .dropna(subset=["_gain"])
        .groupby("feature", dropna=True)["_gain"]
        .agg(["mean", "count"])
        .sort_values(["mean", "count"], ascending=[False, False])
        .head(limit)
    )
    return [
        {"feature": str(index), "mean_gain": float(row["mean"]), "observations": int(row["count"])}
        for index, row in grouped.iterrows()
    ]


def _slice_summary(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty or "mean_excess_return_4w" not in frame.columns:
        return {"rows": int(len(frame)), "positive_ratio": None, "worst_slices": [], "best_slices": []}
    work = frame.copy()
    work["_excess"] = pd.to_numeric(work["mean_excess_return_4w"], errors="coerce")
    valid = work.dropna(subset=["_excess"])
    positive_ratio = float((valid["_excess"] > 0).mean()) if not valid.empty else None
    label_columns = [column for column in ("slice_type", "slice_value", "bundle") if column in valid.columns]

    def pack(row: pd.Series) -> dict[str, Any]:
        payload = {column: _jsonable(row.get(column)) for column in label_columns}
        payload["mean_excess_return_4w"] = _safe_float(row.get("_excess"))
        payload["coverage"] = _safe_float(row.get("coverage"))
        return payload

    worst = [pack(row) for _, row in valid.sort_values("_excess").head(5).iterrows()]
    best = [pack(row) for _, row in valid.sort_values("_excess", ascending=False).head(5).iterrows()]
    return {"rows": int(len(frame)), "positive_ratio": positive_ratio, "worst_slices": worst, "best_slices": best}


def _latest_snapshot(frame: pd.DataFrame, targets: pd.DataFrame, topk: int) -> dict[str, Any]:
    if frame.empty or "score" not in frame.columns:
        return {
            "latest_rows": int(len(frame)),
            "target_rows": int(len(targets)),
            "top_industries": {},
            "macro_match_ratio_top10": None,
            "score_gap_10_20": None,
            "score_gap_1_10": None,
            "score_range_top10": None,
        }
    ranked = frame.sort_values("score", ascending=False).reset_index(drop=True)
    top10 = ranked.head(min(topk, 10))
    industries: dict[str, int] = {}
    if "l1_name" in top10.columns:
        industries = dict(Counter(str(value) for value in top10["l1_name"].dropna().tolist()).most_common(5))
    macro_match = _mean(top10, "macro_industry_match")
    score_gap_10_20 = None
    if len(ranked) >= 20:
        tenth = _safe_float(ranked.iloc[9].get("score"))
        twentieth = _safe_float(ranked.iloc[19].get("score"))
        score_gap_10_20 = tenth - twentieth if tenth is not None and twentieth is not None else None
    score_gap_1_10 = None
    if len(ranked) >= 10:
        first = _safe_float(ranked.iloc[0].get("score"))
        tenth = _safe_float(ranked.iloc[9].get("score"))
        score_gap_1_10 = first - tenth if first is not None and tenth is not None else None
    top_scores = pd.to_numeric(top10["score"], errors="coerce").dropna()
    score_range = float(top_scores.max() - top_scores.min()) if not top_scores.empty else None
    return {
        "latest_rows": int(len(frame)),
        "target_rows": int(len(targets)),
        "top_industries": industries,
        "macro_match_ratio_top10": macro_match,
        "score_gap_10_20": score_gap_10_20,
        "score_gap_1_10": score_gap_1_10,
        "score_range_top10": score_range,
    }


def _artifact_issues(statuses: dict[str, dict[str, Any]], recipe: str) -> list[dict[str, Any]]:
    issues = []
    for file, status in statuses.items():
        if status["status"] in {"missing", "empty", "error"}:
            issues.append(
                {
                    "recipe": recipe,
                    "file": file,
                    "severity": "warning" if status["status"] == "empty" else "danger",
                    "issue": status["status"],
                    "detail": status.get("error"),
                }
            )
    return issues


def _scorecard_conflicts(scorecard: dict[str, Any], metrics: dict[str, Any], statuses: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    conflicts: list[dict[str, Any]] = []
    score_metrics = scorecard.get("metrics", {}) if isinstance(scorecard.get("metrics"), dict) else {}
    bridge_rows = metrics.get("bridge_rows")
    if score_metrics.get("bridge_complete") is False and bridge_rows:
        conflicts.append(
            {
                "field": "metrics.bridge_complete",
                "scorecard_value": False,
                "raw_value": f"{bridge_rows} bridge rows",
                "message": "scorecard says bridge is incomplete, but signal_realization_bridge.csv has rows",
            }
        )
    for key in (
        "rolling_rank_ic_ir",
        "walk_forward_rank_ic_ir",
        "rolling_topk_mean_excess_return_4w",
        "walk_forward_topk_mean_excess_return_4w",
    ):
        if score_metrics.get(key) is None and metrics.get(key) is not None:
            conflicts.append(
                {
                    "field": f"metrics.{key}",
                    "scorecard_value": None,
                    "raw_value": metrics.get(key),
                    "message": "scorecard omitted a metric that is present in raw summaries",
                }
            )
    if score_metrics.get("top1_sector_weight_mean") is None and metrics.get("top1_sector_weight_mean") is not None:
        conflicts.append(
            {
                "field": "metrics.top1_sector_weight_mean",
                "scorecard_value": None,
                "raw_value": metrics.get("top1_sector_weight_mean"),
                "message": "scorecard omitted sector exposure although sector_exposure_history.csv has rows",
            }
        )
    headline = " ".join(str(scorecard.get(key) or "") for key in ("headline", "current_problem"))
    if "缺少 signal realization bridge" in headline and bridge_rows:
        conflicts.append(
            {
                "field": "headline/current_problem",
                "scorecard_value": headline,
                "raw_value": f"{bridge_rows} bridge rows",
                "message": "scorecard text claims missing signal realization bridge, but raw bridge exists",
            }
        )
    if statuses.get("execution_diff_summary.csv", {}).get("status") == "empty":
        conflicts.append(
            {
                "field": "execution_diff_summary.csv",
                "scorecard_value": None,
                "raw_value": "empty CSV",
                "message": "validation comparison is absent or empty; execution gap must not be treated as healthy",
            }
        )
    return conflicts


def _gate_warnings(recipe_name: str, metrics: dict[str, Any], promotion_gate: dict[str, Any] | None, topk: int) -> list[str]:
    warnings: list[str] = []
    passed = bool((promotion_gate or {}).get("promotion_gate_passed"))
    if not passed:
        return warnings
    unique = _safe_float(metrics.get("signal_unique_mean"))
    topk_excess = _safe_float(metrics.get("walk_forward_topk_mean_excess_return_4w"))
    drawdown = _safe_float(metrics.get("walk_forward_max_drawdown"))
    actual_hold_max = _safe_float(metrics.get("actual_hold_max"))
    if unique is not None and unique < 0.85:
        warnings.append(f"{recipe_name} passed gate but signal_unique_mean is below 0.85")
    if topk_excess is not None and topk_excess < 0:
        warnings.append(f"{recipe_name} passed gate but walk-forward TopK excess is negative")
    if drawdown is not None and drawdown <= -0.30:
        warnings.append(f"{recipe_name} passed gate but walk-forward max drawdown is deeper than 30%")
    if actual_hold_max is not None and actual_hold_max > topk + 4:
        warnings.append(f"{recipe_name} passed gate but actual holding max is materially above topk")
    return warnings


def _recipe_score(metrics: dict[str, Any], latest: dict[str, Any], topk: int, execution_status: str) -> float:
    wf_net = _safe_float(metrics.get("walk_forward_net_total_return")) or 0.0
    wf_sharpe = _safe_float(metrics.get("walk_forward_sharpe_ratio")) or 0.0
    wf_icir = _safe_float(metrics.get("walk_forward_rank_ic_ir")) or 0.0
    wf_topk = _safe_float(metrics.get("walk_forward_topk_mean_excess_return_4w")) or 0.0
    wf_drawdown = abs(_safe_float(metrics.get("walk_forward_max_drawdown")) or 0.0)
    unique = _safe_float(metrics.get("signal_unique_mean")) or 0.0
    actual_hold_mean = _safe_float(metrics.get("actual_hold_mean")) or float(topk)
    top1_sector = _safe_float(metrics.get("top1_sector_weight_mean")) or 0.0
    score_gap = _safe_float(latest.get("score_gap_10_20"))
    score = (
        wf_net
        + wf_sharpe * 0.03
        + max(wf_icir, -0.2) * 0.08
        + wf_topk * 4.0
        - wf_drawdown * 0.30
        + min(unique, 1.0) * 0.04
    )
    score -= max(actual_hold_mean - (topk + 1.0), 0.0) * 0.025
    score -= max(top1_sector - 0.35, 0.0) * 0.40
    if score_gap is not None and score_gap < 0.01:
        score -= 0.04
    if execution_status == "empty":
        score -= 0.01
    return float(score)


def _initial_role(metrics: dict[str, Any], latest: dict[str, Any]) -> str:
    wf_net = _safe_float(metrics.get("walk_forward_net_total_return"))
    wf_icir = _safe_float(metrics.get("walk_forward_rank_ic_ir"))
    wf_topk = _safe_float(metrics.get("walk_forward_topk_mean_excess_return_4w"))
    wf_drawdown = _safe_float(metrics.get("walk_forward_max_drawdown"))
    unique = _safe_float(metrics.get("signal_unique_mean"))
    score_gap = _safe_float(latest.get("score_gap_10_20"))
    severe_unique = unique is not None and unique < 0.60
    severe_drawdown = wf_drawdown is not None and wf_drawdown <= -0.35
    weak_return = wf_net is None or wf_net <= 0
    weak_signal = (wf_icir is not None and wf_icir < 0) and (wf_topk is not None and wf_topk < 0)
    if severe_unique and wf_net is not None and wf_net > 0:
        return "diagnose_only"
    if weak_return and weak_signal:
        return "reject"
    if wf_net is not None and wf_net > 0 and not severe_drawdown and (unique is None or unique >= 0.75):
        if wf_topk is not None and wf_topk < 0:
            return "candidate"
        return "candidate"
    if wf_net is not None and wf_net > 0:
        return "diagnose_only"
    if wf_icir is not None and wf_icir > 0.2 and not severe_drawdown:
        return "filter"
    if score_gap is not None and score_gap <= 0:
        return "reject"
    return "diagnose_only"


def _build_metrics(frames: dict[str, pd.DataFrame]) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    for bundle in BUNDLES:
        summary = _first_row(_bundle_frame(frames[f"{bundle}_summary"], bundle))
        performance = _first_row(_bundle_frame(frames[f"{bundle}_performance_metrics"], bundle))
        metrics[f"{bundle}_rank_ic_ir"] = _safe_float(summary.get("rank_ic_ir"))
        metrics[f"{bundle}_topk_mean_excess_return_4w"] = _safe_float(summary.get("topk_mean_excess_return_4w"))
        metrics[f"{bundle}_topk_hit_rate"] = _safe_float(summary.get("topk_hit_rate"))
        metrics[f"{bundle}_coverage_mean"] = _safe_float(summary.get("coverage_mean"))
        metrics[f"{bundle}_net_total_return"] = _safe_float(performance.get("net_total_return"))
        metrics[f"{bundle}_annualized_return"] = _safe_float(performance.get("annualized_return"))
        metrics[f"{bundle}_sharpe_ratio"] = _safe_float(performance.get("sharpe_ratio"))
        metrics[f"{bundle}_max_drawdown"] = _safe_float(performance.get("max_drawdown"))
        metrics[f"{bundle}_win_rate"] = _safe_float(performance.get("win_rate"))
    signal_walk = _bundle_frame(frames["signal_diagnostics"], "walk_forward")
    hold_walk = _bundle_frame(frames["holding_count_drift"], "walk_forward")
    sector_walk = _bundle_frame(frames["sector_exposure_history"], "walk_forward")
    bridge_walk = _bundle_frame(frames["signal_realization_bridge"], "walk_forward")
    metrics.update(
        {
            "signal_unique_mean": _mean(signal_walk, "topk_unique_score_ratio"),
            "signal_unique_min": _min(signal_walk, "topk_unique_score_ratio"),
            "score_dispersion_mean": _mean(signal_walk, "score_dispersion"),
            "topk_overlap_mean": _mean(signal_walk, "topk_overlap_prev"),
            "decile_spread_mean": _mean(signal_walk, "excess_return_top_bottom_decile_spread"),
            "actual_hold_mean": _mean(hold_walk, "actual_hold_count"),
            "actual_hold_max": _max(hold_walk, "actual_hold_count"),
            "locked_residual_mean": _mean(hold_walk, "locked_residual_count"),
            "locked_residual_max": _max(hold_walk, "locked_residual_count"),
            "sell_blocked_total_max": _max(hold_walk, "sell_blocked_total_count"),
            "top1_sector_weight_mean": _mean(sector_walk, "top1_sector_weight"),
            "top1_sector_weight_max": _max(sector_walk, "top1_sector_weight"),
            "top3_sector_concentration_mean": _mean(sector_walk, "top3_sector_concentration"),
            "finance_weight_mean": _mean(sector_walk, "finance_weight"),
            "bridge_rows": int(len(bridge_walk)),
            "realized_portfolio_return_mean": _mean(bridge_walk, "realized_portfolio_return"),
            "bridge_topk_excess_mean": _mean(bridge_walk, "topk_mean_excess_return_4w"),
            "new_position_return_mean": _mean(bridge_walk, "new_position_return_4w"),
            "carry_position_return_mean": _mean(bridge_walk, "carry_position_return_4w"),
            "rebalance_drag_mean": _mean(bridge_walk, "rebalance_drag"),
            "execution_cost_drag_mean": _mean(bridge_walk, "execution_cost_drag"),
        }
    )
    if not sector_walk.empty and "top1_sector_name" in sector_walk.columns:
        metrics["latest_top1_sector_name"] = _jsonable(sector_walk.iloc[-1].get("top1_sector_name"))
    return metrics


def _build_recipe_dossier(
    *,
    run_dir: Path,
    recipe_name: str,
    run_summary: dict[str, Any],
    promotion_gate: dict[str, Any] | None,
    topk: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    recipe_dir = run_dir / recipe_name
    statuses: dict[str, dict[str, Any]] = {}
    frames: dict[str, pd.DataFrame] = {}
    for file in RECIPE_FILES:
        path = recipe_dir / file
        if file.endswith(".csv"):
            frame, status = _read_csv(path)
            frames[file.removesuffix(".csv")] = frame
            statuses[file] = status
        else:
            statuses[file] = {
                "file": file,
                "exists": path.exists(),
                "status": "ok" if path.exists() else "missing",
                "rows": None,
                "columns": [],
                "error": None,
            }
    scorecard = _safe_json(recipe_dir / "experiment_scorecard.json")
    manifest = _safe_json(recipe_dir / "native_workflow_manifest.json")
    metrics = _build_metrics(frames)
    latest = _latest_snapshot(frames["latest_score_frame"], frames["portfolio_targets"], topk=topk)
    slice_info = _slice_summary(frames["slice_regime_summary"])
    top_features = _top_features(frames["walk_forward_feature_importance"])
    execution_status = statuses.get("execution_diff_summary.csv", {}).get("status", "missing")
    score = _recipe_score(metrics, latest, topk=topk, execution_status=str(execution_status))
    role = _initial_role(metrics, latest)
    evidence_refs = [
        _metric_ref(recipe=recipe_name, bundle="walk_forward", file="walk_forward_summary.csv", metric="rank_ic_ir", value=metrics.get("walk_forward_rank_ic_ir")),
        _metric_ref(recipe=recipe_name, bundle="walk_forward", file="walk_forward_summary.csv", metric="topk_mean_excess_return_4w", value=metrics.get("walk_forward_topk_mean_excess_return_4w")),
        _metric_ref(recipe=recipe_name, bundle="walk_forward", file="walk_forward_performance_metrics.csv", metric="net_total_return", value=metrics.get("walk_forward_net_total_return")),
        _metric_ref(recipe=recipe_name, bundle="walk_forward", file="walk_forward_performance_metrics.csv", metric="max_drawdown", value=metrics.get("walk_forward_max_drawdown")),
        _metric_ref(recipe=recipe_name, bundle="walk_forward", file="signal_diagnostics.csv", metric="topk_unique_score_ratio_mean", value=metrics.get("signal_unique_mean")),
        _metric_ref(recipe=recipe_name, bundle="walk_forward", file="holding_count_drift.csv", metric="actual_hold_max", value=metrics.get("actual_hold_max")),
        _metric_ref(recipe=recipe_name, bundle="walk_forward", file="sector_exposure_history.csv", metric="top1_sector_weight_mean", value=metrics.get("top1_sector_weight_mean")),
        _metric_ref(recipe=recipe_name, bundle="latest", file="latest_score_frame.csv", metric="score_gap_10_20", value=latest.get("score_gap_10_20")),
    ]
    artifact_issues = _artifact_issues(statuses, recipe_name)
    conflicts = _scorecard_conflicts(scorecard, metrics, statuses)
    warnings = _gate_warnings(recipe_name, metrics, promotion_gate, topk=topk)
    dossier = {
        "recipe": recipe_name,
        "recipe_config": _recipe_config(recipe_name, run_summary) or manifest.get("recipe", {}),
        "artifact_status": statuses,
        "artifact_issues": artifact_issues,
        "metrics": metrics,
        "latest_snapshot": latest,
        "slice_summary": slice_info,
        "top_features": top_features,
        "scorecard_conflicts": conflicts,
        "gate_warnings": warnings,
        "scorecard": scorecard,
        "promotion_gate": promotion_gate or {},
        "evidence_refs": evidence_refs,
        "role": role,
        "score": score,
    }
    matrix = {
        "recipe": recipe_name,
        "role": role,
        "score": score,
        "gate_passed": (promotion_gate or {}).get("promotion_gate_passed"),
        "scorecard_verdict": scorecard.get("verdict"),
        "rolling_rank_ic_ir": metrics.get("rolling_rank_ic_ir"),
        "walk_forward_rank_ic_ir": metrics.get("walk_forward_rank_ic_ir"),
        "rolling_topk_mean_excess_return_4w": metrics.get("rolling_topk_mean_excess_return_4w"),
        "walk_forward_topk_mean_excess_return_4w": metrics.get("walk_forward_topk_mean_excess_return_4w"),
        "rolling_net_total_return": metrics.get("rolling_net_total_return"),
        "walk_forward_net_total_return": metrics.get("walk_forward_net_total_return"),
        "walk_forward_max_drawdown": metrics.get("walk_forward_max_drawdown"),
        "walk_forward_sharpe_ratio": metrics.get("walk_forward_sharpe_ratio"),
        "signal_unique_mean": metrics.get("signal_unique_mean"),
        "actual_hold_mean": metrics.get("actual_hold_mean"),
        "actual_hold_max": metrics.get("actual_hold_max"),
        "top1_sector_weight_mean": metrics.get("top1_sector_weight_mean"),
        "latest_top_industries": json.dumps(latest.get("top_industries", {}), ensure_ascii=False),
        "score_gap_10_20": latest.get("score_gap_10_20"),
        "bridge_rows": metrics.get("bridge_rows"),
        "artifact_issue_count": len(artifact_issues),
        "scorecard_conflict_count": len(conflicts),
        "gate_warning_count": len(warnings),
    }
    return dossier, matrix


def _select_lead_and_finalize_roles(matrix: list[dict[str, Any]], dossiers: dict[str, dict[str, Any]]) -> str | None:
    ranked = sorted(matrix, key=lambda row: _safe_float(row.get("score")) or -999.0, reverse=True)
    lead = None
    for row in ranked:
        role = str(row.get("role") or "")
        wf_net = _safe_float(row.get("walk_forward_net_total_return"))
        unique = _safe_float(row.get("signal_unique_mean"))
        if role in {"candidate", "filter"} and wf_net is not None and wf_net > 0 and (unique is None or unique >= 0.75):
            lead = str(row["recipe"])
            break
    if lead is None:
        for row in ranked:
            if row.get("role") not in {"reject"}:
                lead = str(row["recipe"])
                break
    if lead:
        for row in matrix:
            if row["recipe"] == lead:
                row["role"] = "lead"
                dossiers[lead]["role"] = "lead"
                break
        lead_row = next((row for row in ranked if row.get("recipe") == lead), {})
        lead_drawdown = abs(_safe_float(lead_row.get("walk_forward_max_drawdown")) or 0.0)
        filter_candidates = [
            row
            for row in ranked
            if row.get("recipe") != lead
            and row.get("role") == "candidate"
            and (_safe_float(row.get("walk_forward_net_total_return")) or 0.0) > 0
            and (_safe_float(row.get("walk_forward_rank_ic_ir")) or 0.0) > (_safe_float(lead_row.get("walk_forward_rank_ic_ir")) or -999.0)
            and (_safe_float(row.get("signal_unique_mean")) or 0.0) >= 0.75
            and abs(_safe_float(row.get("walk_forward_max_drawdown")) or 0.0) < max(lead_drawdown * 0.75, 0.01)
        ]
        if filter_candidates:
            filter_row = filter_candidates[0]
            filter_row["role"] = "filter"
            dossiers[str(filter_row["recipe"])]["role"] = "filter"
    return lead


def _run_findings(matrix: list[dict[str, Any]], dossiers: dict[str, dict[str, Any]], lead_recipe: str | None) -> tuple[list[str], list[dict[str, Any]], str]:
    findings: list[str] = []
    gaps: list[dict[str, Any]] = []
    lead = dossiers.get(lead_recipe or "", {})
    if lead_recipe and lead:
        metrics = lead.get("metrics", {})
        findings.append(
            (
                f"当前首选 recipe 是 {lead_recipe}，walk-forward net_total_return="
                f"{_safe_float(metrics.get('walk_forward_net_total_return'))}, max_drawdown="
                f"{_safe_float(metrics.get('walk_forward_max_drawdown'))}。"
            )
        )
        topk = _safe_float(metrics.get("walk_forward_topk_mean_excess_return_4w"))
        if topk is not None and topk < 0:
            findings.append(f"{lead_recipe} 的 walk-forward TopK 超额为负，收益兑现仍需要解释。")
    for recipe, dossier in dossiers.items():
        for conflict in dossier.get("scorecard_conflicts", []):
            gaps.append({"recipe": recipe, "type": "scorecard_conflict", **conflict})
        for warning in dossier.get("gate_warnings", []):
            gaps.append({"recipe": recipe, "type": "gate_warning", "message": warning})
        execution_status = dossier.get("artifact_status", {}).get("execution_diff_summary.csv", {}).get("status")
        if execution_status == "empty":
            gaps.append(
                {
                    "recipe": recipe,
                    "type": "empty_execution_diff",
                    "message": "execution_diff_summary.csv is empty; validation comparison is unavailable",
                }
            )
    severe_drawdown = [
        row for row in matrix if (_safe_float(row.get("walk_forward_max_drawdown")) or 0.0) <= -0.30
    ]
    if severe_drawdown:
        findings.append("部分 recipe walk-forward 回撤超过 30%，需要先做回撤区间归因。")
    lead_row = next((row for row in matrix if row.get("recipe") == lead_recipe), {})
    if not lead_recipe:
        verdict = "reject"
    elif gaps or (_safe_float(lead_row.get("walk_forward_topk_mean_excess_return_4w")) or 0.0) < 0:
        verdict = "investigate"
    elif (_safe_float(lead_row.get("walk_forward_net_total_return")) or 0.0) > 0:
        verdict = "promote"
    else:
        verdict = "hold"
    return findings, gaps, verdict


def _next_experiments(lead_recipe: str | None, dossiers: dict[str, dict[str, Any]], gaps: list[dict[str, Any]]) -> list[dict[str, str]]:
    experiments: list[dict[str, str]] = []
    lead = dossiers.get(lead_recipe or "", {})
    metrics = lead.get("metrics", {}) if lead else {}
    if gaps:
        experiments.append(
            {
                "name": "rebuild_scorecard_from_raw_artifacts",
                "priority": "P0",
                "rationale": "先修正 scorecard 与 raw artifact 的冲突，避免诊断继续复述陈旧结论。",
            }
        )
    if (_safe_float(metrics.get("walk_forward_topk_mean_excess_return_4w")) or 0.0) < 0:
        experiments.append(
            {
                "name": "min_score_spread_or_consensus_filter",
                "priority": "P0",
                "rationale": "TopK 超额为负，需要验证强弱分层或共识过滤能否改善兑现。",
            }
        )
    if (_safe_float(metrics.get("walk_forward_max_drawdown")) or 0.0) <= -0.25:
        experiments.append(
            {
                "name": "walk_forward_drawdown_attribution",
                "priority": "P1",
                "rationale": "回撤偏深，需要按时间、行业和持仓贡献拆解风险来源。",
            }
        )
    if (_safe_float(metrics.get("actual_hold_max")) or 0.0) > 14:
        experiments.append(
            {
                "name": "align_target_and_actual_holding_budget",
                "priority": "P1",
                "rationale": "实际持仓上限高于名义 topk，需确认组合定义和退出规则。",
            }
        )
    if not experiments:
        experiments.append(
            {
                "name": "controlled_lead_vs_filter_rerun",
                "priority": "P1",
                "rationale": "用当前 lead 与候选 filter 做同口径下一轮实验。",
            }
        )
    return experiments


def build_evidence_pack(run_dir: str | Path) -> dict[str, Any]:
    resolved_run_dir = Path(run_dir).expanduser().resolve()
    run_summary = _safe_json(resolved_run_dir / "native_workflow_summary.json")
    recipe_names = _resolve_recipe_names(resolved_run_dir, run_summary)
    config = run_summary.get("config", {}) if isinstance(run_summary.get("config"), dict) else {}
    topk = _safe_int(config.get("topk")) or 10
    promotion_gate_payload = run_summary.get("promotion_gate") or run_summary.get("promotion_gate_summary") or {}
    promotion_gate_payload = promotion_gate_payload if isinstance(promotion_gate_payload, dict) else {}

    dossiers: dict[str, dict[str, Any]] = {}
    matrix: list[dict[str, Any]] = []
    for recipe_name in recipe_names:
        promotion_gate = promotion_gate_payload.get(recipe_name)
        dossier, row = _build_recipe_dossier(
            run_dir=resolved_run_dir,
            recipe_name=recipe_name,
            run_summary=run_summary,
            promotion_gate=promotion_gate if isinstance(promotion_gate, dict) else None,
            topk=topk,
        )
        dossiers[recipe_name] = dossier
        matrix.append(row)
    lead_recipe = _select_lead_and_finalize_roles(matrix, dossiers)
    matrix = sorted(matrix, key=lambda row: _safe_float(row.get("score")) or -999.0, reverse=True)
    for index, row in enumerate(matrix, start=1):
        row["rank"] = index
    findings, gaps, verdict = _run_findings(matrix, dossiers, lead_recipe)
    experiments = _next_experiments(lead_recipe, dossiers, gaps)
    evidence_refs = [
        ref
        for recipe in dossiers.values()
        for ref in recipe.get("evidence_refs", [])
        if ref.get("value") is not None
    ]
    return _jsonable(
        {
            "schema_version": SCHEMA_VERSION,
            "generated_at": datetime.now().astimezone().isoformat(),
            "run_id": resolved_run_dir.name,
            "run_dir": str(resolved_run_dir),
            "config_summary": {
                "universe_profile": config.get("universe_profile"),
                "panel_path": config.get("panel_path"),
                "start_date": config.get("start_date"),
                "end_date": config.get("end_date"),
                "topk": topk,
                "industry_max_weight": config.get("industry_max_weight"),
                "universe_exit_policy": config.get("universe_exit_policy"),
                "rebalance_interval_weeks": config.get("rebalance_interval_weeks"),
            },
            "recipe_names": recipe_names,
            "lead_recipe": lead_recipe,
            "verdict": verdict,
            "system_findings": findings,
            "evidence_gaps": gaps,
            "next_experiments": experiments,
            "recipe_matrix": matrix,
            "recipe_dossiers": dossiers,
            "evidence_refs": evidence_refs,
        }
    )


def _markdown_table(rows: list[dict[str, Any]], columns: list[str]) -> list[str]:
    lines = ["| " + " | ".join(columns) + " |", "| " + " | ".join("---" for _ in columns) + " |"]
    for row in rows:
        values = []
        for column in columns:
            value = row.get(column)
            if isinstance(value, float):
                values.append(f"{value:.4f}")
            else:
                values.append(str(value if value is not None else ""))
        lines.append("| " + " | ".join(values) + " |")
    return lines


ROLE_LABELS = {
    "lead": "主线",
    "filter": "过滤器",
    "candidate": "候选",
    "reject": "淘汰",
    "diagnose_only": "仅诊断",
}


def _fmt_num(value: Any, digits: int = 2) -> str:
    number = _safe_float(value)
    if number is None:
        return "无数据"
    return f"{number:.{digits}f}"


def _fmt_pct(value: Any, digits: int = 1) -> str:
    number = _safe_float(value)
    if number is None:
        return "无数据"
    return f"{number * 100:.{digits}f}%"


def _fmt_count(value: Any) -> str:
    number = _safe_float(value)
    if number is None:
        return "无数据"
    return str(int(round(number)))


def _role_label(role: Any) -> str:
    raw = str(role or "")
    return ROLE_LABELS.get(raw, raw or "未定")


def _top_industries_text(value: Any, *, limit: int = 4) -> str:
    payload = value
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            return payload
    if not isinstance(payload, dict) or not payload:
        return "无数据"
    parts = []
    for industry, count in list(payload.items())[:limit]:
        parts.append(f"{industry}{_fmt_count(count)}只")
    return "、".join(parts)


def _row_by_recipe(pack: dict[str, Any], recipe: str | None) -> dict[str, Any]:
    if not recipe:
        return {}
    return next((row for row in pack.get("recipe_matrix", []) if row.get("recipe") == recipe), {})


def _dossier(pack: dict[str, Any], recipe: str | None) -> dict[str, Any]:
    if not recipe:
        return {}
    return pack.get("recipe_dossiers", {}).get(str(recipe), {})


def _ranking_rationale(row: dict[str, Any]) -> str:
    role = str(row.get("role") or "")
    recipe = str(row.get("recipe") or "")
    wf_return = _fmt_pct(row.get("walk_forward_net_total_return"))
    drawdown = _fmt_pct(row.get("walk_forward_max_drawdown"))
    topk = _fmt_pct(row.get("walk_forward_topk_mean_excess_return_4w"), 2)
    unique = _safe_float(row.get("signal_unique_mean"))
    if role == "lead":
        return f"收益最强的一档，但 TopK 超额仍为 {topk}，最大回撤 {drawdown}，只能作为下一轮主线。"
    if role == "filter":
        return f"收益 {wf_return} 低于主线，但回撤 {drawdown} 更轻，适合检验主线的风险过滤价值。"
    if role == "diagnose_only":
        if unique is not None and unique < 0.60:
            return "收益表面突出，但分数区分度太低，先用来解释组合机制，不作为主线。"
        return "结果存在明显解释缺口，暂时只用于诊断。"
    if role == "reject":
        return "收益、排序或风险证据不支持继续投入。"
    if recipe == "baseline":
        return "保留为对照组，帮助判断新 recipe 是否真的改善研究链路。"
    return "有局部亮点，但尚未形成比主线更完整的收益解释。"


def _experiment_label(name: Any) -> str:
    labels = {
        "rebuild_scorecard_from_raw_artifacts": "重建结构化摘要",
        "min_score_spread_or_consensus_filter": "验证分数间隔或共识过滤",
        "align_target_and_actual_holding_budget": "对齐目标持仓与实际持仓",
        "walk_forward_drawdown_attribution": "拆解 walk-forward 回撤来源",
        "controlled_lead_vs_filter_rerun": "主线与过滤器同口径复跑",
    }
    raw = str(name or "")
    return labels.get(raw, raw or "下一轮实验")


def _experiment_rationale_text(name: Any, rationale: Any) -> str:
    raw = str(name or "")
    if raw == "rebuild_scorecard_from_raw_artifacts":
        return "把结构化摘要从原始产物重新生成，避免后续诊断沿用已经失准的字段。"
    if raw == "min_score_spread_or_consensus_filter":
        return "当前高分股票没有稳定跑赢候选池，需要确认分数差距、共识过滤或阈值能否改善兑现。"
    if raw == "align_target_and_actual_holding_budget":
        return "名义 TopK 与实际持仓上限不一致，必须先确认组合定义再谈实盘。"
    if raw == "walk_forward_drawdown_attribution":
        return "回撤偏深，需要按月份、行业和持仓贡献定位风险来源。"
    if raw == "controlled_lead_vs_filter_rerun":
        return "用同一窗口、同一成本假设复跑主线与过滤器，确认哪一个改善来自信号本身。"
    return str(rationale or "")


def build_system_report_from_evidence_pack(pack: dict[str, Any]) -> dict[str, Any]:
    run_id = str(pack.get("run_id") or "native_workflow_run")
    lead_recipe = pack.get("lead_recipe")
    verdict = str(pack.get("verdict") or "investigate")
    lead_row = _row_by_recipe(pack, str(lead_recipe) if lead_recipe else None)
    lead_dossier = _dossier(pack, str(lead_recipe) if lead_recipe else None)
    lead_metrics = lead_dossier.get("metrics", {}) if isinstance(lead_dossier.get("metrics"), dict) else {}
    filter_row = next((row for row in pack.get("recipe_matrix", []) if row.get("role") == "filter"), {})
    diagnose_row = next((row for row in pack.get("recipe_matrix", []) if row.get("role") == "diagnose_only"), {})
    config = pack.get("config_summary", {}) if isinstance(pack.get("config_summary"), dict) else {}
    topk = _safe_int(config.get("topk")) or 10
    empty_execution_count = sum(1 for gap in pack.get("evidence_gaps", []) if gap.get("type") == "empty_execution_diff")
    scorecard_conflict_count = sum(1 for gap in pack.get("evidence_gaps", []) if gap.get("type") == "scorecard_conflict")
    topk_rows = [
        row
        for row in pack.get("recipe_matrix", [])
        if _safe_float(row.get("walk_forward_topk_mean_excess_return_4w")) is not None
    ]
    topk_all_negative = bool(topk_rows) and all(
        (_safe_float(row.get("walk_forward_topk_mean_excess_return_4w")) or 0.0) < 0 for row in topk_rows
    )
    live_status = "caution" if verdict in {"investigate", "hold"} else ("ready" if verdict == "promote" else "blocked")
    latest = lead_dossier.get("latest_snapshot", {}) if lead_dossier else {}
    rankings = [
        {
            "recipe": row.get("recipe"),
            "role": row.get("role"),
            "rank": row.get("rank"),
            "score": row.get("score"),
            "rationale": _ranking_rationale(row),
        }
        for row in pack.get("recipe_matrix", [])
    ]

    headline = (
        f"{lead_recipe} 暂时最适合作为下一轮主线，但还不能进入实盘。"
        if lead_recipe
        else "本轮没有形成足够清晰的主线。"
    )
    current_problem = (
        "组合收益看起来不错，但高分股票本身没有稳定兑现超额；实际持仓也会超过名义 TopK，"
        "所以现在最大问题是收益来源还没有被解释清楚。"
    )
    recommended_action = (
        f"保留 {lead_recipe} 做主线"
        + (f"，用 {filter_row.get('recipe')} 做风险过滤对照" if filter_row else "")
        + "，先修复 TopK 兑现、持仓边界和验证比较，再考虑实盘。"
        if lead_recipe
        else "先补齐证据缺口，再重新选择主线。"
    )
    system_findings = [
        (
            f"{lead_recipe} 的 walk-forward 总收益为 {_fmt_pct(lead_row.get('walk_forward_net_total_return'))}，"
            f"夏普约 {_fmt_num(lead_metrics.get('walk_forward_sharpe_ratio'), 2)}，在本轮横向比较中最强。"
            if lead_recipe
            else "本轮没有可作为主线的 recipe。"
        ),
        (
            f"但它的 TopK 超额为 {_fmt_pct(lead_row.get('walk_forward_topk_mean_excess_return_4w'), 2)}，"
            f"最大回撤为 {_fmt_pct(lead_row.get('walk_forward_max_drawdown'))}；这意味着收益强，不等于排序信号已经可靠。"
            if lead_recipe
            else "缺少主线后，收益与风险需要重新跑同口径对比。"
        ),
    ]
    if topk_all_negative:
        system_findings.append("所有 recipe 的 walk-forward TopK 超额都为负，问题更像是信号到组合兑现的系统性断点，而不是某一个 recipe 的单点失败。")
    if filter_row:
        system_findings.append(
            f"{filter_row.get('recipe')} 的最大回撤为 {_fmt_pct(filter_row.get('walk_forward_max_drawdown'))}，"
            f"低于主线，适合用来测试风险过滤，而不是直接替代主线。"
        )
    if diagnose_row:
        system_findings.append(
            f"{diagnose_row.get('recipe')} 虽然收益高，但排序 IC IR 为 {_fmt_num(diagnose_row.get('walk_forward_rank_ic_ir'), 2)}，"
            f"分数唯一性约 {_fmt_pct(diagnose_row.get('signal_unique_mean'))}，更适合解释组合机制。"
        )

    live_checks: list[str] = []
    if lead_recipe:
        live_checks.extend(
            [
                f"目标组合为 {topk} 只，但历史实际持仓最多到 {_fmt_count(lead_row.get('actual_hold_max'))} 只，需要确认持仓延续、残留持仓和调仓规则。",
                f"最新目标持仓行业分布为 {_top_industries_text(latest.get('top_industries'))}，上线前需要检查行业权重而不只是股票数量。",
                f"前 10 名和 11-20 名的分数差距约 {_fmt_num(latest.get('score_gap_10_20'), 3)}，分数边界尚可，但还要证明它能转化成超额收益。",
            ]
        )
    if empty_execution_count:
        live_checks.append("执行验证表为空，说明 validation comparison 没有跑出可用数据，不能据此判断执行差异健康。")

    markdown: list[str] = [
        f"# {run_id} 系统诊断报告",
        "",
        "## Executive Verdict",
        f"- 结论：{verdict}。{headline}",
        f"- 主线建议：{recommended_action}",
        f"- 当前问题：{current_problem}",
        f"- 一句话解释：本轮最值得继续研究的是收益和分数边界更好的 {lead_recipe or '候选组合'}；但 TopK 兑现、持仓边界和执行验证还没有闭环，所以不能直接进入实盘。",
        "",
        "## Recipe Ranking & Roles",
    ]
    markdown.extend(
        [
            "| 排名 | Recipe | 角色 | 收益 | 最大回撤 | TopK 超额 | 研究判断 |",
            "| --- | --- | --- | ---: | ---: | ---: | --- |",
            *[
                (
                    f"| {row.get('rank')} | {row.get('recipe')} | {_role_label(row.get('role'))} | "
                    f"{_fmt_pct(row.get('walk_forward_net_total_return'))} | "
                    f"{_fmt_pct(row.get('walk_forward_max_drawdown'))} | "
                    f"{_fmt_pct(row.get('walk_forward_topk_mean_excess_return_4w'), 2)} | "
                    f"{_ranking_rationale(row)} |"
                )
                for row in pack.get("recipe_matrix", [])
            ],
        ]
    )
    markdown.extend(
        [
            "",
            "## System Diagnosis",
            "### 信号兑现",
            (
                "所有 recipe 的 TopK 超额都为负。这里的含义是：模型挑出的最高分股票，"
                "在 walk-forward 检验里没有稳定跑赢参考池。主线不是没有收益，而是收益不能简单解释为“高分股票有效”。"
                if topk_all_negative
                else "TopK 兑现并不一致，需要逐个拆分高分段和低分段的收益差异。"
            ),
            "### 收益来源",
            (
                f"{lead_recipe} 的组合总收益较强，但新进持仓平均收益为 {_fmt_pct(lead_metrics.get('new_position_return_mean'))}，"
                f"持有仓位平均收益为 {_fmt_pct(lead_metrics.get('carry_position_return_mean'))}。"
                "这更像是持仓延续、调仓规则或行业暴露共同贡献的结果，需要下一轮归因。"
                if lead_recipe
                else "缺少主线 recipe，无法判断收益来自新进信号还是持仓延续。"
            ),
            "### 风险与持仓边界",
            (
                f"{lead_recipe} 的最大回撤为 {_fmt_pct(lead_row.get('walk_forward_max_drawdown'))}，"
                f"实际持仓最多 {_fmt_count(lead_row.get('actual_hold_max'))} 只，高于配置的 TopK={topk}。"
                "这会让研究结论和真实组合之间出现口径差异。"
                if lead_recipe
                else "缺少持仓边界诊断。"
            ),
            "### 行业暴露",
            (
                f"最新目标持仓集中在 {_top_industries_text(latest.get('top_industries'))}。"
                "即使历史平均行业集中度不算极端，实盘前也要按最新组合单独检查行业权重。"
                if lead_recipe
                else "缺少最新目标持仓，无法判断行业集中度。"
            ),
            "### 验证完整性",
            (
                f"{empty_execution_count} 个 recipe 的执行验证表为空；这表示 validation comparison 没有可用结果，"
                "不能把执行差异理解为健康。"
                if empty_execution_count
                else "执行验证表有数据，但仍需要结合成本和换手检查。"
            ),
        ]
    )
    markdown.extend(["", "## Recipe Dossiers"])
    for row in pack.get("recipe_matrix", []):
        recipe = row.get("recipe")
        dossier = pack.get("recipe_dossiers", {}).get(str(recipe), {})
        metrics = dossier.get("metrics", {})
        latest_snapshot = dossier.get("latest_snapshot", {})
        role = str(row.get("role") or "")
        if role == "lead":
            role_text = "主线。继续投入，但下一轮必须解释收益来源和持仓口径。"
        elif role == "filter":
            role_text = "过滤器。优先验证它能否降低主线回撤，而不是单独上线。"
        elif role == "diagnose_only":
            role_text = "仅诊断。收益或机制有研究价值，但当前不适合作为主线。"
        elif role == "reject":
            role_text = "淘汰。当前证据不支持继续投入。"
        else:
            role_text = "候选。保留观察，但优先级低于主线和过滤器实验。"
        markdown.extend(
            [
                f"### {recipe}",
                f"- 角色：{role_text}",
                f"- 可以相信的部分：walk-forward 收益 {_fmt_pct(metrics.get('walk_forward_net_total_return'))}，最大回撤 {_fmt_pct(metrics.get('walk_forward_max_drawdown'))}，排序 IC IR {_fmt_num(metrics.get('walk_forward_rank_ic_ir'), 2)}。",
                f"- 需要警惕的部分：TopK 超额 {_fmt_pct(metrics.get('walk_forward_topk_mean_excess_return_4w'), 2)}，实际持仓最多 {_fmt_count(metrics.get('actual_hold_max'))} 只，最新行业分布 {_top_industries_text(latest_snapshot.get('top_industries'))}。",
                f"- 下一步用法：{_ranking_rationale(row)}",
            ]
        )
    markdown.extend(["", "## Live Portfolio Feasibility"])
    markdown.append(f"- 状态：{live_status}。当前只适合继续研究或纸面跟踪，不建议直接进入实盘。")
    for check in live_checks or ["缺少最新信号快照或目标持仓，无法判断实盘可行性。"]:
        markdown.append(f"- {check}")
    markdown.extend(["", "## Next Experiment Roadmap"])
    for experiment in pack.get("next_experiments", []):
        markdown.append(
            f"- {experiment.get('priority')}：{_experiment_label(experiment.get('name'))}。"
            f"{_experiment_rationale_text(experiment.get('name'), experiment.get('rationale'))}"
        )
    markdown.extend(["", "## Evidence Gaps"])
    if scorecard_conflict_count:
        markdown.append(f"- 结构化摘要与原始产物存在 {scorecard_conflict_count} 处字段冲突，本报告采用原始 CSV/JSON 重新汇总的 evidence pack。")
    if empty_execution_count:
        markdown.append(f"- {empty_execution_count} 个 recipe 的 `execution_diff_summary.csv` 为空，validation comparison 未运行或无数据。")
    if not scorecard_conflict_count and not empty_execution_count:
        markdown.append("- 暂无高优先级证据缺口。")
    markdown.append("- 仍缺少对收益来源的完整归因：行业 beta、持仓延续、调仓缓冲和交易成本需要在下一轮实验中拆开。")
    return {
        "headline": headline,
        "verdict": verdict,
        "lead_recipe": lead_recipe,
        "recipe_rankings": rankings,
        "current_problem": current_problem,
        "recommended_action": recommended_action,
        "system_findings": system_findings,
        "live_feasibility": {
            "status": live_status,
            "summary": "实盘前必须补齐执行验证、持仓边界、TopK 兑现和行业权重约束。",
            "checks": live_checks,
        },
        "next_experiments": pack.get("next_experiments", []),
        "evidence_refs": pack.get("evidence_refs", []),
        "markdown": "\n".join(markdown).strip() + "\n",
    }


def write_evidence_outputs(pack: dict[str, Any], output_dir: str | Path) -> dict[str, str]:
    resolved_output = Path(output_dir).expanduser().resolve()
    resolved_output.mkdir(parents=True, exist_ok=True)
    evidence_path = resolved_output / "evidence_pack.json"
    matrix_path = resolved_output / "recipe_matrix.csv"
    evidence_path.write_text(json.dumps(pack, ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame(pack.get("recipe_matrix", [])).to_csv(matrix_path, index=False)
    return {"evidence_pack": str(evidence_path), "recipe_matrix": str(matrix_path)}


def main() -> int:
    args = parse_args()
    pack = build_evidence_pack(args.run_dir)
    if args.no_write:
        print(json.dumps(pack, ensure_ascii=False, indent=2))
        return 0
    output_dir = Path(args.output_dir).expanduser() if args.output_dir else Path(args.run_dir).expanduser() / "analysis"
    paths = write_evidence_outputs(pack, output_dir)
    print(json.dumps({"run_id": pack.get("run_id"), **paths}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

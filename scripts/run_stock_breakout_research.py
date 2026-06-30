"""Run the stock breakout research workflow from FDH or an OHLCV CSV file."""

from __future__ import annotations

import argparse
import asyncio
import json
import pickle
from pathlib import Path

import pandas as pd

from qlib_research.breakout.features import FEATURE_GROUPS
from qlib_research.breakout.config import stable_config_hash
from qlib_research.breakout.evaluation import feature_importance_frame
from qlib_research.config import close_fdh, get_fdh, get_qlib_artifacts_dir
from qlib_research.core.breakout_stock import (
    StockBreakoutConfig,
    build_stock_breakout_research_frame,
    build_stock_breakout_signal_frame,
    evaluate_stock_breakout_scores,
    score_stock_breakout_events,
    stock_breakout_feature_columns,
    train_lightgbm_stock_breakout_model,
)
from qlib_research.core.research_universe import SUPPORTED_UNIVERSE_PROFILES, resolve_universe_symbols
from qlib_research.io.artifacts import publish_score_snapshot, publish_strategy_signals


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run stock breakout event research and publish artifacts.")
    parser.add_argument("--input-source", choices=["fdh", "csv"], default="fdh", help="Data source for OHLCV input. FDH is the production default.")
    parser.add_argument("--prices-csv", default=None, help="CSV with code,date,open,high,low,close and optional volume columns. Used when --input-source csv.")
    parser.add_argument("--symbols", default=None, help="Comma-separated stock codes for FDH, e.g. 600519.SH,000858.SZ.")
    parser.add_argument("--symbols-file", default=None, help="Optional text/CSV file with one stock code per line or first column.")
    parser.add_argument("--universe-profile", choices=sorted(SUPPORTED_UNIVERSE_PROFILES), default=None, help="FDH stock universe profile, used when --symbols/--symbols-file are not provided.")
    parser.add_argument("--universe-mode", choices=["historical_membership", "fixed_universe"], default="fixed_universe", help="How to resolve index universe profiles.")
    parser.add_argument("--start-date", default=None, help="FDH start date, YYYY-MM-DD.")
    parser.add_argument("--end-date", default=None, help="FDH end date, YYYY-MM-DD.")
    parser.add_argument("--adjust", choices=["qfq", "hfq", "none"], default="qfq", help="FDH adjusted daily price mode.")
    parser.add_argument("--fdh-dataset", choices=["daily_adjusted", "processed_daily"], default="daily_adjusted", help="FDH dataset to query.")
    parser.add_argument("--fdh-batch-size", type=int, default=300, help="Number of symbols per FDH query batch.")
    parser.add_argument("--model-id", default="stock-breakout-lgbm-v1")
    parser.add_argument("--artifacts-dir", default=None)
    parser.add_argument("--train-end-date", default=None, help="Optional inclusive train end date. Later events are scored out-of-sample.")
    parser.add_argument("--valid-end-date", default=None, help="Optional inclusive validation end date. Later events are test/scoring samples.")
    parser.add_argument("--evaluation-mode", choices=["fixed_split", "walk_forward"], default="fixed_split")
    parser.add_argument("--walk-forward-train-days", type=int, default=756)
    parser.add_argument("--walk-forward-valid-days", type=int, default=126)
    parser.add_argument("--walk-forward-test-days", type=int, default=63)
    parser.add_argument("--walk-forward-step-days", type=int, default=63)
    parser.add_argument("--walk-forward-max-folds", type=int, default=0, help="0 means use every available fold.")
    parser.add_argument("--lookback-window", type=int, default=60)
    parser.add_argument("--consolidation-window", type=int, default=20)
    parser.add_argument("--label-horizon-days", type=int, default=13)
    parser.add_argument(
        "--target-mode",
        choices=["absolute_return", "daily_excess_return", "path_adjusted_return", "daily_excess_path_adjusted_return"],
        default="absolute_return",
        help="Training target. daily_excess modes demean by same event date candidate pool; path_adjusted penalizes drawdown and rewards MFE.",
    )
    parser.add_argument("--success-return-pct", type=float, default=0.03)
    parser.add_argument("--breakout-pct", type=float, default=0.0)
    parser.add_argument("--max-breakout-pct", type=float, default=None)
    parser.add_argument("--min-volume-ratio", type=float, default=1.0)
    parser.add_argument("--max-consolidation-range", type=float, default=None)
    parser.add_argument("--min-close-position", type=float, default=0.0)
    parser.add_argument("--learning-rate", type=float, default=0.03)
    parser.add_argument("--num-leaves", type=int, default=63)
    parser.add_argument("--max-depth", type=int, default=-1)
    parser.add_argument("--min-data-in-leaf", type=int, default=20)
    parser.add_argument("--lambda-l1", type=float, default=0.0)
    parser.add_argument("--lambda-l2", type=float, default=0.0)
    parser.add_argument("--feature-fraction", type=float, default=1.0)
    parser.add_argument("--bagging-fraction", type=float, default=1.0)
    parser.add_argument("--bagging-freq", type=int, default=0)
    parser.add_argument("--num-boost-round", type=int, default=500)
    parser.add_argument("--early-stopping-rounds", type=int, default=30)
    parser.add_argument("--early-stopping-metric", choices=["rmse", "spearman"], default="rmse")
    parser.add_argument(
        "--feature-profile",
        choices=["full", "no_market", "stable_slim"],
        default="full",
        help="Feature subset for controlled experiments.",
    )
    parser.add_argument(
        "--score-transform",
        choices=["raw", "date_rank", "date_z", "fold_rank", "fold_z"],
        default="raw",
        help="Post-model score calibration used for ranking and evaluation. raw_signal_score is preserved when transformed.",
    )
    parser.add_argument(
        "--fresh-signal-days",
        type=int,
        default=0,
        help="Write fresh_scores.csv from events whose signal date is within N calendar days of the latest event date.",
    )
    parser.add_argument("--cache-policy", choices=["auto", "refresh", "reuse"], default="auto")
    parser.add_argument("--update-latest", action="store_true", help="Update latest_model.json to this model.")
    return parser.parse_args()


def _parse_symbols(raw_symbols: str | None, symbols_file: str | None) -> list[str]:
    symbols: list[str] = []
    if raw_symbols:
        symbols.extend(item.strip().upper() for item in raw_symbols.split(",") if item.strip())
    if symbols_file:
        path = Path(symbols_file).expanduser().resolve()
        text = path.read_text(encoding="utf-8")
        for line in text.splitlines():
            value = line.strip()
            if not value or value.startswith("#"):
                continue
            symbols.append(value.split(",", 1)[0].strip().upper())
    seen: set[str] = set()
    unique: list[str] = []
    for symbol in symbols:
        if symbol and symbol not in seen:
            seen.add(symbol)
            unique.append(symbol)
    return unique


def _batched_symbols(symbols: list[str], batch_size: int) -> list[list[str]]:
    size = max(int(batch_size or 300), 1)
    return [symbols[index : index + size] for index in range(0, len(symbols), size)]


def _write_frame(frame: pd.DataFrame, csv_path: Path, parquet_path: Path | None = None) -> dict[str, str | None]:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(csv_path, index=False)
    result: dict[str, str | None] = {"csv": csv_path.name, "parquet": None}
    if parquet_path is not None:
        try:
            frame.to_parquet(parquet_path, index=False)
            result["parquet"] = parquet_path.name
        except Exception:
            result["parquet"] = None
    return result


def _read_cached_feature_panel(cache_dir: Path) -> pd.DataFrame:
    csv_path = cache_dir / "feature_panel.csv"
    if csv_path.exists():
        return pd.read_csv(csv_path)
    return pd.DataFrame()


def _stage_columns(frame: pd.DataFrame, prefixes: tuple[str, ...]) -> list[str]:
    return [column for column in frame.columns if any(str(column).startswith(prefix) for prefix in prefixes)]


def _attach_training_target(research: pd.DataFrame, *, horizon_days: int, target_mode: str) -> tuple[pd.DataFrame, str]:
    frame = research.copy()
    raw_label = f"future_return_{horizon_days}d"
    if target_mode == "absolute_return":
        return frame, raw_label
    if raw_label not in frame.columns:
        raise ValueError(f"Cannot build {target_mode} target because {raw_label} is missing")
    if target_mode in {"path_adjusted_return", "daily_excess_path_adjusted_return"}:
        mfe_col = f"mfe_return_{horizon_days}d"
        mae_col = f"mae_return_{horizon_days}d"
        if mfe_col not in frame.columns or mae_col not in frame.columns:
            raise ValueError(f"Cannot build {target_mode} target because {mfe_col}/{mae_col} is missing")
        target = f"future_path_adjusted_return_{horizon_days}d"
        future_return = pd.to_numeric(frame[raw_label], errors="coerce")
        mfe_return = pd.to_numeric(frame[mfe_col], errors="coerce")
        mae_return = pd.to_numeric(frame[mae_col], errors="coerce")
        frame[target] = future_return + 0.5 * mfe_return + mae_return.clip(upper=0)
        if target_mode == "path_adjusted_return":
            return frame, target
        raw = frame[target]
        target = f"future_daily_excess_path_adjusted_return_{horizon_days}d"
        date_key = pd.to_datetime(frame["event_date"], errors="coerce").dt.date
        frame[target] = raw - raw.groupby(date_key).transform("mean")
        return frame, target
    if target_mode == "daily_excess_return":
        target = f"future_daily_excess_return_{horizon_days}d"
        raw = pd.to_numeric(frame[raw_label], errors="coerce")
        date_key = pd.to_datetime(frame["event_date"], errors="coerce").dt.date
        frame[target] = raw - raw.groupby(date_key).transform("mean")
        return frame, target
    raise ValueError(f"Unsupported target mode: {target_mode}")


def _attach_outcome_columns(research: pd.DataFrame, *, horizon_days: int) -> pd.DataFrame:
    frame = research.copy()
    raw_label = f"future_return_{horizon_days}d"
    daily_excess_label = f"future_daily_excess_return_{horizon_days}d"
    if raw_label in frame.columns and daily_excess_label not in frame.columns:
        raw = pd.to_numeric(frame[raw_label], errors="coerce")
        date_key = pd.to_datetime(frame["event_date"], errors="coerce").dt.date
        frame[daily_excess_label] = raw - raw.groupby(date_key).transform("mean")

    path_label = f"future_path_adjusted_return_{horizon_days}d"
    mfe_col = f"mfe_return_{horizon_days}d"
    mae_col = f"mae_return_{horizon_days}d"
    if path_label not in frame.columns and {raw_label, mfe_col, mae_col}.issubset(frame.columns):
        future_return = pd.to_numeric(frame[raw_label], errors="coerce")
        mfe_return = pd.to_numeric(frame[mfe_col], errors="coerce")
        mae_return = pd.to_numeric(frame[mae_col], errors="coerce")
        frame[path_label] = future_return + 0.5 * mfe_return + mae_return.clip(upper=0)

    daily_excess_path_label = f"future_daily_excess_path_adjusted_return_{horizon_days}d"
    if path_label in frame.columns and daily_excess_path_label not in frame.columns:
        path_return = pd.to_numeric(frame[path_label], errors="coerce")
        date_key = pd.to_datetime(frame["event_date"], errors="coerce").dt.date
        frame[daily_excess_path_label] = path_return - path_return.groupby(date_key).transform("mean")
    return frame


def _compact_evaluation_metrics(metrics: dict[str, object]) -> dict[str, object]:
    return {
        key: value
        for key, value in metrics.items()
        if key not in {"annual_slices", "symbol_slices"}
    }


def _date_topk_selection_metrics(
    scored: pd.DataFrame,
    *,
    return_column: str,
    score_column: str = "signal_score",
    top_counts: tuple[int, ...] = (1, 2, 3, 5),
) -> dict[str, object]:
    required = {"event_date", score_column, return_column}
    if scored.empty or not required.issubset(scored.columns):
        return {"date_count": 0, "evaluated_count": 0}

    frame = scored[["event_date", score_column, return_column]].copy()
    frame["event_date"] = pd.to_datetime(frame["event_date"], errors="coerce")
    frame[score_column] = pd.to_numeric(frame[score_column], errors="coerce")
    frame[return_column] = pd.to_numeric(frame[return_column], errors="coerce")
    frame = frame.dropna(subset=["event_date", score_column, return_column])
    if frame.empty:
        return {"date_count": 0, "evaluated_count": 0}

    frame["_event_day"] = frame["event_date"].dt.date
    sorted_frame = frame.sort_values(["_event_day", score_column], ascending=[True, False])
    pool_mean = float(frame[return_column].mean())
    out: dict[str, object] = {
        "date_count": int(frame["_event_day"].nunique()),
        "evaluated_count": int(len(frame)),
        "pool_mean_return": pool_mean,
        "pool_hit_rate": float((frame[return_column] > 0).mean()),
        "mean_candidates_per_date": float(frame.groupby("_event_day").size().mean()),
    }
    for top_count in top_counts:
        picked = sorted_frame.groupby("_event_day", group_keys=False).head(max(int(top_count), 1))
        mean_return = float(picked[return_column].mean()) if not picked.empty else None
        tag = f"top{int(top_count)}_per_date"
        out[f"{tag}_count"] = int(len(picked))
        out[f"{tag}_mean_return"] = mean_return
        out[f"{tag}_excess_return"] = mean_return - pool_mean if mean_return is not None else None
        out[f"{tag}_hit_rate"] = float((picked[return_column] > 0).mean()) if not picked.empty else None
    return out


def _outcome_columns(*, horizon_days: int, label_column: str) -> dict[str, str]:
    return {
        "training_label": label_column,
        "absolute_return": f"future_return_{horizon_days}d",
        "daily_excess_return": f"future_daily_excess_return_{horizon_days}d",
        "path_adjusted_return": f"future_path_adjusted_return_{horizon_days}d",
        "daily_excess_path_adjusted_return": f"future_daily_excess_path_adjusted_return_{horizon_days}d",
    }


def _evaluate_outcome_metrics(
    scored: pd.DataFrame,
    *,
    horizon_days: int,
    label_column: str,
) -> dict[str, dict[str, object]]:
    metrics: dict[str, dict[str, object]] = {}
    for name, column in _outcome_columns(horizon_days=horizon_days, label_column=label_column).items():
        if column not in scored.columns:
            continue
        metrics[name] = _compact_evaluation_metrics(
            evaluate_stock_breakout_scores(scored, return_column=column, include_slices=False)
        )
        metrics[name]["date_selection"] = _date_topk_selection_metrics(scored, return_column=column)
    return metrics


def _feature_columns_for_profile(research: pd.DataFrame, profile: str) -> list[str]:
    columns = stock_breakout_feature_columns(research)
    if profile == "full":
        return columns

    market_columns = set(FEATURE_GROUPS["market_env"])
    if profile == "no_market":
        blocked = market_columns
    elif profile == "stable_slim":
        blocked = {
            *market_columns,
            *FEATURE_GROUPS["fundamentals"],
            "entry_price",
            "ref_high",
            "ref_low",
        }
    else:
        raise ValueError(f"Unsupported feature profile: {profile}")

    selected: list[str] = []
    for column in columns:
        if column in blocked:
            continue
        missing_ratio = pd.to_numeric(research[column], errors="coerce").isna().mean()
        if profile == "stable_slim" and missing_ratio >= 0.95:
            continue
        selected.append(column)
    return selected


def _apply_score_transform(scored: pd.DataFrame, transform: str, *, score_column: str = "signal_score") -> pd.DataFrame:
    if scored.empty or transform == "raw":
        return scored
    if score_column not in scored.columns:
        return scored

    frame = scored.copy()
    raw_column = f"raw_{score_column}"
    if raw_column not in frame.columns:
        frame[raw_column] = pd.to_numeric(frame[score_column], errors="coerce")
    score = pd.to_numeric(frame[raw_column], errors="coerce")

    if transform.startswith("date_"):
        if "event_date" not in frame.columns:
            raise ValueError(f"{transform} score transform requires event_date")
        group_key = pd.to_datetime(frame["event_date"], errors="coerce").dt.date
    elif transform.startswith("fold_"):
        if "walk_forward_fold_id" in frame.columns:
            group_key = frame["walk_forward_fold_id"].fillna("__missing_fold__")
        elif "dataset_split" in frame.columns:
            group_key = frame["dataset_split"].fillna("__missing_split__")
        else:
            group_key = pd.Series("__all__", index=frame.index)
    else:
        raise ValueError(f"Unsupported score transform: {transform}")

    grouped = score.groupby(group_key)
    if transform.endswith("_rank"):
        frame[score_column] = grouped.rank(method="average", pct=True)
    elif transform.endswith("_z"):
        mean = grouped.transform("mean")
        std = grouped.transform("std").replace(0, pd.NA)
        frame[score_column] = ((score - mean) / std).fillna(0.0)
    else:
        raise ValueError(f"Unsupported score transform: {transform}")
    frame["score_transform"] = transform
    return frame


def _fold_stability_metrics(folds: pd.DataFrame, *, prefix: str = "") -> dict[str, object]:
    if folds.empty or "status" not in folds.columns:
        return {"fold_count": 0}
    scored = folds.loc[folds["status"] == "scored"].copy()
    if scored.empty:
        return {"fold_count": 0}
    rank_ic = pd.to_numeric(scored.get(f"{prefix}rank_ic"), errors="coerce").dropna()
    top20_excess = pd.to_numeric(scored.get(f"{prefix}top20_excess_return"), errors="coerce").dropna()
    top20_spread = pd.to_numeric(scored.get(f"{prefix}long_short_spread"), errors="coerce").dropna()
    return {
        "fold_count": int(len(scored)),
        "rank_ic_mean": float(rank_ic.mean()) if not rank_ic.empty else None,
        "rank_ic_positive_rate": float((rank_ic > 0).mean()) if not rank_ic.empty else None,
        "top20_excess_mean": float(top20_excess.mean()) if not top20_excess.empty else None,
        "top20_excess_positive_rate": float((top20_excess > 0).mean()) if not top20_excess.empty else None,
        "long_short_spread_mean": float(top20_spread.mean()) if not top20_spread.empty else None,
        "long_short_spread_positive_rate": float((top20_spread > 0).mean()) if not top20_spread.empty else None,
    }


def _date_selection_fold_stability(folds: pd.DataFrame, *, prefix: str = "") -> dict[str, object]:
    if folds.empty or "status" not in folds.columns:
        return {"fold_count": 0}
    scored = folds.loc[folds["status"] == "scored"].copy()
    if scored.empty:
        return {"fold_count": 0}
    out: dict[str, object] = {"fold_count": int(len(scored))}
    for top_count in (1, 2):
        tag = f"date_top{top_count}"
        excess = pd.to_numeric(scored.get(f"{prefix}{tag}_excess_return"), errors="coerce").dropna()
        hit = pd.to_numeric(scored.get(f"{prefix}{tag}_hit_rate"), errors="coerce").dropna()
        out[f"{tag}_excess_mean"] = float(excess.mean()) if not excess.empty else None
        out[f"{tag}_excess_positive_rate"] = float((excess > 0).mean()) if not excess.empty else None
        out[f"{tag}_hit_rate_mean"] = float(hit.mean()) if not hit.empty else None
    return out


def _score_snapshot_frame(signals: pd.DataFrame, *, horizon_days: int, feature_date: str) -> pd.DataFrame:
    if signals.empty:
        return pd.DataFrame(columns=["code", "qlib_score", "qlib_rank", "pred_return", "feature_date", "signal_date", "signal_age_days"])
    latest_scores = (
        signals.sort_values(["signal_date", "signal_score"], ascending=[True, False])
        .groupby("code", as_index=False)
        .tail(1)
        .sort_values("signal_score", ascending=False)
        .reset_index(drop=True)
    )
    latest_scores["qlib_score"] = latest_scores["signal_score"]
    latest_scores["qlib_rank"] = latest_scores.index + 1
    feature_ts = pd.to_datetime(feature_date)
    signal_dates = pd.to_datetime(latest_scores["signal_date"], errors="coerce")
    latest_scores["signal_age_days"] = (feature_ts - signal_dates).dt.days
    return latest_scores[
        ["code", "qlib_score", "qlib_rank", f"pred_return_{horizon_days}d", "feature_date", "signal_date", "signal_age_days"]
    ].rename(columns={f"pred_return_{horizon_days}d": "pred_return"})


def _fresh_score_snapshot_frame(signals: pd.DataFrame, *, horizon_days: int, feature_date: str, fresh_signal_days: int) -> pd.DataFrame:
    if signals.empty:
        return pd.DataFrame(columns=["code", "qlib_score", "qlib_rank", "pred_return", "feature_date", "signal_date", "signal_age_days"])
    feature_ts = pd.to_datetime(feature_date)
    cutoff = feature_ts - pd.Timedelta(days=max(int(fresh_signal_days), 0))
    frame = signals.loc[pd.to_datetime(signals["signal_date"], errors="coerce") >= cutoff].copy()
    if frame.empty:
        return pd.DataFrame(columns=["code", "qlib_score", "qlib_rank", "pred_return", "feature_date", "signal_date", "signal_age_days"])
    frame = (
        frame.sort_values(["signal_date", "signal_score"], ascending=[True, False])
        .groupby("code", as_index=False)
        .tail(1)
        .sort_values(["signal_score", "signal_date"], ascending=[False, False])
        .reset_index(drop=True)
    )
    frame["qlib_score"] = frame["signal_score"]
    frame["qlib_rank"] = frame.index + 1
    signal_dates = pd.to_datetime(frame["signal_date"], errors="coerce")
    frame["signal_age_days"] = (feature_ts - signal_dates).dt.days
    return frame[["code", "qlib_score", "qlib_rank", f"pred_return_{horizon_days}d", "feature_date", "signal_date", "signal_age_days"]].rename(
        columns={f"pred_return_{horizon_days}d": "pred_return"}
    )


def _split_research_frame(research: pd.DataFrame, train_end: str | None, valid_end: str | None) -> pd.DataFrame:
    frame = research.copy()
    dates = pd.to_datetime(frame["event_date"], errors="coerce")
    frame["dataset_split"] = "train"
    if train_end:
        train_end_ts = pd.to_datetime(train_end)
        frame.loc[dates > train_end_ts, "dataset_split"] = "test"
        if valid_end:
            valid_end_ts = pd.to_datetime(valid_end)
            frame.loc[(dates > train_end_ts) & (dates <= valid_end_ts), "dataset_split"] = "valid"
            frame.loc[dates > valid_end_ts, "dataset_split"] = "test"
    return frame


def _date_mask(frame: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> pd.Series:
    dates = pd.to_datetime(frame["event_date"], errors="coerce")
    return dates.between(start, end, inclusive="both")


def _walk_forward_fold_plan(
    research: pd.DataFrame,
    *,
    train_days: int,
    valid_days: int,
    test_days: int,
    step_days: int,
    max_folds: int = 0,
) -> pd.DataFrame:
    dates = pd.to_datetime(research.get("event_date"), errors="coerce").dropna().sort_values()
    if dates.empty:
        return pd.DataFrame()

    min_date = pd.Timestamp(dates.min()).normalize()
    max_date = pd.Timestamp(dates.max()).normalize()
    train_days = max(1, int(train_days))
    valid_days = max(0, int(valid_days))
    test_days = max(1, int(test_days))
    step_days = max(1, int(step_days))

    eval_start = min_date + pd.Timedelta(days=train_days + valid_days)
    rows: list[dict[str, object]] = []
    fold_index = 1
    while eval_start <= max_date:
        train_start = eval_start - pd.Timedelta(days=train_days + valid_days)
        train_end = eval_start - pd.Timedelta(days=valid_days + 1)
        valid_start = train_end + pd.Timedelta(days=1)
        valid_end = eval_start - pd.Timedelta(days=1)
        test_start = eval_start
        test_end = min(eval_start + pd.Timedelta(days=test_days - 1), max_date)
        if test_end >= test_start:
            rows.append(
                {
                    "fold_id": f"wf_{fold_index:03d}",
                    "train_start": train_start.date().isoformat(),
                    "train_end": train_end.date().isoformat(),
                    "valid_start": valid_start.date().isoformat() if valid_days else None,
                    "valid_end": valid_end.date().isoformat() if valid_days else None,
                    "test_start": test_start.date().isoformat(),
                    "test_end": test_end.date().isoformat(),
                }
            )
            fold_index += 1
        eval_start = eval_start + pd.Timedelta(days=step_days)

    plan = pd.DataFrame(rows)
    if max_folds and max_folds > 0 and len(plan) > max_folds:
        plan = plan.tail(max_folds).reset_index(drop=True)
        plan["fold_id"] = [f"wf_{index + 1:03d}" for index in range(len(plan))]
    return plan


def _run_walk_forward_evaluation(
    research: pd.DataFrame,
    *,
    label_column: str,
    horizon_days: int,
    score_transform: str,
    feature_columns: list[str],
    model_params: dict,
    num_boost_round: int,
    early_stopping_rounds: int,
    early_stopping_metric: str,
    train_days: int,
    valid_days: int,
    test_days: int,
    step_days: int,
    max_folds: int,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object]]:
    plan = _walk_forward_fold_plan(
        research,
        train_days=train_days,
        valid_days=valid_days,
        test_days=test_days,
        step_days=step_days,
        max_folds=max_folds,
    )
    if plan.empty:
        return pd.DataFrame(), plan, {"fold_count": 0, "reason": "no_available_folds"}

    scored_parts: list[pd.DataFrame] = []
    fold_rows: list[dict[str, object]] = []
    for row in plan.to_dict(orient="records"):
        train_frame = research.loc[
            _date_mask(research, pd.Timestamp(row["train_start"]), pd.Timestamp(row["train_end"]))
        ].copy()
        valid_frame = pd.DataFrame()
        if row.get("valid_start") and row.get("valid_end"):
            valid_frame = research.loc[
                _date_mask(research, pd.Timestamp(row["valid_start"]), pd.Timestamp(row["valid_end"]))
            ].copy()
        test_frame = research.loc[
            _date_mask(research, pd.Timestamp(row["test_start"]), pd.Timestamp(row["test_end"]))
        ].copy()
        fold_info = {
            **row,
            "train_rows": int(len(train_frame)),
            "valid_rows": int(len(valid_frame)),
            "test_rows": int(len(test_frame)),
            "status": "skipped",
            "message": None,
        }
        if train_frame.empty or test_frame.empty:
            fold_info["message"] = "empty train/test window"
            fold_rows.append(fold_info)
            continue
        try:
            model, trained_features = train_lightgbm_stock_breakout_model(
                train_frame,
                label_column,
                feature_columns,
                valid_frame=valid_frame if not valid_frame.empty else None,
                model_params=model_params,
                num_boost_round=num_boost_round,
                early_stopping_rounds=early_stopping_rounds,
                early_stopping_metric=early_stopping_metric,
            )
            scored_fold = score_stock_breakout_events(model, test_frame, trained_features)
            scored_fold["walk_forward_fold_id"] = row["fold_id"]
            scored_fold["dataset_split"] = "walk_forward_test"
            scored_fold = _apply_score_transform(scored_fold, score_transform)
            scored_parts.append(scored_fold)
            fold_metrics = evaluate_stock_breakout_scores(scored_fold, return_column=label_column)
            fold_info.update(
                {
                    "status": "scored",
                    "rank_ic": fold_metrics.get("rank_ic"),
                    "top20_mean_return": fold_metrics.get("top20_mean_return"),
                    "top20_excess_return": fold_metrics.get("top20_excess_return"),
                    "top20_hit_rate": fold_metrics.get("top20_hit_rate"),
                    "long_short_spread": fold_metrics.get("long_short_spread"),
                }
            )
            fold_outcomes = _evaluate_outcome_metrics(
                scored_fold,
                horizon_days=horizon_days,
                label_column=label_column,
            )
            for outcome_name, outcome_metrics in fold_outcomes.items():
                prefix = f"{outcome_name}_"
                date_selection = outcome_metrics.get("date_selection") if isinstance(outcome_metrics, dict) else {}
                fold_info.update(
                    {
                        f"{prefix}rank_ic": outcome_metrics.get("rank_ic"),
                        f"{prefix}top20_mean_return": outcome_metrics.get("top20_mean_return"),
                        f"{prefix}top20_excess_return": outcome_metrics.get("top20_excess_return"),
                        f"{prefix}top20_hit_rate": outcome_metrics.get("top20_hit_rate"),
                        f"{prefix}long_short_spread": outcome_metrics.get("long_short_spread"),
                        f"{prefix}date_top1_excess_return": date_selection.get("top1_per_date_excess_return")
                        if isinstance(date_selection, dict)
                        else None,
                        f"{prefix}date_top1_hit_rate": date_selection.get("top1_per_date_hit_rate")
                        if isinstance(date_selection, dict)
                        else None,
                        f"{prefix}date_top2_excess_return": date_selection.get("top2_per_date_excess_return")
                        if isinstance(date_selection, dict)
                        else None,
                        f"{prefix}date_top2_hit_rate": date_selection.get("top2_per_date_hit_rate")
                        if isinstance(date_selection, dict)
                        else None,
                    }
                )
        except Exception as exc:
            fold_info["message"] = str(exc)
        fold_rows.append(fold_info)

    scored = pd.concat(scored_parts, ignore_index=True) if scored_parts else pd.DataFrame()
    folds = pd.DataFrame(fold_rows)
    metrics = evaluate_stock_breakout_scores(scored, return_column=label_column) if not scored.empty else {"event_count": 0, "evaluated_count": 0}
    outcome_metrics = _evaluate_outcome_metrics(scored, horizon_days=horizon_days, label_column=label_column) if not scored.empty else {}
    metrics["fold_count"] = int((folds.get("status") == "scored").sum()) if "status" in folds else 0
    metrics["requested_fold_count"] = int(len(plan))
    metrics["fold_stability"] = _fold_stability_metrics(folds)
    metrics["outcomes"] = outcome_metrics
    metrics["outcome_fold_stability"] = {
        name: _fold_stability_metrics(folds, prefix=f"{name}_")
        for name in outcome_metrics
    }
    metrics["date_selection_fold_stability"] = {
        name: _date_selection_fold_stability(folds, prefix=f"{name}_")
        for name in outcome_metrics
    }
    return scored, folds, metrics


async def _resolve_effective_symbols(args: argparse.Namespace) -> tuple[list[str], str | None]:
    explicit_symbols = _parse_symbols(args.symbols, args.symbols_file)
    if explicit_symbols:
        return explicit_symbols, None
    if args.universe_profile:
        symbols, _ = await resolve_universe_symbols(
            universe_profile=args.universe_profile,
            start_date=args.start_date,
            end_date=args.end_date,
            universe_mode=args.universe_mode,
        )
        return symbols, args.universe_profile
    return [], None


async def _load_price_frame(args: argparse.Namespace) -> tuple[pd.DataFrame, list[str], str | None]:
    if args.input_source == "csv":
        if not args.prices_csv:
            raise ValueError("--prices-csv is required when --input-source csv")
        return pd.read_csv(args.prices_csv), [], None

    symbols, resolved_universe_profile = await _resolve_effective_symbols(args)
    if not symbols:
        raise ValueError("--symbols, --symbols-file, or --universe-profile is required when --input-source fdh")
    if not args.start_date:
        raise ValueError("--start-date is required when --input-source fdh")

    fdh = await get_fdh()
    frames: list[pd.DataFrame] = []
    try:
        for batch in _batched_symbols(symbols, args.fdh_batch_size):
            if args.fdh_dataset == "processed_daily":
                frame = await fdh.get_processed_daily_async(
                    symbols=batch,
                    start_date=args.start_date,
                    end_date=args.end_date,
                )
            else:
                frame = await fdh.get_daily_adjusted_async(
                    symbols=batch,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    adjust=args.adjust,
                )
            if frame is not None and not frame.empty:
                frames.append(frame)
    finally:
        await close_fdh()
    frame = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if frame is None or frame.empty:
        raise ValueError("FDH returned no stock daily rows for the requested symbols/date range")
    return frame, symbols, resolved_universe_profile


def _load_existing_research_frame(model_dir: Path) -> tuple[pd.DataFrame, dict]:
    manifest_path = model_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
    candidates = [
        model_dir / str(manifest.get("feature_panel_path") or "feature_panel.csv"),
        model_dir / "feature_panel.csv",
    ]
    for path in candidates:
        if path.exists():
            return pd.read_csv(path), manifest
    return pd.DataFrame(), manifest


async def async_main() -> None:
    args = parse_args()
    cfg = StockBreakoutConfig(
        lookback_window=args.lookback_window,
        consolidation_window=args.consolidation_window,
        breakout_pct=args.breakout_pct,
        max_breakout_pct=args.max_breakout_pct,
        label_horizon_days=args.label_horizon_days,
        success_return_pct=args.success_return_pct,
        min_volume_ratio=args.min_volume_ratio,
        max_consolidation_range=args.max_consolidation_range,
        min_close_position=args.min_close_position,
    )
    artifacts_dir = Path(args.artifacts_dir or get_qlib_artifacts_dir()).expanduser().resolve()
    model_dir = artifacts_dir / args.model_id
    model_dir.mkdir(parents=True, exist_ok=True)

    price_frame = pd.DataFrame()
    resolved_symbols: list[str] = []
    resolved_universe_profile: str | None = None
    existing_manifest: dict = {}
    source_error: Exception | None = None
    try:
        price_frame, resolved_symbols, resolved_universe_profile = await _load_price_frame(args)
    except Exception as exc:
        source_error = exc
        if args.cache_policy in {"auto", "reuse"}:
            research, existing_manifest = _load_existing_research_frame(model_dir)
            if not research.empty:
                print(
                    json.dumps(
                        {
                            "event": "breakout_reused_existing_feature_panel",
                            "model_id": args.model_id,
                            "reason": str(exc),
                            "feature_panel_path": str(model_dir / str(existing_manifest.get("feature_panel_path") or "feature_panel.csv")),
                        },
                        ensure_ascii=False,
                    )
                )
        else:
            raise
    source_config = existing_manifest.get("source_data") if isinstance(existing_manifest.get("source_data"), dict) else {
        "input_source": args.input_source,
        "fdh_dataset": args.fdh_dataset if args.input_source == "fdh" else None,
        "symbols": resolved_symbols if args.input_source == "fdh" else None,
        "symbol_count": len(resolved_symbols) if args.input_source == "fdh" else None,
        "universe_profile": resolved_universe_profile,
        "universe_mode": args.universe_mode if resolved_universe_profile else None,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "adjust": args.adjust if args.input_source == "fdh" else None,
    }
    config_hash = stable_config_hash(
        {
            "detector": {
                "lookback_window": cfg.lookback_window,
                "consolidation_window": cfg.consolidation_window,
                "breakout_pct": cfg.breakout_pct,
                "max_breakout_pct": cfg.max_breakout_pct,
                "min_volume_ratio": cfg.min_volume_ratio,
                "max_consolidation_range": cfg.max_consolidation_range,
                "min_close_position": cfg.min_close_position,
                "min_history_days": cfg.min_history_days,
            },
            "labeler": {
                "label_horizon_days": cfg.label_horizon_days,
                "success_return_pct": cfg.success_return_pct,
            },
            "features": {"feature_set": "stock_breakout_80_v1"},
            "source_data": source_config,
        }
    )
    dataset_id = f"stock-D-{resolved_universe_profile or 'explicit'}-{args.start_date or 'na'}-{args.end_date or 'latest'}-{config_hash}"
    cache_dir = artifacts_dir / "breakout_cache" / dataset_id
    research = research if "research" in locals() else pd.DataFrame()
    if args.cache_policy in {"auto", "reuse"}:
        cached = _read_cached_feature_panel(cache_dir)
        if not cached.empty:
            research = cached
    if research.empty:
        if price_frame.empty and source_error is not None:
            raise SystemExit(f"Unable to load FDH data and no reusable feature panel exists for {args.model_id}: {source_error}")
        research = build_stock_breakout_research_frame(price_frame, cfg)
        cache_dir.mkdir(parents=True, exist_ok=True)
        event_columns = [
            "event_id",
            "asset_class",
            "code",
            "event_date",
            "side",
            "frequency",
            "candidate_rule",
            "entry_price",
            "ref_high",
            "ref_low",
            "breakout_strength",
            "volume_ratio",
            "consolidation_range",
        ]
        label_columns = ["event_id", "code", "event_date", *_stage_columns(research, ("future_", "mfe_", "mae_", "label_"))]
        _write_frame(research[[column for column in event_columns if column in research.columns]], cache_dir / "events.csv", cache_dir / "events.parquet")
        _write_frame(research[[column for column in label_columns if column in research.columns]], cache_dir / "labels.csv", cache_dir / "labels.parquet")
        _write_frame(research, cache_dir / "feature_panel.csv", cache_dir / "feature_panel.parquet")
        (cache_dir / "cache_manifest.json").write_text(
            json.dumps(
                {
                    "dataset_id": dataset_id,
                    "config_hash": config_hash,
                    "generated_at": pd.Timestamp.utcnow().isoformat(),
                    "source_data": source_config,
                    "cache_policy": args.cache_policy,
                },
                ensure_ascii=False,
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )
    if research.empty:
        raise SystemExit("No stock breakout events detected.")
    research = _split_research_frame(research, args.train_end_date, args.valid_end_date)
    research, label_column = _attach_training_target(research, horizon_days=cfg.label_horizon_days, target_mode=args.target_mode)
    research = _attach_outcome_columns(research, horizon_days=cfg.label_horizon_days)
    feature_columns = _feature_columns_for_profile(research, args.feature_profile)
    train_frame = research.loc[research["dataset_split"] == "train"].copy()
    valid_frame = research.loc[research["dataset_split"] == "valid"].copy()
    if train_frame.empty:
        train_frame = research.copy()

    model_params = {
        "learning_rate": args.learning_rate,
        "num_leaves": args.num_leaves,
        "max_depth": args.max_depth,
        "min_data_in_leaf": args.min_data_in_leaf,
        "lambda_l1": args.lambda_l1,
        "lambda_l2": args.lambda_l2,
        "feature_fraction": args.feature_fraction,
        "bagging_fraction": args.bagging_fraction,
        "bagging_freq": args.bagging_freq,
    }
    model, trained_features = train_lightgbm_stock_breakout_model(
        train_frame,
        label_column,
        feature_columns,
        valid_frame=valid_frame if not valid_frame.empty else None,
        model_params=model_params,
        num_boost_round=args.num_boost_round,
        early_stopping_rounds=args.early_stopping_rounds,
        early_stopping_metric=args.early_stopping_metric,
    )
    scored = score_stock_breakout_events(model, research, trained_features)
    scored = _apply_score_transform(scored, args.score_transform)
    signals = build_stock_breakout_signal_frame(scored, model_id=args.model_id, config=cfg)
    metrics = evaluate_stock_breakout_scores(scored, return_column=label_column)
    metrics["outcomes"] = _evaluate_outcome_metrics(scored, horizon_days=cfg.label_horizon_days, label_column=label_column)
    importance = feature_importance_frame(model, trained_features)
    walk_forward_scored = pd.DataFrame()
    walk_forward_folds = pd.DataFrame()
    if args.evaluation_mode == "walk_forward":
        walk_forward_scored, walk_forward_folds, walk_forward_metrics = _run_walk_forward_evaluation(
            research,
            label_column=label_column,
            horizon_days=cfg.label_horizon_days,
            score_transform=args.score_transform,
            feature_columns=feature_columns,
            model_params=model_params,
            num_boost_round=args.num_boost_round,
            early_stopping_rounds=args.early_stopping_rounds,
            early_stopping_metric=args.early_stopping_metric,
            train_days=args.walk_forward_train_days,
            valid_days=args.walk_forward_valid_days,
            test_days=args.walk_forward_test_days,
            step_days=args.walk_forward_step_days,
            max_folds=args.walk_forward_max_folds,
        )
        metrics["walk_forward"] = walk_forward_metrics
        metrics["walk_forward_fold_count"] = walk_forward_metrics.get("fold_count")
        metrics["walk_forward_rank_ic"] = walk_forward_metrics.get("rank_ic")
        metrics["walk_forward_top20_mean_return"] = walk_forward_metrics.get("top20_mean_return")
        metrics["walk_forward_top20_hit_rate"] = walk_forward_metrics.get("top20_hit_rate")

    feature_date = str(pd.to_datetime(scored["event_date"]).max().date())
    score_output = _score_snapshot_frame(signals, horizon_days=cfg.label_horizon_days, feature_date=feature_date)
    fresh_score_output = _fresh_score_snapshot_frame(
        signals,
        horizon_days=cfg.label_horizon_days,
        feature_date=feature_date,
        fresh_signal_days=args.fresh_signal_days,
    )

    event_columns = [
        "event_id",
        "asset_class",
        "code",
        "event_date",
        "side",
        "frequency",
        "candidate_rule",
        "entry_price",
        "ref_high",
        "ref_low",
        "breakout_strength",
        "volume_ratio",
        "consolidation_range",
    ]
    label_columns = ["event_id", "code", "event_date", *_stage_columns(research, ("future_", "mfe_", "mae_", "label_"))]
    _write_frame(research[[column for column in event_columns if column in research.columns]], model_dir / "events.csv", model_dir / "events.parquet")
    _write_frame(research[[column for column in label_columns if column in research.columns]], model_dir / "labels.csv", model_dir / "labels.parquet")
    panel_paths = _write_frame(research, model_dir / "feature_panel.csv", model_dir / "feature_panel.parquet")
    _write_frame(scored, model_dir / "events_scored.csv", model_dir / "events_scored.parquet")
    if args.evaluation_mode == "walk_forward":
        _write_frame(walk_forward_scored, model_dir / "walk_forward_predictions.csv", model_dir / "walk_forward_predictions.parquet")
        _write_frame(walk_forward_folds, model_dir / "walk_forward_folds.csv", model_dir / "walk_forward_folds.parquet")
    fresh_score_output.to_csv(model_dir / "fresh_scores.csv", index=False)
    importance.to_csv(model_dir / "feature_importance.csv", index=False)
    (model_dir / "metrics.json").write_text(json.dumps(metrics, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    with (model_dir / "model.pkl").open("wb") as fh:
        pickle.dump({"model": model, "feature_columns": trained_features, "config": cfg}, fh)

    publish_score_snapshot(
        score_output,
        model_id=args.model_id,
        feature_date=feature_date,
        artifacts_dir=artifacts_dir,
        update_latest=args.update_latest,
        extra_manifest={
            "research_kind": "breakout_event",
            "asset_class": "stock",
            "frequency": "D",
            "dataset_id": dataset_id,
            "config_hash": config_hash,
            "label_policy": {
                "horizon_days": cfg.label_horizon_days,
                "success_return_pct": cfg.success_return_pct,
                "target": label_column,
                "target_mode": args.target_mode,
            },
            "detector_policy": {
                "lookback_window": cfg.lookback_window,
                "consolidation_window": cfg.consolidation_window,
                "breakout_pct": cfg.breakout_pct,
                "max_breakout_pct": cfg.max_breakout_pct,
                "min_volume_ratio": cfg.min_volume_ratio,
                "max_consolidation_range": cfg.max_consolidation_range,
                "min_close_position": cfg.min_close_position,
            },
            "feature_policy": {
                "feature_set": "stock_breakout_80_v1",
                "feature_profile": args.feature_profile,
                "feature_columns": trained_features,
            },
            "score_policy": {
                "score_column": "signal_score",
                "raw_score_column": "raw_signal_score" if args.score_transform != "raw" else None,
                "score_transform": args.score_transform,
            },
            "trainer_policy": {
                "evaluation_mode": args.evaluation_mode,
                "train_end_date": args.train_end_date,
                "valid_end_date": args.valid_end_date,
                "learning_rate": args.learning_rate,
                "num_leaves": args.num_leaves,
                "max_depth": args.max_depth,
                "min_data_in_leaf": args.min_data_in_leaf,
                "lambda_l1": args.lambda_l1,
                "lambda_l2": args.lambda_l2,
                "feature_fraction": args.feature_fraction,
                "bagging_fraction": args.bagging_fraction,
                "bagging_freq": args.bagging_freq,
                "num_boost_round": args.num_boost_round,
                "early_stopping_rounds": args.early_stopping_rounds,
                "early_stopping_metric": args.early_stopping_metric,
                "walk_forward_train_days": args.walk_forward_train_days,
                "walk_forward_valid_days": args.walk_forward_valid_days,
                "walk_forward_test_days": args.walk_forward_test_days,
                "walk_forward_step_days": args.walk_forward_step_days,
                "walk_forward_max_folds": args.walk_forward_max_folds,
            },
            "source_data": source_config,
            "cache_policy": {"cache_dir": str(cache_dir), "cache_policy": args.cache_policy},
            "events_path": "events.csv",
            "labels_path": "labels.csv",
            "signal_path": "signals.csv",
            "fresh_scores_path": "fresh_scores.csv",
            "fresh_signal_days": args.fresh_signal_days,
            "metrics_path": "metrics.json",
            "feature_panel_path": "feature_panel.csv",
            "feature_panel_parquet_path": panel_paths.get("parquet"),
            "events_scored_path": "events_scored.csv",
            "walk_forward_predictions_path": "walk_forward_predictions.csv" if args.evaluation_mode == "walk_forward" else None,
            "walk_forward_folds_path": "walk_forward_folds.csv" if args.evaluation_mode == "walk_forward" else None,
            "feature_importance_path": "feature_importance.csv",
            "model_path": "model.pkl",
        },
    )
    publish_strategy_signals(signals, model_id=args.model_id, artifacts_dir=artifacts_dir)
    print(json.dumps({"model_id": args.model_id, "feature_date": feature_date, "metrics": metrics}, ensure_ascii=False, default=str))


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()

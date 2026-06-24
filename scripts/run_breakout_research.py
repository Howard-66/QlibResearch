"""End-to-end breakout research pipeline: detect → label → features → train → evaluate."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import traceback
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from qlib_research.breakout.config import (  # noqa: E402
    BreakoutResearchConfig,
    DetectorConfig,
    LabelConfig,
    TrainConfig,
)
from qlib_research.breakout.detector import detect_breakouts  # noqa: E402
from qlib_research.breakout.evaluator import evaluate_predictions  # noqa: E402
from qlib_research.breakout.features import (  # noqa: E402
    ALL_FEATURE_COLUMNS,
    extract_features_batch,
)
from qlib_research.breakout.labeler import label_events  # noqa: E402
from qlib_research.breakout.scorer import BreakoutScorer  # noqa: E402
from qlib_research.breakout.trainer import train_breakout_model  # noqa: E402

_INDEX_CODE_MAP = {
    "csi300": "000300.SH",
    "csi500": "000905.SH",
    "all": "000300.SH",  # fallback market proxy
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run breakout strategy ML research pipeline",
    )
    parser.add_argument("--frequency", choices=["daily", "weekly"], default="daily")
    parser.add_argument("--universe", default="csi300", help="csi300 | csi500 | all")
    parser.add_argument("--train-end", default="2025-08-31")
    parser.add_argument("--valid-end", default="2026-01-31")
    parser.add_argument("--data-start", default=None,
                        help="Data fetch start date (default: train_end - 3y)")
    parser.add_argument("--data-end", default=None,
                        help="Data fetch end date (default: today)")
    parser.add_argument("--lookback", type=int, default=120)
    parser.add_argument("--horizon", type=int, default=13)
    parser.add_argument("--min-consolidation", type=int, default=10)
    parser.add_argument("--volume-threshold", type=float, default=1.5)
    parser.add_argument("--success-threshold", type=float, default=0.03)
    parser.add_argument("--max-symbols", type=int, default=None,
                        help="Cap on number of symbols (debugging)")
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--output-dir", default=None,
                        help="Output directory (default: artifacts/breakout/)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print config and exit without running")
    return parser.parse_args()


def build_config(args: argparse.Namespace) -> BreakoutResearchConfig:
    artifacts_dir = (
        Path(args.output_dir).resolve()
        if args.output_dir
        else PROJECT_ROOT / "artifacts" / "breakout"
    )
    return BreakoutResearchConfig(
        detector=DetectorConfig(
            lookback=int(args.lookback),
            min_consolidation=int(args.min_consolidation),
            frequency=args.frequency,
            volume_threshold=float(args.volume_threshold),
        ),
        labeler=LabelConfig(
            horizon_days=int(args.horizon),
            success_threshold=float(args.success_threshold),
        ),
        trainer=TrainConfig(
            train_end=args.train_end,
            valid_end=args.valid_end,
        ),
        artifacts_dir=artifacts_dir,
    )


def _serialize_config(cfg: BreakoutResearchConfig) -> dict[str, Any]:
    return {
        "detector": asdict(cfg.detector),
        "labeler": asdict(cfg.labeler),
        "trainer": asdict(cfg.trainer),
        "artifacts_dir": str(cfg.artifacts_dir),
    }


# ---------------------------------------------------------------------------
# Data loading via FinanceDataHub
# ---------------------------------------------------------------------------
async def _resolve_symbols(universe: str, end_date: str | None,
                           max_symbols: int | None) -> list[str]:
    from qlib_research.core.research_universe import resolve_universe_symbols

    if universe == "all":
        # Use the csi300+csi500 union as a stand-in for "all".
        profile = "merged_csi300_500"
    else:
        profile = universe

    symbols, _ = await resolve_universe_symbols(
        universe_profile=profile,
        end_date=end_date,
        universe_mode="fixed_universe",
    )
    if max_symbols is not None:
        symbols = symbols[: max(int(max_symbols), 0)]
    return symbols


async def _fetch_market_bars(universe: str, start: str, end: str) -> pd.DataFrame:
    from qlib_research.config import get_fdh

    fdh = await get_fdh()
    code = _INDEX_CODE_MAP.get(universe, "000300.SH")
    frame = await fdh.get_index_daily_async(
        ts_code=code, start_date=start, end_date=end,
    )
    if frame is None or len(frame) == 0:
        return pd.DataFrame()
    df = frame.copy()
    date_col = "trade_date" if "trade_date" in df.columns else (
        "time" if "time" in df.columns else None
    )
    if date_col is None:
        return pd.DataFrame()
    df = df.rename(columns={date_col: "date"})
    df["date"] = pd.to_datetime(df["date"])
    keep = [c for c in ("date", "open", "high", "low", "close", "volume")
            if c in df.columns]
    df = df[keep].sort_values("date").reset_index(drop=True)
    return df


async def _fetch_bars_dict(
    symbols: list[str],
    frequency: str,
    start: str,
    end: str,
    batch_size: int,
) -> dict[str, pd.DataFrame]:
    """Fetch processed daily/weekly bars and split per symbol."""
    from qlib_research.config import get_fdh

    fdh = await get_fdh()
    bars_dict: dict[str, pd.DataFrame] = {}
    method = (
        fdh.get_processed_daily_async if frequency == "daily"
        else fdh.get_processed_weekly_async
    )

    total = len(symbols)
    for i in range(0, total, batch_size):
        batch = symbols[i : i + batch_size]
        print(f"  Fetching {frequency} bars batch {i // batch_size + 1} "
              f"({len(batch)} symbols, {i + len(batch)}/{total})...")
        frame = await method(symbols=batch, start_date=start, end_date=end)
        if frame is None or len(frame) == 0:
            continue
        df = frame.copy()
        sym_col = "symbol" if "symbol" in df.columns else "ts_code"
        time_col = "time" if "time" in df.columns else "trade_date"
        if sym_col not in df.columns or time_col not in df.columns:
            continue
        df = df.rename(columns={sym_col: "symbol", time_col: "date"})
        df["date"] = pd.to_datetime(df["date"])
        df["symbol"] = df["symbol"].astype(str)
        keep_cols = [c for c in
                     ("date", "symbol", "open", "high", "low", "close", "volume")
                     if c in df.columns]
        df = df[keep_cols].sort_values(["symbol", "date"])
        for sym, group in df.groupby("symbol", sort=False):
            bars_dict[sym] = group.drop(columns=["symbol"]).reset_index(drop=True)
    return bars_dict


async def _load_data(args: argparse.Namespace, cfg: BreakoutResearchConfig
                    ) -> tuple[list[str], dict[str, pd.DataFrame], pd.DataFrame]:
    """Resolve symbols and fetch all required bars."""
    train_end_ts = pd.Timestamp(args.train_end)
    valid_end_ts = pd.Timestamp(args.valid_end)
    data_start = args.data_start or (
        (train_end_ts - pd.DateOffset(years=3)).strftime("%Y-%m-%d")
    )
    data_end = args.data_end or (
        # Extend past valid_end for forward-return labels.
        (valid_end_ts + pd.Timedelta(days=max(args.horizon * 2, 30))).strftime("%Y-%m-%d")
    )
    print(f"[1/8] Resolving universe ({args.universe}) ...")
    symbols = await _resolve_symbols(args.universe, args.train_end, args.max_symbols)
    print(f"      -> {len(symbols)} symbols")

    if not symbols:
        return [], {}, pd.DataFrame()

    print(f"[2/8] Fetching market index bars ({data_start} → {data_end}) ...")
    market_bars = await _fetch_market_bars(args.universe, data_start, data_end)
    print(f"      -> {len(market_bars)} index bars")

    print(f"[3/8] Fetching {args.frequency} bars for {len(symbols)} symbols ...")
    bars_dict = await _fetch_bars_dict(
        symbols, args.frequency, data_start, data_end, args.batch_size
    )
    nonempty = sum(1 for v in bars_dict.values() if v is not None and len(v) > 0)
    print(f"      -> bars for {nonempty}/{len(symbols)} symbols")
    return symbols, bars_dict, market_bars


# ---------------------------------------------------------------------------
# Pipeline stages (sync, after async data load)
# ---------------------------------------------------------------------------
def _detect_all(bars_dict: dict[str, pd.DataFrame],
                cfg: BreakoutResearchConfig) -> list:
    all_events: list = []
    for sym, bars in bars_dict.items():
        if bars is None or len(bars) == 0:
            continue
        try:
            events = detect_breakouts(bars=bars, symbol=sym, config=cfg.detector)
        except Exception as exc:  # pragma: no cover - defensive
            print(f"      ! detector failed for {sym}: {exc}")
            continue
        all_events.extend(events)
    return all_events


def _save_run_artifacts(
    cfg: BreakoutResearchConfig,
    args: argparse.Namespace,
    events_df: pd.DataFrame,
    features_df: pd.DataFrame,
    labels_df: pd.DataFrame,
    train_result,
    valid_metrics: dict[str, float],
) -> Path:
    """Persist config, events, features and an evaluation summary."""
    cfg.config_dir().mkdir(parents=True, exist_ok=True)
    cfg.events_dir().mkdir(parents=True, exist_ok=True)
    cfg.evaluations_dir().mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_id = f"breakout_run_{timestamp}"

    config_path = cfg.config_dir() / f"{run_id}.json"
    config_path.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "args": vars(args),
                "config": _serialize_config(cfg),
                "created_at": timestamp,
            },
            ensure_ascii=False, indent=2, default=str,
        ),
        encoding="utf-8",
    )

    events_path = cfg.events_dir() / f"{run_id}_events.csv"
    if not events_df.empty:
        # Merge labels (future_return, label) and model scores into events
        enriched = events_df.copy()
        if not labels_df.empty and len(labels_df) == len(enriched):
            for col in ["future_return", "label"]:
                if col in labels_df.columns:
                    enriched[col] = labels_df[col].values
        # Add model scores if available
        if not features_df.empty and train_result is not None and train_result.model_path:
            try:
                scorer = BreakoutScorer(train_result.model_path)
                scored = scorer.score(features_df.copy())
                if "score" in scored.columns and len(scored) == len(enriched):
                    enriched["score"] = scored["score"].values
            except Exception as exc:
                print(f"  Warning: could not compute scores for events: {exc}")
        enriched.to_csv(events_path, index=False)

    features_path = cfg.events_dir() / f"{run_id}_features.parquet"
    if not features_df.empty:
        try:
            features_df.to_parquet(features_path, index=False)
        except Exception:
            features_df.to_csv(features_path.with_suffix(".csv"), index=False)

    eval_path = cfg.evaluations_dir() / f"{run_id}_summary.json"
    eval_path.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "model_path": str(train_result.model_path),
                "metrics": {k: (None if pd.isna(v) else float(v))
                            for k, v in train_result.metrics.items()},
                "valid_metrics": {k: (None if pd.isna(v) else float(v))
                                  for k, v in valid_metrics.items()},
                "train_samples": train_result.train_samples,
                "valid_samples": train_result.valid_samples,
                "n_events": int(len(events_df)),
                "n_labeled": int(labels_df["label"].notna().sum())
                if "label" in labels_df.columns else 0,
            },
            ensure_ascii=False, indent=2, default=str,
        ),
        encoding="utf-8",
    )
    return config_path


def _print_metrics_summary(title: str, metrics: dict[str, float]) -> None:
    if not metrics:
        print(f"  {title}: <empty>")
        return
    print(f"  {title}:")
    keys = [
        "n_samples", "spearman_corr", "rmse", "mae",
        "top5_return", "top10_return", "top20_return",
        "top10_winrate", "bottom20_return", "long_short_spread",
        "ic_mean", "icir",
    ]
    for k in keys:
        if k in metrics:
            v = metrics[k]
            if isinstance(v, float):
                print(f"    {k:>20s} = {v:.4f}")
            else:
                print(f"    {k:>20s} = {v}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
async def _run_async(args: argparse.Namespace, cfg: BreakoutResearchConfig) -> None:
    from qlib_research.config import close_fdh

    try:
        symbols, bars_dict, market_bars = await _load_data(args, cfg)
    finally:
        try:
            await close_fdh()
        except Exception:
            pass

    if not symbols or not bars_dict:
        raise RuntimeError(
            "No bars loaded from FinanceDataHub. Check FDH connectivity / sources.yml."
        )

    print(f"[4/8] Detecting breakouts (lookback={cfg.detector.lookback}, "
          f"min_consolidation={cfg.detector.min_consolidation}, "
          f"vol_thr={cfg.detector.volume_threshold}) ...")
    events = _detect_all(bars_dict, cfg)
    print(f"      -> {len(events)} events across {len(bars_dict)} symbols")
    if not events:
        raise RuntimeError("Detector found 0 events; aborting.")

    print(f"[5/8] Labeling events (horizon={cfg.labeler.horizon_days}, "
          f"success_thr={cfg.labeler.success_threshold}) ...")
    labels_df = label_events(events, bars_dict, cfg.labeler)
    n_valid = int(labels_df["future_return"].notna().sum()) if "future_return" in labels_df.columns else 0
    print(f"      -> {len(labels_df)} rows, {n_valid} with valid forward returns")

    print(f"[6/8] Extracting {len(ALL_FEATURE_COLUMNS)} features ...")
    features_df = extract_features_batch(
        events=events,
        bars_dict=bars_dict,
        market_bars=market_bars if not market_bars.empty else None,
        fundamentals_dict=None,
    )
    print(f"      -> features shape {features_df.shape}")

    print(f"[7/8] Training LightGBM (train_end={cfg.trainer.train_end}, "
          f"valid_end={cfg.trainer.valid_end}) ...")
    train_result = train_breakout_model(
        features_df=features_df,
        labels_df=labels_df,
        config=cfg.trainer,
        output_dir=cfg.models_dir(),
    )
    print(f"      -> model saved to {train_result.model_path}")
    print(f"      -> train={train_result.train_samples}, valid={train_result.valid_samples}")

    print("[8/8] Evaluating on validation split ...")
    valid_metrics: dict[str, float] = {}
    if train_result.valid_samples > 0:
        # Re-score with BreakoutScorer for an end-to-end check.
        scorer = BreakoutScorer(train_result.model_path)
        events_df = pd.DataFrame([asdict(e) for e in events])
        labeled = labels_df.copy()
        labeled["__row__"] = range(len(labeled))
        scored = scorer.score(features_df.copy())
        eval_df = pd.concat(
            [labeled.reset_index(drop=True),
             scored[["score"]].reset_index(drop=True)],
            axis=1,
        )
        eval_df = eval_df.rename(columns={"score": "pred"})
        eval_dates = pd.to_datetime(eval_df["date"])
        train_end_ts = pd.Timestamp(cfg.trainer.train_end)
        valid_end_ts = pd.Timestamp(cfg.trainer.valid_end)
        if hasattr(eval_dates.dt, "tz") and eval_dates.dt.tz is not None:
            train_end_ts = train_end_ts.tz_localize(eval_dates.dt.tz)
            valid_end_ts = valid_end_ts.tz_localize(eval_dates.dt.tz)
        valid_only = eval_df[
            (eval_dates > train_end_ts) & (eval_dates <= valid_end_ts)
        ]
        valid_metrics = evaluate_predictions(valid_only)

    _print_metrics_summary("train metrics", train_result.metrics)
    _print_metrics_summary("recomputed valid metrics", valid_metrics)

    config_path = _save_run_artifacts(
        cfg, args,
        events_df=pd.DataFrame([asdict(e) for e in events]),
        features_df=features_df,
        labels_df=labels_df,
        train_result=train_result,
        valid_metrics=valid_metrics,
    )
    print(f"\nRun artifacts saved under: {cfg.artifacts_dir}")
    print(f"  config:  {config_path}")
    print(f"  model:   {train_result.model_path}")


def main() -> None:
    args = parse_args()
    cfg = build_config(args)
    print("=" * 70)
    print("Breakout research pipeline")
    print("=" * 70)
    print(json.dumps({"args": vars(args), "config": _serialize_config(cfg)},
                     ensure_ascii=False, indent=2, default=str))
    if args.dry_run:
        print("\n[dry-run] Exiting without running pipeline.")
        return

    cfg.artifacts_dir.mkdir(parents=True, exist_ok=True)
    cfg.models_dir().mkdir(parents=True, exist_ok=True)

    try:
        asyncio.run(_run_async(args, cfg))
    except ModuleNotFoundError as exc:
        print(f"\nERROR: missing dependency: {exc}", file=sys.stderr)
        print("FinanceDataHub is required for data loading. Install it (or "
              "configure DATA_SOURCE_MODE / sources.yml) before running.",
              file=sys.stderr)
        sys.exit(2)
    except Exception as exc:
        print(f"\nERROR: pipeline failed: {exc}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

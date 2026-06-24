"""Run the stock breakout research workflow from FDH or an OHLCV CSV file."""

from __future__ import annotations

import argparse
import asyncio
import json
import pickle
from pathlib import Path

import pandas as pd

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
    parser.add_argument("--lookback-window", type=int, default=60)
    parser.add_argument("--label-horizon-days", type=int, default=13)
    parser.add_argument("--success-return-pct", type=float, default=0.03)
    parser.add_argument("--min-volume-ratio", type=float, default=1.0)
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


async def async_main() -> None:
    args = parse_args()
    cfg = StockBreakoutConfig(
        lookback_window=args.lookback_window,
        label_horizon_days=args.label_horizon_days,
        success_return_pct=args.success_return_pct,
        min_volume_ratio=args.min_volume_ratio,
    )
    artifacts_dir = Path(args.artifacts_dir or get_qlib_artifacts_dir()).expanduser().resolve()
    model_dir = artifacts_dir / args.model_id
    model_dir.mkdir(parents=True, exist_ok=True)

    price_frame, resolved_symbols, resolved_universe_profile = await _load_price_frame(args)
    research = build_stock_breakout_research_frame(price_frame, cfg)
    feature_columns = stock_breakout_feature_columns(research)
    label_column = f"future_return_{cfg.label_horizon_days}d"

    if research.empty:
        raise SystemExit("No stock breakout events detected.")
    if args.train_end_date:
        train_mask = pd.to_datetime(research["event_date"]) <= pd.to_datetime(args.train_end_date)
        train_frame = research.loc[train_mask].copy()
        score_frame = research.loc[~train_mask].copy()
        if score_frame.empty:
            score_frame = research.copy()
    else:
        train_frame = research.copy()
        score_frame = research.copy()

    model, trained_features = train_lightgbm_stock_breakout_model(train_frame, label_column, feature_columns)
    scored = score_stock_breakout_events(model, score_frame, trained_features)
    signals = build_stock_breakout_signal_frame(scored, model_id=args.model_id, config=cfg)
    metrics = evaluate_stock_breakout_scores(scored, return_column=label_column)

    feature_date = str(pd.to_datetime(scored["event_date"]).max().date())
    latest_scores = (
        signals.sort_values(["signal_date", "signal_score"], ascending=[True, False])
        .groupby("code", as_index=False)
        .tail(1)
        .sort_values("signal_score", ascending=False)
        .reset_index(drop=True)
    )
    latest_scores["qlib_score"] = latest_scores["signal_score"]
    latest_scores["qlib_rank"] = latest_scores.index + 1
    score_output = latest_scores[["code", "qlib_score", "qlib_rank", f"pred_return_{cfg.label_horizon_days}d", "feature_date"]].rename(
        columns={f"pred_return_{cfg.label_horizon_days}d": "pred_return"}
    )

    research.to_csv(model_dir / "feature_panel.csv", index=False)
    scored.to_csv(model_dir / "events_scored.csv", index=False)
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
            "label_policy": {
                "horizon_days": cfg.label_horizon_days,
                "success_return_pct": cfg.success_return_pct,
                "target": label_column,
            },
            "feature_policy": {"feature_columns": trained_features},
            "source_data": {
                "input_source": args.input_source,
                "fdh_dataset": args.fdh_dataset if args.input_source == "fdh" else None,
                "symbols": resolved_symbols if args.input_source == "fdh" else None,
                "symbol_count": len(resolved_symbols) if args.input_source == "fdh" else None,
                "universe_profile": resolved_universe_profile,
                "universe_mode": args.universe_mode if resolved_universe_profile else None,
                "start_date": args.start_date,
                "end_date": args.end_date,
                "adjust": args.adjust if args.input_source == "fdh" else None,
            },
            "signal_path": "signals.csv",
            "metrics_path": "metrics.json",
            "feature_panel_path": "feature_panel.csv",
            "model_path": "model.pkl",
        },
    )
    publish_strategy_signals(signals, model_id=args.model_id, artifacts_dir=artifacts_dir)
    print(json.dumps({"model_id": args.model_id, "feature_date": feature_date, "metrics": metrics}, ensure_ascii=False, default=str))


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()

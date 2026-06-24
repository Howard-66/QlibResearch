"""Score today's breakout candidates using the latest trained model."""

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

from qlib_research.breakout.config import DetectorConfig  # noqa: E402
from qlib_research.breakout.detector import detect_breakouts  # noqa: E402
from qlib_research.breakout.features import extract_features_batch  # noqa: E402
from qlib_research.breakout.scorer import BreakoutScorer  # noqa: E402
from qlib_research.io.artifacts import (  # noqa: E402
    BREAKOUT_MANIFEST_FILE,
    BREAKOUT_SIGNALS_FILE,
    get_breakout_artifacts_dir,
    get_breakout_signals_dir,
    get_latest_breakout_model,
)

_INDEX_CODE_MAP = {
    "csi300": "000300.SH",
    "csi500": "000905.SH",
    "all": "000300.SH",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Score breakout candidates")
    parser.add_argument("--model", default=None,
                        help="Model file path (default: latest in artifacts/breakout/models/)")
    parser.add_argument("--date", default=None,
                        help="Target date YYYY-MM-DD (default: latest available)")
    parser.add_argument("--frequency", choices=["daily", "weekly"], default="daily")
    parser.add_argument("--universe", default="csi300", help="csi300 | csi500 | all")
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--lookback", type=int, default=120)
    parser.add_argument("--min-consolidation", type=int, default=10)
    parser.add_argument("--volume-threshold", type=float, default=1.5)
    parser.add_argument("--data-window-days", type=int, default=400,
                        help="History window for detection (calendar days)")
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--max-symbols", type=int, default=None)
    parser.add_argument("--output", default=None,
                        help="Output CSV path (default: artifacts/breakout/signals/scores_breakout.csv)")
    return parser.parse_args()


def resolve_model_path(arg_path: str | None) -> Path:
    if arg_path:
        path = Path(arg_path).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"Model not found: {path}")
        return path
    latest = get_latest_breakout_model()
    if latest is None:
        raise FileNotFoundError(
            "No breakout model found. Train one via "
            "`python scripts/run_breakout_research.py` or pass --model."
        )
    return latest


def load_model_metadata(model_path: Path) -> dict[str, Any]:
    """Load the sibling ``*.json`` metadata for a model, if present."""
    meta_path = model_path.with_suffix(".json")
    if not meta_path.exists():
        return {}
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
async def _resolve_symbols(universe: str, end_date: str | None,
                           max_symbols: int | None) -> list[str]:
    from qlib_research.core.research_universe import resolve_universe_symbols

    profile = "merged_csi300_500" if universe == "all" else universe
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
    return df[keep].sort_values("date").reset_index(drop=True)


async def _fetch_bars_dict(
    symbols: list[str],
    frequency: str,
    start: str,
    end: str,
    batch_size: int,
) -> dict[str, pd.DataFrame]:
    from qlib_research.config import get_fdh

    fdh = await get_fdh()
    method = (
        fdh.get_processed_daily_async if frequency == "daily"
        else fdh.get_processed_weekly_async
    )
    bars_dict: dict[str, pd.DataFrame] = {}
    total = len(symbols)
    for i in range(0, total, batch_size):
        batch = symbols[i : i + batch_size]
        print(f"  Fetching {frequency} bars {i // batch_size + 1} "
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


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
async def _load(args: argparse.Namespace, target_date: pd.Timestamp
               ) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    from qlib_research.config import close_fdh

    window = max(int(args.data_window_days), int(args.lookback) * 2)
    data_start = (target_date - pd.Timedelta(days=window)).strftime("%Y-%m-%d")
    data_end = target_date.strftime("%Y-%m-%d")

    try:
        print(f"[1/5] Resolving universe ({args.universe}) ...")
        symbols = await _resolve_symbols(
            args.universe, data_end, args.max_symbols
        )
        print(f"      -> {len(symbols)} symbols")
        if not symbols:
            return {}, pd.DataFrame()

        print(f"[2/5] Fetching market index bars ({data_start} → {data_end}) ...")
        market_bars = await _fetch_market_bars(args.universe, data_start, data_end)

        print(f"[3/5] Fetching {args.frequency} bars ...")
        bars_dict = await _fetch_bars_dict(
            symbols, args.frequency, data_start, data_end, args.batch_size,
        )
        return bars_dict, market_bars
    finally:
        try:
            await close_fdh()
        except Exception:
            pass


def _detect_for_target(
    bars_dict: dict[str, pd.DataFrame],
    detector_cfg: DetectorConfig,
    target_date: pd.Timestamp,
) -> list:
    """Detect events anywhere in the window, then keep only target_date events."""
    keep: list = []
    target_norm = pd.Timestamp(target_date).normalize()
    for sym, bars in bars_dict.items():
        if bars is None or len(bars) == 0:
            continue
        try:
            events = detect_breakouts(bars=bars, symbol=sym, config=detector_cfg)
        except Exception as exc:  # pragma: no cover - defensive
            print(f"      ! detector failed for {sym}: {exc}")
            continue
        for ev in events:
            if pd.Timestamp(ev.date).normalize() == target_norm:
                keep.append(ev)
    return keep


def _events_to_dates(events: list) -> set[pd.Timestamp]:
    return {pd.Timestamp(e.date).normalize() for e in events}


def _resolve_target_date(
    args_date: str | None,
    bars_dict: dict[str, pd.DataFrame],
) -> pd.Timestamp:
    if args_date:
        return pd.Timestamp(args_date).normalize()
    # Pick the latest date seen in any symbol's bars.
    latest: pd.Timestamp | None = None
    for bars in bars_dict.values():
        if bars is None or len(bars) == 0 or "date" not in bars.columns:
            continue
        cand = pd.Timestamp(bars["date"].max()).normalize()
        if latest is None or cand > latest:
            latest = cand
    return latest if latest is not None else pd.Timestamp(datetime.now().date())


def _build_signal_frame(events: list, scored: pd.DataFrame,
                       frequency: str) -> pd.DataFrame:
    if not events or scored.empty:
        return pd.DataFrame(
            columns=["date", "symbol", "score", "breakout_price",
                     "prev_high_date", "prev_high_price",
                     "volume_ratio", "frequency"]
        )
    events_df = pd.DataFrame([asdict(e) for e in events])
    # ``scored`` is sorted by score desc; join via (symbol, date) so order is
    # preserved.
    events_df["date"] = pd.to_datetime(events_df["date"]).dt.normalize()
    scored = scored.copy()
    # extract_features_batch preserves event order, so we rely on positional
    # alignment with ``events``. Attach the meta columns back.
    if len(scored) != len(events):
        # Fallback: drop scoring and emit empty.
        return pd.DataFrame()
    meta = events_df.reset_index(drop=True)
    out = pd.concat([meta, scored[["score"]].reset_index(drop=True)], axis=1)
    out = out.sort_values("score", ascending=False).reset_index(drop=True)
    out["frequency"] = frequency
    cols = ["date", "symbol", "score", "breakout_price",
            "prev_high_date", "prev_high_price",
            "volume_ratio", "frequency"]
    return out[cols]


def _write_outputs(
    signals: pd.DataFrame,
    args: argparse.Namespace,
    model_path: Path,
    model_meta: dict[str, Any],
    target_date: pd.Timestamp,
) -> tuple[Path, Path]:
    if args.output:
        signals_path = Path(args.output).expanduser().resolve()
    else:
        signals_path = get_breakout_signals_dir() / BREAKOUT_SIGNALS_FILE
    signals_path.parent.mkdir(parents=True, exist_ok=True)
    signals.to_csv(signals_path, index=False)

    manifest_path = signals_path.parent / BREAKOUT_MANIFEST_FILE
    manifest = {
        "model_id": model_path.stem,
        "model_path": str(model_path),
        "signals_path": signals_path.name,
        "target_date": target_date.strftime("%Y-%m-%d"),
        "frequency": args.frequency,
        "universe": args.universe,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "signal_count": int(len(signals)),
        "top_k": int(args.top_k),
        "model_metrics": (model_meta.get("metrics") if isinstance(model_meta, dict)
                          else None),
        "detector": {
            "lookback": int(args.lookback),
            "min_consolidation": int(args.min_consolidation),
            "volume_threshold": float(args.volume_threshold),
            "frequency": args.frequency,
        },
    }
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    return signals_path, manifest_path


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    args = parse_args()
    print("=" * 70)
    print("Breakout candidate scoring")
    print("=" * 70)

    try:
        model_path = resolve_model_path(args.model)
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(2)
    print(f"Using model: {model_path}")
    model_meta = load_model_metadata(model_path)

    detector_cfg = DetectorConfig(
        lookback=int(args.lookback),
        min_consolidation=int(args.min_consolidation),
        frequency=args.frequency,
        volume_threshold=float(args.volume_threshold),
    )

    # Determine the temporal anchor: prefer user-provided date, else fetch with
    # today and let _resolve_target_date pick the latest bar afterwards.
    anchor_date = pd.Timestamp(args.date).normalize() if args.date else pd.Timestamp(
        datetime.now().date()
    )

    try:
        bars_dict, market_bars = asyncio.run(_load(args, anchor_date))
    except ModuleNotFoundError as exc:
        print(f"\nERROR: missing dependency: {exc}", file=sys.stderr)
        print("FinanceDataHub is required for data loading.", file=sys.stderr)
        sys.exit(2)
    except Exception as exc:
        print(f"\nERROR: data loading failed: {exc}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)

    if not bars_dict:
        print("ERROR: no bars loaded; aborting.", file=sys.stderr)
        sys.exit(1)

    target_date = _resolve_target_date(args.date, bars_dict)
    print(f"Target date: {target_date.strftime('%Y-%m-%d')}")

    print(f"[4/5] Detecting breakouts at {target_date.date()} ...")
    events = _detect_for_target(bars_dict, detector_cfg, target_date)
    print(f"      -> {len(events)} candidates")

    if not events:
        # Still emit empty outputs so downstream consumers see a fresh manifest.
        empty = pd.DataFrame(
            columns=["date", "symbol", "score", "breakout_price",
                     "prev_high_date", "prev_high_price",
                     "volume_ratio", "frequency"]
        )
        signals_path, manifest_path = _write_outputs(
            empty, args, model_path, model_meta, target_date,
        )
        print(f"\n[empty] signals: {signals_path}")
        print(f"[empty] manifest: {manifest_path}")
        return

    print("[5/5] Extracting features and scoring ...")
    features_df = extract_features_batch(
        events=events,
        bars_dict=bars_dict,
        market_bars=market_bars if not market_bars.empty else None,
        fundamentals_dict=None,
    )
    scorer = BreakoutScorer(model_path)
    # Score: we lose positional order because BreakoutScorer sorts by score.
    # Pass an explicit index column we can use to realign.
    features_with_idx = features_df.copy()
    features_with_idx["__event_idx__"] = range(len(features_with_idx))
    scored = scorer.score(features_with_idx)
    # Re-align scored rows to the original event order.
    scored = scored.sort_values("__event_idx__").reset_index(drop=True)

    signals = _build_signal_frame(events, scored, args.frequency)
    if args.top_k and args.top_k > 0:
        top = signals.head(int(args.top_k))
    else:
        top = signals

    print("\nTop candidates:")
    if top.empty:
        print("  <none>")
    else:
        for _, row in top.iterrows():
            print(f"  {row['symbol']:>12s}  score={row['score']:+.4f}  "
                  f"breakout={row['breakout_price']:.2f}  "
                  f"prev_high_date={pd.Timestamp(row['prev_high_date']).date()}  "
                  f"vol_ratio={row['volume_ratio']:.2f}")

    signals_path, manifest_path = _write_outputs(
        signals, args, model_path, model_meta, target_date,
    )
    print(f"\nWrote {len(signals)} signals to: {signals_path}")
    print(f"Manifest: {manifest_path}")
    _ = get_breakout_artifacts_dir  # silence unused-import warning


if __name__ == "__main__":
    main()

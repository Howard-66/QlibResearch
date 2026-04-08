"""
Offline qlib artifact helpers.

These helpers keep online serving simple: training publishes a score snapshot,
and the screener only consumes the latest published scores.
"""

from __future__ import annotations

from contextlib import contextmanager, redirect_stderr, redirect_stdout
from fnmatch import fnmatchcase
import importlib.resources as importlib_resources
import io
import logging
import os
import json
from pathlib import Path
import sys
from typing import Optional, Sequence
import types
import warnings

import numpy as np
import pandas as pd

from qlib_research.config import get_project_root, get_qlib_artifacts_dir


@contextmanager
def suppress_external_output(enabled: bool = True):
    """Mute noisy third-party runtime output while keeping exceptions intact."""
    if not enabled:
        yield
        return

    sink = io.StringIO()
    logger_names = ("qlib", "mlflow", "lightgbm")
    logger_states = {
        name: logging.getLogger(name).level
        for name in logger_names
    }
    try:
        for name in logger_names:
            logging.getLogger(name).setLevel(logging.ERROR)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with redirect_stdout(sink), redirect_stderr(sink):
                yield
    finally:
        for name, level in logger_states.items():
            logging.getLogger(name).setLevel(level)


def require_qlib():
    """Import qlib lazily so the main API can run without the dependency."""
    mpl_dir = get_qlib_artifacts_dir() / ".mplconfig"
    mpl_dir.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("MPLCONFIGDIR", str(mpl_dir))
    try:
        import qlib  # type: ignore
    except ImportError as exc:  # pragma: no cover - depends on local environment
        raise RuntimeError(
            "pyqlib is not installed in the project environment. "
            "Install pyqlib>=0.9.7,<0.10 before running qlib training."
        ) from exc
    return qlib


def _ensure_mlflow_legacy_compatibility() -> None:
    """Install narrow shims needed by the older MLflow version bundled with qlib."""
    try:
        from google.protobuf import service as _service  # type: ignore  # noqa: F401
    except ImportError:
        service_module = types.ModuleType("google.protobuf.service")

        class Service:  # pragma: no cover - behavior exercised through qlib.init
            pass

        class RpcController:  # pragma: no cover - behavior exercised through qlib.init
            pass

        class RpcChannel:  # pragma: no cover - behavior exercised through qlib.init
            pass

        class ServiceException(Exception):  # pragma: no cover - compatibility shim
            pass

        service_module.Service = Service
        service_module.RpcController = RpcController
        service_module.RpcChannel = RpcChannel
        service_module.ServiceException = ServiceException
        sys.modules["google.protobuf.service"] = service_module

    try:
        import pkg_resources  # type: ignore  # noqa: F401
    except ImportError:
        pkg_resources = types.ModuleType("pkg_resources")

        def resource_filename(package_or_requirement: str, resource_name: str) -> str:
            package = str(package_or_requirement)
            return str(importlib_resources.files(package).joinpath(resource_name))

        pkg_resources.resource_filename = resource_filename
        sys.modules["pkg_resources"] = pkg_resources


def load_panel_dataframe(panel_path: str | Path) -> pd.DataFrame:
    """Load a previously exported feature panel."""
    path = Path(panel_path).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"Feature panel not found: {path}")

    suffix = path.suffix.lower()
    if suffix == ".csv":
        frame = pd.read_csv(path, low_memory=False)
    elif suffix in {".parquet", ".pq"}:
        frame = pd.read_parquet(path)
    else:
        raise ValueError(f"Unsupported feature panel format: {path.suffix}")

    if "datetime" in frame.columns:
        frame["datetime"] = pd.to_datetime(frame["datetime"])
    elif "time" in frame.columns:
        frame["datetime"] = pd.to_datetime(frame["time"])
    else:
        raise ValueError("Feature panel must contain either 'datetime' or 'time'")

    if "instrument" not in frame.columns:
        instrument_col = "symbol" if "symbol" in frame.columns else "code"
        frame["instrument"] = frame[instrument_col]

    return frame


def publish_score_snapshot(
    score_frame: pd.DataFrame,
    model_id: str,
    feature_date: str,
    artifacts_dir: Optional[str | Path] = None,
    extra_manifest: Optional[dict] = None,
) -> Path:
    """
    Publish a score frame so the API can consume it.

    Required columns: code/symbol/instrument and qlib_score/score.
    """
    root = Path(artifacts_dir or get_qlib_artifacts_dir()).expanduser().resolve()
    model_dir = root / model_id
    model_dir.mkdir(parents=True, exist_ok=True)

    snapshot_path = model_dir / "scores.csv"
    manifest_path = model_dir / "manifest.json"
    latest_path = root / "latest_model.json"

    score_frame.to_csv(snapshot_path, index=False)

    manifest = {
        "model_id": model_id,
        "feature_date": feature_date,
        "generated_at": pd.Timestamp.utcnow().isoformat(),
        "snapshot_path": snapshot_path.name,
    }
    if extra_manifest:
        manifest.update(extra_manifest)
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    latest_path.write_text(json.dumps({
        **manifest,
        "snapshot_path": str(snapshot_path.relative_to(root)),
    }, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return snapshot_path


TECHNICAL_CORE_COLUMNS = (
    "signal_strength",
    "signal_grade_num",
    "ma20",
    "ma50",
    "dif",
    "dea",
    "macd_hist",
    "rsi",
    "atr",
    "volume",
    "amount",
)

TECHNICAL_FLOW_COLUMNS = (
    "nda_value",
    "volume_confirmed",
)

VALUATION_ABSOLUTE_COLUMNS = (
    "pe_ttm",
    "pb",
    "ps_ttm",
    "dv_ttm",
    "peg",
)

VALUATION_PERCENTILE_COLUMNS = (
    "pe_ttm_pct_1250d",
    "pb_pct_1250d",
    "ps_ttm_pct_1250d",
)

INDUSTRY_VALUATION_CONTEXT_COLUMNS = (
    "core_indicator_pct_1250d",
    "core_indicator_industry_pct",
)

QUALITY_SUMMARY_COLUMNS = (
    "f_score",
    "roe_5y_avg",
    "ni_cfo_corr_3y",
    "debt_ratio",
    "current_ratio",
)

FSCORE_COMPONENT_COLUMNS = (
    "f_roa",
    "f_cfo",
    "f_delta_roa",
    "f_accrual",
    "f_delta_lever",
    "f_delta_liquid",
    "f_eq_offer",
    "f_delta_margin",
    "f_delta_turn",
)

TTM_PROFITABILITY_COLUMNS = (
    "roa_ttm",
    "cfo_ttm",
    "ni_ttm",
    "gpm_ttm",
    "at_ttm",
)

BUFFETT_MOAT_COLUMNS = (
    "npm_ttm",
    "gpm_ttm_12q_std",
    "gpm_ttm_12q_delta",
    "npm_ttm_12q_std",
    "cfo_to_ni_ttm",
    "buffett_gpm_flag",
    "buffett_npm_stable_flag",
    "buffett_roa_flag",
    "buffett_cashflow_flag",
    "industry_npm_ttm_rank_pct",
    "buffett_npm_flag",
    "buffett_moat_coverage",
    "buffett_moat_score",
    "buffett_moat_pass",
)

MACRO_CYCLE_NUMERIC_COLUMNS = (
    "macro_m2_yoy",
    "macro_gdp_yoy",
    "macro_ppi_yoy",
    "macro_pmi",
    "macro_credit_impulse",
    "macro_phase_y",
)

MACRO_CYCLE_FLAG_COLUMNS = (
    "macro_phase_changed",
    "macro_phase_reflation",
    "macro_phase_recovery",
    "macro_phase_overheat",
    "macro_phase_stagflation",
    "macro_industry_match",
)

DERIVED_MOMENTUM_COLUMNS = (
    "mom_1w",
    "mom_2w",
    "mom_4w",
    "mom_8w",
    "rev_1w",
    "rev_2w",
    "rev_4w",
    "rev_8w",
)

DERIVED_VOLATILITY_COLUMNS = (
    "volatility_4w",
    "volatility_8w",
    "downside_volatility_8w",
)

DERIVED_LIQUIDITY_COLUMNS = (
    "amount_change_4w",
    "volume_change_4w",
    "amount_zscore_4w",
)

DERIVED_VALUATION_DELTA_COLUMNS = (
    "pe_ttm_delta_4w",
    "pb_delta_4w",
    "ps_ttm_delta_4w",
    "pe_ttm_pct_1250d_delta_4w",
)

DERIVED_QUALITY_DELTA_COLUMNS = (
    "f_score_delta_8w",
    "roa_ttm_delta_8w",
    "current_ratio_delta_8w",
)

INDUSTRY_RELATIVE_COLUMNS = (
    "industry_mom_4w_rank_pct",
    "industry_pe_ttm_pct_rank_pct",
    "industry_f_score_rank_pct",
)

MACRO_INTERACTION_COLUMNS = (
    "macro_reflation_x_mom_4w",
    "macro_recovery_x_mom_8w",
    "macro_overheat_x_volatility_8w",
    "macro_stagflation_x_f_score",
    "macro_industry_match_x_mom_4w",
)

FEATURE_GROUP_COLUMNS = {
    "technical_core": TECHNICAL_CORE_COLUMNS,
    "technical_flow": TECHNICAL_FLOW_COLUMNS,
    "valuation_absolute": VALUATION_ABSOLUTE_COLUMNS,
    "valuation_percentile": VALUATION_PERCENTILE_COLUMNS,
    "industry_valuation_context": INDUSTRY_VALUATION_CONTEXT_COLUMNS,
    "quality_summary": QUALITY_SUMMARY_COLUMNS,
    "fscore_components": FSCORE_COMPONENT_COLUMNS,
    "ttm_profitability": TTM_PROFITABILITY_COLUMNS,
    "buffett_moat": BUFFETT_MOAT_COLUMNS,
    "macro_cycle_numeric": MACRO_CYCLE_NUMERIC_COLUMNS,
    "macro_cycle_flags": MACRO_CYCLE_FLAG_COLUMNS,
    "derived_momentum": DERIVED_MOMENTUM_COLUMNS,
    "derived_volatility": DERIVED_VOLATILITY_COLUMNS,
    "derived_liquidity": DERIVED_LIQUIDITY_COLUMNS,
    "derived_valuation_delta": DERIVED_VALUATION_DELTA_COLUMNS,
    "derived_quality_delta": DERIVED_QUALITY_DELTA_COLUMNS,
    "industry_relative": INDUSTRY_RELATIVE_COLUMNS,
    "macro_interactions": MACRO_INTERACTION_COLUMNS,
}

FULL_POSTFIX_BASELINE_FEATURE_COLUMNS = tuple(
    dict.fromkeys(
        sum((list(columns) for columns in FEATURE_GROUP_COLUMNS.values()), [])
    )
)

BASELINE_FEATURE_COLUMNS = tuple(
    dict.fromkeys(
        list(TECHNICAL_CORE_COLUMNS)
        + list(VALUATION_ABSOLUTE_COLUMNS)
        + list(INDUSTRY_VALUATION_CONTEXT_COLUMNS)
    )
)

FEATURE_COLUMNS = FULL_POSTFIX_BASELINE_FEATURE_COLUMNS

INDUSTRY_NORMALIZED_GROUPS = {
    "valuation_absolute",
    "valuation_percentile",
    "industry_valuation_context",
    "quality_summary",
    "fscore_components",
    "ttm_profitability",
    "derived_valuation_delta",
    "derived_quality_delta",
}
LABEL_COLUMN = "label_excess_return_4w"


def compose_feature_columns(group_names: Sequence[str]) -> tuple[str, ...]:
    columns: list[str] = []
    for group_name in group_names:
        columns.extend(FEATURE_GROUP_COLUMNS.get(group_name, ()))
    return tuple(dict.fromkeys(columns))


def normalize_feature_name_list(values: Sequence[str] | str | None) -> tuple[str, ...]:
    if values is None:
        return tuple()

    if isinstance(values, str):
        raw_values = values.split(",")
    else:
        raw_values = []
        for value in values:
            if value is None:
                continue
            raw_values.extend(str(value).split(","))

    normalized: list[str] = []
    for value in raw_values:
        feature_name = str(value).strip()
        if feature_name and feature_name not in normalized:
            normalized.append(feature_name)
    return tuple(normalized)


def _resolve_feature_matchers(
    feature_columns: Sequence[str],
    feature_matchers: Sequence[str] | str | None = None,
) -> tuple[str, ...]:
    matchers = normalize_feature_name_list(feature_matchers)
    if not matchers:
        return tuple()

    unique_columns = tuple(dict.fromkeys(str(column) for column in feature_columns))
    matched_columns: list[str] = []
    for matcher in matchers:
        is_pattern = any(token in matcher for token in ("*", "?", "["))
        for column in unique_columns:
            if (is_pattern and fnmatchcase(column, matcher)) or (not is_pattern and column == matcher):
                if column not in matched_columns:
                    matched_columns.append(column)
    return tuple(matched_columns)


def include_feature_columns(
    feature_columns: Sequence[str],
    included_features: Sequence[str] | str | None = None,
) -> tuple[str, ...]:
    selected = tuple(dict.fromkeys(str(column) for column in feature_columns))
    if not normalize_feature_name_list(included_features):
        return selected
    included = set(_resolve_feature_matchers(selected, included_features))
    return tuple(column for column in selected if column in included)


def exclude_feature_columns(
    feature_columns: Sequence[str],
    excluded_features: Sequence[str] | str | None = None,
) -> tuple[str, ...]:
    excluded = set(_resolve_feature_matchers(feature_columns, excluded_features))
    return tuple(
        column for column in dict.fromkeys(str(column) for column in feature_columns)
        if column not in excluded
    )


def resolve_feature_columns(
    feature_columns: Optional[Sequence[str]] = None,
    included_features: Sequence[str] | str | None = None,
    excluded_features: Sequence[str] | str | None = None,
    default_features: Optional[Sequence[str]] = None,
) -> tuple[str, ...]:
    base_features = default_features or FEATURE_COLUMNS
    selected = tuple(base_features) if feature_columns is None else tuple(feature_columns)
    included = include_feature_columns(selected, included_features=included_features)
    return exclude_feature_columns(included, excluded_features=excluded_features)


def get_normalized_feature_candidates(feature_columns: Sequence[str]) -> list[str]:
    normalized_candidates = {
        column
        for group_name, columns in FEATURE_GROUP_COLUMNS.items()
        if group_name in INDUSTRY_NORMALIZED_GROUPS
        for column in columns
    }
    return [column for column in feature_columns if column in normalized_candidates]


def _robust_scale_frame(
    frame: pd.DataFrame,
    column: str,
    industry_column: str,
    min_group_size: int,
    datetime_values: pd.Series | pd.DatetimeIndex | None = None,
    industry_values: pd.Series | None = None,
    has_industry: pd.Series | None = None,
) -> pd.Series:
    values = pd.to_numeric(frame[column], errors="coerce")
    if values.notna().sum() == 0:
        return values

    datetime_values = pd.Series(
        pd.to_datetime(frame["datetime"]) if datetime_values is None else pd.to_datetime(datetime_values),
        index=frame.index,
    )
    industry_values = pd.Series(
        frame[industry_column].fillna("__MISSING__").astype(str) if industry_values is None else industry_values,
        index=frame.index,
    )
    has_industry = pd.Series(
        frame[industry_column].notna() if has_industry is None else has_industry,
        index=frame.index,
    )

    def _build_group_stats(group_keys: list[pd.Series], include_count: bool = False) -> pd.DataFrame:
        quantiles = (
            values.groupby(group_keys, sort=False)
            .quantile([0.25, 0.5, 0.75])
            .unstack(level=-1)
            .rename(columns={0.25: "q1", 0.5: "median", 0.75: "q3"})
        )
        if include_count:
            quantiles["count"] = values.groupby(group_keys, sort=False).count()
        return quantiles

    stats_frame = pd.DataFrame(
        {
            "__datetime": datetime_values,
            "__industry": industry_values,
            "__has_industry": has_industry.astype(bool),
        },
        index=frame.index,
    )

    market_stats = _build_group_stats([datetime_values]).rename(
        columns=lambda name: f"market_{name}",
    )
    stats_frame = stats_frame.join(market_stats, on="__datetime")

    industry_stats = _build_group_stats([datetime_values, industry_values], include_count=True).rename(
        columns=lambda name: f"industry_{name}",
    )
    stats_frame = stats_frame.join(industry_stats, on=["__datetime", "__industry"])

    market_iqr = (stats_frame["market_q3"] - stats_frame["market_q1"]).replace(0.0, np.nan)
    industry_iqr = (stats_frame["industry_q3"] - stats_frame["industry_q1"]).replace(0.0, np.nan)

    use_industry = (
        stats_frame["__has_industry"]
        & (stats_frame["industry_count"] >= min_group_size)
        & industry_iqr.notna()
    )
    center = stats_frame["market_median"].where(~use_industry, stats_frame["industry_median"])
    scale = market_iqr.where(~use_industry, industry_iqr)
    scaled = (values - center) / scale
    return scaled.replace([np.inf, -np.inf], np.nan)


def apply_industry_normalization(
    panel: pd.DataFrame,
    feature_columns: Sequence[str],
    method: str = "none",
    industry_column: str = "l1_name",
    min_group_size: int = 5,
) -> pd.DataFrame:
    """
    Normalize valuation/quality features within each weekly industry cross-section.
    """
    if method == "none" or panel.empty or industry_column not in panel.columns:
        return panel.copy()
    if method != "l1_weekly_robust":
        raise ValueError(f"Unsupported industry normalization method: {method}")

    result = panel.copy()
    result["datetime"] = pd.to_datetime(result["datetime"])
    datetime_values = pd.Series(result["datetime"], index=result.index)
    has_industry = pd.Series(result[industry_column].notna(), index=result.index)
    industry_values = pd.Series(result[industry_column].fillna("__MISSING__").astype(str), index=result.index)
    for column in get_normalized_feature_candidates(feature_columns):
        if column not in result.columns:
            continue
        result[column] = _robust_scale_frame(
            result,
            column=column,
            industry_column=industry_column,
            min_group_size=min_group_size,
            datetime_values=datetime_values,
            industry_values=industry_values,
            has_industry=has_industry,
        )
    return result


def build_training_frame(
    panel: pd.DataFrame,
    feature_columns: Optional[Sequence[str]] = None,
    label_column: str = LABEL_COLUMN,
) -> tuple[pd.DataFrame, list[str]]:
    """Convert a feature panel into qlib's feature/label dataframe layout."""
    frame = panel.copy()
    frame["datetime"] = pd.to_datetime(frame["datetime"])
    frame["instrument"] = frame["instrument"].astype(str)
    frame["signal_grade_num"] = frame.get("signal_grade", pd.Series(index=frame.index)).map({
        "F": 0,
        "D": 1,
        "C": 2,
        "B": 3,
        "A": 4,
    }).fillna(0)

    candidates = list(FEATURE_COLUMNS if feature_columns is None else feature_columns)
    available = [
        col for col in candidates
        if col in frame.columns
        and (
            pd.api.types.is_numeric_dtype(frame[col])
            or pd.api.types.is_bool_dtype(frame[col])
        )
    ]
    if not available:
        raise ValueError("No numeric feature columns available for qlib training")
    if label_column not in frame.columns:
        raise ValueError(f"Required label column missing: {label_column}")

    qlib_frame = frame[["datetime", "instrument", *available, label_column]].copy()
    feature_frame = qlib_frame[available].apply(pd.to_numeric, errors="coerce")
    valid_rows = feature_frame.notna().any(axis=1)
    if not valid_rows.any():
        raise ValueError("No usable feature rows available for qlib training")
    feature_frame = feature_frame.loc[valid_rows]
    label_series = pd.to_numeric(qlib_frame.loc[valid_rows, label_column], errors="coerce")

    for column in available:
        median = feature_frame[column].median()
        fill_value = 0.0 if pd.isna(median) else float(median)
        feature_frame[column] = feature_frame[column].fillna(fill_value)

    qlib_frame = qlib_frame.loc[valid_rows, ["datetime", "instrument"]].copy()
    qlib_frame[available] = feature_frame
    qlib_frame[label_column] = label_series
    qlib_frame = qlib_frame.set_index(["datetime", "instrument"]).sort_index()

    feature_df = qlib_frame[available]
    label_df = qlib_frame[[label_column]].rename(columns={label_column: "LABEL0"})
    merged = pd.concat({"feature": feature_df, "label": label_df}, axis=1)
    return merged, available


def infer_feature_date(panel: pd.DataFrame) -> pd.Timestamp:
    """Infer the feature date to score if one is not provided explicitly."""
    if panel.empty:
        raise ValueError("Feature panel is empty")
    return pd.to_datetime(panel["datetime"]).max()


def init_qlib_runtime(
    exp_name: str,
    artifacts_dir: Optional[str | Path] = None,
    skip_if_registered: bool = True,
    provider_uri: Optional[str | Path | dict[str, str | Path]] = None,
):
    """Initialize qlib with a local MLflow tracking path under artifacts."""
    _ensure_mlflow_legacy_compatibility()
    qlib = require_qlib()
    artifacts_root = Path(artifacts_dir or get_qlib_artifacts_dir()).expanduser().resolve()
    mlruns_dir = artifacts_root / "mlruns"
    mlruns_dir.mkdir(parents=True, exist_ok=True)
    resolved_provider_uri: str | dict[str, str]
    if provider_uri is None:
        resolved_provider_uri = str(get_project_root())
    elif isinstance(provider_uri, dict):
        resolved_provider_uri = {
            str(freq): str(Path(path).expanduser().resolve())
            for freq, path in provider_uri.items()
        }
    else:
        resolved_provider_uri = str(Path(provider_uri).expanduser().resolve())
    with suppress_external_output():
        qlib.init(
            provider_uri=resolved_provider_uri,
            region="cn",
            exp_manager={
                "class": "MLflowExpManager",
                "module_path": "qlib.workflow.expm",
                "kwargs": {
                    "uri": mlruns_dir.resolve().as_uri(),
                    "default_exp_name": exp_name,
                },
            },
            skip_if_reg=skip_if_registered,
        )
    return qlib

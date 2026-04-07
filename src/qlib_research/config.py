"""Configuration helpers for the standalone QlibResearch project."""

from __future__ import annotations

import logging
import os
from enum import Enum
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class DataSourceMode(str, Enum):
    TUSHARE = "tushare"
    FDH = "fdh"


DATA_SOURCE_MODE = DataSourceMode(os.getenv("DATA_SOURCE_MODE", "fdh").lower())

_fdh_instance = None


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def get_qlib_artifacts_dir() -> Path:
    configured = os.getenv("QLIB_ARTIFACTS_DIR")
    if configured:
        return (get_project_root() / configured).resolve() if not Path(configured).is_absolute() else Path(configured).resolve()
    return (get_project_root() / "artifacts").resolve()


def get_sources_config_path() -> Path:
    configured = os.getenv("SOURCES_CONFIG_PATH")
    if configured:
        return (get_project_root() / configured).resolve() if not Path(configured).is_absolute() else Path(configured).resolve()
    return (get_project_root() / "sources.yml").resolve()


def get_valueinvesting_root() -> Path:
    configured = os.getenv("VALUEINVESTING_ROOT")
    if configured:
        return (get_project_root() / configured).resolve() if not Path(configured).is_absolute() else Path(configured).resolve()
    return (get_project_root().parent / "ValueInvesting").resolve()


def get_valueinvesting_artifacts_dir() -> Path:
    return (get_valueinvesting_root() / "data" / "qlib_artifacts").resolve()


async def get_fdh():
    global _fdh_instance
    if _fdh_instance is None:
        from finance_data_hub import FinanceDataHub
        from finance_data_hub.config import get_settings

        settings = get_settings()
        _fdh_instance = FinanceDataHub(
            settings=settings,
            backend="postgresql",
            router_config_path=str(get_sources_config_path()),
        )
        await _fdh_instance.initialize()
        logger.info("FinanceDataHub SDK initialized for QlibResearch")
    return _fdh_instance


async def close_fdh():
    global _fdh_instance
    if _fdh_instance is not None:
        await _fdh_instance.close()
        _fdh_instance = None
        logger.info("FinanceDataHub SDK closed for QlibResearch")

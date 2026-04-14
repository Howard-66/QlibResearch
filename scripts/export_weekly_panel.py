"""
Export a weekly FDH feature panel for qlib-style offline research.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from qlib_research.core.notebook_workflow import export_or_load_panel


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export weekly feature panel from FinanceDataHub")
    parser.add_argument("--output", default="artifacts/panels/weekly_features.parquet")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--symbols", nargs="*", default=None)
    parser.add_argument("--universe-profile", default=None)
    parser.add_argument(
        "--universe-mode",
        choices=["historical_membership", "fixed_universe"],
        default="historical_membership",
    )
    parser.add_argument("--batch-size", type=int, default=300)
    parser.add_argument(
        "--enrichment-scope",
        choices=["none", "symbol_local", "research_full"],
        default="research_full",
        help="Panel enrichment scope. Defaults to the full research feature layer.",
    )
    parser.add_argument("--feature-group", action="append", dest="feature_groups", default=None)
    parser.add_argument("--include-feature", action="append", dest="included_features", default=None)
    parser.add_argument("--exclude-feature", action="append", dest="excluded_features", default=None)
    parser.add_argument("--task-description", default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = export_or_load_panel(
        panel_path=args.output,
        symbols=args.symbols,
        start_date=args.start_date,
        end_date=args.end_date,
        batch_size=args.batch_size,
        universe_profile=args.universe_profile,
        universe_mode=args.universe_mode,
        run_export="always",
        return_panel=False,
        enrichment_scope=args.enrichment_scope,
        feature_groups=args.feature_groups,
        included_features=args.included_features,
        excluded_features=args.excluded_features,
        task_description=args.task_description,
    )
    print(f"Exported weekly panel to {result['path']}")


if __name__ == "__main__":
    main()

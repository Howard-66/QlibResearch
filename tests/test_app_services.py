from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from qlib_research.app.contracts import (
    DataTablePayload,
    RunListItem,
    RunQuickSummary,
    RunResearchAnalysisTaskRequest,
    TaskReorderRequest,
)
from qlib_research.app import services
from qlib_research.core.weekly_feature_panel import select_panel_feature_columns


def test_list_runs_returns_quick_summary(monkeypatch, tmp_path):
    workflow_root = tmp_path / "artifacts" / "native_workflow"
    run_dir = workflow_root / "demo_run"
    recipe_dir = run_dir / "baseline"
    recipe_dir.mkdir(parents=True)
    (run_dir / "native_workflow_summary.json").write_text(
        json.dumps(
            {
                "config": {
                    "universe_profile": "csi300",
                    "panel_path": "artifacts/panels/demo_panel.csv",
                    "start_date": "2020-01-01",
                    "end_date": "2020-12-31",
                },
                "promotion_gate": {"baseline": {"promotion_gate_passed": True}},
                "recipe_registry": {"executed_recipes": ["baseline"]},
            }
        ),
        encoding="utf-8",
    )
    (recipe_dir / "native_workflow_manifest.json").write_text(
        json.dumps({"used_feature_columns": ["ma20", "ma50"]}),
        encoding="utf-8",
    )
    pd.DataFrame(
        [{"rank_ic_ir": 0.11, "topk_mean_excess_return_4w": 0.01, "used_feature_count": 2}]
    ).to_csv(recipe_dir / "rolling_summary.csv", index=False)
    pd.DataFrame(
        [{"rank_ic_ir": 0.22, "topk_mean_excess_return_4w": 0.02, "used_feature_count": 2}]
    ).to_csv(recipe_dir / "walk_forward_summary.csv", index=False)
    pd.DataFrame([{"bundle": "walk_forward", "native_minus_validation_return": 0.02}]).to_csv(
        recipe_dir / "execution_diff_summary.csv",
        index=False,
    )
    pd.DataFrame([{"net_value": 1_180_000}]).to_csv(recipe_dir / "walk_forward_native_report.csv", index=False)

    monkeypatch.setattr(services, "NATIVE_WORKFLOW_ROOT", workflow_root)

    runs = services.list_runs()

    assert len(runs) == 1
    assert runs[0].run_id == "demo_run"
    assert runs[0].quick_summary.universe_profile == "csi300"
    assert runs[0].quick_summary.baseline_metrics["walk_forward_rank_ic_ir"] == 0.22
    assert runs[0].quick_summary.artifact_status == "partial"
    assert (run_dir / services.RUN_INDEX_FILENAME).exists()


def test_get_run_detail_does_not_depend_on_listing_all_runs(monkeypatch, tmp_path):
    workflow_root = tmp_path / "artifacts" / "native_workflow"
    run_dir = workflow_root / "demo_run"
    recipe_dir = run_dir / "baseline"
    recipe_dir.mkdir(parents=True)
    (run_dir / "native_workflow_summary.json").write_text(
        json.dumps(
            {
                "config": {
                    "universe_profile": "csi300",
                    "panel_path": "artifacts/panels/demo_panel.csv",
                    "start_date": "2020-01-01",
                    "end_date": "2020-12-31",
                },
                "recipe_registry": {"executed_recipes": ["baseline"]},
            }
        ),
        encoding="utf-8",
    )
    pd.DataFrame([{"rank_ic_ir": 0.11, "topk_mean_excess_return_4w": 0.01, "used_feature_count": 12}]).to_csv(
        recipe_dir / "rolling_summary.csv",
        index=False,
    )
    pd.DataFrame([{"rank_ic_ir": 0.22, "topk_mean_excess_return_4w": 0.02, "used_feature_count": 12}]).to_csv(
        recipe_dir / "walk_forward_summary.csv",
        index=False,
    )
    pd.DataFrame([{"net_value": 1_180_000}]).to_csv(recipe_dir / "walk_forward_native_report.csv", index=False)
    pd.DataFrame([{"bundle": "walk_forward", "native_minus_validation_return": 0.02}]).to_csv(
        recipe_dir / "execution_diff_summary.csv", index=False
    )
    pd.DataFrame([{"score_dispersion": 0.1, "topk_unique_score_ratio": 1.0}]).to_csv(
        recipe_dir / "signal_diagnostics.csv",
        index=False,
    )
    pd.DataFrame([{"blocked_sell_count": 0, "actual_hold_count": 10, "target_hold_count": 10}]).to_csv(
        recipe_dir / "portfolio_diagnostics.csv",
        index=False,
    )
    pd.DataFrame([{"mean_excess_return_4w": 0.02}]).to_csv(recipe_dir / "slice_regime_summary.csv", index=False)
    pd.DataFrame([{"feature_date": "2026-01-03", "score": 0.5}]).to_csv(recipe_dir / "latest_score_frame.csv", index=False)
    pd.DataFrame([{"keep": 1}]).to_csv(recipe_dir / "feature_prefilter.csv", index=False)
    (run_dir / "experiment_scorecard.json").write_text(
        json.dumps(
            {
                "headline": "当前主线建议：baseline",
                "verdict": "incumbent",
                "current_problem": "等待候选胜出",
                "recommended_action": "继续推进实验 B",
                "key_findings": ["baseline 仍是主线"],
                "risks": ["行业集中仍需控制"],
                "recommended_next_actions": ["继续推进实验 B"],
            }
        ),
        encoding="utf-8",
    )
    (recipe_dir / "native_workflow_manifest.json").write_text(
        json.dumps({"used_feature_columns": ["ma20"]}),
        encoding="utf-8",
    )

    monkeypatch.setattr(services, "NATIVE_WORKFLOW_ROOT", workflow_root)
    monkeypatch.setattr(services, "list_runs", lambda limit=50: (_ for _ in ()).throw(AssertionError("should not list all runs")))

    detail = services.get_run_detail("demo_run")

    assert detail.quick_summary.universe_profile == "csi300"
    assert detail.quick_summary.baseline_metrics["walk_forward_rank_ic_ir"] == 0.22
    assert detail.quick_summary.baseline_metrics["walk_forward_net_total_return"] == pytest.approx(0.18)
    assert detail.research_summary.verdict == "incumbent"
    assert detail.research_summary.current_problem == "等待候选胜出"
    assert detail.artifact_inventory


def test_get_recipe_detail_backfills_drawdowns_from_native_reports(monkeypatch):
    run_dir = Path("/tmp/demo_run")
    index_payload = {
        "run_id": "demo_run",
        "quick_summary": RunQuickSummary(
            run_id="demo_run",
            output_dir=str(run_dir),
            recipe_names=["baseline"],
            baseline_recipe="baseline",
            artifact_status="ready",
        ).model_dump(mode="json"),
        "overview_lookup": {"baseline": {"rolling_rank_ic_ir": 0.11}},
    }
    summary_payload = {
        "recipe_registry": {"executed_recipes": ["baseline"]},
        "promotion_gate": {},
    }
    frames = {
        "manifest": {"used_feature_columns": ["ma20"]},
        "rolling_summary": pd.DataFrame([{"rank_ic_ir": 0.11, "topk_mean_excess_return_4w": 0.01}]),
        "walk_forward_summary": pd.DataFrame([{"rank_ic_ir": 0.22, "topk_mean_excess_return_4w": 0.02}]),
        "rolling_native_report": pd.DataFrame([{"net_value": 1_120_000, "relative_drawdown": -0.08}]),
        "walk_forward_native_report": pd.DataFrame([{"net_value": 1_180_000, "relative_drawdown": -0.06}]),
        "feature_prefilter": pd.DataFrame(),
        "signal_diagnostics": pd.DataFrame(),
        "portfolio_diagnostics": pd.DataFrame(),
        "execution_diff_summary": pd.DataFrame(),
        "slice_regime_summary": pd.DataFrame(),
        "latest_score_frame": pd.DataFrame(),
    }

    monkeypatch.setattr(services, "_collect_run_context", lambda run_id: (run_dir, summary_payload, index_payload))
    monkeypatch.setattr(services, "_load_recipe_frames", lambda run_dir, recipe_name, table_names: frames)
    monkeypatch.setattr(services, "_get_recipe_config", lambda summary_payload, recipe_name: {})
    monkeypatch.setattr(services, "_panel_summary_for_path", lambda panel_path: None)
    monkeypatch.setattr(services, "_resolve_artifact_path", lambda value: None)
    monkeypatch.setattr(services, "_recipe_inventory", lambda recipe_dir, prefix: [])

    detail = services.get_recipe_detail("demo_run", "baseline")

    assert detail.overview["rolling_net_total_return"] == pytest.approx(0.12)
    assert detail.overview["rolling_max_drawdown"] == pytest.approx(-0.08)
    assert detail.overview["rolling_sharpe_ratio"] is None
    assert detail.overview["walk_forward_net_total_return"] == pytest.approx(0.18)
    assert detail.overview["walk_forward_max_drawdown"] == pytest.approx(-0.06)
    assert detail.overview["walk_forward_win_rate"] is None


def test_get_recipe_detail_reads_new_performance_metric_files(monkeypatch):
    run_dir = Path("/tmp/demo_run")
    index_payload = {
        "run_id": "demo_run",
        "quick_summary": RunQuickSummary(
            run_id="demo_run",
            output_dir=str(run_dir),
            recipe_names=["baseline"],
            baseline_recipe="baseline",
            artifact_status="ready",
        ).model_dump(mode="json"),
        "overview_lookup": {"baseline": {}},
    }
    summary_payload = {
        "recipe_registry": {"executed_recipes": ["baseline"]},
        "promotion_gate": {},
    }
    frames = {
        "manifest": {"used_feature_columns": ["ma20"]},
        "rolling_summary": pd.DataFrame([{"rank_ic_ir": 0.11}]),
        "walk_forward_summary": pd.DataFrame([{"rank_ic_ir": 0.22}]),
        "rolling_native_report": pd.DataFrame([{"net_value": 1_120_000, "relative_drawdown": -0.08}]),
        "walk_forward_native_report": pd.DataFrame([{"net_value": 1_180_000, "relative_drawdown": -0.06}]),
        "rolling_performance_metrics": pd.DataFrame(
            [{"annualized_return": 0.13, "annualized_volatility": 0.21, "sharpe_ratio": 0.62, "win_rate": 0.56, "calmar_ratio": 1.63}]
        ),
        "walk_forward_performance_metrics": pd.DataFrame(
            [{"annualized_return": 0.16, "annualized_volatility": 0.24, "sharpe_ratio": 0.71, "win_rate": 0.58, "calmar_ratio": 2.67}]
        ),
        "feature_prefilter": pd.DataFrame(),
        "signal_diagnostics": pd.DataFrame(),
        "portfolio_diagnostics": pd.DataFrame(),
        "execution_diff_summary": pd.DataFrame(),
        "slice_regime_summary": pd.DataFrame(),
        "latest_score_frame": pd.DataFrame(),
    }

    monkeypatch.setattr(services, "_collect_run_context", lambda run_id: (run_dir, summary_payload, index_payload))
    monkeypatch.setattr(services, "_load_recipe_frames", lambda run_dir, recipe_name, table_names: frames)
    monkeypatch.setattr(services, "_get_recipe_config", lambda summary_payload, recipe_name: {})
    monkeypatch.setattr(services, "_panel_summary_for_path", lambda panel_path: None)
    monkeypatch.setattr(services, "_resolve_artifact_path", lambda value: None)
    monkeypatch.setattr(services, "_recipe_inventory", lambda recipe_dir, prefix: [])

    detail = services.get_recipe_detail("demo_run", "baseline")

    assert detail.overview["rolling_annualized_return"] == pytest.approx(0.13)
    assert detail.overview["rolling_sharpe_ratio"] == pytest.approx(0.62)
    assert detail.overview["walk_forward_win_rate"] == pytest.approx(0.58)
    assert detail.overview["walk_forward_calmar_ratio"] == pytest.approx(2.67)


def test_get_recipe_detail_reads_analysis_reports(monkeypatch, tmp_path):
    run_dir = tmp_path / "demo_run"
    recipe_dir = run_dir / "baseline"
    analysis_dir = recipe_dir / "analysis"
    analysis_dir.mkdir(parents=True)
    (analysis_dir / "latest_summary.json").write_text(
        json.dumps({"engine": "codex_cli", "template": "experiment_review", "verdict": "hold"}),
        encoding="utf-8",
    )
    (analysis_dir / "latest_summary.md").write_text("# report", encoding="utf-8")
    (recipe_dir / "experiment_scorecard.json").write_text(
        json.dumps({"headline": "baseline", "key_findings": ["a"], "risks": [], "recommended_next_experiments": ["b"]}),
        encoding="utf-8",
    )
    index_payload = {
        "run_id": "demo_run",
        "quick_summary": RunQuickSummary(
            run_id="demo_run",
            output_dir=str(run_dir),
            recipe_names=["baseline"],
            baseline_recipe="baseline",
            artifact_status="ready",
        ).model_dump(mode="json"),
        "overview_lookup": {"baseline": {}},
    }
    summary_payload = {"recipe_registry": {"executed_recipes": ["baseline"]}, "promotion_gate": {}}
    frames = {
        "manifest": {"used_feature_columns": ["ma20"]},
        "rolling_summary": pd.DataFrame(),
        "walk_forward_summary": pd.DataFrame(),
        "rolling_native_report": pd.DataFrame(),
        "walk_forward_native_report": pd.DataFrame(),
        "rolling_performance_metrics": pd.DataFrame(),
        "walk_forward_performance_metrics": pd.DataFrame(),
        "feature_prefilter": pd.DataFrame(),
        "signal_diagnostics": pd.DataFrame(),
        "portfolio_diagnostics": pd.DataFrame(),
        "execution_diff_summary": pd.DataFrame(),
        "slice_regime_summary": pd.DataFrame(),
        "latest_score_frame": pd.DataFrame(),
        "signal_realization_bridge": pd.DataFrame(),
        "holding_count_drift": pd.DataFrame(),
        "sector_exposure_history": pd.DataFrame(),
        "regime_gate_diagnostics": pd.DataFrame(),
    }
    monkeypatch.setattr(services, "_collect_run_context", lambda run_id: (run_dir, summary_payload, index_payload))
    monkeypatch.setattr(services, "_load_recipe_frames", lambda run_dir, recipe_name, table_names: frames)
    monkeypatch.setattr(services, "_get_recipe_config", lambda summary_payload, recipe_name: {})
    monkeypatch.setattr(services, "_panel_summary_for_path", lambda panel_path: None)
    monkeypatch.setattr(services, "_resolve_artifact_path", lambda value: None)

    detail = services.get_recipe_detail("demo_run", "baseline")

    assert detail.analysis_reports[0].engine == "codex_cli"
    assert any(item.content_type == "json" for item in detail.analysis_reports)
    assert any("experiment_review" in (item.content_preview or "") for item in detail.analysis_reports)
    assert detail.research_summary.headline == "report"


def test_get_recipe_detail_includes_run_recipe_dossier(monkeypatch, tmp_path):
    run_dir = tmp_path / "demo_run"
    recipe_dir = run_dir / "rank_blended"
    recipe_dir.mkdir(parents=True)
    run_analysis_dir = run_dir / "analysis"
    run_analysis_dir.mkdir(parents=True)
    (run_analysis_dir / "latest_summary.json").write_text(
        json.dumps({"engine": "auto", "template": "native_workflow_system_report", "verdict": "investigate"}),
        encoding="utf-8",
    )
    (run_analysis_dir / "latest_summary.md").write_text(
        "\n".join(
            [
                "# demo_run 系统诊断报告",
                "",
                "## Recipe Dossiers",
                "### baseline",
                "- baseline content",
                "### rank_blended",
                "- 角色：主线。",
                "- 需要警惕的部分：TopK 超额为负。",
                "### mae_4w",
                "- mae content",
                "",
                "## Live Portfolio Feasibility",
                "- live content",
            ]
        ),
        encoding="utf-8",
    )
    index_payload = {
        "run_id": "demo_run",
        "quick_summary": RunQuickSummary(
            run_id="demo_run",
            output_dir=str(run_dir),
            recipe_names=["rank_blended"],
            baseline_recipe="rank_blended",
            artifact_status="ready",
        ).model_dump(mode="json"),
        "overview_lookup": {"rank_blended": {}},
    }
    summary_payload = {"recipe_registry": {"executed_recipes": ["rank_blended"]}, "promotion_gate": {}}
    empty_frames = {name: pd.DataFrame() for name in {
        "rolling_summary",
        "walk_forward_summary",
        "rolling_native_report",
        "walk_forward_native_report",
        "rolling_performance_metrics",
        "walk_forward_performance_metrics",
        "feature_prefilter",
        "signal_diagnostics",
        "portfolio_diagnostics",
        "execution_diff_summary",
        "slice_regime_summary",
        "latest_score_frame",
        "signal_realization_bridge",
        "holding_count_drift",
        "sector_exposure_history",
        "regime_gate_diagnostics",
    }}
    frames = {"manifest": {}, **empty_frames}
    monkeypatch.setattr(services, "_collect_run_context", lambda run_id: (run_dir, summary_payload, index_payload))
    monkeypatch.setattr(services, "_load_recipe_frames", lambda run_dir, recipe_name, table_names: frames)
    monkeypatch.setattr(services, "_get_recipe_config", lambda summary_payload, recipe_name: {})
    monkeypatch.setattr(services, "_panel_summary_for_path", lambda panel_path: None)
    monkeypatch.setattr(services, "_resolve_artifact_path", lambda value: None)
    monkeypatch.setattr(services, "_recipe_inventory", lambda recipe_dir, prefix: [])

    detail = services.get_recipe_detail("demo_run", "rank_blended")
    dossier = next(item for item in detail.analysis_reports if item.name == "run_recipe_dossier.md")

    assert "### rank_blended" in (dossier.content_preview or "")
    assert "TopK 超额为负" in (dossier.content_preview or "")
    assert "baseline content" not in (dossier.content_preview or "")
    assert "mae content" not in (dossier.content_preview or "")


def test_read_research_summary_prefers_latest_summary_markdown_sections(tmp_path):
    base_dir = tmp_path / "demo_run"
    analysis_dir = base_dir / "analysis"
    analysis_dir.mkdir(parents=True)
    (base_dir / "experiment_scorecard.json").write_text(
        json.dumps(
            {
                "headline": "scorecard headline",
                "verdict": "hold",
                "key_findings": ["scorecard finding"],
                "risks": ["scorecard risk"],
                "recommended_next_actions": ["scorecard action"],
            }
        ),
        encoding="utf-8",
    )
    (analysis_dir / "latest_summary.md").write_text(
        "\n".join(
            [
                "# md headline",
                "",
                "- verdict: promoted",
                "",
                "## Key Findings",
                "- md finding 1",
                "- md finding 2",
                "",
                "## Risks",
                "- md risk",
                "",
                "## Recommended Next Actions",
                "- md action",
            ]
        ),
        encoding="utf-8",
    )

    summary = services._read_research_summary(base_dir)

    assert summary.headline == "md headline"
    assert summary.verdict == "promoted"
    assert summary.key_findings == ["md finding 1", "md finding 2"]
    assert summary.risks == ["md risk"]
    assert summary.recommended_next_actions == ["md action"]


def test_read_research_summary_accepts_numbered_chinese_headings(tmp_path):
    base_dir = tmp_path / "demo_run"
    analysis_dir = base_dir / "analysis"
    analysis_dir.mkdir(parents=True)
    (analysis_dir / "latest_summary.md").write_text(
        "\n".join(
            [
                "# 深度研究摘要",
                "",
                "- verdict: investigate",
                "",
                "## 1. 主线建议",
                "1. 继续保留 baseline 为主线，但先验证执行定义是否收敛。",
                "",
                "## 2. 当前问题",
                "- 名义 topk 与实际持仓数仍有偏差。",
                "",
                "## 3. 结论摘要",
                "- baseline 的收益与回撤更均衡。",
                "- binary_4w 收益高但信号退化。",
                "",
                "## 4. 风险与约束",
                "- 行业集中度仍偏高。",
                "",
                "## 5. 优化方案",
                "- 先做行业上限实验。",
                "- 再做 score dispersion 门限实验。",
            ]
        ),
        encoding="utf-8",
    )

    summary = services._read_research_summary(base_dir)

    assert summary.headline == "深度研究摘要"
    assert summary.verdict == "investigate"
    assert summary.recommended_action == "继续保留 baseline 为主线，但先验证执行定义是否收敛。"
    assert summary.current_problem == "名义 topk 与实际持仓数仍有偏差。"
    assert summary.key_findings == ["baseline 的收益与回撤更均衡。", "binary_4w 收益高但信号退化。"]
    assert summary.risks == ["行业集中度仍偏高。"]
    assert summary.recommended_next_actions == ["先做行业上限实验。", "再做 score dispersion 门限实验。"]


def test_read_research_summary_accepts_native_workflow_json(tmp_path):
    base_dir = tmp_path / "demo_run"
    analysis_dir = base_dir / "analysis"
    analysis_dir.mkdir(parents=True)
    (analysis_dir / "latest_summary.json").write_text(
        json.dumps(
            {
                "headline": "rank_blended 暂时最适合作为下一轮主线，但还不能进入实盘。",
                "verdict": "investigate",
                "current_problem": "收益来源还没有解释清楚。",
                "recommended_action": "先修复 TopK 兑现。",
                "system_findings": ["rank_blended 收益领先。", "TopK 兑现仍为负。"],
                "live_feasibility": {"checks": ["执行验证表为空。"]},
                "next_experiments": [
                    {"priority": "P0", "name": "topk_repair", "rationale": "拆解信号兑现。"}
                ],
            }
        ),
        encoding="utf-8",
    )

    summary = services._read_research_summary(base_dir)

    assert summary.headline == "rank_blended 暂时最适合作为下一轮主线，但还不能进入实盘。"
    assert summary.verdict == "investigate"
    assert summary.key_findings == ["rank_blended 收益领先。", "TopK 兑现仍为负。"]
    assert summary.risks == ["执行验证表为空。"]
    assert summary.recommended_next_actions == ["P0：topk_repair。拆解信号兑现。"]


def test_build_run_index_payload_backfills_recipe_overview_from_summary_and_existing_index(tmp_path):
    run_dir = tmp_path / "demo_run"
    baseline_dir = run_dir / "baseline"
    candidate_dir = run_dir / "candidate"
    baseline_dir.mkdir(parents=True)
    candidate_dir.mkdir(parents=True)

    summary_payload = {
        "config": {"universe_profile": "csi300"},
        "recipe_registry": {
            "executed_recipes": ["baseline", "candidate"],
            "baseline_recipe": {"name": "baseline", "signal_objective": "huber_regression"},
            "candidate_recipes": {"candidate": {"name": "candidate", "signal_objective": "mae_regression"}},
        },
        "promotion_gate": {
            "candidate": {
                "promotion_gate_passed": True,
                "candidate_walk_forward_net_total_return": 0.18,
                "baseline_walk_forward_net_total_return": 0.12,
                "candidate_walk_forward_drawdown": -0.09,
                "baseline_walk_forward_drawdown": -0.05,
            }
        },
    }
    (run_dir / "native_workflow_summary.json").write_text(json.dumps(summary_payload), encoding="utf-8")

    for recipe_dir in (baseline_dir, candidate_dir):
        (recipe_dir / "native_workflow_manifest.json").write_text(
            json.dumps({"used_feature_columns": ["ma20", "ma50"]}),
            encoding="utf-8",
        )

    pd.DataFrame([{"rank_ic_ir": 0.11}]).to_csv(baseline_dir / "rolling_summary.csv", index=False)
    pd.DataFrame([{"rank_ic_ir": 0.22}]).to_csv(baseline_dir / "walk_forward_summary.csv", index=False)
    pd.DataFrame([{"rank_ic_ir": 0.33}]).to_csv(candidate_dir / "rolling_summary.csv", index=False)
    pd.DataFrame([{"rank_ic_ir": 0.44}]).to_csv(candidate_dir / "walk_forward_summary.csv", index=False)

    existing_payload = {
        "overview_lookup": {
            "baseline": {"rolling_net_total_return": 0.10},
            "candidate": {
                "rolling_net_total_return": 0.21,
                "walk_forward_net_total_return": 0.18,
            },
        },
        "quick_summary": {"promotion_gate_summary": summary_payload["promotion_gate"]},
    }

    payload = services._build_run_index_payload(run_dir, existing_payload=existing_payload)
    recipes_by_name = {item["recipe_name"]: item for item in payload["recipes"]}

    assert payload["schema_version"] == services.RUN_INDEX_SCHEMA_VERSION
    assert recipes_by_name["candidate"]["rolling_net_total_return"] == pytest.approx(0.21)
    assert recipes_by_name["candidate"]["walk_forward_net_total_return"] == pytest.approx(0.18)
    assert recipes_by_name["candidate"]["walk_forward_max_drawdown"] == pytest.approx(-0.09)
    assert recipes_by_name["candidate"]["promotion_gate_passed"] is True
    assert recipes_by_name["baseline"]["walk_forward_max_drawdown"] == pytest.approx(-0.05)
    assert payload["quick_summary"]["baseline_metrics"]["walk_forward_max_drawdown"] == pytest.approx(-0.05)


def test_load_run_index_payload_upgrades_legacy_cache(monkeypatch, tmp_path):
    workflow_root = tmp_path / "artifacts" / "native_workflow"
    run_dir = workflow_root / "demo_run"
    baseline_dir = run_dir / "baseline"
    candidate_dir = run_dir / "candidate"
    baseline_dir.mkdir(parents=True)
    candidate_dir.mkdir(parents=True)

    summary_payload = {
        "config": {"universe_profile": "csi300"},
        "recipe_registry": {
            "executed_recipes": ["baseline", "candidate"],
            "baseline_recipe": {"name": "baseline"},
            "candidate_recipes": {"candidate": {"name": "candidate"}},
        },
        "promotion_gate": {
            "candidate": {
                "promotion_gate_passed": True,
                "candidate_walk_forward_net_total_return": 0.16,
                "baseline_walk_forward_net_total_return": 0.11,
                "candidate_walk_forward_drawdown": -0.08,
                "baseline_walk_forward_drawdown": -0.04,
            }
        },
    }
    (run_dir / "native_workflow_summary.json").write_text(json.dumps(summary_payload), encoding="utf-8")
    for recipe_dir in (baseline_dir, candidate_dir):
        (recipe_dir / "native_workflow_manifest.json").write_text(
            json.dumps({"used_feature_columns": ["ma20"]}),
            encoding="utf-8",
        )
        pd.DataFrame([{"rank_ic_ir": 0.1}]).to_csv(recipe_dir / "rolling_summary.csv", index=False)
        pd.DataFrame([{"rank_ic_ir": 0.2}]).to_csv(recipe_dir / "walk_forward_summary.csv", index=False)

    legacy_payload = {
        "run_id": "demo_run",
        "quick_summary": {
            "run_id": "demo_run",
            "output_dir": str(run_dir),
            "recipe_names": ["baseline", "candidate"],
            "artifact_status": "partial",
            "promotion_gate_summary": summary_payload["promotion_gate"],
        },
        "recipes": [
            {"run_id": "demo_run", "recipe_name": "baseline"},
            {"run_id": "demo_run", "recipe_name": "candidate"},
        ],
        "overview_lookup": {
            "baseline": {"rolling_net_total_return": 0.09},
            "candidate": {"walk_forward_net_total_return": 0.16},
        },
    }
    (run_dir / services.RUN_INDEX_FILENAME).write_text(json.dumps(legacy_payload), encoding="utf-8")

    monkeypatch.setattr(services, "NATIVE_WORKFLOW_ROOT", workflow_root)
    services._RUN_INDEX_CACHE.clear()

    payload = services._load_run_index_payload(run_dir)
    recipes_by_name = {item["recipe_name"]: item for item in payload["recipes"]}
    disk_payload = json.loads((run_dir / services.RUN_INDEX_FILENAME).read_text(encoding="utf-8"))

    assert payload["schema_version"] == services.RUN_INDEX_SCHEMA_VERSION
    assert disk_payload["schema_version"] == services.RUN_INDEX_SCHEMA_VERSION
    assert recipes_by_name["candidate"]["walk_forward_max_drawdown"] == pytest.approx(-0.08)
    assert recipes_by_name["baseline"]["walk_forward_max_drawdown"] == pytest.approx(-0.04)


def test_get_panel_detail_reads_panel_and_links_runs(monkeypatch, tmp_path):
    panels_root = tmp_path / "artifacts" / "panels"
    panels_root.mkdir(parents=True)
    panel_path = panels_root / "demo_panel.csv"
    pd.DataFrame(
        {
            "datetime": ["2026-01-03", "2026-01-10"],
            "instrument": ["AAA.SH", "BBB.SZ"],
            "in_csi300": [True, True],
            "close": [10.0, 11.2],
        }
    ).to_csv(panel_path, index=False)
    (panels_root / "demo_panel.csv.metadata.json").write_text(
        json.dumps(
            {
                "panel_enrichment_scope": "research_full",
                "universe_mode": "fixed_universe",
                "universe_profile": "csi300",
                "requested_start_date": "2010-01-01",
                "requested_end_date": None,
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(services, "PANELS_ROOT", panels_root)
    monkeypatch.setattr(
        services,
        "list_runs",
        lambda limit=200: [
            RunListItem(
                run_id="demo_run",
                quick_summary=RunQuickSummary(
                    run_id="demo_run",
                    output_dir="/tmp/demo_run",
                    panel_path=str(panel_path),
                    recipe_names=["baseline"],
                    artifact_status="ready",
                ),
            )
        ],
    )

    detail = services.get_panel_detail("demo_panel.csv")

    assert detail.panel_id == "demo_panel.csv"
    assert detail.summary["rows"] == 2
    assert "demo_run" in detail.linked_runs
    assert "close" in detail.columns
    assert detail.universe_mode == "fixed_universe"
    assert detail.universe_profile == "csi300"


def test_get_panel_detail_normalizes_array_like_samples(monkeypatch, tmp_path):
    panels_root = tmp_path / "artifacts" / "panels"
    panels_root.mkdir(parents=True)
    panel_path = panels_root / "demo_panel.parquet"
    panel_path.write_text("placeholder", encoding="utf-8")

    monkeypatch.setattr(services, "PANELS_ROOT", panels_root)
    monkeypatch.setattr(services, "_build_panel_run_links", lambda limit=200: {})
    monkeypatch.setattr(
        services,
        "load_panel_dataframe",
        lambda path: pd.DataFrame(
            {
                "datetime": ["2026-01-03"],
                "instrument": ["AAA.SH"],
                "embedding": [[1.0, 2.0, 3.0]],
            }
        ),
    )

    detail = services.get_panel_detail("demo_panel.parquet")

    embedding_row = next(row for row in detail.column_catalog.rows if row["column"] == "embedding")
    assert embedding_row["sample"] == [1.0, 2.0, 3.0]


def test_create_export_panel_task_persists_command(monkeypatch, tmp_path):
    tasks_root = tmp_path / "artifacts" / "app_tasks"
    monkeypatch.setattr(services, "TASKS_ROOT", tasks_root)

    summary = services.create_export_panel_task(
        services.ExportPanelTaskRequest(
            output="artifacts/panels/from_test.parquet",
            universe_profile="csi300",
            universe_mode="fixed_universe",
            feature_groups=["technical_core"],
            included_features=["ma20"],
            excluded_features=["amount"],
        )
    )

    task_file = tasks_root / summary.task_id / "task.json"
    queue_file = tasks_root / services.TASK_QUEUE_FILENAME
    assert task_file.exists()
    assert queue_file.exists()
    payload = json.loads(task_file.read_text(encoding="utf-8"))
    assert payload["task_kind"] == "export_panel"
    assert "scripts/export_weekly_panel.py" in " ".join(payload["command"])
    assert "--feature-group" in payload["command"]
    assert "--include-feature" in payload["command"]
    assert "--exclude-feature" in payload["command"]
    assert "--universe-mode" in payload["command"]
    queue_payload = json.loads(queue_file.read_text(encoding="utf-8"))
    assert queue_payload["queued_task_ids"] == [summary.task_id]
    assert summary.status == "queued"


def test_compare_recipe_items_returns_recipe_level_payload(monkeypatch):
    run_dir = Path("/tmp/demo_run")
    index_payload = {
        "run_id": "demo_run",
        "quick_summary": RunQuickSummary(
            run_id="demo_run",
            output_dir=str(run_dir),
            recipe_names=["baseline", "candidate"],
            artifact_status="ready",
        ).model_dump(mode="json"),
        "overview_lookup": {"baseline": {"used_feature_count": 12}, "candidate": {"used_feature_count": 8}},
    }
    baseline_frames = {
        "manifest": {},
        "rolling_summary": pd.DataFrame(
            [{"rank_ic_ir": 0.21, "topk_mean_excess_return_4w": 0.03, "topk_hit_rate": 0.61, "coverage_mean": 298}]
        ),
        "rolling_native_report": pd.DataFrame(
            [
                {
                    "datetime": "2026-01-02",
                    "net_value": 1_000_000,
                    "benchmark_value": 1_000_000,
                    "turnover": 0.3,
                    "relative_drawdown": -0.08,
                },
                {
                    "datetime": "2026-01-09",
                    "net_value": 1_120_000,
                    "benchmark_value": 1_050_000,
                    "turnover": 0.6,
                    "relative_drawdown": -0.03,
                },
            ]
        ),
        "rolling_performance_metrics": pd.DataFrame(
            [{"annualized_return": 0.18, "sharpe_ratio": 1.12, "calmar_ratio": 2.4}]
        ),
        "execution_diff_summary": pd.DataFrame(
            [{"bundle": "rolling", "native_minus_validation_return": 0.02, "native_max_drawdown": -0.08}]
        ),
        "slice_regime_summary": pd.DataFrame(
            [{"bundle": "rolling", "slice_type": "feature_year", "mean_excess_return_4w": 0.01}]
        ),
        "rolling_feature_importance": pd.DataFrame([{"feature": "ma20", "importance_gain": 10.0}]),
        "latest_score_frame": pd.DataFrame([{"instrument": "AAA.SH", "score": 0.5}]),
        "walk_forward_summary": pd.DataFrame(),
        "walk_forward_native_report": pd.DataFrame(),
        "walk_forward_performance_metrics": pd.DataFrame(),
        "walk_forward_feature_importance": pd.DataFrame(),
    }
    candidate_frames = {
        "manifest": {},
        "rolling_summary": pd.DataFrame(
            [{"rank_ic_ir": 0.18, "topk_mean_excess_return_4w": 0.02, "topk_hit_rate": 0.55, "coverage_mean": 260}]
        ),
        "rolling_native_report": pd.DataFrame(
            [
                {
                    "datetime": "2026-01-02",
                    "net_value": 1_000_000,
                    "benchmark_value": 1_000_000,
                    "turnover": 0.2,
                    "relative_drawdown": -0.05,
                },
                {
                    "datetime": "2026-01-09",
                    "net_value": 1_080_000,
                    "benchmark_value": 1_050_000,
                    "turnover": 0.4,
                    "relative_drawdown": -0.02,
                },
            ]
        ),
        "rolling_performance_metrics": pd.DataFrame(
            [{"annualized_return": 0.14, "sharpe_ratio": 0.95, "calmar_ratio": 1.8}]
        ),
        "execution_diff_summary": pd.DataFrame(
            [{"bundle": "rolling", "native_minus_validation_return": 0.01, "native_max_drawdown": -0.05}]
        ),
        "slice_regime_summary": pd.DataFrame(
            [{"bundle": "rolling", "slice_type": "feature_year", "mean_excess_return_4w": 0.02}]
        ),
        "rolling_feature_importance": pd.DataFrame([{"feature": "pb", "importance_gain": 8.0}]),
        "latest_score_frame": pd.DataFrame([{"instrument": "BBB.SZ", "score": 0.4}]),
        "walk_forward_summary": pd.DataFrame(),
        "walk_forward_native_report": pd.DataFrame(),
        "walk_forward_performance_metrics": pd.DataFrame(),
        "walk_forward_feature_importance": pd.DataFrame(),
    }

    monkeypatch.setattr(services, "_collect_run_context", lambda run_id: (run_dir, {"config": {}}, index_payload))
    monkeypatch.setattr(
        services,
        "_load_recipe_frames",
        lambda run_dir, recipe_name, table_names: baseline_frames if recipe_name == "baseline" else candidate_frames,
    )

    response = services.compare_recipe_items(
        [
            services.CompareItemRef(run_id="demo_run", recipe_name="baseline", bundle="rolling"),
            services.CompareItemRef(run_id="demo_run", recipe_name="candidate", bundle="rolling"),
        ]
    )

    assert len(response.items) == 2
    assert response.summary_metrics.rows[0]["rank_ic_ir"] == 0.21
    assert response.summary_metrics.rows[0]["topk_hit_rate"] == pytest.approx(0.61)
    assert response.summary_metrics.rows[0]["net_total_return"] == pytest.approx(0.12)
    assert response.summary_metrics.rows[0]["max_drawdown"] == pytest.approx(-0.08)
    assert response.summary_metrics.rows[0]["annualized_return"] == pytest.approx(0.18)
    assert response.summary_metrics.rows[0]["sharpe_ratio"] == pytest.approx(1.12)
    assert "native_minus_validation_return" not in response.summary_metrics.columns
    assert "native_max_drawdown" not in response.summary_metrics.columns
    assert len(response.net_value_curves) == 3
    assert response.net_value_curves[-1].role == "benchmark"
    assert response.net_value_curves[-1].label == "Shared benchmark"
    assert response.net_value_curves[0].points[-1].value == pytest.approx(1_120_000)
    assert response.feature_importance


def test_task_dispatcher_runs_queue_serially(monkeypatch, tmp_path):
    tasks_root = tmp_path / "artifacts" / "app_tasks"
    monkeypatch.setattr(services, "TASKS_ROOT", tasks_root)

    first = services.create_export_panel_task(services.ExportPanelTaskRequest(output="artifacts/panels/first.parquet"))
    second = services.create_export_panel_task(services.ExportPanelTaskRequest(output="artifacts/panels/second.parquet"))
    execution_order: list[str] = []

    def fake_task_worker(task_dir: Path) -> int:
        execution_order.append(task_dir.name)
        services.update_task_status(
            task_dir,
            status="succeeded",
            started_at="2026-04-13T10:00:00+08:00",
            finished_at="2026-04-13T10:01:00+08:00",
            message="Task completed",
            metadata={"worker_pid": 1000, "return_code": 0},
        )
        services._write_json_file(
            task_dir / "result.json",
            {"task_id": task_dir.name, "status": "succeeded", "return_code": 0},
        )
        return 0

    monkeypatch.setattr(services, "task_worker_run", fake_task_worker)

    return_code = services.task_dispatcher_run()
    board = services.list_tasks()

    assert return_code == 0
    assert execution_order == [first.task_id, second.task_id]
    assert board.running_task is None
    assert board.queue_state.dispatcher_status == "idle"
    assert board.queue_state.queued_task_ids == []
    assert {task.task_id for task in board.history_tasks[:2]} == {first.task_id, second.task_id}


def test_reorder_and_remove_queued_tasks(monkeypatch, tmp_path):
    tasks_root = tmp_path / "artifacts" / "app_tasks"
    monkeypatch.setattr(services, "TASKS_ROOT", tasks_root)

    first = services.create_export_panel_task(services.ExportPanelTaskRequest(output="artifacts/panels/first.parquet"))
    second = services.create_export_panel_task(services.ExportPanelTaskRequest(output="artifacts/panels/second.parquet"))

    board = services.reorder_tasks(TaskReorderRequest(queued_task_ids=[second.task_id, first.task_id]))
    assert [task.task_id for task in board.queued_tasks] == [second.task_id, first.task_id]

    board = services.remove_task(second.task_id)
    assert [task.task_id for task in board.queued_tasks] == [first.task_id]
    assert not (tasks_root / second.task_id).exists()


def test_get_run_and_panel_task_presets(monkeypatch, tmp_path):
    workflow_root = tmp_path / "artifacts" / "native_workflow"
    panels_root = tmp_path / "artifacts" / "panels"
    tasks_root = tmp_path / "artifacts" / "app_tasks"
    workflow_root.mkdir(parents=True)
    panels_root.mkdir(parents=True)
    monkeypatch.setattr(services, "NATIVE_WORKFLOW_ROOT", workflow_root)
    monkeypatch.setattr(services, "PANELS_ROOT", panels_root)
    monkeypatch.setattr(services, "TASKS_ROOT", tasks_root)

    panel_path = panels_root / "demo_panel.csv"
    pd.DataFrame(
        {
            "datetime": ["2026-01-03", "2026-01-10"],
            "instrument": ["AAA.SH", "BBB.SZ"],
            "in_csi300": [True, True],
            "ma20": [10.0, 11.0],
        }
    ).to_csv(panel_path, index=False)

    run_dir = workflow_root / "demo_run"
    recipe_dir = run_dir / "baseline"
    recipe_dir.mkdir(parents=True)
    (run_dir / "native_workflow_summary.json").write_text(
        json.dumps(
            {
                "config": {
                    "universe_profile": "csi300",
                    "panel_path": str(panel_path),
                    "output_dir": "artifacts/native_workflow/demo_run",
                },
                "recipe_registry": {"executed_recipes": ["baseline"]},
            }
        ),
        encoding="utf-8",
    )
    pd.DataFrame([{"rank_ic_ir": 0.12}]).to_csv(recipe_dir / "rolling_summary.csv", index=False)
    pd.DataFrame([{"rank_ic_ir": 0.18}]).to_csv(recipe_dir / "walk_forward_summary.csv", index=False)
    (recipe_dir / "native_workflow_manifest.json").write_text(json.dumps({"used_feature_columns": ["ma20"]}), encoding="utf-8")

    historical_task = services.ResearchTaskSummary(
        task_id="task-demo-export",
        task_kind="export_panel",
        status="succeeded",
        display_name="Export Panel",
        requested_by="webapp",
        created_at="2026-04-12T10:00:00+08:00",
        finished_at="2026-04-12T10:10:00+08:00",
        output_dir=str(panel_path),
        message="Task completed",
        command=["uv", "run", "python", "scripts/export_weekly_panel.py", "--output", str(panel_path)],
        config_payload={
            "display_name": "Export Panel",
            "requested_by": "webapp",
            "output": str(panel_path),
            "enrichment_scope": "research_full",
            "feature_groups": ["technical_core"],
            "included_features": ["ma20"],
            "excluded_features": ["amount"],
        },
        logs={},
        metadata={},
        source_ref=services.TaskSourceRef(kind="panel", source_id=panel_path.name, label=panel_path.name, path=str(panel_path)),
    )
    services._write_task(historical_task)

    run_preset = services.get_run_task_preset("demo_run")
    panel_preset = services.get_panel_task_preset("demo_panel.csv")

    assert run_preset.task_kind == "run_native_workflow"
    assert run_preset.payload["recipe_names"] == ["baseline"]
    assert str(run_preset.payload["config_payload"]["output_dir"]).startswith("artifacts/native_workflow/demo_run-rerun-")
    assert panel_preset.task_kind == "export_panel"
    assert panel_preset.payload["feature_groups"] == ["technical_core"]
    assert panel_preset.payload["included_features"] == ["ma20"]


def test_select_panel_feature_columns_preserves_base_columns():
    panel = pd.DataFrame(
        {
            "symbol": ["AAA.SH"],
            "time": ["2026-01-03"],
            "in_csi300": [True],
            "ma20": [10.0],
            "rsi": [55.0],
            "amount": [1_000_000],
        }
    )

    selected = select_panel_feature_columns(
        panel,
        feature_groups=["technical_core"],
        included_features=["rsi"],
        excluded_features=["amount"],
    )

    assert "symbol" in selected.columns
    assert "time" in selected.columns
    assert "in_csi300" in selected.columns
    assert "ma20" in selected.columns
    assert "rsi" in selected.columns
    assert "amount" not in selected.columns


def test_create_native_workflow_task_falls_back_to_source_run_execution_panel(monkeypatch, tmp_path):
    tasks_root = tmp_path / "artifacts" / "app_tasks"
    execution_panel = tmp_path / "artifacts" / "native_workflow" / "source_run" / "csi300_execution_panel.parquet"
    execution_panel.parent.mkdir(parents=True, exist_ok=True)
    execution_panel.write_text("placeholder", encoding="utf-8")

    monkeypatch.setattr(services, "TASKS_ROOT", tasks_root)
    monkeypatch.setattr(
        services,
        "get_run_detail",
        lambda run_id: services.RunDetail(
            run_id=run_id,
            output_dir=str(execution_panel.parent),
            quick_summary=RunQuickSummary(run_id=run_id, output_dir=str(execution_panel.parent), recipe_names=["baseline"], artifact_status="ready"),
            config={"execution_panel_path": str(execution_panel)},
            recipe_registry={},
            promotion_gate={},
            nodes=[],
            recipes=[],
            artifact_inventory=[],
        ),
    )

    request = services.RunNativeWorkflowTaskRequest(
        display_name="rerun task",
        requested_by="webapp",
        recipe_names=["baseline"],
        source_ref=services.TaskSourceRef(kind="run", source_id="source_run", label="source_run", path=str(execution_panel.parent)),
        config_payload={
            "panel_path": "artifacts/panels/csi300_weekly_20260410.parquet",
            "output_dir": "artifacts/native_workflow/target_run",
            "execution_panel_path": "artifacts/native_workflow/target_run/csi300_weekly_20260410.parquet",
            "universe_profile": "csi300",
            "run_export": "never",
        },
    )

    task = services.create_native_workflow_task(request)

    assert task.config_payload["config_payload"]["execution_panel_path"] == str(execution_panel)
    assert "--execution-panel" in task.command
    assert str(execution_panel) in task.command


def test_create_research_analysis_task_uses_analysis_script(monkeypatch, tmp_path):
    monkeypatch.setattr(services, "TASKS_ROOT", tmp_path / "tasks")
    monkeypatch.setattr(services, "_resolve_artifact_path", lambda value, base_dir=services.PROJECT_ROOT: Path(value) if value else None)

    task = services.create_research_analysis_task(
        RunResearchAnalysisTaskRequest(
            display_name="Analyze demo",
            source_ref=services.TaskSourceRef(kind="run", source_id="demo_run", label="demo_run"),
            source_kind="run",
            run_id="demo_run",
            analysis_template="investment_report",
            analysis_engine="codex_cli",
            skills=["skill_a"],
            output_dir=str(tmp_path / "analysis"),
        )
    )

    assert task.task_kind == "run_research_analysis"
    assert "scripts/run_research_analysis.py" in " ".join(task.command)
    assert "--analysis-engine" in task.command
    assert task.config_payload["source_kind"] == "run"


def test_create_research_analysis_task_supports_run_and_all_recipes_batch(monkeypatch, tmp_path):
    monkeypatch.setattr(services, "TASKS_ROOT", tmp_path / "tasks")
    monkeypatch.setattr(services, "_resolve_artifact_path", lambda value, base_dir=services.PROJECT_ROOT: Path(value) if value else None)

    task = services.create_research_analysis_task(
        RunResearchAnalysisTaskRequest(
            display_name="Analyze demo batch",
            source_ref=services.TaskSourceRef(kind="run", source_id="demo_run", label="demo_run"),
            source_kind="run",
            batch_mode="run_plus_all_recipes",
            run_id="demo_run",
            analysis_template="investment_report",
            analysis_engine="codex_cli",
            output_dir=str(tmp_path / "analysis"),
        )
    )

    assert "--batch-mode" in task.command
    assert "run_plus_all_recipes" in task.command
    assert task.config_payload["batch_mode"] == "run_plus_all_recipes"
    assert task.config_payload["include_all_recipes"] is True


def test_create_research_analysis_task_adds_native_workflow_skill(monkeypatch, tmp_path):
    monkeypatch.setattr(services, "TASKS_ROOT", tmp_path / "tasks")
    monkeypatch.setattr(services, "_resolve_artifact_path", lambda value, base_dir=services.PROJECT_ROOT: Path(value) if value else None)

    task = services.create_research_analysis_task(
        RunResearchAnalysisTaskRequest(
            display_name="Diagnose demo",
            source_ref=services.TaskSourceRef(kind="run", source_id="demo_run", label="demo_run"),
            source_kind="run",
            run_id="demo_run",
            analysis_template="native_workflow_system_report",
            analysis_engine="codex_cli",
            output_dir=str(tmp_path / "analysis"),
        )
    )

    assert "--skill" in task.command
    assert "native-workflow-artifact-analysis" in task.command
    assert task.config_payload["skills"] == ["native-workflow-artifact-analysis"]


def test_get_run_analysis_task_preset_defaults_to_batch_mode(monkeypatch):
    monkeypatch.setattr(
        services,
        "get_run_detail",
        lambda run_id: services.RunDetail(
            run_id=run_id,
            output_dir=f"/tmp/{run_id}",
            quick_summary=services.RunQuickSummary(
                run_id=run_id,
                output_dir=f"/tmp/{run_id}",
                recipe_names=["baseline", "rank_blended"],
                baseline_recipe="baseline",
                artifact_status="ready",
            ),
            config={},
            recipe_registry={},
            promotion_gate={},
            research_summary=services.ResearchSummary(headline="run headline"),
            nodes=[],
            recipes=[],
            analysis_reports=[],
            artifact_inventory=[],
        ),
    )

    preset = services.get_run_analysis_task_preset("demo_run")

    assert preset.payload["source_kind"] == "run"
    assert preset.payload["batch_mode"] == "run_only"
    assert preset.payload["include_all_recipes"] is False
    assert preset.payload["analysis_template"] == "native_workflow_system_report"
    assert preset.payload["analysis_engine"] == "codex_cli"
    assert preset.payload["skills"] == ["native-workflow-artifact-analysis"]

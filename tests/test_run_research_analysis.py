from __future__ import annotations

import json
import importlib.util
import sys
import types
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.modules.pop("scripts", None)
sys.path.insert(0, str(PROJECT_ROOT))
_SPEC = importlib.util.spec_from_file_location(
    "run_research_analysis",
    PROJECT_ROOT / "scripts" / "run_research_analysis.py",
)
assert _SPEC and _SPEC.loader
run_research_analysis = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(run_research_analysis)


def test_build_local_markdown_uses_research_memo_sections():
    content = run_research_analysis._build_local_markdown(
        {
            "headline": "Demo",
            "verdict": "hold",
            "key_findings": ["finding"],
            "risks": ["risk"],
            "recommended_next_experiments": ["next step"],
            "recommended_action": "continue baseline",
            "current_problem": "execution gap is still unclear",
            "metrics": {"a": 1},
        },
        template="investment_report",
        engine="auto",
        skills=[],
    )

    assert "## 主线建议" in content
    assert "## 当前问题" in content
    assert "finding" in content
    assert "UI Recommendations" not in content


def test_build_cli_prompt_requires_structured_markdown_without_json():
    prompt = run_research_analysis._build_cli_prompt(
        {
            "source_kind": "run",
            "headline": "Demo",
            "key_findings": ["finding"],
        },
        template="investment_report",
        skills=[],
    )

    assert "`## 主线建议`" in prompt
    assert "`## Recommended Next Actions`" in prompt
    assert "不要输出 JSON" in prompt
    assert "当前 source_kind 的重点" in prompt


def test_build_cli_prompt_requires_native_workflow_json_contract():
    prompt = run_research_analysis._build_cli_prompt(
        {
            "source_kind": "run",
            "run_id": "demo_run",
            "evidence_pack": {"run_id": "demo_run", "recipe_matrix": []},
        },
        template="native_workflow_system_report",
        skills=["native-workflow-artifact-analysis"],
    )

    assert "严格只输出一个 JSON 对象" in prompt
    assert "native-workflow-artifact-analysis" in prompt
    assert "execution_diff_summary.csv" in prompt
    assert "新旧 run summary 比较" in prompt
    assert "四舍五入" in prompt
    assert "markdown" in prompt
    assert "排名、recipe、角色、总收益、最大回撤、Rank IC IR、TopK 超额、分数区分度、行业暴露、研究判断" in prompt


def test_native_workflow_skill_path_prefers_repo_copy():
    assert run_research_analysis.NATIVE_WORKFLOW_ANALYSIS_SKILL_PATH == (
        PROJECT_ROOT / "skills" / "native-workflow-artifact-analysis" / "SKILL.md"
    )


def test_evidence_pack_detects_csi300_fixture_risks():
    pack = run_research_analysis.build_evidence_pack(
        Path("artifacts/native_workflow/csi300_2016_20260410_a1_industry")
    )

    assert pack["lead_recipe"] == "rank_blended"
    assert pack["verdict"] == "investigate"
    assert len(pack["recipe_matrix"]) == 5
    roles = {row["recipe"]: row["role"] for row in pack["recipe_matrix"]}
    assert roles["rank_blended"] == "lead"
    assert roles["binary_4w"] == "diagnose_only"
    assert roles["mae_4w"] == "filter"
    assert any(gap["type"] == "empty_execution_diff" for gap in pack["evidence_gaps"])
    assert any("signal_realization_bridge" in gap.get("message", "") for gap in pack["evidence_gaps"])


def test_native_workflow_report_uses_readable_chinese_summary():
    pack = run_research_analysis.build_evidence_pack(
        Path("artifacts/native_workflow/csi300_2016_20260410_a1_industry")
    )

    report = run_research_analysis.build_system_report_from_evidence_pack(pack)
    markdown = report["markdown"]

    assert "## Executive Verdict" in markdown
    assert "rank_blended 暂时最适合作为下一轮主线" in markdown
    assert "67.5%" in markdown
    assert "| 排名 | recipe | 角色 | 总收益 | 最大回撤 | TopK 超额 | 分数区分度 | 研究判断 |" in markdown
    assert "0.6745489921187544" not in markdown
    assert "walk_forward_performance_metrics.csv" not in markdown
    assert "新旧 run summary" not in markdown


def test_evidence_pack_detects_csi500_investigate_fixture():
    pack = run_research_analysis.build_evidence_pack(
        Path("artifacts/native_workflow/csi500_2016_20260410_a1_industry")
    )

    assert pack["lead_recipe"] == "rank_blended"
    assert pack["verdict"] == "investigate"
    roles = {row["recipe"]: row["role"] for row in pack["recipe_matrix"]}
    assert roles["binary_4w"] == "diagnose_only"
    assert any(row["walk_forward_max_drawdown"] <= -0.30 for row in pack["recipe_matrix"])


def test_prepare_native_workflow_payload_omits_large_analysis_reports(tmp_path):
    payload = {
        "source_kind": "run",
        "run_id": "csi300_2016_20260410_a1",
        "analysis_reports": [{"content_preview": "x" * 1_100_000}],
    }

    prepared, manifest = run_research_analysis._prepare_native_workflow_payload(
        payload,
        cwd=Path("artifacts/native_workflow/csi300_2016_20260410_a1"),
        output_dir=tmp_path,
    )
    prompt = run_research_analysis._build_cli_prompt(
        prepared,
        template="native_workflow_system_report",
        skills=["native-workflow-artifact-analysis"],
    )

    assert "analysis_reports" not in prepared
    assert manifest["analysis_reports_omitted"] == "analysis_reports"
    assert prepared["evidence_pack"]
    assert Path(manifest["evidence_pack_path"]).exists()
    assert len(prompt.encode("utf-8")) < 1_048_576


def test_invoke_codex_cli_runs_subprocess(monkeypatch):
    calls: list[list[str]] = []
    stdin_prompts: list[str | None] = []

    def fake_which(name: str) -> str | None:
        return f"/usr/local/bin/{name}"

    def fake_run(command, input=None, text=None, capture_output=None, cwd=None, check=None):
        calls.append(command)
        stdin_prompts.append(input)
        return subprocess.CompletedProcess(command, 0, stdout="codex output", stderr="")

    monkeypatch.setattr(run_research_analysis.shutil, "which", fake_which)
    monkeypatch.setattr(run_research_analysis.subprocess, "run", fake_run)

    result = run_research_analysis._invoke_codex_cli(
        {"headline": "Demo"},
        template="investment_report",
        skills=[],
        cwd=Path("/tmp"),
    )

    assert result["engine"] == "codex_cli"
    assert calls
    assert calls[0][0].endswith("codex")
    assert "exec" in calls[0]
    assert calls[0][-1] == "-"
    assert stdin_prompts[0]
    assert "Demo" in stdin_prompts[0]
    assert "Demo" not in calls[0]


def test_generate_analysis_content_falls_back_locally_when_codex_models_are_at_capacity(monkeypatch):
    capacity_error = RuntimeError("ERROR: Selected model is at capacity. Please try a different model.")

    def fake_invoke(payload, *, template, skills, cwd):
        raise capacity_error

    def fake_invoke_with_model(payload, *, template, skills, cwd, model):
        raise capacity_error

    monkeypatch.setattr(run_research_analysis, "_invoke_codex_cli", fake_invoke)
    monkeypatch.setattr(run_research_analysis, "_invoke_codex_cli_with_model", fake_invoke_with_model)

    content, metadata = run_research_analysis._generate_analysis_content(
        {"source_kind": "run", "run_id": "demo_run"},
        analysis_template="native_workflow_system_report",
        analysis_engine="codex_cli",
        skills=[],
        cwd=Path("."),
    )

    parsed = json.loads(content)
    assert metadata["engine_used"] == "codex_cli_capacity_fallback_local"
    assert metadata["codex_retry_count"] >= 1
    assert isinstance(metadata.get("codex_capacity_retries"), list)
    assert parsed["verdict"] == "investigate"
    assert "markdown" in parsed


def test_invoke_claude_cli_runs_subprocess(monkeypatch):
    calls: list[list[str]] = []
    stdin_prompts: list[str | None] = []

    def fake_which(name: str) -> str | None:
        return f"/usr/local/bin/{name}"

    def fake_run(command, input=None, text=None, capture_output=None, cwd=None, check=None):
        calls.append(command)
        stdin_prompts.append(input)
        return subprocess.CompletedProcess(command, 0, stdout="claude output", stderr="")

    monkeypatch.setattr(run_research_analysis.shutil, "which", fake_which)
    monkeypatch.setattr(run_research_analysis.subprocess, "run", fake_run)

    result = run_research_analysis._invoke_claude_cli(
        {"headline": "Demo"},
        template="investment_report",
        skills=[],
        cwd=Path("/tmp"),
    )

    assert result["engine"] == "claude_cli"
    assert calls
    assert calls[0][0].endswith("claude")
    assert "-p" in calls[0]
    assert stdin_prompts[0]
    assert "Demo" in stdin_prompts[0]
    assert "Demo" not in calls[0]


def test_invoke_gemini_cli_runs_subprocess(monkeypatch):
    calls: list[list[str]] = []
    stdin_prompts: list[str | None] = []

    def fake_which(name: str) -> str | None:
        return f"/usr/local/bin/{name}"

    def fake_run(command, input=None, text=None, capture_output=None, cwd=None, check=None):
        calls.append(command)
        stdin_prompts.append(input)
        return subprocess.CompletedProcess(command, 0, stdout="gemini output", stderr="")

    monkeypatch.setattr(run_research_analysis.shutil, "which", fake_which)
    monkeypatch.setattr(run_research_analysis.subprocess, "run", fake_run)

    result = run_research_analysis._invoke_gemini_cli(
        {"headline": "Demo"},
        template="investment_report",
        skills=[],
        cwd=Path("/tmp"),
    )

    assert result["engine"] == "gemini_cli"
    assert calls
    assert calls[0][0].endswith("gemini")
    assert "--prompt" in calls[0]
    assert "--approval-mode" in calls[0]
    assert stdin_prompts[0]
    assert "Demo" in stdin_prompts[0]
    assert "Demo" not in calls[0]


def test_run_single_native_workflow_analysis_falls_back_on_invalid_cli_json(monkeypatch, tmp_path):
    pack = {
        "run_id": "demo_run",
        "lead_recipe": "rank_blended",
        "verdict": "investigate",
        "recipe_matrix": [
            {
                "rank": 1,
                "recipe": "rank_blended",
                "role": "lead",
                "score": 0.1,
                "walk_forward_net_total_return": 0.2,
                "walk_forward_max_drawdown": -0.1,
                "walk_forward_topk_mean_excess_return_4w": -0.01,
                "signal_unique_mean": 0.9,
                "actual_hold_max": 10,
            }
        ],
        "recipe_dossiers": {
            "rank_blended": {
                "metrics": {
                    "walk_forward_rank_ic_ir": 0.2,
                    "walk_forward_topk_mean_excess_return_4w": -0.01,
                    "walk_forward_net_total_return": 0.2,
                    "walk_forward_max_drawdown": -0.1,
                },
                "latest_snapshot": {"top_industries": {"银行": 4}, "score_gap_10_20": 0.02, "target_rows": 10},
            }
        },
        "system_findings": ["finding"],
        "evidence_gaps": [{"message": "gap"}],
        "next_experiments": [],
        "evidence_refs": [],
    }

    def fake_prepare(payload, *, cwd, output_dir):
        return {
            **payload,
            "evidence_pack": pack,
            "run_artifact_dir": str(cwd),
            "skill_name": "native-workflow-artifact-analysis",
            "skill_path": "/tmp/skill/SKILL.md",
            "evidence_pack_path": str(output_dir / "evidence_pack.json"),
            "recipe_matrix_path": str(output_dir / "recipe_matrix.csv"),
        }, {}

    monkeypatch.setattr(run_research_analysis, "_prepare_native_workflow_payload", fake_prepare)
    monkeypatch.setattr(
        run_research_analysis,
        "_invoke_codex_cli",
        lambda payload, template, skills, cwd: {"engine": "codex_cli", "command": ["codex"], "content": "not json"},
    )

    result = run_research_analysis._run_single_analysis(
        payload={"source_kind": "run", "run_id": "demo_run"},
        output_dir=tmp_path,
        analysis_template="native_workflow_system_report",
        analysis_engine="codex_cli",
        skills=[],
        cwd=Path("/tmp/demo_run"),
    )

    summary = json.loads((tmp_path / "latest_summary.json").read_text(encoding="utf-8"))
    assert result["engine_used"] == "codex_cli"
    assert summary["lead_recipe"] == "rank_blended"
    assert summary["validation_warnings"]
    markdown = (tmp_path / "latest_summary.md").read_text(encoding="utf-8")
    assert "# demo_run 系统诊断报告" in markdown
    assert "| 排名 | recipe | 角色 | 总收益 | 最大回撤 | TopK 超额 | 分数区分度 | 研究判断 |" in markdown


def test_run_single_native_workflow_analysis_writes_valid_json_report(monkeypatch, tmp_path):
    report = {
        "headline": "Demo headline",
        "verdict": "investigate",
        "lead_recipe": "rank_blended",
        "recipe_rankings": [],
        "system_findings": ["finding"],
        "live_feasibility": {"status": "caution", "summary": "summary", "checks": []},
        "next_experiments": [],
        "evidence_refs": [],
        "markdown": (
            "# demo_run 系统诊断报告\n\n"
            "## Executive Verdict\n"
            "- verdict: investigate\n\n"
            "## Recipe Ranking & Roles\n"
            "| recipe | role |\n"
            "| --- | --- |\n"
            "| stale | stale |\n"
        ),
    }

    def fake_prepare(payload, *, cwd, output_dir):
        return {
            **payload,
            "evidence_pack": {
                "run_id": "demo_run",
                "recipe_matrix": [
                    {
                        "rank": 1,
                        "recipe": "rank_blended",
                        "role": "lead",
                        "walk_forward_net_total_return": 0.2,
                        "walk_forward_max_drawdown": -0.1,
                        "walk_forward_topk_mean_excess_return_4w": -0.01,
                        "signal_unique_mean": 0.9,
                    }
                ],
            },
            "evidence_pack_path": str(output_dir / "evidence_pack.json"),
            "recipe_matrix_path": str(output_dir / "recipe_matrix.csv"),
        }, {}

    monkeypatch.setattr(run_research_analysis, "_prepare_native_workflow_payload", fake_prepare)
    monkeypatch.setattr(
        run_research_analysis,
        "_invoke_codex_cli",
        lambda payload, template, skills, cwd: {"engine": "codex_cli", "command": ["codex"], "content": json.dumps(report)},
    )

    run_research_analysis._run_single_analysis(
        payload={"source_kind": "run", "run_id": "demo_run"},
        output_dir=tmp_path,
        analysis_template="native_workflow_system_report",
        analysis_engine="codex_cli",
        skills=[],
        cwd=Path("/tmp/demo_run"),
    )

    summary = json.loads((tmp_path / "latest_summary.json").read_text(encoding="utf-8"))
    assert summary["headline"] == "Demo headline"
    assert summary["lead_recipe"] == "rank_blended"
    markdown = (tmp_path / "latest_summary.md").read_text(encoding="utf-8")
    assert markdown.startswith("# demo_run")
    assert "| 排名 | recipe | 角色 | 总收益 | 最大回撤 | TopK 超额 | 分数区分度 | 研究判断 |" in markdown
    assert "| stale | stale |" not in markdown


def test_run_batch_analysis_runs_run_and_all_recipes(monkeypatch, tmp_path):
    calls: list[tuple[str, str]] = []

    monkeypatch.setattr(run_research_analysis, "_run_payload", lambda run_id: {"source_kind": "run", "run_id": run_id})
    monkeypatch.setattr(
        run_research_analysis,
        "_recipe_payload",
        lambda run_id, recipe_name: {"source_kind": "recipe", "run_id": run_id, "recipe_name": recipe_name},
    )
    monkeypatch.setattr(
        run_research_analysis,
        "get_run_detail",
        lambda run_id: types.SimpleNamespace(
            recipes=[
                types.SimpleNamespace(recipe_name="baseline"),
                types.SimpleNamespace(recipe_name="rank_blended"),
            ]
        ),
    )

    def fake_run_single_analysis(**kwargs):
        payload = kwargs["payload"]
        calls.append((str(payload.get("source_kind")), str(payload.get("recipe_name") or payload.get("run_id"))))
        output_dir = kwargs["output_dir"]
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "manifest.json").write_text("{}", encoding="utf-8")
        return {"output_dir": str(output_dir), "generated_files": [], "engine_used": "auto", "cli_invoked": False}

    monkeypatch.setattr(run_research_analysis, "_run_single_analysis", fake_run_single_analysis)

    args = types.SimpleNamespace(
        source_kind="run",
        batch_mode="run_plus_all_recipes",
        include_all_recipes=True,
        run_id="demo_run",
        analysis_template="investment_report",
        analysis_engine="auto",
        skill=[],
    )

    exit_code = run_research_analysis._run_batch_analysis(args, tmp_path / "analysis")

    assert exit_code == 0
    assert calls == [("run", "demo_run"), ("recipe", "baseline"), ("recipe", "rank_blended")]
    manifest = (tmp_path / "analysis" / "manifest.json").read_text(encoding="utf-8")
    assert "recipe_results" in manifest


def test_run_batch_analysis_runs_run_and_lead_recipe(monkeypatch, tmp_path):
    calls: list[tuple[str, str]] = []

    monkeypatch.setattr(run_research_analysis, "_run_payload", lambda run_id: {"source_kind": "run", "run_id": run_id})
    monkeypatch.setattr(
        run_research_analysis,
        "_recipe_payload",
        lambda run_id, recipe_name: {"source_kind": "recipe", "run_id": run_id, "recipe_name": recipe_name},
    )
    monkeypatch.setattr(
        run_research_analysis,
        "get_run_detail",
        lambda run_id: types.SimpleNamespace(
            quick_summary=types.SimpleNamespace(incumbent_recipe="rank_blended", baseline_recipe="baseline"),
            recipes=[
                types.SimpleNamespace(recipe_name="baseline"),
                types.SimpleNamespace(recipe_name="rank_blended"),
            ],
        ),
    )

    def fake_run_single_analysis(**kwargs):
        payload = kwargs["payload"]
        calls.append((str(payload.get("source_kind")), str(payload.get("recipe_name") or payload.get("run_id"))))
        output_dir = kwargs["output_dir"]
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "manifest.json").write_text("{}", encoding="utf-8")
        return {"output_dir": str(output_dir), "generated_files": [], "engine_used": "auto", "cli_invoked": False}

    monkeypatch.setattr(run_research_analysis, "_run_single_analysis", fake_run_single_analysis)

    args = types.SimpleNamespace(
        source_kind="run",
        batch_mode="run_plus_lead_recipe",
        include_all_recipes=False,
        run_id="demo_run",
        analysis_template="investment_report",
        analysis_engine="auto",
        skill=[],
    )

    exit_code = run_research_analysis._run_batch_analysis(args, tmp_path / "analysis")

    assert exit_code == 0
    assert calls == [("run", "demo_run"), ("recipe", "rank_blended")]

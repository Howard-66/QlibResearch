from __future__ import annotations

import subprocess
from pathlib import Path

from scripts import run_research_analysis


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


def test_invoke_codex_cli_runs_subprocess(monkeypatch):
    calls: list[list[str]] = []

    def fake_which(name: str) -> str | None:
        return f"/usr/local/bin/{name}"

    def fake_run(command, input=None, text=None, capture_output=None, cwd=None, check=None):
        calls.append(command)
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


def test_invoke_claude_cli_runs_subprocess(monkeypatch):
    calls: list[list[str]] = []

    def fake_which(name: str) -> str | None:
        return f"/usr/local/bin/{name}"

    def fake_run(command, input=None, text=None, capture_output=None, cwd=None, check=None):
        calls.append(command)
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

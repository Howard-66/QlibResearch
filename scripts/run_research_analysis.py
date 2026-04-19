from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from pathlib import Path
from typing import Any

from qlib_research.app.contracts import CompareItemRef
from qlib_research.app.services import compare_recipe_items, get_recipe_detail, get_run_detail
from qlib_research.core.notebook_workflow import sanitize_for_json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate structured research analysis artifacts.")
    parser.add_argument("--source-kind", choices=["run", "recipe", "compare"], required=True)
    parser.add_argument("--include-all-recipes", action="store_true")
    parser.add_argument("--run-id")
    parser.add_argument("--recipe-name")
    parser.add_argument("--compare-items-json")
    parser.add_argument("--analysis-template", default="investment_report")
    parser.add_argument("--analysis-engine", default="codex_cli")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--skill", action="append", default=[])
    return parser.parse_args()


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item is not None]


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(sanitize_for_json(payload), ensure_ascii=False, indent=2), encoding="utf-8")


def _write_markdown(path: Path, content: str) -> None:
    path.write_text(content.strip() + "\n", encoding="utf-8")


def _run_payload(run_id: str) -> dict[str, Any]:
    detail = get_run_detail(run_id)
    summary = detail.research_summary.model_dump(mode="json")
    return {
        "source_kind": "run",
        "run_id": run_id,
        "headline": summary.get("headline") or f"Run {run_id} 研究摘要",
        "verdict": summary.get("verdict") or detail.quick_summary.research_status,
        "key_findings": summary.get("key_findings") or [],
        "risks": summary.get("risks") or [],
        "recommended_next_experiments": summary.get("recommended_next_actions") or [],
        "current_problem": summary.get("current_problem"),
        "recommended_action": summary.get("recommended_action"),
        "metrics": summary.get("metrics") or detail.quick_summary.baseline_metrics,
        "analysis_reports": [report.model_dump(mode="json") for report in detail.analysis_reports],
    }


def _recipe_payload(run_id: str, recipe_name: str) -> dict[str, Any]:
    detail = get_recipe_detail(run_id, recipe_name)
    summary = detail.research_summary.model_dump(mode="json")
    return {
        "source_kind": "recipe",
        "run_id": run_id,
        "recipe_name": recipe_name,
        "headline": summary.get("headline") or f"{recipe_name} 实验摘要",
        "verdict": summary.get("verdict"),
        "key_findings": summary.get("key_findings") or [],
        "risks": summary.get("risks") or [],
        "recommended_next_experiments": summary.get("recommended_next_actions") or [],
        "current_problem": summary.get("current_problem"),
        "recommended_action": summary.get("recommended_action"),
        "metrics": summary.get("metrics") or detail.overview,
        "analysis_reports": [report.model_dump(mode="json") for report in detail.analysis_reports],
    }


def _compare_payload(compare_items_json: str) -> dict[str, Any]:
    refs = [CompareItemRef(**item) for item in json.loads(compare_items_json)]
    response = compare_recipe_items(refs)
    summary = response.analysis_summary.model_dump(mode="json")
    return {
        "source_kind": "compare",
        "headline": summary.get("headline") or "Compare 分析摘要",
        "verdict": summary.get("verdict"),
        "key_findings": summary.get("key_findings") or [],
        "risks": summary.get("risks") or [],
        "recommended_next_experiments": summary.get("recommended_next_actions") or [],
        "metrics": summary.get("metrics") or {},
        "items": [item.model_dump(mode="json") for item in response.items],
    }


def _build_local_markdown(payload: dict[str, Any], *, template: str, engine: str, skills: list[str]) -> str:
    recommended_action = str(
        payload.get("recommended_action")
        or (payload.get("recommended_next_experiments") or [None])[0]
        or "延续当前主线，但先用新一轮实验验证收益解释链与风险约束。"
    )
    current_problem = str(
        payload.get("current_problem")
        or "当前最大问题不是单点收益，而是信号解释、组合兑现与风险暴露之间的桥接仍不够透明。"
    )
    lines = [
        f"# {payload.get('headline') or 'Research Analysis'}",
        "",
        f"- verdict: {payload.get('verdict') or 'n/a'}",
        f"- template: {template}",
        f"- engine: {engine}",
        f"- skills: {', '.join(skills) if skills else 'none'}",
        "",
        "## 主线建议",
        f"- {recommended_action}",
        "",
        "## 当前问题",
        f"- {current_problem}",
        "",
        "## Key Findings",
    ]
    findings = _string_list(payload.get("key_findings"))
    lines.extend([f"- {item}" for item in findings] or ["- 暂无结构化发现"])
    lines.extend(["", "## Risks"])
    risks = _string_list(payload.get("risks"))
    lines.extend([f"- {item}" for item in risks] or ["- 暂无高优先级风险"])
    lines.extend(["", "## Recommended Next Actions"])
    actions = _string_list(payload.get("recommended_next_experiments"))
    lines.extend([f"- {item}" for item in actions] or ["- 暂无明确下一步动作"])
    if payload.get("metrics"):
        lines.extend(["", "## Metrics Snapshot"])
        metrics = payload.get("metrics")
        if isinstance(metrics, dict):
            scalar_metrics = [
                f"- {key}: {value}"
                for key, value in metrics.items()
                if isinstance(value, (str, int, float, bool)) and value is not None
            ][:8]
            lines.extend(scalar_metrics or ["- 指标较多，建议结合原始结构化产物继续查看"])
        else:
            lines.append("- 指标较多，建议结合原始结构化产物继续查看")
    return "\n".join(lines)


def _build_cli_prompt(payload: dict[str, Any], *, template: str, skills: list[str]) -> str:
    prompt_payload = {
        "template": template,
        "skills": skills,
        "analysis_input": payload,
    }
    source_kind = str(payload.get("source_kind") or "")
    focus_map = {
        "run": "需要在多个 recipe 之间做主线判断，明确谁应继续推进、谁只适合作为备选或过滤器，并解释收益、回撤、信号质量与组合兑现之间的关系。",
        "recipe": "需要像策略研究员一样诊断单个 recipe 的收益来源、回撤特征、信号兑现、行业暴露和下一轮实验重点，而不是只重复指标表。",
        "compare": "需要给出清晰的 winner / hold / reject / investigate 结论，说明胜负依据、指标变化方向，以及是否通过实验 gate。",
    }
    return (
        "你正在为 Qlib Research Workbench 生成 `latest_summary.md`。\n"
        "这是一份内部研究备忘录，不是泛泛的摘要，也不是把输入数据重新誊抄一遍。\n"
        "请保留研究员式的深度、比较与优先级判断，但把结构收敛到前端可稳定拆块展示的 markdown。\n\n"
        "输出要求：\n"
        "1. 使用中文。\n"
        "2. 严格输出 markdown，不要输出 JSON、代码块或原始输入 dump。\n"
        "3. 必须包含以下结构：\n"
        "   - `# 标题`\n"
        "   - 顶部元信息列表，至少包含 `- verdict: ...`\n"
        "   - `## 主线建议`\n"
        "   - `## 当前问题`\n"
        "   - `## Key Findings`\n"
        "   - `## Risks`\n"
        "   - `## Recommended Next Actions`\n"
        "4. 在上述必选章节之外，可以增加 1-3 个最有价值的深度章节。优先考虑：`收益与风险画像`、`信号与组合兑现`、`因子与行业暴露`、`当前组合观察`、`方法学约束`、`实验优先级`。\n"
        "5. `主线建议` 和 `当前问题` 的第一条必须是明确判断，尽量用一句话概括，不要模糊。\n"
        "6. `Key Findings`、`Risks`、`Recommended Next Actions` 请尽量条目化，每节 3-6 条；需要时可以在节内补 1-2 句短段落或使用 `###` 小节。\n"
        "7. 允许有研究自由度，但要像之前深入的投资分析报告那样：先结论，再解释收益/回撤/信号/暴露/方法学，再给下一步动作。\n"
        "8. 不要编造不存在的数据；如果输入不足以支撑某个判断，要明确指出“不足以判断”的具体点。\n"
        "9. template 只作为元信息，不需要改变整体输出协议。\n"
        f"10. 当前 source_kind 的重点：{focus_map.get(source_kind, '请在结论、风险和下一步动作之间形成完整闭环。')}\n\n"
        f"{json.dumps(prompt_payload, ensure_ascii=False, indent=2)}"
    )


def _run_subprocess(command: list[str], *, prompt: str | None, cwd: Path) -> str:
    result = subprocess.run(
        command,
        input=prompt,
        text=True,
        capture_output=True,
        cwd=str(cwd),
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"CLI command failed with exit code {result.returncode}: "
            f"{result.stderr.strip() or result.stdout.strip() or 'no output'}"
        )
    output = result.stdout.strip()
    if not output:
        raise RuntimeError("CLI command returned empty output")
    return output


def _invoke_codex_cli(payload: dict[str, Any], *, template: str, skills: list[str], cwd: Path) -> dict[str, Any]:
    executable = shutil.which("codex")
    if not executable:
        raise RuntimeError("codex executable not found")
    prompt = _build_cli_prompt(payload, template=template, skills=skills)
    command = [
        executable,
        "exec",
        "--skip-git-repo-check",
        "--sandbox",
        "read-only",
        "--cd",
        str(cwd),
        prompt,
    ]
    content = _run_subprocess(command, prompt=None, cwd=cwd)
    return {
        "engine": "codex_cli",
        "command": command,
        "content": content,
    }


def _invoke_claude_cli(payload: dict[str, Any], *, template: str, skills: list[str], cwd: Path) -> dict[str, Any]:
    executable = shutil.which("claude")
    if not executable:
        raise RuntimeError("claude executable not found")
    prompt = _build_cli_prompt(payload, template=template, skills=skills)
    command = [
        executable,
        "-p",
        "--output-format",
        "text",
        "--permission-mode",
        "default",
        prompt,
    ]
    content = _run_subprocess(command, prompt=None, cwd=cwd)
    return {
        "engine": "claude_cli",
        "command": command,
        "content": content,
    }


def _analysis_context(args: argparse.Namespace) -> tuple[dict[str, Any], Path]:
    if args.source_kind == "run":
        if not args.run_id:
            raise SystemExit("--run-id is required for source-kind=run")
        return _run_payload(args.run_id), Path("artifacts/native_workflow") / args.run_id
    if args.source_kind == "recipe":
        if not args.run_id or not args.recipe_name:
            raise SystemExit("--run-id and --recipe-name are required for source-kind=recipe")
        return _recipe_payload(args.run_id, args.recipe_name), Path("artifacts/native_workflow") / args.run_id / args.recipe_name
    if not args.compare_items_json:
        raise SystemExit("--compare-items-json is required for source-kind=compare")
    return _compare_payload(args.compare_items_json), Path(".")


def _generate_analysis_content(
    payload: dict[str, Any],
    *,
    analysis_template: str,
    analysis_engine: str,
    skills: list[str],
    cwd: Path,
) -> tuple[str, dict[str, Any]]:
    if analysis_engine == "auto":
        return _build_local_markdown(
            payload,
            template=analysis_template,
            engine="auto",
            skills=skills,
        ), {"engine_used": "auto", "cli_invoked": False}
    if analysis_engine == "codex_cli":
        cli_result = _invoke_codex_cli(payload, template=analysis_template, skills=skills, cwd=cwd.resolve())
        return str(cli_result["content"]), {
            "engine_used": "codex_cli",
            "cli_invoked": True,
            "cli_command": cli_result["command"],
        }
    if analysis_engine == "claude_cli":
        cli_result = _invoke_claude_cli(payload, template=analysis_template, skills=skills, cwd=cwd.resolve())
        return str(cli_result["content"]), {
            "engine_used": "claude_cli",
            "cli_invoked": True,
            "cli_command": cli_result["command"],
        }
    raise SystemExit(f"Unsupported analysis engine: {analysis_engine}")


def _write_analysis_artifacts(
    *,
    output_dir: Path,
    payload: dict[str, Any],
    content: str,
    cli_metadata: dict[str, Any],
    analysis_template: str,
    extra_manifest: dict[str, Any] | None = None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_json = {
        **payload,
        **cli_metadata,
    }
    summary_json_path = output_dir / "latest_summary.json"
    summary_md_path = output_dir / "latest_summary.md"
    generated_files = [str(summary_json_path), str(summary_md_path)]
    _write_json(summary_json_path, summary_json)
    _write_markdown(summary_md_path, content)
    manifest_payload = {
        "generated_files": generated_files,
        **cli_metadata,
        "template": analysis_template,
    }
    if extra_manifest:
        manifest_payload.update(sanitize_for_json(extra_manifest))
    _write_json(output_dir / "manifest.json", manifest_payload)
    return {
        "output_dir": str(output_dir),
        "generated_files": generated_files,
        "engine_used": cli_metadata.get("engine_used"),
        "cli_invoked": cli_metadata.get("cli_invoked", False),
    }


def _run_single_analysis(
    *,
    payload: dict[str, Any],
    output_dir: Path,
    analysis_template: str,
    analysis_engine: str,
    skills: list[str],
    cwd: Path,
    extra_manifest: dict[str, Any] | None = None,
) -> dict[str, Any]:
    content, cli_metadata = _generate_analysis_content(
        payload,
        analysis_template=analysis_template,
        analysis_engine=analysis_engine,
        skills=skills,
        cwd=cwd,
    )
    return _write_analysis_artifacts(
        output_dir=output_dir,
        payload=payload,
        content=content,
        cli_metadata=cli_metadata,
        analysis_template=analysis_template,
        extra_manifest=extra_manifest,
    )


def _run_batch_analysis(args: argparse.Namespace, run_output_dir: Path) -> int:
    if args.source_kind != "run" or not args.run_id:
        raise SystemExit("--include-all-recipes is only supported with source-kind=run and --run-id")

    run_payload = _run_payload(args.run_id)
    run_payload.update({"template": args.analysis_template, "engine": args.analysis_engine, "skills": args.skill})
    run_result = _run_single_analysis(
        payload=run_payload,
        output_dir=run_output_dir,
        analysis_template=args.analysis_template,
        analysis_engine=args.analysis_engine,
        skills=args.skill,
        cwd=(Path("artifacts/native_workflow") / args.run_id),
        extra_manifest={"batch_scope": "run_and_all_recipes"},
    )

    run_detail = get_run_detail(args.run_id)
    recipe_results: list[dict[str, Any]] = []
    for recipe in run_detail.recipes:
        recipe_payload = _recipe_payload(args.run_id, recipe.recipe_name)
        recipe_payload.update({"template": args.analysis_template, "engine": args.analysis_engine, "skills": args.skill})
        recipe_output_dir = (Path("artifacts/native_workflow") / args.run_id / recipe.recipe_name / "analysis").resolve()
        recipe_result = _run_single_analysis(
            payload=recipe_payload,
            output_dir=recipe_output_dir,
            analysis_template=args.analysis_template,
            analysis_engine=args.analysis_engine,
            skills=args.skill,
            cwd=(Path("artifacts/native_workflow") / args.run_id / recipe.recipe_name),
            extra_manifest={
                "batch_scope": "recipe_from_run_batch",
                "batch_parent_run_id": args.run_id,
            },
        )
        recipe_results.append(
            {
                "recipe_name": recipe.recipe_name,
                **recipe_result,
            }
        )

    batch_manifest_path = run_output_dir / "manifest.json"
    existing_manifest = json.loads(batch_manifest_path.read_text(encoding="utf-8"))
    existing_manifest.update(
        {
            "batch_scope": "run_and_all_recipes",
            "run_result": run_result,
            "recipe_results": recipe_results,
        }
    )
    _write_json(batch_manifest_path, existing_manifest)
    return 0


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()
    if args.include_all_recipes:
        return _run_batch_analysis(args, output_dir)

    output_dir.mkdir(parents=True, exist_ok=True)
    payload, cwd = _analysis_context(args)
    payload.update(
        {
            "template": args.analysis_template,
            "engine": args.analysis_engine,
            "skills": args.skill,
        }
    )
    _run_single_analysis(
        payload=payload,
        output_dir=output_dir,
        analysis_template=args.analysis_template,
        analysis_engine=args.analysis_engine,
        skills=args.skill,
        cwd=cwd,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

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
try:
    from scripts.build_native_workflow_evidence_pack import (
        build_evidence_pack,
        build_system_report_from_evidence_pack,
        write_evidence_outputs,
    )
except ModuleNotFoundError:  # pragma: no cover - supports direct `python scripts/run_research_analysis.py`.
    from build_native_workflow_evidence_pack import (
        build_evidence_pack,
        build_system_report_from_evidence_pack,
        write_evidence_outputs,
    )


NATIVE_WORKFLOW_SYSTEM_TEMPLATE = "native_workflow_system_report"
NATIVE_WORKFLOW_ANALYSIS_SKILL_NAME = "native-workflow-artifact-analysis"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPO_NATIVE_WORKFLOW_ANALYSIS_SKILL_PATH = PROJECT_ROOT / "skills" / NATIVE_WORKFLOW_ANALYSIS_SKILL_NAME / "SKILL.md"
USER_NATIVE_WORKFLOW_ANALYSIS_SKILL_PATH = Path.home() / ".codex" / "skills" / NATIVE_WORKFLOW_ANALYSIS_SKILL_NAME / "SKILL.md"
NATIVE_WORKFLOW_ANALYSIS_SKILL_PATH = (
    REPO_NATIVE_WORKFLOW_ANALYSIS_SKILL_PATH
    if REPO_NATIVE_WORKFLOW_ANALYSIS_SKILL_PATH.exists()
    else USER_NATIVE_WORKFLOW_ANALYSIS_SKILL_PATH
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate structured research analysis artifacts.")
    parser.add_argument("--source-kind", choices=["run", "recipe", "compare"], required=True)
    parser.add_argument("--batch-mode", choices=["run_only", "run_plus_lead_recipe", "run_plus_all_recipes"], default="run_only")
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


def _normalized_batch_mode(args: argparse.Namespace) -> str:
    batch_mode = str(getattr(args, "batch_mode", "run_only") or "run_only")
    if batch_mode in {"run_plus_lead_recipe", "run_plus_all_recipes"}:
        return batch_mode
    return "run_plus_all_recipes" if getattr(args, "include_all_recipes", False) else "run_only"


def _is_native_workflow_system_report(template: str, payload: dict[str, Any] | None = None) -> bool:
    if template != NATIVE_WORKFLOW_SYSTEM_TEMPLATE:
        return False
    return payload is None or payload.get("source_kind") == "run"


def _effective_skills(skills: list[str], template: str) -> list[str]:
    result = [str(skill) for skill in skills if str(skill).strip()]
    if template == NATIVE_WORKFLOW_SYSTEM_TEMPLATE and NATIVE_WORKFLOW_ANALYSIS_SKILL_NAME not in result:
        result.append(NATIVE_WORKFLOW_ANALYSIS_SKILL_NAME)
    return result


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
    if _is_native_workflow_system_report(template, payload):
        return str(_native_workflow_local_report(payload)["markdown"])
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


def _native_workflow_local_report(payload: dict[str, Any]) -> dict[str, Any]:
    evidence_pack = payload.get("evidence_pack")
    if not isinstance(evidence_pack, dict):
        evidence_pack = {
            "run_id": payload.get("run_id"),
            "lead_recipe": None,
            "verdict": "investigate",
            "recipe_matrix": [],
            "recipe_dossiers": {},
            "system_findings": ["缺少 evidence_pack，无法完成 run 系统诊断。"],
            "evidence_gaps": [{"message": "native workflow evidence_pack was not generated"}],
            "next_experiments": [],
            "evidence_refs": [],
        }
    return build_system_report_from_evidence_pack(evidence_pack)


def _build_cli_prompt(payload: dict[str, Any], *, template: str, skills: list[str]) -> str:
    prompt_payload = {
        "template": template,
        "skills": skills,
        "analysis_input": payload,
    }
    if _is_native_workflow_system_report(template, payload):
        return (
            "你正在为 Qlib Research Workbench 生成 native workflow run 级系统诊断报告。\n"
            f"必须使用 `{NATIVE_WORKFLOW_ANALYSIS_SKILL_NAME}` skill；skill 路径是 `{NATIVE_WORKFLOW_ANALYSIS_SKILL_PATH}`。\n"
            "这次不是泛化摘要任务。请基于 evidence_pack 和原始 run_artifact_dir 判断，而不是复述旧 scorecard。\n\n"
            "输出要求：\n"
            "1. 严格只输出一个 JSON 对象，不要输出 markdown 代码块、解释性前后缀或原始输入 dump。\n"
            "2. JSON 必须包含：headline, verdict, lead_recipe, recipe_rankings, system_findings, live_feasibility, next_experiments, evidence_refs, markdown。\n"
            "3. verdict 只能是 promote, hold, investigate, reject。\n"
            "4. 每个 recipe 都必须有角色：lead, filter, candidate, reject, diagnose_only。\n"
            "5. markdown 字段必须包含这些一级章节：Executive Verdict, Recipe Ranking & Roles, System Diagnosis, Recipe Dossiers, Live Portfolio Feasibility, Next Experiment Roadmap, Evidence Gaps。\n"
            "6. `execution_diff_summary.csv` 为空时，必须写成 validation comparison 未运行或无数据，不能写成执行差异健康。\n"
            "7. 如果 scorecard 与 raw artifact 冲突，只在 Evidence Gaps 中简洁说明冲突类型；不要在 Executive Verdict 做“新旧 run summary 比较”。\n"
            "8. 正文面向研究用户：使用自然中文，术语可保留英文；不要写“promote 到 live”这类中英文混排表达，改写成“进入实盘”。\n"
            "9. 正文数字必须四舍五入：收益/回撤/超额用百分比，普通比率保留 2 位，持仓数取整数；不要把文件名和完整小数塞进结论句。\n"
            "10. 表格只做横向比较，正文必须解释这些数字对研究决策的含义，不能只堆指标。\n"
            "11. 文件名、metric 名和完整 evidence_refs 放在 JSON 的 evidence_refs 或 Evidence Gaps 中；正文优先用中文指标名。\n\n"
            f"{json.dumps(prompt_payload, ensure_ascii=False, indent=2)}"
        )
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
        if _is_native_workflow_system_report(analysis_template, payload):
            return json.dumps(_native_workflow_local_report(payload), ensure_ascii=False), {
                "engine_used": "auto",
                "cli_invoked": False,
            }
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


def _prepare_native_workflow_payload(
    payload: dict[str, Any],
    *,
    cwd: Path,
    output_dir: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    run_dir = cwd.expanduser().resolve()
    evidence_pack = build_evidence_pack(run_dir)
    evidence_paths = write_evidence_outputs(evidence_pack, output_dir)
    prepared = {
        **payload,
        "native_workflow_system_report": True,
        "run_artifact_dir": str(run_dir),
        "skill_name": NATIVE_WORKFLOW_ANALYSIS_SKILL_NAME,
        "skill_path": str(NATIVE_WORKFLOW_ANALYSIS_SKILL_PATH),
        "evidence_pack_path": evidence_paths["evidence_pack"],
        "recipe_matrix_path": evidence_paths["recipe_matrix"],
        "evidence_pack": evidence_pack,
    }
    manifest = {
        "run_artifact_dir": str(run_dir),
        "skill_name": NATIVE_WORKFLOW_ANALYSIS_SKILL_NAME,
        "skill_path": str(NATIVE_WORKFLOW_ANALYSIS_SKILL_PATH),
        "evidence_pack_path": evidence_paths["evidence_pack"],
        "recipe_matrix_path": evidence_paths["recipe_matrix"],
    }
    return prepared, manifest


def _strip_json_fence(content: str) -> str:
    stripped = content.strip()
    if stripped.startswith("```"):
        lines = stripped.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        stripped = "\n".join(lines).strip()
    return stripped


def _parse_json_object(content: str) -> dict[str, Any]:
    stripped = _strip_json_fence(content)
    try:
        payload = json.loads(stripped)
    except json.JSONDecodeError:
        start = stripped.find("{")
        end = stripped.rfind("}")
        if start < 0 or end <= start:
            raise
        payload = json.loads(stripped[start : end + 1])
    if not isinstance(payload, dict):
        raise ValueError("analysis output must be a JSON object")
    return payload


def _normalize_native_workflow_report(content: str, payload: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    warnings: list[str] = []
    try:
        report = _parse_json_object(content)
    except Exception as exc:
        warnings.append(f"CLI output was not valid JSON; used local evidence-pack fallback: {exc}")
        report = _native_workflow_local_report(payload)
    if not isinstance(report.get("markdown"), str) or not str(report.get("markdown")).strip():
        warnings.append("JSON output omitted markdown; generated markdown from evidence pack")
        fallback = _native_workflow_local_report(payload)
        report["markdown"] = fallback["markdown"]
    fallback = _native_workflow_local_report(payload)
    required_defaults = {
        "headline": fallback.get("headline"),
        "verdict": fallback.get("verdict"),
        "lead_recipe": fallback.get("lead_recipe"),
        "recipe_rankings": fallback.get("recipe_rankings", []),
        "system_findings": fallback.get("system_findings", []),
        "live_feasibility": fallback.get("live_feasibility", {}),
        "next_experiments": fallback.get("next_experiments", []),
        "evidence_refs": fallback.get("evidence_refs", []),
    }
    for key, value in required_defaults.items():
        if key not in report or report.get(key) in (None, ""):
            report[key] = value
    verdict = str(report.get("verdict") or "").strip()
    if verdict not in {"promote", "hold", "investigate", "reject"}:
        warnings.append(f"Invalid verdict '{verdict}' normalized to investigate")
        report["verdict"] = "investigate"
    return report, warnings


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
    summary_json_path = output_dir / "latest_summary.json"
    summary_md_path = output_dir / "latest_summary.md"
    generated_files = [str(summary_json_path), str(summary_md_path)]
    if _is_native_workflow_system_report(analysis_template, payload):
        report, validation_warnings = _normalize_native_workflow_report(content, payload)
        summary_json = {
            **report,
            **cli_metadata,
            "template": analysis_template,
            "source_kind": payload.get("source_kind"),
            "run_id": payload.get("run_id"),
            "run_artifact_dir": payload.get("run_artifact_dir"),
            "skill_name": payload.get("skill_name"),
            "skill_path": payload.get("skill_path"),
            "evidence_pack_path": payload.get("evidence_pack_path"),
            "recipe_matrix_path": payload.get("recipe_matrix_path"),
            "validation_warnings": validation_warnings,
        }
        for path_key in ("evidence_pack_path", "recipe_matrix_path"):
            if summary_json.get(path_key):
                generated_files.append(str(summary_json[path_key]))
        _write_json(summary_json_path, summary_json)
        _write_markdown(summary_md_path, str(report["markdown"]))
    else:
        summary_json = {
            **payload,
            **cli_metadata,
        }
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
    manifest = dict(extra_manifest or {})
    effective_skills = _effective_skills(skills, analysis_template)
    if _is_native_workflow_system_report(analysis_template, payload):
        payload, native_manifest = _prepare_native_workflow_payload(payload, cwd=cwd, output_dir=output_dir)
        manifest.update(native_manifest)
        payload["skills"] = effective_skills
    content, cli_metadata = _generate_analysis_content(
        payload,
        analysis_template=analysis_template,
        analysis_engine=analysis_engine,
        skills=effective_skills,
        cwd=cwd,
    )
    return _write_analysis_artifacts(
        output_dir=output_dir,
        payload=payload,
        content=content,
        cli_metadata=cli_metadata,
        analysis_template=analysis_template,
        extra_manifest=manifest,
    )


def _resolve_lead_recipe_name(run_detail: Any) -> str | None:
    quick_summary = getattr(run_detail, "quick_summary", None)
    lead_recipe = (
        getattr(quick_summary, "incumbent_recipe", None)
        or getattr(quick_summary, "baseline_recipe", None)
    )
    if lead_recipe:
        return str(lead_recipe)
    recipes = getattr(run_detail, "recipes", None) or []
    if recipes:
        first_recipe_name = getattr(recipes[0], "recipe_name", None)
        if first_recipe_name:
            return str(first_recipe_name)
    return None


def _run_batch_analysis(args: argparse.Namespace, run_output_dir: Path) -> int:
    batch_mode = _normalized_batch_mode(args)
    if args.source_kind != "run" or not args.run_id or batch_mode == "run_only":
        raise SystemExit("--batch-mode is only supported with source-kind=run and --run-id")

    run_payload = _run_payload(args.run_id)
    run_payload.update({"template": args.analysis_template, "engine": args.analysis_engine, "skills": args.skill})
    run_result = _run_single_analysis(
        payload=run_payload,
        output_dir=run_output_dir,
        analysis_template=args.analysis_template,
        analysis_engine=args.analysis_engine,
        skills=args.skill,
        cwd=(Path("artifacts/native_workflow") / args.run_id),
        extra_manifest={"batch_scope": batch_mode},
    )

    run_detail = get_run_detail(args.run_id)
    selected_recipe_names: list[str]
    if batch_mode == "run_plus_lead_recipe":
        lead_recipe = _resolve_lead_recipe_name(run_detail)
        selected_recipe_names = [lead_recipe] if lead_recipe else []
    else:
        selected_recipe_names = [str(recipe.recipe_name) for recipe in run_detail.recipes]

    recipe_results: list[dict[str, Any]] = []
    for recipe_name in selected_recipe_names:
        recipe_payload = _recipe_payload(args.run_id, recipe_name)
        recipe_payload.update({"template": args.analysis_template, "engine": args.analysis_engine, "skills": args.skill})
        recipe_output_dir = (Path("artifacts/native_workflow") / args.run_id / recipe_name / "analysis").resolve()
        recipe_result = _run_single_analysis(
            payload=recipe_payload,
            output_dir=recipe_output_dir,
            analysis_template=args.analysis_template,
            analysis_engine=args.analysis_engine,
            skills=args.skill,
            cwd=(Path("artifacts/native_workflow") / args.run_id / recipe_name),
            extra_manifest={
                "batch_scope": "recipe_from_run_batch",
                "batch_parent_run_id": args.run_id,
                "batch_parent_mode": batch_mode,
            },
        )
        recipe_results.append(
            {
                "recipe_name": recipe_name,
                **recipe_result,
            }
        )

    batch_manifest_path = run_output_dir / "manifest.json"
    existing_manifest = json.loads(batch_manifest_path.read_text(encoding="utf-8"))
    existing_manifest.update(
        {
            "batch_scope": batch_mode,
            "run_result": run_result,
            "recipe_results": recipe_results,
        }
    )
    _write_json(batch_manifest_path, existing_manifest)
    return 0


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()
    if _normalized_batch_mode(args) != "run_only":
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

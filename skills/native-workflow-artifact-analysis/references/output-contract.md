# Output Contract

## Markdown

Produce exactly these top-level sections for a run-level report:

```markdown
# <run_id> 系统诊断报告

## Executive Verdict

## Recipe Ranking & Roles

## System Diagnosis

## Recipe Dossiers

## Live Portfolio Feasibility

## Next Experiment Roadmap

## Evidence Gaps
```

Use bullet lists and compact tables. Material claims should be supported by metrics, but the prose must remain readable:

- Round values in Markdown: returns/drawdowns/excess returns as percentages, ratios to 2 decimals, holding counts as integers.
- Use Chinese metric labels in user-facing sections; reserve raw file names and metric keys for Evidence Gaps or `evidence_refs`.
- Do not write long evidence strings such as `recipe / file / metric = 0.123456789` in Executive Verdict, System Diagnosis, or Recipe Dossiers.
- Explain the implication after every important number, for example: "TopK 超额为 -0.18%，说明高分股票没有稳定跑赢候选池。"
- Do not compare "old run summary" against a new report as a headline finding. Scorecard/raw conflicts are evidence quality issues and belong in Evidence Gaps.

## JSON

Return a JSON object when the caller asks for structured output. Required keys:

```json
{
  "headline": "string",
  "verdict": "promote | hold | investigate | reject",
  "lead_recipe": "string | null",
  "recipe_rankings": [
    {
      "recipe": "string",
      "role": "lead | filter | candidate | reject | diagnose_only",
      "rank": 1,
      "score": 0.0,
      "rationale": "string"
    }
  ],
  "system_findings": ["string"],
  "live_feasibility": {
    "status": "ready | caution | blocked | unknown",
    "summary": "string",
    "checks": ["string"]
  },
  "next_experiments": [
    {
      "name": "string",
      "priority": "P0 | P1 | P2",
      "rationale": "string"
    }
  ],
  "evidence_refs": [
    {
      "recipe": "string | null",
      "bundle": "rolling | walk_forward | latest | run | null",
      "file": "string",
      "metric": "string",
      "value": "number | string | null",
      "comparator": "string | null"
    }
  ],
  "markdown": "string"
}
```

Additional keys are allowed. Do not omit `markdown`; downstream scripts write it to `latest_summary.md`.

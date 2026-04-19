import Link from "next/link";

import { Bot, CalendarCheck } from "lucide-react";
import { LatestSummaryLayout } from "@/components/common/latest-summary-layout";
import { MarkdownPreviewDialog } from "@/components/common/markdown-preview-dialog";
import { PageHeader } from "@/components/common/page-header";
import { DataTable } from "@/components/data/data-table";
import { NodeCard } from "@/components/diagnostics/node-card";
import { RunArtifactInventory } from "@/components/runs/run-artifact-inventory";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { getRunDetail } from "@/lib/api";
import { formatNumber, formatPathName, formatPercent } from "@/lib/format";
import { parseLatestSummaryMarkdown } from "@/lib/latest-summary";
import { describeWorkflowConfigKey } from "@/lib/workflow-config-descriptions";

export default async function RunDetailPage({ params }: { params: Promise<{ runId: string }> }) {
  const { runId } = await params;
  const detail = await getRunDetail(runId);
  const latestSummaryMarkdown =
    detail.analysis_reports.find((item) => item.name === "latest_summary.md")?.content_preview ?? null;
  const parsedLatestSummary = latestSummaryMarkdown ? parseLatestSummaryMarkdown(latestSummaryMarkdown) : null;
  const summaryVerdict = parsedLatestSummary?.verdict || detail.research_summary.verdict;

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap gap-3">
        <Button asChild>
          <Link href={`/tasks?create=run_native_workflow&sourceType=run&sourceId=${encodeURIComponent(detail.run_id)}`}>
            Create Workflow Task
            <CalendarCheck className="h-4 w-4" />
          </Link>
        </Button>
        <Button variant="outline" asChild>
          <Link href={`/tasks?create=run_research_analysis&sourceType=run&sourceId=${encodeURIComponent(detail.run_id)}`}>
            Generate Research Task
            <Bot className="h-4 w-4" />
          </Link>
        </Button>
        {detail.recipes.map((recipe) => (
          <Button key={recipe.recipe_name} variant="outline" asChild>
            <Link href={`/runs/${detail.run_id}/recipes/${recipe.recipe_name}`}>{recipe.recipe_name}</Link>
          </Button>
        ))}
      </div>

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <SummaryCard title="Universe" value={String(detail.quick_summary.universe_profile ?? "—")} />
        <SummaryCard title="Panel" value={formatPathName(detail.quick_summary.panel_path)} />
        <SummaryCard title="Recipes" value={`${detail.recipes.length}`} />
        <SummaryCard title="Artifacts" value={`${detail.quick_summary.artifact_ready_count}/${detail.quick_summary.artifact_total_count}`} />
      </div>

      <Card className="glass-card">
        <CardHeader className="space-y-3">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div className="flex flex-wrap items-center gap-2">
              <CardTitle className="text-base">Research Summary</CardTitle>
              {summaryVerdict ? <Badge variant="info">{summaryVerdict}</Badge> : null}
            </div>
            {latestSummaryMarkdown ? (
              <MarkdownPreviewDialog
                title={`${detail.run_id} · Research Summary`}
                content={latestSummaryMarkdown}
                triggerLabel="查看详情"
              />
            ) : null}
          </div>
        </CardHeader>
        <CardContent className="space-y-4">
          {latestSummaryMarkdown ? (
            <LatestSummaryLayout content={latestSummaryMarkdown} mode="compact" />
          ) : (
            <>
              <div className="grid gap-4 xl:grid-cols-2">
                <SummaryList title="Key Findings" items={detail.research_summary.key_findings} />
                <SummaryList title="Risks" items={detail.research_summary.risks} />
              </div>
              <div className="grid gap-4 xl:grid-cols-2">
                <SummaryCard title="Current Problem" value={detail.research_summary.current_problem ?? "—"} />
                <SummaryCard title="Recommended Action" value={detail.research_summary.recommended_action ?? "—"} />
              </div>
              {detail.research_summary.recommended_next_actions.length ? (
                <DataTable
                  table={{
                    columns: ["next_action"],
                    rows: detail.research_summary.recommended_next_actions.map((item) => ({ next_action: item })),
                  }}
                  maxRows={8}
                />
              ) : null}
            </>
          )}
        </CardContent>
      </Card>

      <Card className="glass-card">
        <CardHeader>
          <CardTitle className="text-base">Recipe Overview</CardTitle>
        </CardHeader>
        <CardContent>
          <DataTable
            table={{
              columns: [
                "recipe_name",
                "signal_objective",
                "label_recipe",
                "used_feature_count",
                "rolling_rank_ic_ir",
                "rolling_net_total_return",
                "rolling_max_drawdown",
                "walk_forward_rank_ic_ir",
                "walk_forward_net_total_return",
                "walk_forward_max_drawdown",
                "promotion_gate_passed",
              ],
              rows: detail.recipes.map((recipe) => ({
                recipe_name: recipe.recipe_name,
                signal_objective: recipe.signal_objective,
                label_recipe: recipe.label_recipe,
                used_feature_count: recipe.used_feature_count,
                rolling_rank_ic_ir: formatNumber(recipe.rolling_rank_ic_ir, 3),
                rolling_net_total_return: formatPercent(recipe.rolling_net_total_return, 2),
                rolling_max_drawdown: formatPercent(recipe.rolling_max_drawdown, 2),
                walk_forward_rank_ic_ir: formatNumber(recipe.walk_forward_rank_ic_ir, 3),
                walk_forward_net_total_return: formatPercent(recipe.walk_forward_net_total_return, 2),
                walk_forward_max_drawdown: formatPercent(recipe.walk_forward_max_drawdown, 2),
                promotion_gate_passed: recipe.promotion_gate_passed,
                _row_class_name: recipe.promotion_gate_passed ? "bg-tone-success-soft/55 hover:bg-tone-success-soft/70" : "",
              })),
            }}
            maxRows={20}
            rowClassNameKey="_row_class_name"
          />
        </CardContent>
      </Card>

      <Card className="glass-card">
        <CardHeader>
          <CardTitle className="text-base">Config Summary</CardTitle>
        </CardHeader>
        <CardContent>
          <details className="group rounded-xl border border-border/60 bg-surface-2/40">
            <summary className="cursor-pointer list-none px-4 py-3 text-sm font-medium text-foreground marker:hidden">
              <div className="flex items-center justify-between gap-4">
                <span>展开查看全部 {Object.keys(detail.config).length} 项参数</span>
                <span className="text-xs text-muted-foreground group-open:hidden">默认折叠</span>
                <span className="hidden text-xs text-muted-foreground group-open:inline">点击收起</span>
              </div>
            </summary>
            <div className="border-t border-border/60 px-4 py-4">
              <DataTable
                table={{
                  columns: ["key", "value", "description"],
                  rows: Object.entries(detail.config).map(([key, value]) => ({
                    key,
                    value: formatConfigValue(key, value),
                    description: describeConfigKey(key),
                  })),
                }}
                maxRows={Math.max(Object.keys(detail.config).length, 1)}
              />
            </div>
          </details>
        </CardContent>
      </Card>

      <div className="grid gap-4 xl:grid-cols-2">
        {detail.nodes.map((node) => (
          <NodeCard key={node.key} node={node} />
        ))}
      </div>

      <div className="grid gap-4 xl:grid-cols-2">
        <Card className="glass-card">
          <CardHeader>
            <CardTitle className="text-base">Risk & Exposure</CardTitle>
          </CardHeader>
          <CardContent>
            <DataTable
              table={{
                columns: ["metric", "value"],
                rows: [
                  { metric: "research_status", value: detail.quick_summary.research_status ?? "—" },
                  { metric: "incumbent_recipe", value: detail.quick_summary.incumbent_recipe ?? "—" },
                  { metric: "wf_rank_ic_ir", value: formatNumber(detail.quick_summary.baseline_metrics.walk_forward_rank_ic_ir, 3) },
                  { metric: "wf_net_return", value: formatPercent(detail.quick_summary.baseline_metrics.walk_forward_net_total_return, 2) },
                  { metric: "wf_max_drawdown", value: formatPercent(detail.quick_summary.baseline_metrics.walk_forward_max_drawdown, 2) },
                ],
              }}
              maxRows={10}
            />
          </CardContent>
        </Card>
        <Card className="glass-card">
          <CardHeader>
            <CardTitle className="text-base">Experiment Recommendations</CardTitle>
          </CardHeader>
          <CardContent>
            <SummaryList title="Next Actions" items={detail.research_summary.recommended_next_actions} />
          </CardContent>
        </Card>
      </div>

      <Card className="glass-card">
        <CardContent>
          <RunArtifactInventory runId={detail.run_id} />
        </CardContent>
      </Card>
    </div>
  );
}

function SummaryCard({ title, value }: { title: string; value: string }) {
  return (
    <Card className="glass-card">
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-medium text-muted-foreground">{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="text-lg font-semibold">{value}</div>
      </CardContent>
    </Card>
  );
}

function SummaryList({ title, items }: { title: string; items: string[] }) {
  return (
    <Card className="border border-border/60 bg-surface-2/40">
      <CardHeader>
        <CardTitle className="text-sm">{title}</CardTitle>
      </CardHeader>
      <CardContent>
        {items.length ? (
          <ul className="space-y-2 text-sm">
            {items.map((item) => (
              <li key={item} className="rounded-lg border border-border/50 px-3 py-2">
                {item}
              </li>
            ))}
          </ul>
        ) : (
          <div className="text-sm text-muted-foreground">暂无内容</div>
        )}
      </CardContent>
    </Card>
  );
}

function formatConfigValue(key: string, value: unknown) {
  if (typeof value === "string" && (key.endsWith("_path") || key.endsWith("_dir"))) {
    return formatPathName(value);
  }
  if (Array.isArray(value)) return value.join(", ");
  if (typeof value === "object" && value !== null) return JSON.stringify(value);
  return value;
}

function describeConfigKey(key: string) {
  return describeWorkflowConfigKey(key);
}

import Link from "next/link";

import { ChartPayloadPanel } from "@/components/charts/chart-payload-panel";
import { LatestSummaryLayout } from "@/components/common/latest-summary-layout";
import { MarkdownPreviewDialog } from "@/components/common/markdown-preview-dialog";
import { DataTable } from "@/components/data/data-table";
import { NodeCard } from "@/components/diagnostics/node-card";
import { RunAnomalyPeriods } from "@/components/runs/run-anomaly-periods";
import { RunArtifactInventory } from "@/components/runs/run-artifact-inventory";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { getRunDetail } from "@/lib/api";
import { formatNumber, formatPathName, formatPercent } from "@/lib/format";
import { parseLatestSummaryMarkdown } from "@/lib/latest-summary";
import { buildRecommendationHref } from "@/lib/utils";
import { describeWorkflowConfigKey } from "@/lib/workflow-config-descriptions";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export default async function RunDetailPage({ params }: { params: Promise<{ runId: string }> }) {
  const { runId } = await params;
  const detail = await getRunDetail(runId);
  const latestSummaryMarkdown =
    detail.analysis_reports.find((item) => item.name === "latest_summary.md")?.content_preview ?? null;
  const parsedLatestSummary = latestSummaryMarkdown ? parseLatestSummaryMarkdown(latestSummaryMarkdown) : null;
  const summaryVerdict = parsedLatestSummary?.verdict || detail.research_summary.verdict;
  const anomalyPeriods = Array.isArray(detail.experiment_scorecard?.anomaly_periods)
    ? (detail.experiment_scorecard.anomaly_periods as Array<Record<string, unknown>>)
    : [];
  const chartList = Object.values(detail.run_level_charts);
  const exposureCharts = chartList.filter((chart) => chart.key.includes("exposure") || chart.key.includes("holding_trend"));
  const executionCharts = chartList.filter((chart) => chart.key.includes("blocked_sell") || chart.key.includes("realization_bridge"));
  const diagnosisAction = detail.recommendation_actions.find((action) => action.task_kind === "run_research_analysis");

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap gap-3">
        <Button asChild>
          <Link href={`/tasks?create=run_native_workflow&sourceType=run&sourceId=${encodeURIComponent(detail.run_id)}`}>
            Create Workflow Task
          </Link>
        </Button>
        {diagnosisAction ? (
          <Button variant="outline" asChild>
            <Link href={buildRecommendationHref(diagnosisAction)}>Run Diagnosis Task</Link>
          </Button>
        ) : null}
        {detail.recipes.map((recipe) => (
          <Button key={recipe.recipe_name} variant="outline" asChild>
            <Link href={`/runs/${detail.run_id}/recipes/${recipe.recipe_name}`}>{recipe.recipe_name}</Link>
          </Button>
        ))}
      </div>

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <SummaryCard title="Universe" value={String(detail.quick_summary.universe_profile ?? "—")} />
        <SummaryCard title="Panel" value={formatPathName(detail.quick_summary.panel_path)} />
        <SummaryCard title="Avg Actual Holds" value={formatNumber(detail.execution_anomaly_summary.avg_actual_hold_count, 2)} />
        <SummaryCard title="Top1 Sector Weight" value={formatPercent(detail.quick_summary.top1_sector_weight, 2)} />
      </div>

      <Card className="glass-card">
        <CardHeader className="space-y-3">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div className="flex flex-wrap items-center gap-2">
              <CardTitle className="text-base">Research Summary</CardTitle>
              {summaryVerdict ? <Badge variant="info">{summaryVerdict}</Badge> : null}
              {detail.execution_anomaly_summary.severity ? (
                <Badge variant={detail.execution_anomaly_summary.severity === "low" ? "success" : detail.execution_anomaly_summary.severity === "medium" ? "warning" : "destructive"}>
                  {detail.execution_anomaly_summary.severity}
                </Badge>
              ) : null}
            </div>
            {latestSummaryMarkdown ? (
              <MarkdownPreviewDialog
                title="Research Diagnosis Details"
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
            <SummaryList title="Key Findings" items={detail.research_summary.key_findings} />
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

      <div className="grid gap-4 xl:grid-cols-2">
        {detail.nodes.map((node) => (
          <NodeCard key={node.key} node={node} />
        ))}
      </div>

      {exposureCharts.length ? (
        <div className="grid gap-4 xl:grid-cols-2">
          {exposureCharts.map((chart) => (
            <ChartPayloadPanel key={chart.key} chart={chart} />
          ))}
        </div>
      ) : null}

      {executionCharts.length ? (
        <div className="grid gap-4 xl:grid-cols-2">
          {executionCharts.map((chart) => (
            <ChartPayloadPanel key={chart.key} chart={chart} />
          ))}
        </div>
      ) : null}

      <div className="grid gap-4 xl:grid-cols-[1.2fr,0.8fr]">
        <Card className="glass-card">
          <CardHeader>
            <CardTitle className="text-base">异常期列表</CardTitle>
          </CardHeader>
          <CardContent>
            <RunAnomalyPeriods anomalyPeriods={anomalyPeriods} firstRecipeName={detail.recipes[0]?.recipe_name} />
          </CardContent>
        </Card>

        <Card className="glass-card">
          <CardHeader>
            <CardTitle className="text-base">Artifact Inventory</CardTitle>
          </CardHeader>
          <CardContent>
            <RunArtifactInventory runId={detail.run_id} />
          </CardContent>
        </Card>
      </div>

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

"use client";

import Link from "next/link";
import * as React from "react";
import { ReceiptText } from "lucide-react";
import { GitCompare } from "lucide-react";
import { CalendarCheck, Bot } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { RunListItem } from "@/lib/types";
import { formatCompactDate, formatNumber, formatPercent } from "@/lib/format";
import { buildRecommendationHref, cn } from "@/lib/utils";

const artifactVariantMap = {
  ready: "success",
  partial: "warning",
  missing: "destructive",
} as const;

const researchVariantMap = {
  incumbent: "info",
  promoted: "success",
  rejected: "destructive",
  needs_explanation: "warning",
  hold: "neutral",
} as const;

const portfolioVariantMap = {
  healthy: "success",
  warning: "warning",
  danger: "destructive",
  missing: "outline",
  info: "info",
} as const;

export function RunsPageClient({ runs }: { runs: RunListItem[] }) {
  const [selectedRunId, setSelectedRunId] = React.useState(runs[0]?.run_id);
  const selected = runs.find((item) => item.run_id === selectedRunId) ?? runs[0];

  const showQuickJudge = React.useCallback((runId: string) => {
    setSelectedRunId(runId);
  }, []);

  return (
    <div className="space-y-5">
      <div className="space-y-3">
        {runs.map((run) => {
          const summary = run.quick_summary;
          const isSelected = selected?.run_id === run.run_id;
          return (
            <Card
              key={run.run_id}
              onClick={() => showQuickJudge(run.run_id)}
              className={cn(
                "surface-card hover-lift cursor-pointer rounded-xl border-0 text-left",
                isSelected && "ring-2 ring-ring/35",
              )}
            >
              <CardContent className="space-y-3 p-3 sm:p-4">
                <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
                  <div className="text-sm font-semibold tracking-tight sm:text-base">{run.run_id}</div>
                  <div className="text-[11px] text-muted-foreground sm:text-xs">{formatCompactDate(summary.updated_at)}</div>
                  <Badge variant={artifactVariantMap[summary.artifact_status]}>{summary.artifact_status}</Badge>
                  {summary.research_status ? (
                    <Badge variant={researchVariantMap[summary.research_status as keyof typeof researchVariantMap] ?? "neutral"}>
                      {summary.research_status}
                    </Badge>
                  ) : null}
                </div>

                <div className="grid grid-cols-2 gap-2 sm:grid-cols-5">
                  <InfoPill label="Panel" value={summary.panel_path?.split("/").pop() ?? "—"} />
                  <InfoPill label="Lead" value={summary.incumbent_recipe ?? summary.baseline_recipe ?? "—"} />
                  <InfoPill label="WF IC" value={formatNumber(summary.baseline_metrics.walk_forward_rank_ic_ir, 3)} />
                  <InfoPill label="WF Excess" value={formatPercent(summary.baseline_metrics.walk_forward_topk_mean_excess_return_4w, 2)} />
                  <InfoPill label="Avg Actual Holds" value={formatNumber(summary.avg_actual_hold_count, 2)} />
                  <InfoPill label="Max Actual Holds" value={formatNumber(summary.max_actual_hold_count, 0)} />
                  <InfoPill label="Top1 Sector Weight" value={formatPercent(summary.top1_sector_weight, 2)} />
                </div>

                {isSelected ? (
                  <div className="space-y-3 border-t border-border/60 pt-3">
                    {renderQuickJudgeContent(run)}
                  </div>
                ) : null}
              </CardContent>
            </Card>
          );
        })}

        {!selected ? (
          <Card className="glass-card">
            <CardContent className="flex min-h-[360px] items-center justify-center text-muted-foreground">
              当前没有可用 runs。
            </CardContent>
          </Card>
        ) : null}
      </div>
    </div>
  );
}

function renderQuickJudgeContent(selected: RunListItem) {
  const summary = selected.quick_summary;
  const promotionEntries = Object.entries(summary.promotion_gate_summary ?? {});

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap gap-2">
        <Button
          asChild
          size="sm"
          onClick={(event) => {
            event.stopPropagation();
          }}
        >
          <Link href={`/runs/${selected.run_id}`}>
            View Detail
            <ReceiptText className="h-4 w-4" />
          </Link>
        </Button>
        <Button
          asChild
          size="sm"
          variant="outline"
          onClick={(event) => {
            event.stopPropagation();
          }}
        >
          <Link href={`/compare?runId=${encodeURIComponent(selected.run_id)}`}>
            Compare vs Candidate
            <GitCompare className="h-4 w-4" />
          </Link>
        </Button>
        <Button
          asChild
          size="sm"
          variant="outline"
          onClick={(event) => {
            event.stopPropagation();
          }}
        >
          <Link
            href={buildRecommendationHref({
              label: "Run Diagnosis Task",
              task_kind: "run_research_analysis",
              source_type: "run",
              source_id: selected.run_id,
              prefill_config: {
                display_name: `Diagnose ${selected.run_id}`,
                description: "Diagnose execution drift and exposure anomalies",
                analysis_template: "anomaly_diagnosis",
              },
              reason: "先补一份异常诊断，能更快确认需要如何调整 Universe Exit Policy 或持仓上限。",
            })}
          >
            Run Diagnosis Task
            <Bot className="h-4 w-4" />
          </Link>
        </Button>
        <Button
          asChild
          size="sm"
          variant="outline"
          onClick={(event) => {
            event.stopPropagation();
          }}
        >
          <Link href={`/tasks?create=run_native_workflow&sourceType=run&sourceId=${encodeURIComponent(selected.run_id)}`}>
            Create Workflow Task
            <CalendarCheck className="h-4 w-4" />
          </Link>
        </Button>
      </div>

      <div className="flex flex-wrap gap-2">
        {summary.portfolio_definition_status ? (
          <Badge variant={portfolioVariantMap[summary.portfolio_definition_status] ?? "neutral"}>
            组合定义可信度: {summary.portfolio_definition_status}
          </Badge>
        ) : null}
        {promotionEntries.length ? (
          promotionEntries.map(([recipe, gate]) => (
            <Badge
              key={recipe}
              variant={(gate as Record<string, unknown>).promotion_gate_passed ? "success" : "warning"}
            >
              {recipe}: {(gate as Record<string, unknown>).promotion_gate_passed ? "pass" : "hold"}
            </Badge>
          ))
        ) : (
          <Badge variant="neutral">暂无 promotion gate</Badge>
        )}
        {summary.has_execution_gap_issue ? <Badge variant="destructive">执行偏差需关注</Badge> : null}
        {summary.portfolio_definition_status === "danger" || summary.portfolio_definition_status === "warning" ? (
          <Badge variant="warning">需要实验 A</Badge>
        ) : promotionEntries.length > 1 ? (
          <Badge variant="info">可直接对比</Badge>
        ) : (
          <Badge variant="success">可继续推进</Badge>
        )}
      </div>

      <div className="grid gap-2 md:grid-cols-3">
        <InfoPill label="Current Problem" value={summary.current_problem ?? "—"} />
        <InfoPill label="Recommended Action" value={summary.recommended_action ?? "—"} />
        <InfoPill label="Dominant Cause" value={summary.dominant_execution_cause ?? "—"} />
      </div>

      {summary.missing_artifacts.length ? (
        <div className="rounded-lg border border-dashed border-tone-warning-border bg-tone-warning-soft/70 p-3 text-sm">
          <div className="font-medium">缺失产物</div>
          <div className="mt-1 text-muted-foreground">{summary.missing_artifacts.join(", ")}</div>
        </div>
      ) : null}
    </div>
  );
}

function InfoPill({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0 rounded-lg border border-border/60 bg-background/70 px-2.5 py-1.5 text-xs">
      <div className="truncate text-[11px] text-muted-foreground">{label}</div>
      <div className="truncate font-medium text-foreground">{value}</div>
    </div>
  );
}

"use client";

import Link from "next/link";
import * as React from "react";
import { FolderSearch } from "lucide-react";

import { PageHeader } from "@/components/common/page-header";
import { StatCard } from "@/components/common/stat-card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { RunListItem } from "@/lib/types";
import { formatCompactDate, formatPercent } from "@/lib/format";
import { cn } from "@/lib/utils";

const artifactVariantMap = {
  ready: "success",
  partial: "warning",
  missing: "destructive",
} as const;

export function RunsPageClient({ runs }: { runs: RunListItem[] }) {
  const [selectedRunId, setSelectedRunId] = React.useState(runs[0]?.run_id);
  const selected = runs.find((item) => item.run_id === selectedRunId) ?? runs[0];

  const showQuickJudge = React.useCallback((runId: string) => {
    setSelectedRunId(runId);
  }, []);

  return (
    <div className="space-y-6">
      <PageHeader
        kicker="Workflow Runs"
        title="Runs"
        description="按最近 workflow runs 浏览研究输出。点击某次运行卡片后，会直接在卡片内展开 quick-judge 概要，帮助先判断值不值得继续深挖。"
        badge={`${runs.length} runs`}
      />

      <div className="space-y-4">
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
              <CardContent className="space-y-4 p-4">
                <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
                  <div className="space-y-3">
                    <div>
                      <div className="font-semibold tracking-tight">{run.run_id}</div>
                      <div className="mt-1 text-xs text-muted-foreground">{formatCompactDate(summary.updated_at)}</div>
                    </div>
                    <div className="flex flex-wrap gap-2">
                      <InfoPill label="Universe" value={summary.universe_profile ?? "—"} />
                      <InfoPill label="Recipes" value={`${summary.recipe_names.length}`} />
                      <InfoPill label="WF IC" value={formatPercent(summary.baseline_metrics.walk_forward_rank_ic_ir, 1)} />
                      <InfoPill label="WF Excess" value={formatPercent(summary.baseline_metrics.walk_forward_topk_mean_excess_return_4w, 2)} />
                    </div>
                  </div>
                  <Badge variant={artifactVariantMap[summary.artifact_status]}>{summary.artifact_status}</Badge>
                </div>

                {isSelected ? (
                  <div className="space-y-4 border-t border-border/60 pt-4">
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
    <div className="space-y-4">
      <div>
        <div className="text-sm font-medium">Quick Judge</div>
        <p className="mt-1 text-sm text-muted-foreground">{summary.output_dir}</p>
      </div>

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        <StatCard title="Panel" value={summary.panel_path?.split("/").pop() ?? "—"} detail={summary.end_date ?? "开放区间"} />
        <StatCard title="Recipes" value={`${summary.recipe_names.length}`} detail={summary.recipe_names.join(", ")} />
        <StatCard
          title="Artifacts"
          value={`${summary.artifact_ready_count}/${summary.artifact_total_count}`}
          detail={summary.has_missing_artifacts ? "存在缺失产物" : "产物齐全"}
        />
      </div>

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <StatCard title="Rolling rank_ic_ir" value={formatPercent(summary.baseline_metrics.rolling_rank_ic_ir, 1)} />
        <StatCard title="Rolling topk excess" value={formatPercent(summary.baseline_metrics.rolling_topk_mean_excess_return_4w, 2)} />
        <StatCard title="WF rank_ic_ir" value={formatPercent(summary.baseline_metrics.walk_forward_rank_ic_ir, 1)} />
        <StatCard title="WF topk excess" value={formatPercent(summary.baseline_metrics.walk_forward_topk_mean_excess_return_4w, 2)} />
      </div>

      <div className="flex flex-wrap gap-2">
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
      </div>

      {summary.missing_artifacts.length ? (
        <div className="rounded-lg border border-dashed border-tone-warning-border bg-tone-warning-soft/70 p-3 text-sm">
          <div className="font-medium">缺失产物</div>
          <div className="mt-1 text-muted-foreground">{summary.missing_artifacts.join(", ")}</div>
        </div>
      ) : null}

      <div className="flex flex-wrap gap-3">
        <Button
          asChild
          onClick={(event) => {
            event.stopPropagation();
          }}
        >
          <Link href={`/runs/${selected.run_id}`}>
            打开完整详情
          </Link>
        </Button>
        <Button
          asChild
          variant="outline"
          onClick={(event) => {
            event.stopPropagation();
          }}
        >
          <Link href="/compare">
            去 Compare 选择 recipe
            <FolderSearch className="h-4 w-4" />
          </Link>
        </Button>
      </div>
    </div>
  );
}

function InfoPill({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-full border border-border/60 bg-background/70 px-3 py-1.5 text-xs">
      <span className="text-muted-foreground">{label}</span>
      <span className="ml-2 font-medium text-foreground">{value}</span>
    </div>
  );
}

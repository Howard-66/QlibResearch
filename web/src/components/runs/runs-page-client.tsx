"use client";

import Link from "next/link";
import * as React from "react";
import { ArrowRight, FolderSearch } from "lucide-react";

import { PageHeader } from "@/components/common/page-header";
import { StatCard } from "@/components/common/stat-card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
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
        description="按最近 workflow runs 浏览研究输出。点击左侧某次运行后，会立即在右侧看到 quick-judge 概要，帮助先判断值不值得继续深挖。"
        badge={`${runs.length} runs`}
      />

      <div className="space-y-4">
        {runs.map((run) => {
          const summary = run.quick_summary;
          const isSelected = selected?.run_id === run.run_id;
          return (
            <div key={run.run_id} className="space-y-4">
              <Card
                onClick={() => showQuickJudge(run.run_id)}
                className={cn(
                  "surface-card hover-lift cursor-pointer rounded-xl border-0 text-left",
                  isSelected && "ring-2 ring-ring/35",
                )}
              >
                <CardContent className="space-y-4 p-4">
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <div className="font-semibold">{run.run_id}</div>
                      <div className="mt-1 text-xs text-muted-foreground">{formatCompactDate(summary.updated_at)}</div>
                    </div>
                    <Badge variant={artifactVariantMap[summary.artifact_status]}>{summary.artifact_status}</Badge>
                  </div>
                  <div className="grid grid-cols-2 gap-3 text-sm md:grid-cols-4">
                    <div>
                      <div className="text-xs text-muted-foreground">Universe</div>
                      <div className="font-medium">{summary.universe_profile ?? "—"}</div>
                    </div>
                    <div>
                      <div className="text-xs text-muted-foreground">Recipes</div>
                      <div className="font-medium">{summary.recipe_names.length}</div>
                    </div>
                    <div>
                      <div className="text-xs text-muted-foreground">WF rank_ic_ir</div>
                      <div className="font-medium">{formatPercent(summary.baseline_metrics.walk_forward_rank_ic_ir, 1)}</div>
                    </div>
                    <div>
                      <div className="text-xs text-muted-foreground">WF excess</div>
                      <div className="font-medium">{formatPercent(summary.baseline_metrics.walk_forward_topk_mean_excess_return_4w, 2)}</div>
                    </div>
                  </div>
                  <div className="flex flex-wrap gap-2">
                    <Button
                      size="sm"
                      variant={isSelected ? "default" : "outline"}
                      onClick={(event) => {
                        event.stopPropagation();
                        showQuickJudge(run.run_id);
                      }}
                    >
                      {isSelected ? "已展开 Quick Judge" : "查看 Quick Judge"}
                    </Button>
                    <Button
                      size="sm"
                      variant="outline"
                      asChild
                      onClick={(event) => {
                        event.stopPropagation();
                      }}
                    >
                      <Link href={`/runs/${run.run_id}`}>
                        打开复盘页
                        <ArrowRight className="h-4 w-4" />
                      </Link>
                    </Button>
                  </div>
                </CardContent>
              </Card>

              {isSelected ? renderQuickJudge(run) : null}
            </div>
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

function renderQuickJudge(selected: RunListItem) {
  const summary = selected.quick_summary;
  const promotionEntries = Object.entries(summary.promotion_gate_summary ?? {});

  return (
    <div className="space-y-6">
      <Card className="glass-card">
        <CardHeader>
          <div className="flex items-start justify-between gap-3">
            <div>
              <CardTitle className="text-xl">{selected.run_id}</CardTitle>
              <p className="mt-1 text-sm text-muted-foreground">{summary.output_dir}</p>
            </div>
            <Badge variant={artifactVariantMap[summary.artifact_status]}>{summary.artifact_status}</Badge>
          </div>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <StatCard title="Universe" value={summary.universe_profile ?? "—"} detail={summary.start_date ?? "未指定"} />
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
            <Button asChild>
              <Link href={`/runs/${selected.run_id}`}>
                打开完整详情
                <ArrowRight className="h-4 w-4" />
              </Link>
            </Button>
            <Button asChild variant="outline">
              <Link href="/compare">
                去 Compare 选择 recipe
                <FolderSearch className="h-4 w-4" />
              </Link>
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

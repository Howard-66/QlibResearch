"use client";

import type { EChartsOption } from "echarts";
import Link from "next/link";
import * as React from "react";
import { useQuery } from "@tanstack/react-query";
import { Bot } from "lucide-react";

import { ChartPayloadPanel } from "@/components/charts/chart-payload-panel";
import { EChartsChart } from "@/components/charts/echarts-chart";
import { LatestSummaryLayout } from "@/components/common/latest-summary-layout";
import { MarkdownContent } from "@/components/common/markdown-preview-dialog";
import { TrendChart } from "@/components/charts/trend-chart";
import { StatCard } from "@/components/common/stat-card";
import { DataTable } from "@/components/data/data-table";
import { NodeCard } from "@/components/diagnostics/node-card";
import { ValidationComparisonPanel } from "@/components/diagnostics/validation-comparison-panel";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { getRecipeTables } from "@/lib/api";
import { parseLatestSummaryMarkdown } from "@/lib/latest-summary";
import { DataTablePayload, RecipeDetail } from "@/lib/types";
import { formatInteger, formatNumber, formatPathName, formatPercent } from "@/lib/format";
import { buildRecommendationHref } from "@/lib/utils";

export function RecipeDetailClient({ detail }: { detail: RecipeDetail }) {
  const emptyTable = React.useMemo<DataTablePayload>(() => ({ columns: [], rows: [] }), []);
  const [activeTab, setActiveTab] = React.useState("diagnostics");
  const [focusedTradeDate, setFocusedTradeDate] = React.useState<string | null>(null);
  const lazyTableNames = React.useMemo(() => {
    if (activeTab === "rolling") {
      return [
        "rolling_native_report",
        "rolling_performance_metrics",
        "rolling_native_monthly_return_heatmap",
        "rolling_native_annual_return_heatmap",
        "rolling_feature_importance",
      ];
    }
    if (activeTab === "walk-forward") {
      return [
        "walk_forward_native_report",
        "walk_forward_performance_metrics",
        "walk_forward_native_monthly_return_heatmap",
        "walk_forward_native_annual_return_heatmap",
        "walk_forward_feature_importance",
      ];
    }
    if (activeTab === "snapshot") {
      return ["latest_score_frame", "portfolio_targets"];
    }
    if (activeTab === "realization") {
      return ["signal_realization_bridge", "holding_count_drift"];
    }
    if (activeTab === "exposure") {
      return ["sector_exposure_history", "regime_gate_diagnostics"];
    }
    return [];
  }, [activeTab]);

  const lazyTablesQuery = useQuery({
    queryKey: ["recipe-tables", detail.run_id, detail.recipe_name, activeTab],
    queryFn: () => getRecipeTables(detail.run_id, detail.recipe_name, lazyTableNames),
    enabled: lazyTableNames.length > 0,
  });

  const tables = React.useMemo(
    () => ({
      ...detail.tables,
      ...(lazyTablesQuery.data?.tables ?? {}),
    }),
    [detail.tables, lazyTablesQuery.data?.tables],
  );
  const getTable = React.useCallback((name: string) => tables[name] ?? emptyTable, [emptyTable, tables]);
  const rollingSummary = getTable("rolling_summary").rows[0] ?? {};
  const walkSummary = getTable("walk_forward_summary").rows[0] ?? {};
  const rollingReport = getTable("rolling_native_report").rows as Record<string, unknown>[];
  const walkReport = getTable("walk_forward_native_report").rows as Record<string, unknown>[];
  const rollingSignalDiagnostics = React.useMemo(() => filterTableByBundle(getTable("signal_diagnostics"), "rolling"), [getTable]);
  const walkSignalDiagnostics = React.useMemo(() => filterTableByBundle(getTable("signal_diagnostics"), "walk_forward"), [getTable]);
  const rollingPortfolioDiagnostics = React.useMemo(() => filterTableByBundle(getTable("portfolio_diagnostics"), "rolling"), [getTable]);
  const walkPortfolioDiagnostics = React.useMemo(() => filterTableByBundle(getTable("portfolio_diagnostics"), "walk_forward"), [getTable]);
  const rollingSliceStability = React.useMemo(() => filterTableByBundle(getTable("slice_regime_summary"), "rolling"), [getTable]);
  const walkSliceStability = React.useMemo(() => filterTableByBundle(getTable("slice_regime_summary"), "walk_forward"), [getTable]);
  const rollingExecutionGap = React.useMemo(() => filterTableByBundle(getTable("execution_diff_summary"), "rolling"), [getTable]);
  const walkExecutionGap = React.useMemo(() => filterTableByBundle(getTable("execution_diff_summary"), "walk_forward"), [getTable]);
  const latestSummaryMarkdown = React.useMemo(
    () => detail.analysis_reports.find((item) => item.name === "latest_summary.md")?.content_preview ?? null,
    [detail.analysis_reports],
  );
  const runRecipeDossierMarkdown = React.useMemo(
    () => detail.analysis_reports.find((item) => item.name === "run_recipe_dossier.md")?.content_preview ?? null,
    [detail.analysis_reports],
  );
  const parsedLatestSummary = React.useMemo(
    () => (latestSummaryMarkdown ? parseLatestSummaryMarkdown(latestSummaryMarkdown) : null),
    [latestSummaryMarkdown],
  );
  const summaryVerdict = parsedLatestSummary?.verdict ?? detail.research_summary.verdict;
  const filteredRealizationBridge = React.useMemo(
    () => filterTableByDate(getTable("signal_realization_bridge"), focusedTradeDate),
    [focusedTradeDate, getTable],
  );
  const filteredHoldingDrift = React.useMemo(
    () => filterTableByDate(getTable("holding_count_drift"), focusedTradeDate),
    [focusedTradeDate, getTable],
  );
  const filteredExposureHistory = React.useMemo(
    () => filterTableByDate(getTable("sector_exposure_history"), focusedTradeDate),
    [focusedTradeDate, getTable],
  );

  return (
    <div className="space-y-6">
      <SectionCard title="Performance Snapshot">
        <div className="mb-4 flex flex-wrap gap-2">
          {detail.recommendation_actions.length ? (
            detail.recommendation_actions.slice(0, 3).map((action) => (
              <Button key={`${action.task_kind}-${action.label}`} size="sm" variant="outline" asChild>
                <Link href={buildRecommendationHref(action)}>
                  {action.label}
                  <Bot className="h-4 w-4" />
                </Link>
              </Button>
            ))
          ) : (
            <Button size="sm" variant="outline" asChild>
              <Link href={`/tasks?create=run_research_analysis&sourceType=recipe&sourceId=${encodeURIComponent(`${detail.run_id}:${detail.recipe_name}`)}`}>
                Generate Analysis Task
                <Bot className="h-4 w-4" />
              </Link>
            </Button>
          )}
          {summaryVerdict ? <Badge variant="info">{summaryVerdict}</Badge> : null}
          {detail.portfolio_realization_summary.severity ? (
            <Badge variant={detail.portfolio_realization_summary.severity === "low" ? "success" : detail.portfolio_realization_summary.severity === "medium" ? "warning" : "destructive"}>
              {detail.portfolio_realization_summary.severity}
            </Badge>
          ) : null}
        </div>
        <div className="grid gap-6 xl:grid-cols-2">
          <PerformanceMetricPanel
            title="Rolling"
            metrics={buildOverviewPerformanceMetrics(detail.overview, rollingSummary, "rolling")}
          />
          <PerformanceMetricPanel
            title="Walk-forward"
            metrics={buildOverviewPerformanceMetrics(detail.overview, walkSummary, "walk_forward")}
          />
        </div>
      </SectionCard>

      <Tabs defaultValue="interpretation" className="space-y-4" onValueChange={setActiveTab}>
        <TabsList>
          <TabsTrigger value="interpretation">Interpretation</TabsTrigger>
          <TabsTrigger value="diagnostics">Diagnostics</TabsTrigger>
          <TabsTrigger value="feature">Feature</TabsTrigger>
          <TabsTrigger value="rolling">Rolling</TabsTrigger>
          <TabsTrigger value="walk-forward">Walk Forward</TabsTrigger>
          <TabsTrigger value="realization">Portfolio Realization</TabsTrigger>
          <TabsTrigger value="exposure">Exposure</TabsTrigger>
          <TabsTrigger value="snapshot">Snapshot</TabsTrigger>
          <TabsTrigger value="artifacts">Artifacts</TabsTrigger>
        </TabsList>

        <TabsContent value="diagnostics" className="space-y-6">
          <div className="grid gap-4 xl:grid-cols-2">
            {detail.nodes.map((node) => (
              <NodeCard key={node.key} node={node} />
            ))}
          </div>
          <SectionCard title="标签定义">
            <div className="flex flex-wrap gap-2">
              <Badge variant="info">label: {String(detail.recipe_config.label_recipe ?? "—")}</Badge>
              <Badge variant="info">objective: {String(detail.recipe_config.signal_objective ?? "—")}</Badge>
              <Badge variant="neutral">features: {formatInteger(detail.overview.used_feature_count)}</Badge>
            </div>
          </SectionCard>
        </TabsContent>

        <TabsContent value="feature" className="space-y-6">
          <SectionCard title="Feature Prefilter">
            <DataTable table={getTable("feature_prefilter")} maxRows={12} />
          </SectionCard>
        </TabsContent>

        <TabsContent value="rolling" className="space-y-6">
          <LazyState queryState={lazyTablesQuery} />
          <SectionCard title="Rolling Native Report Trend">
            <TrendChart
              data={rollingReport}
              xKey="datetime"
              yKeys={[
                { key: "net_value", color: "hsl(var(--chart-1))", name: "Net Value" },
                { key: "benchmark_value", color: "hsl(var(--chart-4))", name: "Benchmark" },
              ]}
            />
          </SectionCard>
          <div className="grid gap-6 xl:grid-cols-2">
            <SectionCard title="Rolling Monthly Heatmap">
              <EChartsChart option={buildMonthlyHeatmap(getTable("rolling_native_monthly_return_heatmap"), "Rolling 月度收益热力图")} />
            </SectionCard>
            <SectionCard title="Rolling Annual Heatmap">
              <EChartsChart option={buildAnnualHeatmap(getTable("rolling_native_annual_return_heatmap"), "Rolling 年度收益热力图")} />
            </SectionCard>
          </div>
          <div className="grid gap-6 xl:grid-cols-2">
            <SectionCard title="Rolling Mean Gain By Feature">
              <EChartsChart option={buildFeatureImportanceMeanChart(getTable("rolling_feature_importance"), "Rolling mean_gain_by_feature")} />
            </SectionCard>
            <SectionCard title="Rolling Latest Feature Date / 出现次数">
              <EChartsChart option={buildFeatureImportanceCountChart(getTable("rolling_feature_importance"), "Rolling latest_feature_date / 出现次数")} />
            </SectionCard>
          </div>
          <SectionCard title="Rolling Summary">
            <DataTable table={getTable("rolling_summary")} maxRows={5} />
          </SectionCard>
          <SectionCard title="Signal Diagnostics">
            <DataTable table={rollingSignalDiagnostics} maxRows={16} />
          </SectionCard>
          <SectionCard title="Portfolio Diagnostics">
            <DataTable table={rollingPortfolioDiagnostics} maxRows={16} />
          </SectionCard>
          <SectionCard title="Slice Stability">
            <DataTable table={rollingSliceStability} maxRows={16} />
          </SectionCard>
          <SectionCard title="Validation Comparison">
            <ValidationComparisonPanel table={rollingExecutionGap} />
          </SectionCard>
        </TabsContent>

        <TabsContent value="walk-forward" className="space-y-6">
          <LazyState queryState={lazyTablesQuery} />
          <SectionCard title="Walk-forward Native Report Trend">
            <TrendChart
              data={walkReport}
              xKey="datetime"
              yKeys={[
                { key: "net_value", color: "hsl(var(--chart-2))", name: "Net Value" },
                { key: "benchmark_value", color: "hsl(var(--chart-4))", name: "Benchmark" },
              ]}
            />
          </SectionCard>
          <div className="grid gap-6 xl:grid-cols-2">
            <SectionCard title="Walk-forward Monthly Heatmap">
              <EChartsChart option={buildMonthlyHeatmap(getTable("walk_forward_native_monthly_return_heatmap"), "Walk-forward 月度收益热力图")} />
            </SectionCard>
            <SectionCard title="Walk-forward Annual Heatmap">
              <EChartsChart option={buildAnnualHeatmap(getTable("walk_forward_native_annual_return_heatmap"), "Walk-forward 年度收益热力图")} />
            </SectionCard>
          </div>
          <div className="grid gap-6 xl:grid-cols-2">
            <SectionCard title="Walk-forward Mean Gain By Feature">
              <EChartsChart option={buildFeatureImportanceMeanChart(getTable("walk_forward_feature_importance"), "Walk-forward mean_gain_by_feature")} />
            </SectionCard>
            <SectionCard title="Walk-forward Latest Feature Date / 出现次数">
              <EChartsChart option={buildFeatureImportanceCountChart(getTable("walk_forward_feature_importance"), "Walk-forward latest_feature_date / 出现次数")} />
            </SectionCard>
          </div>
          <SectionCard title="Walk-forward Summary">
            <DataTable table={getTable("walk_forward_summary")} maxRows={5} />
          </SectionCard>
          <SectionCard title="Signal Diagnostics">
            <DataTable table={walkSignalDiagnostics} maxRows={16} />
          </SectionCard>
          <SectionCard title="Portfolio Diagnostics">
            <DataTable table={walkPortfolioDiagnostics} maxRows={16} />
          </SectionCard>
          <SectionCard title="Slice Stability">
            <DataTable table={walkSliceStability} maxRows={16} />
          </SectionCard>
          <SectionCard title="Validation Comparison">
            <ValidationComparisonPanel table={walkExecutionGap} />
          </SectionCard>
        </TabsContent>

        <TabsContent value="snapshot" className="space-y-6">
          <LazyState queryState={lazyTablesQuery} />
          <div className="grid gap-6 xl:grid-cols-2">
            <SectionCard title="Latest Score Snapshot">
              <DataTable table={getTable("latest_score_frame")} maxRows={20} />
            </SectionCard>
            <SectionCard title="Target vs Actual Snapshot">
              <DataTable
                table={{
                  columns: ["view", "rows"],
                  rows: [
                    { view: "portfolio_targets", rows: getTable("portfolio_targets").rows.length },
                    { view: "post_trade_holdings", rows: filteredHoldingDrift.rows.length },
                    {
                      view: "差集数量",
                      rows: Math.max(
                        getTable("portfolio_targets").rows.length - filteredHoldingDrift.rows.length,
                        filteredHoldingDrift.rows.length - getTable("portfolio_targets").rows.length,
                      ),
                    },
                  ],
                }}
                maxRows={5}
              />
            </SectionCard>
          </div>
          <SectionCard title="Portfolio Targets">
            <DataTable table={getTable("portfolio_targets")} maxRows={20} />
          </SectionCard>
        </TabsContent>

        <TabsContent value="realization" className="space-y-6">
          <LazyState queryState={lazyTablesQuery} />
          <div className="grid gap-6 xl:grid-cols-2">
            {Object.values(detail.chart_payloads)
              .filter((chart) => chart.key.includes("holding_trend") || chart.key.includes("blocked_sell"))
              .map((chart) => (
                <ChartPayloadPanel key={chart.key} chart={chart} />
              ))}
          </div>
          <SectionCard title="异常时间点与解释">
            <RealizationExplanationPanel
              summary={detail.portfolio_realization_summary}
              focusedTradeDate={focusedTradeDate}
              onFocusDate={setFocusedTradeDate}
              table={getTable("holding_count_drift")}
              actions={detail.recommendation_actions}
            />
          </SectionCard>
          <SectionCard title="Signal To Portfolio Bridge">
            <DataTable table={filteredRealizationBridge} maxRows={20} />
          </SectionCard>
          <SectionCard title="Holding Count Drift">
            <DataTable table={filteredHoldingDrift} maxRows={20} />
          </SectionCard>
        </TabsContent>

        <TabsContent value="exposure" className="space-y-6">
          <LazyState queryState={lazyTablesQuery} />
          <div className="grid gap-6 xl:grid-cols-2">
            {Object.values(detail.chart_payloads)
              .filter((chart) => chart.key.includes("exposure"))
              .map((chart) => (
                <ChartPayloadPanel key={chart.key} chart={chart} />
              ))}
          </div>
          <SectionCard title="Sector Exposure History">
            <DataTable table={filteredExposureHistory} maxRows={20} />
          </SectionCard>
          <SectionCard title="Regime Gate Diagnostics">
            <DataTable table={getTable("regime_gate_diagnostics")} maxRows={20} />
          </SectionCard>
        </TabsContent>

        <TabsContent value="interpretation" className="space-y-6">
          <SectionCard title="Interpretation Summary">
            <div className="space-y-4">
              {runRecipeDossierMarkdown ? (
                <div className="rounded-lg border border-border/50 bg-surface-2/30 p-4">
                  <MarkdownContent content={runRecipeDossierMarkdown} />
                </div>
              ) : latestSummaryMarkdown ? (
                <LatestSummaryLayout content={latestSummaryMarkdown} mode="compact" />
              ) : (
                <>
                  <SummaryList title="Key Findings" items={detail.research_summary.key_findings} />
                  <SummaryList title="Risks" items={detail.research_summary.risks} />
                  <SummaryList title="Next Actions" items={detail.research_summary.recommended_next_actions} />
                </>
              )}
            </div>
          </SectionCard>
        </TabsContent>

        <TabsContent value="artifacts">
          {Object.entries(groupArtifactRows(detail.artifact_inventory)).map(([group, rows]) => (
            <SectionCard key={group} title={group}>
              <DataTable
                table={{
                  columns: ["name", "path", "exists", "updated_at"],
                  rows: rows.map((item) => ({
                    name: item.name,
                    path: formatPathName(item.path),
                    exists: item.exists,
                    updated_at: item.updated_at,
                  })),
                }}
                maxRows={50}
              />
            </SectionCard>
          ))}
        </TabsContent>
      </Tabs>
    </div>
  );
}

function filterTableByBundle(table: DataTablePayload, bundle: string): DataTablePayload {
  if (!table.columns.length) {
    return table;
  }
  if (!table.columns.includes("bundle")) {
    return table;
  }
  return {
    columns: table.columns,
    rows: table.rows.filter((row) => String(row.bundle ?? "") === bundle),
  };
}

function SectionCard({ title, action, children }: { title: string; action?: React.ReactNode; children: React.ReactNode }) {
  return (
    <Card className="glass-card">
      <CardHeader className="flex-row items-center justify-between space-y-0">
        <CardTitle className="text-base">{title}</CardTitle>
        {action}
      </CardHeader>
      <CardContent>{children}</CardContent>
    </Card>
  );
}

function SummaryList({ title, items }: { title: string; items: string[] }) {
  return (
    <div className="space-y-2">
      <div className="text-sm font-medium">{title}</div>
      {items.length ? (
        <div className="space-y-2">
          {items.map((item) => (
            <div key={item} className="rounded-lg border border-border/60 bg-surface-2/40 px-3 py-2 text-sm">
              {item}
            </div>
          ))}
        </div>
      ) : (
        <div className="text-sm text-muted-foreground">暂无内容</div>
      )}
    </div>
  );
}

function LazyState({
  queryState,
}: {
  queryState: {
    isLoading: boolean;
    error: unknown;
  };
}) {
  if (queryState.isLoading) {
    return <Badge variant="info">正在按需加载当前 tab 的大表…</Badge>;
  }
  if (queryState.error) {
    return <Badge variant="destructive">当前 tab 数据加载失败</Badge>;
  }
  return null;
}

function filterTableByDate(table: DataTablePayload, tradeDate: string | null): DataTablePayload {
  if (!tradeDate || !table.columns.length) {
    return table;
  }
  const key = table.columns.includes("trade_date") ? "trade_date" : table.columns.includes("signal_date") ? "signal_date" : null;
  if (!key) {
    return table;
  }
  return {
    columns: table.columns,
    rows: table.rows.filter((row) => String(row[key] ?? "") === tradeDate),
  };
}

function groupArtifactRows(items: RecipeDetail["artifact_inventory"]) {
  return items.reduce<Record<string, RecipeDetail["artifact_inventory"]>>((groups, item) => {
    const group = item.name.includes("analysis/") ? "Research" : item.name.includes("holding_count_drift") || item.name.includes("signal_realization") || item.name.includes("sector_exposure") || item.name.includes("rebalance_audit") ? "Diagnostics" : item.name.includes("native_report") || item.name.includes("summary") || item.name.includes("benchmark") ? "Backtest" : "Exports";
    groups[group] = [...(groups[group] ?? []), item];
    return groups;
  }, {});
}

function RealizationExplanationPanel({
  summary,
  focusedTradeDate,
  onFocusDate,
  table,
  actions,
}: {
  summary: RecipeDetail["portfolio_realization_summary"];
  focusedTradeDate: string | null;
  onFocusDate: (tradeDate: string | null) => void;
  table: DataTablePayload;
  actions: RecipeDetail["recommendation_actions"];
}) {
  const dateKey = table.columns.includes("trade_date") ? "trade_date" : table.columns.includes("signal_date") ? "signal_date" : null;
  const candidateDates = dateKey
    ? table.rows
        .filter((row) => Number(row.residual_hold_count ?? 0) > 0 || Number(row.locked_residual_count ?? 0) > 0)
        .slice(0, 8)
        .map((row) => String(row[dateKey] ?? ""))
        .filter(Boolean)
    : [];

  return (
    <div className="space-y-4">
      <div className="grid gap-4 xl:grid-cols-3">
        <StatCard compact title="Dominant Cause" value={summary.dominant_cause ?? "—"} />
        <StatCard compact title="Avg Actual Holds" value={formatNumber(summary.avg_actual_hold_count, 2)} />
        <StatCard compact title="Avg Locked Residual" value={formatNumber(summary.avg_locked_residual_count, 2)} />
      </div>
      <div className="flex flex-wrap gap-2">
        {candidateDates.map((tradeDate) => (
          <Button key={tradeDate} size="sm" variant={focusedTradeDate === tradeDate ? "default" : "outline"} onClick={() => onFocusDate(focusedTradeDate === tradeDate ? null : tradeDate)}>
            {tradeDate}
          </Button>
        ))}
      </div>
      <div className="grid gap-4 xl:grid-cols-2">
        <SummaryList title="研究解释" items={[summary.summary_label ?? "暂无摘要", ...(summary.recommended_experiments ?? [])]} />
        <div className="flex flex-wrap gap-2">
          {actions.map((action) => (
            <Button key={`${action.task_kind}-${action.label}-explain`} size="sm" variant="outline" asChild>
              <Link href={buildRecommendationHref(action)}>{action.label}</Link>
            </Button>
          ))}
        </div>
      </div>
    </div>
  );
}

type PerformanceMetricItem = {
  label: string;
  value: string;
};

const PERFORMANCE_METRIC_GUIDES: Record<string, string> = {
  rank_ic_ir: "衡量排序信号稳定性的核心指标，越高通常说明分数和未来收益的排序关系越稳定。",
  "topk excess": "看头部选股相对基准的平均超额收益，越高说明高分股票更能跑赢基准。",
  "Net Return": "整个回测区间扣成本后的累计净收益，先看赚钱能力，但不要脱离回撤单独判断。",
  "Max DD": "区间内从高点回撤的最深幅度，绝对值越小越稳。",
  "Ann Return": "把区间收益年化后的回报水平，便于和其他策略或资产做横向比较。",
  "Ann Vol": "按周收益波动折算的年化波动率，越高说明净值路径越不平稳。",
  Sharpe: "单位波动对应的超额回报效率，越高越好；接近 0 说明收益对波动的补偿不明显。",
  "Win Rate": "单期收益为正的占比，用来看策略命中率，但不能替代收益幅度判断。",
  Calmar: "年化收益相对最大回撤的效率比，越高说明回撤承受下的收益产出越好。",
};

function formatMetricValue(value: unknown, kind: "percent" | "number") {
  return kind === "percent" ? formatPercent(value, 2) : formatNumber(value, 2);
}

function buildOverviewPerformanceMetrics(
  overview: Record<string, unknown>,
  summary: Record<string, unknown>,
  prefix: "rolling" | "walk_forward",
): PerformanceMetricItem[] {
  return [
    { label: "rank_ic_ir", value: formatNumber(summary.rank_ic_ir, 3) },
    { label: "topk excess", value: formatPercent(summary.topk_mean_excess_return_4w, 2) },
    { label: "Net Return", value: formatMetricValue(overview[`${prefix}_net_total_return`], "percent") },
    { label: "Max DD", value: formatMetricValue(overview[`${prefix}_max_drawdown`], "percent") },
    { label: "Ann Return", value: formatMetricValue(overview[`${prefix}_annualized_return`], "percent") },
    { label: "Ann Vol", value: formatMetricValue(overview[`${prefix}_annualized_volatility`], "percent") },
    { label: "Sharpe", value: formatMetricValue(overview[`${prefix}_sharpe_ratio`], "number") },
    { label: "Win Rate", value: formatMetricValue(overview[`${prefix}_win_rate`], "percent") },
    { label: "Calmar", value: formatMetricValue(overview[`${prefix}_calmar_ratio`], "number") },
  ];
}

function PerformanceMetricPanel({
  title,
  metrics,
  detail,
}: {
  title: string;
  metrics: PerformanceMetricItem[];
  detail?: string;
}) {
  return (
    <div className="space-y-3">
      <div>
        <div className="text-sm font-semibold text-foreground">{title}</div>
        {detail ? <div className="text-xs text-muted-foreground">{detail}</div> : null}
      </div>
      <TooltipProvider delayDuration={150}>
        <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-5">
          {metrics.map((metric) => (
            <Tooltip key={`${title}-${metric.label}`}>
              <TooltipTrigger asChild>
                <div className="min-w-0">
                  <StatCard compact title={metric.label} value={metric.value} />
                </div>
              </TooltipTrigger>
              <TooltipContent className="max-w-64 whitespace-normal leading-5">
                {PERFORMANCE_METRIC_GUIDES[metric.label] ?? "用于辅助判断策略收益质量和稳定性的表现指标。"}
              </TooltipContent>
            </Tooltip>
          ))}
        </div>
      </TooltipProvider>
    </div>
  );
}

type AggregatedFeatureImportance = {
  feature: string;
  meanGain: number;
  occurrenceCount: number;
  latestFeatureDate: string | null;
};

function aggregateFeatureImportance(table: DataTablePayload): AggregatedFeatureImportance[] {
  const grouped = new Map<string, { totalGain: number; occurrenceCount: number; latestFeatureDate: string | null }>();

  for (const row of table?.rows ?? []) {
    const feature = String(row.feature ?? "").trim();
    if (!feature) {
      continue;
    }
    const importanceGain = Number(row.importance_gain ?? 0);
    const featureDate = row.feature_date ? String(row.feature_date) : null;
    const current = grouped.get(feature) ?? { totalGain: 0, occurrenceCount: 0, latestFeatureDate: null };
    current.totalGain += Number.isFinite(importanceGain) ? importanceGain : 0;
    current.occurrenceCount += 1;
    if (featureDate && (!current.latestFeatureDate || featureDate > current.latestFeatureDate)) {
      current.latestFeatureDate = featureDate;
    }
    grouped.set(feature, current);
  }

  return Array.from(grouped.entries()).map(([feature, stats]) => ({
    feature,
    meanGain: stats.occurrenceCount > 0 ? stats.totalGain / stats.occurrenceCount : 0,
    occurrenceCount: stats.occurrenceCount,
    latestFeatureDate: stats.latestFeatureDate,
  }));
}

function formatFeatureDateLabel(value: string | null): string {
  if (!value) {
    return "—";
  }
  return value.includes("T") ? value.split("T")[0] : value;
}

function buildFeatureImportanceMeanChart(table: DataTablePayload, title: string): EChartsOption {
  const rows = aggregateFeatureImportance(table)
    .sort((a, b) => b.meanGain - a.meanGain || b.occurrenceCount - a.occurrenceCount || a.feature.localeCompare(b.feature))
    .slice(0, 15);
  return {
    title: { text: title, textStyle: { color: "hsl(var(--foreground))", fontSize: 13 } },
    tooltip: {
      trigger: "axis",
      formatter: (params: Array<{ axisValueLabel?: string; data?: { meanGain?: number; occurrenceCount?: number; latestFeatureDate?: string | null } }>) => {
        const item = params[0];
        const data = item?.data;
        return [
          item?.axisValueLabel ?? "—",
          `mean_gain: ${Number(data?.meanGain ?? 0).toFixed(2)}`,
          `出现次数: ${Number(data?.occurrenceCount ?? 0)}`,
          `latest_feature_date: ${formatFeatureDateLabel(data?.latestFeatureDate ?? null)}`,
        ].join("<br/>");
      },
    },
    grid: { left: 120, right: 16, top: 40, bottom: 24 },
    xAxis: { type: "value" as const },
    yAxis: {
      type: "category" as const,
      data: rows.map((row) => String(row.feature ?? "—")).reverse(),
    },
    series: [
      {
        type: "bar" as const,
        data: rows.map((row) => ({
          value: row.meanGain,
          meanGain: row.meanGain,
          occurrenceCount: row.occurrenceCount,
          latestFeatureDate: row.latestFeatureDate,
        })).reverse(),
        itemStyle: { color: "hsl(var(--chart-1))" },
        label: {
          show: true,
          position: "right",
          formatter: (params: { data?: { value?: number } }) => Number(params.data?.value ?? 0).toFixed(1),
        },
      },
    ],
  } as EChartsOption;
}

function buildFeatureImportanceCountChart(table: DataTablePayload, title: string): EChartsOption {
  const rows = aggregateFeatureImportance(table)
    .sort((a, b) => b.occurrenceCount - a.occurrenceCount || b.meanGain - a.meanGain || a.feature.localeCompare(b.feature))
    .slice(0, 15);
  return {
    title: { text: title, textStyle: { color: "hsl(var(--foreground))", fontSize: 13 } },
    tooltip: {
      trigger: "axis",
      formatter: (params: Array<{ axisValueLabel?: string; data?: { value?: number; meanGain?: number; latestFeatureDate?: string | null } }>) => {
        const item = params[0];
        const data = item?.data;
        return [
          item?.axisValueLabel ?? "—",
          `出现次数: ${Number(data?.value ?? 0)}`,
          `latest_feature_date: ${formatFeatureDateLabel(data?.latestFeatureDate ?? null)}`,
          `mean_gain: ${Number(data?.meanGain ?? 0).toFixed(2)}`,
        ].join("<br/>");
      },
    },
    grid: { left: 120, right: 16, top: 40, bottom: 24 },
    xAxis: { type: "value" as const, minInterval: 1 },
    yAxis: {
      type: "category" as const,
      data: rows.map((row) => row.feature).reverse(),
    },
    series: [
      {
        type: "bar" as const,
        data: rows.map((row) => ({
          value: row.occurrenceCount,
          meanGain: row.meanGain,
          latestFeatureDate: row.latestFeatureDate,
        })).reverse(),
        itemStyle: { color: "hsl(var(--chart-2))" },
        label: {
          show: true,
          position: "right",
          formatter: (params: { data?: { value?: number; latestFeatureDate?: string | null } }) =>
            `${Number(params.data?.value ?? 0)} / ${formatFeatureDateLabel(params.data?.latestFeatureDate ?? null)}`,
        },
      },
    ],
  } as EChartsOption;
}

function buildMonthlyHeatmap(table: DataTablePayload, title: string): EChartsOption {
  const rows = table?.rows ?? [];
  const months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"];
  const years = rows.map((row) => String(row.year ?? "—"));
  const data = rows.flatMap((row, yIndex) =>
    months.map((month, xIndex) => [xIndex, yIndex, Number(row[month] ?? 0)]),
  );
  const absoluteMax = Math.max(
    0.01,
    ...data.map((entry) => Math.abs(Number(entry[2] ?? 0))),
  );
  return {
    title: { text: title, textStyle: { color: "hsl(var(--foreground))", fontSize: 13 } },
    tooltip: { position: "top" },
    grid: { left: 48, right: 16, top: 40, bottom: 24 },
    xAxis: { type: "category" as const, data: months },
    yAxis: { type: "category" as const, data: years },
    visualMap: {
      show: false,
      min: -absoluteMax,
      max: absoluteMax,
      orient: "horizontal",
      left: "center",
      bottom: 0,
      inRange: {
        color: ["#198754", "#f7f1e3", "#c92a2a"],
      },
    },
    series: [
      {
        type: "heatmap" as const,
        data,
        label: {
          show: true,
          formatter: (params: { value?: [number, number, number] | undefined }) =>
            `${(Number(params.value?.[2] ?? 0) * 100).toFixed(1)}%`,
        },
      },
    ],
  } as EChartsOption;
}

function buildAnnualHeatmap(table: DataTablePayload, title: string): EChartsOption {
  const row = table?.rows?.[0] ?? {};
  const years = (table?.columns ?? []).filter((column) => /^\d{4}$/.test(column));
  const data = years.map((year, xIndex) => [xIndex, 0, Number(row[year] ?? 0)]);
  const absoluteMax = Math.max(
    0.01,
    ...data.map((entry) => Math.abs(Number(entry[2] ?? 0))),
  );
  return {
    title: { text: title, textStyle: { color: "hsl(var(--foreground))", fontSize: 13 } },
    tooltip: { position: "top" },
    grid: { left: 16, right: 16, top: 40, bottom: 24 },
    xAxis: { type: "category" as const, data: years },
    yAxis: {
      type: "category" as const,
      data: [""],
      axisLabel: { show: false },
      axisTick: { show: false },
      axisLine: { show: false },
    },
    visualMap: {
      show: false,
      min: -absoluteMax,
      max: absoluteMax,
      orient: "horizontal",
      left: "center",
      bottom: 0,
      inRange: {
        color: ["#198754", "#f7f1e3", "#c92a2a"],
      },
    },
    series: [
      {
        type: "heatmap" as const,
        data,
        label: {
          show: true,
          formatter: (params: { value?: [number, number, number] | undefined }) =>
            `${(Number(params.value?.[2] ?? 0) * 100).toFixed(1)}%`,
        },
      },
    ],
  } as EChartsOption;
}

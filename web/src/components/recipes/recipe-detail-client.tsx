"use client";

import type { EChartsOption } from "echarts";
import * as React from "react";
import { useQuery } from "@tanstack/react-query";

import { EChartsChart } from "@/components/charts/echarts-chart";
import { TrendChart } from "@/components/charts/trend-chart";
import { PageHeader } from "@/components/common/page-header";
import { StatCard } from "@/components/common/stat-card";
import { DataTable } from "@/components/data/data-table";
import { NodeCard } from "@/components/diagnostics/node-card";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { getRecipeTables } from "@/lib/api";
import { DataTablePayload, RecipeDetail } from "@/lib/types";
import { formatInteger, formatPercent } from "@/lib/format";

export function RecipeDetailClient({ detail }: { detail: RecipeDetail }) {
  const emptyTable = React.useMemo<DataTablePayload>(() => ({ columns: [], rows: [] }), []);
  const [activeTab, setActiveTab] = React.useState("diagnostics");
  const lazyTableNames = React.useMemo(() => {
    if (activeTab === "rolling") {
      return ["rolling_native_report", "rolling_native_monthly_return_heatmap", "rolling_feature_importance"];
    }
    if (activeTab === "walk-forward") {
      return ["walk_forward_native_report", "walk_forward_native_monthly_return_heatmap", "walk_forward_feature_importance"];
    }
    if (activeTab === "snapshot") {
      return ["latest_score_frame", "portfolio_targets"];
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

  return (
    <div className="space-y-6">
      <PageHeader
        kicker={detail.run_id}
        title={detail.recipe_name}
        description="单 recipe 详情页把标签口径、分数质量、回测兑现、切片稳定性和最新快照放到同一条阅读路径里。"
        badge={String(detail.recipe_config.signal_objective ?? "recipe")}
      />

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <StatCard title="Rolling rank_ic_ir" value={formatPercent(rollingSummary.rank_ic_ir, 1)} />
        <StatCard title="Rolling topk excess" value={formatPercent(rollingSummary.topk_mean_excess_return_4w, 2)} />
        <StatCard title="Walk-forward rank_ic_ir" value={formatPercent(walkSummary.rank_ic_ir, 1)} />
        <StatCard title="Walk-forward topk excess" value={formatPercent(walkSummary.topk_mean_excess_return_4w, 2)} />
      </div>

      <Tabs defaultValue="diagnostics" className="space-y-4" onValueChange={setActiveTab}>
        <TabsList>
          <TabsTrigger value="diagnostics">诊断总览</TabsTrigger>
          <TabsTrigger value="rolling">Rolling</TabsTrigger>
          <TabsTrigger value="walk-forward">Walk Forward</TabsTrigger>
          <TabsTrigger value="snapshot">Snapshot</TabsTrigger>
          <TabsTrigger value="artifacts">Artifacts</TabsTrigger>
        </TabsList>

        <TabsContent value="diagnostics" className="space-y-6">
          <div className="grid gap-4 xl:grid-cols-2">
            {detail.nodes.map((node) => (
              <NodeCard key={node.key} node={node} />
            ))}
          </div>
          <div className="grid gap-6 xl:grid-cols-2">
            <SectionCard title="标签定义">
              <div className="flex flex-wrap gap-2">
                <Badge variant="info">label: {String(detail.recipe_config.label_recipe ?? "—")}</Badge>
                <Badge variant="info">objective: {String(detail.recipe_config.signal_objective ?? "—")}</Badge>
                <Badge variant="neutral">features: {formatInteger(detail.overview.used_feature_count)}</Badge>
              </div>
            </SectionCard>
            <SectionCard title="Feature Prefilter">
              <DataTable table={getTable("feature_prefilter")} maxRows={12} />
            </SectionCard>
          </div>
          <div className="grid gap-6 xl:grid-cols-2">
            <SectionCard title="Signal Diagnostics">
              <DataTable table={getTable("signal_diagnostics")} maxRows={16} />
            </SectionCard>
            <SectionCard title="Portfolio Diagnostics">
              <DataTable table={getTable("portfolio_diagnostics")} maxRows={16} />
            </SectionCard>
          </div>
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
            <SectionCard title="Rolling Feature Importance">
              <EChartsChart option={buildFeatureImportance(getTable("rolling_feature_importance"), "Rolling Importance Gain")} />
            </SectionCard>
          </div>
          <SectionCard title="Rolling Summary">
            <DataTable table={getTable("rolling_summary")} maxRows={5} />
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
            <SectionCard title="Walk-forward Feature Importance">
              <EChartsChart option={buildFeatureImportance(getTable("walk_forward_feature_importance"), "Walk-forward Importance Gain")} />
            </SectionCard>
          </div>
          <SectionCard title="Walk-forward Summary">
            <DataTable table={getTable("walk_forward_summary")} maxRows={5} />
          </SectionCard>
          <SectionCard title="Execution Gap">
            <DataTable table={getTable("execution_diff_summary")} maxRows={6} />
          </SectionCard>
          <SectionCard title="Slice Stability">
            <DataTable table={getTable("slice_regime_summary")} maxRows={16} />
          </SectionCard>
        </TabsContent>

        <TabsContent value="snapshot" className="space-y-6">
          <LazyState queryState={lazyTablesQuery} />
          <div className="grid gap-6 xl:grid-cols-2">
            <SectionCard title="Latest Score Snapshot">
              <DataTable table={getTable("latest_score_frame")} maxRows={20} />
            </SectionCard>
            <SectionCard title="Portfolio Targets">
              <DataTable table={getTable("portfolio_targets")} maxRows={20} />
            </SectionCard>
          </div>
        </TabsContent>

        <TabsContent value="artifacts">
          <SectionCard title="Artifact Inventory">
            <DataTable
              table={{
                columns: ["name", "path", "exists", "updated_at"],
                rows: detail.artifact_inventory.map((item) => ({
                  name: item.name,
                  path: item.path,
                  exists: item.exists,
                  updated_at: item.updated_at,
                })),
              }}
              maxRows={50}
            />
          </SectionCard>
        </TabsContent>
      </Tabs>
    </div>
  );
}

function SectionCard({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <Card className="glass-card">
      <CardHeader>
        <CardTitle className="text-base">{title}</CardTitle>
      </CardHeader>
      <CardContent>{children}</CardContent>
    </Card>
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

function buildFeatureImportance(table: DataTablePayload, title: string): EChartsOption {
  const rows = [...(table?.rows ?? [])].sort((a, b) => Number(b.importance_gain ?? 0) - Number(a.importance_gain ?? 0)).slice(0, 15);
  return {
    title: { text: title, textStyle: { color: "hsl(var(--foreground))", fontSize: 13 } },
    tooltip: { trigger: "axis" },
    grid: { left: 120, right: 16, top: 40, bottom: 24 },
    xAxis: { type: "value" as const },
    yAxis: {
      type: "category" as const,
      data: rows.map((row) => String(row.feature ?? "—")).reverse(),
    },
    series: [
      {
        type: "bar" as const,
        data: rows.map((row) => Number(row.importance_gain ?? 0)).reverse(),
        itemStyle: { color: "hsl(var(--chart-1))" },
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
      min: -absoluteMax,
      max: absoluteMax,
      calculable: true,
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

"use client";

import * as React from "react";
import { useQuery } from "@tanstack/react-query";
import type { EChartsOption } from "echarts";

import { EChartsChart } from "@/components/charts/echarts-chart";
import { PageHeader } from "@/components/common/page-header";
import { DataTable } from "@/components/data/data-table";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Trash2 } from "lucide-react";
import { getCompare } from "@/lib/api";
import { CompareItemRef, CompareResponse, CompareTimeseriesSeries, RunListItem } from "@/lib/types";

export function ComparePageClient({
  runs,
  initialRunId,
}: {
  runs: RunListItem[];
  initialRunId?: string;
}) {
  const [mounted, setMounted] = React.useState(false);
  const [items, setItems] = React.useState<CompareItemRef[]>(() => buildDefaultSelections(runs, initialRunId));
  const [submittedItems, setSubmittedItems] = React.useState<CompareItemRef[] | null>(null);

  React.useEffect(() => {
    setMounted(true);
  }, []);

  React.useEffect(() => {
    setItems(buildDefaultSelections(runs, initialRunId));
    setSubmittedItems(null);
  }, [runs, initialRunId]);

  const compareQuery = useQuery<CompareResponse>({
    queryKey: ["compare", submittedItems],
    queryFn: () => getCompare(submittedItems ?? []),
    enabled: mounted && !!submittedItems && submittedItems.length >= 2 && submittedItems.length <= 4,
  });

  if (!mounted) {
    return (
      <div className="space-y-6">
        <div className="grid gap-4 xl:grid-cols-2">
          {Array.from({ length: 2 }).map((_, index) => (
            <Card key={index} className="glass-card">
              <CardHeader>
                <CardTitle className="text-base">对比项 {index + 1}</CardTitle>
              </CardHeader>
              <CardContent className="grid gap-3 md:grid-cols-3">
                {Array.from({ length: 3 }).map((__, fieldIndex) => (
                  <div key={fieldIndex} className="h-10 rounded-lg bg-surface-2/70" />
                ))}
              </CardContent>
            </Card>
          ))}
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="grid gap-4 xl:grid-cols-2">
        {items.map((item, index) => (
          <CompareSelector
            key={index}
            index={index}
            value={item}
            runs={runs}
            onChange={(next) => {
              const copy = [...items];
              copy[index] = next;
              setItems(copy);
            }}
            onRemove={items.length > 2 ? () => setItems(items.filter((_, itemIndex) => itemIndex !== index)) : undefined}
          />
        ))}
      </div>

      <div className="flex gap-3">
        <Button
          variant="outline"
          onClick={() => {
            if (items.length >= 4) return;
            setItems([...items, buildFallbackSelection(runs, items.length)]);
          }}
        >
          New Compare Item
        </Button>
        <Button
          onClick={() => setSubmittedItems(items)}
          disabled={items.length < 2 || items.length > 4}
        >
          Start Comparison
        </Button>
        {compareQuery.isLoading ? <Badge variant="info">对比中…</Badge> : null}
        {compareQuery.error ? <Badge variant="destructive">对比加载失败</Badge> : null}
      </div>

      {submittedItems && compareQuery.data ? (
        <div className="space-y-6">
          <SectionCard title="Summary Metrics">
            <DataTable table={compareQuery.data.summary_metrics} maxRows={12} />
          </SectionCard>
          {compareQuery.data.net_value_curves.length > 0 ? (
            <SectionCard title="Net Value Curves">
              <EChartsChart option={buildNetValueCurveOption(compareQuery.data.net_value_curves)} height={380} />
            </SectionCard>
          ) : null}
          <SectionCard title="Execution Gap">
            <DataTable table={compareQuery.data.execution_gap} maxRows={12} />
          </SectionCard>
          <SectionCard title="Slice Stability">
            <DataTable table={compareQuery.data.slice_stability} maxRows={18} />
          </SectionCard>
          <div className="grid gap-6 xl:grid-cols-2">
            {Object.entries(compareQuery.data.feature_importance).map(([label, table]) => (
              <SectionCard key={label} title={label}>
                <EChartsChart option={buildFeatureOption(label, table.rows)} />
              </SectionCard>
            ))}
          </div>
          <div className="grid gap-6 xl:grid-cols-2">
            {Object.entries(compareQuery.data.latest_signal_snapshot).map(([label, table]) => (
              <SectionCard key={label} title={`${label} Snapshot`}>
                <DataTable table={table} maxRows={12} />
              </SectionCard>
            ))}
          </div>
        </div>
      ) : (
        <Card className="glass-card">
          <CardContent className="py-8 text-sm text-muted-foreground">
            对比结果改为按需加载。确认 2-4 个对比项后点击“开始对比”。
          </CardContent>
        </Card>
      )}
    </div>
  );
}

function buildDefaultSelections(runs: RunListItem[], preferredRunId?: string) {
  const preferred = runs.find((run) => run.run_id === preferredRunId);
  const first = preferred ?? runs[0];
  const firstRecipe = first?.quick_summary.recipe_names[0] ?? "baseline";
  const secondRecipe = first?.quick_summary.recipe_names[1] ?? firstRecipe;
  return [
    {
      run_id: first?.run_id ?? "",
      recipe_name: firstRecipe,
      bundle: "rolling" as const,
    },
    {
      run_id: first?.run_id ?? "",
      recipe_name: secondRecipe,
      bundle: "walk_forward" as const,
    },
  ];
}

function buildFallbackSelection(runs: RunListItem[], index: number): CompareItemRef {
  const run = runs[index % Math.max(runs.length, 1)];
  return {
    run_id: run?.run_id ?? "",
    recipe_name: run?.quick_summary.recipe_names[0] ?? "baseline",
    bundle: index % 2 === 0 ? "rolling" : "walk_forward",
  };
}

function CompareSelector({
  index,
  value,
  runs,
  onChange,
  onRemove,
}: {
  index: number;
  value: CompareItemRef;
  runs: RunListItem[];
  onChange: (value: CompareItemRef) => void;
  onRemove?: () => void;
}) {
  const run = runs.find((item) => item.run_id === value.run_id) ?? runs[0];
  const recipeNames = run?.quick_summary.recipe_names ?? [];
  return (
    <Card className="glass-card">
      <CardHeader>
        <div className="flex items-center justify-between gap-3">
          <CardTitle className="text-base">Compare Item {index + 1}</CardTitle>
          {onRemove ? (
            <Button size="sm" variant="ghost" className="h-8 w-8 p-0" onClick={onRemove}>
              <Trash2 className="h-4 w-4" />
            </Button>
          ) : null}
        </div>
      </CardHeader>
      <CardContent className="grid gap-3 md:grid-cols-3">
        <Select value={value.run_id} onValueChange={(runId) => onChange({ run_id: runId, recipe_name: runs.find((item) => item.run_id === runId)?.quick_summary.recipe_names[0] ?? "baseline", bundle: value.bundle })}>
          <SelectTrigger>
            <SelectValue placeholder="选择 run" />
          </SelectTrigger>
          <SelectContent>
            {runs.map((runItem) => (
              <SelectItem key={runItem.run_id} value={runItem.run_id}>
                {runItem.run_id}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Select value={value.recipe_name} onValueChange={(recipeName) => onChange({ ...value, recipe_name: recipeName })}>
          <SelectTrigger>
            <SelectValue placeholder="选择 recipe" />
          </SelectTrigger>
          <SelectContent>
            {recipeNames.map((recipeName) => (
              <SelectItem key={recipeName} value={recipeName}>
                {recipeName}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Select value={value.bundle} onValueChange={(bundle: "rolling" | "walk_forward") => onChange({ ...value, bundle })}>
          <SelectTrigger>
            <SelectValue placeholder="选择 bundle" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="rolling">rolling</SelectItem>
            <SelectItem value="walk_forward">walk_forward</SelectItem>
          </SelectContent>
        </Select>
      </CardContent>
    </Card>
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

function buildFeatureOption(title: string, rows: Record<string, unknown>[]): EChartsOption {
  const sorted = [...rows].sort((a, b) => Number(b.importance_gain ?? 0) - Number(a.importance_gain ?? 0)).slice(0, 15);
  return {
    title: { text: title, textStyle: { color: "hsl(var(--foreground))", fontSize: 12 } },
    tooltip: { trigger: "axis" },
    grid: { left: 120, right: 12, top: 40, bottom: 24 },
    xAxis: { type: "value" as const },
    yAxis: { type: "category" as const, data: sorted.map((row) => String(row.feature ?? "—")).reverse() },
    series: [
      {
        type: "bar" as const,
        data: sorted.map((row) => Number(row.importance_gain ?? 0)).reverse(),
        itemStyle: { color: "hsl(var(--chart-2))" },
      },
    ],
  } as EChartsOption;
}

function buildNetValueCurveOption(seriesList: CompareTimeseriesSeries[]): EChartsOption {
  const palette = [
    "#2563eb",
    "#ea580c",
    "#059669",
    "#dc2626",
    "#7c3aed",
  ];
  const lineSeries = seriesList.map((series, index) => ({
    name: series.label,
    type: "line" as const,
    showSymbol: false,
    smooth: false,
    lineStyle: {
      width: series.role === "benchmark" ? 2 : 2.5,
      type: series.role === "benchmark" ? "dashed" as const : "solid" as const,
      opacity: series.role === "benchmark" ? 0.9 : 1,
    },
    itemStyle: {
      color: series.role === "benchmark" ? "#64748b" : palette[index % palette.length],
    },
    emphasis: { focus: "series" as const },
    data: series.points.map((point) => [point.date, point.value]),
  }));

  return {
    color: palette,
    tooltip: {
      trigger: "axis",
      valueFormatter: (value) =>
        typeof value === "number"
          ? value.toLocaleString(undefined, { maximumFractionDigits: 0 })
          : String(value ?? "—"),
    },
    legend: {
      type: "scroll",
      top: 0,
      textStyle: { color: "#334155", fontSize: 12, fontWeight: 500 },
    },
    grid: { left: 56, right: 20, top: 52, bottom: 42 },
    xAxis: {
      type: "time" as const,
      axisLabel: { color: "#64748b" },
      axisLine: { lineStyle: { color: "#cbd5e1" } },
    },
    yAxis: {
      type: "value" as const,
      scale: true,
      axisLabel: {
        color: "#64748b",
        formatter: (value: number) => value.toLocaleString(undefined, { notation: "compact", maximumFractionDigits: 1 }),
      },
      splitLine: { lineStyle: { color: "#e2e8f0" } },
    },
    series: lineSeries,
  } satisfies EChartsOption;
}

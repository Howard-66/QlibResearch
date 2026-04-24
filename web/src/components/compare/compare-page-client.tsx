"use client";

import * as React from "react";
import { useQuery } from "@tanstack/react-query";
import type { EChartsOption } from "echarts";

import { ChartPayloadPanel } from "@/components/charts/chart-payload-panel";
import { EChartsChart } from "@/components/charts/echarts-chart";
import { DataTable } from "@/components/data/data-table";
import { ValidationComparisonPanel } from "@/components/diagnostics/validation-comparison-panel";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Trash2 } from "lucide-react";
import { getCompare } from "@/lib/api";
import { ChartPayload, CompareItemRef, CompareResponse, CompareTimeseriesSeries, RunListItem } from "@/lib/types";
import { formatPercent } from "@/lib/format";

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
  const hasMixedBundle = submittedItems ? new Set(submittedItems.map((item) => item.bundle)).size > 1 : false;

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
        {hasMixedBundle ? <Badge variant="warning">mixed bundle</Badge> : null}
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
          {compareQuery.data.slice_stability.rows.length ? (
            <SliceStabilityPanel rows={compareQuery.data.slice_stability.rows} />
          ) : null}
          {Object.values(compareQuery.data.chart_payloads).length ? (
            <div className="grid gap-6 xl:grid-cols-2">
              {Object.values(compareQuery.data.chart_payloads).map((chart) => (
                <ChartPayloadPanel key={chart.key} chart={transposeCompareChartPayload(chart)} />
              ))}
            </div>
          ) : null}
          <SectionCard title="Validation Comparison">
            <ValidationComparisonPanel table={compareQuery.data.execution_gap} />
          </SectionCard>
          <div className="grid gap-6 xl:grid-cols-2">
            {Object.entries(compareQuery.data.signal_realization).map(([label, table]) => (
              <SectionCard key={label} title={`${label} Signal Realization`}>
                <DataTable table={table} maxRows={12} />
              </SectionCard>
            ))}
          </div>
          <div className="grid gap-6 xl:grid-cols-2">
            {Object.entries(compareQuery.data.sector_exposure).map(([label, table]) => (
              <SectionCard key={label} title={`${label} Sector Exposure`}>
                <DataTable table={table} maxRows={12} />
              </SectionCard>
            ))}
          </div>
          <div className="grid gap-6 xl:grid-cols-2">
            {Object.entries(compareQuery.data.holding_count_drift).map(([label, table]) => (
              <SectionCard key={label} title={`${label} Holding Drift`}>
                <DataTable table={table} maxRows={12} />
              </SectionCard>
            ))}
          </div>
          <div className="flex flex-wrap gap-2">
            {compareQuery.data.winner_summary.recommended_winner ? (
              <Badge variant="success">Promote Winner: {compareQuery.data.winner_summary.recommended_winner}</Badge>
            ) : null}
            {compareQuery.data.winner_summary.recommended_next_experiment ? (
              <Badge variant="info">{compareQuery.data.winner_summary.recommended_next_experiment}</Badge>
            ) : null}
            {!hasMixedBundle ? <Badge variant="neutral">benchmark shared</Badge> : <Badge variant="warning">benchmark not shared</Badge>}
          </div>
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

function SliceStabilityPanel({ rows }: { rows: Record<string, unknown>[] }) {
  const scorecard = React.useMemo(() => buildSliceScorecard(rows), [rows]);
  const heatmap = React.useMemo(() => buildSliceComparisonMatrix(rows), [rows]);

  return (
    <SectionCard title="Slice Stability">
      <div className="space-y-5">
        <div className="space-y-3">
          <div>
            <div className="text-sm font-medium">Stability Scorecard</div>
            <p className="text-xs text-muted-foreground">
              先看正向切片比例和最弱切片；如果 winner 只靠少数年份或行业拉动，这里会更早暴露。
            </p>
          </div>
          <DataTable
            table={{
              columns: [
                "item",
                "positive_slice_ratio",
                "weighted_mean_excess_4w",
                "worst_slice_excess_4w",
                "weak_slice_count",
                "worst_slice",
              ],
              rows: scorecard.map((row) => ({
                item: row.item,
                positive_slice_ratio: formatPercent(row.positiveSliceRatio, 0),
                weighted_mean_excess_4w: formatPercent(row.weightedMeanExcess, 2),
                worst_slice_excess_4w: formatPercent(row.worstExcess, 2),
                weak_slice_count: row.weakSliceCount,
                worst_slice: row.worstSlice,
                _row_class_name: row.rowClassName,
              })),
            }}
            maxRows={12}
            rowClassNameKey="_row_class_name"
          />
        </div>

        <div className="space-y-3">
          <div>
            <div className="text-sm font-medium">Slice Comparison Matrix</div>
            <p className="text-xs text-muted-foreground">
              按横截面差异最大、且最容易拖累结论的切片排序。每列是一个 compare item，数值为该切片 4w 平均超额。
            </p>
          </div>
          <DataTable
            table={{
              columns: heatmap.columns,
              rows: heatmap.rows,
            }}
            maxRows={24}
            rowClassNameKey="_row_class_name"
          />
        </div>
      </div>
    </SectionCard>
  );
}

type SliceScorecardRow = {
  item: string;
  positiveSliceRatio: number | null;
  weightedMeanExcess: number | null;
  worstExcess: number | null;
  bestExcess: number | null;
  weakSliceCount: number;
  sliceCount: number;
  worstSlice: string;
  rowClassName: string;
};

function buildSliceScorecard(rows: Record<string, unknown>[]): SliceScorecardRow[] {
  const grouped = new Map<string, Record<string, unknown>[]>();
  for (const row of rows) {
    const item = String(row.item ?? "");
    if (!item) continue;
    grouped.set(item, [...(grouped.get(item) ?? []), row]);
  }

  return Array.from(grouped.entries())
    .map(([item, itemRows]) => {
      let weightedSum = 0;
      let weightTotal = 0;
      let positiveCount = 0;
      let validCount = 0;
      let weakSliceCount = 0;
      let worstExcess: number | null = null;
      let bestExcess: number | null = null;
      let worstSlice = "—";

      for (const row of itemRows) {
        const excess = toFiniteNumber(row.mean_excess_return_4w);
        if (excess === null) continue;
        const coverage = Math.max(toFiniteNumber(row.coverage) ?? 1, 1);
        weightedSum += excess * coverage;
        weightTotal += coverage;
        positiveCount += excess > 0 ? 1 : 0;
        weakSliceCount += excess < 0 ? 1 : 0;
        validCount += 1;
        if (worstExcess === null || excess < worstExcess) {
          worstExcess = excess;
          worstSlice = `${String(row.slice_type ?? "slice")}: ${String(row.slice_value ?? "—")}`;
        }
        if (bestExcess === null || excess > bestExcess) {
          bestExcess = excess;
        }
      }

      const positiveSliceRatio = validCount ? positiveCount / validCount : null;
      const weightedMeanExcess = weightTotal ? weightedSum / weightTotal : null;
      const rowClassName =
        positiveSliceRatio !== null && positiveSliceRatio < 0.45
          ? "bg-tone-danger-soft/45 hover:bg-tone-danger-soft/60"
          : worstExcess !== null && worstExcess < -0.02
            ? "bg-tone-warning-soft/55 hover:bg-tone-warning-soft/70"
            : "";

      return {
        item,
        positiveSliceRatio,
        weightedMeanExcess,
        worstExcess,
        bestExcess,
        weakSliceCount,
        sliceCount: validCount,
        worstSlice,
        rowClassName,
      };
    })
    .sort((left, right) => {
      const ratioDiff = (right.positiveSliceRatio ?? -1) - (left.positiveSliceRatio ?? -1);
      if (ratioDiff !== 0) return ratioDiff;
      return (right.weightedMeanExcess ?? -999) - (left.weightedMeanExcess ?? -999);
    });
}

function buildSliceComparisonMatrix(rows: Record<string, unknown>[]) {
  const itemNames = Array.from(new Set(rows.map((row) => String(row.item ?? "")).filter(Boolean)));
  const sliceMap = new Map<string, { label: string; values: Map<string, { sum: number; weight: number }> }>();

  for (const row of rows) {
    const item = String(row.item ?? "");
    if (!item) continue;
    const excess = toFiniteNumber(row.mean_excess_return_4w);
    if (excess === null) continue;
    const sliceType = String(row.slice_type ?? "slice");
    const sliceValue = String(row.slice_value ?? "—");
    const key = `${sliceType}:${sliceValue}`;
    const entry = sliceMap.get(key) ?? { label: `${sliceType}: ${sliceValue}`, values: new Map() };
    const current = entry.values.get(item) ?? { sum: 0, weight: 0 };
    const coverage = Math.max(toFiniteNumber(row.coverage) ?? 1, 1);
    current.sum += excess * coverage;
    current.weight += coverage;
    entry.values.set(item, current);
    sliceMap.set(key, entry);
  }

  const matrixRows = Array.from(sliceMap.values())
    .map((entry) => {
      const numericValues = itemNames
        .map((item) => {
          const value = entry.values.get(item);
          return value && value.weight > 0 ? value.sum / value.weight : null;
        })
        .filter((value): value is number => value !== null);
      const minValue = numericValues.length ? Math.min(...numericValues) : null;
      const maxValue = numericValues.length ? Math.max(...numericValues) : null;
      const spread = minValue !== null && maxValue !== null ? maxValue - minValue : null;
      const formattedValues = Object.fromEntries(
        itemNames.map((item) => {
          const value = entry.values.get(item);
          const mean = value && value.weight > 0 ? value.sum / value.weight : null;
          return [item, mean === null ? "—" : formatPercent(mean, 2)];
        }),
      );
      return {
        slice: entry.label,
        spread,
        minValue,
        rowClassName: minValue !== null && minValue < -0.02 ? "bg-tone-warning-soft/55 hover:bg-tone-warning-soft/70" : "",
        values: formattedValues,
      };
    })
    .sort((left, right) => {
      const leftRisk = Math.abs(left.minValue ?? 0) + (left.spread ?? 0);
      const rightRisk = Math.abs(right.minValue ?? 0) + (right.spread ?? 0);
      return rightRisk - leftRisk;
    })
    .map((row) => ({
      slice: row.slice,
      spread: formatPercent(row.spread, 2),
      ...row.values,
      _row_class_name: row.rowClassName,
    }));

  return {
    columns: ["slice", "spread", ...itemNames],
    rows: matrixRows,
  };
}

function toFiniteNumber(value: unknown) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function transposeCompareChartPayload(chart: ChartPayload): ChartPayload {
  if (!["holding_summary", "exposure_summary", "realization_summary"].includes(chart.key)) {
    return chart;
  }
  const palette = ["#2563eb", "#ea580c", "#059669", "#dc2626", "#7c3aed", "#0891b2"];
  return {
    ...chart,
    x: chart.series.map((series) => series.label),
    series: chart.x.map((itemLabel, itemIndex) => ({
      key: `item-${itemIndex}`,
      label: itemLabel,
      role: "bar" as const,
      color: palette[itemIndex % palette.length],
      values: chart.series.map((series) => series.values[itemIndex] ?? null),
    })),
    thresholds: [],
  };
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

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
import { getCompare } from "@/lib/api";
import { CompareItemRef, CompareResponse, RunListItem } from "@/lib/types";

export function ComparePageClient({ runs }: { runs: RunListItem[] }) {
  const [mounted, setMounted] = React.useState(false);
  const [items, setItems] = React.useState<CompareItemRef[]>(() => buildDefaultSelections(runs));
  const [submittedItems, setSubmittedItems] = React.useState<CompareItemRef[] | null>(null);

  React.useEffect(() => {
    setMounted(true);
  }, []);

  const compareQuery = useQuery<CompareResponse>({
    queryKey: ["compare", submittedItems],
    queryFn: () => getCompare(submittedItems ?? []),
    enabled: mounted && !!submittedItems && submittedItems.length >= 2 && submittedItems.length <= 4,
  });

  if (!mounted) {
    return (
      <div className="space-y-6">
        <PageHeader
          kicker="Recipe Compare"
          title="Compare"
          description="对比粒度固定在 `(run, recipe, bundle)`。你可以同 run 比不同 recipe，也可以跨 run 比同 recipe 或不同 recipe。"
          badge="2-4 compare items"
        />
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
      <PageHeader
        kicker="Recipe Compare"
        title="Compare"
        description="对比粒度固定在 `(run, recipe, bundle)`。你可以同 run 比不同 recipe，也可以跨 run 比同 recipe 或不同 recipe。"
        badge="2-4 compare items"
      />

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
          新增对比项
        </Button>
        <Button
          onClick={() => setSubmittedItems(items)}
          disabled={items.length < 2 || items.length > 4}
        >
          开始对比
        </Button>
        {compareQuery.isLoading ? <Badge variant="info">对比中…</Badge> : null}
        {compareQuery.error ? <Badge variant="destructive">对比加载失败</Badge> : null}
      </div>

      {submittedItems && compareQuery.data ? (
        <div className="space-y-6">
          <SectionCard title="Summary Metrics">
            <DataTable table={compareQuery.data.summary_metrics} maxRows={12} />
          </SectionCard>
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

function buildDefaultSelections(runs: RunListItem[]) {
  const first = runs[0];
  const secondRecipe = first?.quick_summary.recipe_names[1] ?? first?.quick_summary.recipe_names[0] ?? "baseline";
  return [
    {
      run_id: first?.run_id ?? "",
      recipe_name: first?.quick_summary.recipe_names[0] ?? "baseline",
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
          <CardTitle className="text-base">对比项 {index + 1}</CardTitle>
          {onRemove ? (
            <Button size="sm" variant="ghost" onClick={onRemove}>
              删除
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

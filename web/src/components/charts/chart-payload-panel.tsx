"use client";

import type { EChartsOption } from "echarts";

import { EChartsChart } from "@/components/charts/echarts-chart";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { ChartPayload } from "@/lib/types";

const ANNOTATION_COLORS = {
  default: "#64748b",
  info: "#2563eb",
  success: "#059669",
  warning: "#f59e0b",
  danger: "#dc2626",
  neutral: "#64748b",
} as const;

export function ChartPayloadPanel({ chart, height = 320 }: { chart: ChartPayload; height?: number }) {
  if (!chart.x.length || !chart.series.length) {
    return (
      <Card className="glass-card">
        <CardHeader>
          <CardTitle className="text-base">{chart.title}</CardTitle>
        </CardHeader>
        <CardContent className="text-sm text-muted-foreground">当前图表缺少可展示数据。</CardContent>
      </Card>
    );
  }

  return (
    <Card className="glass-card">
      <CardHeader>
        <CardTitle className="text-base">{chart.title}</CardTitle>
        {chartDescription(chart.key) ? (
          <p className="text-xs text-muted-foreground">{chartDescription(chart.key)}</p>
        ) : null}
      </CardHeader>
      <CardContent>
        <EChartsChart option={buildChartOption(chart)} height={height} />
      </CardContent>
    </Card>
  );
}

function buildChartOption(chart: ChartPayload): EChartsOption {
  const isBar = chart.kind === "stacked_bar";
  const usesLongCategoryLabels = chart.x.length <= 8 && chart.x.some((value) => value.length > 24);
  const hasBarSeries = chart.series.some((series) => series.role === "bar") || isBar;
  const thresholdData = chart.thresholds.map((threshold) => ({
    yAxis: threshold.value,
    label: { formatter: threshold.label },
  }));
  const seriesOptions = chart.series.map((series, index) => ({
    name: series.label,
    type: (series.role === "bar" || isBar ? "bar" : "line") as "bar" | "line",
    areaStyle: chart.kind === "area" || series.role === "area" ? { opacity: 0.18 } : undefined,
    stack: series.stack ?? (isBar ? "total" : undefined),
    smooth: !isBar,
    showSymbol: false,
    itemStyle: series.color ? { color: series.color } : undefined,
    lineStyle: series.color ? { color: series.color, width: 2.5 } : { width: 2.5 },
    data: series.values,
    emphasis: { focus: "series" as const },
    markLine:
      index === 0 && thresholdData.length
        ? {
            silent: true,
            symbol: "none",
            lineStyle: { type: "dashed" as const, color: "#94a3b8" },
            data: thresholdData,
          }
        : undefined,
    markPoint:
      index === 0 && chart.annotations.length
        ? {
            symbol: "pin",
            symbolSize: 34,
            label: { show: false },
            data: chart.annotations
              .map((annotation) => {
                const pointIndex = chart.x.indexOf(annotation.x);
                if (pointIndex === -1) return null;
                return {
                  xAxis: annotation.x,
                  yAxis: maxValueAtIndex(chart, pointIndex),
                  value: annotation.label,
                  itemStyle: { color: ANNOTATION_COLORS[annotation.tone] ?? ANNOTATION_COLORS.warning },
                };
              })
              .filter(Boolean),
          }
        : undefined,
  })) as EChartsOption["series"];
  return {
    tooltip: { trigger: "axis", axisPointer: { type: hasBarSeries ? "shadow" : "line" } },
    legend: {
      type: "scroll",
      top: 0,
      textStyle: { color: "#334155", fontSize: 12, fontWeight: 500 },
    },
    grid: { left: 56, right: 20, top: 48, bottom: usesLongCategoryLabels ? 92 : 36 },
    xAxis: {
      type: "category",
      data: chart.x,
      axisLabel: {
        color: "#64748b",
        hideOverlap: true,
        interval: 0,
        rotate: usesLongCategoryLabels ? 22 : 0,
        formatter: (value: string) => formatCategoryLabel(value, usesLongCategoryLabels),
      },
      axisLine: { lineStyle: { color: "#cbd5e1" } },
    },
    yAxis: {
      type: "value",
      scale: true,
      axisLabel: { color: "#64748b" },
      splitLine: { lineStyle: { color: "#e2e8f0" } },
    },
    series: seriesOptions,
  } satisfies EChartsOption;
}

function chartDescription(key: string) {
  if (key === "holding_summary") {
    return "展示超出名义 TopK 的平均/最大持仓偏差，越接近 0 越符合组合定义。";
  }
  if (key === "exposure_summary") {
    return "展示行业集中度峰值，便于直接比较哪条 recipe 的行业风险更集中。";
  }
  if (key === "realization_summary") {
    return "并排比较纸面 TopK 收益、真实组合收益和执行拖累，观察信号是否兑现。";
  }
  return null;
}

function formatCategoryLabel(value: string, compact: boolean) {
  if (!compact) {
    return value;
  }
  const parts = value.split(" / ");
  if (parts.length >= 3) {
    return `${parts[1]}\n${parts[2]}`;
  }
  return value.length > 24 ? `${value.slice(0, 22)}...` : value;
}

function maxValueAtIndex(chart: ChartPayload, index: number) {
  let max = 0;
  for (const series of chart.series) {
    const candidate = series.values[index];
    if (candidate !== null && candidate !== undefined) {
      max = Math.max(max, candidate);
    }
  }
  return max;
}

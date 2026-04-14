"use client";

import * as React from "react";
import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

export function TrendChart({
  data,
  xKey,
  yKeys,
  color = "hsl(var(--chart-1))",
  benchmarkColor = "hsl(var(--chart-4))",
}: {
  data: Record<string, unknown>[];
  xKey: string;
  yKey?: string;
  yKeys?: { key: string; color?: string; name?: string }[];
  color?: string;
  benchmarkColor?: string;
}) {
  const lines = React.useMemo(() => {
    if (yKeys?.length) {
      return yKeys;
    }
    return [
      { key: "net_value", color, name: "Net Value" },
      { key: "benchmark_value", color: benchmarkColor, name: "Benchmark" },
    ];
  }, [benchmarkColor, color, yKeys]);

  const numericValues = React.useMemo(() => {
    const values: number[] = [];
    for (const row of data) {
      for (const line of lines) {
        const numeric = Number(row[line.key]);
        if (Number.isFinite(numeric)) {
          values.push(numeric);
        }
      }
    }
    return values;
  }, [data, lines]);

  const yDomain = React.useMemo<[number, number] | undefined>(() => {
    if (!numericValues.length) return undefined;
    const min = Math.min(...numericValues);
    const max = Math.max(...numericValues);
    if (min === max) {
      const padding = Math.max(Math.abs(min) * 0.05, 0.01);
      return [min - padding, max + padding];
    }
    const padding = Math.max((max - min) * 0.08, 0.01);
    return [min - padding, max + padding];
  }, [numericValues]);

  if (!data.length) {
    return <div className="rounded-lg border border-dashed border-border/70 p-8 text-sm text-muted-foreground">当前没有可绘制的趋势数据。</div>;
  }

  return (
    <div className="h-72 w-full">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data}>
          <CartesianGrid stroke="hsl(var(--chart-grid))" strokeDasharray="4 4" />
          <XAxis dataKey={xKey} stroke="hsl(var(--chart-axis))" tick={{ fontSize: 12 }} />
          <YAxis
            stroke="hsl(var(--chart-axis))"
            tick={{ fontSize: 12 }}
            domain={yDomain ?? ["auto", "auto"]}
            tickFormatter={(value) => Number(value).toFixed(2)}
          />
          <Tooltip />
          {lines.map((line) => (
            <Line
              key={line.key}
              type="monotone"
              dataKey={line.key}
              name={line.name ?? line.key}
              stroke={line.color ?? color}
              strokeWidth={2.2}
              dot={false}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

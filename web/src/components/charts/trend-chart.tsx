"use client";

import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

export function TrendChart({
  data,
  xKey,
  yKey,
  color = "hsl(var(--chart-1))",
}: {
  data: Record<string, unknown>[];
  xKey: string;
  yKey: string;
  color?: string;
}) {
  if (!data.length) {
    return <div className="rounded-lg border border-dashed border-border/70 p-8 text-sm text-muted-foreground">当前没有可绘制的趋势数据。</div>;
  }

  return (
    <div className="h-72 w-full">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data}>
          <CartesianGrid stroke="hsl(var(--chart-grid))" strokeDasharray="4 4" />
          <XAxis dataKey={xKey} stroke="hsl(var(--chart-axis))" tick={{ fontSize: 12 }} />
          <YAxis stroke="hsl(var(--chart-axis))" tick={{ fontSize: 12 }} />
          <Tooltip />
          <Line type="monotone" dataKey={yKey} stroke={color} strokeWidth={2.2} dot={false} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

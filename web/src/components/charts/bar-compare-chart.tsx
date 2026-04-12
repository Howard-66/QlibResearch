"use client";

import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

export function BarCompareChart({
  data,
  xKey,
  yKey,
}: {
  data: Record<string, unknown>[];
  xKey: string;
  yKey: string;
}) {
  if (!data.length) {
    return <div className="rounded-lg border border-dashed border-border/70 p-8 text-sm text-muted-foreground">当前没有可比较的数据。</div>;
  }
  return (
    <div className="h-72 w-full">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data}>
          <CartesianGrid stroke="hsl(var(--chart-grid))" strokeDasharray="4 4" />
          <XAxis dataKey={xKey} stroke="hsl(var(--chart-axis))" tick={{ fontSize: 12 }} />
          <YAxis stroke="hsl(var(--chart-axis))" tick={{ fontSize: 12 }} />
          <Tooltip />
          <Bar dataKey={yKey} fill="hsl(var(--chart-1))" radius={[8, 8, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

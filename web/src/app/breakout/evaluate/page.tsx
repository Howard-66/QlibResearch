"use client";

import * as React from "react";
import type { EChartsOption } from "echarts";
import { ChevronDown, ChevronRight, RefreshCcw } from "lucide-react";

import { EChartsChart } from "@/components/charts/echarts-chart";
import { StatCard } from "@/components/common/stat-card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import {
  fetchEvaluationDetail,
  fetchEvaluations,
  type EvaluationDetail,
  type EvaluationItem,
} from "@/lib/breakout-api";
import { formatDateTime, formatNumber, formatPercent } from "@/lib/format";

const METRIC_KEYS = [
  { key: "spearman_corr", label: "Spearman" },
  { key: "test_spearman_corr", label: "Test Spearman" },
  { key: "rmse", label: "RMSE" },
  { key: "top5_return", label: "Top5 收益" },
  { key: "top5_winrate", label: "Top5 胜率" },
  { key: "top10_winrate", label: "Top10 胜率" },
];

function pickMetric(metrics: Record<string, number> | undefined, candidates: string[]): number | null {
  if (!metrics) return null;
  for (const k of candidates) if (k in metrics) return metrics[k];
  return null;
}

export default function BreakoutEvaluatePage() {
  const [items, setItems] = React.useState<EvaluationItem[]>([]);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState<string | null>(null);
  const [expandedId, setExpandedId] = React.useState<string | null>(null);
  const [detailMap, setDetailMap] = React.useState<Record<string, EvaluationDetail>>({});
  const [detailLoading, setDetailLoading] = React.useState<string | null>(null);

  const load = React.useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const list = await fetchEvaluations();
      setItems(list);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  React.useEffect(() => {
    load();
  }, [load]);

  async function toggle(id: string) {
    if (expandedId === id) {
      setExpandedId(null);
      return;
    }
    setExpandedId(id);
    if (!detailMap[id]) {
      setDetailLoading(id);
      try {
        const detail = await fetchEvaluationDetail(id);
        setDetailMap((prev) => ({ ...prev, [id]: detail }));
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
      } finally {
        setDetailLoading(null);
      }
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-2xl font-semibold">评估对比</h1>
          <p className="text-sm text-muted-foreground">查看模型评估列表、特征重要性与核心指标</p>
        </div>
        <Button variant="outline" size="sm" onClick={load} disabled={loading}>
          <RefreshCcw className={`mr-2 h-4 w-4 ${loading ? "animate-spin" : ""}`} />
          刷新
        </Button>
      </div>

      {error ? (
        <Card className="glass-card border-tone-danger-border">
          <CardContent className="py-3 text-sm text-tone-danger">加载失败：{error}</CardContent>
        </Card>
      ) : null}

      <Card className="glass-card">
        <CardHeader>
          <CardTitle className="text-base">评估列表</CardTitle>
        </CardHeader>
        <CardContent>
          {items.length === 0 ? (
            <div className="rounded-md border border-dashed border-border/60 p-6 text-center text-sm text-muted-foreground">
              {loading ? "加载中..." : "暂无评估记录"}
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="w-8"></TableHead>
                  <TableHead>模型</TableHead>
                  <TableHead>创建时间</TableHead>
                  <TableHead>Spearman</TableHead>
                  <TableHead>RMSE</TableHead>
                  <TableHead>TopK 胜率</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {items.map((item) => {
                  const expanded = expandedId === item.id;
                  const detail = detailMap[item.id];
                  const sp = pickMetric(item.metrics, ["test_spearman_corr", "spearman_corr"]);
                  const rmse = pickMetric(item.metrics, ["test_rmse", "rmse"]);
                  const win = pickMetric(item.metrics, ["top5_winrate", "top10_winrate"]);
                  return (
                    <React.Fragment key={item.id}>
                      <TableRow
                        className="cursor-pointer"
                        onClick={() => toggle(item.id)}
                      >
                        <TableCell>
                          {expanded ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
                        </TableCell>
                        <TableCell className="font-medium">{item.model_name}</TableCell>
                        <TableCell>{formatDateTime(item.created_at)}</TableCell>
                        <TableCell className="font-mono">{sp !== null ? formatNumber(sp) : "—"}</TableCell>
                        <TableCell className="font-mono">{rmse !== null ? formatNumber(rmse) : "—"}</TableCell>
                        <TableCell className="font-mono">{win !== null ? formatPercent(win) : "—"}</TableCell>
                      </TableRow>
                      {expanded ? (
                        <TableRow>
                          <TableCell colSpan={6} className="max-w-none bg-surface-2/30 p-4">
                            {detailLoading === item.id || !detail ? (
                              <div className="text-sm text-muted-foreground">加载详情中...</div>
                            ) : (
                              <EvaluationDetailView detail={detail} />
                            )}
                          </TableCell>
                        </TableRow>
                      ) : null}
                    </React.Fragment>
                  );
                })}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function EvaluationDetailView({ detail }: { detail: EvaluationDetail }) {
  const top20 = React.useMemo(() => {
    const list = [...(detail.feature_importance ?? [])];
    list.sort((a, b) => (b.gain ?? 0) - (a.gain ?? 0));
    return list.slice(0, 20).reverse();
  }, [detail]);

  const option = React.useMemo<EChartsOption>(() => {
    return {
      grid: { top: 20, right: 30, bottom: 30, left: 130, containLabel: true },
      tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
      xAxis: { type: "value", name: "Gain" },
      yAxis: {
        type: "category",
        data: top20.map((d) => d.feature),
        axisLabel: { fontSize: 11 },
      },
      series: [
        {
          name: "Gain",
          type: "bar",
          data: top20.map((d) => Number(d.gain ?? 0)),
          itemStyle: { color: "#5b8def" },
        },
      ],
    };
  }, [top20]);

  return (
    <div className="space-y-4">
      <div className="grid gap-3 md:grid-cols-3 xl:grid-cols-6">
        {METRIC_KEYS.map((m) => {
          const value = detail.metrics?.[m.key];
          if (value === undefined || value === null) return null;
          const formatted = m.key.includes("rate")
            ? formatPercent(value)
            : formatNumber(Number(value));
          return <StatCard key={m.key} title={m.label} value={formatted} compact />;
        })}
      </div>

      <div className="grid gap-3 md:grid-cols-3">
        <StatCard title="训练样本" value={String(detail.train_samples)} compact />
        <StatCard title="验证样本" value={String(detail.valid_samples)} compact />
        <StatCard title="测试样本" value={detail.test_samples != null ? String(detail.test_samples) : "—"} compact />
      </div>

      <Card className="glass-card">
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle className="text-sm">特征重要性 Top 20</CardTitle>
          <Badge variant="outline">按 Gain 排序</Badge>
        </CardHeader>
        <CardContent>
          {top20.length === 0 ? (
            <div className="text-sm text-muted-foreground">暂无特征重要性数据</div>
          ) : (
            <EChartsChart option={option} height={Math.max(320, top20.length * 22)} />
          )}
        </CardContent>
      </Card>
    </div>
  );
}

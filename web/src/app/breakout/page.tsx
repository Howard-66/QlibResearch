"use client";

import * as React from "react";
import Link from "next/link";
import { Loader2, RefreshCcw, Zap, Activity, BarChart3 } from "lucide-react";

import { StatCard } from "@/components/common/stat-card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import {
  fetchBreakoutOverview,
  fetchLatestSignals,
  triggerScoring,
  type BreakoutOverview,
  type SignalItem,
} from "@/lib/breakout-api";
import { formatDateTime, formatNumber, formatPercent } from "@/lib/format";

export default function BreakoutOverviewPage() {
  const [overview, setOverview] = React.useState<BreakoutOverview | null>(null);
  const [signals, setSignals] = React.useState<SignalItem[]>([]);
  const [modelUsed, setModelUsed] = React.useState<string | null>(null);
  const [scoredAt, setScoredAt] = React.useState<string | null>(null);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState<string | null>(null);
  const [scoring, setScoring] = React.useState(false);
  const [actionMsg, setActionMsg] = React.useState<string | null>(null);

  const load = React.useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [ov, sig] = await Promise.all([fetchBreakoutOverview(), fetchLatestSignals()]);
      setOverview(ov);
      setSignals(sig.signals ?? []);
      setModelUsed(sig.model_used ?? null);
      setScoredAt(sig.scored_at ?? null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  React.useEffect(() => {
    load();
  }, [load]);

  async function onTriggerScore() {
    setScoring(true);
    setActionMsg(null);
    try {
      const res = await triggerScoring({ top_k: 20 });
      setActionMsg(res.message ?? `评分任务状态：${res.status}`);
      await load();
    } catch (e) {
      setActionMsg(`评分失败：${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setScoring(false);
    }
  }

  const metrics = overview?.model_metrics ?? null;
  const spearman = metrics?.["test_spearman"] ?? metrics?.["spearman"] ?? null;
  const topkWin = metrics?.["topk_win_rate"] ?? metrics?.["topk_hit_rate"] ?? null;

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-2xl font-semibold">Breakout 研究概览</h1>
          <p className="text-sm text-muted-foreground">突破事件检测、模型与最新信号一站式查看</p>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" onClick={load} disabled={loading}>
            <RefreshCcw className={`mr-2 h-4 w-4 ${loading ? "animate-spin" : ""}`} />
            刷新
          </Button>
          <Button size="sm" onClick={onTriggerScore} disabled={scoring}>
            {scoring ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Zap className="mr-2 h-4 w-4" />}
            触发评分
          </Button>
          <Button variant="outline" size="sm" asChild>
            <Link href="/breakout/train">
              <Activity className="mr-2 h-4 w-4" />
              触发训练
            </Link>
          </Button>
          <Button variant="outline" size="sm" asChild>
            <Link href="/breakout/models">
              <BarChart3 className="mr-2 h-4 w-4" />
              查看模型
            </Link>
          </Button>
        </div>
      </div>

      {error ? (
        <Card className="glass-card border-tone-danger-border">
          <CardContent className="py-4 text-sm text-tone-danger">加载失败：{error}</CardContent>
        </Card>
      ) : null}

      {actionMsg ? (
        <Card className="glass-card border-tone-info-border">
          <CardContent className="py-3 text-sm text-tone-info">{actionMsg}</CardContent>
        </Card>
      ) : null}

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <StatCard
          title="总事件数"
          value={overview ? String(overview.total_events) : "—"}
          detail={`最近评分：${formatDateTime(overview?.latest_score_date)}`}
        />
        <StatCard
          title="近 7 日事件"
          value={overview ? String(overview.recent_events_7d) : "—"}
          detail="过去 7 天内检测到的突破事件"
        />
        <StatCard
          title="模型 Spearman"
          value={spearman !== null ? formatNumber(spearman) : "—"}
          detail={`训练时间：${formatDateTime(overview?.latest_train_date)}`}
        />
        <StatCard
          title="TopK 胜率"
          value={topkWin !== null ? formatPercent(topkWin) : "—"}
          detail={overview?.active_model ? `当前模型：${String(overview.active_model.id ?? overview.active_model.filename ?? "—")}` : "暂无活跃模型"}
        />
      </div>

      <Card className="glass-card">
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle className="text-base">最新信号 Top 5</CardTitle>
          <div className="flex items-center gap-2 text-xs text-muted-foreground">
            {modelUsed ? <Badge variant="info">{modelUsed}</Badge> : null}
            {scoredAt ? <span>评分时间：{formatDateTime(scoredAt)}</span> : null}
          </div>
        </CardHeader>
        <CardContent>
          {signals.length === 0 ? (
            <div className="rounded-md border border-dashed border-border/60 p-6 text-center text-sm text-muted-foreground">
              {loading ? "加载中..." : "暂无信号，请先触发评分"}
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>排名</TableHead>
                  <TableHead>股票</TableHead>
                  <TableHead>评分</TableHead>
                  <TableHead>突破价格</TableHead>
                  <TableHead>前高日期</TableHead>
                  <TableHead>频率</TableHead>
                  <TableHead>信号日期</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {signals.slice(0, 5).map((s, idx) => (
                  <TableRow key={`${s.symbol}-${s.date}-${idx}`}>
                    <TableCell className="font-mono">#{idx + 1}</TableCell>
                    <TableCell className="font-medium">{s.symbol}</TableCell>
                    <TableCell className="font-mono">{formatNumber(s.score)}</TableCell>
                    <TableCell className="font-mono">{formatNumber(s.breakout_price, 2)}</TableCell>
                    <TableCell>{s.prev_high_date}</TableCell>
                    <TableCell>
                      <Badge variant="outline">{s.frequency}</Badge>
                    </TableCell>
                    <TableCell>{s.date}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

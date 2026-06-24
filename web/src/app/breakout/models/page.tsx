"use client";

import * as React from "react";
import { CheckCircle2, Loader2, RefreshCcw, Send, Sparkles } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  activateModel,
  fetchModels,
  syncModel,
  type ModelItem,
} from "@/lib/breakout-api";
import { formatDateTime, formatNumber, formatPercent } from "@/lib/format";
import { cn } from "@/lib/utils";

export default function BreakoutModelsPage() {
  const [models, setModels] = React.useState<ModelItem[]>([]);
  const [activeId, setActiveId] = React.useState<string | null>(null);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState<string | null>(null);
  const [actingId, setActingId] = React.useState<string | null>(null);
  const [syncingId, setSyncingId] = React.useState<string | null>(null);
  const [actionMsg, setActionMsg] = React.useState<string | null>(null);

  const load = React.useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetchModels();
      setModels(res.models ?? []);
      setActiveId(res.active_model ?? null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  React.useEffect(() => {
    load();
  }, [load]);

  async function onActivate(id: string) {
    setActingId(id);
    setActionMsg(null);
    try {
      await activateModel(id);
      setActionMsg(`已激活模型：${id}`);
      await load();
    } catch (e) {
      setActionMsg(`激活失败：${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setActingId(null);
    }
  }

  async function onSync(id: string) {
    setSyncingId(id);
    setActionMsg(null);
    try {
      const res = await syncModel(id);
      setActionMsg(res.message ?? `已同步：${id}`);
    } catch (e) {
      setActionMsg(`同步失败：${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setSyncingId(null);
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-2xl font-semibold">模型版本管理</h1>
          <p className="text-sm text-muted-foreground">激活、同步并查看所有训练产出的模型版本</p>
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

      {actionMsg ? (
        <Card className="glass-card border-tone-info-border">
          <CardContent className="py-3 text-sm text-tone-info">{actionMsg}</CardContent>
        </Card>
      ) : null}

      {models.length === 0 ? (
        <Card className="glass-card">
          <CardContent className="py-8 text-center text-sm text-muted-foreground">
            {loading ? "加载中..." : "暂无模型，请先到训练页面提交训练任务"}
          </CardContent>
        </Card>
      ) : (
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          {models.map((m) => {
            const active = m.is_active || m.id === activeId;
            return (
              <Card
                key={m.id}
                className={cn(
                  "glass-card hover-lift transition-shadow",
                  active && "border-primary/50 shadow-[0_0_0_1px_hsl(var(--primary)/0.35)]",
                )}
              >
                <CardHeader className="flex flex-row items-start justify-between gap-2">
                  <div className="min-w-0">
                    <CardTitle className="truncate text-sm">{m.filename}</CardTitle>
                    <div className="mt-1 truncate text-xs text-muted-foreground">{m.id}</div>
                  </div>
                  {active ? (
                    <Badge variant="success">
                      <CheckCircle2 className="mr-1 h-3 w-3" />
                      Active
                    </Badge>
                  ) : (
                    <Badge variant="neutral">Inactive</Badge>
                  )}
                </CardHeader>
                <CardContent className="space-y-3">
                  <div className="text-xs text-muted-foreground">创建于 {formatDateTime(m.created_at)}</div>
                  <MetricsRow metrics={m.metrics} />
                  <div className="flex flex-wrap gap-2 pt-1">
                    <Button
                      size="sm"
                      onClick={() => onActivate(m.id)}
                      disabled={active || actingId === m.id}
                    >
                      {actingId === m.id ? (
                        <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                      ) : (
                        <Sparkles className="mr-2 h-4 w-4" />
                      )}
                      {active ? "已激活" : "激活"}
                    </Button>
                    <Button
                      size="sm"
                      variant="outline"
                      onClick={() => onSync(m.id)}
                      disabled={syncingId === m.id}
                    >
                      {syncingId === m.id ? (
                        <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                      ) : (
                        <Send className="mr-2 h-4 w-4" />
                      )}
                      同步到 ValueInvesting
                    </Button>
                  </div>
                </CardContent>
              </Card>
            );
          })}
        </div>
      )}
    </div>
  );
}

function MetricsRow({ metrics }: { metrics: Record<string, number> | null }) {
  if (!metrics) {
    return <div className="text-xs text-muted-foreground">无指标</div>;
  }
  const sp = metrics["test_spearman"] ?? metrics["spearman"];
  const rmse = metrics["test_rmse"] ?? metrics["rmse"];
  const win = metrics["topk_win_rate"] ?? metrics["topk_hit_rate"];
  return (
    <div className="grid grid-cols-3 gap-2 text-xs">
      <Metric label="Spearman" value={sp !== undefined ? formatNumber(sp) : "—"} />
      <Metric label="RMSE" value={rmse !== undefined ? formatNumber(rmse) : "—"} />
      <Metric label="TopK 胜率" value={win !== undefined ? formatPercent(win) : "—"} />
    </div>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-md border border-border/60 bg-surface-2/40 px-2 py-1.5">
      <div className="text-[10px] uppercase tracking-wide text-muted-foreground">{label}</div>
      <div className="mt-0.5 font-mono text-xs">{value}</div>
    </div>
  );
}

"use client";

import * as React from "react";
import { Loader2, PlayCircle, RefreshCcw } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import {
  fetchTrainStatus,
  triggerTraining,
  type TrainStatus,
} from "@/lib/breakout-api";
import { formatDateTime } from "@/lib/format";

type FormState = {
  // detector
  lookback_days: string;
  volume_ratio_min: string;
  // labeler
  forward_days: string;
  positive_threshold: string;
  // trainer
  model_type: string;
  num_leaves: string;
  learning_rate: string;
  num_boost_round: string;
};

const DEFAULTS: FormState = {
  lookback_days: "60",
  volume_ratio_min: "1.5",
  forward_days: "20",
  positive_threshold: "0.05",
  model_type: "lgbm",
  num_leaves: "31",
  learning_rate: "0.05",
  num_boost_round: "500",
};

function statusVariant(status: TrainStatus["status"]) {
  switch (status) {
    case "running":
      return "info";
    case "completed":
      return "success";
    case "failed":
      return "destructive";
    default:
      return "neutral";
  }
}

export default function BreakoutTrainPage() {
  const [form, setForm] = React.useState<FormState>(DEFAULTS);
  const [status, setStatus] = React.useState<TrainStatus | null>(null);
  const [loading, setLoading] = React.useState(false);
  const [submitting, setSubmitting] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [actionMsg, setActionMsg] = React.useState<string | null>(null);

  const loadStatus = React.useCallback(async () => {
    setLoading(true);
    try {
      const s = await fetchTrainStatus();
      setStatus(s);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  React.useEffect(() => {
    loadStatus();
  }, [loadStatus]);

  // Polling while running
  React.useEffect(() => {
    if (status?.status !== "running") return;
    const id = setInterval(loadStatus, 3000);
    return () => clearInterval(id);
  }, [status?.status, loadStatus]);

  function update<K extends keyof FormState>(key: K, value: string) {
    setForm((prev) => ({ ...prev, [key]: value }));
  }

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    setSubmitting(true);
    setActionMsg(null);
    try {
      const payload = {
        detector: {
          lookback_days: Number(form.lookback_days),
          volume_ratio_min: Number(form.volume_ratio_min),
        },
        labeler: {
          forward_days: Number(form.forward_days),
          positive_threshold: Number(form.positive_threshold),
        },
        trainer: {
          model_type: form.model_type,
          num_leaves: Number(form.num_leaves),
          learning_rate: Number(form.learning_rate),
          num_boost_round: Number(form.num_boost_round),
        },
      };
      const res = await triggerTraining(payload);
      setActionMsg(res.message ?? `训练任务已提交：${res.task_id ?? res.status ?? "ok"}`);
      await loadStatus();
    } catch (err) {
      setActionMsg(`提交失败：${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setSubmitting(false);
    }
  }

  const isRunning = status?.status === "running";

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-2xl font-semibold">训练管理</h1>
          <p className="text-sm text-muted-foreground">配置参数并触发模型训练</p>
        </div>
        <Button variant="outline" size="sm" onClick={loadStatus} disabled={loading}>
          <RefreshCcw className={`mr-2 h-4 w-4 ${loading ? "animate-spin" : ""}`} />
          刷新状态
        </Button>
      </div>

      {/* Status Card */}
      <Card className="glass-card">
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle className="text-base">训练状态</CardTitle>
          {status ? <Badge variant={statusVariant(status.status)}>{status.status}</Badge> : null}
        </CardHeader>
        <CardContent className="space-y-2 text-sm">
          {error ? <p className="text-tone-danger">加载失败：{error}</p> : null}
          {!status ? (
            <p className="text-muted-foreground">{loading ? "加载中..." : "暂无训练记录"}</p>
          ) : (
            <div className="grid gap-2 md:grid-cols-2">
              <Field label="任务 ID" value={status.task_id ?? "—"} />
              <Field label="进度" value={status.progress ?? "—"} />
              <Field label="开始时间" value={formatDateTime(status.started_at)} />
              <Field label="完成时间" value={formatDateTime(status.completed_at)} />
              {status.status === "completed" && status.result ? (
                <div className="md:col-span-2 rounded-md border border-border/60 bg-surface-2/40 p-3">
                  <div className="mb-1 text-xs font-medium text-muted-foreground">训练结果摘要</div>
                  <pre className="whitespace-pre-wrap text-xs text-foreground">{JSON.stringify(status.result, null, 2)}</pre>
                </div>
              ) : null}
              {status.status === "failed" && status.error ? (
                <div className="md:col-span-2 rounded-md border border-tone-danger-border bg-tone-danger-soft/30 p-3 text-tone-danger">
                  错误信息：{status.error}
                </div>
              ) : null}
            </div>
          )}
        </CardContent>
      </Card>

      {/* Parameter Form */}
      <form onSubmit={onSubmit} className="space-y-4">
        <div className="grid gap-4 lg:grid-cols-3">
          <Card className="glass-card">
            <CardHeader>
              <CardTitle className="text-base">检测参数</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <Field label="回看天数 (lookback_days)">
                <Input
                  type="number"
                  value={form.lookback_days}
                  onChange={(e) => update("lookback_days", e.target.value)}
                />
              </Field>
              <Field label="最小成交量比 (volume_ratio_min)">
                <Input
                  type="number"
                  step="0.1"
                  value={form.volume_ratio_min}
                  onChange={(e) => update("volume_ratio_min", e.target.value)}
                />
              </Field>
            </CardContent>
          </Card>

          <Card className="glass-card">
            <CardHeader>
              <CardTitle className="text-base">标注参数</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <Field label="前瞻天数 (forward_days)">
                <Input
                  type="number"
                  value={form.forward_days}
                  onChange={(e) => update("forward_days", e.target.value)}
                />
              </Field>
              <Field label="正样本阈值 (positive_threshold)">
                <Input
                  type="number"
                  step="0.01"
                  value={form.positive_threshold}
                  onChange={(e) => update("positive_threshold", e.target.value)}
                />
              </Field>
            </CardContent>
          </Card>

          <Card className="glass-card">
            <CardHeader>
              <CardTitle className="text-base">模型参数</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <Field label="模型类型 (model_type)">
                <Input value={form.model_type} onChange={(e) => update("model_type", e.target.value)} />
              </Field>
              <Field label="num_leaves">
                <Input
                  type="number"
                  value={form.num_leaves}
                  onChange={(e) => update("num_leaves", e.target.value)}
                />
              </Field>
              <Field label="learning_rate">
                <Input
                  type="number"
                  step="0.001"
                  value={form.learning_rate}
                  onChange={(e) => update("learning_rate", e.target.value)}
                />
              </Field>
              <Field label="num_boost_round">
                <Input
                  type="number"
                  value={form.num_boost_round}
                  onChange={(e) => update("num_boost_round", e.target.value)}
                />
              </Field>
            </CardContent>
          </Card>
        </div>

        <div className="flex items-center gap-3">
          <Button type="submit" disabled={submitting || isRunning}>
            {submitting ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <PlayCircle className="mr-2 h-4 w-4" />}
            {isRunning ? "训练进行中..." : "开始训练"}
          </Button>
          {actionMsg ? <span className="text-sm text-muted-foreground">{actionMsg}</span> : null}
        </div>
      </form>
    </div>
  );
}

function Field({ label, value, children }: { label: string; value?: string; children?: React.ReactNode }) {
  return (
    <div className="space-y-1">
      <div className="text-xs font-medium text-muted-foreground">{label}</div>
      {children ?? <div className="text-sm">{value}</div>}
    </div>
  );
}

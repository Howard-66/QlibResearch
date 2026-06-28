"use client";

import * as React from "react";
import Link from "next/link";
import { Activity, Database, PlayCircle, RefreshCcw } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { StatCard } from "@/components/common/stat-card";
import { getBreakoutConfigProfile, postBreakoutResearchTask, postRunQueue, putBreakoutConfigProfile } from "@/lib/api";
import type { BreakoutResearchSummary } from "@/lib/types";
import { formatDateTime, formatInteger, formatPercent } from "@/lib/format";

function fmt(value: number | null | undefined, digits = 4) {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  return value.toFixed(digits);
}

type FormState = {
  model_id: string;
  universe_profile: string;
  start_date: string;
  end_date: string;
  evaluation_mode: "fixed_split" | "walk_forward";
  train_end_date: string;
  valid_end_date: string;
  walk_forward_train_days: string;
  walk_forward_valid_days: string;
  walk_forward_test_days: string;
  walk_forward_step_days: string;
  walk_forward_max_folds: string;
  lookback_window: string;
  min_volume_ratio: string;
  cache_policy: "auto" | "refresh" | "reuse";
};

function formatDateInput(date: Date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function parseDateInput(value: string) {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value);
  if (!match) return null;
  const date = new Date(Number(match[1]), Number(match[2]) - 1, Number(match[3]));
  return Number.isNaN(date.getTime()) ? null : date;
}

function addDays(date: Date, days: number) {
  const next = new Date(date);
  next.setDate(next.getDate() + days);
  return next;
}

function yesterdayInput() {
  return formatDateInput(addDays(new Date(), -1));
}

function splitDateDefaults(startDate: string, endDate: string) {
  const start = parseDateInput(startDate);
  const end = parseDateInput(endDate);
  if (!start || !end || start >= end) return { train_end_date: "", valid_end_date: "" };

  const dayMs = 24 * 60 * 60 * 1000;
  const days = Math.max(1, Math.floor((end.getTime() - start.getTime()) / dayMs));
  if (days < 3) return { train_end_date: "", valid_end_date: "" };
  const validOffset = Math.min(days - 1, Math.max(2, Math.floor(days * 0.85)));
  const trainOffset = Math.min(validOffset - 1, Math.max(1, Math.floor(days * 0.7)));
  return {
    train_end_date: formatDateInput(addDays(start, trainOffset)),
    valid_end_date: formatDateInput(addDays(start, validOffset)),
  };
}

function createDefaultForm(latest?: BreakoutResearchSummary): FormState {
  const start_date = "2020-01-01";
  const end_date = yesterdayInput();
  return {
    ...defaultForm,
    ...splitDateDefaults(start_date, end_date),
    model_id: latest?.model_id ?? defaultForm.model_id,
    universe_profile: latest?.universe_profile ?? defaultForm.universe_profile,
    start_date,
    end_date,
  };
}

function withDateSplits(form: FormState, patch: Partial<Pick<FormState, "start_date" | "end_date">>) {
  const next = { ...form, ...patch };
  return {
    ...next,
    ...splitDateDefaults(next.start_date, next.end_date),
  };
}

const defaultForm: FormState = {
  model_id: "stock-breakout-lgbm-v1",
  universe_profile: "csi300",
  start_date: "2020-01-01",
  end_date: "",
  evaluation_mode: "fixed_split",
  train_end_date: "",
  valid_end_date: "",
  walk_forward_train_days: "756",
  walk_forward_valid_days: "126",
  walk_forward_test_days: "63",
  walk_forward_step_days: "63",
  walk_forward_max_folds: "12",
  lookback_window: "60",
  min_volume_ratio: "1.0",
  cache_policy: "auto",
};

export function BreakoutDashboardClient({ runs }: { runs: BreakoutResearchSummary[] }) {
  const latest = runs.find((run) => run.is_latest) ?? runs[0];
  const [form, setForm] = React.useState<FormState>(() => createDefaultForm(latest));
  const [submitting, setSubmitting] = React.useState(false);
  const [trainOpen, setTrainOpen] = React.useState(false);
  const [message, setMessage] = React.useState<string | null>(null);
  const [profileJson, setProfileJson] = React.useState("");
  const [profileMessage, setProfileMessage] = React.useState<string | null>(null);

  React.useEffect(() => {
    let alive = true;
    getBreakoutConfigProfile("default")
      .then((profile) => {
        if (!alive) return;
        setProfileJson(JSON.stringify(profile.config, null, 2));
        const detector = (profile.config.detector ?? {}) as Record<string, unknown>;
        setForm((current) => ({
          ...current,
          lookback_window: String(detector.lookback_window ?? current.lookback_window),
          min_volume_ratio: String(detector.min_volume_ratio ?? current.min_volume_ratio),
        }));
      })
      .catch(() => undefined);
    return () => {
      alive = false;
    };
  }, []);

  async function submitTask() {
    setSubmitting(true);
    setMessage(null);
    try {
      const task = await postBreakoutResearchTask({
        model_id: form.model_id,
        universe_profile: form.universe_profile,
        universe_mode: "fixed_universe",
        start_date: form.start_date || null,
        end_date: form.end_date || null,
        evaluation_mode: form.evaluation_mode,
        train_end_date: form.train_end_date || null,
        valid_end_date: form.valid_end_date || null,
        walk_forward_train_days: Number(form.walk_forward_train_days || 756),
        walk_forward_valid_days: Number(form.walk_forward_valid_days || 126),
        walk_forward_test_days: Number(form.walk_forward_test_days || 63),
        walk_forward_step_days: Number(form.walk_forward_step_days || 63),
        walk_forward_max_folds: Number(form.walk_forward_max_folds || 0),
        lookback_window: Number(form.lookback_window || 60),
        min_volume_ratio: Number(form.min_volume_ratio || 1),
        cache_policy: form.cache_policy,
        update_latest: true,
      });
      await postRunQueue();
      setMessage(`已提交任务 ${task.task_id}`);
      setTrainOpen(false);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : String(error));
    } finally {
      setSubmitting(false);
    }
  }

  async function saveProfile() {
    setProfileMessage(null);
    try {
      const parsed = JSON.parse(profileJson || "{}") as Record<string, unknown>;
      const profile = await putBreakoutConfigProfile("default", parsed);
      setProfileJson(JSON.stringify(profile.config, null, 2));
      setProfileMessage("saved");
    } catch (error) {
      setProfileMessage(error instanceof Error ? error.message : String(error));
    }
  }

  return (
    <div className="space-y-6">
      <div className="grid gap-4 md:grid-cols-4">
        <StatCard title="Models" value={formatInteger(runs.length)} detail="breakout_event artifacts" />
        <StatCard title="Latest Events" value={formatInteger(latest?.event_count)} detail={latest?.model_id} />
        <StatCard title="Rank IC" value={fmt(latest?.rank_ic)} detail={latest?.dataset_id ?? "event score ordering"} />
        <StatCard title="Top Hit Rate" value={formatPercent(latest?.top_quantile_hit_rate)} detail="top quantile" />
      </div>

      <Card>
        <CardHeader className="flex flex-row items-center justify-between gap-3">
          <CardTitle>Breakout Research Models</CardTitle>
          <div className="flex flex-wrap items-center gap-2">
            <Button variant="default" size="sm" onClick={() => setTrainOpen(true)}>
              <PlayCircle className="h-4 w-4" />
              Run
            </Button>
            <Button variant="outline" size="sm" asChild>
              <Link href="/tasks">
                <Activity className="h-4 w-4" />
                Tasks
              </Link>
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          {runs.length === 0 ? (
            <div className="rounded-lg border border-dashed border-border/70 p-6 text-sm text-muted-foreground">
              当前没有股票突破研究产物。
            </div>
          ) : (
            <div className="overflow-auto rounded-lg border border-border/60">
              <table className="w-full min-w-[980px] text-sm">
                <thead className="bg-surface-1 text-left text-xs uppercase text-muted-foreground">
                  <tr>
                    <th className="px-3 py-2">Model</th>
                    <th className="px-3 py-2">Universe</th>
                    <th className="px-3 py-2">Feature Date</th>
                    <th className="px-3 py-2">Events</th>
                    <th className="px-3 py-2">Features</th>
                    <th className="px-3 py-2">Rank IC</th>
                    <th className="px-3 py-2">Top Return</th>
                    <th className="px-3 py-2">Status</th>
                  </tr>
                </thead>
                <tbody>
                  {runs.map((run) => (
                    <tr key={run.model_id} className="border-t border-border/50">
                      <td className="px-3 py-2 font-medium">
                        <div className="flex items-center gap-2">
                          <Link className="text-primary hover:underline" href={`/breakout/${encodeURIComponent(run.model_id)}`}>
                            {run.model_id}
                          </Link>
                          {run.is_latest ? <Badge variant="info">latest</Badge> : null}
                        </div>
                      </td>
                      <td className="px-3 py-2 text-muted-foreground">{run.universe_profile || "explicit"}</td>
                      <td className="px-3 py-2">{run.feature_date || "—"}</td>
                      <td className="px-3 py-2">{formatInteger(run.event_count)}</td>
                      <td className="px-3 py-2">{formatInteger(run.feature_count)}</td>
                      <td className="px-3 py-2">{fmt(run.rank_ic)}</td>
                      <td className="px-3 py-2">{formatPercent(run.top_quantile_mean_return)}</td>
                      <td className="px-3 py-2">
                        <Badge variant={run.artifact_status === "ready" ? "success" : run.artifact_status === "partial" ? "warning" : "destructive"}>
                          {run.artifact_status}
                        </Badge>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          <RunDialog
            open={trainOpen}
            onOpenChange={setTrainOpen}
            form={form}
            setForm={setForm}
            submitting={submitting}
            submitTask={submitTask}
            message={message}
            latestUpdatedAt={latest?.updated_at}
          />
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="flex flex-row items-center justify-between gap-3">
          <CardTitle className="text-base">Default Config Profile</CardTitle>
          <Button size="sm" variant="outline" onClick={saveProfile}>Save</Button>
        </CardHeader>
        <CardContent className="space-y-3">
          <textarea
            value={profileJson}
            onChange={(event) => setProfileJson(event.target.value)}
            className="min-h-64 w-full rounded-lg border border-input/85 bg-surface-1/80 p-3 font-mono text-xs text-foreground"
          />
          {profileMessage ? <div className="text-xs text-muted-foreground">{profileMessage}</div> : null}
        </CardContent>
      </Card>
    </div>
  );
}

function Field({
  label,
  value,
  onChange,
  className,
}: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  className?: string;
}) {
  return (
    <div className={className}>
      <label className="text-xs text-muted-foreground">{label}</label>
      <Input value={value} onChange={(event) => onChange(event.target.value)} className="mt-1.5" />
    </div>
  );
}

function RunDialog({
  open,
  onOpenChange,
  form,
  setForm,
  submitting,
  submitTask,
  message,
  latestUpdatedAt,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  form: FormState;
  setForm: React.Dispatch<React.SetStateAction<FormState>>;
  submitting: boolean;
  submitTask: () => Promise<void>;
  message: string | null;
  latestUpdatedAt?: string | null;
}) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="overflow-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Database className="h-5 w-5 text-muted-foreground" />
            Run Breakout Research
          </DialogTitle>
          <DialogDescription>
            提交股票突破研究任务到统一 Tasks 队列。`auto` 缓存会在 FDH 暂不可用时复用当前模型已有 feature panel。
          </DialogDescription>
        </DialogHeader>
        <div className="grid gap-3 sm:grid-cols-2">
          <Field label="Model ID" value={form.model_id} onChange={(value) => setForm((current) => ({ ...current, model_id: value }))} className="sm:col-span-2" />
          <div className="space-y-1.5">
            <label className="text-xs text-muted-foreground">Universe</label>
            <Select value={form.universe_profile} onValueChange={(value) => setForm((current) => ({ ...current, universe_profile: value }))}>
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="csi300">csi300</SelectItem>
                <SelectItem value="csi500">csi500</SelectItem>
                <SelectItem value="merged_csi300_500">merged_csi300_500</SelectItem>
                <SelectItem value="watchlist">watchlist</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-1.5">
            <label className="text-xs text-muted-foreground">Cache</label>
            <Select value={form.cache_policy} onValueChange={(value) => setForm((current) => ({ ...current, cache_policy: value as FormState["cache_policy"] }))}>
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="auto">auto</SelectItem>
                <SelectItem value="refresh">refresh</SelectItem>
                <SelectItem value="reuse">reuse</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <Field label="Start" value={form.start_date} onChange={(value) => setForm((current) => withDateSplits(current, { start_date: value }))} />
          <Field label="End" value={form.end_date} onChange={(value) => setForm((current) => withDateSplits(current, { end_date: value }))} />
          <div className="space-y-1.5 sm:col-span-2">
            <label className="text-xs text-muted-foreground">Evaluation</label>
            <Select value={form.evaluation_mode} onValueChange={(value) => setForm((current) => ({ ...current, evaluation_mode: value as FormState["evaluation_mode"] }))}>
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="fixed_split">fixed_split</SelectItem>
                <SelectItem value="walk_forward">walk_forward</SelectItem>
              </SelectContent>
            </Select>
          </div>
          {form.evaluation_mode === "fixed_split" ? (
            <>
              <Field label="Train End" value={form.train_end_date} onChange={(value) => setForm((current) => ({ ...current, train_end_date: value }))} />
              <Field label="Valid End" value={form.valid_end_date} onChange={(value) => setForm((current) => ({ ...current, valid_end_date: value }))} />
            </>
          ) : (
            <>
              <Field label="WF Train Days" value={form.walk_forward_train_days} onChange={(value) => setForm((current) => ({ ...current, walk_forward_train_days: value }))} />
              <Field label="WF Valid Days" value={form.walk_forward_valid_days} onChange={(value) => setForm((current) => ({ ...current, walk_forward_valid_days: value }))} />
              <Field label="WF Test Days" value={form.walk_forward_test_days} onChange={(value) => setForm((current) => ({ ...current, walk_forward_test_days: value }))} />
              <Field label="WF Step Days" value={form.walk_forward_step_days} onChange={(value) => setForm((current) => ({ ...current, walk_forward_step_days: value }))} />
              <Field label="WF Max Folds" value={form.walk_forward_max_folds} onChange={(value) => setForm((current) => ({ ...current, walk_forward_max_folds: value }))} />
            </>
          )}
          <Field label="Lookback" value={form.lookback_window} onChange={(value) => setForm((current) => ({ ...current, lookback_window: value }))} />
          <Field label="Min Volume" value={form.min_volume_ratio} onChange={(value) => setForm((current) => ({ ...current, min_volume_ratio: value }))} />
        </div>
        {message ? <div className="rounded-md border border-border/60 bg-surface-1/70 p-3 text-xs text-muted-foreground">{message}</div> : null}
        {latestUpdatedAt ? <p className="text-xs text-muted-foreground">Latest updated {formatDateTime(latestUpdatedAt)}</p> : null}
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)} disabled={submitting}>Cancel</Button>
          <Button onClick={submitTask} disabled={submitting}>
            {submitting ? <RefreshCcw className="h-4 w-4 animate-spin" /> : <PlayCircle className="h-4 w-4" />}
            Submit
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

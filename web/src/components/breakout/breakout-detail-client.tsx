"use client";

import * as React from "react";
import { BarChart3, Filter, Info, RefreshCcw } from "lucide-react";
import type { EChartsOption } from "echarts";

import { DataTable } from "@/components/data/data-table";
import { EChartsChart } from "@/components/charts/echarts-chart";
import { StatCard } from "@/components/common/stat-card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { getBreakoutTable } from "@/lib/api";
import type { BreakoutResearchDetail, BreakoutTableResponse, ChartPayload, DataTablePayload } from "@/lib/types";
import { formatBytes, formatInteger } from "@/lib/format";

function fmt(value: unknown, digits = 4) {
  if (typeof value !== "number" || Number.isNaN(value)) return value == null || value === "" ? "—" : String(value);
  return value.toFixed(digits);
}

function pct(value: unknown) {
  if (typeof value !== "number" || Number.isNaN(value)) return "—";
  return `${(value * 100).toFixed(2)}%`;
}

function jsonBlock(value: unknown) {
  return <pre className="max-h-[28rem] overflow-auto rounded-lg border border-border/60 bg-surface-1 p-4 text-xs">{JSON.stringify(value, null, 2)}</pre>;
}

const featureGroups = [
  "breakout_strength",
  "prev_high_pattern",
  "consolidation",
  "volume_price",
  "trend_momentum",
  "market_env",
  "price_momentum",
  "fundamentals",
  "interaction",
];

export function BreakoutDetailClient({ detail }: { detail: BreakoutResearchDetail }) {
  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center gap-3">
        <h2 className="text-xl font-semibold">{detail.model_id}</h2>
        <Badge variant={detail.artifact_status === "ready" ? "success" : detail.artifact_status === "partial" ? "warning" : "destructive"}>
          {detail.artifact_status}
        </Badge>
        {detail.is_latest ? <Badge variant="info">latest</Badge> : null}
        <Badge variant="outline">{detail.universe_profile || "explicit universe"}</Badge>
        <Badge variant="outline">{detail.feature_date || "no feature date"}</Badge>
      </div>

      <div className="grid gap-4 md:grid-cols-5">
        <StatCard title="Events" value={formatInteger(detail.event_count)} detail={`${formatInteger(detail.symbol_count)} symbols`} compact />
        <StatCard title="Evaluated" value={formatInteger(detail.evaluated_count)} detail={detail.label_target || undefined} compact />
        <StatCard title="Features" value={formatInteger(detail.feature_count)} detail={detail.config_hash ?? "model inputs"} compact />
        <StatCard title="Rank IC" value={fmt(detail.rank_ic)} detail="ordering quality" compact />
        <StatCard title="Top Hit" value={pct(detail.top_quantile_hit_rate)} detail="top quantile" compact />
      </div>

      <Tabs defaultValue="events" className="space-y-4">
        <TabsList className="flex flex-wrap">
          <TabsTrigger value="events">事件</TabsTrigger>
          <TabsTrigger value="labels">标注</TabsTrigger>
          <TabsTrigger value="features">特征</TabsTrigger>
          <TabsTrigger value="training">训练</TabsTrigger>
          <TabsTrigger value="evaluation">评估</TabsTrigger>
          <TabsTrigger value="model">模型</TabsTrigger>
          <TabsTrigger value="config">配置</TabsTrigger>
          <TabsTrigger value="artifacts">产物</TabsTrigger>
        </TabsList>

        <TabsContent value="events" className="space-y-4">
          <KeyValueGrid title="Event Overview" values={detail.event_overview} />
          <RemoteTable modelId={detail.model_id} tableName="events" initialTable={detail.tables.events_sample} candidateRules={eventRules(detail)} />
        </TabsContent>

        <TabsContent value="labels" className="space-y-4">
          <KeyValueGrid title="Label Overview" values={detail.label_overview} />
          <RemoteTable modelId={detail.model_id} tableName="labels" initialTable={detail.tables.labels_sample} />
        </TabsContent>

        <TabsContent value="features" className="space-y-4">
          <KeyValueGrid title="Feature Overview" values={detail.feature_overview} />
          <div className="grid gap-4 xl:grid-cols-[minmax(0,0.9fr)_minmax(0,1.1fr)]">
            <Card>
              <CardHeader><CardTitle className="text-base">Feature Catalog</CardTitle></CardHeader>
              <CardContent><DataTable table={detail.tables.feature_catalog ?? emptyTable()} maxRows={80} /></CardContent>
            </Card>
            <ChartCard chart={detail.chart_payloads.feature_importance} fallbackTitle="Feature Importance" />
          </div>
          <RemoteTable modelId={detail.model_id} tableName="features" initialTable={detail.tables.features_sample} showFeatureGroup />
        </TabsContent>

        <TabsContent value="training" className="space-y-4">
          <KeyValueGrid title="Training Overview" values={detail.training_overview} />
          <Card>
            <CardHeader><CardTitle className="text-base">Scores Snapshot</CardTitle></CardHeader>
            <CardContent><DataTable table={detail.tables.scores ?? emptyTable()} maxRows={80} /></CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="evaluation" className="space-y-4">
          <EvaluationMetrics metrics={detail.evaluation_overview} />
          <div className="grid gap-4 xl:grid-cols-2">
            <ChartCard chart={detail.chart_payloads.score_distribution} fallbackTitle="Score Distribution" />
            <ChartCard chart={detail.chart_payloads.return_distribution} fallbackTitle="Return Distribution" />
          </div>
          <Card>
            <CardHeader><CardTitle className="text-base">Annual Slices</CardTitle></CardHeader>
            <CardContent><DataTable table={detail.tables.annual_slices ?? emptyTable()} maxRows={40} /></CardContent>
          </Card>
          <Card>
            <CardHeader><CardTitle className="text-base">Symbol Slices</CardTitle></CardHeader>
            <CardContent><DataTable table={detail.tables.symbol_slices ?? emptyTable()} maxRows={40} /></CardContent>
          </Card>
          {detail.tables.walk_forward_folds?.rows?.length ? (
            <Card>
              <CardHeader><CardTitle className="text-base">Walk-forward Folds</CardTitle></CardHeader>
              <CardContent><DataTable table={detail.tables.walk_forward_folds} maxRows={80} /></CardContent>
            </Card>
          ) : null}
          <Card>
            <CardHeader><CardTitle className="text-base">Feature Importance</CardTitle></CardHeader>
            <CardContent><DataTable table={detail.tables.feature_importance ?? emptyTable()} maxRows={50} /></CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="model" className="space-y-4">
          <KeyValueGrid title="Model Overview" values={detail.model_overview} />
          <RemoteTable modelId={detail.model_id} tableName="signals" initialTable={detail.tables.top_signals} />
        </TabsContent>

        <TabsContent value="config" className="space-y-4">
          <KeyValueGrid title="Source Data" values={(detail.config_sections.source_data as Record<string, unknown>) || {}} />
          <KeyValueGrid title="Detector" values={(detail.config_sections.detector_policy as Record<string, unknown>) || {}} />
          <KeyValueGrid title="Label Policy" values={(detail.config_sections.label_policy as Record<string, unknown>) || {}} />
          <KeyValueGrid title="Trainer" values={(detail.config_sections.trainer_policy as Record<string, unknown>) || {}} />
          <Card>
            <CardHeader><CardTitle className="text-base">Manifest</CardTitle></CardHeader>
            <CardContent>{jsonBlock(detail.manifest)}</CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="artifacts" className="space-y-4">
          <Card>
            <CardHeader><CardTitle className="text-base">Stage Artifacts</CardTitle></CardHeader>
            <CardContent>
              <DataTable
                table={{
                  columns: ["stage", "name", "exists", "size_bytes", "updated_at", "path"],
                  rows: Object.entries(detail.stage_artifacts ?? {}).map(([stage, item]) => ({ stage, ...item, size_bytes: formatBytes(item.size_bytes) })),
                }}
                maxRows={50}
              />
            </CardContent>
          </Card>
          <Card>
            <CardHeader><CardTitle className="text-base">Artifact Inventory</CardTitle></CardHeader>
            <CardContent>
              <DataTable
                table={{
                  columns: ["name", "exists", "size_bytes", "updated_at", "path"],
                  rows: detail.artifact_inventory.map((item) => ({ ...item, size_bytes: formatBytes(item.size_bytes) })),
                }}
                maxRows={80}
              />
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}

type MetricFormat = "integer" | "number" | "percent" | "ratio";

type MetricSpec = {
  key: string;
  label: string;
  description: string;
  format?: MetricFormat;
};

const metricDescriptions: Record<string, string> = {
  event_count: "检测到的突破候选事件总数，包含尚未完成标注或未参与有效评估的事件。",
  evaluated_count: "同时具备预测分数和未来收益标签、可用于模型效果评估的事件数量。",
  n_samples: "当前统计口径下实际参与计算的样本数。",
  rank_ic: "预测分数与未来收益的 Spearman 秩相关，越高说明排序能力越强。",
  spearman: "预测分数与未来收益的 Spearman 秩相关，衡量事件排序质量。",
  rmse: "预测分数与未来收益之间的均方根误差，越低越好。",
  mae: "预测分数与未来收益之间的平均绝对误差，越低越好。",
  top5_mean_return: "预测分数前 5% 事件的平均未来收益。",
  top5_hit_rate: "预测分数前 5% 事件中未来收益为正的比例。",
  top10_mean_return: "预测分数前 10% 事件的平均未来收益。",
  top10_hit_rate: "预测分数前 10% 事件中未来收益为正的比例。",
  top20_mean_return: "预测分数前 20% 事件的平均未来收益。",
  top20_hit_rate: "预测分数前 20% 事件中未来收益为正的比例。",
  bottom20_mean_return: "预测分数后 20% 事件的平均未来收益，用于判断多空区分度。",
  long_short_spread: "Top20 平均收益减 Bottom20 平均收益，越大说明分数区分度越强。",
  top_quantile_mean_return: "兼容字段，当前等同于 Top20 平均未来收益。",
  top_quantile_hit_rate: "兼容字段，当前等同于 Top20 正收益比例。",
  overfit_ratio: "训练集 Rank IC 与验证/测试集 Rank IC 的比值；过高可能意味着训练集表现无法泛化。",
  walk_forward_fold_count: "成功完成训练和测试评分的 walk-forward 折数。",
  walk_forward_rank_ic: "所有 walk-forward 测试窗口合并后的 Rank IC。",
  walk_forward_top20_mean_return: "walk-forward 测试窗口中 Top20% 高分事件的平均未来收益。",
  walk_forward_top20_hit_rate: "walk-forward 测试窗口中 Top20% 高分事件未来收益为正的比例。",
  count: "分布中有效数值的数量。",
  min: "有效数值中的最小值。",
  p05: "5% 分位数，用于观察左尾极端情况。",
  p25: "25% 分位数。",
  median: "中位数。",
  p75: "75% 分位数。",
  p95: "95% 分位数，用于观察右尾极端情况。",
  max: "有效数值中的最大值。",
  mean: "平均值。",
  std: "标准差，衡量分布离散程度。",
};

const evaluationGroups: { title: string; detail?: string; metrics: MetricSpec[] }[] = [
  {
    title: "样本覆盖",
    detail: "先确认样本规模是否足够支撑后续评估。",
    metrics: [
      { key: "event_count", label: "Events", format: "integer", description: metricDescriptions.event_count },
      { key: "evaluated_count", label: "Evaluated", format: "integer", description: metricDescriptions.evaluated_count },
      { key: "n_samples", label: "Samples", format: "integer", description: metricDescriptions.n_samples },
    ],
  },
  {
    title: "排序与误差",
    detail: "衡量模型分数是否能排序未来收益，以及预测误差大小。",
    metrics: [
      { key: "rank_ic", label: "Rank IC", format: "number", description: metricDescriptions.rank_ic },
      { key: "spearman", label: "Spearman", format: "number", description: metricDescriptions.spearman },
      { key: "rmse", label: "RMSE", format: "number", description: metricDescriptions.rmse },
      { key: "mae", label: "MAE", format: "number", description: metricDescriptions.mae },
      { key: "overfit_ratio", label: "Overfit Ratio", format: "ratio", description: metricDescriptions.overfit_ratio },
    ],
  },
  {
    title: "Top 分位效果",
    detail: "重点看高分事件是否真的带来更好的收益和胜率。",
    metrics: [
      { key: "top5_mean_return", label: "Top5 Return", format: "percent", description: metricDescriptions.top5_mean_return },
      { key: "top5_hit_rate", label: "Top5 Hit", format: "percent", description: metricDescriptions.top5_hit_rate },
      { key: "top10_mean_return", label: "Top10 Return", format: "percent", description: metricDescriptions.top10_mean_return },
      { key: "top10_hit_rate", label: "Top10 Hit", format: "percent", description: metricDescriptions.top10_hit_rate },
      { key: "top20_mean_return", label: "Top20 Return", format: "percent", description: metricDescriptions.top20_mean_return },
      { key: "top20_hit_rate", label: "Top20 Hit", format: "percent", description: metricDescriptions.top20_hit_rate },
      { key: "bottom20_mean_return", label: "Bottom20 Return", format: "percent", description: metricDescriptions.bottom20_mean_return },
      { key: "long_short_spread", label: "Long-Short", format: "percent", description: metricDescriptions.long_short_spread },
    ],
  },
];

const distributionMetrics: MetricSpec[] = [
  { key: "count", label: "Count", format: "integer", description: metricDescriptions.count },
  { key: "min", label: "Min", description: metricDescriptions.min },
  { key: "p05", label: "P05", description: metricDescriptions.p05 },
  { key: "median", label: "Median", description: metricDescriptions.median },
  { key: "p95", label: "P95", description: metricDescriptions.p95 },
  { key: "max", label: "Max", description: metricDescriptions.max },
  { key: "mean", label: "Mean", description: metricDescriptions.mean },
  { key: "std", label: "Std", description: metricDescriptions.std },
];

function EvaluationMetrics({ metrics }: { metrics: Record<string, unknown> }) {
  const splits = asRecord(metrics.splits);
  const scoreDistribution = asRecord(metrics.score_distribution);
  const returnDistribution = asRecord(metrics.return_distribution);

  return (
    <div className="space-y-4">
      {evaluationGroups.map((group) => (
        <MetricGroup key={group.title} title={group.title} detail={group.detail} metrics={group.metrics} values={metrics} />
      ))}
      {Object.keys(splits).length ? <SplitMetricGroup splits={splits} /> : null}
      <div className="grid gap-4 xl:grid-cols-2">
        <MetricGroup title="Score 分布摘要" detail="模型输出分数的范围和离散程度。" metrics={distributionMetrics} values={scoreDistribution} />
        <MetricGroup title="Return 分布摘要" detail="未来收益标签的范围和离散程度。" metrics={distributionMetrics} values={returnDistribution} valueFormat="percent" />
      </div>
      {asRecord(metrics.walk_forward).fold_count !== undefined ? <WalkForwardMetricGroup metrics={metrics} /> : null}
    </div>
  );
}

function WalkForwardMetricGroup({ metrics }: { metrics: Record<string, unknown> }) {
  return (
    <MetricGroup
      title="Walk-forward 汇总"
      detail="按滚动时间窗口反复训练、验证并预测后续测试窗口，用于观察模型跨时间泛化。"
      metrics={[
        { key: "walk_forward_fold_count", label: "Folds", format: "integer", description: metricDescriptions.walk_forward_fold_count },
        { key: "walk_forward_rank_ic", label: "WF Rank IC", format: "number", description: metricDescriptions.walk_forward_rank_ic },
        { key: "walk_forward_top20_mean_return", label: "WF Top20 Return", format: "percent", description: metricDescriptions.walk_forward_top20_mean_return },
        { key: "walk_forward_top20_hit_rate", label: "WF Top20 Hit", format: "percent", description: metricDescriptions.walk_forward_top20_hit_rate },
      ]}
      values={metrics}
    />
  );
}

function SplitMetricGroup({ splits }: { splits: Record<string, unknown> }) {
  const splitNames = ["train", "valid", "test", ...Object.keys(splits).filter((key) => !["train", "valid", "test"].includes(key))];
  const rows = splitNames
    .map((split) => [split, asRecord(splits[split])] as const)
    .filter(([, values]) => Object.keys(values).length);
  if (!rows.length) return null;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">训练/验证/测试切分</CardTitle>
      </CardHeader>
      <CardContent>
        <TooltipProvider delayDuration={120}>
          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
            {rows.map(([split, values]) => (
              <div key={split} className="rounded-md border border-border/60 bg-surface-1/60 p-3">
                <div className="mb-3 text-sm font-semibold capitalize">{split}</div>
                <div className="grid gap-2 sm:grid-cols-2">
                  <MetricCell spec={{ key: "n_samples", label: "Samples", format: "integer", description: metricDescriptions.n_samples }} values={values} compact />
                  <MetricCell spec={{ key: "spearman", label: "Rank IC", format: "number", description: metricDescriptions.spearman }} values={values} compact />
                  <MetricCell spec={{ key: "top20_mean_return", label: "Top20 Return", format: "percent", description: metricDescriptions.top20_mean_return }} values={values} compact />
                  <MetricCell spec={{ key: "top20_hit_rate", label: "Top20 Hit", format: "percent", description: metricDescriptions.top20_hit_rate }} values={values} compact />
                </div>
              </div>
            ))}
          </div>
        </TooltipProvider>
      </CardContent>
    </Card>
  );
}

function MetricGroup({
  title,
  detail,
  metrics,
  values,
  valueFormat,
}: {
  title: string;
  detail?: string;
  metrics: MetricSpec[];
  values: Record<string, unknown>;
  valueFormat?: MetricFormat;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">{title}</CardTitle>
        {detail ? <div className="text-xs text-muted-foreground">{detail}</div> : null}
      </CardHeader>
      <CardContent>
        <TooltipProvider delayDuration={120}>
          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
            {metrics.map((metric) => (
              <MetricCell key={metric.key} spec={{ ...metric, format: metric.format ?? valueFormat }} values={values} />
            ))}
          </div>
        </TooltipProvider>
      </CardContent>
    </Card>
  );
}

function MetricCell({ spec, values, compact = false }: { spec: MetricSpec; values: Record<string, unknown>; compact?: boolean }) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <div className={`rounded-md border border-border/60 bg-surface-1/60 ${compact ? "p-2" : "p-3"}`}>
          <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
            <span>{spec.label}</span>
            <Info className="h-3.5 w-3.5" />
          </div>
          <div className={`mt-1 break-words font-medium ${compact ? "text-xs" : "text-sm"}`}>
            {formatMetric(values[spec.key], spec.format)}
          </div>
        </div>
      </TooltipTrigger>
      <TooltipContent className="max-w-72 whitespace-normal leading-5">{spec.description}</TooltipContent>
    </Tooltip>
  );
}

function formatMetric(value: unknown, format: MetricFormat = "number") {
  if (value === null || value === undefined || value === "") return "—";
  if (typeof value === "object") return "—";
  const numeric = Number(value);
  if (Number.isNaN(numeric)) return String(value);
  if (format === "integer") return formatInteger(numeric);
  if (format === "percent") return `${(numeric * 100).toFixed(2)}%`;
  if (format === "ratio") return numeric.toFixed(2);
  return numeric.toFixed(4);
}

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

function RemoteTable({
  modelId,
  tableName,
  initialTable,
  candidateRules = [],
  showFeatureGroup = false,
}: {
  modelId: string;
  tableName: "events" | "labels" | "features" | "signals";
  initialTable?: DataTablePayload;
  candidateRules?: string[];
  showFeatureGroup?: boolean;
}) {
  const [table, setTable] = React.useState<DataTablePayload>(initialTable ?? emptyTable());
  const [meta, setMeta] = React.useState<Pick<BreakoutTableResponse, "total" | "page" | "page_size"> | null>(null);
  const [filters, setFilters] = React.useState({
    code: "",
    start_date: "",
    end_date: "",
    label_status: "all",
    score_quantile: "all",
    candidate_rule: "all",
    feature_group: "all",
    page: "1",
    page_size: "80",
  });
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  async function load(nextPage?: string) {
    setLoading(true);
    setError(null);
    try {
      const response = await getBreakoutTable(modelId, tableName, {
        ...filters,
        page: nextPage ?? filters.page,
        label_status: filters.label_status === "all" ? undefined : filters.label_status,
        score_quantile: filters.score_quantile === "all" ? undefined : filters.score_quantile,
        candidate_rule: filters.candidate_rule === "all" ? undefined : filters.candidate_rule,
        feature_group: filters.feature_group === "all" ? undefined : filters.feature_group,
      });
      setTable(response.table);
      setMeta({ total: response.total, page: response.page, page_size: response.page_size });
      setFilters((current) => ({ ...current, page: String(response.page) }));
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between gap-3">
        <CardTitle className="flex items-center gap-2 text-base">
          <Filter className="h-4 w-4 text-muted-foreground" />
          {tableName}
        </CardTitle>
        <Button size="sm" variant="outline" onClick={() => load()} disabled={loading}>
          <RefreshCcw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
          Load
        </Button>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid gap-3 md:grid-cols-4 xl:grid-cols-7">
          <FilterInput label="Code" value={filters.code} onChange={(value) => setFilters((current) => ({ ...current, code: value, page: "1" }))} />
          <FilterInput label="Start" value={filters.start_date} onChange={(value) => setFilters((current) => ({ ...current, start_date: value, page: "1" }))} />
          <FilterInput label="End" value={filters.end_date} onChange={(value) => setFilters((current) => ({ ...current, end_date: value, page: "1" }))} />
          <SelectFilter label="Label" value={filters.label_status} onChange={(value) => setFilters((current) => ({ ...current, label_status: value, page: "1" }))} options={["all", "success", "failed", "pending"]} />
          <SelectFilter label="Score" value={filters.score_quantile} onChange={(value) => setFilters((current) => ({ ...current, score_quantile: value, page: "1" }))} options={["all", "top20", "bottom20"]} />
          {candidateRules.length ? (
            <SelectFilter label="Rule" value={filters.candidate_rule} onChange={(value) => setFilters((current) => ({ ...current, candidate_rule: value, page: "1" }))} options={["all", ...candidateRules]} />
          ) : null}
          {showFeatureGroup ? (
            <SelectFilter label="Group" value={filters.feature_group} onChange={(value) => setFilters((current) => ({ ...current, feature_group: value, page: "1" }))} options={["all", ...featureGroups]} />
          ) : null}
        </div>
        {error ? <div className="rounded-md border border-tone-danger-border bg-tone-danger-soft p-3 text-sm text-tone-danger">{error}</div> : null}
        <DataTable table={table} maxRows={Number(filters.page_size || 80)} />
        <div className="flex flex-wrap items-center justify-between gap-3 text-xs text-muted-foreground">
          <span>{meta ? `${formatInteger(table.rows.length)} / ${formatInteger(meta.total)} rows · page ${meta.page}` : `${formatInteger(table.rows.length)} rows`}</span>
          <div className="flex items-center gap-2">
            <Button size="sm" variant="outline" disabled={loading || Number(filters.page) <= 1} onClick={() => load(String(Math.max(1, Number(filters.page) - 1)))}>
              Prev
            </Button>
            <Button size="sm" variant="outline" disabled={loading || (meta ? Number(filters.page) * meta.page_size >= meta.total : true)} onClick={() => load(String(Number(filters.page) + 1))}>
              Next
            </Button>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

function KeyValueGrid({ title, values }: { title: string; values: Record<string, unknown> }) {
  return (
    <Card>
      <CardHeader><CardTitle className="text-base">{title}</CardTitle></CardHeader>
      <CardContent>
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
          {Object.entries(values).map(([key, value]) => (
            <div key={key} className="rounded-md border border-border/60 bg-surface-1/60 p-3">
              <div className="text-xs text-muted-foreground">{key}</div>
              <div className="mt-1 break-words text-sm font-medium">{Array.isArray(value) ? value.join(", ") || "—" : fmt(value)}</div>
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}

function ChartCard({ chart, fallbackTitle }: { chart?: ChartPayload; fallbackTitle: string }) {
  const option = React.useMemo(() => chartToOption(chart), [chart]);
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2 text-base">
          <BarChart3 className="h-4 w-4 text-muted-foreground" />
          {chart?.title ?? fallbackTitle}
        </CardTitle>
      </CardHeader>
      <CardContent>{option ? <EChartsChart option={option} height={320} /> : <div className="rounded-lg border border-dashed border-border/70 p-6 text-sm text-muted-foreground">当前没有可展示的数据。</div>}</CardContent>
    </Card>
  );
}

function chartToOption(chart?: ChartPayload): EChartsOption | null {
  if (!chart || !chart.x?.length || !chart.series?.length) return null;
  return {
    tooltip: { trigger: "axis" },
    grid: { left: 48, right: 18, top: 18, bottom: 70 },
    xAxis: { type: "category", data: chart.x, axisLabel: { rotate: chart.x.length > 8 ? 35 : 0 } },
    yAxis: { type: "value" },
    series: chart.series.map((series) => ({
      name: series.label,
      type: series.role === "line" ? "line" : "bar",
      data: series.values,
      smooth: series.role === "line",
    })),
  };
}

function FilterInput({ label, value, onChange }: { label: string; value: string; onChange: (value: string) => void }) {
  return (
    <div className="space-y-1.5">
      <label className="text-xs text-muted-foreground">{label}</label>
      <Input value={value} onChange={(event) => onChange(event.target.value)} />
    </div>
  );
}

function SelectFilter({ label, value, onChange, options }: { label: string; value: string; onChange: (value: string) => void; options: string[] }) {
  return (
    <div className="space-y-1.5">
      <label className="text-xs text-muted-foreground">{label}</label>
      <Select value={value} onValueChange={onChange}>
        <SelectTrigger><SelectValue /></SelectTrigger>
        <SelectContent>
          {options.map((option) => <SelectItem key={option} value={option}>{option}</SelectItem>)}
        </SelectContent>
      </Select>
    </div>
  );
}

function eventRules(detail: BreakoutResearchDetail) {
  const value = detail.event_overview.candidate_rules;
  return Array.isArray(value) ? value.map(String) : [];
}

function emptyTable(): DataTablePayload {
  return { columns: [], rows: [] };
}

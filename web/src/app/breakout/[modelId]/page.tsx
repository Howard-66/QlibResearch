import { notFound } from "next/navigation";

import { DataTable } from "@/components/data/data-table";
import { StatCard } from "@/components/common/stat-card";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { getBreakoutResearchDetail } from "@/lib/api";
import type { BreakoutResearchDetail } from "@/lib/types";

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

function KeyValueGrid({ title, values }: { title: string; values: Record<string, unknown> }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
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

export default async function BreakoutDetailPage({ params }: { params: Promise<{ modelId: string }> }) {
  const { modelId } = await params;
  let detail: BreakoutResearchDetail;
  try {
    detail = await getBreakoutResearchDetail(modelId);
  } catch {
    notFound();
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center gap-3">
        <h2 className="text-xl font-semibold">{detail.model_id}</h2>
        <Badge variant={detail.artifact_status === "ready" ? "success" : detail.artifact_status === "partial" ? "warning" : "destructive"}>
          {detail.artifact_status}
        </Badge>
        <Badge variant="outline">{detail.universe_profile || "explicit universe"}</Badge>
        <Badge variant="outline">{detail.feature_date || "no feature date"}</Badge>
      </div>

      <div className="grid gap-4 md:grid-cols-5">
        <StatCard title="Events" value={`${detail.event_count ?? "—"}`} detail={`${detail.symbol_count ?? "—"} symbols`} compact />
        <StatCard title="Evaluated" value={`${detail.evaluated_count ?? "—"}`} detail={detail.label_target || undefined} compact />
        <StatCard title="Features" value={`${detail.feature_count ?? "—"}`} detail="model inputs" compact />
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
          <Card>
            <CardHeader><CardTitle className="text-base">Event Sample</CardTitle></CardHeader>
            <CardContent><DataTable table={detail.tables.events_sample} maxRows={50} /></CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="labels" className="space-y-4">
          <KeyValueGrid title="Label Overview" values={detail.label_overview} />
          <Card>
            <CardHeader><CardTitle className="text-base">Label Sample</CardTitle></CardHeader>
            <CardContent><DataTable table={detail.tables.labels_sample} maxRows={50} /></CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="features" className="space-y-4">
          <KeyValueGrid title="Feature Overview" values={detail.feature_overview} />
          <Card>
            <CardHeader><CardTitle className="text-base">Feature Sample</CardTitle></CardHeader>
            <CardContent><DataTable table={detail.tables.features_sample} maxRows={50} /></CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="training" className="space-y-4">
          <KeyValueGrid title="Training Overview" values={detail.training_overview} />
          <Card>
            <CardHeader><CardTitle className="text-base">Scores Snapshot</CardTitle></CardHeader>
            <CardContent><DataTable table={detail.tables.scores} maxRows={50} /></CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="evaluation" className="space-y-4">
          <KeyValueGrid title="Evaluation Metrics" values={detail.evaluation_overview} />
          <Card>
            <CardHeader><CardTitle className="text-base">Metrics JSON</CardTitle></CardHeader>
            <CardContent>{jsonBlock(detail.metrics)}</CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="model" className="space-y-4">
          <KeyValueGrid title="Model Overview" values={detail.model_overview} />
          <Card>
            <CardHeader><CardTitle className="text-base">Top Signals</CardTitle></CardHeader>
            <CardContent><DataTable table={detail.tables.top_signals} maxRows={50} /></CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="config" className="space-y-4">
          <KeyValueGrid title="Source Data" values={(detail.config_sections.source_data as Record<string, unknown>) || {}} />
          <KeyValueGrid title="Label Policy" values={(detail.config_sections.label_policy as Record<string, unknown>) || {}} />
          <Card>
            <CardHeader><CardTitle className="text-base">Manifest</CardTitle></CardHeader>
            <CardContent>{jsonBlock(detail.manifest)}</CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="artifacts" className="space-y-4">
          <Card>
            <CardHeader><CardTitle className="text-base">Artifact Inventory</CardTitle></CardHeader>
            <CardContent>
              <DataTable
                table={{
                  columns: ["name", "exists", "size_bytes", "updated_at", "path"],
                  rows: detail.artifact_inventory.map((item) => ({ ...item })),
                }}
                maxRows={50}
              />
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}

import { PageHeader } from "@/components/common/page-header";
import { StatCard } from "@/components/common/stat-card";
import { DataTable } from "@/components/data/data-table";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { getPanelDetail } from "@/lib/api";
import { formatBytes, formatInteger } from "@/lib/format";

export default async function PanelDetailPage({ params }: { params: Promise<{ panelId: string }> }) {
  const { panelId } = await params;
  const panel = await getPanelDetail(panelId);

  return (
    <div className="space-y-6">
      <PageHeader
        kicker="Panel Detail"
        title={panel.name}
        description="Panel 详情页会展示样本规模、列目录、空值覆盖和样例记录，方便判断当前 panel 是否适合继续被 workflow 消费。"
        badge={panel.enrichment_scope ?? "unknown"}
      />

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <StatCard title="Rows" value={formatInteger(panel.summary.rows)} />
        <StatCard title="Instruments" value={formatInteger(panel.summary.instrument_count)} />
        <StatCard title="Columns" value={`${panel.columns.length}`} />
        <StatCard title="Size" value={formatBytes(panel.size_bytes)} />
      </div>

      <div className="flex flex-wrap gap-2">
        {panel.linked_runs.map((runId) => (
          <Badge key={runId} variant="info">
            {runId}
          </Badge>
        ))}
      </div>

      <Card className="glass-card">
        <CardHeader>
          <CardTitle className="text-base">Column Catalog</CardTitle>
        </CardHeader>
        <CardContent>
          <DataTable table={panel.column_catalog} maxRows={40} />
        </CardContent>
      </Card>

      <Card className="glass-card">
        <CardHeader>
          <CardTitle className="text-base">Sample Rows</CardTitle>
        </CardHeader>
        <CardContent>
          <DataTable table={panel.sample_rows} maxRows={20} />
        </CardContent>
      </Card>
    </div>
  );
}

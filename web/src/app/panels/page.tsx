import Link from "next/link";

import { PageHeader } from "@/components/common/page-header";
import { StatCard } from "@/components/common/stat-card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { getPanels } from "@/lib/api";
import { formatBytes, formatCompactDate, formatInteger } from "@/lib/format";

export default async function PanelsPage() {
  const panels = await getPanels();

  return (
    <div className="space-y-6">
      <PageHeader
        kicker="Panels"
        title="Panels"
        description="第一阶段只做 panel 目录与详情只读展示，帮助确认 panel 的日期范围、样本规模、enrichment scope 以及它被哪些 runs 消费。"
        badge={`${panels.length} panels`}
      />

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <StatCard title="Total Panels" value={`${panels.length}`} />
        <StatCard title="CSV Panels" value={`${panels.filter((panel) => panel.format === "csv").length}`} />
        <StatCard title="Parquet Panels" value={`${panels.filter((panel) => panel.format !== "csv").length}`} />
        <StatCard title="Linked Runs" value={`${panels.reduce((sum, panel) => sum + panel.linked_runs.length, 0)}`} />
      </div>

      <div className="grid gap-4 xl:grid-cols-2">
        {panels.map((panel) => (
          <Card key={panel.panel_id} className="glass-card hover-lift">
            <CardHeader>
              <div className="flex items-start justify-between gap-3">
                <div>
                  <CardTitle className="text-base">{panel.name}</CardTitle>
                  <p className="mt-1 text-sm text-muted-foreground">{panel.path}</p>
                </div>
                <Badge variant="info">{panel.enrichment_scope ?? "unknown"}</Badge>
              </div>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid grid-cols-2 gap-3 text-sm">
                <div>
                  <div className="text-xs text-muted-foreground">Rows</div>
                  <div className="font-medium">{formatInteger(panel.summary.rows)}</div>
                </div>
                <div>
                  <div className="text-xs text-muted-foreground">Instruments</div>
                  <div className="font-medium">{formatInteger(panel.summary.instrument_count)}</div>
                </div>
                <div>
                  <div className="text-xs text-muted-foreground">Updated</div>
                  <div className="font-medium">{formatCompactDate(panel.updated_at)}</div>
                </div>
                <div>
                  <div className="text-xs text-muted-foreground">Size</div>
                  <div className="font-medium">{formatBytes(panel.size_bytes)}</div>
                </div>
              </div>
              <div className="flex flex-wrap gap-2">
                {panel.linked_runs.map((runId) => (
                  <Badge key={runId} variant="neutral">
                    {runId}
                  </Badge>
                ))}
              </div>
              <Button asChild>
                <Link href={`/panels/${panel.panel_id}`}>打开 Panel 详情</Link>
              </Button>
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  );
}

import Link from "next/link";

import { ReceiptText } from "lucide-react";
import { CalendarCheck } from "lucide-react";
import { PageHeader } from "@/components/common/page-header";
import { StatCard } from "@/components/common/stat-card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { getPanels } from "@/lib/api";
import { formatBytes, formatCompactDate, formatInteger, formatPathName } from "@/lib/format";

export default async function PanelsPage() {
  const panels = await getPanels();

  return (
    <div className="space-y-6">
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
                  <p className="mt-1 text-sm text-muted-foreground">{formatPathName(panel.path)}</p>
                  {panel.task_description ? <p className="mt-2 text-sm text-muted-foreground">{panel.task_description}</p> : null}
                </div>
                <div className="flex flex-wrap justify-end gap-2">
                  <Badge
                    variant={
                      panel.universe_mode === "fixed_universe"
                        ? "warning"
                        : panel.universe_mode === "historical_membership"
                          ? "info"
                          : "neutral"
                    }
                  >
                    {panel.universe_mode === "fixed_universe"
                      ? "Fixed Universe"
                      : panel.universe_mode === "historical_membership"
                        ? "Historical Membership"
                        : "Unknown Universe Mode"}
                  </Badge>
                  <Badge variant="info">{panel.enrichment_scope ?? "unknown"}</Badge>
                </div>
              </div>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid grid-cols-2 gap-3 text-sm xl:grid-cols-3">
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
                <div className="col-span-2 xl:col-span-1">
                  <div className="text-xs text-muted-foreground">Date Range</div>
                  <div className="font-medium">
                    {formatCompactDate(panel.summary.start_date)} → {formatCompactDate(panel.summary.end_date)}
                  </div>
                </div>
              </div>
              <div className="flex flex-wrap gap-2">
                {panel.linked_runs.map((runId) => (
                  <Badge key={runId} variant="neutral">
                    {runId}
                  </Badge>
                ))}
              </div>
              <div className="flex flex-wrap gap-2">
                <Button asChild>
                  <Link href={`/panels/${panel.panel_id}`}>
                  Details
                  <ReceiptText className="h-4 w-4" />
                  </Link>
                </Button>
                <Button variant="outline" asChild>
                  <Link href={`/tasks?create=export_panel&sourceType=panel&sourceId=${encodeURIComponent(panel.panel_id)}`}>
                  Create Export Task
                  <CalendarCheck className="h-4 w-4" />
                  </Link>
                </Button>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  );
}

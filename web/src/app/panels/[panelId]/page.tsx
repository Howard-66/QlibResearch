import Link from "next/link";

import { CalendarCheck } from "lucide-react";
import { PageHeader } from "@/components/common/page-header";
import { StatCard } from "@/components/common/stat-card";
import { DataTable } from "@/components/data/data-table";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { getPanelDetail } from "@/lib/api";
import { formatBytes, formatCompactDate, formatInteger } from "@/lib/format";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export default async function PanelDetailPage({ params }: { params: Promise<{ panelId: string }> }) {
  const { panelId } = await params;
  const panel = await getPanelDetail(panelId);

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap gap-3">
        <Button asChild>
          <Link href={`/tasks?create=export_panel&sourceType=panel&sourceId=${encodeURIComponent(panel.panel_id)}`}>
          Create Export Task
          <CalendarCheck className="h-4 w-4" />
          </Link>
        </Button>
        <Badge
          className="h-[22px] text-[11px] px-2"
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
        {panel.universe_profile ? <Badge variant="outline" className="h-[22px] text-[11px] px-2">{panel.universe_profile}</Badge> : null}        
      </div>

      {panel.universe_mode === "historical_membership" ? (
        <Card className="glass-card">
          <CardContent className="p-4 text-sm text-muted-foreground">
            {`当前 panel 使用历史成分模式。它会按 ${panel.universe_profile ?? "所选指数"} 的逐周真实成分过滤样本，因此口径更严格，但受 index_weight 历史长度限制。`}
          </CardContent>
        </Card>
      ) : null}

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
        <StatCard
          title="Date Range"
          value={`${formatCompactDate(panel.summary.start_date)} ~ ${formatCompactDate(panel.summary.end_date)}`}
          detail={
            panel.requested_start_date || panel.requested_end_date
              ? `Requested ${formatCompactDate(panel.requested_start_date)} ~ ${formatCompactDate(panel.requested_end_date)}`
              : panel.summary.start_date && panel.summary.end_date
                ? ""
                : "未识别时间列"
          }
        />
        <StatCard title="Rows" value={formatInteger(panel.summary.rows)} />
        <StatCard title="Instruments" value={formatInteger(panel.summary.instrument_count)} />
        <StatCard title="Columns" value={`${panel.columns.length}`} />
        <StatCard title="Size" value={formatBytes(panel.size_bytes)} />
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

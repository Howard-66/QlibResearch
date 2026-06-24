import Link from "next/link";

import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { StatCard } from "@/components/common/stat-card";
import { getBreakoutResearchRuns } from "@/lib/api";

function fmt(value: number | null | undefined, digits = 4) {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  return value.toFixed(digits);
}

function pct(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  return `${(value * 100).toFixed(2)}%`;
}

export default async function BreakoutPage() {
  const runs = await getBreakoutResearchRuns();
  const latest = runs[0];

  return (
    <div className="space-y-6">
      <div className="grid gap-4 md:grid-cols-4">
        <StatCard title="Models" value={`${runs.length}`} detail="breakout_event artifacts" />
        <StatCard title="Latest Events" value={latest?.event_count != null ? `${latest.event_count}` : "—"} detail={latest?.model_id} />
        <StatCard title="Rank IC" value={fmt(latest?.rank_ic)} detail="event score ordering" />
        <StatCard title="Top Hit Rate" value={pct(latest?.top_quantile_hit_rate)} detail="top quantile" />
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Breakout Research Models</CardTitle>
        </CardHeader>
        <CardContent>
          {runs.length === 0 ? (
            <div className="rounded-lg border border-dashed border-border/70 p-6 text-sm text-muted-foreground">
              当前没有股票突破研究产物。运行 `scripts/run_stock_breakout_research.py` 后，这里会显示事件、标签、特征、训练和评估摘要。
            </div>
          ) : (
            <div className="overflow-auto rounded-lg border border-border/60">
              <table className="w-full min-w-[900px] text-sm">
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
                        <Link className="text-primary hover:underline" href={`/breakout/${encodeURIComponent(run.model_id)}`}>
                          {run.model_id}
                        </Link>
                      </td>
                      <td className="px-3 py-2 text-muted-foreground">{run.universe_profile || "explicit"}</td>
                      <td className="px-3 py-2">{run.feature_date || "—"}</td>
                      <td className="px-3 py-2">{run.event_count ?? "—"}</td>
                      <td className="px-3 py-2">{run.feature_count ?? "—"}</td>
                      <td className="px-3 py-2">{fmt(run.rank_ic)}</td>
                      <td className="px-3 py-2">{pct(run.top_quantile_mean_return)}</td>
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
        </CardContent>
      </Card>
    </div>
  );
}

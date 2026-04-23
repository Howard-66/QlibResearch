import { BarCompareChart } from "@/components/charts/bar-compare-chart";
import { PageHeader } from "@/components/common/page-header";
import { StatCard } from "@/components/common/stat-card";
import { DataTable } from "@/components/data/data-table";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { getOverview } from "@/lib/api";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export default async function OverviewPage() {
  const overview = await getOverview();
  const recentRows = overview.recent_runs.map((item) => ({
    item: item.run_id,
    rank_ic_ir: Number(item.quick_summary.baseline_metrics.walk_forward_rank_ic_ir ?? 0),
  }));

  return (
    <div className="space-y-6">
      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <StatCard title="Runs" value={`${overview.total_runs}`} detail="按最近修改时间排序" />
        <StatCard title="Panels" value={`${overview.total_panels}`} detail="panel 目录只读浏览" />
        <StatCard title="Tasks" value={`${overview.total_tasks}`} detail="文件落盘任务队列" />
        <StatCard title="Ready Runs" value={`${overview.ready_runs}`} detail="产物齐全的运行记录" />
      </div>

      <div className="grid gap-6 xl:grid-cols-[1.2fr,0.8fr]">
        <Card className="glass-card">
          <CardHeader>
            <CardTitle className="text-base">Recent Walk-forward rank_ic_ir</CardTitle>
          </CardHeader>
          <CardContent>
            <BarCompareChart data={recentRows} xKey="item" yKey="rank_ic_ir" />
          </CardContent>
        </Card>

        <Card className="glass-card">
          <CardHeader>
            <CardTitle className="text-base">Latest Runs Snapshot</CardTitle>
          </CardHeader>
          <CardContent>
            <DataTable
              table={{
                columns: ["run_id", "artifact_status", "baseline_recipe", "walk_forward_rank_ic_ir", "walk_forward_topk_mean_excess_return_4w"],
                rows: overview.recent_runs.map((item) => ({
                  run_id: item.run_id,
                  artifact_status: item.quick_summary.artifact_status,
                  baseline_recipe: item.quick_summary.baseline_recipe,
                  walk_forward_rank_ic_ir: item.quick_summary.baseline_metrics.walk_forward_rank_ic_ir,
                  walk_forward_topk_mean_excess_return_4w: item.quick_summary.baseline_metrics.walk_forward_topk_mean_excess_return_4w,
                })),
              }}
              maxRows={8}
            />
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

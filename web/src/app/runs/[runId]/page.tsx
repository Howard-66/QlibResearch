import Link from "next/link";

import { PageHeader } from "@/components/common/page-header";
import { DataTable } from "@/components/data/data-table";
import { NodeCard } from "@/components/diagnostics/node-card";
import { RunArtifactInventory } from "@/components/runs/run-artifact-inventory";
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from "@/components/ui/breadcrumb";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { getRunDetail } from "@/lib/api";

export default async function RunDetailPage({ params }: { params: Promise<{ runId: string }> }) {
  const { runId } = await params;
  const detail = await getRunDetail(runId);

  return (
    <div className="space-y-6">
      <Breadcrumb>
        <BreadcrumbList>
          <BreadcrumbItem>
            <BreadcrumbLink href="/runs">Runs</BreadcrumbLink>
          </BreadcrumbItem>
          <BreadcrumbSeparator />
          <BreadcrumbItem>
            <BreadcrumbPage>{detail.run_id}</BreadcrumbPage>
          </BreadcrumbItem>
        </BreadcrumbList>
      </Breadcrumb>

      <PageHeader
        kicker="Run Review"
        title={detail.run_id}
        description="完整 run 详情页保留配置摘要、recipe overview、promotion gate、节点诊断和 artifact 文件树，便于从 quick-judge 继续下钻。"
        badge={detail.quick_summary.artifact_status}
      />

      <div className="flex flex-wrap gap-3">
        {detail.recipes.map((recipe) => (
          <Button key={recipe.recipe_name} variant="outline" asChild>
            <Link href={`/runs/${detail.run_id}/recipes/${recipe.recipe_name}`}>{recipe.recipe_name}</Link>
          </Button>
        ))}
      </div>

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <SummaryCard title="Universe" value={String(detail.quick_summary.universe_profile ?? "—")} />
        <SummaryCard title="Panel" value={String(detail.quick_summary.panel_path ?? "—")} />
        <SummaryCard title="Recipes" value={`${detail.recipes.length}`} />
        <SummaryCard title="Artifacts" value={`${detail.quick_summary.artifact_ready_count}/${detail.quick_summary.artifact_total_count}`} />
      </div>

      <Card className="glass-card">
        <CardHeader>
          <CardTitle className="text-base">Recipe Overview</CardTitle>
        </CardHeader>
        <CardContent>
          <DataTable
            table={{
              columns: ["recipe_name", "signal_objective", "label_recipe", "used_feature_count", "rolling_rank_ic_ir", "walk_forward_rank_ic_ir", "promotion_gate_passed"],
              rows: detail.recipes.map((recipe) => ({
                recipe_name: recipe.recipe_name,
                signal_objective: recipe.signal_objective,
                label_recipe: recipe.label_recipe,
                used_feature_count: recipe.used_feature_count,
                rolling_rank_ic_ir: recipe.rolling_rank_ic_ir,
                walk_forward_rank_ic_ir: recipe.walk_forward_rank_ic_ir,
                promotion_gate_passed: recipe.promotion_gate_passed,
              })),
            }}
            maxRows={20}
          />
        </CardContent>
      </Card>

      <Card className="glass-card">
        <CardHeader>
          <CardTitle className="text-base">Config Summary</CardTitle>
        </CardHeader>
        <CardContent>
          <details className="group rounded-xl border border-border/60 bg-surface-2/40">
            <summary className="cursor-pointer list-none px-4 py-3 text-sm font-medium text-foreground marker:hidden">
              <div className="flex items-center justify-between gap-4">
                <span>展开查看全部 {Object.keys(detail.config).length} 项参数</span>
                <span className="text-xs text-muted-foreground group-open:hidden">默认折叠</span>
                <span className="hidden text-xs text-muted-foreground group-open:inline">点击收起</span>
              </div>
            </summary>
            <div className="border-t border-border/60 px-4 py-4">
              <DataTable
                table={{
                  columns: ["key", "value"],
                  rows: Object.entries(detail.config).map(([key, value]) => ({
                    key,
                    value: Array.isArray(value) ? value.join(", ") : typeof value === "object" && value !== null ? JSON.stringify(value) : value,
                  })),
                }}
                maxRows={Math.max(Object.keys(detail.config).length, 1)}
              />
            </div>
          </details>
        </CardContent>
      </Card>

      <div className="grid gap-4 xl:grid-cols-2">
        {detail.nodes.map((node) => (
          <NodeCard key={node.key} node={node} />
        ))}
      </div>

      <Card className="glass-card">
        <CardHeader>
          <CardTitle className="text-base">Promotion Gate</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex flex-wrap gap-2">
            {Object.entries(detail.promotion_gate).map(([recipe, gate]) => (
              <Badge key={recipe} variant={(gate as Record<string, unknown>).promotion_gate_passed ? "success" : "warning"}>
                {recipe}: {(gate as Record<string, unknown>).promotion_gate_passed ? "pass" : "hold"}
              </Badge>
            ))}
          </div>
        </CardContent>
      </Card>

      <Card className="glass-card">
        <CardHeader>
          <CardTitle className="text-base">Artifact Inventory</CardTitle>
        </CardHeader>
        <CardContent>
          <RunArtifactInventory runId={detail.run_id} />
        </CardContent>
      </Card>
    </div>
  );
}

function SummaryCard({ title, value }: { title: string; value: string }) {
  return (
    <Card className="glass-card">
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-medium text-muted-foreground">{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="text-lg font-semibold">{value}</div>
      </CardContent>
    </Card>
  );
}

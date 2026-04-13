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
import { formatNumber, formatPathName, formatPercent } from "@/lib/format";

const CONFIG_DESCRIPTIONS: Record<string, string> = {
  universe_profile: "研究股票池或回测范围，例如 CSI300、CSI500。",
  panel_path: "训练与评估使用的特征面板文件路径。",
  execution_panel_path: "执行层回放使用的行情或交易面板路径。",
  output_dir: "本次 workflow 产物输出目录。",
  start_date: "滚动训练与评估的起始日期。",
  end_date: "滚动训练与评估的结束日期；为空表示用最新可用数据。",
  batch_size: "按批处理样本时的批大小。",
  run_export: "控制已有产物存在时是否重新导出。",
  topk: "每期持有或评估的头部股票数量。",
  train_weeks: "主训练窗口长度，单位为周。",
  valid_weeks: "主验证窗口长度，单位为周。",
  eval_count: "主流程滚动评估次数。",
  rolling_recent_weeks: "Rolling 汇总时回看的最近周数。",
  step_weeks: "主流程每次向前滚动的周数。",
  walk_forward_enabled: "是否启用 walk-forward 评估。",
  walk_forward_start_date: "Walk-forward 评估起始日期。",
  walk_forward_end_date: "Walk-forward 评估结束日期；为空表示持续到最新。",
  walk_forward_train_weeks: "Walk-forward 训练窗口长度，单位为周。",
  walk_forward_valid_weeks: "Walk-forward 验证窗口长度，单位为周。",
  walk_forward_step_weeks: "Walk-forward 每次前移的步长，单位为周。",
  walk_forward_eval_count: "Walk-forward 评估次数；0 通常表示按可用区间自动推断。",
  benchmark_mode: "基准收益或对照策略的选择模式。",
  signal_objective: "模型优化目标，例如 huber、mae、rank 等。",
  label_recipe: "标签构造方案，决定模型预测什么目标。",
  rebalance_interval_weeks: "组合调仓频率，单位为周。",
  hold_buffer_rank: "保留已有持仓时允许的排名缓冲区。",
  universe_exit_policy: "标的退出股票池后的持仓处理规则。",
  min_liquidity_filter: "最小流动性过滤阈值。",
  min_score_spread: "最小分数离散度要求，避免信号过于拥挤。",
  industry_max_weight: "单行业权重上限。",
  diagnostics_enabled: "是否生成诊断类产物。",
  run_validation_comparison: "是否比较 native 执行与 validation 结果。",
  validation_execution_lag_steps: "Validation 执行相对信号滞后的步数。",
  validation_only_tradable: "Validation 是否只保留可交易标的。",
  validation_risk_degree: "Validation 组合风险暴露比例。",
  native_risk_degree: "Native 执行组合风险暴露比例。",
  native_only_tradable: "Native 执行是否只保留可交易标的。",
  account: "回测账户初始资金。",
  seed: "随机种子，用于保证实验可复现。",
  reproducibility_mode: "复现模式，决定是否更严格地固定随机性和线程。",
  recipe_parallel_workers: "并行执行 recipe 的 worker 数量。",
  model_num_threads: "单个模型训练允许使用的线程数。",
  publish_model: "是否发布训练后的模型产物。",
  feature_spec_path: "外部特征配置文件路径。",
  feature_groups: "启用的特征组列表。",
  included_features: "强制纳入的特征列名单。",
  excluded_features: "显式排除的特征列名单。",
};

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
        title={detail.run_id}
        description=""
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
        <SummaryCard title="Panel" value={formatPathName(detail.quick_summary.panel_path)} />
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
              columns: [
                "recipe_name",
                "signal_objective",
                "label_recipe",
                "used_feature_count",
                "rolling_rank_ic_ir",
                "rolling_net_total_return",
                "rolling_max_drawdown",
                "walk_forward_rank_ic_ir",
                "walk_forward_net_total_return",
                "walk_forward_max_drawdown",
                "promotion_gate_passed",
              ],
              rows: detail.recipes.map((recipe) => ({
                recipe_name: recipe.recipe_name,
                signal_objective: recipe.signal_objective,
                label_recipe: recipe.label_recipe,
                used_feature_count: recipe.used_feature_count,
                rolling_rank_ic_ir: formatNumber(recipe.rolling_rank_ic_ir, 3),
                rolling_net_total_return: formatPercent(recipe.rolling_net_total_return, 2),
                rolling_max_drawdown: formatPercent(recipe.rolling_max_drawdown, 2),
                walk_forward_rank_ic_ir: formatNumber(recipe.walk_forward_rank_ic_ir, 3),
                walk_forward_net_total_return: formatPercent(recipe.walk_forward_net_total_return, 2),
                walk_forward_max_drawdown: formatPercent(recipe.walk_forward_max_drawdown, 2),
                promotion_gate_passed: recipe.promotion_gate_passed,
                _row_class_name: recipe.promotion_gate_passed ? "bg-tone-success-soft/55 hover:bg-tone-success-soft/70" : "",
              })),
            }}
            maxRows={20}
            rowClassNameKey="_row_class_name"
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
                  columns: ["key", "value", "description"],
                  rows: Object.entries(detail.config).map(([key, value]) => ({
                    key,
                    value: formatConfigValue(key, value),
                    description: describeConfigKey(key),
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

function formatConfigValue(key: string, value: unknown) {
  if (typeof value === "string" && (key.endsWith("_path") || key.endsWith("_dir"))) {
    return formatPathName(value);
  }
  if (Array.isArray(value)) return value.join(", ");
  if (typeof value === "object" && value !== null) return JSON.stringify(value);
  return value;
}

function describeConfigKey(key: string) {
  if (CONFIG_DESCRIPTIONS[key]) {
    return CONFIG_DESCRIPTIONS[key];
  }
  if (key.endsWith("_path")) return "文件或目录路径参数。";
  if (key.endsWith("_date")) return "日期边界参数。";
  if (key.endsWith("_weeks")) return "时间窗口或步长参数，单位为周。";
  if (key.endsWith("_count")) return "计数类参数，用于控制样本数或评估次数。";
  if (key.endsWith("_enabled")) return "功能开关参数。";
  if (key.endsWith("_workers") || key.endsWith("_threads")) return "并发或线程资源控制参数。";
  return "工作流运行参数。";
}

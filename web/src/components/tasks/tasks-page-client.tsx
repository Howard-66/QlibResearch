"use client";

import * as React from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { PageHeader } from "@/components/common/page-header";
import { DataTable } from "@/components/data/data-table";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { getTaskLogs, getTasks, postExportPanelTask, postNativeWorkflowTask } from "@/lib/api";
import { TaskLogResponse } from "@/lib/types";

export function TasksPageClient() {
  const queryClient = useQueryClient();
  const tasksQuery = useQuery({ queryKey: ["tasks"], queryFn: getTasks, refetchInterval: 5_000 });
  const [selectedTaskId, setSelectedTaskId] = React.useState<string | null>(null);

  const logsQuery = useQuery<TaskLogResponse>({
    queryKey: ["task-logs", selectedTaskId],
    queryFn: () => getTaskLogs(selectedTaskId as string),
    enabled: Boolean(selectedTaskId),
    refetchInterval: 5_000,
  });

  const exportMutation = useMutation({
    mutationFn: postExportPanelTask,
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["tasks"] }),
  });

  const workflowMutation = useMutation({
    mutationFn: postNativeWorkflowTask,
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["tasks"] }),
  });

  return (
    <div className="space-y-6">
      <PageHeader
        title="Tasks"
        description="这里已经接入文件落盘的任务队列。你可以直接从页面发起 panel 导出或 native workflow 运行，并查看 stdout / stderr。"
        badge={`${tasksQuery.data?.length ?? 0} tasks`}
      />

      <div className="grid gap-6 xl:grid-cols-2">
        <TaskFormCard title="Export Panel" onSubmit={(payload) => exportMutation.mutate(payload)}>
          {(form) => (
            <>
              <Input placeholder="artifacts/panels/csi300_weekly.parquet" value={form.output} onChange={(event) => form.set("output", event.target.value)} />
              <Input placeholder="csi300" value={form.universe_profile} onChange={(event) => form.set("universe_profile", event.target.value)} />
              <Input placeholder="research_full" value={form.enrichment_scope} onChange={(event) => form.set("enrichment_scope", event.target.value)} />
            </>
          )}
        </TaskFormCard>

        <TaskFormCard
          title="Run Native Workflow"
          onSubmit={(payload) => workflowMutation.mutate(payload)}
          initialState={{
            display_name: "Run Native Workflow",
            requested_by: "webapp",
            config_payload: JSON.stringify({ output_dir: "artifacts/native_workflow/webapp_run", panel_path: "artifacts/panels/csi300_weekly.parquet" }, null, 2),
            recipe_names: "baseline,mae_4w",
          }}
        >
          {(form) => (
            <>
              <textarea
                className="min-h-[220px] w-full rounded-lg border border-input/85 bg-surface-1/80 px-3 py-2 text-sm"
                value={form.config_payload}
                onChange={(event) => form.set("config_payload", event.target.value)}
              />
              <Input placeholder="baseline,mae_4w" value={form.recipe_names} onChange={(event) => form.set("recipe_names", event.target.value)} />
            </>
          )}
        </TaskFormCard>
      </div>

      <Card className="glass-card">
        <CardHeader>
          <CardTitle className="text-base">Recent Tasks</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <DataTable
            table={{
              columns: ["task_id", "task_kind", "status", "display_name", "output_dir", "created_at"],
              rows: (tasksQuery.data ?? []).map((task) => ({
                task_id: task.task_id,
                task_kind: task.task_kind,
                status: task.status,
                display_name: task.display_name,
                output_dir: task.output_dir,
                created_at: task.created_at,
              })),
            }}
            maxRows={20}
          />
          <div className="flex flex-wrap gap-2">
            {(tasksQuery.data ?? []).map((task) => (
              <Button key={task.task_id} size="sm" variant={selectedTaskId === task.task_id ? "default" : "outline"} onClick={() => setSelectedTaskId(task.task_id)}>
                {task.task_id}
              </Button>
            ))}
          </div>
          {selectedTaskId ? (
            <div className="grid gap-4 xl:grid-cols-2">
              <LogCard title="stdout" content={logsQuery.data?.stdout ?? ""} />
              <LogCard title="stderr" content={logsQuery.data?.stderr ?? ""} />
            </div>
          ) : (
            <Badge variant="neutral">选择一个 task 查看日志。</Badge>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function TaskFormCard({
  title,
  onSubmit,
  children,
  initialState,
}: {
  title: string;
  onSubmit: (payload: Record<string, unknown>) => void;
  children: (form: { [key: string]: string } & { set: (key: string, value: string) => void }) => React.ReactNode;
  initialState?: Record<string, string>;
}) {
  const [state, setState] = React.useState<Record<string, string>>({
    display_name: title,
    requested_by: "webapp",
    output: "artifacts/panels/csi300_weekly.parquet",
    universe_profile: "csi300",
    enrichment_scope: "research_full",
    config_payload: "{}",
    recipe_names: "baseline",
    ...(initialState ?? {}),
  });

  const form = Object.assign({}, state, {
    set: (key: string, value: string) => setState((current) => ({ ...current, [key]: value })),
  });

  return (
    <Card className="glass-card">
      <CardHeader>
        <CardTitle className="text-base">{title}</CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        <Input placeholder="display name" value={state.display_name} onChange={(event) => setState((current) => ({ ...current, display_name: event.target.value }))} />
        {children(form)}
        <Button
          onClick={() => {
            if (title === "Run Native Workflow") {
              let parsedConfig: Record<string, unknown> = {};
              try {
                parsedConfig = JSON.parse(state.config_payload || "{}");
              } catch {
                return;
              }
              onSubmit({
                display_name: state.display_name,
                requested_by: state.requested_by,
                config_payload: parsedConfig,
                recipe_names: state.recipe_names.split(",").map((item) => item.trim()).filter(Boolean),
              });
              return;
            }
            onSubmit({
              display_name: state.display_name,
              requested_by: state.requested_by,
              output: state.output,
              universe_profile: state.universe_profile,
              enrichment_scope: state.enrichment_scope,
            });
          }}
        >
          提交任务
        </Button>
      </CardContent>
    </Card>
  );
}

function LogCard({ title, content }: { title: string; content: string }) {
  return (
    <Card className="glass-card">
      <CardHeader>
        <CardTitle className="text-base">{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <pre className="max-h-[420px] overflow-auto rounded-lg bg-surface-2/70 p-4 text-xs leading-6">{content || "暂无输出"}</pre>
      </CardContent>
    </Card>
  );
}

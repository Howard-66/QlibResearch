"use client";

import * as React from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { ArrowDown, ArrowUp, CircleHelp, Loader2, PlayCircle, Square, Trash2 } from "lucide-react";

import { PageHeader } from "@/components/common/page-header";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Switch } from "@/components/ui/switch";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import {
  getPanels,
  getPanelTaskPreset,
  getRunTaskPreset,
  getTask,
  getTaskLogs,
  getTasks,
  postExportPanelTask,
  postNativeWorkflowTask,
  postRemoveTask,
  postReorderTasks,
  postRunQueue,
  postStopCurrent,
} from "@/lib/api";
import {
  PanelSummary,
  ResearchTaskDetail,
  ResearchTaskSummary,
  TaskBoardResponse,
  TaskLogResponse,
  TaskPresetResponse,
  TaskSourceRef,
} from "@/lib/types";
import { formatDateTime, formatPathName } from "@/lib/format";
import { cn } from "@/lib/utils";
import { describeWorkflowConfigKey } from "@/lib/workflow-config-descriptions";

type EditorTaskKind = "export_panel" | "run_native_workflow";
type HistoryFilter = "all" | "succeeded" | "failed" | "cancelled";

type ExportTaskFormState = {
  display_name: string;
  description: string;
  requested_by: string;
  output: string;
  start_date: string;
  end_date: string;
  universe_profile: string;
  universe_mode: "historical_membership" | "fixed_universe";
  batch_size: string;
  enrichment_scope: string;
  feature_mode: "all" | "custom";
  feature_groups: string[];
  included_features: string;
  excluded_features: string;
};

type WorkflowTaskFormState = {
  display_name: string;
  description: string;
  requested_by: string;
  recipe_names: string;
  panel_path: string;
  execution_panel_path: string;
  execution_panel_dir: string;
  output_dir: string;
  output_root_dir: string;
  feature_spec_path: string;
  universe_profile: string;
  benchmark_mode: string;
  signal_objective: string;
  label_recipe: string;
  run_export: string;
  reproducibility_mode: string;
  universe_exit_policy: string;
  start_date: string;
  end_date: string;
  batch_size: string;
  topk: string;
  train_weeks: string;
  valid_weeks: string;
  eval_count: string;
  rolling_recent_weeks: string;
  step_weeks: string;
  rebalance_interval_weeks: string;
  hold_buffer_rank: string;
  min_liquidity_filter: string;
  min_score_spread: string;
  industry_max_weight: string;
  validation_execution_lag_steps: string;
  validation_risk_degree: string;
  native_risk_degree: string;
  account: string;
  seed: string;
  recipe_parallel_workers: string;
  model_num_threads: string;
  walk_forward_enabled: boolean;
  walk_forward_start_date: string;
  walk_forward_end_date: string;
  walk_forward_train_weeks: string;
  walk_forward_valid_weeks: string;
  walk_forward_step_weeks: string;
  walk_forward_eval_count: string;
  diagnostics_enabled: boolean;
  run_validation_comparison: boolean;
  validation_only_tradable: boolean;
  native_only_tradable: boolean;
  publish_model: boolean;
  advanced_overrides: string;
};

const UNIVERSE_PROFILE_OPTIONS = ["csi300", "csi500", "merged_csi300_500", "watchlist"] as const;
const WORKFLOW_OUTPUT_ROOT_DIR = "artifacts/native_workflow";
const WORKFLOW_DEFAULT_PANEL_PATH = "artifacts/panels/csi300_weekly.parquet";
const WORKFLOW_RECIPE_OPTIONS = ["baseline", "mae_4w", "binary_4w", "rank_blended", "huber_8w"] as const;
const BUILTIN_BENCHMARK_MODE_OPTIONS = ["auto", "flat_zero"] as const;
const SIGNAL_OBJECTIVE_OPTIONS = ["huber_regression", "mae_regression", "binary_top_quintile", "grouped_rank"] as const;
const LABEL_RECIPE_OPTIONS = ["blended_excess_4w_8w", "excess_4w", "excess_8w"] as const;
const RUN_EXPORT_OPTIONS = ["always", "auto_if_missing", "never"] as const;
const REPRODUCIBILITY_OPTIONS = ["balanced", "strict"] as const;
const UNIVERSE_EXIT_POLICY_OPTIONS = ["retain_quotes_for_existing_positions", "strict_membership_only"] as const;

const MANUAL_SOURCE: TaskSourceRef = {
  kind: "manual",
  source_id: "manual",
  label: "Manual Task",
};

export function TasksPageClient() {
  const queryClient = useQueryClient();
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();

  const tasksQuery = useQuery({
    queryKey: ["tasks"],
    queryFn: getTasks,
    refetchInterval: 4_000,
  });
  const panelsQuery = useQuery({
    queryKey: ["panels"],
    queryFn: getPanels,
  });
  const [selectedTaskId, setSelectedTaskId] = React.useState<string | null>(null);
  const [historyFilter, setHistoryFilter] = React.useState<HistoryFilter>("all");
  const [editorOpen, setEditorOpen] = React.useState(false);
  const [editorTaskKind, setEditorTaskKind] = React.useState<EditorTaskKind>("export_panel");
  const [editorSourceRef, setEditorSourceRef] = React.useState<TaskSourceRef>(MANUAL_SOURCE);
  const [exportForm, setExportForm] = React.useState<ExportTaskFormState>(defaultExportTaskForm());
  const [workflowForm, setWorkflowForm] = React.useState<WorkflowTaskFormState>(defaultWorkflowTaskForm());
  const appliedPresetKeyRef = React.useRef<string | null>(null);

  const board = tasksQuery.data;
  const selectedTaskSummary = React.useMemo(
    () => findTaskSummary(board, selectedTaskId),
    [board, selectedTaskId],
  );

  React.useEffect(() => {
    if (!board) return;
    const allTaskIds = [
      board.running_task?.task_id,
      ...board.queued_tasks.map((task) => task.task_id),
      ...board.history_tasks.map((task) => task.task_id),
    ].filter(Boolean) as string[];
    if (selectedTaskId && allTaskIds.includes(selectedTaskId)) {
      return;
    }
    const nextTaskId = board.running_task?.task_id ?? board.queued_tasks[0]?.task_id ?? board.history_tasks[0]?.task_id ?? null;
    setSelectedTaskId(nextTaskId);
  }, [board, selectedTaskId]);

  const taskDetailQuery = useQuery<ResearchTaskDetail>({
    queryKey: ["task-detail", selectedTaskId],
    queryFn: () => getTask(selectedTaskId as string),
    enabled: Boolean(selectedTaskId),
    refetchInterval: selectedTaskSummary && ["running", "stopping"].includes(selectedTaskSummary.status) ? 4_000 : false,
  });

  const logsQuery = useQuery<TaskLogResponse>({
    queryKey: ["task-logs", selectedTaskId],
    queryFn: () => getTaskLogs(selectedTaskId as string),
    enabled: Boolean(selectedTaskId),
    refetchInterval: selectedTaskSummary && ["running", "stopping"].includes(selectedTaskSummary.status) ? 4_000 : false,
  });

  const presetTaskKind = normalizeTaskKind(searchParams.get("create"));
  const presetSourceType = searchParams.get("sourceType");
  const presetSourceId = searchParams.get("sourceId");
  const presetKey = presetTaskKind ? `${presetTaskKind}:${presetSourceType ?? "manual"}:${presetSourceId ?? "manual"}` : null;

  const presetQuery = useQuery<TaskPresetResponse>({
    queryKey: ["task-preset", presetTaskKind, presetSourceType, presetSourceId],
    queryFn: () => {
      if (presetSourceType === "panel" && presetSourceId) {
        return getPanelTaskPreset(presetSourceId);
      }
      if (presetSourceType === "run" && presetSourceId) {
        return getRunTaskPreset(presetSourceId);
      }
      throw new Error("Unsupported preset source");
    },
    enabled: Boolean(presetTaskKind && presetSourceType && presetSourceId),
  });

  function openManualEditor(taskKind: EditorTaskKind) {
    setEditorTaskKind(taskKind);
    setEditorSourceRef(MANUAL_SOURCE);
    setExportForm(defaultExportTaskForm());
    setWorkflowForm(defaultWorkflowTaskForm());
    setEditorOpen(true);
  }

  React.useEffect(() => {
    if (!presetTaskKind || !presetKey || appliedPresetKeyRef.current === presetKey) {
      return;
    }
    if (!presetSourceType || !presetSourceId) {
      openManualEditor(presetTaskKind);
      appliedPresetKeyRef.current = presetKey;
      router.replace(pathname, { scroll: false });
      return;
    }
    if (!presetQuery.data) {
      setEditorOpen(true);
      return;
    }
    applyPresetToEditor(presetQuery.data, {
      setEditorOpen,
      setEditorTaskKind,
      setEditorSourceRef,
      setExportForm,
      setWorkflowForm,
    });
    appliedPresetKeyRef.current = presetKey;
    router.replace(pathname, { scroll: false });
  }, [pathname, presetKey, presetQuery.data, presetSourceId, presetSourceType, presetTaskKind, router]);

  const invalidateTaskQueries = React.useCallback(
    async (taskId?: string) => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["tasks"] }),
        queryClient.invalidateQueries({ queryKey: ["task-detail"] }),
        queryClient.invalidateQueries({ queryKey: ["task-logs"] }),
      ]);
      if (taskId) {
        setSelectedTaskId(taskId);
      }
    },
    [queryClient],
  );

  const exportMutation = useMutation({
    mutationFn: postExportPanelTask,
    onSuccess: async (task) => {
      setEditorOpen(false);
      await invalidateTaskQueries(task.task_id);
    },
  });

  const workflowMutation = useMutation({
    mutationFn: postNativeWorkflowTask,
    onSuccess: async (task) => {
      setEditorOpen(false);
      await invalidateTaskQueries(task.task_id);
    },
  });

  const runQueueMutation = useMutation({
    mutationFn: postRunQueue,
    onSuccess: async () => {
      await invalidateTaskQueries(board?.running_task?.task_id ?? board?.queued_tasks[0]?.task_id);
    },
  });

  const stopCurrentMutation = useMutation({
    mutationFn: postStopCurrent,
    onSuccess: async () => {
      await invalidateTaskQueries(board?.running_task?.task_id ?? selectedTaskId ?? undefined);
    },
  });

  const reorderMutation = useMutation({
    mutationFn: postReorderTasks,
    onSuccess: async () => {
      await invalidateTaskQueries(selectedTaskId ?? undefined);
    },
  });

  const removeMutation = useMutation({
    mutationFn: postRemoveTask,
    onSuccess: async () => {
      await invalidateTaskQueries();
    },
  });

  const runningTask = board?.running_task ?? null;
  const queuedTasks = board?.queued_tasks ?? [];
  const historyTasks = React.useMemo(
    () =>
      (board?.history_tasks ?? []).filter((task) => {
        if (historyFilter === "all") return true;
        return task.status === historyFilter;
      }),
    [board?.history_tasks, historyFilter],
  );

  const handleMoveTask = (taskId: string, direction: "up" | "down") => {
    const ids = [...queuedTasks.map((task) => task.task_id)];
    const index = ids.indexOf(taskId);
    if (index === -1) return;
    const swapIndex = direction === "up" ? index - 1 : index + 1;
    if (swapIndex < 0 || swapIndex >= ids.length) return;
    [ids[index], ids[swapIndex]] = [ids[swapIndex], ids[index]];
    reorderMutation.mutate(ids);
  };

  const handleSubmitEditor = () => {
    if (editorTaskKind === "export_panel") {
      exportMutation.mutate(buildExportTaskPayload(exportForm, editorSourceRef));
      return;
    }
    workflowMutation.mutate(buildWorkflowTaskPayload(workflowForm, editorSourceRef));
  };

  const isSubmitting = exportMutation.isPending || workflowMutation.isPending;
  const taskCount = (board?.history_tasks.length ?? 0) + (board?.queued_tasks.length ?? 0) + (board?.running_task ? 1 : 0);

  return (
    <div className="space-y-6">
      <PageHeader
        title="Tasks"
        description=""
        badge={`${taskCount} tasks`}
      />

      <div className="flex flex-wrap items-center gap-3">
        <Button onClick={() => openManualEditor("export_panel")}>Add Task</Button>
        <Button
          variant="outline"
          onClick={() => runQueueMutation.mutate()}
          disabled={runQueueMutation.isPending || !queuedTasks.length || Boolean(runningTask)}
        >
          {runQueueMutation.isPending ? <Loader2 className="h-4 w-4 animate-spin" /> : <PlayCircle className="h-4 w-4" />}
          Run Queue
        </Button>
        <Button
          variant="outline"
          onClick={() => stopCurrentMutation.mutate()}
          disabled={stopCurrentMutation.isPending || !runningTask}
        >
          {stopCurrentMutation.isPending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Square className="h-4 w-4" />}
          Stop Current
        </Button>
        <Badge variant={board?.queue_state.dispatcher_status === "running" ? "success" : board?.queue_state.dispatcher_status === "stopping" ? "warning" : "neutral"}>
          Queue {board?.queue_state.dispatcher_status ?? "idle"}
        </Badge>
        {board?.queue_state.updated_at ? <Badge variant="outline">Updated {formatDateTime(board.queue_state.updated_at)}</Badge> : null}
      </div>

      <div className="grid gap-6 xl:grid-cols-[1.1fr,0.9fr]">
        <div className="space-y-6">
          <Card className="glass-card">
            <CardHeader>
              <CardTitle className="text-base">Running</CardTitle>
            </CardHeader>
            <CardContent>
              {runningTask ? (
                <TaskListItem
                  task={runningTask}
                  selected={selectedTaskId === runningTask.task_id}
                  onSelect={() => setSelectedTaskId(runningTask.task_id)}
                  actions={
                    <Button size="sm" variant="outline" onClick={() => stopCurrentMutation.mutate()} disabled={stopCurrentMutation.isPending}>
                      <Square className="h-4 w-4" />
                      Stop
                    </Button>
                  }
                />
              ) : (
                <EmptyCard message="当前没有运行中的任务。先 Add Task，再点击 Run Queue 启动串行调度。" />
              )}
            </CardContent>
          </Card>

          <Card className="glass-card">
            <CardHeader>
              <CardTitle className="text-base">Queued</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              {queuedTasks.length ? (
                queuedTasks.map((task) => (
                  <TaskListItem
                    key={task.task_id}
                    task={task}
                    selected={selectedTaskId === task.task_id}
                    onSelect={() => setSelectedTaskId(task.task_id)}
                    actions={
                      <div className="flex flex-wrap gap-2">
                        <Button
                          size="sm"
                          variant="outline"
                          onClick={(event) => {
                            event.stopPropagation();
                            handleMoveTask(task.task_id, "up");
                          }}
                          disabled={reorderMutation.isPending || task.queue_position === 1}
                        >
                          <ArrowUp className="h-4 w-4" />
                        </Button>
                        <Button
                          size="sm"
                          variant="outline"
                          onClick={(event) => {
                            event.stopPropagation();
                            handleMoveTask(task.task_id, "down");
                          }}
                          disabled={reorderMutation.isPending || task.queue_position === queuedTasks.length}
                        >
                          <ArrowDown className="h-4 w-4" />
                        </Button>
                        <Button
                          size="sm"
                          variant="outline"
                          onClick={(event) => {
                            event.stopPropagation();
                            removeMutation.mutate(task.task_id);
                          }}
                          disabled={removeMutation.isPending}
                        >
                          <Trash2 className="h-4 w-4" />
                        </Button>
                      </div>
                    }
                  />
                ))
              ) : (
                <EmptyCard message="队列当前为空。Add Task 会先把任务放入 queued，等待你统一启动。" />
              )}
            </CardContent>
          </Card>

          <Card className="glass-card">
            <CardHeader className="gap-3">
              <div className="flex flex-wrap items-center justify-between gap-3">
                <CardTitle className="text-base">History</CardTitle>
                <div className="flex flex-wrap gap-2">
                  {(["all", "succeeded", "failed", "cancelled"] as const).map((filter) => (
                    <Button
                      key={filter}
                      size="sm"
                      variant={historyFilter === filter ? "default" : "outline"}
                      onClick={() => setHistoryFilter(filter)}
                    >
                      {filter}
                    </Button>
                  ))}
                </div>
              </div>
            </CardHeader>
            <CardContent className="space-y-3 max-h-[400px] overflow-y-auto">
              {historyTasks.length ? (
                historyTasks.map((task) => (
                  <TaskListItem
                    key={task.task_id}
                    task={task}
                    selected={selectedTaskId === task.task_id}
                    onSelect={() => setSelectedTaskId(task.task_id)}
                  />
                ))
              ) : (
                <EmptyCard message="历史任务会在这里按状态归档，支持随时回看配置、日志和结果摘要。" />
              )}
            </CardContent>
          </Card>
        </div>

        <TaskDetailPanel
          task={taskDetailQuery.data}
          logs={logsQuery.data}
          isLoading={taskDetailQuery.isLoading}
        />
      </div>

      <Dialog open={editorOpen} onOpenChange={setEditorOpen}>
        <DialogContent className="overflow-y-auto">
          <div className="space-y-6">
            <DialogHeader className="space-y-3">
              <div className="flex flex-wrap items-center gap-2">
                <DialogTitle>Add Task</DialogTitle>
                <Badge variant={editorSourceRef.kind === "manual" ? "neutral" : "info"}>
                  Source: {editorSourceRef.label ?? editorSourceRef.kind}
                </Badge>
              </div>
              <DialogDescription>
                手动创建任务时可以切换类型；从 run 或 panel 入口带过来的预填任务会锁定到对应类型。
              </DialogDescription>
            </DialogHeader>

            {editorSourceRef.kind === "manual" ? (
              <div className="flex flex-wrap gap-2">
                <Button
                  variant={editorTaskKind === "export_panel" ? "default" : "outline"}
                  onClick={() => setEditorTaskKind("export_panel")}
                >
                  Export Panel
                </Button>
                <Button
                  variant={editorTaskKind === "run_native_workflow" ? "default" : "outline"}
                  onClick={() => setEditorTaskKind("run_native_workflow")}
                >
                  Run Native Workflow
                </Button>
              </div>
            ) : null}

            {presetQuery.isLoading && editorSourceRef.kind !== "manual" ? (
              <Badge variant="info">正在加载来源预填参数…</Badge>
            ) : null}

            {editorTaskKind === "export_panel" ? (
              <ExportTaskEditor
                featureGroupOptions={board?.feature_group_options ?? []}
                form={exportForm}
                onChange={setExportForm}
              />
            ) : (
              <WorkflowTaskEditor form={workflowForm} onChange={setWorkflowForm} panels={panelsQuery.data ?? []} />
            )}

            <DialogFooter>
              <Button onClick={handleSubmitEditor} disabled={isSubmitting}>
                {isSubmitting ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
                提交任务
              </Button>
              <Button variant="outline" onClick={() => setEditorOpen(false)}>
                取消
              </Button>
            </DialogFooter>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  );
}

function TaskDetailPanel({
  task,
  logs,
  isLoading,
}: {
  task?: ResearchTaskDetail;
  logs?: TaskLogResponse;
  isLoading: boolean;
}) {
  if (isLoading) {
    return (
      <Card className="glass-card min-w-0">
        <CardContent className="p-6 text-sm text-muted-foreground">正在加载任务详情…</CardContent>
      </Card>
    );
  }
  if (!task) {
    return (
      <Card className="glass-card min-w-0">
        <CardContent className="p-6 text-sm text-muted-foreground">选择一个任务后，这里会显示状态、配置、控制台输出和结果摘要。</CardContent>
      </Card>
    );
  }

  return (
    <Card className="glass-card min-w-0">
      <CardHeader className="gap-3">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="space-y-1">
            <CardTitle className="text-base">{task.display_name || task.task_id}</CardTitle>
            <p className="text-sm text-muted-foreground">{task.task_id}</p>
            {task.description ? <p className="text-sm text-muted-foreground">{task.description}</p> : null}
          </div>
          <div className="flex flex-wrap gap-2">
            <StatusBadge status={task.status} />
            <Badge variant="outline">{task.task_kind}</Badge>
          </div>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <Tabs defaultValue="overview" className="space-y-4">
          <TabsList>
            <TabsTrigger value="overview">Overview</TabsTrigger>
            <TabsTrigger value="config">Config</TabsTrigger>
            <TabsTrigger value="console">Console</TabsTrigger>
            <TabsTrigger value="result">Result</TabsTrigger>
          </TabsList>

          <TabsContent value="overview" className="space-y-4">
            <div className="grid gap-3 md:grid-cols-2">
              <KeyValueCard label="Task Kind" value={task.task_kind} />
              <KeyValueCard label="Source" value={task.source_ref?.label ?? task.source_ref?.kind ?? "—"} />
              <KeyValueCard label="Description" value={task.description ?? "—"} />
              <KeyValueCard label="Created" value={formatDateTime(task.created_at)} />
              <KeyValueCard label="Started" value={formatDateTime(task.started_at)} />
              <KeyValueCard label="Finished" value={formatDateTime(task.finished_at)} />
              <KeyValueCard label="Output" value={formatPathName(task.output_dir)} />
            </div>
            <Card className="border border-border/60 bg-surface-2/40">
              <CardHeader>
                <CardTitle className="text-sm">Timeline</CardTitle>
              </CardHeader>
              <CardContent className="space-y-2">
                {task.timeline.length ? (
                  task.timeline.map((event, index) => (
                    <div key={`${event.label}-${index}`} className="flex items-center justify-between gap-3 rounded-lg border border-border/50 px-3 py-2 text-sm">
                      <span>{String(event.label ?? "Event")}</span>
                      <span className="text-muted-foreground">{formatDateTime(event.at)}</span>
                    </div>
                  ))
                ) : (
                  <div className="text-sm text-muted-foreground">暂无时间线事件。</div>
                )}
              </CardContent>
            </Card>
            <Card className="border border-border/60 bg-surface-2/40">
              <CardHeader>
                <CardTitle className="text-sm">Command</CardTitle>
              </CardHeader>
              <CardContent>
                <pre className="w-full overflow-x-auto whitespace-pre-wrap break-all rounded-lg bg-surface-1/70 p-4 text-xs leading-6">{task.command.join(" ") || "—"}</pre>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="config">
            <pre className="max-h-[36rem] overflow-auto rounded-lg bg-surface-2/70 p-4 text-xs leading-6">
              {JSON.stringify(task.config_payload, null, 2)}
            </pre>
          </TabsContent>

          <TabsContent value="console" className="space-y-4">
            <div className="flex flex-wrap gap-2">
              <Badge variant="neutral">Logs updated {formatDateTime(logs?.updated_at)}</Badge>
            </div>
            <Tabs defaultValue="stdout" className="space-y-3">
              <TabsList>
                <TabsTrigger value="stdout">stdout</TabsTrigger>
                <TabsTrigger value="stderr">stderr</TabsTrigger>
              </TabsList>
              <TabsContent value="stdout">
                <pre className="max-h-[26rem] overflow-auto rounded-lg bg-surface-2/70 p-4 text-xs leading-6">{logs?.stdout || "暂无输出"}</pre>
              </TabsContent>
              <TabsContent value="stderr">
                <pre className="max-h-[26rem] overflow-auto rounded-lg bg-surface-2/70 p-4 text-xs leading-6">{logs?.stderr || "暂无错误输出"}</pre>
              </TabsContent>
            </Tabs>
          </TabsContent>

          <TabsContent value="result">
            <pre className="max-h-[36rem] overflow-auto rounded-lg bg-surface-2/70 p-4 text-xs leading-6">
              {JSON.stringify(task.result, null, 2)}
            </pre>
          </TabsContent>
        </Tabs>
      </CardContent>
    </Card>
  );
}

function ExportTaskEditor({
  form,
  onChange,
  featureGroupOptions,
}: {
  form: ExportTaskFormState;
  onChange: React.Dispatch<React.SetStateAction<ExportTaskFormState>>;
  featureGroupOptions: string[];
}) {
  const toggleGroup = (group: string) => {
    onChange((current) => ({
      ...current,
      feature_groups: current.feature_groups.includes(group)
        ? current.feature_groups.filter((item) => item !== group)
        : [...current.feature_groups, group],
    }));
  };

  return (
    <div className="space-y-4">
      <FormField label="Display Name">
        <Input value={form.display_name} onChange={(event) => onChange((current) => ({ ...current, display_name: event.target.value }))} />
      </FormField>
      <FormField label="Task Description">
        <textarea
          className="min-h-[88px] w-full rounded-lg border border-input/85 bg-surface-1/80 px-3 py-2 text-sm"
          value={form.description}
          onChange={(event) => onChange((current) => ({ ...current, description: event.target.value }))}
          placeholder="补充这个导出任务的目的、口径或备注"
        />
      </FormField>
      <div className="grid gap-4 md:grid-cols-2">
        <FormField label="Output">
          <Input value={form.output} onChange={(event) => onChange((current) => ({ ...current, output: event.target.value }))} />
        </FormField>
        <FormField label="Universe Profile">
          <Input value={form.universe_profile} onChange={(event) => onChange((current) => ({ ...current, universe_profile: event.target.value }))} />
        </FormField>
        <div className="space-y-2 md:col-span-2">
          <div className="text-sm font-medium">Universe Mode</div>
          <div className="flex flex-wrap gap-2">
            <Button
              size="sm"
              variant={form.universe_mode === "historical_membership" ? "default" : "outline"}
              onClick={() => onChange((current) => ({ ...current, universe_mode: "historical_membership" }))}
            >
              Historical Membership
            </Button>
            <Button
              size="sm"
              variant={form.universe_mode === "fixed_universe" ? "default" : "outline"}
              onClick={() => onChange((current) => ({ ...current, universe_mode: "fixed_universe" }))}
            >
              Fixed Universe
            </Button>
          </div>
          <div className="text-sm text-muted-foreground">
            {form.universe_mode === "fixed_universe"
              ? "固定池模式会用所选指数在结束日之前最近一次可用成分快照，拉长历史样本，但不再代表严格历史成分。"
              : "历史成分模式会按每周真实指数成分过滤样本，更严格，但受 index_weight 历史长度限制。"}
          </div>
        </div>
        <FormField label="Start Date">
          <Input value={form.start_date} onChange={(event) => onChange((current) => ({ ...current, start_date: event.target.value }))} placeholder="YYYY-MM-DD" />
        </FormField>
        <FormField label="End Date">
          <Input value={form.end_date} onChange={(event) => onChange((current) => ({ ...current, end_date: event.target.value }))} placeholder="YYYY-MM-DD" />
        </FormField>
        <FormField label="Batch Size">
          <Input value={form.batch_size} onChange={(event) => onChange((current) => ({ ...current, batch_size: event.target.value }))} />
        </FormField>
        <FormField label="Enrichment Scope">
          <Input value={form.enrichment_scope} onChange={(event) => onChange((current) => ({ ...current, enrichment_scope: event.target.value }))} placeholder="research_full" />
        </FormField>
      </div>

      <Card className="border border-border/60 bg-surface-2/40">
        <CardHeader>
          <CardTitle className="text-sm">Feature Selection</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex flex-wrap gap-2">
            <Button
              size="sm"
              variant={form.feature_mode === "all" ? "default" : "outline"}
              onClick={() => onChange((current) => ({ ...current, feature_mode: "all", feature_groups: [], included_features: "", excluded_features: "" }))}
            >
              All Features
            </Button>
            <Button
              size="sm"
              variant={form.feature_mode === "custom" ? "default" : "outline"}
              onClick={() => onChange((current) => ({ ...current, feature_mode: "custom" }))}
            >
              Custom
            </Button>
          </div>

          {form.feature_mode === "custom" ? (
            <div className="space-y-4">
              <div className="space-y-2">
                <div className="text-sm font-medium">Feature Groups</div>
                <div className="flex flex-wrap gap-2">
                  {featureGroupOptions.map((group) => (
                    <Button
                      key={group}
                      size="sm"
                      variant={form.feature_groups.includes(group) ? "default" : "outline"}
                      onClick={() => toggleGroup(group)}
                    >
                      {group}
                    </Button>
                  ))}
                </div>
              </div>
              <FormField label="Included Features">
                <textarea
                  className="min-h-[88px] w-full rounded-lg border border-input/85 bg-surface-1/80 px-3 py-2 text-sm"
                  value={form.included_features}
                  onChange={(event) => onChange((current) => ({ ...current, included_features: event.target.value }))}
                  placeholder="ma20, rsi, macro_*"
                />
              </FormField>
              <FormField label="Excluded Features">
                <textarea
                  className="min-h-[88px] w-full rounded-lg border border-input/85 bg-surface-1/80 px-3 py-2 text-sm"
                  value={form.excluded_features}
                  onChange={(event) => onChange((current) => ({ ...current, excluded_features: event.target.value }))}
                  placeholder="volume, amount"
                />
              </FormField>
            </div>
          ) : (
            <div className="text-sm text-muted-foreground">默认导出全部可用 research features；基础列始终保留。</div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function WorkflowTaskEditor({
  form,
  onChange,
  panels,
}: {
  form: WorkflowTaskFormState;
  onChange: React.Dispatch<React.SetStateAction<WorkflowTaskFormState>>;
  panels: PanelSummary[];
}) {
  const panelOptions = React.useMemo(() => buildWorkflowPanelOptions(panels, form.panel_path), [form.panel_path, panels]);
  const recipeOptions = React.useMemo(() => buildWorkflowRecipeOptions(form.recipe_names), [form.recipe_names]);
  const selectedRecipeNames = React.useMemo(() => parseListInput(form.recipe_names), [form.recipe_names]);
  const benchmarkModeSelectValue = getBenchmarkModeSelectValue(form.benchmark_mode);
  const describe = describeWorkflowConfigKey;

  const updateDisplayName = (displayName: string) => {
    onChange((current) => syncWorkflowDerivedFields({ ...current, display_name: displayName }));
  };

  const updatePanelPath = (panelPath: string) => {
    onChange((current) => syncWorkflowDerivedFields({ ...current, panel_path: panelPath }));
  };

  const updateUniverseProfile = (universeProfile: string) => {
    onChange((current) => syncWorkflowDerivedFields({ ...current, universe_profile: universeProfile }));
  };

  const toggleRecipeName = (recipeName: string) => {
    onChange((current) => {
      const selected = parseListInput(current.recipe_names);
      const nextSelected = selected.includes(recipeName)
        ? selected.filter((item) => item !== recipeName)
        : recipeOptions.filter((item) => item === recipeName || selected.includes(item));
      return {
        ...current,
        recipe_names: (nextSelected.length ? nextSelected : selected).join(", "),
      };
    });
  };

  const updateBenchmarkModeSelection = (value: string) => {
    onChange((current) => ({
      ...current,
      benchmark_mode: value === "custom_index" ? (isBuiltinBenchmarkMode(current.benchmark_mode) ? "000001.SH" : current.benchmark_mode) : value,
    }));
  };

  return (
    <TooltipProvider delayDuration={120}>
      <div className="space-y-4">
        <FormField label="Display Name" description={describe("display_name")}>
          <Input value={form.display_name} onChange={(event) => updateDisplayName(event.target.value)} />
        </FormField>
        <FormField label="Task Description" description="这段说明会随任务一起保存，并展示在任务、run 和 panel 页面中。">
          <textarea
            className="min-h-[88px] w-full rounded-lg border border-input/85 bg-surface-1/80 px-3 py-2 text-sm"
            value={form.description}
            onChange={(event) => onChange((current) => ({ ...current, description: event.target.value }))}
            placeholder="补充这次 workflow 的研究目标、假设或备注"
          />
        </FormField>
        <ReadonlyPathField
          label="Output Dir"
          value={form.output_dir}
          description={describe("output_dir")}
          hint={`固定输出根目录：${form.output_root_dir}；根据 Display Name 自动生成。`}
        />
        <ReadonlyPathField
          label="Execution Panel Path"
          value={form.execution_panel_path}
          description={describe("execution_panel_path")}
          hint="Execution Panel Dir 与 Output Dir 保持一致，路径自动生成。"
        />
        <div className="grid gap-4 md:grid-cols-2">
          <FormField label="Panel Path" description={describe("panel_path")}>
            <select
              className="flex h-10 w-full rounded-lg border border-input/85 bg-surface-1/80 px-3 py-2 text-sm"
              value={form.panel_path}
              onChange={(event) => updatePanelPath(event.target.value)}
            >
              {panelOptions.map((panel) => (
                <option key={panel.path} value={panel.path}>
                  {panel.label}
                </option>
              ))}
            </select>
          </FormField>
          <div className="md:col-span-2">
            <FormField label="Recipe Names" description={describe("recipe_names")}>
              <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-3">
                {recipeOptions.map((recipeName) => {
                  const checked = selectedRecipeNames.includes(recipeName);
                  return (
                    <label
                      key={recipeName}
                      className={cn(
                        "flex items-center gap-2 rounded-lg border px-3 py-2 text-sm transition-colors",
                        checked
                          ? "border-accent/60 bg-accent/10 text-foreground"
                          : "border-border/60 bg-surface-1/40 text-muted-foreground",
                      )}
                    >
                      <input
                        type="checkbox"
                        className="h-4 w-4 rounded border-input"
                        checked={checked}
                        onChange={() => toggleRecipeName(recipeName)}
                      />
                      <span>{recipeName}</span>
                    </label>
                  );
                })}
              </div>
            </FormField>
          </div>
          <FormField label="Feature Spec Path" description={describe("feature_spec_path")}>
            <Input value={form.feature_spec_path} onChange={(event) => onChange((current) => ({ ...current, feature_spec_path: event.target.value }))} placeholder="可选" />
          </FormField>
          <FormSelect
            label="Universe Profile"
            value={form.universe_profile}
            description={describe("universe_profile")}
            options={UNIVERSE_PROFILE_OPTIONS}
            onChange={updateUniverseProfile}
          />
          <FormField label="Benchmark Mode" description={describe("benchmark_mode")}>
            <div className="space-y-2">
              <select
                className="flex h-10 w-full rounded-lg border border-input/85 bg-surface-1/80 px-3 py-2 text-sm"
                value={benchmarkModeSelectValue}
                onChange={(event) => updateBenchmarkModeSelection(event.target.value)}
              >
                {BUILTIN_BENCHMARK_MODE_OPTIONS.map((option) => (
                  <option key={option} value={option}>
                    {option}
                  </option>
                ))}
                <option value="custom_index">custom index code</option>
              </select>
              {benchmarkModeSelectValue === "custom_index" ? (
                <Input
                  value={form.benchmark_mode}
                  onChange={(event) => onChange((current) => ({ ...current, benchmark_mode: event.target.value.toUpperCase() }))}
                  placeholder="000001.SH"
                />
              ) : null}
            </div>
          </FormField>
          <FormSelect
            label="Run Export"
            value={form.run_export}
            description={describe("run_export")}
            options={RUN_EXPORT_OPTIONS}
            onChange={(value) => onChange((current) => ({ ...current, run_export: value }))}
          />
          <FormSelect
            label="Signal Objective"
            value={form.signal_objective}
            description={describe("signal_objective")}
            options={SIGNAL_OBJECTIVE_OPTIONS}
            onChange={(value) => onChange((current) => ({ ...current, signal_objective: value }))}
          />
          <FormSelect
            label="Label Recipe"
            value={form.label_recipe}
            description={describe("label_recipe")}
            options={LABEL_RECIPE_OPTIONS}
            onChange={(value) => onChange((current) => ({ ...current, label_recipe: value }))}
          />
          <FormSelect
            label="Reproducibility"
            value={form.reproducibility_mode}
            description={describe("reproducibility_mode")}
            options={REPRODUCIBILITY_OPTIONS}
            onChange={(value) => onChange((current) => ({ ...current, reproducibility_mode: value }))}
          />
          <FormSelect
            label="Universe Exit Policy"
            value={form.universe_exit_policy}
            description={describe("universe_exit_policy")}
            options={UNIVERSE_EXIT_POLICY_OPTIONS}
            onChange={(value) => onChange((current) => ({ ...current, universe_exit_policy: value }))}
          />
        </div>

        <Card className="border border-border/60 bg-surface-2/40">
          <CardHeader>
            <CardTitle className="text-sm">Time Window</CardTitle>
          </CardHeader>
          <CardContent className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <FormField label="Start Date" description={describe("start_date")}>
              <Input value={form.start_date} onChange={(event) => onChange((current) => ({ ...current, start_date: event.target.value }))} placeholder="YYYY-MM-DD" />
            </FormField>
            <FormField label="End Date" description={describe("end_date")}>
              <Input value={form.end_date} onChange={(event) => onChange((current) => ({ ...current, end_date: event.target.value }))} placeholder="YYYY-MM-DD" />
            </FormField>
            <FormField label="Walk-forward Start" description={describe("walk_forward_start_date")}>
              <Input
                value={form.walk_forward_start_date}
                onChange={(event) => onChange((current) => ({ ...current, walk_forward_start_date: event.target.value }))}
                placeholder="YYYY-MM-DD"
              />
            </FormField>
            <FormField label="Walk-forward End" description={describe("walk_forward_end_date")}>
              <Input
                value={form.walk_forward_end_date}
                onChange={(event) => onChange((current) => ({ ...current, walk_forward_end_date: event.target.value }))}
                placeholder="YYYY-MM-DD"
              />
            </FormField>
          </CardContent>
        </Card>

        <Card className="border border-border/60 bg-surface-2/40">
          <CardHeader>
            <CardTitle className="text-sm">Rolling / Portfolio</CardTitle>
          </CardHeader>
          <CardContent className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <FormField label="Batch Size" description={describe("batch_size")}><Input value={form.batch_size} onChange={(event) => onChange((current) => ({ ...current, batch_size: event.target.value }))} /></FormField>
            <FormField label="TopK" description={describe("topk")}><Input value={form.topk} onChange={(event) => onChange((current) => ({ ...current, topk: event.target.value }))} /></FormField>
            <FormField label="Train Weeks" description={describe("train_weeks")}><Input value={form.train_weeks} onChange={(event) => onChange((current) => ({ ...current, train_weeks: event.target.value }))} /></FormField>
            <FormField label="Valid Weeks" description={describe("valid_weeks")}><Input value={form.valid_weeks} onChange={(event) => onChange((current) => ({ ...current, valid_weeks: event.target.value }))} /></FormField>
            <FormField label="Eval Count" description={describe("eval_count")}><Input value={form.eval_count} onChange={(event) => onChange((current) => ({ ...current, eval_count: event.target.value }))} /></FormField>
            <FormField label="Rolling Recent Weeks" description={describe("rolling_recent_weeks")}><Input value={form.rolling_recent_weeks} onChange={(event) => onChange((current) => ({ ...current, rolling_recent_weeks: event.target.value }))} /></FormField>
            <FormField label="Step Weeks" description={describe("step_weeks")}><Input value={form.step_weeks} onChange={(event) => onChange((current) => ({ ...current, step_weeks: event.target.value }))} /></FormField>
            <FormField label="Rebalance Interval" description={describe("rebalance_interval_weeks")}><Input value={form.rebalance_interval_weeks} onChange={(event) => onChange((current) => ({ ...current, rebalance_interval_weeks: event.target.value }))} /></FormField>
            <FormField label="Hold Buffer Rank" description={describe("hold_buffer_rank")}><Input value={form.hold_buffer_rank} onChange={(event) => onChange((current) => ({ ...current, hold_buffer_rank: event.target.value }))} placeholder="可选" /></FormField>
            <FormField label="Min Liquidity Filter" description={describe("min_liquidity_filter")}><Input value={form.min_liquidity_filter} onChange={(event) => onChange((current) => ({ ...current, min_liquidity_filter: event.target.value }))} /></FormField>
            <FormField label="Min Score Spread" description={describe("min_score_spread")}><Input value={form.min_score_spread} onChange={(event) => onChange((current) => ({ ...current, min_score_spread: event.target.value }))} /></FormField>
            <FormField label="Industry Max Weight" description={describe("industry_max_weight")}><Input value={form.industry_max_weight} onChange={(event) => onChange((current) => ({ ...current, industry_max_weight: event.target.value }))} placeholder="可选" /></FormField>
          </CardContent>
        </Card>

        <Card className="border border-border/60 bg-surface-2/40">
          <CardHeader>
            <CardTitle className="text-sm">Walk-forward / Execution</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
              <BooleanSwitchField label="Walk-forward Enabled" description={describe("walk_forward_enabled")} value={form.walk_forward_enabled} onChange={(value) => onChange((current) => ({ ...current, walk_forward_enabled: value }))} />
              <BooleanSwitchField label="Diagnostics Enabled" description={describe("diagnostics_enabled")} value={form.diagnostics_enabled} onChange={(value) => onChange((current) => ({ ...current, diagnostics_enabled: value }))} />
              <BooleanSwitchField label="Validation Comparison" description={describe("run_validation_comparison")} value={form.run_validation_comparison} onChange={(value) => onChange((current) => ({ ...current, run_validation_comparison: value }))} />
              <BooleanSwitchField label="Validation Only Tradable" description={describe("validation_only_tradable")} value={form.validation_only_tradable} onChange={(value) => onChange((current) => ({ ...current, validation_only_tradable: value }))} />
              <BooleanSwitchField label="Native Only Tradable" description={describe("native_only_tradable")} value={form.native_only_tradable} onChange={(value) => onChange((current) => ({ ...current, native_only_tradable: value }))} />
              <BooleanSwitchField label="Publish Model" description={describe("publish_model")} value={form.publish_model} onChange={(value) => onChange((current) => ({ ...current, publish_model: value }))} />
            </div>
            <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
              <FormField label="WF Train Weeks" description={describe("walk_forward_train_weeks")}><Input value={form.walk_forward_train_weeks} onChange={(event) => onChange((current) => ({ ...current, walk_forward_train_weeks: event.target.value }))} /></FormField>
              <FormField label="WF Valid Weeks" description={describe("walk_forward_valid_weeks")}><Input value={form.walk_forward_valid_weeks} onChange={(event) => onChange((current) => ({ ...current, walk_forward_valid_weeks: event.target.value }))} /></FormField>
              <FormField label="WF Step Weeks" description={describe("walk_forward_step_weeks")}><Input value={form.walk_forward_step_weeks} onChange={(event) => onChange((current) => ({ ...current, walk_forward_step_weeks: event.target.value }))} /></FormField>
              <FormField label="WF Eval Count" description={describe("walk_forward_eval_count")}><Input value={form.walk_forward_eval_count} onChange={(event) => onChange((current) => ({ ...current, walk_forward_eval_count: event.target.value }))} /></FormField>
              <FormField label="Validation Lag Steps" description={describe("validation_execution_lag_steps")}><Input value={form.validation_execution_lag_steps} onChange={(event) => onChange((current) => ({ ...current, validation_execution_lag_steps: event.target.value }))} /></FormField>
              <FormField label="Validation Risk Degree" description={describe("validation_risk_degree")}><Input value={form.validation_risk_degree} onChange={(event) => onChange((current) => ({ ...current, validation_risk_degree: event.target.value }))} /></FormField>
              <FormField label="Native Risk Degree" description={describe("native_risk_degree")}><Input value={form.native_risk_degree} onChange={(event) => onChange((current) => ({ ...current, native_risk_degree: event.target.value }))} /></FormField>
              <FormField label="Account" description={describe("account")}><Input value={form.account} onChange={(event) => onChange((current) => ({ ...current, account: event.target.value }))} /></FormField>
            </div>
          </CardContent>
        </Card>

        <Card className="border border-border/60 bg-surface-2/40">
          <CardHeader>
            <CardTitle className="text-sm">Runtime</CardTitle>
          </CardHeader>
          <CardContent className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <FormField label="Seed" description={describe("seed")}><Input value={form.seed} onChange={(event) => onChange((current) => ({ ...current, seed: event.target.value }))} /></FormField>
            <FormField label="Recipe Parallel Workers" description={describe("recipe_parallel_workers")}><Input value={form.recipe_parallel_workers} onChange={(event) => onChange((current) => ({ ...current, recipe_parallel_workers: event.target.value }))} /></FormField>
            <FormField label="Model Num Threads" description={describe("model_num_threads")}><Input value={form.model_num_threads} onChange={(event) => onChange((current) => ({ ...current, model_num_threads: event.target.value }))} placeholder="自动" /></FormField>
          </CardContent>
        </Card>

        <Card className="border border-border/60 bg-surface-2/40">
          <CardHeader>
            <CardTitle className="text-sm">Advanced Overrides (JSON)</CardTitle>
          </CardHeader>
          <CardContent>
            <FormField label="Advanced Overrides" description={describe("advanced_overrides")}>
              <textarea
                className="min-h-[180px] w-full rounded-lg border border-input/85 bg-surface-1/80 px-3 py-2 text-sm"
                value={form.advanced_overrides}
                onChange={(event) => onChange((current) => ({ ...current, advanced_overrides: event.target.value }))}
                placeholder='{"feature_groups":["technical_core"]}'
              />
            </FormField>
          </CardContent>
        </Card>
      </div>
    </TooltipProvider>
  );
}

function TaskListItem({
  task,
  selected,
  onSelect,
  actions,
}: {
  task: ResearchTaskSummary;
  selected: boolean;
  onSelect: () => void;
  actions?: React.ReactNode;
}) {
  return (
    <div
      role="button"
      tabIndex={0}
      onClick={onSelect}
      onKeyDown={(event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          onSelect();
        }
      }}
      className={cn(
        "w-full rounded-xl border px-4 py-4 text-left transition-colors",
        selected ? "border-primary/50 bg-primary/10" : "border-border/60 bg-surface-1/50 hover:bg-accent/35",
      )}
    >
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="space-y-2">
          <div className="flex flex-wrap items-center gap-2">
            <span className="font-medium">{task.display_name || task.task_id}</span>
            <StatusBadge status={task.status} />
            {task.queue_position ? <Badge variant="outline">#{task.queue_position}</Badge> : null}
          </div>
          {task.description ? <div className="text-sm text-muted-foreground">{task.description}</div> : null}
          <div className="text-sm text-muted-foreground">{task.task_id}</div>
          <div className="flex flex-wrap gap-2 text-xs text-muted-foreground">
            <span>{task.task_kind}</span>
            <span>{formatDateTime(task.started_at || task.created_at)}</span>
            <span>{task.message || "—"}</span>
          </div>
          {task.output_dir ? <div className="text-xs text-muted-foreground">Output: {formatPathName(task.output_dir)}</div> : null}
        </div>
        {actions ? <div onClick={(event) => event.stopPropagation()}>{actions}</div> : null}
      </div>
    </div>
  );
}

function StatusBadge({ status }: { status: ResearchTaskSummary["status"] }) {
  const variant =
    status === "succeeded"
      ? "success"
      : status === "failed"
        ? "destructive"
        : status === "cancelled" || status === "stopping"
          ? "warning"
          : status === "running"
            ? "info"
            : "neutral";
  return <Badge variant={variant}>{status}</Badge>;
}

function KeyValueCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-xl border border-border/60 bg-surface-2/40 px-4 py-3">
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className="mt-1 text-sm font-medium">{value || "—"}</div>
    </div>
  );
}

function FieldLabel({ label, description }: { label: string; description?: string }) {
  return (
    <div className="flex items-center gap-1.5">
      <span className="text-sm font-medium">{label}</span>
      {description ? (
        <Tooltip>
          <TooltipTrigger asChild>
            <button
              type="button"
              className="inline-flex h-4 w-4 items-center justify-center rounded-full text-muted-foreground transition-colors hover:text-foreground"
              aria-label={`${label} description`}
            >
              <CircleHelp className="h-3.5 w-3.5" />
            </button>
          </TooltipTrigger>
          <TooltipContent className="max-w-sm text-pretty leading-5">{description}</TooltipContent>
        </Tooltip>
      ) : null}
    </div>
  );
}

function FormField({ label, description, children }: { label: string; description?: string; children: React.ReactNode }) {
  return (
    <label className="space-y-2">
      <FieldLabel label={label} description={description} />
      {children}
    </label>
  );
}

function ReadonlyPathField({
  label,
  value,
  description,
  hint,
}: {
  label: string;
  value: string;
  description?: string;
  hint?: string;
}) {
  return (
    <div className="space-y-2">
      <FieldLabel label={label} description={description} />
      <div className="rounded-lg border border-dashed border-border/70 bg-surface-2/50 px-3 py-2 text-sm text-foreground/90">
        {value || "—"}
      </div>
      {hint ? <div className="text-xs text-muted-foreground">{hint}</div> : null}
    </div>
  );
}

function FormSelect({
  label,
  value,
  description,
  options,
  onChange,
}: {
  label: string;
  value: string;
  description?: string;
  options: readonly string[];
  onChange: (value: string) => void;
}) {
  return (
    <label className="space-y-2">
      <FieldLabel label={label} description={description} />
      <select
        className="flex h-10 w-full rounded-lg border border-input/85 bg-surface-1/80 px-3 py-2 text-sm"
        value={value}
        onChange={(event) => onChange(event.target.value)}
      >
        {options.map((option) => (
          <option key={option} value={option}>
            {option}
          </option>
        ))}
      </select>
    </label>
  );
}

function BooleanSwitchField({
  label,
  value,
  description,
  onChange,
}: {
  label: string;
  value: boolean;
  description?: string;
  onChange: (value: boolean) => void;
}) {
  return (
    <div className="flex items-center justify-between gap-3 rounded-lg border border-border/60 bg-surface-1/40 px-3 py-2">
      <div className="space-y-1">
        <FieldLabel label={label} description={description} />
        <div className="text-xs text-muted-foreground">{value ? "Enabled" : "Disabled"}</div>
      </div>
      <Switch checked={value} onCheckedChange={onChange} />
    </div>
  );
}

function EmptyCard({ message }: { message: string }) {
  return <div className="rounded-xl border border-dashed border-border/70 p-4 text-sm text-muted-foreground">{message}</div>;
}

function normalizeTaskKind(value: string | null): EditorTaskKind | null {
  if (value === "export_panel" || value === "run_native_workflow") {
    return value;
  }
  return null;
}

function defaultExportTaskForm(): ExportTaskFormState {
  return {
    display_name: "Export Panel",
    description: "",
    requested_by: "webapp",
    output: "artifacts/panels/weekly_features.parquet",
    start_date: "",
    end_date: "",
    universe_profile: "",
    universe_mode: "historical_membership",
    batch_size: "300",
    enrichment_scope: "research_full",
    feature_mode: "all",
    feature_groups: [],
    included_features: "",
    excluded_features: "",
  };
}

function defaultWorkflowTaskForm(): WorkflowTaskFormState {
  const defaultUniverseProfile = "csi300";
  return syncWorkflowDerivedFields({
    display_name: "Run Native Workflow",
    description: "",
    requested_by: "webapp",
    recipe_names: "baseline",
    panel_path: WORKFLOW_DEFAULT_PANEL_PATH,
    execution_panel_path: "",
    execution_panel_dir: "",
    output_dir: "",
    output_root_dir: WORKFLOW_OUTPUT_ROOT_DIR,
    feature_spec_path: "",
    universe_profile: defaultUniverseProfile,
    benchmark_mode: "auto",
    signal_objective: "huber_regression",
    label_recipe: "blended_excess_4w_8w",
    run_export: "auto_if_missing",
    reproducibility_mode: "balanced",
    universe_exit_policy: "retain_quotes_for_existing_positions",
    start_date: "2016-01-01",
    end_date: "",
    batch_size: "200",
    topk: "10",
    train_weeks: "260",
    valid_weeks: "52",
    eval_count: "52",
    rolling_recent_weeks: "52",
    step_weeks: "1",
    rebalance_interval_weeks: "1",
    hold_buffer_rank: "",
    min_liquidity_filter: "0",
    min_score_spread: "0",
    industry_max_weight: "",
    validation_execution_lag_steps: "1",
    validation_risk_degree: "1",
    native_risk_degree: "0.95",
    account: "1000000",
    seed: "42",
    recipe_parallel_workers: "1",
    model_num_threads: "",
    walk_forward_enabled: true,
    walk_forward_start_date: "2016-01-01",
    walk_forward_end_date: "",
    walk_forward_train_weeks: "260",
    walk_forward_valid_weeks: "52",
    walk_forward_step_weeks: "1",
    walk_forward_eval_count: "0",
    diagnostics_enabled: true,
    run_validation_comparison: true,
    validation_only_tradable: false,
    native_only_tradable: true,
    publish_model: false,
    advanced_overrides: "{}",
  });
}

function parseListInput(value: string) {
  return value
    .split(/[\n,]/g)
    .map((item) => item.trim())
    .filter(Boolean);
}

function buildExportTaskPayload(form: ExportTaskFormState, sourceRef: TaskSourceRef) {
  const featureGroups = form.feature_mode === "custom" ? form.feature_groups : [];
  const includedFeatures = form.feature_mode === "custom" ? parseListInput(form.included_features) : [];
  const excludedFeatures = form.feature_mode === "custom" ? parseListInput(form.excluded_features) : [];
  return {
    display_name: form.display_name,
    description: form.description || null,
    requested_by: form.requested_by,
    source_ref: sourceRef,
    output: form.output,
    start_date: form.start_date || null,
    end_date: form.end_date || null,
    universe_profile: form.universe_profile || null,
    universe_mode: form.universe_mode,
    batch_size: Number(form.batch_size || 300),
    enrichment_scope: form.enrichment_scope || null,
    feature_groups: featureGroups.length ? featureGroups : null,
    included_features: includedFeatures.length ? includedFeatures : null,
    excluded_features: excludedFeatures.length ? excludedFeatures : null,
  };
}

function buildWorkflowTaskPayload(form: WorkflowTaskFormState, sourceRef: TaskSourceRef) {
  let advancedOverrides: Record<string, unknown> = {};
  try {
    advancedOverrides = JSON.parse(form.advanced_overrides || "{}");
  } catch {
    advancedOverrides = {};
  }
  const configPayload: Record<string, unknown> = {
    panel_path: form.panel_path,
    output_dir: form.output_dir,
    universe_profile: form.universe_profile,
    benchmark_mode: form.benchmark_mode,
    signal_objective: form.signal_objective,
    label_recipe: form.label_recipe,
    run_export: form.run_export,
    reproducibility_mode: form.reproducibility_mode,
    universe_exit_policy: form.universe_exit_policy,
    start_date: form.start_date || null,
    end_date: form.end_date || null,
    batch_size: toNumberOrNull(form.batch_size),
    topk: toNumberOrNull(form.topk),
    train_weeks: toNumberOrNull(form.train_weeks),
    valid_weeks: toNumberOrNull(form.valid_weeks),
    eval_count: toNumberOrNull(form.eval_count),
    rolling_recent_weeks: toNumberOrNull(form.rolling_recent_weeks),
    step_weeks: toNumberOrNull(form.step_weeks),
    rebalance_interval_weeks: toNumberOrNull(form.rebalance_interval_weeks),
    hold_buffer_rank: toNumberOrNull(form.hold_buffer_rank),
    min_liquidity_filter: toFloatOrNull(form.min_liquidity_filter),
    min_score_spread: toFloatOrNull(form.min_score_spread),
    industry_max_weight: toFloatOrNull(form.industry_max_weight),
    validation_execution_lag_steps: toNumberOrNull(form.validation_execution_lag_steps),
    validation_risk_degree: toFloatOrNull(form.validation_risk_degree),
    native_risk_degree: toFloatOrNull(form.native_risk_degree),
    account: toFloatOrNull(form.account),
    seed: toNumberOrNull(form.seed),
    recipe_parallel_workers: toNumberOrNull(form.recipe_parallel_workers),
    model_num_threads: toNumberOrNull(form.model_num_threads),
    walk_forward_enabled: form.walk_forward_enabled,
    walk_forward_start_date: form.walk_forward_start_date || null,
    walk_forward_end_date: form.walk_forward_end_date || null,
    walk_forward_train_weeks: toNumberOrNull(form.walk_forward_train_weeks),
    walk_forward_valid_weeks: toNumberOrNull(form.walk_forward_valid_weeks),
    walk_forward_step_weeks: toNumberOrNull(form.walk_forward_step_weeks),
    walk_forward_eval_count: toNumberOrNull(form.walk_forward_eval_count),
    diagnostics_enabled: form.diagnostics_enabled,
    run_validation_comparison: form.run_validation_comparison,
    validation_only_tradable: form.validation_only_tradable,
    native_only_tradable: form.native_only_tradable,
    publish_model: form.publish_model,
  };
  if (form.execution_panel_path) {
    configPayload.execution_panel_path = form.execution_panel_path;
  }
  if (form.feature_spec_path) {
    configPayload.feature_spec_path = form.feature_spec_path;
  }
  return {
    display_name: form.display_name,
    description: form.description || null,
    requested_by: form.requested_by,
    source_ref: sourceRef,
    config_payload: { ...advancedOverrides, ...configPayload },
    recipe_names: parseListInput(form.recipe_names),
  };
}

function findTaskSummary(board: TaskBoardResponse | undefined, taskId: string | null) {
  if (!board || !taskId) return undefined;
  const items = [
    board.running_task,
    ...board.queued_tasks,
    ...board.history_tasks,
  ].filter(Boolean) as ResearchTaskSummary[];
  return items.find((task) => task.task_id === taskId);
}

function applyPresetToEditor(
  preset: TaskPresetResponse,
  setters: {
    setEditorOpen: React.Dispatch<React.SetStateAction<boolean>>;
    setEditorTaskKind: React.Dispatch<React.SetStateAction<EditorTaskKind>>;
    setEditorSourceRef: React.Dispatch<React.SetStateAction<TaskSourceRef>>;
    setExportForm: React.Dispatch<React.SetStateAction<ExportTaskFormState>>;
    setWorkflowForm: React.Dispatch<React.SetStateAction<WorkflowTaskFormState>>;
  },
) {
  setters.setEditorTaskKind(preset.task_kind);
  setters.setEditorSourceRef(preset.source_ref);
  if (preset.task_kind === "export_panel") {
    const payload = preset.payload;
    const featureGroups = toStringArray(payload.feature_groups);
    const includedFeatures = toStringArray(payload.included_features);
    const excludedFeatures = toStringArray(payload.excluded_features);
    setters.setExportForm({
      display_name: String(payload.display_name ?? "Export Panel"),
      description: String(payload.description ?? ""),
      requested_by: String(payload.requested_by ?? "webapp"),
      output: String(payload.output ?? "artifacts/panels/weekly_features.parquet"),
      start_date: String(payload.start_date ?? ""),
      end_date: String(payload.end_date ?? ""),
      universe_profile: String(payload.universe_profile ?? ""),
      universe_mode: payload.universe_mode === "fixed_universe" ? "fixed_universe" : "historical_membership",
      batch_size: String(payload.batch_size ?? 300),
      enrichment_scope: String(payload.enrichment_scope ?? "research_full"),
      feature_mode: featureGroups.length || includedFeatures.length || excludedFeatures.length ? "custom" : "all",
      feature_groups: featureGroups,
      included_features: includedFeatures.join(", "),
      excluded_features: excludedFeatures.join(", "),
    });
  } else {
    const payload = preset.payload;
    setters.setWorkflowForm(syncWorkflowDerivedFields({
      ...workflowFormFromConfigPayload(payload.config_payload),
      display_name: String(payload.display_name ?? "Run Native Workflow"),
      description: String(payload.description ?? (payload.config_payload as Record<string, unknown> | undefined)?.task_description ?? ""),
      requested_by: String(payload.requested_by ?? "webapp"),
      recipe_names: toStringArray(payload.recipe_names).join(", "),
    }));
  }
  setters.setEditorOpen(true);
}

function toStringArray(value: unknown) {
  if (!Array.isArray(value)) return [];
  return value.map((item) => String(item)).filter(Boolean);
}

function toNumberOrNull(value: string) {
  const trimmed = value.trim();
  if (!trimmed) return null;
  const parsed = Number.parseInt(trimmed, 10);
  return Number.isNaN(parsed) ? null : parsed;
}

function toFloatOrNull(value: string) {
  const trimmed = value.trim();
  if (!trimmed) return null;
  const parsed = Number.parseFloat(trimmed);
  return Number.isNaN(parsed) ? null : parsed;
}

function toBoolean(value: unknown, fallback: boolean) {
  return typeof value === "boolean" ? value : fallback;
}

function normalizeDirectory(value: string, fallback: string) {
  const trimmed = value.trim().replace(/\/+$/g, "");
  return trimmed || fallback;
}

function joinPath(root: string, leaf: string) {
  return `${normalizeDirectory(root, ".")}/${leaf.replace(/^\/+/g, "")}`;
}

function basename(path: string) {
  const normalized = path.replace(/\/+$/g, "");
  const segments = normalized.split("/");
  return segments[segments.length - 1] || normalized;
}

function slugifyPathSegment(value: string, fallback: string) {
  const slug = value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
  return slug || fallback;
}

function buildOutputDir(root: string, displayName: string) {
  return joinPath(root, slugifyPathSegment(displayName, "workflow_run"));
}

function buildWorkflowDerivedPaths({
  display_name,
  panel_path,
  universe_profile,
}: Pick<WorkflowTaskFormState, "display_name" | "panel_path" | "universe_profile">) {
  const output_root_dir = WORKFLOW_OUTPUT_ROOT_DIR;
  const output_dir = buildOutputDir(output_root_dir, display_name);
  const execution_panel_dir = output_dir;
  const execution_panel_path = buildExecutionPanelPath(execution_panel_dir, panel_path, universe_profile);
  return { output_root_dir, output_dir, execution_panel_dir, execution_panel_path };
}

function syncWorkflowDerivedFields(form: WorkflowTaskFormState): WorkflowTaskFormState {
  return {
    ...form,
    ...buildWorkflowDerivedPaths(form),
  };
}

function panelExtension(path: string) {
  const name = basename(path);
  const index = name.lastIndexOf(".");
  if (index <= 0) return ".parquet";
  return name.slice(index);
}

function buildExecutionPanelFilename(panelPath: string, universeProfile: string) {
  return `${slugifyPathSegment(universeProfile, "universe")}_execution_panel${panelExtension(panelPath)}`;
}

function buildExecutionPanelPath(root: string, panelPath: string, universeProfile: string) {
  if (!panelPath) return "";
  return joinPath(root, buildExecutionPanelFilename(panelPath, universeProfile));
}

function buildWorkflowRecipeOptions(recipeNames: string) {
  const current = parseListInput(recipeNames);
  return Array.from(new Set([...WORKFLOW_RECIPE_OPTIONS, ...current]));
}

function isBuiltinBenchmarkMode(value: string) {
  return BUILTIN_BENCHMARK_MODE_OPTIONS.includes(value as (typeof BUILTIN_BENCHMARK_MODE_OPTIONS)[number]);
}

function getBenchmarkModeSelectValue(value: string) {
  return isBuiltinBenchmarkMode(value) ? value : "custom_index";
}

function buildWorkflowPanelOptions(panels: PanelSummary[], currentPath: string) {
  const items = panels
    .map((panel) => ({
      path: panel.path,
      label: `${panel.name} · ${panel.format.toUpperCase()}`,
    }))
    .sort((left, right) => left.label.localeCompare(right.label));
  if (currentPath && !items.some((item) => item.path === currentPath)) {
    items.unshift({
      path: currentPath,
      label: `${basename(currentPath)} · Current path`,
    });
  }
  if (!items.length) {
    items.push({
      path: currentPath || "",
      label: currentPath ? basename(currentPath) : "No panels available",
    });
  }
  return items;
}

function workflowFormFromConfigPayload(value: unknown): WorkflowTaskFormState {
  const payload = value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
  const knownKeys = new Set([
    "panel_path",
    "execution_panel_path",
    "output_dir",
    "task_description",
    "feature_spec_path",
    "universe_profile",
    "benchmark_mode",
    "signal_objective",
    "label_recipe",
    "run_export",
    "reproducibility_mode",
    "universe_exit_policy",
    "start_date",
    "end_date",
    "batch_size",
    "topk",
    "train_weeks",
    "valid_weeks",
    "eval_count",
    "rolling_recent_weeks",
    "step_weeks",
    "rebalance_interval_weeks",
    "hold_buffer_rank",
    "min_liquidity_filter",
    "min_score_spread",
    "industry_max_weight",
    "validation_execution_lag_steps",
    "validation_risk_degree",
    "native_risk_degree",
    "account",
    "seed",
    "recipe_parallel_workers",
    "model_num_threads",
    "walk_forward_enabled",
    "walk_forward_start_date",
    "walk_forward_end_date",
    "walk_forward_train_weeks",
    "walk_forward_valid_weeks",
    "walk_forward_step_weeks",
    "walk_forward_eval_count",
    "diagnostics_enabled",
    "run_validation_comparison",
    "validation_only_tradable",
    "native_only_tradable",
    "publish_model",
  ]);
  const advancedOverrides = Object.fromEntries(Object.entries(payload).filter(([key]) => !knownKeys.has(key)));
  const universeProfile = String(payload.universe_profile ?? "csi300");
  const panelPath = String(payload.panel_path ?? WORKFLOW_DEFAULT_PANEL_PATH);
  return syncWorkflowDerivedFields({
    ...defaultWorkflowTaskForm(),
    panel_path: panelPath,
    feature_spec_path: String(payload.feature_spec_path ?? ""),
    universe_profile: universeProfile,
    benchmark_mode: String(payload.benchmark_mode ?? "auto"),
    signal_objective: String(payload.signal_objective ?? "huber_regression"),
    label_recipe: String(payload.label_recipe ?? "blended_excess_4w_8w"),
    run_export: String(payload.run_export ?? "auto_if_missing"),
    reproducibility_mode: String(payload.reproducibility_mode ?? "balanced"),
    universe_exit_policy: String(payload.universe_exit_policy ?? "retain_quotes_for_existing_positions"),
    start_date: String(payload.start_date ?? "2016-01-01"),
    end_date: String(payload.end_date ?? ""),
    batch_size: String(payload.batch_size ?? "200"),
    topk: String(payload.topk ?? "10"),
    train_weeks: String(payload.train_weeks ?? "260"),
    valid_weeks: String(payload.valid_weeks ?? "52"),
    eval_count: String(payload.eval_count ?? "52"),
    rolling_recent_weeks: String(payload.rolling_recent_weeks ?? "52"),
    step_weeks: String(payload.step_weeks ?? "1"),
    rebalance_interval_weeks: String(payload.rebalance_interval_weeks ?? "1"),
    hold_buffer_rank: String(payload.hold_buffer_rank ?? ""),
    min_liquidity_filter: String(payload.min_liquidity_filter ?? "0"),
    min_score_spread: String(payload.min_score_spread ?? "0"),
    industry_max_weight: String(payload.industry_max_weight ?? ""),
    validation_execution_lag_steps: String(payload.validation_execution_lag_steps ?? "1"),
    validation_risk_degree: String(payload.validation_risk_degree ?? "1"),
    native_risk_degree: String(payload.native_risk_degree ?? "0.95"),
    account: String(payload.account ?? "1000000"),
    seed: String(payload.seed ?? "42"),
    recipe_parallel_workers: String(payload.recipe_parallel_workers ?? "1"),
    model_num_threads: String(payload.model_num_threads ?? ""),
    walk_forward_enabled: toBoolean(payload.walk_forward_enabled, true),
    walk_forward_start_date: String(payload.walk_forward_start_date ?? "2016-01-01"),
    walk_forward_end_date: String(payload.walk_forward_end_date ?? ""),
    walk_forward_train_weeks: String(payload.walk_forward_train_weeks ?? "260"),
    walk_forward_valid_weeks: String(payload.walk_forward_valid_weeks ?? "52"),
    walk_forward_step_weeks: String(payload.walk_forward_step_weeks ?? "1"),
    walk_forward_eval_count: String(payload.walk_forward_eval_count ?? "0"),
    diagnostics_enabled: toBoolean(payload.diagnostics_enabled, true),
    run_validation_comparison: toBoolean(payload.run_validation_comparison, true),
    validation_only_tradable: toBoolean(payload.validation_only_tradable, false),
    native_only_tradable: toBoolean(payload.native_only_tradable, true),
    publish_model: toBoolean(payload.publish_model, false),
    advanced_overrides: JSON.stringify(advancedOverrides, null, 2),
  });
}

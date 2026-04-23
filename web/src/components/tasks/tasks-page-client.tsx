"use client";

import * as React from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import Link from "next/link";
import { ArrowDown, ArrowUp, CircleHelp, ExternalLink, FileText, LayoutGrid, Loader2, PlayCircle, Square, Trash2 } from "lucide-react";

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
  getRecipeAnalysisTaskPreset,
  getPanelTaskPreset,
  getRunAnalysisTaskPreset,
  getRunTaskPreset,
  getTask,
  getTaskLogs,
  getTasks,
  postExportPanelTask,
  postNativeWorkflowTask,
  postResearchAnalysisTask,
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

type EditorTaskKind = "export_panel" | "run_native_workflow" | "run_research_analysis";
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
  customize_baseline_recipe: boolean;
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
  consensus_specs: ConsensusSpecFormState[];
  advanced_overrides: string;
};

type ConsensusSpecFormState = {
  id: string;
  enabled: boolean;
  primary_recipe: string;
  filter_recipe: string;
  filter_topn: string;
  name: string;
  use_custom_name: boolean;
};

type ResearchAnalysisTaskFormState = {
  display_name: string;
  description: string;
  requested_by: string;
  source_kind: "run" | "recipe" | "compare";
  batch_mode: "run_only" | "run_plus_lead_recipe" | "run_plus_all_recipes";
  run_id: string;
  recipe_name: string;
  compare_items_json: string;
  analysis_template: "investment_report" | "experiment_review" | "ui_insight" | "anomaly_diagnosis" | "native_workflow_system_report";
  analysis_engine: "auto" | "codex_cli" | "claude_cli" | "gemini_cli";
  skills: string;
  output_dir: string;
};

const UNIVERSE_PROFILE_OPTIONS = ["csi300", "csi500", "merged_csi300_500", "watchlist"] as const;
const WORKFLOW_OUTPUT_ROOT_DIR = "artifacts/native_workflow";
const WORKFLOW_PANELS_ROOT_DIR = "artifacts/panels";
const WORKFLOW_DEFAULT_PANEL_PATH = "artifacts/panels/csi300_weekly.parquet";
const WORKFLOW_RECIPE_OPTIONS = ["baseline", "mae_4w", "binary_4w", "rank_blended", "huber_8w"] as const;
const BUILTIN_BENCHMARK_MODE_OPTIONS = ["auto", "flat_zero"] as const;
const DEFAULT_BASELINE_SIGNAL_OBJECTIVE = "huber_regression";
const DEFAULT_BASELINE_LABEL_RECIPE = "blended_excess_4w_8w";
const SIGNAL_OBJECTIVE_OPTIONS = ["huber_regression", "mae_regression", "binary_top_quintile", "grouped_rank"] as const;
const LABEL_RECIPE_OPTIONS = ["blended_excess_4w_8w", "excess_4w", "excess_8w"] as const;
const RUN_EXPORT_OPTIONS = ["always", "auto_if_missing", "never"] as const;
const REPRODUCIBILITY_OPTIONS = ["balanced", "strict"] as const;
const UNIVERSE_EXIT_POLICY_OPTIONS = ["retain_quotes_for_existing_positions", "strict_membership_only"] as const;
let consensusSpecIdCounter = 0;

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
  const [selectedHistoryTaskId, setSelectedHistoryTaskId] = React.useState<string | null>(null);
  const [historyFilter, setHistoryFilter] = React.useState<HistoryFilter>("all");
  const [editorOpen, setEditorOpen] = React.useState(false);
  const [editorTaskKind, setEditorTaskKind] = React.useState<EditorTaskKind>("export_panel");
  const [editorSourceRef, setEditorSourceRef] = React.useState<TaskSourceRef>(MANUAL_SOURCE);
  const [editorRecommendationReason, setEditorRecommendationReason] = React.useState<string>("");
  const [exportForm, setExportForm] = React.useState<ExportTaskFormState>(defaultExportTaskForm());
  const [workflowForm, setWorkflowForm] = React.useState<WorkflowTaskFormState>(defaultWorkflowTaskForm());
  const [analysisForm, setAnalysisForm] = React.useState<ResearchAnalysisTaskFormState>(defaultResearchAnalysisTaskForm());
  const appliedPresetKeyRef = React.useRef<string | null>(null);

  const board = tasksQuery.data;
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
  const selectedTaskSummary = React.useMemo(
    () => findTaskSummary(board, selectedHistoryTaskId),
    [board, selectedHistoryTaskId],
  );
  const selectedTaskRefreshId = selectedTaskSummary?.task_id;
  const selectedTaskRefreshToken = selectedTaskSummary
    ? [
        selectedTaskSummary.status,
        selectedTaskSummary.started_at ?? "",
        selectedTaskSummary.finished_at ?? "",
      ].join("|")
    : "";

  React.useEffect(() => {
    if (!board) return;
    const allTaskIds = [
      board.running_task?.task_id,
      ...board.queued_tasks.map((task) => task.task_id),
    ].filter(Boolean) as string[];
    if (selectedTaskId && allTaskIds.includes(selectedTaskId)) {
      return;
    }
    const nextTaskId = board.running_task?.task_id ?? board.queued_tasks[0]?.task_id ?? null;
    setSelectedTaskId(nextTaskId);
  }, [board, selectedTaskId]);

  React.useEffect(() => {
    if (selectedHistoryTaskId && historyTasks.some((task) => task.task_id === selectedHistoryTaskId)) {
      return;
    }
    setSelectedHistoryTaskId(historyTasks[0]?.task_id ?? null);
  }, [historyTasks, selectedHistoryTaskId]);

  const taskDetailQuery = useQuery<ResearchTaskDetail>({
    queryKey: ["task-detail", selectedHistoryTaskId],
    queryFn: () => getTask(selectedHistoryTaskId as string),
    enabled: Boolean(selectedHistoryTaskId),
    refetchInterval: selectedTaskSummary && ["running", "stopping"].includes(selectedTaskSummary.status) ? 4_000 : false,
  });

  const logsQuery = useQuery<TaskLogResponse>({
    queryKey: ["task-logs", selectedHistoryTaskId],
    queryFn: () => getTaskLogs(selectedHistoryTaskId as string),
    enabled: Boolean(selectedHistoryTaskId),
    refetchInterval: selectedTaskSummary && ["running", "stopping"].includes(selectedTaskSummary.status) ? 4_000 : false,
  });

  React.useEffect(() => {
    if (!selectedTaskRefreshId) return;
    void queryClient.invalidateQueries({ queryKey: ["task-detail", selectedTaskRefreshId] });
    void queryClient.invalidateQueries({ queryKey: ["task-logs", selectedTaskRefreshId] });
  }, [queryClient, selectedTaskRefreshId, selectedTaskRefreshToken]);

  const presetTaskKind = normalizeTaskKind(searchParams.get("create"));
  const presetSourceType = searchParams.get("sourceType");
  const presetSourceId = searchParams.get("sourceId");
  const prefillConfig = parsePrefillConfig(searchParams.get("prefillConfig"));
  const prefillReason = searchParams.get("reason") ?? "";
  const presetKey = presetTaskKind ? `${presetTaskKind}:${presetSourceType ?? "manual"}:${presetSourceId ?? "manual"}` : null;
  const hasPresetEndpoint = supportsPresetSource(presetTaskKind, presetSourceType, presetSourceId);

  const presetQuery = useQuery<TaskPresetResponse>({
    queryKey: ["task-preset", presetTaskKind, presetSourceType, presetSourceId],
    queryFn: () => {
      if (presetSourceType === "panel" && presetSourceId) {
        return getPanelTaskPreset(presetSourceId);
      }
      if (presetSourceType === "run" && presetSourceId) {
        if (presetTaskKind === "run_research_analysis") {
          return getRunAnalysisTaskPreset(presetSourceId);
        }
        return getRunTaskPreset(presetSourceId);
      }
      if (presetSourceType === "recipe" && presetSourceId && presetTaskKind === "run_research_analysis") {
        const [runId, recipeName] = presetSourceId.split(":");
        return getRecipeAnalysisTaskPreset(runId, recipeName);
      }
      throw new Error("Unsupported preset source");
    },
    enabled: hasPresetEndpoint,
  });

  const openManualEditor = React.useCallback((taskKind: EditorTaskKind) => {
    setEditorTaskKind(taskKind);
    setEditorSourceRef(MANUAL_SOURCE);
    setEditorRecommendationReason(prefillReason);
    setExportForm(defaultExportTaskForm());
    setWorkflowForm(defaultWorkflowTaskForm());
    setAnalysisForm(defaultResearchAnalysisTaskForm());
    setEditorOpen(true);
  }, [prefillReason]);

  const handleEditorOpenChange = React.useCallback(
    (nextOpen: boolean) => {
      setEditorOpen(nextOpen);
      if (nextOpen) {
        return;
      }
      if (presetKey) {
        appliedPresetKeyRef.current = presetKey;
      }
      if (searchParams.toString()) {
        router.replace(pathname, { scroll: false });
      }
    },
    [pathname, presetKey, router, searchParams],
  );

  React.useEffect(() => {
    if (!presetTaskKind || !presetKey || appliedPresetKeyRef.current === presetKey) {
      return;
    }
    if (!presetSourceType || !presetSourceId || !hasPresetEndpoint) {
      openManualEditor(presetTaskKind);
      setEditorSourceRef({
        kind: normalizeSourceKind(presetSourceType),
        source_id: presetSourceId ?? "manual",
        label: presetSourceId ?? "Manual Task",
      });
      applyPrefillToEditor(prefillConfig, presetTaskKind, {
        setExportForm,
        setWorkflowForm,
        setAnalysisForm,
      });
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
      setEditorRecommendationReason,
      setExportForm,
      setWorkflowForm,
      setAnalysisForm,
    }, prefillConfig, prefillReason);
    appliedPresetKeyRef.current = presetKey;
    router.replace(pathname, { scroll: false });
  }, [hasPresetEndpoint, openManualEditor, pathname, presetKey, presetQuery.data, presetSourceId, presetSourceType, presetTaskKind, prefillConfig, prefillReason, router]);

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

  const analysisMutation = useMutation({
    mutationFn: postResearchAnalysisTask,
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
    if (editorTaskKind === "run_native_workflow") {
      workflowMutation.mutate(buildWorkflowTaskPayload(workflowForm, editorSourceRef));
      return;
    }
    analysisMutation.mutate(buildResearchAnalysisTaskPayload(analysisForm, editorSourceRef));
  };

  const isSubmitting = exportMutation.isPending || workflowMutation.isPending || analysisMutation.isPending;
  return (
    <div className="space-y-6">
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

      <div className="grid items-start gap-6 lg:grid-cols-[minmax(22rem,0.95fr)_minmax(0,1.05fr)]">
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

        <Card className="glass-card min-w-0">
          <CardHeader>
            <CardTitle className="text-base">Queued</CardTitle>
          </CardHeader>
          <CardContent className="max-h-[360px] space-y-3 overflow-y-auto">
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
      </div>

      <div role="separator" aria-orientation="horizontal" className="h-px bg-border/70" />

      <div className="grid items-start gap-6 lg:grid-cols-[minmax(22rem,0.95fr)_minmax(0,1.05fr)]">
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
          <CardContent className="max-h-[680px] space-y-3 overflow-y-auto">
            {historyTasks.length ? (
              historyTasks.map((task) => (
                <TaskListItem
                  key={task.task_id}
                  task={task}
                  selected={selectedHistoryTaskId === task.task_id}
                  onSelect={() => setSelectedHistoryTaskId(task.task_id)}
                />
              ))
            ) : (
              <EmptyCard message="历史任务会在这里按状态归档，支持随时回看配置、日志和结果摘要。" />
            )}
          </CardContent>
        </Card>

        <div className="min-w-0">
          <TaskDetailPanel
            task={taskDetailQuery.data}
            logs={logsQuery.data}
            isLoading={taskDetailQuery.isLoading}
          />
        </div>
      </div>

      <Dialog open={editorOpen} onOpenChange={handleEditorOpenChange}>
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
              {editorRecommendationReason ? (
                <div className="rounded-lg border border-border/60 bg-surface-2/50 px-3 py-2 text-sm text-muted-foreground">
                  推荐原因：{editorRecommendationReason}
                </div>
              ) : null}
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
                <Button
                  variant={editorTaskKind === "run_research_analysis" ? "default" : "outline"}
                  onClick={() => setEditorTaskKind("run_research_analysis")}
                >
                  Research Analysis
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
            ) : editorTaskKind === "run_native_workflow" ? (
              <WorkflowTaskEditor form={workflowForm} onChange={setWorkflowForm} panels={panelsQuery.data ?? []} />
            ) : (
              <ResearchAnalysisTaskEditor form={analysisForm} onChange={setAnalysisForm} />
            )}

            <DialogFooter>
              <Button onClick={handleSubmitEditor} disabled={isSubmitting}>
                {isSubmitting ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
                Submit Task
              </Button>
              <Button variant="outline" onClick={() => setEditorOpen(false)}>
                Cancel
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
        <FormSelect
          label="Universe Profile"
          value={form.universe_profile}
          options={UNIVERSE_PROFILE_OPTIONS}
          onChange={(value) => onChange((current) => ({ ...current, universe_profile: value }))}
        />
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
  const panelFileOptionsId = React.useId();
  const panelFileOptions = React.useMemo(() => buildWorkflowPanelFileOptions(panels, form.panel_path), [form.panel_path, panels]);
  const panelsByFilename = React.useMemo(
    () => new Map(panels.map((panel) => [workflowPanelFilename(panel.path), panel])),
    [panels],
  );
  const panelsByPath = React.useMemo(
    () => new Map(panels.map((panel) => [panel.path, panel])),
    [panels],
  );
  const baseRecipeOptions = React.useMemo(
    () => buildBaseWorkflowRecipeOptions(form.recipe_names, form.consensus_specs),
    [form.consensus_specs, form.recipe_names],
  );
  const consensusRecipeOptions = React.useMemo(() => ["", ...baseRecipeOptions], [baseRecipeOptions]);
  const requiredRecipeNames = React.useMemo(
    () => getConsensusRequiredRecipeNames(form.consensus_specs),
    [form.consensus_specs],
  );
  const requiredRecipeNameSet = React.useMemo(() => new Set(requiredRecipeNames), [requiredRecipeNames]);
  const manualRecipeNames = React.useMemo(() => parseListInput(form.recipe_names), [form.recipe_names]);
  const selectedRecipeNames = React.useMemo(
    () => Array.from(new Set([...manualRecipeNames, ...requiredRecipeNames])),
    [manualRecipeNames, requiredRecipeNames],
  );
  const hasBaselineRecipe = selectedRecipeNames.includes("baseline");
  const benchmarkModeSelectValue = getBenchmarkModeSelectValue(form.benchmark_mode);
  const allowCustomPanelPath = allowsWorkflowMissingPanelPath(form.run_export);
  const describe = describeWorkflowConfigKey;

  const updateDisplayName = (displayName: string) => {
    onChange((current) => syncWorkflowDerivedFields({ ...current, display_name: displayName }));
  };

  const updatePanelFile = (panelFile: string) => {
    const normalizedPanelPath = normalizeWorkflowPanelPathInput(panelFile);
    const panel = panelsByPath.get(normalizedPanelPath) ?? panelsByFilename.get(workflowPanelFilename(normalizedPanelPath));
    onChange((current) => syncWorkflowPanelSelection(current, normalizedPanelPath, panel));
  };

  const updateUniverseProfile = (universeProfile: string) => {
    onChange((current) => syncWorkflowDerivedFields({ ...current, universe_profile: universeProfile }));
  };

  const toggleRecipeName = (recipeName: string) => {
    if (requiredRecipeNameSet.has(recipeName)) return;
    onChange((current) => {
      const selected = parseListInput(current.recipe_names);
      const nextSelected = selected.includes(recipeName)
        ? selected.filter((item) => item !== recipeName)
        : baseRecipeOptions.filter((item) => item === recipeName || selected.includes(item));
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
          <FormField label="Panel File" description={`${describe("panel_path")} 选择已有 panel 时会保留完整路径；也可以只填写文件名并自动放到 ${WORKFLOW_PANELS_ROOT_DIR} 下。`}>
            <div className="space-y-2">
              {allowCustomPanelPath ? (
                <>
                  <Input
                    list={panelFileOptionsId}
                    value={form.panel_path}
                    onChange={(event) => updatePanelFile(event.target.value)}
                    placeholder={`${WORKFLOW_PANELS_ROOT_DIR}/csi500_weekly_20260410.parquet`}
                  />
                  <datalist id={panelFileOptionsId}>
                    {panelFileOptions.map((panel) => (
                      <option key={panel.path} value={panel.path}>
                        {panel.label}
                      </option>
                    ))}
                  </datalist>
                  <div className="text-xs text-muted-foreground">
                    当前 `Run Export` 允许先导出再执行；如果只填文件名，实际路径会自动固定到 <code>{WORKFLOW_PANELS_ROOT_DIR}</code> 下。
                  </div>
                </>
              ) : (
                <select
                  className="flex h-10 w-full rounded-lg border border-input/85 bg-surface-1/80 px-3 py-2 text-sm"
                  value={form.panel_path}
                  onChange={(event) => updatePanelFile(event.target.value)}
                >
                  {panelFileOptions.map((panel) => (
                    <option key={panel.path} value={panel.path}>
                      {panel.label}
                    </option>
                  ))}
                </select>
              )}
            </div>
          </FormField>
          <div className="md:col-span-2">
            <FormField label="Recipe Names" description={describe("recipe_names")}>
              <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-3">
                {baseRecipeOptions.map((recipeName) => {
                  const checked = selectedRecipeNames.includes(recipeName);
                  const locked = requiredRecipeNameSet.has(recipeName);
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
                        disabled={locked}
                        onChange={() => toggleRecipeName(recipeName)}
                      />
                      <span>{recipeName}</span>
                      {locked ? <Badge variant="info">required by consensus</Badge> : null}
                    </label>
                  );
                })}
              </div>
            </FormField>
          </div>
          <div className="md:col-span-2">
            <Card className="border border-border/60 bg-surface-2/40">
              <CardHeader className="flex flex-row items-center justify-between gap-3">
                <CardTitle className="text-sm">Consensus Filter Recipes</CardTitle>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  onClick={() =>
                    onChange((current) => ({
                      ...current,
                      consensus_specs: [...current.consensus_specs, createConsensusSpecFormState()],
                    }))
                  }
                >
                  Add Consensus Spec
                </Button>
              </CardHeader>
              <CardContent className="space-y-4">
                {form.consensus_specs.length ? (
                  form.consensus_specs.map((spec, index) => {
                    const derivedName = buildConsensusRecipeName(spec, form);
                    const filterTopNValue = spec.filter_topn || consensusDefaultFilterTopN(form);
                    const usesInvalidPair = !!spec.primary_recipe && spec.primary_recipe === spec.filter_recipe;
                    return (
                      <div key={spec.id} className="rounded-lg border border-border/60 bg-surface-1/40 p-4">
                        <div className="mb-3 flex items-center justify-between gap-3">
                          <div className="flex items-center gap-2">
                            <Badge variant={spec.enabled ? "success" : "outline"}>{`Spec ${index + 1}`}</Badge>
                            {usesInvalidPair ? <Badge variant="destructive">primary/filter must differ</Badge> : null}
                          </div>
                          <div className="flex items-center gap-3">
                            <BooleanSwitchField
                              label="Enabled"
                              value={spec.enabled}
                              onChange={(value) =>
                                onChange((current) => ({
                                  ...current,
                                  consensus_specs: current.consensus_specs.map((item) =>
                                    item.id === spec.id ? { ...item, enabled: value } : item,
                                  ),
                                }))
                              }
                            />
                            <Button
                              type="button"
                              variant="ghost"
                              size="sm"
                              onClick={() =>
                                onChange((current) => ({
                                  ...current,
                                  consensus_specs: current.consensus_specs.filter((item) => item.id !== spec.id),
                                }))
                              }
                            >
                              Remove
                            </Button>
                          </div>
                        </div>
                        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                          <FormSelect
                            label="Primary Recipe"
                            value={spec.primary_recipe}
                            description={describe("consensus_recipe_specs")}
                            options={consensusRecipeOptions}
                            onChange={(value) =>
                              onChange((current) => ({
                                ...current,
                                consensus_specs: current.consensus_specs.map((item) =>
                                  item.id === spec.id ? { ...item, primary_recipe: value } : item,
                                ),
                              }))
                            }
                          />
                          <FormSelect
                            label="Filter Recipe"
                            value={spec.filter_recipe}
                            description={describe("consensus_recipe_specs")}
                            options={consensusRecipeOptions}
                            onChange={(value) =>
                              onChange((current) => ({
                                ...current,
                                consensus_specs: current.consensus_specs.map((item) =>
                                  item.id === spec.id ? { ...item, filter_recipe: value } : item,
                                ),
                              }))
                            }
                          />
                          <FormField label="Filter TopN" description={describe("consensus_recipe_specs")}>
                            <Input
                              value={spec.filter_topn}
                              onChange={(event) =>
                                onChange((current) => ({
                                  ...current,
                                  consensus_specs: current.consensus_specs.map((item) =>
                                    item.id === spec.id ? { ...item, filter_topn: event.target.value } : item,
                                  ),
                                }))
                              }
                              placeholder={consensusDefaultFilterTopN(form)}
                            />
                          </FormField>
                          <ReadonlyPathField
                            label="Derived Recipe Name"
                            value={derivedName}
                            description="默认名称会随 primary、filter 和 TopN 自动更新。"
                            hint={`Effective TopN: ${filterTopNValue}`}
                          />
                        </div>
                        <div className="mt-3 grid gap-4 md:grid-cols-2">
                          <BooleanSwitchField
                            label="Custom Name"
                            value={spec.use_custom_name}
                            onChange={(value) =>
                              onChange((current) => ({
                                ...current,
                                consensus_specs: current.consensus_specs.map((item) =>
                                  item.id === spec.id
                                    ? { ...item, use_custom_name: value, name: value ? item.name : "" }
                                    : item,
                                ),
                              }))
                            }
                          />
                          {spec.use_custom_name ? (
                            <FormField label="Custom Derived Name">
                              <Input
                                value={spec.name}
                                onChange={(event) =>
                                  onChange((current) => ({
                                    ...current,
                                    consensus_specs: current.consensus_specs.map((item) =>
                                      item.id === spec.id ? { ...item, name: event.target.value } : item,
                                    ),
                                  }))
                                }
                                placeholder={buildConsensusRecipeName({ ...spec, use_custom_name: false, name: "" }, form)}
                              />
                            </FormField>
                          ) : (
                            <div className="text-sm text-muted-foreground">
                              关闭自定义名称时，派生 recipe 名称会按默认规则自动生成。
                            </div>
                          )}
                        </div>
                      </div>
                    );
                  })
                ) : (
                  <div className="text-sm text-muted-foreground">
                    没有配置共识过滤器时，只运行上面勾选的基础 recipe。
                  </div>
                )}
              </CardContent>
            </Card>
          </div>
          {hasBaselineRecipe ? (
            <div className="md:col-span-2">
              <BooleanSwitchField
                label="Customize Baseline Recipe"
                value={form.customize_baseline_recipe}
                onChange={(value) => onChange((current) => ({ ...current, customize_baseline_recipe: value }))}
              />
            </div>
          ) : null}
          {hasBaselineRecipe && form.customize_baseline_recipe ? (
            <>
              <FormSelect
                label="Baseline Signal Objective"
                value={form.signal_objective}
                description={describe("signal_objective")}
                options={SIGNAL_OBJECTIVE_OPTIONS}
                onChange={(value) => onChange((current) => ({ ...current, signal_objective: value }))}
              />
              <FormSelect
                label="Baseline Label Recipe"
                value={form.label_recipe}
                description={describe("label_recipe")}
                options={LABEL_RECIPE_OPTIONS}
                onChange={(value) => onChange((current) => ({ ...current, label_recipe: value }))}
              />
            </>
          ) : null}
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

function ResearchAnalysisTaskEditor({
  form,
  onChange,
}: {
  form: ResearchAnalysisTaskFormState;
  onChange: React.Dispatch<React.SetStateAction<ResearchAnalysisTaskFormState>>;
}) {
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
          placeholder="说明这次分析要回答什么问题"
        />
      </FormField>
      <div className="grid gap-4 md:grid-cols-2">
          <FormField label="Source Kind">
            <select
            className="flex h-10 w-full rounded-lg border border-input/85 bg-surface-1/80 px-3 py-2 text-sm"
            value={form.source_kind}
              onChange={(event) =>
                onChange((current) => ({
                  ...current,
                  source_kind: event.target.value as ResearchAnalysisTaskFormState["source_kind"],
                  batch_mode: event.target.value === "run" ? current.batch_mode : "run_only",
                }))
              }
            >
            <option value="run">run</option>
            <option value="recipe">recipe</option>
            <option value="compare">compare</option>
          </select>
        </FormField>
        <FormField label="Template">
          <select
            className="flex h-10 w-full rounded-lg border border-input/85 bg-surface-1/80 px-3 py-2 text-sm"
            value={form.analysis_template}
            onChange={(event) => onChange((current) => ({ ...current, analysis_template: event.target.value as ResearchAnalysisTaskFormState["analysis_template"] }))}
          >
            <option value="investment_report">investment_report</option>
            <option value="experiment_review">experiment_review</option>
            <option value="ui_insight">ui_insight</option>
            <option value="anomaly_diagnosis">anomaly_diagnosis</option>
            <option value="native_workflow_system_report">native_workflow_system_report</option>
          </select>
        </FormField>
        <FormField label="Engine">
          <select
            className="flex h-10 w-full rounded-lg border border-input/85 bg-surface-1/80 px-3 py-2 text-sm"
            value={form.analysis_engine}
            onChange={(event) => onChange((current) => ({ ...current, analysis_engine: event.target.value as ResearchAnalysisTaskFormState["analysis_engine"] }))}
          >
            <option value="auto">auto</option>
            <option value="codex_cli">codex_cli</option>
            <option value="claude_cli">claude_cli</option>
            <option value="gemini_cli">gemini_cli</option>
          </select>
        </FormField>
        <FormField label="Output Dir">
          <Input value={form.output_dir} onChange={(event) => onChange((current) => ({ ...current, output_dir: event.target.value }))} />
        </FormField>
        {form.source_kind === "run" ? (
          <FormField label="Batch Mode">
            <select
              className="flex h-10 w-full rounded-lg border border-input/85 bg-surface-1/80 px-3 py-2 text-sm"
              value={form.batch_mode}
              onChange={(event) => onChange((current) => ({ ...current, batch_mode: event.target.value as ResearchAnalysisTaskFormState["batch_mode"] }))}
            >
              <option value="run_only">run only</option>
              <option value="run_plus_lead_recipe">run + lead recipe</option>
              <option value="run_plus_all_recipes">run + all recipes</option>
            </select>
          </FormField>
        ) : null}
        <FormField label="Run ID">
          <Input value={form.run_id} onChange={(event) => onChange((current) => ({ ...current, run_id: event.target.value }))} placeholder="demo_run" />
        </FormField>
        <FormField label="Recipe Name">
          <Input value={form.recipe_name} onChange={(event) => onChange((current) => ({ ...current, recipe_name: event.target.value }))} placeholder="baseline" />
        </FormField>
      </div>
      <FormField label="Skills">
        <Input value={form.skills} onChange={(event) => onChange((current) => ({ ...current, skills: event.target.value }))} placeholder="skill_a, skill_b" />
      </FormField>
      <FormField label="Compare Items JSON">
        <textarea
          className="min-h-[120px] w-full rounded-lg border border-input/85 bg-surface-1/80 px-3 py-2 text-sm"
          value={form.compare_items_json}
          onChange={(event) => onChange((current) => ({ ...current, compare_items_json: event.target.value }))}
          placeholder='[{"run_id":"demo","recipe_name":"baseline","bundle":"walk_forward"}]'
        />
      </FormField>
    </div>
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
            <SourceRefLinks task={task} />
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

function SourceRefLinks({ task }: { task: ResearchTaskSummary }) {
  const links: Array<{ href: string; icon: React.ReactNode; label: string }> = [];
  const sourceRef = task.source_ref;
  const targetPanelId = getTaskTargetPanelId(task);
  const targetRunId = getTaskTargetRunId(task);

  if (task.task_kind === "export_panel" && targetPanelId) {
    links.push({ href: `/panels/${encodeURIComponent(targetPanelId)}`, icon: <LayoutGrid className="h-3.5 w-3.5" />, label: "Go to Panel" });
  } else if (task.task_kind === "run_native_workflow" && targetRunId) {
    links.push({ href: `/runs/${encodeURIComponent(targetRunId)}`, icon: <FileText className="h-3.5 w-3.5" />, label: "Go to Run" });
  }

  if (!links.length && sourceRef && sourceRef.kind !== "manual") {
    if (sourceRef.kind === "run") {
      links.push({ href: `/runs/${encodeURIComponent(sourceRef.source_id)}`, icon: <FileText className="h-3.5 w-3.5" />, label: "Go to Run" });
    }

    if (sourceRef.kind === "panel") {
      links.push({ href: `/panels/${encodeURIComponent(sourceRef.source_id)}`, icon: <LayoutGrid className="h-3.5 w-3.5" />, label: "Go to Panel" });
    }

    if (sourceRef.kind === "recipe") {
      const parts = sourceRef.source_id.split(":");
      const runId = parts[0];
      const recipeName = parts.length > 1 ? parts[1] : sourceRef.label ?? parts[0];
      if (runId && recipeName) {
        links.push({ href: `/runs/${encodeURIComponent(runId)}/recipes/${encodeURIComponent(recipeName)}`, icon: <ExternalLink className="h-3.5 w-3.5" />, label: "Go to Recipe" });
      }
    }

    if (sourceRef.kind === "compare") {
      links.push({ href: `/compare?runId=${encodeURIComponent(sourceRef.source_id)}`, icon: <ExternalLink className="h-3.5 w-3.5" />, label: "Go to Compare" });
    }
  }

  if (links.length === 0) return null;

  return (
    <TooltipProvider>
      <div className="flex items-center gap-1" onClick={(event) => event.stopPropagation()}>
        {links.map((link) => (
          <Tooltip key={link.href}>
            <TooltipTrigger asChild>
              <Link
                href={link.href}
                className="inline-flex items-center justify-center rounded-md border border-border/60 bg-background/70 p-1 text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
              >
                {link.icon}
              </Link>
            </TooltipTrigger>
            <TooltipContent>{link.label}</TooltipContent>
          </Tooltip>
        ))}
      </div>
    </TooltipProvider>
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
            {option || "Select recipe"}
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
  if (value === "export_panel" || value === "run_native_workflow" || value === "run_research_analysis") {
    return value;
  }
  return null;
}

function isAnalysisTemplate(value: unknown): value is ResearchAnalysisTaskFormState["analysis_template"] {
  return (
    value === "investment_report" ||
    value === "experiment_review" ||
    value === "ui_insight" ||
    value === "anomaly_diagnosis" ||
    value === "native_workflow_system_report"
  );
}

function defaultExportTaskForm(): ExportTaskFormState {
  return {
    display_name: "Export Panel",
    description: "",
    requested_by: "webapp",
    output: "artifacts/panels/weekly_features.parquet",
    start_date: "",
    end_date: "",
    universe_profile: "csi300",
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
    customize_baseline_recipe: false,
    panel_path: WORKFLOW_DEFAULT_PANEL_PATH,
    execution_panel_path: "",
    execution_panel_dir: "",
    output_dir: "",
    output_root_dir: WORKFLOW_OUTPUT_ROOT_DIR,
    feature_spec_path: "",
    universe_profile: defaultUniverseProfile,
    benchmark_mode: "auto",
    signal_objective: DEFAULT_BASELINE_SIGNAL_OBJECTIVE,
    label_recipe: DEFAULT_BASELINE_LABEL_RECIPE,
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
    validation_risk_degree: "0.95",
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
    validation_only_tradable: true,
    native_only_tradable: true,
    publish_model: false,
    consensus_specs: [],
    advanced_overrides: "{}",
  });
}

function defaultResearchAnalysisTaskForm(): ResearchAnalysisTaskFormState {
  return {
    display_name: "Run Research Analysis",
    description: "",
    requested_by: "webapp",
    source_kind: "run",
    batch_mode: "run_only",
    run_id: "",
    recipe_name: "",
    compare_items_json: "[]",
    analysis_template: "native_workflow_system_report",
    analysis_engine: "codex_cli",
    skills: "",
    output_dir: "artifacts/analysis",
  };
}

function parseListInput(value: string) {
  return value
    .split(/[\n,]/g)
    .map((item) => item.trim())
    .filter(Boolean);
}

function nextConsensusSpecId() {
  consensusSpecIdCounter += 1;
  return `consensus-spec-${consensusSpecIdCounter}`;
}

function createConsensusSpecFormState(overrides: Partial<ConsensusSpecFormState> = {}): ConsensusSpecFormState {
  return {
    id: overrides.id ?? nextConsensusSpecId(),
    enabled: overrides.enabled ?? true,
    primary_recipe: overrides.primary_recipe ?? "",
    filter_recipe: overrides.filter_recipe ?? "",
    filter_topn: overrides.filter_topn ?? "",
    name: overrides.name ?? "",
    use_custom_name: overrides.use_custom_name ?? false,
  };
}

function consensusDefaultFilterTopN(form: Pick<WorkflowTaskFormState, "topk" | "hold_buffer_rank">) {
  const topk = Math.max(Number(form.topk || 10) || 10, 1);
  const holdBufferRank = Number(form.hold_buffer_rank || 0) || 0;
  return String(Math.max(topk, holdBufferRank > 0 ? holdBufferRank : topk));
}

function consensusFilterTopNValue(
  spec: Pick<ConsensusSpecFormState, "filter_topn">,
  form: Pick<WorkflowTaskFormState, "topk" | "hold_buffer_rank">,
) {
  const explicitValue = Number(spec.filter_topn || "");
  if (Number.isFinite(explicitValue) && explicitValue > 0) {
    return Math.floor(explicitValue);
  }
  return Number(consensusDefaultFilterTopN(form));
}

function buildConsensusRecipeName(
  spec: Pick<ConsensusSpecFormState, "primary_recipe" | "filter_recipe" | "filter_topn" | "name" | "use_custom_name">,
  form: Pick<WorkflowTaskFormState, "topk" | "hold_buffer_rank">,
) {
  if (spec.use_custom_name && spec.name.trim()) {
    return spec.name.trim();
  }
  const primaryRecipe = spec.primary_recipe.trim();
  const filterRecipe = spec.filter_recipe.trim();
  if (!primaryRecipe || !filterRecipe) return "";
  return `${primaryRecipe}__consensus__${filterRecipe}_top${consensusFilterTopNValue(spec, form)}`;
}

function activeConsensusSpecs(form: WorkflowTaskFormState) {
  return form.consensus_specs
    .filter((spec) => spec.enabled)
    .filter((spec) => spec.primary_recipe.trim() && spec.filter_recipe.trim());
}

function getConsensusRequiredRecipeNames(consensusSpecs: ConsensusSpecFormState[]) {
  const required = new Set<string>();
  for (const spec of consensusSpecs) {
    if (!spec.enabled) continue;
    if (spec.primary_recipe.trim()) required.add(spec.primary_recipe.trim());
    if (spec.filter_recipe.trim()) required.add(spec.filter_recipe.trim());
  }
  return Array.from(required);
}

function buildBaseWorkflowRecipeOptions(recipeNames: string, consensusSpecs: ConsensusSpecFormState[]) {
  const current = parseListInput(recipeNames);
  return Array.from(
    new Set([
      ...WORKFLOW_RECIPE_OPTIONS,
      ...current,
      ...consensusSpecs.flatMap((spec) => [spec.primary_recipe.trim(), spec.filter_recipe.trim()]).filter(Boolean),
    ]),
  );
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
  const consensusSpecs = activeConsensusSpecs(form).map((spec) => ({
    name: buildConsensusRecipeName(spec, form),
    primary_recipe: spec.primary_recipe.trim(),
    filter_recipe: spec.filter_recipe.trim(),
    filter_topn: consensusFilterTopNValue(spec, form),
  }));
  const requiredRecipeNames = getConsensusRequiredRecipeNames(form.consensus_specs);
  const recipeNames = Array.from(new Set([...parseListInput(form.recipe_names), ...requiredRecipeNames]));
  const shouldCustomizeBaselineRecipe = form.customize_baseline_recipe && recipeNames.includes("baseline");
  let advancedOverrides: Record<string, unknown> = {};
  try {
    advancedOverrides = JSON.parse(form.advanced_overrides || "{}");
  } catch {
    advancedOverrides = {};
  }
  delete advancedOverrides.consensus_recipe_specs;
  const configPayload: Record<string, unknown> = {
    panel_path: form.panel_path,
    output_dir: form.output_dir,
    universe_profile: form.universe_profile,
    benchmark_mode: form.benchmark_mode,
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
  if (consensusSpecs.length) {
    configPayload.consensus_recipe_specs = consensusSpecs;
  }
  if (shouldCustomizeBaselineRecipe) {
    configPayload.signal_objective = form.signal_objective;
    configPayload.label_recipe = form.label_recipe;
  }
  if (form.execution_panel_path) {
    configPayload.execution_panel_path = form.execution_panel_path;
  }
  if (form.feature_spec_path) {
    configPayload.feature_spec_path = form.feature_spec_path;
  }
  const mergedConfigPayload = { ...advancedOverrides, ...configPayload };
  if (!shouldCustomizeBaselineRecipe) {
    delete mergedConfigPayload.signal_objective;
    delete mergedConfigPayload.label_recipe;
  }
  return {
    display_name: form.display_name,
    description: form.description || null,
    requested_by: form.requested_by,
    source_ref: sourceRef,
    config_payload: mergedConfigPayload,
    recipe_names: recipeNames,
  };
}


function buildResearchAnalysisTaskPayload(form: ResearchAnalysisTaskFormState, sourceRef: TaskSourceRef) {
  let compareItems: Record<string, unknown>[] | null = null;
  try {
    const parsed = JSON.parse(form.compare_items_json || "[]");
    compareItems = Array.isArray(parsed) ? parsed : null;
  } catch {
    compareItems = null;
  }
  return {
    display_name: form.display_name,
    description: form.description || null,
    requested_by: form.requested_by,
    source_ref: sourceRef,
    source_kind: form.source_kind,
    batch_mode: form.source_kind === "run" ? form.batch_mode : "run_only",
    include_all_recipes: form.source_kind === "run" ? form.batch_mode === "run_plus_all_recipes" : false,
    run_id: form.run_id || null,
    recipe_name: form.recipe_name || null,
    compare_items: compareItems,
    analysis_template: form.analysis_template,
    analysis_engine: form.analysis_engine,
    skills: parseListInput(form.skills),
    output_dir: form.output_dir || null,
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
    setEditorRecommendationReason: React.Dispatch<React.SetStateAction<string>>;
    setExportForm: React.Dispatch<React.SetStateAction<ExportTaskFormState>>;
    setWorkflowForm: React.Dispatch<React.SetStateAction<WorkflowTaskFormState>>;
    setAnalysisForm: React.Dispatch<React.SetStateAction<ResearchAnalysisTaskFormState>>;
  },
  prefillConfig: Record<string, unknown>,
  prefillReason: string,
) {
  setters.setEditorTaskKind(preset.task_kind);
  setters.setEditorSourceRef(preset.source_ref);
  setters.setEditorRecommendationReason(prefillReason);
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
  } else if (preset.task_kind === "run_native_workflow") {
    const payload = preset.payload;
    const nextForm = workflowFormFromConfigPayload(payload.config_payload);
    const requiredRecipeNames = new Set(getConsensusRequiredRecipeNames(nextForm.consensus_specs));
    setters.setWorkflowForm({
      ...nextForm,
      display_name: String(payload.display_name ?? "Run Native Workflow"),
      description: String(payload.description ?? (payload.config_payload as Record<string, unknown> | undefined)?.task_description ?? ""),
      requested_by: String(payload.requested_by ?? "webapp"),
      recipe_names: toStringArray(payload.recipe_names)
        .filter((recipeName) => !requiredRecipeNames.has(recipeName))
        .join(", "),
    });
  } else {
    const payload = preset.payload;
    setters.setAnalysisForm({
      display_name: String(payload.display_name ?? "Run Research Analysis"),
      description: String(payload.description ?? ""),
      requested_by: String(payload.requested_by ?? "webapp"),
      source_kind: (payload.source_kind === "recipe" || payload.source_kind === "compare") ? payload.source_kind : "run",
      batch_mode: normalizeAnalysisBatchMode(payload.batch_mode, payload.include_all_recipes),
      run_id: String(payload.run_id ?? ""),
      recipe_name: String(payload.recipe_name ?? ""),
      compare_items_json: JSON.stringify(payload.compare_items ?? [], null, 2),
      analysis_template: isAnalysisTemplate(payload.analysis_template) ? payload.analysis_template : "investment_report",
      analysis_engine:
        payload.analysis_engine === "codex_cli" || payload.analysis_engine === "claude_cli" || payload.analysis_engine === "gemini_cli"
          ? payload.analysis_engine
          : "codex_cli",
      skills: toStringArray(payload.skills).join(", "),
      output_dir: String(payload.output_dir ?? "artifacts/analysis"),
    });
  }
  applyPrefillToEditor(prefillConfig, preset.task_kind, setters);
  setters.setEditorOpen(true);
}

function parsePrefillConfig(value: string | null) {
  if (!value) return {};
  try {
    const parsed = JSON.parse(value);
    return isRecord(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

function applyPrefillToEditor(
  prefillConfig: Record<string, unknown>,
  taskKind: EditorTaskKind,
  setters: Pick<
    {
      setExportForm: React.Dispatch<React.SetStateAction<ExportTaskFormState>>;
      setWorkflowForm: React.Dispatch<React.SetStateAction<WorkflowTaskFormState>>;
      setAnalysisForm: React.Dispatch<React.SetStateAction<ResearchAnalysisTaskFormState>>;
    },
    "setExportForm" | "setWorkflowForm" | "setAnalysisForm"
  >,
) {
  if (!Object.keys(prefillConfig).length) {
    return;
  }
  if (taskKind === "run_native_workflow") {
    setters.setWorkflowForm((current) => {
      const prefillRecipeNames = toStringArray(prefillConfig.recipe_names);
      const nextBase = {
        ...current,
        display_name: String(prefillConfig.display_name ?? current.display_name),
        description: String(prefillConfig.description ?? current.description),
        recipe_names: prefillRecipeNames.join(", ") || current.recipe_names,
      };
      if (!Object.prototype.hasOwnProperty.call(prefillConfig, "config_payload")) {
        return nextBase;
      }
      const nextForm = workflowFormFromConfigPayload(prefillConfig.config_payload, nextBase);
      const requiredRecipeNames = new Set(getConsensusRequiredRecipeNames(nextForm.consensus_specs));
      return {
        ...nextForm,
        recipe_names: prefillRecipeNames.filter((recipeName) => !requiredRecipeNames.has(recipeName)).join(", ") || nextForm.recipe_names,
      };
    });
    return;
  }
  if (taskKind === "run_research_analysis") {
    setters.setAnalysisForm((current) => ({
      ...current,
      display_name: String(prefillConfig.display_name ?? current.display_name),
      description: String(prefillConfig.description ?? current.description),
      analysis_template: isAnalysisTemplate(prefillConfig.analysis_template) ? prefillConfig.analysis_template : current.analysis_template,
      batch_mode:
        Object.prototype.hasOwnProperty.call(prefillConfig, "batch_mode") ||
        Object.prototype.hasOwnProperty.call(prefillConfig, "include_all_recipes")
          ? normalizeAnalysisBatchMode(prefillConfig.batch_mode, prefillConfig.include_all_recipes)
          : current.batch_mode,
      skills: toStringArray(prefillConfig.skills).join(", ") || current.skills,
    }));
    return;
  }
  setters.setExportForm((current) => ({
    ...current,
    display_name: String(prefillConfig.display_name ?? current.display_name),
    description: String(prefillConfig.description ?? current.description),
  }));
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

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseJsonObject(value: string) {
  try {
    const parsed = JSON.parse(value || "{}");
    return isRecord(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

function joinPath(root: string, leaf: string) {
  return `${normalizeDirectory(root, ".")}/${leaf.replace(/^\/+/g, "")}`;
}

function basename(path: string) {
  const normalized = path.replace(/\/+$/g, "");
  const segments = normalized.split("/");
  return segments[segments.length - 1] || normalized;
}

function dirname(path: string) {
  const normalized = path.replace(/\/+$/g, "");
  const index = normalized.lastIndexOf("/");
  return index > 0 ? normalized.slice(0, index) : "";
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

function workflowPanelFilename(panelPath: string) {
  return basename(panelPath || "");
}

function workflowPanelPathFromFilename(value: string) {
  const filename = basename(value.trim());
  if (!filename) return "";
  return joinPath(WORKFLOW_PANELS_ROOT_DIR, filename);
}

function normalizeWorkflowPanelPathInput(value: string) {
  const trimmed = value.trim();
  if (!trimmed) return "";
  return trimmed.includes("/") ? trimmed : workflowPanelPathFromFilename(trimmed);
}

function inferUniverseProfileFromPanelPath(panelPath: string, panel?: Pick<PanelSummary, "universe_profile"> | null) {
  const panelUniverseProfile = panel?.universe_profile;
  if (panelUniverseProfile && UNIVERSE_PROFILE_OPTIONS.includes(panelUniverseProfile as (typeof UNIVERSE_PROFILE_OPTIONS)[number])) {
    return panelUniverseProfile;
  }
  const normalizedPath = panelPath.trim().toLowerCase();
  if (!normalizedPath) return null;
  if (normalizedPath.includes("merged_csi300_500")) return "merged_csi300_500";
  if (normalizedPath.includes("watchlist")) return "watchlist";
  if (normalizedPath.includes("csi500")) return "csi500";
  if (normalizedPath.includes("csi300")) return "csi300";
  return null;
}

type WorkflowDerivedSyncOptions = {
  preserveOutputRootDir?: boolean;
  preserveOutputDir?: boolean;
  preserveExecutionPanelDir?: boolean;
  preserveExecutionPanelPath?: boolean;
};

function syncWorkflowDerivedFields(form: WorkflowTaskFormState, options: WorkflowDerivedSyncOptions = {}): WorkflowTaskFormState {
  const output_root_dir = options.preserveOutputRootDir && form.output_root_dir.trim()
    ? normalizeDirectory(form.output_root_dir, WORKFLOW_OUTPUT_ROOT_DIR)
    : WORKFLOW_OUTPUT_ROOT_DIR;
  const output_dir = options.preserveOutputDir && form.output_dir.trim()
    ? normalizeDirectory(form.output_dir, buildOutputDir(output_root_dir, form.display_name))
    : buildOutputDir(output_root_dir, form.display_name);
  const execution_panel_dir = options.preserveExecutionPanelDir && form.execution_panel_dir.trim()
    ? normalizeDirectory(form.execution_panel_dir, output_dir)
    : output_dir;
  const execution_panel_path = options.preserveExecutionPanelPath && form.execution_panel_path.trim()
    ? form.execution_panel_path.trim()
    : buildExecutionPanelPath(execution_panel_dir, form.panel_path, form.universe_profile);
  return {
    ...form,
    output_root_dir,
    output_dir,
    execution_panel_dir,
    execution_panel_path,
  };
}

function syncWorkflowPanelSelection(
  form: WorkflowTaskFormState,
  panelPath: string,
  panel?: Pick<PanelSummary, "universe_profile"> | null,
): WorkflowTaskFormState {
  const inferredUniverseProfile = inferUniverseProfileFromPanelPath(panelPath, panel);
  return syncWorkflowDerivedFields({
    ...form,
    panel_path: panelPath,
    universe_profile: inferredUniverseProfile ?? form.universe_profile,
  });
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

function isBuiltinBenchmarkMode(value: string) {
  return BUILTIN_BENCHMARK_MODE_OPTIONS.includes(value as (typeof BUILTIN_BENCHMARK_MODE_OPTIONS)[number]);
}

function getBenchmarkModeSelectValue(value: string) {
  return isBuiltinBenchmarkMode(value) ? value : "custom_index";
}

function allowsWorkflowMissingPanelPath(runExport: string) {
  return runExport === "always" || runExport === "auto_if_missing";
}

function buildWorkflowPanelFileOptions(panels: PanelSummary[], currentPath: string) {
  const items = panels
    .map((panel) => ({
      path: panel.path,
      filename: workflowPanelFilename(panel.path),
      label: `${workflowPanelFilename(panel.path)} · ${panel.path}`,
    }))
    .filter((panel) => panel.filename)
    .sort((left, right) => left.filename.localeCompare(right.filename));
  if (currentPath && !items.some((item) => item.path === currentPath)) {
    items.unshift({
      path: currentPath,
      filename: workflowPanelFilename(currentPath),
      label: `${workflowPanelFilename(currentPath)} · ${currentPath}`,
    });
  }
  if (!items.length) {
    items.push({
      path: currentPath,
      filename: workflowPanelFilename(currentPath),
      label: currentPath ? `${workflowPanelFilename(currentPath)} · ${currentPath}` : "No panels available",
    });
  }
  return Array.from(new Map(items.map((item) => [item.path, item])).values());
}

function normalizeAnalysisBatchMode(
  batchMode: unknown,
  includeAllRecipes: unknown,
): ResearchAnalysisTaskFormState["batch_mode"] {
  return batchMode === "run_plus_lead_recipe" || batchMode === "run_plus_all_recipes"
    ? batchMode
    : includeAllRecipes === true
      ? "run_plus_all_recipes"
      : "run_only";
}

function normalizeSourceKind(value: string | null | undefined): TaskSourceRef["kind"] {
  return value === "run" || value === "panel" || value === "recipe" || value === "compare" ? value : "manual";
}

function supportsPresetSource(
  taskKind: EditorTaskKind | null,
  sourceType: string | null,
  sourceId: string | null,
) {
  if (!taskKind || !sourceType || !sourceId) return false;
  if (sourceType === "panel" && taskKind === "export_panel") return true;
  if (sourceType === "run" && (taskKind === "run_native_workflow" || taskKind === "run_research_analysis")) return true;
  if (sourceType === "recipe" && taskKind === "run_research_analysis") return true;
  return false;
}

function getWorkflowTaskConfig(task: Pick<ResearchTaskSummary, "config_payload">) {
  const configPayload = task.config_payload?.config_payload;
  return isRecord(configPayload) ? configPayload : null;
}

function getTaskTargetPanelId(task: Pick<ResearchTaskSummary, "task_kind" | "output_dir" | "config_payload">) {
  if (task.task_kind !== "export_panel") return null;
  const outputValue = task.config_payload?.output ?? task.output_dir;
  if (typeof outputValue !== "string" || !outputValue.trim()) return null;
  return basename(outputValue);
}

function getTaskTargetRunId(task: Pick<ResearchTaskSummary, "task_kind" | "output_dir" | "config_payload">) {
  if (task.task_kind !== "run_native_workflow") return null;
  const workflowConfig = getWorkflowTaskConfig(task);
  const outputDirValue = workflowConfig?.output_dir ?? task.output_dir;
  if (typeof outputDirValue !== "string" || !outputDirValue.trim()) return null;
  return basename(outputDirValue);
}

function workflowFormFromConfigPayload(value: unknown, baseForm?: WorkflowTaskFormState): WorkflowTaskFormState {
  const payload = value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
  const knownKeys = new Set([
    "panel_path",
    "execution_panel_path",
    "execution_panel_dir",
    "output_dir",
    "output_root_dir",
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
    "consensus_recipe_specs",
  ]);
  const base = baseForm ?? defaultWorkflowTaskForm();
  const has = (key: string) => Object.prototype.hasOwnProperty.call(payload, key);
  const stringValue = (key: string, fallback: string) => (has(key) && payload[key] != null ? String(payload[key]) : fallback);
  const boolValue = (key: string, fallback: boolean) => (has(key) ? toBoolean(payload[key], fallback) : fallback);
  const advancedOverrides = Object.fromEntries(Object.entries(payload).filter(([key]) => !knownKeys.has(key)));
  const existingAdvancedOverrides = parseJsonObject(base.advanced_overrides);
  delete existingAdvancedOverrides.consensus_recipe_specs;
  const consensusSpecs = Array.isArray(payload.consensus_recipe_specs)
    ? payload.consensus_recipe_specs
        .filter((item): item is Record<string, unknown> => isRecord(item))
        .map((item) =>
          createConsensusSpecFormState({
            primary_recipe: String(item.primary_recipe ?? ""),
            filter_recipe: String(item.filter_recipe ?? ""),
            filter_topn: item.filter_topn == null ? "" : String(item.filter_topn),
            name: String(item.name ?? ""),
            use_custom_name: typeof item.name === "string" && item.name.trim().length > 0,
          }),
        )
    : base.consensus_specs;
  const executionPanelPath = stringValue("execution_panel_path", base.execution_panel_path);
  const outputDir = stringValue("output_dir", base.output_dir);
  const signalObjective = stringValue("signal_objective", base.signal_objective);
  const labelRecipe = stringValue("label_recipe", base.label_recipe);
  const nextForm: WorkflowTaskFormState = {
    ...base,
    panel_path: stringValue("panel_path", base.panel_path),
    execution_panel_path: executionPanelPath,
    execution_panel_dir: stringValue("execution_panel_dir", executionPanelPath ? dirname(executionPanelPath) : base.execution_panel_dir),
    output_dir: outputDir,
    output_root_dir: stringValue("output_root_dir", outputDir ? dirname(outputDir) : base.output_root_dir),
    description: stringValue("task_description", base.description),
    feature_spec_path: stringValue("feature_spec_path", base.feature_spec_path),
    universe_profile: stringValue("universe_profile", base.universe_profile),
    benchmark_mode: stringValue("benchmark_mode", base.benchmark_mode),
    customize_baseline_recipe: has("signal_objective") || has("label_recipe")
      ? signalObjective !== DEFAULT_BASELINE_SIGNAL_OBJECTIVE || labelRecipe !== DEFAULT_BASELINE_LABEL_RECIPE
      : base.customize_baseline_recipe,
    signal_objective: signalObjective,
    label_recipe: labelRecipe,
    run_export: stringValue("run_export", base.run_export),
    reproducibility_mode: stringValue("reproducibility_mode", base.reproducibility_mode),
    universe_exit_policy: stringValue("universe_exit_policy", base.universe_exit_policy),
    start_date: stringValue("start_date", base.start_date),
    end_date: stringValue("end_date", base.end_date),
    batch_size: stringValue("batch_size", base.batch_size),
    topk: stringValue("topk", base.topk),
    train_weeks: stringValue("train_weeks", base.train_weeks),
    valid_weeks: stringValue("valid_weeks", base.valid_weeks),
    eval_count: stringValue("eval_count", base.eval_count),
    rolling_recent_weeks: stringValue("rolling_recent_weeks", base.rolling_recent_weeks),
    step_weeks: stringValue("step_weeks", base.step_weeks),
    rebalance_interval_weeks: stringValue("rebalance_interval_weeks", base.rebalance_interval_weeks),
    hold_buffer_rank: stringValue("hold_buffer_rank", base.hold_buffer_rank),
    min_liquidity_filter: stringValue("min_liquidity_filter", base.min_liquidity_filter),
    min_score_spread: stringValue("min_score_spread", base.min_score_spread),
    industry_max_weight: stringValue("industry_max_weight", base.industry_max_weight),
    validation_execution_lag_steps: stringValue("validation_execution_lag_steps", base.validation_execution_lag_steps),
    validation_risk_degree: stringValue("validation_risk_degree", base.validation_risk_degree),
    native_risk_degree: stringValue("native_risk_degree", base.native_risk_degree),
    account: stringValue("account", base.account),
    seed: stringValue("seed", base.seed),
    recipe_parallel_workers: stringValue("recipe_parallel_workers", base.recipe_parallel_workers),
    model_num_threads: stringValue("model_num_threads", base.model_num_threads),
    walk_forward_enabled: boolValue("walk_forward_enabled", base.walk_forward_enabled),
    walk_forward_start_date: stringValue("walk_forward_start_date", base.walk_forward_start_date),
    walk_forward_end_date: stringValue("walk_forward_end_date", base.walk_forward_end_date),
    walk_forward_train_weeks: stringValue("walk_forward_train_weeks", base.walk_forward_train_weeks),
    walk_forward_valid_weeks: stringValue("walk_forward_valid_weeks", base.walk_forward_valid_weeks),
    walk_forward_step_weeks: stringValue("walk_forward_step_weeks", base.walk_forward_step_weeks),
    walk_forward_eval_count: stringValue("walk_forward_eval_count", base.walk_forward_eval_count),
    diagnostics_enabled: boolValue("diagnostics_enabled", base.diagnostics_enabled),
    run_validation_comparison: boolValue("run_validation_comparison", base.run_validation_comparison),
    validation_only_tradable: boolValue("validation_only_tradable", base.validation_only_tradable),
    native_only_tradable: boolValue("native_only_tradable", base.native_only_tradable),
    publish_model: boolValue("publish_model", base.publish_model),
    consensus_specs: consensusSpecs,
    advanced_overrides: JSON.stringify({ ...existingAdvancedOverrides, ...advancedOverrides }, null, 2),
  };
  return syncWorkflowDerivedFields(nextForm, {
    preserveOutputRootDir: has("output_root_dir"),
    preserveOutputDir: has("output_dir"),
    preserveExecutionPanelDir: has("execution_panel_dir"),
    preserveExecutionPanelPath: has("execution_panel_path"),
  });
}

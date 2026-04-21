"use client";

import * as React from "react";

import { DataTable } from "@/components/data/data-table";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";

type RunAnomalyPeriodsProps = {
  anomalyPeriods: Record<string, unknown>[];
  firstRecipeName?: string | null;
};

export function RunAnomalyPeriods({ anomalyPeriods, firstRecipeName }: RunAnomalyPeriodsProps) {
  const [enabled, setEnabled] = React.useState(false);
  const recipeName = firstRecipeName || "recipe";

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-wrap items-center gap-2">
          {!enabled ? (
            <Button size="sm" onClick={() => setEnabled(true)}>
              加载异常期列表
            </Button>
          ) : (
            <Button size="sm" variant="outline" onClick={() => setEnabled(false)}>
              收起列表
            </Button>
          )}
          <Badge variant={anomalyPeriods.length ? "warning" : "neutral"}>
            {anomalyPeriods.length ? `${anomalyPeriods.length} 个异常期` : "暂无异常期"}
          </Badge>
        </div>
      </div>

      {!enabled ? (
        <p className="text-sm text-muted-foreground">
          按需加载最严重的 trade_date 明细，避免 Run Detail 首屏默认展开诊断表。
        </p>
      ) : (
        <DataTable
          table={{
            columns: ["trade_date", "actual_hold_count", "target_hold_count", "locked_residual_count", "sell_blocked_total_count", "actions"],
            rows: anomalyPeriods.map((row) => ({
              trade_date: row.trade_date,
              actual_hold_count: row.actual_hold_count,
              target_hold_count: row.target_hold_count,
              locked_residual_count: row.locked_residual_count,
              sell_blocked_total_count: row.sell_blocked_total_count,
              actions: `Jump ${recipeName} / Create Workflow Task`,
            })),
          }}
          maxRows={10}
        />
      )}
    </div>
  );
}

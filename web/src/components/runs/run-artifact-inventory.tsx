"use client";

import * as React from "react";
import { useQuery } from "@tanstack/react-query";

import { DataTable } from "@/components/data/data-table";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { getRunArtifactInventory } from "@/lib/api";
import { formatPathName } from "@/lib/format";
import { ArtifactInventoryResponse } from "@/lib/types";

export function RunArtifactInventory({ runId }: { runId: string }) {
  const [enabled, setEnabled] = React.useState(false);
  const inventoryQuery = useQuery({
    queryKey: ["run-artifact-inventory", runId],
    queryFn: () => getRunArtifactInventory(runId),
    enabled,
  });

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-wrap items-center gap-2">
          {!enabled ? (
            <Button size="sm" onClick={() => setEnabled(true)}>加载 Artifact Inventory</Button>
          ) : null}
          {enabled && inventoryQuery.isLoading ? <Badge variant="info">加载中…</Badge> : null}
          {enabled && inventoryQuery.error ? (
            <>
              <Badge variant="destructive">加载失败</Badge>
              <Button size="sm" variant="outline" onClick={() => inventoryQuery.refetch()}>
                重试
              </Button>
            </>
          ) : null}
        </div>
      </div>

      {!enabled ? (
        <p className="text-sm text-muted-foreground">按需加载完整产物清单。</p>
      ) : null}

      {enabled && inventoryQuery.data ? (
        <div className="space-y-4">
          {Object.entries(groupArtifacts(inventoryQuery.data.artifact_inventory)).map(([group, items]) => (
            <div key={group} className="space-y-2">
              <div className="text-sm font-medium">{group}</div>
              <DataTable
                table={{
                  columns: ["name", "path", "exists", "updated_at"],
                  rows: items.map((item) => ({
                    name: item.name,
                    path: formatPathName(item.path),
                    exists: item.exists,
                    updated_at: item.updated_at,
                  })),
                }}
                maxRows={40}
              />
            </div>
          ))}
        </div>
      ) : null}
    </div>
  );
}

function groupArtifacts(items: ArtifactInventoryResponse["artifact_inventory"]) {
  return items.reduce<Record<string, ArtifactInventoryResponse["artifact_inventory"]>>((groups, item) => {
    const group = item.name.includes("analysis/") ? "Research" : item.name.includes("holding_count_drift") || item.name.includes("signal_realization") || item.name.includes("sector_exposure") || item.name.includes("rebalance_audit") ? "Diagnostics" : item.name.includes("native_report") || item.name.includes("summary") || item.name.includes("benchmark") ? "Backtest" : "Exports";
    groups[group] = [...(groups[group] ?? []), item];
    return groups;
  }, {});
}

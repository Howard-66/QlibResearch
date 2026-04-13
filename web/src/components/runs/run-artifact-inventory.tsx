"use client";

import * as React from "react";
import { useQuery } from "@tanstack/react-query";

import { DataTable } from "@/components/data/data-table";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { getRunArtifactInventory } from "@/lib/api";
import { formatPathName } from "@/lib/format";

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
        <div className="text-base font-semibold">Artifact Inventory</div>
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
        <DataTable
          table={{
            columns: ["name", "path", "exists", "updated_at"],
            rows: inventoryQuery.data.artifact_inventory.map((item) => ({
              name: item.name,
              path: formatPathName(item.path),
              exists: item.exists,
              updated_at: item.updated_at,
            })),
          }}
          maxRows={40}
        />
      ) : null}
    </div>
  );
}

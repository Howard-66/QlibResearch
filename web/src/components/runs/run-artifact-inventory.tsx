"use client";

import * as React from "react";
import { useQuery } from "@tanstack/react-query";

import { DataTable } from "@/components/data/data-table";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { getRunArtifactInventory } from "@/lib/api";

export function RunArtifactInventory({ runId }: { runId: string }) {
  const [enabled, setEnabled] = React.useState(false);
  const inventoryQuery = useQuery({
    queryKey: ["run-artifact-inventory", runId],
    queryFn: () => getRunArtifactInventory(runId),
    enabled,
  });

  if (!enabled) {
    return (
      <div className="space-y-3">
        <p className="text-sm text-muted-foreground">artifact 文件树改为按需加载，避免进入 run 详情时枚举整个目录。</p>
        <Button onClick={() => setEnabled(true)}>加载 Artifact Inventory</Button>
      </div>
    );
  }

  if (inventoryQuery.isLoading) {
    return <Badge variant="info">加载 artifact inventory…</Badge>;
  }

  if (inventoryQuery.error || !inventoryQuery.data) {
    return (
      <div className="space-y-3">
        <Badge variant="destructive">artifact inventory 加载失败</Badge>
        <Button variant="outline" onClick={() => inventoryQuery.refetch()}>
          重试
        </Button>
      </div>
    );
  }

  return (
    <DataTable
      table={{
        columns: ["name", "path", "exists", "updated_at"],
        rows: inventoryQuery.data.artifact_inventory.map((item) => ({
          name: item.name,
          path: item.path,
          exists: item.exists,
          updated_at: item.updated_at,
        })),
      }}
      maxRows={40}
    />
  );
}

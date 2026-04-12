"use client";

import * as React from "react";

import { DataTablePayload } from "@/lib/types";

import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Button } from "@/components/ui/button";

export function DataTable({
  table,
  maxRows = 80,
}: {
  table: DataTablePayload;
  maxRows?: number;
}) {
  if (!table.columns.length) {
    return <div className="rounded-lg border border-dashed border-border/70 p-6 text-sm text-muted-foreground">当前没有可展示的数据。</div>;
  }

  const pageSize = Math.max(maxRows, 1);
  const [visibleRows, setVisibleRows] = React.useState(pageSize);

  React.useEffect(() => {
    setVisibleRows(pageSize);
  }, [pageSize, table.rows.length]);

  const rows = table.rows.slice(0, visibleRows);
  const hasMore = table.rows.length > rows.length;

  return (
    <div className="space-y-2">
      <div className="max-h-[36rem] overflow-auto rounded-xl border border-border/60">
        <Table>
        <TableHeader>
          <TableRow>
            {table.columns.map((column) => (
              <TableHead key={column}>{column}</TableHead>
            ))}
          </TableRow>
        </TableHeader>
        <TableBody>
          {rows.map((row, index) => (
            <TableRow key={index}>
              {table.columns.map((column) => (
                <TableCell key={column}>{renderCell(row[column])}</TableCell>
              ))}
            </TableRow>
          ))}
        </TableBody>
        </Table>
      </div>
      <div className="flex items-center justify-between gap-3">
        <p className="text-xs text-muted-foreground">当前已加载 {rows.length} / {table.rows.length} 行。</p>
        {hasMore ? (
          <Button size="sm" variant="outline" onClick={() => setVisibleRows((current) => Math.min(current + pageSize, table.rows.length))}>
            加载更多
          </Button>
        ) : null}
      </div>
    </div>
  );
}

function renderCell(value: unknown) {
  if (value === null || value === undefined || value === "") return "—";
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "number") return Number.isInteger(value) ? value.toString() : value.toFixed(4);
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

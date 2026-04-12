import { DataTablePayload } from "@/lib/types";

import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";

export function DataTable({
  table,
  maxRows = 20,
}: {
  table: DataTablePayload;
  maxRows?: number;
}) {
  if (!table.columns.length) {
    return <div className="rounded-lg border border-dashed border-border/70 p-6 text-sm text-muted-foreground">当前没有可展示的数据。</div>;
  }

  const rows = table.rows.slice(0, maxRows);
  return (
    <div className="space-y-2">
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
      {table.rows.length > rows.length ? (
        <p className="text-xs text-muted-foreground">已显示前 {rows.length} 行，共 {table.rows.length} 行。</p>
      ) : null}
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

"use client";

import * as React from "react";
import { ChevronLeft, ChevronRight, RefreshCcw, Search } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import {
  fetchBreakoutEvents,
  type BreakoutEvent,
} from "@/lib/breakout-api";
import { formatNumber, formatPercent } from "@/lib/format";

type SortKey = "date" | "symbol" | "score" | "future_return" | "label";
type SortDir = "asc" | "desc";

const PAGE_SIZE = 50;

export default function BreakoutEventsPage() {
  const [events, setEvents] = React.useState<BreakoutEvent[]>([]);
  const [total, setTotal] = React.useState(0);
  const [page, setPage] = React.useState(1);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState<string | null>(null);

  // filters (form state vs applied)
  const [symbolInput, setSymbolInput] = React.useState("");
  const [startInput, setStartInput] = React.useState("");
  const [endInput, setEndInput] = React.useState("");
  const [filters, setFilters] = React.useState<{ symbol?: string; start_date?: string; end_date?: string }>({});

  const [sortKey, setSortKey] = React.useState<SortKey>("date");
  const [sortDir, setSortDir] = React.useState<SortDir>("desc");

  const load = React.useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetchBreakoutEvents({
        page,
        page_size: PAGE_SIZE,
        ...filters,
      });
      setEvents(res.events ?? []);
      setTotal(res.total ?? 0);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [page, filters]);

  React.useEffect(() => {
    load();
  }, [load]);

  function applyFilters() {
    setFilters({
      symbol: symbolInput.trim() || undefined,
      start_date: startInput.trim() || undefined,
      end_date: endInput.trim() || undefined,
    });
    setPage(1);
  }

  function resetFilters() {
    setSymbolInput("");
    setStartInput("");
    setEndInput("");
    setFilters({});
    setPage(1);
  }

  function toggleSort(key: SortKey) {
    if (sortKey === key) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(key);
      setSortDir("desc");
    }
  }

  const sorted = React.useMemo(() => {
    const list = [...events];
    list.sort((a, b) => {
      const av = a[sortKey];
      const bv = b[sortKey];
      if (av === null || av === undefined) return 1;
      if (bv === null || bv === undefined) return -1;
      if (typeof av === "number" && typeof bv === "number") {
        return sortDir === "asc" ? av - bv : bv - av;
      }
      const as = String(av);
      const bs = String(bv);
      return sortDir === "asc" ? as.localeCompare(bs) : bs.localeCompare(as);
    });
    return list;
  }, [events, sortKey, sortDir]);

  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-2xl font-semibold">事件浏览器</h1>
          <p className="text-sm text-muted-foreground">按日期与股票筛选浏览突破事件，共 {total} 条</p>
        </div>
        <Button variant="outline" size="sm" onClick={load} disabled={loading}>
          <RefreshCcw className={`mr-2 h-4 w-4 ${loading ? "animate-spin" : ""}`} />
          刷新
        </Button>
      </div>

      <Card className="glass-card">
        <CardHeader>
          <CardTitle className="text-base">筛选</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid gap-3 md:grid-cols-4">
            <div className="space-y-1">
              <label className="text-xs font-medium text-muted-foreground">股票代码</label>
              <Input placeholder="如 000001.SZ" value={symbolInput} onChange={(e) => setSymbolInput(e.target.value)} />
            </div>
            <div className="space-y-1">
              <label className="text-xs font-medium text-muted-foreground">开始日期</label>
              <Input type="date" value={startInput} onChange={(e) => setStartInput(e.target.value)} />
            </div>
            <div className="space-y-1">
              <label className="text-xs font-medium text-muted-foreground">结束日期</label>
              <Input type="date" value={endInput} onChange={(e) => setEndInput(e.target.value)} />
            </div>
            <div className="flex items-end gap-2">
              <Button onClick={applyFilters} disabled={loading}>
                <Search className="mr-2 h-4 w-4" />
                查询
              </Button>
              <Button variant="outline" onClick={resetFilters} disabled={loading}>
                重置
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>

      {error ? (
        <Card className="glass-card border-tone-danger-border">
          <CardContent className="py-3 text-sm text-tone-danger">加载失败：{error}</CardContent>
        </Card>
      ) : null}

      <Card className="glass-card">
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle className="text-base">事件列表</CardTitle>
          <div className="flex items-center gap-2 text-xs text-muted-foreground">
            <Button
              variant="ghost"
              size="sm"
              onClick={() => setPage((p) => Math.max(1, p - 1))}
              disabled={page <= 1 || loading}
            >
              <ChevronLeft className="h-4 w-4" />
            </Button>
            <span>
              第 {page} / {totalPages} 页
            </span>
            <Button
              variant="ghost"
              size="sm"
              onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
              disabled={page >= totalPages || loading}
            >
              <ChevronRight className="h-4 w-4" />
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          {sorted.length === 0 ? (
            <div className="rounded-md border border-dashed border-border/60 p-6 text-center text-sm text-muted-foreground">
              {loading ? "加载中..." : "未找到符合条件的事件"}
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <SortableTh label="日期" col="date" sortKey={sortKey} sortDir={sortDir} onClick={toggleSort} />
                  <SortableTh label="股票" col="symbol" sortKey={sortKey} sortDir={sortDir} onClick={toggleSort} />
                  <TableHead>频率</TableHead>
                  <TableHead>突破价</TableHead>
                  <TableHead>前高价</TableHead>
                  <TableHead>量比</TableHead>
                  <SortableTh label="评分" col="score" sortKey={sortKey} sortDir={sortDir} onClick={toggleSort} />
                  <SortableTh label="实际收益" col="future_return" sortKey={sortKey} sortDir={sortDir} onClick={toggleSort} />
                  <SortableTh label="命中" col="label" sortKey={sortKey} sortDir={sortDir} onClick={toggleSort} />
                </TableRow>
              </TableHeader>
              <TableBody>
                {sorted.map((ev, idx) => (
                  <TableRow key={`${ev.symbol}_${ev.date}_${idx}`}>
                    <TableCell className="font-mono">{ev.date}</TableCell>
                    <TableCell className="font-medium">{ev.symbol}</TableCell>
                    <TableCell>
                      <Badge variant="outline">{ev.frequency}</Badge>
                    </TableCell>
                    <TableCell className="font-mono">{formatNumber(ev.breakout_price, 2)}</TableCell>
                    <TableCell className="font-mono">{formatNumber(ev.prev_high_price, 2)}</TableCell>
                    <TableCell className="font-mono">{formatNumber(ev.volume_ratio, 2)}</TableCell>
                    <TableCell className="font-mono">{ev.score !== null ? formatNumber(ev.score) : "—"}</TableCell>
                    <TableCell className="font-mono">
                      {ev.future_return !== null ? formatPercent(ev.future_return) : "—"}
                    </TableCell>
                    <TableCell>
                      {ev.label === null ? (
                        <span className="text-muted-foreground">—</span>
                      ) : ev.label === 1 ? (
                        <Badge variant="success">命中</Badge>
                      ) : (
                        <Badge variant="neutral">未命中</Badge>
                      )}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function SortableTh({
  label,
  col,
  sortKey,
  sortDir,
  onClick,
}: {
  label: string;
  col: SortKey;
  sortKey: SortKey;
  sortDir: SortDir;
  onClick: (col: SortKey) => void;
}) {
  const active = sortKey === col;
  return (
    <TableHead>
      <button
        type="button"
        onClick={() => onClick(col)}
        className={`flex items-center gap-1 text-left ${active ? "text-foreground" : ""}`}
      >
        {label}
        {active ? <span className="text-xs">{sortDir === "asc" ? "▲" : "▼"}</span> : null}
      </button>
    </TableHead>
  );
}

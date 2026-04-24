import * as React from "react";

import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { formatInteger, formatNumber, formatPercent } from "@/lib/format";
import type { DataTablePayload } from "@/lib/types";

export function ValidationComparisonPanel({
  table,
  emptyMessage = "暂无 validation comparison 数据",
}: {
  table: DataTablePayload;
  emptyMessage?: string;
}) {
  const rows = React.useMemo(() => table.rows as Record<string, unknown>[], [table.rows]);

  if (!rows.length) {
    return <div className="text-sm text-muted-foreground">{emptyMessage}</div>;
  }

  return (
    <div className="space-y-4">
      {rows.map((row, index) => (
        <ValidationComparisonCard
          key={String(row.item ?? row.recipe ?? `${row.bundle ?? "bundle"}-${index}`)}
          row={row}
        />
      ))}
    </div>
  );
}

function ValidationComparisonCard({ row }: { row: Record<string, unknown> }) {
  const title = typeof row.item === "string" && row.item ? row.item : null;
  const returnGap = toNumber(row.native_minus_validation_return);
  const drawdownGap = subtractNumbers(row.native_max_drawdown, row.validation_max_drawdown);
  const riskDegreeGap = subtractNumbers(row.native_risk_degree, row.validation_risk_degree);
  const nativeTradable = toBoolean(row.native_only_tradable);
  const validationTradable = toBoolean(row.validation_only_tradable);
  const tradableChanged = nativeTradable !== null && validationTradable !== null ? nativeTradable !== validationTradable : null;
  const validationLagSteps = toNumber(row.validation_execution_lag_steps);

  return (
    <Card className="border border-border/60 bg-surface-2/35">
      <CardHeader className="space-y-3">
        {title ? <CardTitle className="text-sm leading-snug">{title}</CardTitle> : null}
        <div className="flex flex-wrap gap-2">
          <Badge variant={toneFromComparisonMagnitude(returnGap)}>return gap {formatSignedPercent(returnGap, 2)}</Badge>
          <Badge variant={toneFromComparisonMagnitude(drawdownGap)}>drawdown delta {formatSignedPercent(drawdownGap, 2)}</Badge>
          {riskDegreeGap !== null ? (
            <Badge variant={toneFromComparisonMagnitude(riskDegreeGap)}>risk degree delta {formatSignedPercent(riskDegreeGap, 2)}</Badge>
          ) : null}
          {validationLagSteps !== null ? (
            <Badge variant="outline">validation lag {formatInteger(validationLagSteps)}</Badge>
          ) : null}
          {tradableChanged !== null ? (
            <Badge variant={tradableChanged ? "warning" : "neutral"}>{tradableChanged ? "tradable rule changed" : "tradable rule aligned"}</Badge>
          ) : null}
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid gap-4 xl:grid-cols-2">
          <ComparisonMetricCard
            title="Final Net Value"
            leftLabel="Native"
            leftValue={formatNumber(row.native_final_net_value, 2)}
            rightLabel="Validation"
            rightValue={formatNumber(row.validation_final_net_value, 2)}
            deltaLabel="Return Gap"
            deltaValue={formatSignedPercent(returnGap, 2)}
            deltaTone={toneFromComparisonMagnitude(returnGap)}
            detail="用最终净值对照实现端与 validation 端的收益落差。"
          />
          <ComparisonMetricCard
            title="Max Drawdown"
            leftLabel="Native"
            leftValue={formatPercent(row.native_max_drawdown, 2)}
            rightLabel="Validation"
            rightValue={formatPercent(row.validation_max_drawdown, 2)}
            deltaLabel="Delta"
            deltaValue={formatSignedPercent(drawdownGap, 2)}
            deltaTone={toneFromComparisonMagnitude(drawdownGap)}
            detail="回撤差值为 Native 减去 Validation；越负表示 Native 回撤更深。"
          />
          {riskDegreeGap !== null ? (
            <ComparisonMetricCard
              title="Risk Degree"
              leftLabel="Native"
              leftValue={formatPercent(row.native_risk_degree, 2)}
              rightLabel="Validation"
              rightValue={formatPercent(row.validation_risk_degree, 2)}
              deltaLabel="Delta"
              deltaValue={formatSignedPercent(riskDegreeGap, 2)}
              deltaTone={toneFromComparisonMagnitude(riskDegreeGap)}
              detail="这组配置差异会直接影响最终仓位规模，是解释收益偏差的第一层上下文。"
            />
          ) : null}
          {nativeTradable !== null || validationTradable !== null ? (
            <ComparisonMetricCard
              title="Tradability Filter"
              leftLabel="Native"
              leftValue={formatTradableMode(nativeTradable)}
              rightLabel="Validation"
              rightValue={formatTradableMode(validationTradable)}
              deltaLabel="Status"
              deltaValue={tradableChanged ? "Different" : "Aligned"}
              deltaTone={tradableChanged ? "warning" : "neutral"}
              detail="仅展示双方是否都要求 only tradable，这类执行假设差异会改变可交易股票集合。"
            />
          ) : null}
        </div>
      </CardContent>
    </Card>
  );
}

function ComparisonMetricCard({
  title,
  leftLabel,
  leftValue,
  rightLabel,
  rightValue,
  deltaLabel,
  deltaValue,
  deltaTone,
  detail,
}: {
  title: string;
  leftLabel: string;
  leftValue: string;
  rightLabel: string;
  rightValue: string;
  deltaLabel: string;
  deltaValue: string;
  deltaTone: "info" | "success" | "warning" | "destructive" | "neutral";
  detail?: string;
}) {
  return (
    <div className="rounded-xl border border-border/60 bg-surface-1/60 p-4">
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <div className="text-sm font-semibold text-foreground">{title}</div>
        <Badge variant={deltaTone}>
          {deltaLabel} {deltaValue}
        </Badge>
      </div>
      <div className="grid gap-3 sm:grid-cols-2">
        <MetricValueBlock label={leftLabel} value={leftValue} />
        <MetricValueBlock label={rightLabel} value={rightValue} />
      </div>
      {detail ? <div className="mt-3 text-xs leading-5 text-muted-foreground">{detail}</div> : null}
    </div>
  );
}

function MetricValueBlock({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border border-border/50 bg-surface-2/45 px-3 py-3">
      <div className="text-[11px] font-medium uppercase tracking-[0.08em] text-muted-foreground">{label}</div>
      <div className="mt-1 text-base font-semibold text-foreground">{value}</div>
    </div>
  );
}

function toNumber(value: unknown) {
  if (value === null || value === undefined || value === "") return null;
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function toBoolean(value: unknown) {
  if (typeof value === "boolean") return value;
  if (value === "true") return true;
  if (value === "false") return false;
  return null;
}

function subtractNumbers(left: unknown, right: unknown) {
  const leftNumber = toNumber(left);
  const rightNumber = toNumber(right);
  if (leftNumber === null || rightNumber === null) return null;
  return leftNumber - rightNumber;
}

function formatSignedPercent(value: number | null, digits = 2) {
  if (value === null) return "—";
  const magnitude = formatPercent(Math.abs(value), digits);
  if (value > 0) return `+${magnitude}`;
  if (value < 0) return `-${magnitude}`;
  return formatPercent(0, digits);
}

function formatTradableMode(value: boolean | null) {
  if (value === null) return "—";
  return value ? "Only Tradable" : "No Filter";
}

function toneFromComparisonMagnitude(value: number | null): "info" | "success" | "warning" | "destructive" | "neutral" {
  if (value === null) return "neutral";
  const magnitude = Math.abs(value);
  if (magnitude < 1e-9) return "neutral";
  if (magnitude < 0.02) return "info";
  return "warning";
}

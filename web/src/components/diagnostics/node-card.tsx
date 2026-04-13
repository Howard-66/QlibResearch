import { AlertTriangle, CheckCircle2, CircleHelp, Clock3, XCircle } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { formatPathName, looksLikePath } from "@/lib/format";
import { DiagnosticNode } from "@/lib/types";

const statusConfig = {
  healthy: { icon: CheckCircle2, badge: "success" as const, label: "健康" },
  warning: { icon: AlertTriangle, badge: "warning" as const, label: "关注" },
  danger: { icon: XCircle, badge: "destructive" as const, label: "风险" },
  missing: { icon: CircleHelp, badge: "neutral" as const, label: "缺失" },
  info: { icon: Clock3, badge: "info" as const, label: "说明" },
};

export function NodeCard({ node }: { node: DiagnosticNode }) {
  const config = statusConfig[node.status];
  const Icon = config.icon;
  return (
    <Card className="glass-card h-full">
      <CardHeader className="pb-3">
        <div className="flex items-start justify-between gap-3">
          <div className="space-y-1">
            <CardTitle className="flex items-center gap-2 text-base">
              <Icon className="h-4 w-4" />
              {node.headline}
            </CardTitle>
            <p className="text-xs uppercase tracking-[0.18em] text-muted-foreground">{node.key}</p>
          </div>
          <Badge variant={config.badge}>{config.label}</Badge>
        </div>
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="space-y-2 text-sm">
          {node.evidence.map((evidence) => (
            <div key={evidence.label} className="flex items-start justify-between gap-4 border-b border-border/40 pb-2 last:border-0 last:pb-0">
              <span className="text-muted-foreground">{evidence.label}</span>
              <span className="text-right font-medium">{formatEvidenceValue(evidence.label, evidence.value)}</span>
            </div>
          ))}
        </div>
        <p className="text-sm text-foreground/90">{node.interpretation}</p>
        {node.next_action ? <p className="text-xs text-muted-foreground">建议动作：{node.next_action}</p> : null}
      </CardContent>
    </Card>
  );
}

function formatEvidenceValue(label: string, value: unknown) {
  if (value === null || value === undefined || value === "") {
    return "—";
  }
  if (typeof value === "string" && (looksLikePath(value) || label.toLowerCase().includes("path") || label.toLowerCase().includes("panel"))) {
    return formatPathName(value);
  }
  return String(value);
}

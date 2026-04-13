import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/utils";

export function StatCard({
  title,
  value,
  detail,
  dense = false,
  compact = false,
}: {
  title: string;
  value: string;
  detail?: string;
  dense?: boolean;
  compact?: boolean;
}) {
  if (compact) {
    return (
      <Card className="glass-card hover-lift">
        <CardContent className="space-y-1 px-3 py-2.5">
          <div className="truncate text-[11px] font-medium uppercase tracking-[0.08em] text-muted-foreground/85">{title}</div>
          <div className="truncate text-sm font-semibold leading-tight text-foreground">{value}</div>
          {detail ? <p className="truncate text-[11px] text-muted-foreground">{detail}</p> : null}
        </CardContent>
      </Card>
    );
  }

  return (
    <Card className="glass-card hover-lift">
      <CardHeader className={cn("pb-2", dense && "px-4 pb-1.5 pt-4")}>
        <CardTitle className="text-sm font-medium text-muted-foreground">{title}</CardTitle>
      </CardHeader>
      <CardContent className={cn(dense && "px-4 pb-4 pt-0")}>
        <div className={cn("text-2xl font-semibold", dense && "text-xl leading-tight")}>{value}</div>
        {detail ? <p className={cn("mt-1 text-xs text-muted-foreground", dense && "mt-0.5 line-clamp-2")}>{detail}</p> : null}
      </CardContent>
    </Card>
  );
}

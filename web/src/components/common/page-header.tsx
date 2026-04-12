import { Badge } from "@/components/ui/badge";

export function PageHeader({
  kicker,
  title,
  description,
  badge,
}: {
  kicker?: string;
  title: string;
  description: string;
  badge?: string;
}) {
  return (
    <div className="space-y-3">
      {kicker ? <div className="theme-kicker">{kicker}</div> : null}
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="space-y-1">
          <h1 className="text-3xl font-semibold tracking-tight text-gradient-blue">{title}</h1>
          <p className="max-w-3xl text-sm text-muted-foreground">{description}</p>
        </div>
        {badge ? <Badge variant="info">{badge}</Badge> : null}
      </div>
    </div>
  );
}

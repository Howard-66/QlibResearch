import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";

export function PageHeader({
  kicker,
  badge,
}: {
  kicker?: string;
  title: string;
  description: string;
  badge?: string;
}) {
  if (!kicker && !badge) return null;

  return (
    <div className={cn("flex flex-wrap items-center gap-3", kicker ? "justify-between" : "justify-end")}>
      {kicker ? <div className="theme-kicker">{kicker}</div> : null}
      {badge ? <Badge variant="info">{badge}</Badge> : null}
    </div>
  );
}

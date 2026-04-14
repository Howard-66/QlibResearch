import { Suspense } from "react";

import { TasksPageClient } from "@/components/tasks/tasks-page-client";

export default function TasksPage() {
  return (
    <Suspense fallback={<div className="rounded-xl border border-dashed border-border/70 p-6 text-sm text-muted-foreground">正在加载 Tasks...</div>}>
      <TasksPageClient />
    </Suspense>
  );
}

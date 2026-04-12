import { getRuns } from "@/lib/api";
import { RunsPageClient } from "@/components/runs/runs-page-client";

export default async function RunsPage() {
  const runs = await getRuns();
  return <RunsPageClient runs={runs} />;
}

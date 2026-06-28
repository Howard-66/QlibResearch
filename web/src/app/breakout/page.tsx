import { BreakoutDashboardClient } from "@/components/breakout/breakout-dashboard-client";
import { getBreakoutResearchRuns } from "@/lib/api";

export default async function BreakoutPage() {
  const runs = await getBreakoutResearchRuns();
  return <BreakoutDashboardClient runs={runs} />;
}

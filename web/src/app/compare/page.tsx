import { ComparePageClient } from "@/components/compare/compare-page-client";
import { getRuns } from "@/lib/api";

export default async function ComparePage() {
  const runs = await getRuns();
  return <ComparePageClient runs={runs} />;
}

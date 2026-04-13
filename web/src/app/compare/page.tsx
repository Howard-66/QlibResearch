import { ComparePageClient } from "@/components/compare/compare-page-client";
import { getRuns } from "@/lib/api";

export default async function ComparePage({
  searchParams,
}: {
  searchParams?: Promise<{ runId?: string }>;
}) {
  const runs = await getRuns();
  const resolvedSearchParams = searchParams ? await searchParams : {};
  return <ComparePageClient runs={runs} initialRunId={resolvedSearchParams.runId} />;
}

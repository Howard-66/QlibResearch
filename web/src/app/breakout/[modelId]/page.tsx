import { notFound } from "next/navigation";

import { BreakoutDetailClient } from "@/components/breakout/breakout-detail-client";
import { getBreakoutResearchDetail } from "@/lib/api";
import type { BreakoutResearchDetail } from "@/lib/types";

export default async function BreakoutDetailPage({ params }: { params: Promise<{ modelId: string }> }) {
  const { modelId } = await params;
  let detail: BreakoutResearchDetail;
  try {
    detail = await getBreakoutResearchDetail(modelId);
  } catch {
    notFound();
  }

  return <BreakoutDetailClient detail={detail} />;
}

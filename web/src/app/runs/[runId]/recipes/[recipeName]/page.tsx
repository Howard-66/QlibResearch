import { RecipeDetailClient } from "@/components/recipes/recipe-detail-client";
import { getRecipeDetail } from "@/lib/api";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export default async function RecipeDetailPage({
  params,
}: {
  params: Promise<{ runId: string; recipeName: string }>;
}) {
  const { runId, recipeName } = await params;
  const detail = await getRecipeDetail(runId, recipeName);
  return (
    <div className="space-y-6">
      <RecipeDetailClient detail={detail} />
    </div>
  );
}

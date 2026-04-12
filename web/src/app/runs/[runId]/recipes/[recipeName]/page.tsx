import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from "@/components/ui/breadcrumb";
import { RecipeDetailClient } from "@/components/recipes/recipe-detail-client";
import { getRecipeDetail } from "@/lib/api";

export default async function RecipeDetailPage({
  params,
}: {
  params: Promise<{ runId: string; recipeName: string }>;
}) {
  const { runId, recipeName } = await params;
  const detail = await getRecipeDetail(runId, recipeName);
  return (
    <div className="space-y-6">
      <Breadcrumb>
        <BreadcrumbList>
          <BreadcrumbItem>
            <BreadcrumbLink href="/runs">Runs</BreadcrumbLink>
          </BreadcrumbItem>
          <BreadcrumbSeparator />
          <BreadcrumbItem>
            <BreadcrumbLink href={`/runs/${runId}`}>{runId}</BreadcrumbLink>
          </BreadcrumbItem>
          <BreadcrumbSeparator />
          <BreadcrumbItem>
            <BreadcrumbPage>{recipeName}</BreadcrumbPage>
          </BreadcrumbItem>
        </BreadcrumbList>
      </Breadcrumb>
      <RecipeDetailClient detail={detail} />
    </div>
  );
}

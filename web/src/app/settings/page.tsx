import { PageHeader } from "@/components/common/page-header";
import { DataTable } from "@/components/data/data-table";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export default function SettingsPage() {
  return (
    <div className="space-y-6">
      <div className="grid gap-6 xl:grid-cols-2">
        <Card className="glass-card">
          <CardHeader>
            <CardTitle className="text-base">Runtime</CardTitle>
          </CardHeader>
          <CardContent>
            <DataTable
              table={{
                columns: ["layer", "tooling", "command"],
                rows: [
                  { layer: "backend", tooling: "uv + FastAPI + Uvicorn", command: "uv sync --extra qlib --extra app && uv run uvicorn qlib_research.app.main:app --host 0.0.0.0 --port 8010 --reload" },
                  { layer: "frontend", tooling: "pnpm + Next.js 16", command: "cd web && pnpm install && pnpm dev --port 3010" },
                ],
              }}
            />
          </CardContent>
        </Card>

        <Card className="glass-card">
          <CardHeader>
            <CardTitle className="text-base">Ports</CardTitle>
          </CardHeader>
          <CardContent>
            <DataTable
              table={{
                columns: ["app", "frontend", "backend"],
                rows: [
                  { app: "QlibResearch", frontend: 3010, backend: 8010 },
                  { app: "ValueInvesting", frontend: 3000, backend: 8000 },
                ],
              }}
            />
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

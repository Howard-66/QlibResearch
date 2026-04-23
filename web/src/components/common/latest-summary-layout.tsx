"use client";

import * as React from "react";

import { MarkdownContent } from "@/components/common/markdown-preview-dialog";
import { parseLatestSummaryMarkdown, type LatestSummarySection } from "@/lib/latest-summary";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

function SummaryList({ title, items, markdown }: { title: string; items: string[]; markdown?: string }) {
  return (
    <Card className="border border-border/60 bg-surface-2/40">
      <CardHeader>
        <CardTitle className="text-sm">{title}</CardTitle>
      </CardHeader>
      <CardContent>
        {items.length ? (
          <ul className="space-y-2 text-sm">
            {items.map((item, index) => (
              <li key={`${item}-${index}`} className="rounded-lg border border-border/50 px-3 py-2">
                {item}
              </li>
            ))}
          </ul>
        ) : markdown ? (
          <MarkdownContent content={markdown} />
        ) : (
          <div className="text-sm text-muted-foreground">暂无内容</div>
        )}
      </CardContent>
    </Card>
  );
}

function MetricCard({ title, value }: { title: string; value: string }) {
  return (
    <Card className="glass-card">
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-medium text-muted-foreground">{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="text-lg font-semibold">{value}</div>
      </CardContent>
    </Card>
  );
}

function ExtraSectionCard({ section }: { section: LatestSummarySection }) {
  const combined = [...section.items, ...section.paragraphs].filter(Boolean);
  return (
    <Card className="border border-border/60 bg-surface-2/40">
      <CardHeader>
        <CardTitle className="text-sm">{section.title}</CardTitle>
      </CardHeader>
      <CardContent>
        {section.markdown ? (
          <MarkdownContent content={section.markdown} />
        ) : combined.length ? (
          <div className="space-y-2 text-sm">
            {combined.map((item, index) => (
              <div key={`${item}-${index}`} className="rounded-lg border border-border/50 px-3 py-2">
                {item}
              </div>
            ))}
          </div>
        ) : (
          <div className="text-sm text-muted-foreground">暂无内容</div>
        )}
      </CardContent>
    </Card>
  );
}

export function LatestSummaryLayout({
  content,
  mode = "full",
}: {
  content: string;
  mode?: "full" | "compact";
}) {
  const parsed = React.useMemo(() => parseLatestSummaryMarkdown(content), [content]);
  const isCompact = mode === "compact";
  const keyFindings = parsed.keyFindings;
  const risks = parsed.risks;
  const nextActions = parsed.nextActions;
  const keyFindingsMarkdown = parsed.keyFindingsMarkdown;
  const risksMarkdown = parsed.risksMarkdown;
  const nextActionsMarkdown = parsed.nextActionsMarkdown;
  const extraSections = isCompact ? [] : parsed.extraSections;
  const hasCoreContent =
    keyFindings.length > 0 ||
    Boolean(keyFindingsMarkdown) ||
    risks.length > 0 ||
    Boolean(risksMarkdown) ||
    nextActions.length > 0 ||
    Boolean(nextActionsMarkdown) ||
    Boolean(parsed.currentProblem) ||
    Boolean(parsed.recommendedAction);

  return (
    <div className="space-y-4">
      {hasCoreContent ? (
        <>
          {isCompact ? (
            <div className="grid gap-4 xl:grid-cols-2">
              <SummaryList title="Key Findings" items={keyFindings} markdown={keyFindingsMarkdown} />
              <SummaryList title="Next Actions" items={nextActions} markdown={nextActionsMarkdown} />
            </div>
          ) : (
            <>
              <div className="grid gap-4 xl:grid-cols-2">
                <SummaryList title="Key Findings" items={keyFindings} markdown={keyFindingsMarkdown} />
                <SummaryList title="Risks" items={risks} markdown={risksMarkdown} />
              </div>
              <div className="grid gap-4 xl:grid-cols-2">
                <MetricCard title="Current Problem" value={parsed.currentProblem ?? "—"} />
                <MetricCard title="Recommended Action" value={parsed.recommendedAction ?? "—"} />
              </div>
              {nextActions.length || nextActionsMarkdown ? (
                <SummaryList title="Next Actions" items={nextActions} markdown={nextActionsMarkdown} />
              ) : null}
            </>
          )}
        </>
      ) : isCompact && content.trim() ? (
        <SummaryList title="Summary" items={[]} markdown={content} />
      ) : null}
      {extraSections.length ? (
        <div className="grid gap-4 xl:grid-cols-2">
          {extraSections.map((section, index) => (
            <ExtraSectionCard key={`${section.title}-${index}`} section={section} />
          ))}
        </div>
      ) : null}
    </div>
  );
}

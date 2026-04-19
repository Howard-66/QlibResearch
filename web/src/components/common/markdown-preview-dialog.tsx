"use client";

import * as React from "react";

import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog";

type MarkdownPreviewDialogProps = {
  title: string;
  content: string;
  triggerLabel?: string;
};

type Block =
  | { type: "heading"; level: 1 | 2 | 3; text: string }
  | { type: "paragraph"; text: string }
  | { type: "list"; items: string[] }
  | { type: "code"; code: string };

function parseMarkdown(content: string): Block[] {
  const lines = content.replace(/\r\n/g, "\n").split("\n");
  const blocks: Block[] = [];
  let index = 0;

  while (index < lines.length) {
    const line = lines[index]?.trimEnd() ?? "";
    const trimmed = line.trim();

    if (!trimmed) {
      index += 1;
      continue;
    }

    if (trimmed.startsWith("```")) {
      const codeLines: string[] = [];
      index += 1;
      while (index < lines.length && !lines[index].trim().startsWith("```")) {
        codeLines.push(lines[index]);
        index += 1;
      }
      index += 1;
      blocks.push({ type: "code", code: codeLines.join("\n") });
      continue;
    }

    if (trimmed.startsWith("### ")) {
      blocks.push({ type: "heading", level: 3, text: trimmed.slice(4).trim() });
      index += 1;
      continue;
    }
    if (trimmed.startsWith("## ")) {
      blocks.push({ type: "heading", level: 2, text: trimmed.slice(3).trim() });
      index += 1;
      continue;
    }
    if (trimmed.startsWith("# ")) {
      blocks.push({ type: "heading", level: 1, text: trimmed.slice(2).trim() });
      index += 1;
      continue;
    }

    if (trimmed.startsWith("- ")) {
      const items: string[] = [];
      while (index < lines.length) {
        const itemLine = lines[index]?.trim() ?? "";
        if (!itemLine.startsWith("- ")) {
          break;
        }
        items.push(itemLine.slice(2).trim());
        index += 1;
      }
      blocks.push({ type: "list", items });
      continue;
    }

    const paragraphLines: string[] = [];
    while (index < lines.length) {
      const next = lines[index]?.trim() ?? "";
      if (!next || next.startsWith("#") || next.startsWith("- ") || next.startsWith("```")) {
        break;
      }
      paragraphLines.push(next);
      index += 1;
    }
    blocks.push({ type: "paragraph", text: paragraphLines.join(" ") });
  }

  return blocks;
}

function InlineMarkdown({ text }: { text: string }) {
  const parts = text.split(/(`[^`]+`)/g);
  return (
    <>
      {parts.map((part, index) =>
        part.startsWith("`") && part.endsWith("`") ? (
          <code key={`${part}-${index}`} className="rounded bg-surface-1/80 px-1.5 py-0.5 font-mono text-[0.9em]">
            {part.slice(1, -1)}
          </code>
        ) : (
          <React.Fragment key={`${part}-${index}`}>{part}</React.Fragment>
        ),
      )}
    </>
  );
}

function MarkdownContent({ content }: { content: string }) {
  const blocks = React.useMemo(() => parseMarkdown(content), [content]);
  return (
    <div className="space-y-4 text-sm leading-7 text-foreground">
      {blocks.map((block, index) => {
        if (block.type === "heading") {
          if (block.level === 1) {
            return (
              <h1 key={index} className="text-2xl font-semibold tracking-tight">
                <InlineMarkdown text={block.text} />
              </h1>
            );
          }
          if (block.level === 2) {
            return (
              <h2 key={index} className="text-lg font-semibold tracking-tight">
                <InlineMarkdown text={block.text} />
              </h2>
            );
          }
          return (
            <h3 key={index} className="text-base font-semibold tracking-tight">
              <InlineMarkdown text={block.text} />
            </h3>
          );
        }
        if (block.type === "list") {
          return (
            <ul key={index} className="list-disc space-y-2 pl-5">
              {block.items.map((item, itemIndex) => (
                <li key={`${item}-${itemIndex}`}>
                  <InlineMarkdown text={item} />
                </li>
              ))}
            </ul>
          );
        }
        if (block.type === "code") {
          return (
            <pre key={index} className="overflow-auto rounded-xl border border-border/60 bg-surface-2/60 p-4 text-xs leading-6">
              <code>{block.code}</code>
            </pre>
          );
        }
        return (
          <p key={index} className="text-sm text-foreground/90">
            <InlineMarkdown text={block.text} />
          </p>
        );
      })}
    </div>
  );
}

export function MarkdownPreviewDialog({
  title,
  content,
  triggerLabel = "查看详情",
}: MarkdownPreviewDialogProps) {
  return (
    <Dialog>
      <DialogTrigger asChild>
        <Button size="sm" variant="outline">
          {triggerLabel}
        </Button>
      </DialogTrigger>
      <DialogContent className="max-w-4xl">
        <DialogHeader>
          <DialogTitle>{title}</DialogTitle>
        </DialogHeader>
        <div className="max-h-[75vh] overflow-auto rounded-xl border border-border/60 bg-background/70 p-5">
          <MarkdownContent content={content} />
        </div>
      </DialogContent>
    </Dialog>
  );
}

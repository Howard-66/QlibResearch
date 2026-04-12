import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/utils";

const badgeVariants = cva(
  "inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-semibold transition-colors shadow-[inset_0_1px_0_hsl(var(--surface-overlay)/0.45)]",
  {
    variants: {
      variant: {
        default: "border-primary/20 bg-primary text-primary-foreground",
        outline: "border-border/80 bg-surface-1/60 text-foreground",
        info: "border-tone-info-border bg-tone-info-soft text-tone-info",
        success: "border-tone-success-border bg-tone-success-soft text-tone-success",
        warning: "border-tone-warning-border bg-tone-warning-soft text-tone-warning",
        destructive: "border-tone-danger-border bg-tone-danger-soft text-tone-danger",
        neutral: "border-tone-neutral-border bg-tone-neutral-soft text-tone-neutral",
      },
    },
    defaultVariants: {
      variant: "default",
    },
  },
);

export interface BadgeProps extends React.HTMLAttributes<HTMLDivElement>, VariantProps<typeof badgeVariants> {}

export function Badge({ className, variant, ...props }: BadgeProps) {
  return <div className={cn(badgeVariants({ variant }), className)} {...props} />;
}

"use client";

import * as React from "react";

import { cn } from "@/lib/utils";

type SwitchProps = Omit<React.ButtonHTMLAttributes<HTMLButtonElement>, "onChange"> & {
  checked: boolean;
  onCheckedChange?: (checked: boolean) => void;
};

export const Switch = React.forwardRef<HTMLButtonElement, SwitchProps>(
  ({ checked, onCheckedChange, className, disabled, ...props }, ref) => (
    <button
      {...props}
      ref={ref}
      type="button"
      role="switch"
      aria-checked={checked}
      disabled={disabled}
      onClick={() => {
        if (!disabled) {
          onCheckedChange?.(!checked);
        }
      }}
      className={cn(
        "inline-flex h-6 w-11 items-center rounded-full border border-transparent px-0.5 transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 focus-visible:ring-offset-background disabled:cursor-not-allowed disabled:opacity-50",
        checked ? "bg-primary" : "bg-muted",
        className,
      )}
    >
      <span
        className={cn(
          "block h-5 w-5 rounded-full bg-background shadow-sm transition-transform",
          checked ? "translate-x-5" : "translate-x-0",
        )}
      />
    </button>
  ),
);

Switch.displayName = "Switch";

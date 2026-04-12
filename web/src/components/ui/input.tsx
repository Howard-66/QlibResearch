import * as React from "react";

import { cn } from "@/lib/utils";

export const Input = React.forwardRef<HTMLInputElement, React.ComponentProps<"input">>(({ className, ...props }, ref) => (
  <input
    ref={ref}
    className={cn(
      "flex h-10 w-full rounded-lg border border-input/85 bg-surface-1/80 px-3 py-2 text-base text-foreground placeholder:text-muted-foreground focus-visible:border-ring/45 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring/25 md:text-sm",
      className,
    )}
    {...props}
  />
));
Input.displayName = "Input";

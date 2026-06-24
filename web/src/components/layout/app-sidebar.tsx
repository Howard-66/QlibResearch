"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  BarChart3,
  ClipboardList,
  FolderKanban,
  LayoutDashboard,
  PlayCircle,
  Settings2,
  Zap,
} from "lucide-react";

import { cn } from "@/lib/utils";

const items = [
  { href: "/", label: "Overview", icon: LayoutDashboard },
  { href: "/runs", label: "Runs", icon: FolderKanban },
  { href: "/compare", label: "Compare", icon: BarChart3 },
  { href: "/panels", label: "Panels", icon: ClipboardList },
  { href: "/tasks", label: "Tasks", icon: PlayCircle },
  { href: "/settings", label: "Settings", icon: Settings2 },
];

const breakoutItems = [
  { href: "/breakout", label: "概览" },
  { href: "/breakout/train", label: "训练" },
  { href: "/breakout/evaluate", label: "评估" },
  { href: "/breakout/events", label: "事件" },
  { href: "/breakout/models", label: "模型" },
  { href: "/breakout/config", label: "配置" },
];

export function AppSidebar({ open }: { open: boolean }) {
  const pathname = usePathname();
  const breakoutActive = pathname === "/breakout" || pathname.startsWith("/breakout/");

  return (
    <aside className="surface-floating flex h-full flex-col border-r border-border/55 rounded-none">
      <div className="h-16 border-b border-border/45 bg-gradient-to-r from-primary/10 via-tone-info-soft/80 to-transparent px-4">
        <Link href="/" className="flex h-full items-center gap-3">
          <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-gradient-to-br from-primary via-primary to-tone-info text-sm font-bold text-primary-foreground">
            QR
          </div>
          <div className={cn("overflow-hidden transition-all duration-300", !open && "lg:w-0 lg:opacity-0")}>
            <div className="font-semibold tracking-tight">QlibResearch</div>
            <div className="text-xs text-muted-foreground">Research Workbench</div>
          </div>
        </Link>
      </div>

      <nav className="space-y-1 p-3">
        {items.map((item) => {
          const Icon = item.icon;
          const active = pathname === item.href || (item.href !== "/" && pathname.startsWith(item.href));
          return (
            <Link
              key={item.href}
              href={item.href}
              className={cn(
                "group relative flex items-center gap-3 rounded-lg px-3 py-2.5 text-sm transition-all",
                active ? "bg-primary/7 text-primary" : "text-muted-foreground hover:bg-accent/50 hover:text-foreground",
                !open && "lg:justify-center",
              )}
              title={!open ? item.label : undefined}
            >
              <Icon className="h-5 w-5 shrink-0" />
              <span className={cn("transition-all duration-300", !open && "lg:w-0 lg:opacity-0 lg:hidden")}>{item.label}</span>
            </Link>
          );
        })}

        {/* Breakout group */}
        <div className="pt-2">
          <Link
            href="/breakout"
            className={cn(
              "group relative flex items-center gap-3 rounded-lg px-3 py-2.5 text-sm transition-all",
              breakoutActive ? "bg-primary/7 text-primary" : "text-muted-foreground hover:bg-accent/50 hover:text-foreground",
              !open && "lg:justify-center",
            )}
            title={!open ? "Breakout" : undefined}
          >
            <Zap className="h-5 w-5 shrink-0" />
            <span className={cn("transition-all duration-300", !open && "lg:w-0 lg:opacity-0 lg:hidden")}>Breakout</span>
          </Link>
          {open ? (
            <div className="mt-1 space-y-0.5 border-l border-border/55 pl-4 ml-5">
              {breakoutItems.map((sub) => {
                const subActive = pathname === sub.href;
                return (
                  <Link
                    key={sub.href}
                    href={sub.href}
                    className={cn(
                      "block rounded-md px-2.5 py-1.5 text-xs transition-colors",
                      subActive
                        ? "bg-primary/10 text-primary"
                        : "text-muted-foreground hover:bg-accent/40 hover:text-foreground",
                    )}
                  >
                    {sub.label}
                  </Link>
                );
              })}
            </div>
          ) : null}
        </div>
      </nav>
    </aside>
  );
}

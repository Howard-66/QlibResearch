"use client";

import { Moon, PanelLeft, Sun } from "lucide-react";
import { usePathname } from "next/navigation";
import { useTheme } from "next-themes";
import * as React from "react";

import { AppSidebar } from "@/components/layout/app-sidebar";
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from "@/components/ui/breadcrumb";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

type HeaderMeta = {
  title: string;
  description: string;
};

type BreadcrumbEntry = {
  href?: string;
  label: string;
};

const titleMap: Record<string, HeaderMeta> = {
  "/": { title: "Overview", description: "工作台总览、最近 runs 与任务状态" },
  "/runs": { title: "Runs", description: "按最近运行记录进入 quick judge 与深度复盘" },
  "/compare": { title: "Compare", description: "按 recipe 粒度做跨 run 对比" },
  "/panels": { title: "Panels", description: "查看 panel 元信息、列摘要与关联 runs" },
  "/tasks": { title: "Tasks", description: "提交 panel 导出与 native workflow 调度任务" },
  "/settings": { title: "Settings", description: "启动命令、端口约定与环境约束" },
};

function decodeSegment(segment: string) {
  return decodeURIComponent(segment);
}

function buildBreadcrumbs(pathname: string): BreadcrumbEntry[] {
  const segments = pathname.split("/").filter(Boolean);
  if (!segments.length) return [];

  if (segments[0] === "runs") {
    if (segments.length === 2) {
      return [
        { href: "/runs", label: "Runs" },
        { label: decodeSegment(segments[1]) },
      ];
    }
    if (segments.length === 4 && segments[2] === "recipes") {
      return [
        { href: "/runs", label: "Runs" },
        { href: `/runs/${segments[1]}`, label: decodeSegment(segments[1]) },
        { label: decodeSegment(segments[3]) },
      ];
    }
  }

  if (segments[0] === "panels" && segments.length === 2) {
    return [
      { href: "/panels", label: "Panels" },
      { label: decodeSegment(segments[1]) },
    ];
  }

  return [];
}

function resolveHeaderMeta(pathname: string): HeaderMeta {
  const direct = titleMap[pathname];
  if (direct) return direct;
  if (pathname.startsWith("/runs/")) {
    if (pathname.includes("/recipes/")) {
      return { title: "Recipe Detail", description: "查看单个 recipe 的表现、诊断、表格与产物。" };
    }
    return { title: "Run Review", description: "查看单次 run 的配置、诊断与 artifact 详情" };
  }
  if (pathname.startsWith("/panels/")) {
    return { title: "Panel Detail", description: "查看 panel 样本范围、列目录与样例数据" };
  }
  return { title: "QlibResearch", description: "研究工作台" };
}

export function AppLayout({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const { resolvedTheme, setTheme } = useTheme();
  const [sidebarOpen, setSidebarOpen] = React.useState(true);
  const headerMeta = resolveHeaderMeta(pathname);
  const breadcrumbs = buildBreadcrumbs(pathname);

  return (
    <div className="flex h-screen overflow-hidden bg-background text-foreground">
      <div className={cn("hidden shrink-0 transition-all duration-300 lg:block", sidebarOpen ? "w-64" : "w-20")}>
        <AppSidebar open={sidebarOpen} />
      </div>

      <main className="flex min-w-0 flex-1 flex-col">
        <header className="surface-floating sticky top-0 z-20 flex h-16 shrink-0 items-center justify-between gap-4 border-b border-border/55 px-4 lg:px-6 rounded-none">
          <div className="flex min-w-0 flex-1 items-start gap-3">
            <Button
              variant="ghost"
              size="sm"
              onClick={() => setSidebarOpen((value) => !value)}
              className="hidden h-9 w-9 p-0 lg:inline-flex"
              aria-label={sidebarOpen ? "收起侧边栏" : "展开侧边栏"}
            >
              <PanelLeft className={cn("h-4 w-4 transition-transform", !sidebarOpen && "rotate-180")} />
            </Button>
            <div className="min-w-0 flex-1 space-y-1.5">
              <div className="flex min-w-0 items-center gap-3 overflow-hidden">
                <p className="shrink-0 text-sm font-semibold">{headerMeta.title}</p>
                {breadcrumbs.length ? (
                  <Breadcrumb className="min-w-0 flex-1">
                    <BreadcrumbList className="min-w-0 flex-nowrap overflow-hidden whitespace-nowrap text-xs">
                      {breadcrumbs.map((item, index) => (
                        <React.Fragment key={`${item.label}-${index}`}>
                          <BreadcrumbItem className="min-w-0">
                            {item.href ? (
                              <BreadcrumbLink href={item.href} className="truncate">
                                {item.label}
                              </BreadcrumbLink>
                            ) : (
                              <BreadcrumbPage className="truncate text-xs">{item.label}</BreadcrumbPage>
                            )}
                          </BreadcrumbItem>
                          {index < breadcrumbs.length - 1 ? <BreadcrumbSeparator className="text-muted-foreground/60" /> : null}
                        </React.Fragment>
                      ))}
                    </BreadcrumbList>
                  </Breadcrumb>
                ) : null}
              </div>
              <p className="truncate text-[11px] text-muted-foreground">{headerMeta.description}</p>
            </div>
          </div>
          <div className="flex shrink-0 items-center gap-2">
            <Button
              variant="ghost"
              size="sm"
              onClick={() => setTheme(resolvedTheme === "dark" ? "light" : "dark")}
              className="h-9 w-9 p-0"
              aria-label={resolvedTheme === "dark" ? "切换到浅色主题" : "切换到深色主题"}
              title={resolvedTheme === "dark" ? "切换到浅色主题" : "切换到深色主题"}
            >
              {resolvedTheme === "dark" ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
            </Button>
          </div>
        </header>
        <div className="flex-1 overflow-auto p-4 md:p-6">{children}</div>
      </main>
    </div>
  );
}

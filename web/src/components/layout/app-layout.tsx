"use client";

import { Moon, PanelLeft, Sun } from "lucide-react";
import { usePathname } from "next/navigation";
import { useTheme } from "next-themes";
import * as React from "react";

import { AppSidebar } from "@/components/layout/app-sidebar";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

const titleMap: Record<string, { title: string; subtitle: string }> = {
  "/": { title: "Overview", subtitle: "工作台总览、最近 runs 与任务状态" },
  "/runs": { title: "Runs", subtitle: "按最近运行记录进入 quick judge 与深度复盘" },
  "/compare": { title: "Compare", subtitle: "按 recipe 粒度做跨 run 对比" },
  "/panels": { title: "Panels", subtitle: "查看 panel 元信息、列摘要与关联 runs" },
  "/tasks": { title: "Tasks", subtitle: "提交 panel 导出与 native workflow 调度任务" },
  "/settings": { title: "Settings", subtitle: "启动命令、端口约定与环境约束" },
};

function resolveTitle(pathname: string) {
  const direct = titleMap[pathname];
  if (direct) return direct;
  if (pathname.startsWith("/runs/")) {
    return { title: "Run Review", subtitle: "查看单次 run 的配置、诊断与 artifact 详情" };
  }
  if (pathname.startsWith("/panels/")) {
    return { title: "Panel Detail", subtitle: "查看 panel 样本范围、列目录与样例数据" };
  }
  return { title: "QlibResearch", subtitle: "研究工作台" };
}

export function AppLayout({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const { resolvedTheme, setTheme } = useTheme();
  const [sidebarOpen, setSidebarOpen] = React.useState(true);
  const title = resolveTitle(pathname);

  return (
    <div className="flex h-screen overflow-hidden bg-background text-foreground">
      <div className={cn("hidden shrink-0 transition-all duration-300 lg:block", sidebarOpen ? "w-64" : "w-20")}>
        <AppSidebar open={sidebarOpen} />
      </div>

      <main className="flex min-w-0 flex-1 flex-col">
        <header className="surface-floating z-20 flex h-16 shrink-0 items-center justify-between gap-4 border-b border-border/55 px-4 lg:px-6 rounded-none">
          <div className="flex items-center gap-3">
            <Button variant="ghost" size="sm" onClick={() => setSidebarOpen((value) => !value)} className="hidden lg:inline-flex">
              <PanelLeft className={cn("h-4 w-4 transition-transform", !sidebarOpen && "rotate-180")} />
            </Button>
            <div>
              <p className="text-sm font-semibold">{title.title}</p>
              <p className="hidden text-xs text-muted-foreground md:block">{title.subtitle}</p>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <span className="theme-kicker hidden md:inline-flex">Artifact-first</span>
            <Button
              variant="outline"
              size="sm"
              onClick={() => setTheme(resolvedTheme === "dark" ? "light" : "dark")}
            >
              {resolvedTheme === "dark" ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
              <span className="hidden md:inline">{resolvedTheme === "dark" ? "浅色" : "深色"}</span>
            </Button>
          </div>
        </header>
        <div className="flex-1 overflow-auto p-4 md:p-6">{children}</div>
      </main>
    </div>
  );
}

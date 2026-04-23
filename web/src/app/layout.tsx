import type { Metadata } from "next";

import { AppLayout } from "@/components/layout/app-layout";
import { Providers } from "@/components/providers";

import "./globals.css";
import "./ui-enhancements.css";

export const metadata: Metadata = {
  title: "QlibResearch Workbench",
  description: "Qlib research workbench for native workflow review and comparison.",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="zh-CN" suppressHydrationWarning>
      <body className="antialiased">
        <Providers>
          <AppLayout>{children}</AppLayout>
        </Providers>
      </body>
    </html>
  );
}

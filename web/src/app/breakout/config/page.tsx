"use client";

import * as React from "react";
import { Loader2, RefreshCcw, Save } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  fetchBreakoutConfig,
  updateBreakoutConfig,
  type BreakoutConfig,
} from "@/lib/breakout-api";

type SectionKey = keyof BreakoutConfig;

export default function BreakoutConfigPage() {
  const [config, setConfig] = React.useState<BreakoutConfig | null>(null);
  const [drafts, setDrafts] = React.useState<Record<SectionKey, string>>({
    detector: "{}",
    labeler: "{}",
    trainer: "{}",
  });
  const [parseErrors, setParseErrors] = React.useState<Record<SectionKey, string | null>>({
    detector: null,
    labeler: null,
    trainer: null,
  });
  const [loading, setLoading] = React.useState(true);
  const [saving, setSaving] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [actionMsg, setActionMsg] = React.useState<string | null>(null);

  const load = React.useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const cfg = await fetchBreakoutConfig();
      setConfig(cfg);
      setDrafts({
        detector: JSON.stringify(cfg.detector ?? {}, null, 2),
        labeler: JSON.stringify(cfg.labeler ?? {}, null, 2),
        trainer: JSON.stringify(cfg.trainer ?? {}, null, 2),
      });
      setParseErrors({ detector: null, labeler: null, trainer: null });
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  React.useEffect(() => {
    load();
  }, [load]);

  function updateDraft(key: SectionKey, value: string) {
    setDrafts((prev) => ({ ...prev, [key]: value }));
    try {
      JSON.parse(value);
      setParseErrors((prev) => ({ ...prev, [key]: null }));
    } catch (e) {
      setParseErrors((prev) => ({ ...prev, [key]: e instanceof Error ? e.message : String(e) }));
    }
  }

  async function onSave() {
    // Validate all drafts are parseable
    const sections: SectionKey[] = ["detector", "labeler", "trainer"];
    const parsed: Partial<BreakoutConfig> = {};
    for (const key of sections) {
      try {
        parsed[key] = JSON.parse(drafts[key]);
      } catch (e) {
        setActionMsg(`${key} 配置 JSON 解析失败：${e instanceof Error ? e.message : String(e)}`);
        return;
      }
    }
    setSaving(true);
    setActionMsg(null);
    try {
      const updated = await updateBreakoutConfig(parsed);
      setConfig(updated);
      setActionMsg("配置保存成功");
    } catch (e) {
      setActionMsg(`保存失败：${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setSaving(false);
    }
  }

  const hasErrors = Object.values(parseErrors).some((v) => v !== null);

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-2xl font-semibold">参数配置</h1>
          <p className="text-sm text-muted-foreground">编辑检测 / 标注 / 训练参数（JSON 格式），保存后持久化生效</p>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" onClick={load} disabled={loading}>
            <RefreshCcw className={`mr-2 h-4 w-4 ${loading ? "animate-spin" : ""}`} />
            重新加载
          </Button>
          <Button size="sm" onClick={onSave} disabled={saving || hasErrors || !config}>
            {saving ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Save className="mr-2 h-4 w-4" />}
            保存配置
          </Button>
        </div>
      </div>

      {error ? (
        <Card className="glass-card border-tone-danger-border">
          <CardContent className="py-3 text-sm text-tone-danger">加载失败：{error}</CardContent>
        </Card>
      ) : null}

      {actionMsg ? (
        <Card className="glass-card border-tone-info-border">
          <CardContent className="py-3 text-sm text-tone-info">{actionMsg}</CardContent>
        </Card>
      ) : null}

      <Card className="glass-card">
        <CardHeader>
          <CardTitle className="text-base">编辑配置</CardTitle>
        </CardHeader>
        <CardContent>
          {!config ? (
            <div className="rounded-md border border-dashed border-border/60 p-6 text-center text-sm text-muted-foreground">
              {loading ? "加载中..." : "暂无配置数据"}
            </div>
          ) : (
            <Tabs defaultValue="detector" className="w-full">
              <TabsList>
                <TabsTrigger value="detector">检测 (detector)</TabsTrigger>
                <TabsTrigger value="labeler">标注 (labeler)</TabsTrigger>
                <TabsTrigger value="trainer">训练 (trainer)</TabsTrigger>
              </TabsList>
              {(["detector", "labeler", "trainer"] as const).map((key) => (
                <TabsContent key={key} value={key} className="space-y-2">
                  <textarea
                    value={drafts[key]}
                    onChange={(e) => updateDraft(key, e.target.value)}
                    className="font-mono text-xs w-full min-h-[320px] rounded-lg border border-input/85 bg-surface-1/80 p-3 focus-visible:border-ring/45 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring/25"
                    spellCheck={false}
                  />
                  {parseErrors[key] ? (
                    <div className="text-xs text-tone-danger">JSON 解析错误：{parseErrors[key]}</div>
                  ) : (
                    <div className="text-xs text-muted-foreground">JSON 格式有效</div>
                  )}
                </TabsContent>
              ))}
            </Tabs>
          )}
        </CardContent>
      </Card>

      {config ? (
        <Card className="glass-card">
          <CardHeader>
            <CardTitle className="text-base">当前持久化配置</CardTitle>
          </CardHeader>
          <CardContent>
            <pre className="overflow-auto rounded-md border border-border/60 bg-surface-2/40 p-3 text-xs">
              {JSON.stringify(config, null, 2)}
            </pre>
          </CardContent>
        </Card>
      ) : null}
    </div>
  );
}

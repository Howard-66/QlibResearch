# FDH Adaptation Guide

这份说明用于约束 `FinanceDataHub` 与 `QlibResearch` 之间的数据语义边界，避免把研究语义过早固化到 FDH。

## 可以下沉到 FDH 的内容

只建议下沉 `symbol-local deterministic` 特征，也就是只依赖单证券自身历史、不会因研究股票池变化而改变定义的派生特征：

- `mom_*`
- `rev_*`
- `volatility_*`
- `downside_volatility_*`
- `amount_change_4w`
- `volume_change_4w`
- `amount_zscore_4w`
- 估值和质量 delta 类特征

这些特征已经在 `weekly_feature_panel` 中被拆分为独立的 enrichment layer，可由 `enrichment_scope=symbol_local` 单独导出。

## 不建议下沉到 FDH 的内容

以下内容继续留在 `QlibResearch`：

- label 构造
- label winsorize / z-score
- feature outlier audit 与任何 feature clipping 策略
- feature policy registry
- 行业内鲁棒标准化
- 研究期特征选择
- universe-scoped 截面语义特征

典型的 universe-scoped / research-semantic 特征包括：

- 行业分位和行业相对 rank
- `industry_*_rank_pct`
- `buffett_npm_flag`
- `buffett_moat_*`
- `macro_*_x_*` 交互项

原因是当前 panel 会先通过 `resolve_universe_symbols()` 收缩 universe，再计算这些特征；如果改成在 FDH 全市场预处理，会直接改变特征定义。

## 当前推荐契约

推荐把 panel 富化层级作为跨系统契约的一部分：

- `none`
  - 原始 weekly panel
- `symbol_local`
  - 可考虑由 FDH 预处理并落表
- `research_full`
  - 继续由 QlibResearch 在研究链路内计算

导出后的 panel 会带 sidecar metadata，记录 `panel_enrichment_scope`。

## 当前落地状态

一期已经在 `QlibResearch` 内完成：

- 派生特征分层
- enrichment contract
- 已富化 panel 的重复计算消除

二期如果要推进 FDH 落表，建议只围绕 `symbol_local` 层展开，并在回归验证通过后逐步切换消费方。

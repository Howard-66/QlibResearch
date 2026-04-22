# Analysis Playbook

## Six-Layer Diagnosis

1. Signal layer: judge `rank_ic_ir`, TopK excess, hit rate, decile spread, and coverage. A good Rank IC with negative TopK excess is a "can rank, cannot realize" pattern.
2. Score granularity layer: judge score dispersion, TopK unique ratio, TopK overlap, and score gap between top 10 and top 20. Low uniqueness or tiny score gaps make recipe promotion fragile.
3. Portfolio realization layer: compare target holdings, actual holdings, locked residuals, sell-blocked counts, rebalance drag, cost drag, and realized portfolio return.
4. Performance/risk layer: use walk-forward net return, annualized return, Sharpe, max drawdown, win rate, and Calmar. Do not promote a high-return recipe if drawdown or realization is unexplained.
5. Exposure/stability layer: inspect top1/top3 sector concentration, finance weight, latest top industries, slice positive ratio, worst slices, and regime gate diagnostics.
6. Live feasibility layer: inspect latest top candidates, current industry concentration, macro-industry match, target rows, score gap, liquidity fields if present, and whether the portfolio definition matches configured `topk`.

## Role Heuristics

- `lead`: strongest walk-forward return/risk profile with tolerable drawdown and no severe granularity/exposure flaw; may still be `investigate` at run level if evidence gaps are material.
- `filter`: improves consensus or risk control but has weak standalone TopK excess, unstable ranking, or too much drawdown.
- `candidate`: positive signal or portfolio evidence, but requires another experiment to prove mechanism.
- `reject`: negative walk-forward return with weak signal and no compensating role.
- `diagnose_only`: high return but severe score uniqueness, empty validation comparison, scorecard conflict, or unexplained portfolio construction issue.

## Verdict Heuristics

- `promote`: one lead recipe is clearly superior and live feasibility risks are manageable.
- `hold`: incumbent remains acceptable; no candidate clearly improves enough.
- `investigate`: high-potential recipe exists but evidence conflicts, drawdown, TopK realization, holding drift, or exposure prevents promotion.
- `reject`: recipes broadly fail signal and realization checks.

## Mandatory Comparisons

- Compare rolling vs walk-forward. If walk-forward is worse, call out possible sample-out degradation.
- Compare signal metrics vs realized portfolio metrics. If they disagree, diagnose the bridge.
- Compare promoted/gated recipes vs raw metrics. Gate pass is not final evidence.
- Compare current latest snapshot vs historical behavior. A live list dominated by one sector needs explicit caveat even if historical top1 sector weights look acceptable.

## Reader-Friendly Style

- Start each section with the decision implication, then give 1-3 rounded metrics that justify it.
- Prefer "这意味着..." and "下一步应..." explanations over a dense metric dump.
- Keep file-level evidence out of the main narrative unless the file status itself is the point, such as an empty validation comparison.
- Use the same Chinese names consistently: TopK 超额, 分数区分度, 实际持仓, 行业集中, 执行验证, 回撤来源.

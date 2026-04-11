# Data Preprocessing Optimization Archive

这份归档用于记录“数据预处理与特征工程优化方案”的实际落地情况，避免方案状态只留在聊天记录里。

## 已完成

### 1. panel enrichment contract

- 派生特征已拆成两层：
  - `symbol_local`
  - `research_full`
- `build_weekly_feature_panel()` / `export_weekly_feature_panel()` 已支持 `enrichment_scope`
- 导出 panel 会写 sidecar metadata，记录 `panel_enrichment_scope`
- native workflow 现在会检测 panel 已富化层级，只补缺失层，不再重复计算研究派生特征
- execution panel 默认按 `enrichment_scope=none` 导出，避免无意义的研究特征重复计算

### 2. train-only median imputation

- `build_training_frame()` 已支持显式传入 `fill_values`
- `weekly_model_eval` 已改为先确定 train segment，再基于 train-only medians 填补 train/valid/test
- `train_and_publish_weekly_snapshot()` 已切换到相同口径

### 3. feature policy registry

- 已引入显式 feature policy registry
- 当前 registry 覆盖：
  - `feature_type`
  - `derivation_layer`
  - `normalization_policy`
  - `outlier_policy`
- `get_normalized_feature_candidates()` 已改为走 policy，而不是只走隐式 group
- 默认不对 flag、rank、percentile、macro flag、interaction 做统一标准化

### 4. 研究诊断与冻结产物

- 已增加 `feature_redundancy.csv`
- 已增加 `feature_outlier_audit.csv`
- convergence workflow 已输出 `feature_spec.json`
- native workflow 已支持 `--feature-spec`
- snapshot 入口已兼容加载 `feature_spec.json`

### 5. 文档与边界说明

- 已更新 native workflow 文档，补充 enrichment scope、feature policy、feature spec、train-only imputation
- 已新增 FDH 适配边界说明文档

## 当前默认行为

- 不对全部 feature 默认 winsorize
- 不对全部 feature 默认标准化
- 不在 native 默认主链路里在线做特征选择
- 高相关特征默认只做诊断，不自动删列
- `feature importance` 仍然是训练后诊断产物，不是在线筛特征依据

## 尚未下沉到 FDH

以下能力仍明确保留在 `QlibResearch`：

- label 构造
- label 截尾与标准化
- feature outlier 策略决策
- feature policy registry
- 行业内鲁棒标准化
- 研究期 feature selection
- universe-scoped 截面语义特征

## 建议的后续动作

如果继续推进二期，建议按下面顺序：

1. 用 convergence workflow 持续产出稳定的 `feature_spec.json`
2. 对 `symbol_local` 层做 FDH 侧落表评估
3. 做 panel schema / 结果口径回归
4. 再决定是否让默认导出从 `research_full` 逐步切到“FDH symbol_local + QlibResearch research_full”

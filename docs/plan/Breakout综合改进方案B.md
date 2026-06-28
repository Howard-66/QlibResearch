# Breakout 突破策略 ML 研究模块综合改进方案B

## 设计原则

1. **流程导向**：用户视角按"检测 - 标注 - 特征 - 训练 - 评估 - 部署"的研究闭环组织信息
2. **增量复用**：每个流水线阶段独立产出可缓存产物，避免重复计算
3. **模型为轴**：以研究运行 (Research Run) 为核心实体，串联全流程产物
4. **域隔离**：截面多因子与择时策略在导航和数据流上彻底分离

---

## Task 1: 导航架构重构 — 研究域分组

### 目标
将 Sidebar 菜单按研究域分组，解决截面分析和择时策略混排的问题。

### 设计

```
Sidebar 结构:
─────────────────────────────
  QlibResearch Workbench
─────────────────────────────
  [Overview]          # 统一首页

  CROSS-SECTION       # 分组标签（灰色小字）
  [Runs]
  [Compare]
  [Panels]

  TIMING STRATEGY     # 分组标签
  [Breakout]          # 一级入口（含流程式子Tab）

  SYSTEM              # 分组标签
  [Tasks]
  [Settings]
─────────────────────────────
```

### 改动文件
- `web/src/components/layout/app-sidebar.tsx` — 引入分组渲染逻辑
- `web/src/components/layout/app-layout.tsx` — 调整 titleMap 和面包屑

### 关键约束
- 分组标签为纯视觉元素（大写灰色文字 + 分隔线），不是可点击的菜单项
- Breakout 不展开二级菜单；内部流程以 Tab/子路由方式在内容区呈现
- 未来可在 TIMING STRATEGY 分组下扩展其他策略（如期货突破、事件驱动等）

---

## Task 2: Breakout 页面信息架构重构

### 目标
保留 iter-A 的聚合 Tab 式详情页优势，同时解决流程割裂和静态展示问题。

### 页面结构

```
/breakout                       → 研究中枢（模型列表 + 快速操作）
/breakout/[runId]               → 单次研究运行详情（Tab 式流程视图）
/breakout/new                   → 发起新研究运行（配置向导）
```

### 研究中枢页面 (`/breakout`)

```
┌─────────────────────────────────────────────────────────────────┐
│ StatCards: 模型总数 | 最新 Rank IC | 最新 Top Hit Rate | 事件总数 │
├─────────────────────────────────────────────────────────────────┤
│ [+ 新建研究运行]  [从配置模板创建]                                  │
├─────────────────────────────────────────────────────────────────┤
│ Research Runs 表格                                               │
│ ┌───────────────────────────────────────────────────────────┐   │
│ │ Run ID | Universe | 事件数 | 特征数 | Rank IC | 状态 | 日期 │   │
│ │ (点击进入详情)                                              │   │
│ └───────────────────────────────────────────────────────────┘   │
│                                                                  │
│ 快速对比：选中2+模型 → [对比评估指标]                              │
└─────────────────────────────────────────────────────────────────┘
```

### 研究运行详情页 (`/breakout/[runId]`) — 流程式 Tab

保留 iter-A 的 Tab 模式，但重新组织 Tab 内容，增强交互性：

| Tab | 内容改进 | 交互增强 |
|-----|---------|---------|
| 概览 | Pipeline DAG 可视化（各阶段状态灯）+ 关键指标汇总 | 阶段可点击跳转 |
| 事件 | 全量事件表（分页+筛选+排序）+ 分布统计卡片 | 按symbol/日期/分数筛选 |
| 标注 | 标注分布直方图 + 成功/失败比例 + 标注样本表 | horizon/threshold 参数展示 |
| 特征 | 特征列表（分九大类折叠）+ 缺失率热力图 + 相关性矩阵 | 特征组开关 |
| 训练 | 训练曲线(loss)、数据分割信息、超参数表 | 重新训练按钮 |
| 评估 | 多维可视化（见 Task 5 详述） | 交互式图表 |
| 模型 | Top Signals + Scores 分布 + 特征重要性 Top20 | 导出/同步按钮 |
| 配置 | 检测器/标注器/训练器全参数 + manifest + 产物清单 | 参数编辑（fork 为新 run） |

---

## Task 3: 增量流水线架构 — 后端重构

### 目标
将当前单体脚本 `run_stock_breakout_research.py` 拆分为可独立执行、可缓存的流水线阶段。

### 流水线阶段与产物

```
Stage 1: detect   → artifacts/breakout/stages/{run_id}/events.parquet
Stage 2: label    → artifacts/breakout/stages/{run_id}/labeled_events.parquet
Stage 3: feature  → artifacts/breakout/stages/{run_id}/feature_panel.parquet
Stage 4: train    → artifacts/breakout/stages/{run_id}/model.pkl + train_log.json
Stage 5: evaluate → artifacts/breakout/stages/{run_id}/metrics.json + eval_detail.json
Stage 6: publish  → artifacts/{model_id}/  (最终发布目录，保持现有格式)
```

### 核心设计

1. **Stage DAG 依赖**：每个 stage 声明输入/输出 hash，变更检测决定是否跳过
2. **参数指纹**：stage 的参数变化 → 重新执行；参数不变 → 复用缓存产物
3. **增量事件检测**：新增 `--incremental` 模式，仅检测最新日期的事件，合并已有事件集
4. **Universe 管理**：统一 universe 配置为独立 JSON，支持 csi300/csi500/custom

### 后端模块拆分

```
src/qlib_research/breakout/       # 新建独立子包（从 core/breakout_stock.py 迁移）
├── __init__.py
├── config.py                     # BreakoutConfig dataclass（含所有阶段参数）
├── detector.py                   # detect_breakouts() — 从 breakout_stock.py 提取
├── labeler.py                    # label_events() — 从 breakout_stock.py 提取
├── features.py                   # 70+ 特征实现（九大类）
├── trainer.py                    # train_model() + 时间分割逻辑
├── evaluator.py                  # evaluate_model() — 扩展评估指标
├── scorer.py                     # score_events()
├── pipeline.py                   # BreakoutPipeline 类 — 编排 stage 执行
└── utils.py                      # 共用工具（ATR、MA 等）
```

### 与 `core/breakout_stock.py` 的关系
- 将现有 404 行代码按职责拆分到上述模块
- `core/breakout_stock.py` 保留为向后兼容的 thin wrapper（import + re-export）
- 新代码全部写入 `breakout/` 子包

---

## Task 4: 特征工程扩展 — 15 → 70+

### 目标
按原始文档实现九大类 70+ 特征，保持向量化计算和无未来函数原则。

### 特征分类实现

| 类别 | 数量 | 代表性特征 | 实现方式 |
|------|------|-----------|---------|
| 突破强度 | 8 | amplitude, body_ratio, upper_shadow, atr_ratio, volume_spike | 向量化 numpy |
| 前高形态 | 6 | days_since_high, high_height, high_vol_ratio, retracement, touch_count | rolling window |
| 盘整蓄力 | 12 | vol_5d/10d/20d/30d, tightness, vol_trend, shrink_ratio, ma_convergence | pandas rolling |
| 量价关系 | 9 | pv_corr, obv_divergence, mfi, vwap_ratio, yin_yang_vol, accumulation | 自定义函数 |
| 趋势动量 | 10 | ma5/10/20/60_slope, rsi6/14, macd, adx | ta-lib 或手算 |
| 市场环境 | 9 | market_return_5d/20d, breadth, market_vol_ratio, market_drawdown | 需要指数数据 |
| 价格动量 | 8 | return_short/mid, max_drawdown, gap, consecutive_up, dist_to_ma | numpy |
| 基本面 | 15 | industry_return, sector_breadth, roe, pe, pb, gross_margin | FDH 基本面 |
| 交互特征 | 3 | amplitude_x_vol, tightness_x_high_age, gap_x_consecutive | 乘积组合 |

### 实现策略
- 每类特征实现为独立函数，接收 event + price_history，返回 dict
- 特征注册表：可配置启用/禁用特征组
- 基本面特征依赖 FDH 提供数据，不可用时优雅降级（填 NaN）
- 市场环境特征需要传入指数数据（沪深300/中证500），在 pipeline 层注入

---

## Task 5: 评估可视化增强

### 目标
提供多维度、可交互的模型评估视图，替代当前仅有 Rank IC + TopK 两个数字的单薄展示。

### 评估指标扩展（后端 `evaluator.py`）

```python
# 现有
rank_ic, top_quantile_mean_return, top_quantile_hit_rate

# 新增
rmse, mae                          # 回归误差
spearman_ic_by_period              # 按月/季分段 IC
top5_return, top10_return, top20_return   # 分组收益
top5_hit, top10_hit, top20_hit    # 分组胜率
train_sharpe, test_sharpe, overfit_ratio  # 过拟合检测
long_short_return                  # 多空收益
coverage_rate                      # 覆盖率（有事件的交易日占比）
turnover                           # 信号换手率
ic_decay                           # IC 衰减（不同 horizon）
```

### 前端图表组件（评估 Tab 内）

| 图表 | 类型 | 数据来源 | 库 |
|------|------|---------|-----|
| 分组收益曲线 | 折线图 | top5/10/20% 分组累计收益 | ECharts |
| 特征重要性 | 横向柱状图 | LightGBM feature_importance | ECharts |
| 预测 vs 实际 | 散点图 | predicted score vs future_return | ECharts |
| IC 时序 | 折线图 | 月度 rank_ic | ECharts |
| 标注分布 | 直方图 | future_return 分布 + 成功阈值线 | ECharts |
| 过拟合仪表盘 | 表格 + 指示灯 | train/test sharpe ratio | 自定义 |
| 分数分布 | 直方图 | signal_score 分布 | ECharts |

### 特征重要性展示位置
放在**评估 Tab** 中（作为"模型解释"子节），而非特征 Tab（特征 Tab 聚焦于输入数据的完整性和分布）。

---

## Task 6: 事件浏览器增强

### 目标
将现有的"Event Sample (50条)"升级为全量事件浏览器，支持筛选、排序和分布统计。

### 设计

```
事件 Tab 内容:
┌─────────────────────────────────────────────────────┐
│ 分布统计卡片行                                        │
│ [总事件数] [成功率] [平均收益] [覆盖股票数] [日均事件]  │
├─────────────────────────────────────────────────────┤
│ 筛选栏:                                              │
│ [Symbol ▼] [日期范围] [评分范围] [标注结果 ▼] [搜索]  │
├─────────────────────────────────────────────────────┤
│ 事件表格（分页，每页50条，支持列排序）                  │
│ 列: symbol | date | score | return_13d | label |    │
│     breakout_strength | volume_ratio | prev_high    │
├─────────────────────────────────────────────────────┤
│ 底部: 分布直方图（突破强度分布 / 收益分布 / 量比分布） │
└─────────────────────────────────────────────────────┘
```

### 后端支持
- 新增 API: `GET /api/breakout/{run_id}/events?page=&size=&symbol=&date_from=&date_to=&sort_by=&order=`
- 新增 API: `GET /api/breakout/{run_id}/events/stats` — 返回分布统计数据
- 使用流式读取 parquet（避免加载全量 8GB CSV）

---

## Task 7: 训练管理与前端触发

### 目标
支持用户在前端配置参数并发起训练任务，复用已有 task_dispatcher。

### 新建研究运行页面 (`/breakout/new`)

```
配置向导（单页表单，分区折叠）:

[数据源配置]
  - Universe: [CSI300 ▼] / [CSI500 ▼] / [自定义列表]
  - 日期范围: [2020-01-01] ~ [2026-06-24]
  - 复权方式: [前复权 ▼]
  - 增量模式: [□ 基于已有事件集追加新数据]

[检测参数]
  - Lookback Window: [60]
  - Min Consolidation: [20]
  - Breakout PCT: [0.0]
  - Min Volume Ratio: [1.0]

[标注参数]
  - Horizon Days: [13]
  - Success Threshold: [0.03]
  - Target Type: [return ▼]

[训练参数]
  - Train End Date: [2025-08-31]
  - Valid End Date: [2026-01-31]
  - Num Leaves: [63]
  - Learning Rate: [0.03]
  - Early Stopping Rounds: [30]

[特征配置]
  - 特征组: [✓突破强度] [✓前高形态] [✓盘整蓄力] [✓量价关系]
           [✓趋势动量] [✓市场环境] [✓价格动量] [□基本面] [✓交互]

[提交] → 创建 Task → 进入运行详情页监控进度
```

### 后端 API
- `POST /api/breakout/runs` — 创建研究运行（参数 → 写入配置 → 调度 task）
- `GET /api/breakout/runs/{run_id}/status` — 流水线执行状态
- `POST /api/breakout/runs/{run_id}/retrain` — 仅重新训练（复用已有 events+features）

### 与 Task 系统集成
- 复用现有 `task_dispatcher` + `task_worker` 架构
- 训练任务类型: `breakout_research_run`
- 状态流转: `pending → detecting → labeling → featuring → training → evaluating → publishing → completed`

---

## Task 8: 配置管理与模型流程化

### 目标
- 将配置管理内嵌在研究运行流程中（而非独立页面）
- 模型切换通过"激活"机制实现，集成在研究中枢

### 配置持久化

```
artifacts/breakout/config/
├── templates/                    # 配置模板（可复用）
│   ├── default_csi300.json
│   └── default_csi500.json
├── active_model.json             # 当前激活模型的指向
└── runs/                         # 每次运行的完整配置快照
    └── {run_id}.json
```

### 模型激活机制（研究中枢页面）
- Runs 表格每行增加 "激活" 按钮
- 当前激活模型有视觉标记（绿色 badge）
- 激活操作: 更新 `active_model.json` + 触发 signal 同步到 ValueInvesting
- API: `POST /api/breakout/runs/{run_id}/activate`

### 配置 Tab 内的编辑功能
- "Fork 为新运行"：基于当前配置修改参数后创建新 Run（保证历史不可变）
- 不允许就地修改已完成的 Run 配置

---

## Task 9: 后端 API 全量设计

### 新增 API 端点

| 方法 | 端点 | 功能 |
|------|------|------|
| GET | `/api/breakout/runs` | 研究运行列表（替代现有 /api/breakout） |
| GET | `/api/breakout/runs/{run_id}` | 运行详情 |
| POST | `/api/breakout/runs` | 创建新研究运行 |
| POST | `/api/breakout/runs/{run_id}/activate` | 激活模型 |
| POST | `/api/breakout/runs/{run_id}/retrain` | 仅重训练 |
| GET | `/api/breakout/runs/{run_id}/status` | 流水线状态 |
| GET | `/api/breakout/runs/{run_id}/events` | 事件列表（分页+筛选） |
| GET | `/api/breakout/runs/{run_id}/events/stats` | 事件分布统计 |
| GET | `/api/breakout/runs/{run_id}/features/stats` | 特征统计 |
| GET | `/api/breakout/runs/{run_id}/evaluation` | 评估详情（含图表数据） |
| GET | `/api/breakout/runs/{run_id}/importance` | 特征重要性 |
| GET | `/api/breakout/active` | 当前激活模型信息 |
| GET | `/api/breakout/templates` | 配置模板列表 |
| POST | `/api/breakout/runs/{run_id}/sync` | 同步到 ValueInvesting |

### 新增后端文件
- `src/qlib_research/app/breakout_router.py` — 独立 router
- `src/qlib_research/app/breakout_services.py` — 业务逻辑
- `src/qlib_research/app/breakout_contracts.py` — Pydantic 模型

---

## Task 10: 产物格式优化

### 问题
当前 events_scored.csv (9GB) 和 feature_panel.csv (8.6GB) 过大，加载缓慢。

### 改进
1. **格式升级**: CSV → Parquet（压缩后通常减少 5-10x）
2. **分阶段存储**: 每个 stage 独立产物文件，避免单文件过大
3. **索引优化**: events 按 (date, symbol) 建立 partition，支持 range 查询
4. **向后兼容**: 最终 publish 阶段仍输出 scores.csv + signals.csv（供 ValueInvesting 消费）

### 产物目录结构

```
artifacts/breakout/
├── stages/                       # 流水线中间产物（可缓存）
│   └── {run_id}/
│       ├── config.json           # 本次运行的完整参数快照
│       ├── events.parquet        # Stage 1 输出
│       ├── labeled_events.parquet # Stage 2 输出
│       ├── feature_panel.parquet # Stage 3 输出
│       ├── model.pkl             # Stage 4 输出
│       ├── train_log.json        # Stage 4 附属
│       ├── metrics.json          # Stage 5 输出
│       ├── eval_detail.json      # Stage 5 详细数据（含图表用数据）
│       └── feature_importance.json # Stage 5 附属
├── published/                    # 发布版本（供外部消费）
│   └── {model_id}/
│       ├── manifest.json
│       ├── scores.csv
│       ├── signals.csv
│       ├── metrics.json
│       └── model.pkl
├── config/
│   ├── templates/
│   └── active_model.json
```

---

## 实施顺序与依赖

```
Task 3 (后端模块拆分)  ─┐
Task 4 (特征扩展)      ─┼─→ Task 7 (训练管理) ─→ Task 10 (产物优化)
                        │
Task 1 (导航重构)      ─┼─→ Task 2 (页面架构) ─→ Task 6 (事件浏览器)
                        │                      ─→ Task 5 (评估可视化)
                        │                      ─→ Task 8 (配置管理)
                        │
Task 9 (API 设计)      ─┘   (贯穿，随各 Task 逐步实现)
```

建议分 3 个迭代交付：
- **Iter 1**: Task 1 + Task 3 + Task 9 (基础架构) — 导航重构 + 后端模块拆分 + API 骨架
- **Iter 2**: Task 4 + Task 2 + Task 6 + Task 7 — 特征扩展 + 页面重构 + 事件浏览 + 训练管理
- **Iter 3**: Task 5 + Task 8 + Task 10 — 评估可视化 + 配置管理 + 产物优化

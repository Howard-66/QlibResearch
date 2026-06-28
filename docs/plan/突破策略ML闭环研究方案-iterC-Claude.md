# 突破策略 ML 全栈闭环研究方案 - Iter C - Claude

## 决策结论：复用 QlibResearch，新增 breakout 子包 + 双端 UI

### 为什么选择扩展 QlibResearch 而非新建项目

| 维度 | 复用 QlibResearch | 新建项目 |
|------|-------------------|----------|
| 技术栈 | LightGBM/pandas/numpy 已存在，零新增核心依赖 | 需重新搭建相同依赖 |
| 数据源 | FinanceDataHub 已集成（editable dep） | 需再配一份 |
| 交付机制 | `io/sync.py` + `scripts/sync_to_valueinvesting.py` 直接复用 | 需重写文件同步逻辑 |
| 任务系统 | 已有 task_dispatcher + task_worker + 前端 tasks 页面 | 需从零搭建 |
| Web 框架 | FastAPI (8010) + Next.js (3010) 已就绪 | 需重建完整前后端 |
| 维护成本 | 一个 .venv、一套 CI、一个 test suite | 额外项目开销 |

风险缓释：通过独立子包 (`breakout/`) 与现有 `core/`（多因子截面）严格隔离，互不干扰。

---

## 系统架构全景

```
                    QlibResearch (研究平台)
┌─────────────────────────────────────────────────────────────┐
│  Frontend (Next.js :3010)          Backend (FastAPI :8010)   │
│  ┌─────────────────────┐          ┌──────────────────────┐  │
│  │ /breakout            │◄────────►│ /api/breakout/*      │  │
│  │   - 研究仪表盘       │          │   - 训练/评估/配置   │  │
│  │   - 训练触发+监控    │          │   - 事件检索         │  │
│  │   - 特征重要性       │          │   - 模型版本管理     │  │
│  │   - 评估对比         │          │                      │  │
│  │   - 标注参数管理     │          │ task_dispatcher      │  │
│  │   - 模型版本管理     │          │   (已有,复用)        │  │
│  └─────────────────────┘          └──────────────────────┘  │
│                                            │                 │
│  src/qlib_research/breakout/              │ 文件产物         │
│    detector → labeler → features          ▼                 │
│    → trainer → scorer → evaluator    artifacts/breakout/     │
└────────────────────────────────────────────┼─────────────────┘
                                             │
                      sync (文件交付)          │
                                             ▼
                    ValueInvesting (消费平台)
┌─────────────────────────────────────────────────────────────┐
│  Frontend (Next.js :3000)          Backend (FastAPI :8000)   │
│  ┌─────────────────────┐          ┌──────────────────────┐  │
│  │ /screener (扩展)     │◄────────►│ /api/breakout/*      │  │
│  │ 或 /breakout (新页)  │          │   - 读取信号文件     │  │
│  │   - 候选列表+评分    │          │   - 历史命中率       │  │
│  │   - K线突破标注      │          │   - 信号聚合         │  │
│  │   - 信号历史追踪     │          │                      │  │
│  │   - 命中率统计       │          │                      │  │
│  └─────────────────────┘          └──────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

---

## Part A: 后端研究引擎 (QlibResearch)

### 目录结构

```
src/qlib_research/
├── core/                          # 现有：多因子截面周频研究（不改动）
├── breakout/                      # 新增：突破策略 ML 研究引擎
│   ├── __init__.py
│   ├── detector.py                # 突破事件检测（日线/周线）
│   ├── labeler.py                 # 自动标注（可配窗口期、阈值）
│   ├── features.py                # 70+ 特征工程（九大类）
│   ├── trainer.py                 # LightGBM 训练管道
│   ├── scorer.py                  # 推理评分器
│   ├── evaluator.py               # 评估体系（Spearman/TopK/胜率）
│   ├── config.py                  # 策略参数配置（dataclass）
│   └── utils.py                   # 公共工具（ATR计算等）
├── app/                           # 现有 FastAPI 应用（扩展）
│   ├── main.py                    # 扩展：注册 breakout router
│   ├── breakout_router.py         # 新增：突破策略 API 路由
│   ├── breakout_services.py       # 新增：突破策略业务逻辑
│   ├── breakout_contracts.py      # 新增：请求/响应 Pydantic 模型
│   ├── services.py                # 现有（不改动）
│   └── task_dispatcher.py         # 现有（复用任务调度）
└── io/                            # 现有：产物 IO（扩展）
    ├── sync.py                    # 扩展：支持 breakout 产物同步
    └── artifacts.py               # 扩展：支持 breakout 产物类型
```

### 新增脚本

```
scripts/
├── run_breakout_research.py       # CLI: 端到端研究（检测→标注→特征→训练→评估）
├── score_breakout_candidates.py   # CLI: 增量评分当日突破候选
└── sync_breakout_signals.py       # CLI: 同步突破信号到 ValueInvesting
```

### 产物目录

```
artifacts/breakout/
├── models/                        # 训练好的模型
│   ├── breakout_daily_v1.pkl
│   └── breakout_weekly_v1.pkl
├── events/                        # 检测到的历史事件
│   └── events_20260624.parquet
├── evaluations/                   # 评估报告
│   └── eval_daily_v1.json
└── signals/                       # 输出信号（待同步到 ValueInvesting）
    ├── scores_breakout.csv
    └── manifest_breakout.json
```

---

## Part B: QlibResearch 前端 — 研究仪表盘（全交互）

### 新增页面结构

```
web/src/app/breakout/
├── page.tsx                       # 研究概览仪表盘
├── train/page.tsx                 # 训练管理（配置+触发+监控）
├── evaluate/page.tsx              # 评估结果对比
├── events/page.tsx                # 历史事件浏览器
├── models/page.tsx                # 模型版本管理
└── config/page.tsx                # 标注/检测参数配置
```

### 页面功能详述

**1. 研究概览 (`/breakout`)**
- 当前最佳模型摘要（Spearman、TopK 胜率、样本数）
- 最近一次训练/评分时间
- 信号统计：今日/本周新突破事件数、评分分布
- 快捷操作入口

**2. 训练管理 (`/breakout/train`)**
- 参数配置面板：
  - 检测参数：lookback 窗口、最短盘整期、频率选择（日线/周线）
  - 标注参数：horizon_days、success_threshold、target_type
  - 模型参数：num_leaves、learning_rate、early_stopping_rounds
  - 数据分割：train_end、valid_end
- 一键触发训练（调用任务系统）
- 训练进度实时展示（复用已有 task 监控）
- 训练日志流式查看

**3. 评估对比 (`/breakout/evaluate`)**
- 多模型版本对比表格（Spearman、RMSE、TopK 收益、胜率）
- 特征重要性 Top-N 柱状图
- 过拟合检测：train vs test Sharpe 比值
- 预测值 vs 实际值散点图
- 分组收益曲线（Top 5%/10%/20% 分组）

**4. 事件浏览器 (`/breakout/events`)**
- 时间范围筛选 + 股票筛选
- 事件列表：symbol、日期、评分、实际收益、是否命中
- 点击事件 → 弹出 K 线图，标注突破点 + 前高 + 后续走势
- 支持按评分/收益/日期排序

**5. 模型版本管理 (`/breakout/models`)**
- 模型列表（版本号、训练日期、关键指标）
- 激活/停用模型
- 删除旧版本
- 一键同步到 ValueInvesting

**6. 参数配置 (`/breakout/config`)**
- 检测规则可视化编辑（突破条件定义）
- 标注策略参数调整
- 特征开关（启用/禁用特征组）
- 配置保存为 JSON 持久化到 `artifacts/breakout/config/`

### API 端点设计 (`breakout_router.py`)

```python
# 概览
GET  /api/breakout/overview          # 研究概览数据

# 事件
GET  /api/breakout/events            # 历史事件列表（分页+筛选）
GET  /api/breakout/events/{id}       # 事件详情（含K线数据）

# 训练
POST /api/breakout/train             # 触发训练任务
GET  /api/breakout/train/status      # 训练状态

# 评估
GET  /api/breakout/evaluations       # 评估结果列表
GET  /api/breakout/evaluations/{id}  # 单次评估详情

# 模型
GET  /api/breakout/models            # 模型列表
POST /api/breakout/models/{id}/activate  # 激活模型
POST /api/breakout/models/{id}/sync  # 同步到 ValueInvesting

# 评分
POST /api/breakout/score             # 对当前候选评分
GET  /api/breakout/signals/latest    # 最新信号

# 配置
GET  /api/breakout/config            # 获取当前配置
PUT  /api/breakout/config            # 更新配置
```

---

## Part C: ValueInvesting 前端 — 信号消费视图

### 新增/扩展页面

```
ValueInvesting/web/src/app/
├── breakout/                      # 新增页面
│   └── page.tsx                   # 突破信号主视图
```

### 功能设计

**突破信号页面 (`/breakout`)**

- **候选评分列表**：当日/本周突破候选，按 ML 评分降序排列
  - 列：股票代码、名称、评分、突破价、前高日期、频率
  - 点击 → 跳转到 analysis/[ticker] 页面（已有）

- **K 线突破标注**（扩展 analysis/[ticker] 页面）：
  - 在现有 TradingView 图表上叠加突破事件标记
  - 标注前高位置、突破点、评分标签
  - 历史突破事件回溯显示

- **信号绩效追踪**：
  - 历史信号命中率统计（7天/14天/30天）
  - 按评分分组的实际收益分布
  - 月度/季度绩效趋势

### ValueInvesting 后端扩展

```python
# ValueInvesting/src/api/ 新增
GET  /api/breakout/signals          # 读取同步过来的信号文件
GET  /api/breakout/performance      # 信号历史绩效
GET  /api/breakout/chart-markers/{ticker}  # 特定股票的突破标记数据
```

---

## Part D: 研究引擎核心模块设计

### D1: 突破事件检测器 (`breakout/detector.py`)

```python
@dataclass
class BreakoutEvent:
    symbol: str
    date: pd.Timestamp
    frequency: str           # "daily" | "weekly"
    breakout_price: float
    prev_high_price: float
    prev_high_date: pd.Timestamp
    volume_ratio: float

def detect_breakouts(
    bars: pd.DataFrame,
    lookback: int = 120,
    min_consolidation: int = 10,
    frequency: str = "daily",
) -> list[BreakoutEvent]: ...
```

### D2: 自动标注器 (`breakout/labeler.py`)

```python
@dataclass
class LabelConfig:
    horizon_days: int = 13
    success_threshold: float = 0.03
    target_type: str = "return"  # "return" | "binary"

def label_events(
    events: list[BreakoutEvent],
    bars_dict: dict[str, pd.DataFrame],
    config: LabelConfig,
) -> pd.DataFrame: ...
```

### D3: 特征工程 (`breakout/features.py`)

九大类 70+ 特征（与文档一致）：
1. 突破强度（8）2. 前高形态（6）3. 盘整蓄力（12）4. 量价关系（9）5. 趋势动量（10）6. 市场环境（9）7. 价格动量（8）8. 基本面（15）9. 交互特征（3）

所有特征仅用事件日及之前数据计算，纯 Python + NumPy + Pandas。

### D4: 训练管道 (`breakout/trainer.py`)

```python
@dataclass
class TrainConfig:
    train_end: str = "2025-08-31"
    valid_end: str = "2026-01-31"
    num_leaves: int = 63
    learning_rate: float = 0.03
    early_stopping_rounds: int = 30

@dataclass
class TrainResult:
    model_path: Path
    feature_importance: pd.DataFrame
    metrics: dict[str, float]
    train_samples: int
    valid_samples: int
```

### D5: 评分器 + 评估器

- `scorer.py`：加载模型，对候选评分排序
- `evaluator.py`：Spearman 秩相关、TopK 收益/胜率、过拟合比值

---

## 数据流闭环

```
FinanceDataHub (日/周 K 线 + 基本面)
        │
        ▼
  detector.py ──→ 突破事件列表
        │
        ▼
  labeler.py ──→ 带标签的事件集
        │
        ▼
  features.py ──→ 特征矩阵 (N x 70+)
        │
        ├──→ trainer.py ──→ 模型 (.pkl) + 评估报告
        │                       │
        │                       ▼
        │              artifacts/breakout/models/
        │
        └──→ scorer.py ──→ scores_breakout.csv
                                │
                                ▼
                sync_breakout_signals.py
                                │
                                ▼
              ValueInvesting/data/qlib_artifacts/breakout/
```

---

## 交付产物格式

### `scores_breakout.csv`
```csv
date,symbol,score,breakout_price,prev_high_date,frequency
2026-06-23,000001.SZ,0.087,15.82,2026-05-10,daily
2026-06-23,600519.SH,0.052,1850.0,2026-04-22,daily
```

### `manifest_breakout.json`
```json
{
  "model_type": "breakout_lgbm",
  "frequency": "daily",
  "model_version": "v1",
  "train_end": "2025-08-31",
  "features_count": 70,
  "spearman": 0.31,
  "top10_winrate": 0.68,
  "generated_at": "2026-06-23T15:30:00"
}
```

---

## 技术栈影响

**零新增核心依赖** — LightGBM、pandas、numpy、scipy、FastAPI 均已存在。

前端沿用现有栈：Next.js + TypeScript + Tailwind + shadcn/ui + ECharts/Plotly。

---

## 实施节奏（8 个 Task）

### Task 1: 基础框架搭建
- 创建 `breakout/` 包结构 + 核心 dataclass
- 创建 `artifacts/breakout/` 目录结构
- 实现 `breakout/config.py` 参数管理

### Task 2: 事件检测 + 自动标注
- 实现 `detector.py`（日线 + 周线）
- 实现 `labeler.py`（可配窗口期 + 阈值）
- 单元测试：无未来函数泄露验证

### Task 3: 特征工程
- 按九大类逐步实现 70+ 特征
- 特征完整性检查 + 缺失值统计
- 性能优化（向量化计算）

### Task 4: 训练管道 + 评估
- `trainer.py`：时间分割 + LightGBM + 早停
- `evaluator.py`：Spearman + TopK + 过拟合检测
- `scorer.py`：推理评分

### Task 5: CLI 脚本 + 文件交付
- `scripts/run_breakout_research.py`
- `scripts/score_breakout_candidates.py`
- 扩展 `io/sync.py` + `scripts/sync_breakout_signals.py`

### Task 6: QlibResearch 后端 API
- `breakout_router.py`：完整 REST API
- `breakout_services.py`：业务逻辑层
- `breakout_contracts.py`：Pydantic 模型
- 集成到 `main.py`（注册 router）
- 复用现有 task_dispatcher 执行训练任务

### Task 7: QlibResearch 前端 — 研究仪表盘
- 6 个页面：概览/训练/评估/事件/模型/配置
- API 对接 + 实时状态更新
- ECharts 图表：特征重要性、收益分组、散点图
- K 线事件标注组件

### Task 8: ValueInvesting 前端 — 信号消费
- 新增 `/breakout` 页面：候选列表 + 绩效追踪
- 扩展 analysis/[ticker]：K 线突破标记叠加
- 后端 API：信号读取 + 绩效统计

---

## 与现有"多因子截面研究"的并行关系

| 维度 | 多因子截面 (core/) | 突破策略 (breakout/) |
|------|-------------------|---------------------|
| 触发方式 | 周频定时全市场评估 | 事件驱动（有突破才评分） |
| 研究对象 | 全市场 3000+ 股票排序 | 突破候选池 5-15 只 |
| 特征来源 | Qlib DataHandler 标准因子 | 自定义 70+ 突破相关特征 |
| 模型 | Qlib 框架内 LightGBM | 独立 LightGBM（不依赖 Qlib） |
| 输出 | scores.csv（全市场排序） | scores_breakout.csv（候选评分） |
| 前端 | /runs, /panels, /compare | /breakout/* |
| 频率 | 周频 | 日频 + 周频 |

两条研究线完全独立运行，共享基础设施（数据、任务系统、同步机制、前端框架）但研究逻辑互不依赖。
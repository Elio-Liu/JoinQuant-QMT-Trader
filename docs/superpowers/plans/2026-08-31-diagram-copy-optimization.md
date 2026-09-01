# 两张交易流程图文案优化 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将交易端执行图和策略逻辑图中的内部变量名、函数名与状态常量改写成自然、准确的中文业务表达，同时保留必要的排障标识。

**Architecture:** 直接更新两份 Excalidraw JSON 中的文字元素，不改变流程结构、节点顺序和业务语义。改写后用本地 `@excalidraw/excalidraw` 导出 SVG，并通过 Playwright 截图检查文字溢出、节点遮挡和箭头穿框。

**Tech Stack:** Excalidraw JSON、Python 3、`@excalidraw/excalidraw` 本地渲染、Playwright 截图

## Global Constraints

- 节点标题使用中文业务语义，正文按照“触发条件 → 执行动作 → 最终结果”组织。
- 关键代码标识只在首次出现时括号保留；普通配置键直接改写为含义。
- Redis、QMT、SQLite、ACK、XACK 等通用系统或协议名保留。
- 不修改交易代码、配置、数据模型、执行语义、颜色体系和分区结构。
- 不在 `docs/diagrams` 新增 PNG 或 SVG；截图只保存到本次任务的可视化目录。

---

### Task 1: 优化交易端执行图文案

**Files:**
- Modify: `docs/diagrams/execution-pipeline.excalidraw`

**Interfaces:**
- Consumes: 设计说明中的“中文业务语义优先、关键代码标识首次括注”规则。
- Produces: 保持原有元素 ID 和布局、但正文自然化的交易端执行图。

- [ ] **Step 1: 建立文字元素替换表**

覆盖所有直接暴露内部实现的节点，至少包括：

```text
StreamMessage → 收到的消息
candidate_plan → 候选计划消息（candidate_plan，仅首次保留）
OpeningSellBarrier → 开盘卖出屏障
wave1_cash_baseline → 第一波买入资金基线
fixed_budget → 固定预算买入
durable terminal → 已持久化的明确终态
calculate_order_price → 订单定价
opening_price_gap_wait_pct → 开盘挂单价格偏离阈值
prefer_book=True → 下一次报价直接锚定实时盘口
request cancel ≠ 已完成 → 发出撤单请求不等于撤单完成
```

- [ ] **Step 2: 更新 Excalidraw 文字元素**

使用 UTF-8 读取 JSON，按元素 ID 更新 `text` 与 `originalText`；保留 `fontFamily: 5`、元素 ID、坐标、颜色和箭头数据。若中文变长，仅增加换行或缩短同义句。

- [ ] **Step 3: 运行结构校验**

Run:

```bash
python - <<'PY'
import json
from pathlib import Path
p = Path('docs/diagrams/execution-pipeline.excalidraw')
d = json.loads(p.read_text(encoding='utf-8'))
ids = [e['id'] for e in d['elements']]
assert len(ids) == len(set(ids))
assert all(e.get('fontFamily') == 5 for e in d['elements'] if e['type'] == 'text')
print('execution diagram structure: ok')
PY
```

Expected: `execution diagram structure: ok`

### Task 2: 优化策略逻辑图文案

**Files:**
- Modify: `docs/diagrams/strategy-pipeline.excalidraw`

**Interfaces:**
- Consumes: 与交易端执行图相同的文案规则。
- Produces: 保持从上至下选股与风控流程不变、正文自然化的策略逻辑图。

- [ ] **Step 1: 改写策略内部变量与函数名**

按以下语义更新对应文字元素：

```text
g.target_list → 当日候选股票池
selection_meta → 选股过程记录
gap → 开盘涨幅
tick → 逐笔行情
handle_data → 分钟级风控检查
closeable_amount > 0 → 可卖数量大于零的隔夜持仓
```

- [ ] **Step 2: 复核选股与风控表达**

确认选股流程仍依次表达各层过滤、排序截断与候选池输出；风控仍表达开盘退出、盘中见顶退出、固定止损、早盘退出和尾盘退出，不引入交易端 Redis 或报单细节。

- [ ] **Step 3: 运行结构校验**

Run:

```bash
python - <<'PY'
import json
from pathlib import Path
p = Path('docs/diagrams/strategy-pipeline.excalidraw')
d = json.loads(p.read_text(encoding='utf-8'))
ids = [e['id'] for e in d['elements']]
assert len(ids) == len(set(ids))
assert all(e.get('fontFamily') == 5 for e in d['elements'] if e['type'] == 'text')
print('strategy diagram structure: ok')
PY
```

Expected: `strategy diagram structure: ok`

### Task 3: 本地渲染与视觉验收

**Files:**
- Verify: `docs/diagrams/execution-pipeline.excalidraw`
- Verify: `docs/diagrams/strategy-pipeline.excalidraw`
- Create outside repository: `/Users/elio/.codex/visualizations/2026/08/31/01a055f7-f1e4-70c3-a14f-8b115268150b/execution-pipeline-copy-preview.png`
- Create outside repository: `/Users/elio/.codex/visualizations/2026/08/31/01a055f7-f1e4-70c3-a14f-8b115268150b/strategy-copy-preview.png`

**Interfaces:**
- Consumes: 两份更新后的 Excalidraw JSON。
- Produces: 两张纯本地渲染截图和结构化验收结果。

- [ ] **Step 1: 使用本地 Excalidraw 渲染器分别导出预览**

启动本地 Vite 页面，通过 `exportToSvg()` 载入每个 `.excalidraw` 文件；不向公开 Excalidraw 站点上传文件。

- [ ] **Step 2: 分别截图全图与重点区域**

交易端重点检查消息分流、线程调度、订单执行与恢复区；策略图重点检查选股漏斗、买入约束和分钟级风控区。

- [ ] **Step 3: 运行文字与走线校验**

检查以下条件并要求全部为空：

```text
horizontal_text_overflow=[]
vertical_text_overflow=[]
arrow_box_interior_hits=[]
```

- [ ] **Step 4: 最终范围检查**

Run:

```bash
git status --short --ignored docs/diagrams docs/superpowers
```

Expected: 两份 `.excalidraw` 位于被忽略的 `docs/diagrams`，计划与设计文档之外没有新增受跟踪文件；现有用户改动保持不变。

### Task 4: 修正节点文字垂直位置与字号层级

**Files:**
- Modify: `docs/diagrams/execution-pipeline.excalidraw`
- Modify: `docs/diagrams/strategy-pipeline.excalidraw`

**Interfaces:**
- Consumes: 每个文字元素的 `containerId`、所属图形的宽高和实际文本行数。
- Produces: 真实渲染时垂直居中、稀疏节点字号更大的两份图文件。

- [ ] **Step 1: 复现并确认根因**

检查节点文字元素的几何高度。若文字元素高度接近整个节点高度，即使 `verticalAlign: middle`，SVG 导出仍会从文字元素顶部开始绘制各行，形成视觉靠上。

- [ ] **Step 2: 按容器容量计算字号**

根据最长行宽、文本行数、节点可用宽高计算字号上限。内容较少的节点目标字号为 18–22px；密集说明节点目标字号为 15–17px；字号不得造成横向或纵向溢出。

- [ ] **Step 3: 按真实行高重建文字几何框**

对每个带 `containerId` 的文字元素设置：

```text
height = 行数 × fontSize × lineHeight
y = container.y + (container.height - height) / 2
verticalAlign = middle
autoResize = false
```

横向保持居中，文字宽度保留容器两侧安全边距。

- [ ] **Step 4: 本地渲染并验收中心偏差**

两张图分别截图，检查所有节点文字块中心与节点中心一致；结构校验要求：

```text
max_vertical_center_delta <= 0.01
horizontal_text_overflow=[]
vertical_text_overflow=[]
arrow_box_interior_hits=[]
```

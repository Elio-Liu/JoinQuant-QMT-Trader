# 交易端执行 Pipeline 文案优化设计

## 目标

优化 `docs/diagrams/execution-pipeline.excalidraw` 与 `docs/diagrams/strategy-pipeline.excalidraw` 的阅读体验：读者先看到业务动作、判断与结果，不需要先理解 Python 变量名、配置键或内部类名；同时保留排障时确实需要对应代码的少量技术标识。

## 文案原则

1. 节点标题使用中文业务语义，例如“开盘卖出屏障”“候选计划入库”“价格拒单恢复”。
2. 正文按照“触发条件 → 执行动作 → 最终结果”组织，避免函数名和变量名连续堆叠。
3. 关键代码标识只在首次出现时放入括号，例如“候选计划消息（`candidate_plan`）”；后续统一使用中文简称。
4. Redis、QMT、SQLite、ACK、XACK 等通用系统或协议名保留，不做生硬直译。
5. `RECOVERY_REQUIRED`、`QUEUED_LIMIT_UP` 等具备运维定位价值的状态，在中文状态后括号保留；普通配置键改写为其含义。
6. 一行只表达一个意思，优先使用短句；不新增业务逻辑，不改变现有流程顺序、颜色体系和分区结构。
7. 所有图形节点内的文字块必须按真实行高计算高度，并在容器内做几何垂直居中；不能只依赖 `verticalAlign` 属性。
8. 字号按节点可用宽高自适应：内容较少的节点优先放大到 18–22px，信息密集节点保持 15–17px，以不溢出为上限。

## 典型改写

| 原表达 | 优化后 |
| --- | --- |
| `OpeningSellBarrier` | 开盘卖出屏障 |
| `wave1_cash_baseline` | 第一波买入资金基线 |
| `opening_price_gap_wait_pct` | 开盘挂单价格偏离阈值 |
| `durable terminal` | 已持久化的明确终态 |
| `QUEUED_* future` | 排队订单的后台跟踪任务 |
| `request cancel ≠ 已完成` | 发出撤单请求不等于撤单完成 |
| `prefer_book=True` | 下一次报价直接锚定实时盘口 |
| `g.target_list` | 当日候选股票池 |
| `selection_meta` | 选股过程记录 |
| `closeable_amount > 0` | 仅检查可卖数量大于零的隔夜持仓 |

## 修改范围

- 仅修改 `execution-pipeline.excalidraw` 与 `strategy-pipeline.excalidraw` 的文字内容和必要的换行。
- 若中文改写导致文字溢出，可局部调整文本行数或节点高度，但不重排整体架构。
- 可调整节点内文字元素的 `x`、`y`、`width`、`height`、`fontSize` 与 `autoResize`，确保真实渲染结果垂直居中且视觉饱满。
- 不修改交易代码、配置、数据模型、执行语义或其他图文件。

## 验收标准

- 节点标题不再直接使用内部函数、类或配置键作为主标题。
- 普通读者只阅读中文即可理解主流程；关键状态仍能对应日志和代码。
- Excalidraw JSON 可解析，元素 ID 唯一，所有文字使用 Excalifont。
- 本地渲染截图中无文字溢出、节点遮挡或箭头穿框。
- 本地渲染后的文字块中心与所属节点中心重合，稀疏节点字号明显大于密集说明节点。

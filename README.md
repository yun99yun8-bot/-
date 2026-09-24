# TRON Monitor V9.0 — Research AI

基于 V8.1.6 稳定采集/Worker/遗漏引擎，重建 AI 决策层。

## V9 核心
- 严格只使用开奖前可见的 1–17 组与此前正式开奖结果。
- 修正旧模型历史序列方向：历史数据为“最新→最旧”，近期窗口、遗漏与转移模型均按正确方向计算。
- 同时运行 long / short / structure17 / transition / omission / binomial 六个候选模型。
- 候选模型权重由过去已锁定且已验证的样本外成绩自动调整，并对小样本做收缩。
- Binomial(7,0.5) 作为简单基准，避免把单3/单4天然高频误认为 AI 规律。
- 每期预测仍在 G20 前持久化锁定；G20 只负责验证，不能反向修改预测。
- ensemble_detail 中保存 V9 权重、候选组件和 rolling-OOS 研究成绩，可继续审计。
- `edgeStatus` 会标记 NO_EDGE / WEAK_EDGE / EVIDENCE；它只是统计证据等级，不代表保证命中。

## 保留
V8.1.6 的 G17 事件锁定、G20 屏障、DB-first、自修复、独立遗漏引擎和 Render Background Worker 架构均保留。

## 部署
Web 与 Background Worker 继续使用同一仓库和 DATABASE_URL。Worker Start Command: `python worker.py`。


## V9.1 多维AI决策中心
- 首页重构为最终综合结论、Top3、多模型预判、真实样本外命中反馈和基准比较。
- 候选模型扩展为17组结构、哈希结构、位置关联、短期、长期、状态转移、遗漏条件、Binomial基础分布。
- 哈希结构只作为开奖前统计特征，不宣称存在可预测公式；只有真实样本外成绩才能提高权重。
- 最近20/100/500期Top1实时反馈仅使用开奖前已锁定并已开奖验证的记录。
- 保留V9.0/V8.1.6的G17/G20生命周期、数据库与遗漏引擎。

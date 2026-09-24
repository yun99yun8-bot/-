Fix7：在保持 Fix6 开奖结果和区块映射不变的基础上，修复“20组区块数据”表。

- 当前期固定展示第1组到第20组，共20行。
- 第20组仍然是正式开奖结果。
- 单0～单7统计仍只使用第1～18组。
- 当前区块映射仍以 2026-09-24：1001期=86522304、每期+20 为准。
- 不修改遗漏统计和开奖结果逻辑。

Fix9 changes:
- 20-row table is always rendered as a fixed frame.
- Missing rows are filled incrementally: up to 4 missing groups per poll.
- Group 20 is prioritized so the official result can appear without waiting for groups 1-19.
- Block requests are parallelized within the small batch.
- Network timeout reduced from 10s to 4s; failed rows remain -- and retry later.
- Frontend refresh interval reduced from 15s to 3s.
- Removed the redundant blocking second fetch for group 20.
- Existing result, omission, and statistics rules are preserved.


DB3 realtime architecture:
- Server background collector checks TRON every 1 second and saves each new block once to PostgreSQL.
- PostgreSQL block_number is the primary key, preventing duplicate rows.
- Rows older than 3 days are deleted hourly.
- /api/draw reads PostgreSQL only; the browser does not query TRON.
- Frontend refresh interval is 1 second.
- /api/history exposes retained saved rows for later history UI work.
- Requires Render environment variable DATABASE_URL (Internal Database URL).

DB stability update:
- Reuses a small thread-safe PostgreSQL connection pool (1-4 connections).
- Retries transient connection acquisition failures.
- /api/draw keeps the last successful payload in memory and returns it with databaseStatus=reconnecting instead of blanking the frontend during a short DB timeout.
- Existing 3-day retention, TRON collector, calculation rules, and 5-second new-group highlight are unchanged.

AI statistical analysis module:
- Adds AI分析 next to the current statistical conclusion.
- Uses a deterministic weighted statistical model over current groups 1-18 and retained historical official results; it is a statistical tendency, not a guaranteed prediction.
- Freezes each prediction before group 20 is available and verifies it after group 20, so hit-rate tracking is not rewritten after the outcome.
- Shows which groups 1-19 have the same 单X result as group 20.
- Accuracy begins accumulating after this version is deployed; prior periods are used as historical features but are not falsely counted as pre-outcome predictions.

本版整理：
- 开奖结果按期锁定：同一期7个号码/单几/区块只接受一次正式第20组，不随1秒轮询跳动。
- 开奖结果旁加入北京时间60秒倒计时，仅显示60..00；00时主动刷新正式结果。
- “AI评分前三”改为“本期AI预测前三”：当前1-19组实时数据 + 历史正式结果 + 固定组次对第20组的历史关联共同评分。
- 增加“历史高关联组次”前三，显示固定组次与第20组单几一致的历史匹配率。
- 1200->1201 的 +18 区块偏移继续保留。


## Fast target-block path
- Group 20 is watched directly by its known target block number.
- TRONGrid FullNode and TRONScan are queried concurrently; the first valid matching block is published to RAM immediately.
- PostgreSQL persistence remains asynchronous and cannot delay the displayed result.
- Frontend live polling is 200 ms.
- `/api/draw` debug.fastTarget reports the winning provider and request latency for diagnosis.

## 遗漏统计持久化恢复
- 每次部署/重启后，从 PostgreSQL 保留的历史第20组正式结果重建 单0～单7 遗漏值，不再从0开始。
- 恢复完成后，继续由当前正式结果实时递增/归零；同一期只更新一次，避免轮询重复累计。
- `/api/draw` 增加 `omissionSource` 与 `omissionHistorySample` 便于确认恢复来源和样本数。


## FIX: fixed 20-slot live board + immutable official result + AI/stat recovery
- 20 group rows are always visible as fixed slots; each arriving group fills its own row immediately.
- A newly filled row uses a high-brightness traffic-light highlight for 5 seconds, then returns to normal.
- Frontend prevents overlapping /api/draw requests, so late HTTP responses cannot repaint older state.
- Official result only advances when officialReady is true and resultBlockNumber exactly equals targetResultBlock; block number must move forward.
- Current groupPeriodKey is always emitted, restoring current-period stats/AI rendering.
- AI display locks once during countdown 10..1 and hides at 00.
- Removed databaseLatestBlock query from the hot 200ms /api/draw path to reduce DB-induced stalls.

## 2026-09-24 全功能复检修复
- 后端期号时钟与前端统一使用北京时间 -4 秒校准，避免00秒附近前后端跨期不一致。
- 00:00 正确归属上一日第1440期。
- 20组固定1-20框架：缺组只显示等待数据，不隐藏整表。
- 开奖结果继续只接受当前期准确第20组目标区块，防止乱序轮询导致跳动。
- AI摘要增加后端短缓存，历史样本与最近20期改为批量数据库读取，显著减少200ms轮询造成的数据库压力。
- AI已锁定时，本期AI前三直接读取数据库保存的 prediction_top3，不再用实时scores重新排序，避免“锁定后前三仍变化”。
- 历史官方结果计算不再假设所有期固定+20，统一走 period_target_block，兼容已记录的+18校准点。
- 数据结论/AI分析前端继续使用last-good保护，短暂空响应不会清空已显示内容。
- 历史数据页移除占位符，新增期级开奖记录和AI预测验证记录。
- 遗漏统计仍由PostgreSQL历史恢复，运行后按正式第20组去重更新。


## AI 最后10秒锁定（本版）
- 60～11秒：AI分析显示“正在分析中…”。
- 10～1秒：后端仅在这个窗口首次写入本期锁定预测；页面显示“AI分析：单X｜第XXXX期｜已锁定”。
- 00秒：页面立即恢复“正在分析中…”，数据库中的锁定预测继续保留用于第20组验证。
- 原“历史样本”展示已替换为该AI分析对应的期号。

## 2026-09-24 全功能重整检查
- 修复 AI 10 秒锁定被 1 秒缓存提前返回而漏触发的问题：页面倒计时显示 10 是唯一锁定触发源，锁定请求优先于缓存。
- 数据结论改为独立 17 组 AI 结论；上方 1-18 统计仅作为描述性统计，不再把最高频直接当预测。
- 17 组模型显式加入 7 个奇偶计数的自然基线，避免把单3/单4天然高频误认为可预测规律；页面使用“模型倾向/评分”而非保证概率。
- AI分析继续在 10~1 秒显示“第XXXX期｜已锁定”，00 后恢复“正在分析中”；锁定记录仍用于开奖验证。
- 20 组区域固定 1~20 框架；单组缺失只显示等待数据，不会清空整表。
- 开奖结果继续只接受当前期准确 group20 目标区块，并防止乱序 HTTP 响应回刷旧结果。
- 遗漏统计启动时从 PostgreSQL 历史恢复，运行中按正式开奖结果去重更新。
- 修复区块映射：已验证 0479/0480/0481/0482、0546~0551、1001、1200、1201 锚点全部一致。0551~1001 之间存在一个已知但位置未知的 +18 转换，程序不伪造转换点。
- 保留 PostgreSQL 异步持久化、实时内存、last-good 容错、历史页、AI前三验证与第20组快速获取。

## 2026-09 AI规律库 / 1000期学习
- 历史数据页新增“AI规律库（滚动1000期）”：长期单0~单7分布、当前遗漏、当前遗漏±2区间的历史条件出现率、历史高发遗漏点及样本数。
- 遗漏“卡期”不是写死规则：每个单X独立从数据库历史正式第20组学习，并使用Beta平滑，低样本不会获得过高权重。
- AI分析和17组AI结论均加入遗漏条件率信号，但权重有上下限，避免追冷/追热过拟合。
- 数据库继续保存原始 block_hash、7号码、single_count；历史页增加哈希字符分布诊断。哈希诊断只用于检查统计偏差，不假设可预测下一哈希。
- 模型最多读取最近1000期正式结果作为滚动分析窗口；不足1000期时显示实际样本数。

## 双层AI（可选 OpenAI 深度复核）
- 本地统计AI始终运行；OpenAI层只在当前期1-17组完整后异步复核，不阻塞开奖接口。
- Render环境变量：`OPENAI_API_KEY`（必需，若不配置则自动降级为本地AI）；`OPENAI_MODEL`（可选，默认 `gpt-5.6-luna`）。
- API密钥只在后端读取，绝不放入 index.html。
- 深度AI只接收整理后的统计特征，不接收第20组正式结果来生成本期判断；第20组仅用于事后验证。
- 页面中的分数/倾向属于历史统计模型输出，不代表可靠开奖概率或保证命中。

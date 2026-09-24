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

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

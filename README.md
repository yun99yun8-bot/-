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


## 终极同步修复
- 当前期 1~20 组均携带 period / periodKey / target20 身份，前端不再把上一期缓存当成本期数据。
- Render/数据库短暂重连跨期时，只保留最后已确认正式开奖结果；当前期组数据、17组结论和AI分析清空等待真实数据，杜绝跨期串期。
- AI结论严格依赖当前期1~17组；10秒AI锁定显示逻辑保持10~1秒可见。
- 首页最近20期继续固定5列×4排，手机端无横向滚动。
- 实时轮询调整为500ms，降低免费实例请求重叠，同时保持实时刷新。


## V3 后端自主AI生命周期
- 新增独立 ai-prediction 后台线程，AI预测不再依赖手机网页10秒请求。
- 当前期1~17组完整且第20组尚未出现时，后台自动保存17组结论和本期预测记录。
- 第20组出现后后台自动执行 verify_prediction，写入 actual_single / verified_at。
- 所有保存均以 period_key UPSERT，重复轮询不会重复创建记录。
- 前端10秒请求仍保留为额外触发，但不再是唯一触发源。
- 因此关闭网页、手机锁屏、前端卡顿都不会阻止后台形成AI预测记录与后续对错验证。


## V4 AI持久化竞态修复
- 修复“部署后第一期正常、下一期开始三个AI功能一起停止”的竞态。
- 预测在第20组出现前先保存为内存快照，再尝试写PostgreSQL。
- 如果数据库连接池/Render瞬时繁忙，快照不会丢失；后台持续重试写入，即使第20组随后已经出现。
- 验证逻辑不再把 UPDATE 0 行误判为验证成功；只有预测行真实存在并写入 actual_single 后才完成本期验证。
- 因此 AI显示、对错验证、AI预测记录不再因为短暂数据库失败同时断链。
- /api/draw debug 增加 aiPending，便于确认是否存在等待持久化的预测快照。


## V5 17/17 强制建档修复
- 根据线上0048漏期、0049已17/17但 frozen=false 的实际诊断修复。
- 不再只依赖后台 daemon 线程捕捉短暂的17组→20组窗口。
- /api/draw 在确认本期严格1~17组齐全且第20组未出现时，直接强制创建本期AI预测。
- prediction_summary 增加第二道自愈：发现17/17但没有预测行时立即再次创建并刷新待写队列。
- 数据库暂时未落盘时，内存 pending 快照也直接作为 frozen AI 返回给前端，避免页面继续显示“分析中”。
- 第20组出现以后仍禁止新建预测，避免用开奖结果倒推预测。
- debug 新增 ai17Ready / aiFrozen，方便线上直接判断建档是否成功。


## V6 SmartDB 架构
- 新增 period_groups：按“期次+组号”永久物化每期1~20组，不再只依赖原始区块表推算。
- 新增 system_events：记录关键后台异常/重试事件，减少静默失败。
- AI记录新增 model_version / locked_at，历史预测可区分算法版本与开奖前锁定时间。
- 新增 smart-db 后台线程：持续保存当前期组数据，并每30秒自愈最近6期的缺失组/验证状态。
- /api/draw 请求路径也同步物化当前组数据，形成双保险。
- 新增 /api/system-health：查看 periodGroups、predictions、events、模型版本和最近后台错误。
- V6保留V5的17/17强制建档与pending重试机制。
- 数据库“智能化”用于完整性、恢复、统计和模型验证，不代表区块哈希可被可靠预测。


## V7 AI Engine
- 多模型融合：长期分布、短期分布、当前期1~17组结构、状态转移、遗漏压力共同评分。
- 所有同期开奖前模型只允许读取当前1~17组和此前已完成正式开奖，避免第20组信息泄漏。
- AI预测记录新增 confidence、ensemble_detail、model_weights、model_version、locked_at，便于后续真实样本外比较。
- confidence 是模型分数分离度，不宣称为真实中奖概率。
- 新增 /api/result-fast 极速正式结果接口：只读取目标第20组，不运行历史统计/AI，避免复杂AI拖慢开奖结果。
- 保留V6 SmartDB自动物化、自修复、系统事件和V5 17/17强制建档。
- 本版增强的是建模、验证与显示通道；不保证密码学哈希存在可持续预测优势。


## V7.1 手机锁屏 / 验证解耦修复
- AI预测锁定与开奖结果验证由Render后台worker负责，不要求手机页面处于前台。
- /api/draw 不再同步执行 verify_prediction，避免开奖后数据库验证阻塞整个实时页面。
- 正式开奖结果新增前端独立 /api/result-fast 轮询；AI/历史接口卡住时结果仍可更新。
- iOS从锁屏/后台恢复时，通过 visibilitychange/pageshow/focus/online 立即重新同步当前秒数、正式结果和主数据。
- 倒计时每次直接按北京时间计算，不从锁屏前旧秒数继续。
- 注意：如果托管平台本身把整个Web Service休眠/停机，任何进程内后台线程都会一起停止；真正24/7独立运行需要部署环境保持服务常驻。

## V7.3 长期稳定修复
- 根据线上证据确认：服务器在页面看似停止时仍持续从0075推进到0076，预测78→79、periodGroups 291→311，因此重点修复前端长期刷新链路。
- 前端增加双请求 watchdog：/api/draw 与 /api/result-fast 任一路超过9秒无成功响应，自动中止旧请求并完整重连。
- iOS 锁屏/切后台恢复、pageshow、online、focus 都强制重新同步服务器当前状态。
- 倒计时始终根据当前北京时间重新计算，不依赖浏览器“上一秒”的计数。
- AI/历史接口和正式开奖结果继续完全分离，AI请求卡住不能阻止正式结果更新。
- system-health 新增 db-writer、tron-ingest、target-result-fast、ai-prediction、smart-db 五条后台任务心跳及 ageSeconds/healthy 状态。
- 模型版本标记为 v7.3-stable-1。


## V8 Autonomous Engine
- 新增独立 period-engine：服务器自己推进每期 COLLECTING → G17_READY → AI_LOCKED → RESULT_READY → VERIFIED。
- 第20组已经出现但开奖前没有合法预测时标记 MISSED_PREDICTION，绝不开奖后补造。
- 新增 period_runtime 持久状态表；进程重启后可根据数据库和现有区块重新判断当前阶段。
- 新增 supervisor：后台worker异常退出会自动重启，并把重启事件写入 system_events。
- system-health 新增 period-engine/supervisor 心跳、workerRestarts、currentPeriodState。
- 网页只是显示器；AI锁定、开奖、验证、数据库保存均不依赖手机页面请求。
- V8增强可靠性、故障隔离和可恢复性，不承诺TRON密码学哈希存在可持续预测优势。

# V10.3 FAST RAW WAREHOUSE / OLD-ONLY TRAINING

- Stage 1: bulk-download the historical TRON block span (about 200k blocks) in 100-block range calls into `historical_raw_blocks`.
- Stage 2: rebuild about 10,000 periods locally from that permanent raw warehouse using the existing 360-period calibration rule.
- Raw hashes are never guessed; incomplete ranges retry. Live collection keeps priority.
- Training is historical-only chronological walk-forward: each archived prediction is generated from older observed records before that archived label is added.
- Live/future trial workers are disabled in V10.3.
- Training report adds 50/100/300/1000-period rates and hit/miss streak diagnostics.

Existing V10.1 tables are migrated automatically by `init_db()`.

# Torn V10.1：持续采集、旧区块补采、10000期历史练习

## 部署

同一套代码部署到现有 Render 的 Web 与 Background Worker，共用原 PostgreSQL `DATABASE_URL`。

| 服务 | 启动命令 | 地址 |
|---|---|---|
| Web | `gunicorn app:app --workers 2 --threads 4 --timeout 45` | `/`：开奖结果、历史、遗漏；`/research-report`：练习报告 |
| Background Worker | `python worker.py` | 持续采集、新期次入库、遗漏及研究 |

## 10000期旧区块补采

- Background Worker 首次运行时将其已完成的最近10000期固定为补采范围，按期号从旧到新检查20组原始区块。已经归档且号码可由哈希重新算出的一期直接跳过；缺失的先从现有 `tron_blocks` 复用，再从 TRON 官方 SolidityNode 的 `getblockbylimitnext` 范围接口读取旧区块，严格核对区块高度、64位哈希与链上时间，然后保存到 `period_groups`。如果官方接口无法返回某高度，不用猜测哈希或开奖结果；记录失败原因并稍后重试。官方接口提供固化区块的按高度范围查询，但可用历史与速率取决于服务提供方及账号配置。
- 任务在 `backfill_runtime` 保存范围、下一期号、已存在/已补采/缺失计数和错误，断线或重启后继续。每次旧链请求前检查实时采集状态、避开临近开奖窗口、间隔至少4秒，并通过程序已有的请求限速器执行。补采速度可能受数据库容量及 TRON 请求配额影响，不承诺几分钟完成。
- 已确认锚点 2026-09-24 第481期之前，V10.1 按用户确认的 `(6,4,2,0,8)` 每360期循环**向前倒推**期号所对应的目标区块；报告明确标示这部分为估算映射。区块哈希本身是真实读取，但更旧的平台期号对应仍需与平台历史结果抽样核对。原始 `tron_blocks` 的7天清理设置不妨碍新归档的 `period_groups` 长期保存。
- 补采期间，新开奖采集和遗漏线程继续运行；历史练习可先显示现有数据，但不选前两名试行。补采完成且失败期数为零后，研究线程自动从最早数据重算，覆盖本版历史复盘行，再开放前两名新期次试行。补采完成后重建遗漏快照。页面 `/research-report` 展示补采状态与真实可练习期数。若有接口缺块/配额限制，页面展示失败期数和错误，不能把不足10000期说成已经练满。

保留原数据库及数据表；不删除既有记录。新网站只开放开奖结果/历史、遗漏、研究报告和逐期练习分页接口。旧版 AI 首页、20组分析面板、旧版预测线程和相关旧 API 均未启动或展示。`collector_core.py` 只作为已经校准的区块/号码计算、数据库采集和遗漏底层兼容模块；其中的旧版 Flask 应用不对外运行。

## 练习口径

- 逐页读取数据库中 `period_groups` 保存的**全部有第20组正式结果**的期次，按期号从旧到新处理；每页最多80期，读完即释放数据库连接，首次读完后每4秒检查新期次。数据库未保存的过往期次无法凭空恢复。
- 较早1000期作为初始训练（其中前700期拟合、后300期调参数）。从第1001期起，每期使用**之前已经出现**的结果训练，先预测当前，再把当前结果加入训练；训练窗维持最近1000期，每100期重新拟合一次。缺少前17组哈希的已开奖期次仍参与历史结果训练，其当期可用特征只有此前开奖结果，报告显示完整前17组的数量。
- 同期比较仅历史开奖、前17组位置、前17组哈希形态、综合特征、固定单3、固定单4六种方法。报告列出所有期次累计与最近至多1000期命中率、最近100期逐期练习预测与结果；逐期记录在 `research_practice_results` 持久化，可在网页分页回查全部已保存的复盘期次。复盘未到200期不选前两名；此后依最近1000期命中数并以累计命中数打破平手，自动更新前两名。
- 首次扫描旧数据期间先展示扫描进度，不启用前两名试行。完成后后台将选中方法在新期次于结果产生之前持久化；只统计预测保存时刻严格早于目标区块链上时间的有效期次，并以**共同有效期次**比较第一、第二名。复盘成绩含选择偏差，不能保证未来命中率上升。
- 原确认的360期尾数 `(6,4,2,0,8)`、每期第20组最终结果、哈希拆号规则和七号码波色继续沿用。原始区块表按既有七天策略维护；归档的 `period_groups` 未设置同样的定期删除。

首次全库扫描和训练期间采集由独立线程持续运行。资源较少的数据库仍可能在首次扫描时出现延迟；报告中的 Worker 心跳、练习进度和首页开奖结果可用于观察。没有线上数据库访问时，软件包测试不能代替真实历史命中率。


V10.3: fixed 10,000-period archive loops; resets model memory each cycle; excludes live/new periods from research; refits every 100 replay periods; reports top two methods and 单0-7 max/current omission gaps.

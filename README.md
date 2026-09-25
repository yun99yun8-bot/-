# V10.5 HISTORY 10000 CLEAN ONLY

纯历史采集版。旧首页、遗漏、开奖记录、训练/预测前端均不再提供。

唯一流程：
1. 下载约20万组真实 TRON 历史区块到 historical_raw_blocks。
2. 原始区块完整后，按现有360规则从本地数据库回填10000期到 period_groups。
3. 首页 `/` 只显示两段进度和当前状态，每2秒刷新。

Render：Web 使用 `gunicorn app:app --workers 2 --threads 4 --timeout 45`；Worker 使用 `python worker.py`。两者必须配置同一个 DATABASE_URL。

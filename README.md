# V10.6.3 HISTORY ONLY — 20万原始区块优先

流程锁死：首次部署本数据集自动清除旧业务数据 → 锁定20万历史TRON区块范围 → 第一阶段只保存 block_number / hash / timestamp，不抓新数据、不计算号码、不做遗漏 → 20万完整验证后才按360规则本地回填10000期 → 全部完成后再另做“补齐最新开奖记录”版本。

Render：Web `gunicorn app:app ...`；Worker `python worker.py`。普通重启不会再次清库，同一 DATASET_ID 只重置一次。

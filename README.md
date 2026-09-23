# TRON 实时区块统计页面 v2

功能：
- 每秒检查 TRON 新区块
- 按既定规则计算 7 个号码
- 保存当前运行实例中的历史记录
- 每组精确统计 7 个号码的单双
- 另外统计 7 个号码个位“尾数”的单双
- 最近 60 期单双组合统计
- 全部已抓取历史单双组合统计
- 手机紧凑布局，一屏查看核心数据
- 统计倾向只作为历史数据参考，不保证下一期结果

Render：
Build Command:
pip install -r requirements.txt

Start Command:
gunicorn app:app --bind 0.0.0.0:$PORT

环境变量：
TRON_PRO_API_KEY = 你的 TronGrid API Key
POLL_SECONDS = 1
HISTORY_LIMIT = 5000

注意：
当前版本历史记录保存在服务运行内存中。Render 服务重启/重新部署后，内存历史会清空。
如果需要真正“永久保存全部历史”，下一版应接数据库（例如 PostgreSQL），这样重启也不会丢记录。

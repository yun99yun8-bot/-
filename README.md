
# TRON 实时区块计算网页

这个项目用于：
- 每秒检查 TRON 主网最新区块；
- 发现新区块后读取 blockID/hash；
- 按你之前保存的规则提取 7 个 A-E 字母和 7 个数字并计算；
- 手机浏览器打开网页查看；
- 每分钟重置一次统计；
- 统计本分钟已经抓到的“唯一区块”号码频率。

注意：
1. 每秒检查并不等于每秒都有新区块。TRON 区块产生速度不是 1 秒 1 个，因此“59 秒快照”和“59 个唯一区块”是两回事。
2. 程序只做链上数据抓取和历史统计，不保证、也不声称能够预测博彩开奖结果。
3. 生产环境建议使用 TronGrid API key，并遵守当前配额/限流规则。

## 本地运行
Python 3.11+：

```bash
python -m venv .venv
# macOS/Linux:
source .venv/bin/activate
# Windows:
# .venv\Scripts\activate

pip install -r requirements.txt
export TRON_PRO_API_KEY="你的API_KEY"
python app.py
```

然后手机和电脑在同一局域网时，可在电脑上查看局域网地址，或部署到云服务器后用 HTTPS 网页访问。

## 云服务器部署
建议部署到支持 Python Web 服务的平台。启动命令：

```bash
gunicorn app:app --bind 0.0.0.0:$PORT
```

环境变量：
- `TRON_PRO_API_KEY`：你的 TronGrid API key
- `POLL_SECONDS`：默认 1
- `TRONGRID_URL`：默认 https://api.trongrid.io/wallet/getnowblock

## 手机
部署完成后，iPhone 用 Safari 打开服务器网址即可；可选择“添加到主屏幕”。

不要把 TronGrid API key 写进网页前端代码；应放在服务器环境变量中。

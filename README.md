# TRON 区块统计面板

这是一个可以直接部署到 GitHub Pages 的静态前端原型。

## 文件

- `index.html`：页面结构
- `style.css`：页面样式
- `app.js`：TRON 区块读取、规则计算、历史记录和统计

## 部署

把三个文件上传到 GitHub 仓库，然后在：

`Settings -> Pages -> Deploy from a branch`

选择包含这些文件的分支和目录即可。

## TRON 数据

程序默认请求：

`https://api.trongrid.io/wallet/getnowblock`

TRON 官方文档说明，该接口用于读取 FullNode 最新区块；FullNode 最新区块可能尚未 solidify。正式做最终状态确认时，应使用 SolidityNode 或按高度查询。

## 重要说明

当前版本的历史数据保存在浏览器 `localStorage`，所以换设备/浏览器不会共享历史。

如果你要做“服务器持续采集 + 所有人看到同一份历史记录”，下一步应该增加一个后端/Serverless API 和数据库，例如 Supabase/Postgres，并让服务器定时采集 TRON 区块。

## 关于同号码

截图中的同号码规则描述比较简略，当前程序采用“最终两位号码去重”的保守实现。
如果你希望 100% 复刻你平台的处理方式，请再提供一两个“出现重复号码时”的实际 Hash -> 开奖结果案例，我可以把算法改成完全一致。

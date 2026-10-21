# A股复盘一体化整合包（v11）

版本：`20261021-integrated-v11`

本包把此前 v9 板块评分与主板过滤、v10 行情源诊断，以及 v11 复盘网页和模拟账户整合到一套可部署文件中。无需再把多个旧补丁逐个叠加；请使用本包内的文件作为完整版本。

## 包含功能

- Flask 后端及手机网页首页。
- 行情优先尝试东方财富，指数和个股快照支持新浪备用源；失败会显示状态和诊断，不编造数据。
- `/api/market-diagnostics` 行情数据源诊断。
- `/api/report` 市场环境、行业/概念板块相对强度、数据新鲜度、主板观察名单。
- 板块评分会说明指标权重/缺失情况；数据不够时不伪造龙二、龙三。
- 主板代码过滤：沪市 600/601/603/605，深市 000/001/002/003；排除创业板、科创板和北交所。
- 5万元模拟账户、每只股票约1万元买入金额上限、模拟买卖、持仓盈亏和交易记录。
- 入场价、止损价和风险预算计算器。
- Render 部署配置。

## 文件

- `app.py`：后端、API、行情源、评分、模拟交易。
- `index.html`：手机网页界面。
- `requirements.txt`：Python 依赖。
- `render.yaml`：Render 部署配置。

## 部署到已有 GitHub 仓库（手机操作）

1. 解压本 ZIP。
2. 打开 GitHub 仓库 `https://github.com/lw307/-a-share-review`。
3. 在仓库根目录上传并覆盖本包中的 `app.py`、`index.html`、`requirements.txt`、`render.yaml` 和 `README.md`。
4. 点击 Commit changes，等待 Render 自动部署。
5. 打开 `https://a-share-review.onrender.com/api/health`，确认 `version` 是 `20261021-integrated-v11`。
6. 打开首页等待数据抓取。免费实例休眠唤醒时可能需要等待。

## 常用接口

- `/api/health`：版本与健康状态。
- `/api/market`：市场快照；打开此接口会触发行情刷新尝试。
- `/api/report`：复盘报告。
- `/api/market-diagnostics`：行情数据源诊断（只读；先打开 `/api/market` 触发刷新）。
- `/api/diagnostics`：AKShare 函数可用性与缓存诊断。
- `/api/test-sectors`：板块数据源诊断。
- `/api/sim/account`：模拟账户。
- `POST /api/sim/buy`、`POST /api/sim/sell`：模拟买卖。

## 重要限制

- 每天 17:24 自动生成、长期归档和消息推送尚未实现；需另行配置定时任务与推送方式。
- 完整板块成分股映射不足时，不虚构龙一/龙二/龙三归属。
- 目前没有经过验证的历史回测；不承诺每笔盈利 5%，板块强度也不是未来收益预测。
- 行情源可能延迟、断连或返回盘中快照；报告应结合数据时间判断，不能把旧快照当成当前行情。
- 模拟账户使用 SQLite。本地文件在 Render 免费实例重启或重建时可能丢失；长期保存需配置持久化磁盘，并将 `SIM_DB_PATH` 指向持久化目录。
- 模拟交易不会连接券商，也不会发出真实委托。

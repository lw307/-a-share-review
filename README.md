# A股每日复盘（替换版本）

## 文件
- `app.py`：Flask 后端、后台行情获取、模拟账户 API、报告 API。
- `index.html`：手机端页面。
- `requirements.txt`：Python 依赖。
- `render.yaml`：Render 部署配置。

## 重要说明
1. 行情依赖 AKShare 的外部数据接口，数据源可能限流、超时或调整字段。代码不会伪造缺失数据。
2. 涨停/跌停数为根据行情快照和常见涨跌停比例估算，不是交易所官方精确统计。
3. 模拟交易不是券商交易；报价可能延迟。佣金按模拟规则计算，不保证与实际券商费用相同。
4. Render 免费实例可能休眠或重启。SQLite 文件在无持久磁盘时可能在部署/重启后丢失；需要长期保存时应配置持久磁盘或外部数据库。
5. 此版本不会自动向微信/手机推送，也不能保证免费实例每天17:24准时运行。要实现可靠定时复盘，需要额外配置外部定时器/任务服务。
6. 板块领涨股仅作为观察数据，不自动等于龙一/龙二/龙三；不承诺盈利。

## 部署
保留现有 GitHub 仓库和 Render 服务。把 `app.py`、`index.html`、`requirements.txt` 分别替换为本包同名文件，`render.yaml` 与当前配置一致时可保留原文件。提交后等待 Render 部署完成，再检查：
- `/api/health`
- `/api/diagnostics`
- `/api/market`
- `/api/report`
- `/api/sim/account`

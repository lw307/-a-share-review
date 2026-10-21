A股每日复盘 v10 后端升级补丁
版本：20261021-marketquality-v10

本次升级重点：
1. 新增 /api/market-diagnostics，只读显示指数与个股行情数据源的逐项尝试结果、返回行数、字段和错误类型。
2. 个股快照至少返回100行才会作为全市场统计来源，避免把异常的少量数据误当全市场快照。
3. 在 /api/report 中新增 market_source_diagnostics 和 fetch_running，便于判断后台是否仍在抓取、哪些来源失败。
4. 每个板块新增 score_data_quality：评分覆盖率、可用指标、缺失指标和数据质量状态。
5. 若板块评分指标缺失，strength_label 会标记“评分部分缺失”，避免只看分数造成误判。
6. 保留 v9 的新浪概念备用源、行业备用源、主板过滤和模拟账户逻辑。

重要限制：
- 本补丁增加诊断与评分质量标记，不承诺解决外部行情网站的网络封锁或连接失败。
- 如果 Render 所在网络无法访问东方财富/新浪等数据源，接口会显示具体错误，不会编造行情。
- 新增字段主要先出现在 JSON API；没有修改 index.html，首页可能暂时不展示全部新字段。
- /api/market-diagnostics 是只读接口，不会主动刷新行情。请先打开首页或 /api/market 触发后台刷新，再查看诊断接口。
- 板块成分股映射仍不完整，因此不虚构每个板块的龙二、龙三。
- 只允许沪深主板：沪市600/601/603/605，深市000/001/002/003；排除创业板、科创板、北交所。
- 模拟交易不连接券商，不执行真实委托。
- Render 免费实例休眠或重启可能丢失内存缓存/本地文件；每天17:24自动生成并推送仍未在此补丁中实现。

部署方法（iPhone GitHub网页）：
1. 打开 https://github.com/lw307/-a-share-review
2. 进入仓库根目录，点 Add file -> Upload files。
3. 上传本文件夹中的 app.py，覆盖根目录 app.py。
4. Commit changes，等待 Render 自动部署完成。
5. 打开 https://a-share-review.onrender.com/api/health，确认 version 为 20261021-marketquality-v10。
6. 打开 https://a-share-review.onrender.com/api/market 触发后台刷新，等待约30-60秒。
7. 打开 https://a-share-review.onrender.com/api/market-diagnostics 查看每个数据源的结果。
8. 打开 https://a-share-review.onrender.com/api/report，检查 score_data_quality 和 market_source_diagnostics。

本地已通过 Python 语法编译检查；未在用户的 Render 环境实测。请以上线后的 API 返回为准。

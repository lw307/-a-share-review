A股每日复盘 v9 后端升级补丁
版本：20261021-sectorfix-v9

本次新增：
1. 板块相对强度评分（0-100）：板块涨幅排名为主；在数据源提供时，纳入上涨广度、成交额排名和领涨股涨幅排名。
2. 主题归类提示：对部分相近板块增加主题分组，便于识别题材重复；不擅自合并原始板块。
3. 严格主板校验：领涨股必须有代码且符合沪深主板代码规则，否则隐藏名称与涨幅。
4. 新增全市场主板强势观察榜 main_board_watchlist，排除创业板、科创板、北交所、ST和退市整理标记。
5. 在 /api/report 中新增 industry_strength_ranked、concept_strength_ranked、main_board_watchlist、sector_scoring_method、dragon_analysis_status 等字段。
6. 保留旧首页使用的 industry_top10/concept_top10，仍按当日涨跌幅排序，避免旧版页面表格排序与显示不一致。

重要限制：
- 本补丁只修改 app.py，不修改 index.html，因此新评分和观察榜目前可通过 /api/report JSON 查看；旧首页未必展示新字段。
- 当前接口没有完整板块成分股映射，不能可靠推断每个板块的龙二、龙三，因此不会编造。
- 单日快照不能证明主线持续性；API 会说明持续性评分需要跨日历史数据。
- 评分是当日相对比较，不是资金流向的精确测量，不预测未来收益，也不构成自动买入信号。
- 模拟交易不连接券商，不执行真实委托。
- Render 免费实例重启可能丢失内存缓存/本地文件；每日 17:24 自动推送功能尚未在本补丁中实现。

部署方法（iPhone GitHub 网页操作）：
1. 打开仓库 https://github.com/lw307/-a-share-review
2. 进入根目录，点 Add file -> Upload files。
3. 上传本文件夹内的 app.py，选择覆盖仓库根目录的 app.py。
4. Commit changes，等待 Render 自动部署完成。
5. 打开 https://a-share-review.onrender.com/api/health，确认 version 为 20261021-sectorfix-v9。
6. 打开 https://a-share-review.onrender.com/api/report，查看新增字段。

本地只完成 Python 语法检查；部署后必须以线上 JSON 为准。

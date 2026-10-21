A股复盘 v8：新浪概念板块备用行情源

本次改动：
1. 版本号 20261021-sectorfix-v8。
2. 概念板块行情优先尝试东方财富实时快照。
3. 东方财富接口失败时，尝试 AKShare 的新浪财经概念板块实时汇总 stock_sector_spot(indicator="概念")。
4. 新浪备用源也失败时，保留东方财富直连接口作为最后尝试。
5. 新增只读诊断接口 /api/test-concept-sina，不会直接修改正式缓存。
6. 不把概念名称清单、历史事件表当作实时涨幅排名。

更新方式：
- 在 GitHub 仓库根目录只替换 app.py。
- Commit changes 后等待 Render 部署完成。
- 打开 /api/health，确认 version 为 20261021-sectorfix-v8。
- 打开 /api/test-concept-sina，复制 JSON 返回给助手。
- 再打开 /api/test-sectors 检查正式缓存是否已使用新浪备用源。

注意：本地只完成代码语法检查，尚未在用户的 Render 环境实际验证新浪接口是否可访问。部署后必须看诊断结果；若来源不可用，程序会报告错误，不会伪造板块排名。

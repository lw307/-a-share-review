A股复盘 v7 - 概念板块备用行情源

本补丁仅需替换 GitHub 仓库根目录的 app.py；其他文件不用改。

v7 变更：
1. 保留原有行业板块和市场环境逻辑。
2. 原有 AKShare 概念行情接口失败时，增加东方财富直接行情接口备用请求，使用较小分页（涨幅榜前40条）和有限重试。
3. 将备用请求的连接/读取超时缩短，避免诊断请求长时间挂起。
4. 只接受实际返回板块名称和数值涨跌幅的行情数据，不会把历史事件表或名称清单当作实时排名。
5. 新增 /api/test-concept-fallback 诊断端点；只测试备用源，不改正式缓存。

部署步骤：
1. 打开 https://github.com/lw307/-a-share-review
2. Add file -> Upload files，上传本文件夹里的 app.py，覆盖仓库根目录旧 app.py。
3. Commit changes，等待 Render 自动部署完成。
4. 打开 https://a-share-review.onrender.com/api/health，确认 version 为 20261021-sectorfix-v7。
5. 打开 https://a-share-review.onrender.com/api/test-concept-fallback，把返回的 JSON 发回检查。
6. 也可以打开 https://a-share-review.onrender.com/api/test-sectors 查看正式缓存与来源状态。

注意：此备用源仍依赖东方财富网络接口。是否能从 Render 稳定访问，必须以部署后的诊断结果为准。若接口仍被 502/网络策略拦截，不会伪造概念排名。该补丁不执行真实交易。

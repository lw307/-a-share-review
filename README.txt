A股复盘程序 v6 诊断补丁
仅需将 app.py 上传到 GitHub 仓库根目录并替换旧 app.py，提交后等待 Render 部署。
部署成功后打开 /api/health 确认版本为 20261021-sectorfix-v6。
然后访问 /api/test-concept-source 查看同花顺概念汇总接口的实际字段结构。
此版本增加诊断端点，不会把未验证的数据写入概念排名。

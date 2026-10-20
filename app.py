from flask import Flask, jsonify, send_from_directory
from datetime import datetime
from zoneinfo import ZoneInfo
import os
app = Flask(__name__)
TZ = ZoneInfo("Asia/Shanghai")
@app.get("/")
def index(): return send_from_directory(".", "index.html")
@app.get("/api/health")
def health(): return jsonify({"ok": True, "service": "A股每日复盘", "timezone": "Asia/Shanghai", "time": datetime.now(TZ).isoformat(), "note": "基础服务已运行；真实行情接口尚未接入。"})
@app.get("/api/market")
def market(): return jsonify({"status": "pending_data_source", "message": "行情数据源尚未配置。接入并核验数据后才会显示真实市场数据。", "fields": ["指数表现", "两市成交额", "上涨/下跌家数", "涨停/跌停数量", "连板高度", "板块强弱排名"]})
if __name__ == "__main__": app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "10000")))

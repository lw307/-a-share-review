from flask import Flask, jsonify, send_from_directory

from datetime import datetime

from zoneinfo import ZoneInfo

import os

app = Flask(__name__)

TZ = ZoneInfo("Asia/Shanghai")

def current_time():

    return datetime.now(TZ).isoformat()

def unavailable(reason):

    return {

        "status": "unavailable",

        "message": reason,

        "data": None

    }

@app.get("/")

def index():

    return send_from_directory(".", "index.html")

@app.get("/api/health")

def health():

    return jsonify({

        "ok": True,

        "service": "A股每日复盘",

        "timezone": "Asia/Shanghai",

        "time": current_time(),

        "note": "基础服务运行正常"

    })

@app.get("/api/market")

def market():

    return jsonify({

        "status": "unavailable",

        "updated_at": current_time(),

        "source": None,

        "disclaimer": "当前为稳定模式，尚未获取真实行情数据。",

        "indices": unavailable("指数行情暂未接入"),

        "breadth": unavailable("市场涨跌家数暂未接入"),

        "amount": unavailable("两市成交额暂未接入"),

        "limit_stats": unavailable("涨停跌停统计暂未接入"),

        "sectors": unavailable("板块排名暂未接入"),

        "message": (

            "基础服务正常。当前暂不请求外部行情接口，"

            "以避免行情请求超时导致网站异常。"

        )

    })

@app.get("/api")

def api_info():

    return jsonify({

        "service": "A股每日复盘",

        "endpoints": [

            "/api/health",

            "/api/market"

        ],

        "time": current_time()

    })

if __name__ == "__main__":

    app.run(

        host="0.0.0.0",

        port=int(os.environ.get("PORT", "10000"))

    )

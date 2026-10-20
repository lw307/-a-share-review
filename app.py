from flask import Flask, jsonify, send_from_directory

from datetime import datetime

from zoneinfo import ZoneInfo

import os

import threading

import json

APP_VERSION = "20261020-index-test-v1"

app = Flask(__name__)

TZ = ZoneInfo("Asia/Shanghai")

# 行情缓存：网页请求不直接等待外部行情接口

market_cache = {

    "status": "waiting",

    "updated_at": None,

    "message": "真实行情数据尚未获取",

    "indices": {

        "status": "unavailable",

        "data": None,

        "message": "指数数据等待获取"

    },

    "breadth": {

        "status": "unavailable",

        "data": None,

        "message": "市场涨跌家数暂未接入"

    },

    "amount": {

        "status": "unavailable",

        "data": None,

        "message": "成交额暂未接入"

    },

    "limit_stats": {

        "status": "unavailable",

        "data": None,

        "message": "涨停跌停统计暂未接入"

    },

    "sectors": {

        "status": "unavailable",

        "data": None,

        "message": "板块排名暂未接入"

    }

}

cache_lock = threading.Lock()

fetch_lock = threading.Lock()

fetch_running = False

last_attempt = 0

sector_lock = threading.Lock()
last_sector_attempt = 0

def current_time():

    return datetime.now(TZ).isoformat()

def unavailable(reason):

    return {

        "status": "unavailable",

        "message": reason,

        "data": None

    }

def fetch_indices():

    """

    在后台尝试获取真实指数数据。

    即使外部接口响应缓慢，网页请求也不会等待这个函数结束。

    """

    global fetch_running, last_attempt

    if not fetch_lock.acquire(blocking=False):

        return

    try:

        fetch_running = True

        last_attempt = datetime.now(TZ).timestamp()

        with cache_lock:

            market_cache["message"] = "正在后台获取真实指数数据"

        import akshare as ak

        try:
            df = ak.stock_zh_index_spot_em()
        except Exception as em_exc:
            try:
                df = ak.stock_zh_index_spot_sina()
            except Exception as sina_exc:
                raise RuntimeError(
                    "东方财富和新浪财经接口均获取失败。"
                    f"东方财富：{type(em_exc).__name__}: "
                    f"{str(em_exc)[:100]}；"
                    f"新浪财经：{type(sina_exc).__name__}: "
                    f"{str(sina_exc)[:100]}"
                ) from sina_exc

        if df is None or df.empty:

            raise ValueError("指数接口没有返回有效数据")

        records = df.head(10).to_dict(orient="records")

        # 将 NaN 等非标准数值转换为 JSON 可处理的 None

        clean_records = []

        for record in records:

            clean_record = {}

            for key, value in record.items():

                try:

                    if value is None:

                        clean_record[key] = None

                    elif str(value).lower() == "nan":

                        clean_record[key] = None

                    elif hasattr(value, "item"):

                        clean_record[key] = value.item()

                    else:

                        clean_record[key] = value

                except Exception:

                    clean_record[key] = str(value)

            clean_records.append(clean_record)

        with cache_lock:

            market_cache["indices"] = {

                "status": "ok",

                "data": clean_records,

                "message": "指数数据已获取"

            }

            market_cache["updated_at"] = current_time()

            market_cache["status"] = "partial"

            market_cache["message"] = (

                "已获取指数数据，其他行情模块正在逐步接入"

            )

    except Exception as exc:
        error_detail = str(exc).replace("\n", " ")[:200]

        with cache_lock:
            market_cache["indices"] = unavailable(
                "指数获取失败："
                + type(exc).__name__
                + "；详情："
                + error_detail
            )
            market_cache["status"] = "unavailable"
            market_cache["updated_at"] = current_time()
            market_cache["message"] = (
                "指数行情获取失败，请检查数据源连接。"
            )

    finally:
        fetch_running = False
        fetch_lock.release()

def start_background_fetch():
    """
    控制请求频率，避免每次打开网页都请求外部数据。
    """
    global last_attempt

    now = datetime.now(TZ).timestamp()

    with cache_lock:
        indices_ready = market_cache["indices"]["status"] == "ok"

    if indices_ready and now - last_attempt < 600:
        return

    if now - last_attempt < 60:
        return

    last_attempt = now

    thread = threading.Thread(
        target=fetch_indices,
        daemon=True
    )
    thread.start()

def fetch_sectors():
    # Fetch industry sector list using Tonghuashun
    global last_sector_attempt

    if not sector_lock.acquire(blocking=False):
        return

    try:
        last_sector_attempt = datetime.now(TZ).timestamp()
        import akshare as ak

        result = {
            "industry": [],
            "concept": []
        }
        errors = []

        # 优先使用已验证可用的同花顺行业板块接口
        try:
            df = ak.stock_board_industry_name_ths()

            if df is None or df.empty:
                raise ValueError("同花顺没有返回有效行业板块数据")

            result["industry"] = json.loads(
                df.to_json(
                    orient="records",
                    force_ascii=False
                )
            )

        except Exception as exc:
            errors.append(
                "同花顺行业板块: "
                + type(exc).__name__
                + ": "
                + str(exc)[:120]
            )

        available = [
            key for key in ("industry", "concept")
            if result[key]
        ]

        with cache_lock:
            if available:
                market_cache["sectors"] = {
                    "status": "partial",
                    "data": result,
                    "message": (
                        "已获取同花顺行业板块列表；"
                        "板块实时行情和概念板块数据尚未完成接入"
                    )
                }
            else:
                market_cache["sectors"] = {
                    "status": "unavailable",
                    "data": None,
                    "message": "板块获取失败：" + "；".join(errors)
                }

            market_cache["updated_at"] = current_time()

    except Exception as exc:
        with cache_lock:
            market_cache["sectors"] = {
                "status": "unavailable",
                "data": None,
                "message": (
                    "板块处理失败："
                    + type(exc).__name__
                    + ": "
                    + str(exc)[:120]
                )
            }

    finally:
        sector_lock.release()
def start_background_sector_fetch():
    """控制板块行情请求频率。"""
    global last_sector_attempt

    now = datetime.now(TZ).timestamp()

    with cache_lock:
        sector_status = market_cache["sectors"]["status"]

    if sector_status in ("ok", "partial"):
        if now - last_sector_attempt < 600:
            return

    if now - last_sector_attempt < 60:
        return

    threading.Thread(
        target=fetch_sectors,
        daemon=True
    ).start()

@app.get("/")

def index():

    return send_from_directory(".", "index.html")

@app.get("/api/diagnostics")
def diagnostics():
    import akshare as ak

    functions = [
    "stock_board_industry_name_em",
    "stock_board_concept_name_em",
    "stock_board_industry_name_ths",
    "stock_board_industry_index_ths",
    "stock_board_industry_cons_ths",
    "stock_board_concept_index_ths",
]

    result = {}

    for name in functions:
        result[name] = {
            "exists": callable(getattr(ak, name, None))
        }

    return jsonify({
        "app_version": APP_VERSION,
        "akshare_version": ak.__version__,
        "functions": result,
        "time": current_time()
    })


@app.get("/api/test-sectors")
def test_sectors():
    import akshare as ak
    import threading
    result = {}
    tests = {
        "industry_em": "stock_board_industry_name_em",
        "concept_em": "stock_board_concept_name_em",
        "industry_ths": "stock_board_industry_name_ths",
    }
    for name, func_name in tests.items():
        result[name] = {
            "status": "testing",
            "message": "测试尚未完成"
        }
        try:
            func = getattr(ak, func_name, None)
            if not callable(func):
                result[name] = {
                    "status": "error",
                    "message": "接口函数不存在"
                }
                continue
            box = {}
            def worker(target, output):
                try:
                    df = target()
                    output["rows"] = len(df) if df is not None else 0
                    output["columns"] = (
                        list(df.columns) if df is not None else []
                    )
                    output["sample"] = (
                        df.head(2).to_dict(orient="records")
                        if df is not None and not df.empty
                        else []
                    )
                except Exception as exc:
                    output["error"] = (
                        type(exc).__name__ + ": " + str(exc)[:200]
                    )
            thread = threading.Thread(
                target=worker,
                args=(func, box),
                daemon=True
            )
            thread.start()
            thread.join(timeout=8)
            if thread.is_alive():
                result[name] = {
                    "status": "timeout",
                    "message": "8秒内没有返回"
                }
            elif "error" in box:
                result[name] = {
                    "status": "error",
                    "message": box["error"]
                }
            elif box.get("rows", 0) > 0:
                result[name] = {
                    "status": "ok",
                    "rows": box["rows"],
                    "columns": box["columns"],
                    "sample": box["sample"]
                }
            else:
                result[name] = {
                    "status": "empty",
                    "message": "接口没有返回有效数据"
                }
        except Exception as exc:
            result[name] = {
                "status": "error",
                "message": type(exc).__name__ + ": " + str(exc)[:200]
            }
    return jsonify(result)

@app.get("/api/health")

def health():

    return jsonify({

        "ok": True,

        "service": "A股每日复盘",

        "version": APP_VERSION,

        "timezone": "Asia/Shanghai",

        "time": current_time(),

        "note": "新版后台行情测试"

    })

@app.get("/api/market")

def market():

    start_background_fetch()
    start_background_sector_fetch()

    with cache_lock:

        result = {

            "status": market_cache["status"],

            "updated_at": market_cache["updated_at"],

            "source": "AKShare",

            "disclaimer": (

                "仅展示实际获取的数据；"

                "未获取的数据不会使用虚构数值填充。"

            ),

            "message": market_cache["message"],

            "indices": dict(market_cache["indices"]),

            "breadth": dict(market_cache["breadth"]),

            "amount": dict(market_cache["amount"]),

            "limit_stats": dict(market_cache["limit_stats"]),

            "sectors": dict(market_cache["sectors"])

        }

    if result["indices"]["status"] == "ok":

        result["status"] = "partial"

    elif fetch_running:

        result["status"] = "loading"

    return jsonify(result)

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

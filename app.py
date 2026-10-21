from flask import Flask, jsonify, send_from_directory

from datetime import datetime

from zoneinfo import ZoneInfo

import pandas as pd

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
    获取真实指数行情。
    优先尝试东方财富，失败后尝试新浪财经。
    修复 DataFrame 条件判断错误，并优先匹配常用指数。
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
        import pandas as pd
        import math

        # 优先尝试东方财富，失败后尝试新浪财经
        try:
            df = ak.stock_zh_index_spot_em()
            source_name = "东方财富"
        except Exception as em_exc:
            try:
                df = ak.stock_zh_index_spot_sina()
                source_name = "新浪财经"
            except Exception as sina_exc:
                raise RuntimeError(
                    f"东方财富接口失败：{str(em_exc)[:100]}；"
                    f"新浪财经接口失败：{str(sina_exc)[:100]}"
                ) from sina_exc

        if df is None or df.empty:
            raise ValueError("指数接口没有返回有效数据")

        # 兼容不同数据源的字段名称
        code_col = next(
            (
                col for col in ["代码", "指数代码", "symbol", "code"]
                if col in df.columns
            ),
            None
        )

        name_col = next(
            (
                col for col in ["名称", "指数名称", "name"]
                if col in df.columns
            ),
            None
        )

        # 先按指数代码筛选
        target_codes = {
            "000001", "399001", "399006",
            "000300", "000016", "000905"
        }

        selected_frames = []

        if code_col is not None:
            codes = (
                df[code_col]
                .astype(str)
                .str.strip()
                .str.replace(r"\.0$", "", regex=True)
            )

            # 兼容 sh000001、sz399001 等代码格式
            codes = codes.str.replace(
                r"^(sh|sz|SH|SZ)", "", regex=True
            )

            code_mask = codes.isin(target_codes)
            code_matches = df.loc[code_mask].copy()

            if not code_matches.empty:
                selected_frames.append(code_matches)

        # 再按指数名称筛选
        target_names = [
            "上证指数",
            "上证综指",
            "深证成指",
            "沪深300",
            "上证50",
            "中证500"
        ]

        if name_col is not None:
            names = df[name_col].astype(str)

            name_mask = names.apply(
                lambda name: any(
                    target in name for target in target_names
                )
            )

            name_matches = df.loc[name_mask].copy()

            if not name_matches.empty:
                selected_frames.append(name_matches)

        # 合并筛选结果；没有匹配到时保留完整真实数据供排查
        if selected_frames:
            selected = pd.concat(
                selected_frames,
                ignore_index=True
            ).drop_duplicates()
        else:
            selected = df.copy()

        # 转成可供 JSON 返回的数据，避免 NaN 等值引发问题
        records = selected.to_dict(orient="records")
        clean_records = []

        for record in records:
            clean_record = {}

            for key, value in record.items():
                try:
                    if value is None:
                        clean_record[key] = None
                    elif hasattr(value, "item"):
                        value = value.item()

                        if (
                            isinstance(value, float)
                            and not math.isfinite(value)
                        ):
                            value = None

                        clean_record[key] = value

                    elif (
                        isinstance(value, float)
                        and not math.isfinite(value)
                    ):
                        clean_record[key] = None

                    else:
                        clean_record[key] = value

                except Exception:
                    clean_record[key] = str(value)

            clean_records.append(clean_record)

        with cache_lock:
            market_cache["indices"] = {
                "status": "ok",
                "data": clean_records,
                "message": (
                    f"{source_name}指数数据已获取，"
                    f"返回{len(clean_records)}条记录"
                )
            }

            market_cache["updated_at"] = current_time()
            market_cache["status"] = "partial"
            market_cache["message"] = (
                "指数数据已更新；成交额、市场涨跌家数及"
                "涨停跌停统计仍需单独接入。"
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
            market_cache["message"] = "指数行情获取失败，请检查数据源连接。"

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
    """获取行业板块列表和概念板块行情。"""
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

        # 1. 获取同花顺行业板块列表
        try:
            df_industry = ak.stock_board_industry_name_ths()

            if df_industry is None or df_industry.empty:
                raise ValueError("没有返回有效行业板块数据")

            result["industry"] = json.loads(
                df_industry.to_json(
                    orient="records",
                    force_ascii=False
                )
            )

        except Exception as exc:
            errors.append(
                "同花顺行业板块："
                + type(exc).__name__
                + ": "
                + str(exc)[:120]
            )

        # 2. 获取已经通过测试的东方财富概念板块行情
        try:
            df_concept = ak.stock_board_concept_name_em()

            if df_concept is None or df_concept.empty:
                raise ValueError("没有返回有效概念板块行情")

            result["concept"] = json.loads(
                df_concept.to_json(
                    orient="records",
                    force_ascii=False
                )
            )

        except Exception as exc:
            errors.append(
                "东方财富概念板块："
                + type(exc).__name__
                + ": "
                + str(exc)[:120]
            )

        # 3. 根据实际获取结果确定状态
        available = [
            key for key in ("industry", "concept")
            if result[key]
        ]

        if len(available) == 2:
            status = "ok"
            message = (
                "行业板块列表和概念板块行情均已获取；"
                "行业板块实时行情仍需单独接入"
            )

        elif available:
            status = "partial"
            message = (
                "部分板块数据已获取；"
                + "；".join(errors)
            )

        else:
            status = "unavailable"
            message = (
                "板块数据获取失败："
                + "；".join(errors)
            )

        with cache_lock:
            market_cache["sectors"] = {
                "status": status,
                "data": result if available else None,
                "message": message
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

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
    """获取真实指数行情；优先东方财富，缺失时尝试新浪补齐。"""
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

        target_codes = {
            "000001", "399001", "399006",
            "000300", "000016", "000905"
        }

        target_names = {
            "上证指数", "上证综指", "深证成指",
            "沪深300", "上证50", "中证500"
        }

        frames = []
        errors = []

        # 分别尝试两个数据源，避免一个数据源缺少指数时直接放弃。
        sources = [
            ("东方财富", ak.stock_zh_index_spot_em),
            ("新浪财经", ak.stock_zh_index_spot_sina),
        ]

        for source_name, source_func in sources:
            try:
                df = source_func()

                if df is None or df.empty:
                    errors.append(f"{source_name}返回空数据")
                    continue

                code_col = next(
                    (
                        c for c in ["代码", "指数代码", "symbol", "code"]
                        if c in df.columns
                    ),
                    None
                )

                name_col = next(
                    (
                        c for c in ["名称", "指数名称", "name"]
                        if c in df.columns
                    ),
                    None
                )

                selected = df.copy()

                if code_col:
                    codes = (
                        selected[code_col]
                        .astype(str)
                        .str.strip()
                        .str.replace(r"\.0$", "", regex=True)
                        .str.replace(r"^(sh|sz|SH|SZ)", "", regex=True)
                    )

                    mask = codes.isin(target_codes)

                    if name_col:
                        names = selected[name_col].astype(str)
                        mask = mask | names.apply(
                            lambda n: any(x in n for x in target_names)
                        )

                    selected = selected.loc[mask].copy()

                elif name_col:
                    names = selected[name_col].astype(str)
                    selected = selected.loc[
                        names.apply(
                            lambda n: any(x in n for x in target_names)
                        )
                    ].copy()
                else:
                    errors.append(f"{source_name}缺少指数代码和名称字段")
                    continue

                if not selected.empty:
                    selected["_数据源"] = source_name
                    frames.append(selected)

            except Exception as exc:
                errors.append(
                    f"{source_name}：{type(exc).__name__}: {str(exc)[:100]}"
                )

        if not frames:
            raise RuntimeError("；".join(errors) or "两个指数数据源均未返回有效数据")

        combined = pd.concat(frames, ignore_index=True, sort=False)

        # 根据代码或名称去重，优先保留东方财富记录。
        code_col = next(
            (
                c for c in ["代码", "指数代码", "symbol", "code"]
                if c in combined.columns
            ),
            None
        )

        name_col = next(
            (
                c for c in ["名称", "指数名称", "name"]
                if c in combined.columns
            ),
            None
        )

        if code_col:
            combined["_匹配代码"] = (
                combined[code_col]
                .astype(str)
                .str.strip()
                .str.replace(r"\.0$", "", regex=True)
                .str.replace(r"^(sh|sz|SH|SZ)", "", regex=True)
            )
            combined = combined.drop_duplicates(
                subset=["_匹配代码"], keep="first"
            )
        elif name_col:
            combined = combined.drop_duplicates(
                subset=[name_col], keep="first"
            )

        # 清理内部辅助字段，转换为可返回的 JSON 数据。
        records = combined.to_dict(orient="records")
        clean_records = []

        for record in records:
            clean = {}

            for key, value in record.items():
                if key.startswith("_"):
                    continue

                try:
                    if value is None:
                        clean[key] = None
                    elif hasattr(value, "item"):
                        value = value.item()
                        if isinstance(value, float) and not math.isfinite(value):
                            value = None
                        clean[key] = value
                    elif isinstance(value, float) and not math.isfinite(value):
                        clean[key] = None
                    else:
                        clean[key] = value
                except Exception:
                    clean[key] = str(value)

            clean_records.append(clean)

        with cache_lock:
            market_cache["indices"] = {
                "status": "ok",
                "data": clean_records,
                "message": (
                    f"指数行情已获取，共{len(clean_records)}条记录；"
                    "请核对是否包含深证成指。"
                )
            }

            market_cache["updated_at"] = current_time()
            market_cache["status"] = "partial"
            market_cache["message"] = (
                "指数行情已更新；成交额、市场涨跌家数及"
                "涨停跌停统计仍需接入。"
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
    """获取真实行业板块行情和概念板块行情。"""
    global last_sector_attempt

    if not sector_lock.acquire(blocking=False):
        return

    try:
        last_sector_attempt = datetime.now(TZ).timestamp()

        import akshare as ak
        import pandas as pd
        import json

        result = {
            "industry": [],
            "concept": []
        }
        errors = []

        def dataframe_to_records(df):
            """将有效 DataFrame 转换成 JSON 兼容的字典列表。"""
            if df is None or not isinstance(df, pd.DataFrame):
                return []

            if df.empty:
                return []

            clean_df = df.copy()

            # 将 NaN、NaT 等缺失值转成 None
            clean_df = clean_df.astype(object)
            clean_df = clean_df.where(pd.notna(clean_df), None)

            return json.loads(
                clean_df.to_json(
                    orient="records",
                    force_ascii=False
                )
            )

        # 1. 优先获取同花顺行业板块行情
        try:
            df_industry = ak.stock_board_industry_index_ths()

            records = dataframe_to_records(df_industry)

            if not records:
                raise ValueError("同花顺行业板块行情没有返回有效记录")

            result["industry"] = records

        except Exception as exc:
            errors.append(
                "同花顺行业行情："
                + type(exc).__name__
                + ": "
                + str(exc)[:120]
            )

        # 2. 获取东方财富概念板块行情
        try:
            df_concept = ak.stock_board_concept_name_em()

            records = dataframe_to_records(df_concept)

            if not records:
                raise ValueError("东方财富概念板块行情没有返回有效记录")

            result["concept"] = records

        except Exception as exc:
            errors.append(
                "东方财富概念行情："
                + type(exc).__name__
                + ": "
                + str(exc)[:120]
            )

        # 3. 根据真实获取结果确定状态
        available = [
            key for key in ("industry", "concept")
            if result[key]
        ]

        if len(available) == 2:
            status = "ok"
            message = (
                "行业板块行情和概念板块行情均已获取"
            )

        elif available:
            status = "partial"
            message = (
                "部分板块行情已获取；"
                + "；".join(errors)
            )

        else:
            status = "unavailable"
            message = (
                "板块行情获取失败："
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
    "industry_em_spot": "stock_board_industry_spot_em",
    "industry_em_names": "stock_board_industry_name_em",
    "industry_ths_quotes": "stock_board_industry_index_ths",
    "concept_ths_quotes": "stock_board_concept_index_ths",
    "concept_em_quotes": "stock_board_concept_name_em",
    "industry_ths_names": "stock_board_industry_name_ths",
}

    def test_one(name, func_name):
        func = getattr(ak, func_name, None)

        if not callable(func):
            result[name] = {
                "status": "error",
                "message": "接口函数不存在"
            }
            return

        box = {}

        def worker():
            try:
                df = func()

                if df is None or df.empty:
                    box["result"] = {
                        "status": "empty",
                        "message": "接口没有返回有效数据"
                    }
                    return

                box["result"] = {
                    "status": "ok",
                    "rows": len(df),
                    "columns": list(df.columns),
                    "sample": df.head(2).to_dict(orient="records")
                }

            except Exception as exc:
                box["result"] = {
                    "status": "error",
                    "message": (
                        type(exc).__name__ + ": " + str(exc)[:200]
                    )
                }

        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
        thread.join(timeout=15)

        if thread.is_alive():
            result[name] = {
                "status": "timeout",
                "message": "15秒内没有返回"
            }
        else:
            result[name] = box.get("result", {
                "status": "error",
                "message": "测试没有产生结果"
            })

    for name, func_name in tests.items():
        test_one(name, func_name)

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

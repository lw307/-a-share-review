from flask import Flask, jsonify, send_from_directory

from datetime import datetime

from zoneinfo import ZoneInfo

import os

import traceback

app = Flask(__name__)

TZ = ZoneInfo("Asia/Shanghai")

def current_time():

    return datetime.now(TZ).isoformat()

def unavailable(reason):

    return {

        "status": "unavailable",

        "message": reason,

        "data": None,

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

        "note": "基础服务正常，行情数据通过独立接口获取并核验。",

    })

@app.get("/api/market")

def market():

    result = {

        "status": "partial",

        "updated_at": current_time(),

        "source": "AKShare",

        "disclaimer": "只展示成功获取并通过基本校验的数据；缺失数据不会用示例值替代。",

        "indices": unavailable("尚未获取指数数据"),

        "breadth": unavailable("尚未获取市场涨跌统计"),

        "amount": unavailable("尚未获取两市成交额"),

        "limit_stats": unavailable("尚未获取涨停跌停统计"),

        "sectors": unavailable("尚未获取板块排名"),

    }

    try:

        import akshare as ak

        import pandas as pd

        # 指数行情

        try:

            df = ak.stock_zh_index_spot_em()

            if df is not None and not df.empty:

                result["indices"] = {

                    "status": "ok",

                    "data": df.head(10).replace(

                        {float("nan"): None}

                    ).to_dict(orient="records"),

                }

        except Exception as exc:

            result["indices"] = unavailable(

                f"指数接口获取失败：{type(exc).__name__}"

            )

        # 全市场股票涨跌家数

        try:

            stocks = ak.stock_zh_a_spot_em()

            if stocks is not None and not stocks.empty:

                change_col = next(

                    (

                        c for c in stocks.columns

                        if c in ("涨跌幅", "涨跌幅(%)")

                    ),

                    None,

                )

                if change_col is not None:

                    changes = pd.to_numeric(

                        stocks[change_col], errors="coerce"

                    ).dropna()

                    result["breadth"] = {

                        "status": "ok",

                        "data": {

                            "上涨": int((changes > 0).sum()),

                            "下跌": int((changes < 0).sum()),

                            "平盘": int((changes == 0).sum()),

                            "统计数量": int(len(changes)),

                        },

                    }

                # 尝试统计成交额

                amount_col = next(

                    (

                        c for c in stocks.columns

                        if c in ("成交额", "成交额(元)")

                    ),

                    None,

                )

                if amount_col is not None:

                    amounts = pd.to_numeric(

                        stocks[amount_col], errors="coerce"

                    ).dropna()

                    if len(amounts) > 0:

                        result["amount"] = {

                            "status": "ok",

                            "data": {

                                "股票成交额合计_元": float(amounts.sum()),

                                "说明": (

                                    "按当前接口返回的股票成交额字段求和；"

                                    "不等同于未经核验的官方两市成交额口径。"

                                ),

                            },

                        }

                # 涨停、跌停：使用接口返回的涨跌幅统计初步识别。

                # 此处只作初步统计，不保证覆盖特殊涨跌幅规则。

                if change_col is not None:

                    limit_up = int((changes >= 9.9).sum())

                    limit_down = int((changes <= -9.9).sum())

                    result["limit_stats"] = {

                        "status": "approximate",

                        "data": {

                            "涨停候选数量": limit_up,

                            "跌停候选数量": limit_down,

                        },

                        "message": (

                            "这是按涨跌幅阈值估算的候选数量，"

                            "尚未核验 ST、不同板块及特殊证券规则，"

                            "不能视为官方涨停跌停统计。"

                        ),

                    }

        except Exception as exc:

            error = f"{type(exc).__name__}"

            result["breadth"] = unavailable(f"市场统计接口获取失败：{error}")

            result["amount"] = unavailable(f"成交额获取失败：{error}")

            result["limit_stats"] = unavailable(f"涨跌停统计获取失败：{error}")

        # 板块排名

        try:

            sectors = ak.stock_board_industry_name_em()

            if sectors is not None and not sectors.empty:

                result["sectors"] = {

                    "status": "ok",

                    "data": sectors.head(20).replace(

                        {float("nan"): None}

                    ).to_dict(orient="records"),

                    "message": (

                        "此处为接口返回的行业板块数据；"

                        "龙头识别和综合强弱评分尚未实现。"

                    ),

                }

        except Exception as exc:

            result["sectors"] = unavailable(

                f"板块接口获取失败：{type(exc).__name__}"

            )

        statuses = [

            result[key]["status"]

            for key in (

                "indices", "breadth", "amount",

                "limit_stats", "sectors"

            )

        ]

        if all(status == "ok" for status in statuses):

            result["status"] = "ok"

    except Exception as exc:

        result["status"] = "unavailable"

        result["error"] = type(exc).__name__

        result["message"] = (

            "行情数据组件导入失败。请检查依赖安装和 Render 部署日志。"

        )

    return jsonify(result)

@app.get("/api")

def api_info():

    return jsonify({

        "service": "A股每日复盘",

        "endpoints": [

            "/api/health",

            "/api/market",

        ],

        "time": current_time(),

    })
if __name__ == "__main__":

    app.run(

        host="0.0.0.0",

        port=int(os.environ.get("PORT", "10000"))

    )

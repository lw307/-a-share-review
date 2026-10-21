# -*- coding: utf-8 -*-
"""
A股每日复盘 - Flask 后端
说明：
1. 行情只使用 AKShare 实际返回的数据；接口失败时明确显示 unavailable，不补造数字。
2. 行情抓取在后台线程中运行，避免网页请求被行情接口长时间阻塞。
3. 模拟交易仅用于研究，不连接券商，不会执行真实委托。
4. Render 免费实例可能休眠，内存缓存和本地 SQLite 数据不保证跨重启永久保存。
"""
from flask import Flask, jsonify, request, send_from_directory
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
import threading
import sqlite3
import json
import math
import os
import re

APP_VERSION = "20261021-full-replacement-v1"
TZ = ZoneInfo("Asia/Shanghai")
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.environ.get("SIM_DB_PATH", str(BASE_DIR / "simulation.db")))

app = Flask(__name__, static_folder=None)

cache_lock = threading.RLock()
market_fetch_lock = threading.Lock()
sector_fetch_lock = threading.Lock()
market_last_attempt = 0.0
sector_last_attempt = 0.0
market_fetch_running = False
sector_fetch_running = False

def now_iso():
    return datetime.now(TZ).isoformat(timespec="seconds")

def unavailable(message):
    return {"status": "unavailable", "data": None, "message": message}

market_cache = {
    "status": "waiting",
    "updated_at": None,
    "message": "尚未获取行情。打开页面后会在后台尝试获取真实数据。",
    "indices": unavailable("指数行情等待获取"),
    "breadth": unavailable("市场涨跌家数等待获取"),
    "amount": unavailable("成交额等待获取"),
    "limit_stats": unavailable("涨停跌停统计等待获取"),
    "sectors": {"status": "unavailable", "data": {"industry": [], "concept": []}, "message": "板块行情等待获取"},
    "stocks": {"status": "unavailable", "data": [], "message": "个股行情等待获取"},
}

def safe_records(df):
    """将 DataFrame 转成 JSON 安全的记录列表。"""
    import pandas as pd
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return []
    clean = df.copy().astype(object)
    clean = clean.where(pd.notna(clean), None)
    return json.loads(clean.to_json(orient="records", force_ascii=False))

def as_float(value):
    try:
        if value is None or value == "":
            return None
        n = float(str(value).replace(",", "").replace("%", "").strip())
        return n if math.isfinite(n) else None
    except Exception:
        return None

def first_column(df, candidates):
    return next((c for c in candidates if c in df.columns), None)

def fetch_market_data():
    """获取指数与个股快照；耗时操作放在后台线程。"""
    global market_fetch_running, market_last_attempt
    if not market_fetch_lock.acquire(blocking=False):
        return
    market_fetch_running = True
    market_last_attempt = datetime.now(TZ).timestamp()
    try:
        import akshare as ak
        errors = []
        indices = []
        stocks = []

        # 指数快照：东方财富优先，新浪补充。单一源失败不阻止另一个源。
        targets = {"000001", "399001", "399006", "000300", "000016", "000905"}
        target_names = ("上证指数", "上证综指", "深证成指", "沪深300", "上证50", "中证500", "创业板指")
        for source_name, func_name in [
            ("东方财富", "stock_zh_index_spot_em"),
            ("新浪财经", "stock_zh_index_spot_sina"),
        ]:
            try:
                fn = getattr(ak, func_name, None)
                if not callable(fn):
                    continue
                df = fn()
                if df is None or df.empty:
                    errors.append(f"{source_name}指数接口返回空数据")
                    continue
                code_col = first_column(df, ["代码", "指数代码", "symbol", "code"])
                name_col = first_column(df, ["名称", "指数名称", "name"])
                if not code_col and not name_col:
                    errors.append(f"{source_name}指数数据缺少名称/代码列")
                    continue
                mask = None
                if code_col:
                    codes = df[code_col].astype(str).str.strip().str.replace(r"\.0$", "", regex=True).str.replace(r"^(sh|sz|SH|SZ)", "", regex=True)
                    mask = codes.isin(targets)
                if name_col:
                    nm = df[name_col].astype(str)
                    by_name = nm.apply(lambda x: any(t in x for t in target_names))
                    mask = by_name if mask is None else (mask | by_name)
                picked = df.loc[mask].copy() if mask is not None else df.iloc[0:0].copy()
                for row in safe_records(picked):
                    row["_source"] = source_name
                    indices.append(row)
            except Exception as exc:
                errors.append(f"{source_name}指数：{type(exc).__name__}: {str(exc)[:100]}")

        # 个股快照：用于成交额、市场宽度、涨跌停估算及模拟账户报价。
        try:
            fn = getattr(ak, "stock_zh_a_spot_em", None)
            if not callable(fn):
                raise RuntimeError("AKShare 未提供 stock_zh_a_spot_em")
            df_stocks = fn()
            stocks = safe_records(df_stocks)
            if not stocks:
                raise RuntimeError("个股快照为空")
        except Exception as exc:
            errors.append(f"个股快照：{type(exc).__name__}: {str(exc)[:120]}")
            df_stocks = None

        # 去重指数，优先东方财富记录。
        seen = set()
        unique_indices = []
        for row in indices:
            key = str(row.get("代码") or row.get("指数代码") or row.get("symbol") or row.get("名称") or row.get("name"))
            if key not in seen:
                seen.add(key)
                row.pop("_source", None)
                unique_indices.append(row)

        breadth = unavailable("未取得完整个股快照，涨跌家数暂不可用")
        amount = unavailable("未取得完整个股快照，成交额暂不可用")
        limit_stats = unavailable("未取得完整个股快照，涨跌停统计暂不可用")
        if df_stocks is not None and not df_stocks.empty:
            change_col = first_column(df_stocks, ["涨跌幅", "涨幅", "change_percent"])
            amount_col = first_column(df_stocks, ["成交额", "成交金额", "amount"])
            name_col = first_column(df_stocks, ["名称", "股票简称", "name"])
            code_col = first_column(df_stocks, ["代码", "股票代码", "code"])
            price_col = first_column(df_stocks, ["最新价", "最新", "现价", "price"])
            up_count = down_count = flat_count = None
            if change_col:
                changes = [as_float(v) for v in df_stocks[change_col].tolist()]
                valid = [v for v in changes if v is not None]
                up_count = sum(v > 0 for v in valid)
                down_count = sum(v < 0 for v in valid)
                flat_count = sum(v == 0 for v in valid)
                breadth = {
                    "status": "ok",
                    "data": {"上涨家数": up_count, "下跌家数": down_count, "平盘家数": flat_count, "统计样本": len(valid)},
                    "message": "根据个股实时快照统计；受数据源覆盖范围和停牌股票口径影响，可能与其他终端略有差异。"
                }
            if amount_col:
                vals = [as_float(v) for v in df_stocks[amount_col].tolist()]
                total = sum(v for v in vals if v is not None)
                amount = {
                    "status": "ok",
                    "data": {"总成交额": total, "单位": "元", "股票数": len(df_stocks)},
                    "message": "由个股快照成交额汇总；若接口返回盘中快照，则为盘中累计值。"
                }
            if change_col:
                limit_up = limit_down = 0
                approximate = True
                for _, row in df_stocks.iterrows():
                    change = as_float(row.get(change_col))
                    if change is None:
                        continue
                    name = str(row.get(name_col, "")) if name_col else ""
                    code = str(row.get(code_col, "")) if code_col else ""
                    # 按常见A股涨跌停规则估算：ST约5%，主板约10%，创业板/科创板约20%。
                    # 北交所及特殊新股规则复杂，因此明确标记为估算。
                    if "ST" in name.upper() or "*ST" in name.upper():
                        threshold = 4.8
                    elif code.startswith(("300", "301", "688", "689")):
                        threshold = 19.5
                    elif code.startswith(("4", "8", "92")):
                        threshold = 29.0
                    else:
                        threshold = 9.5
                    if change >= threshold:
                        limit_up += 1
                    elif change <= -threshold:
                        limit_down += 1
                limit_stats = {
                    "status": "partial",
                    "data": {"涨停家数_估算": limit_up, "跌停家数_估算": limit_down},
                    "message": "根据涨跌幅及常见涨跌停比例估算，非交易所官方精确统计；新股、特殊规则股可能造成偏差。"
                }

        with cache_lock:
            market_cache["indices"] = {
                "status": "ok" if unique_indices else "unavailable",
                "data": unique_indices or None,
                "message": f"获取到 {len(unique_indices)} 条指数记录" if unique_indices else "指数接口没有返回目标指数；" + "；".join(errors[:2])
            }
            market_cache["breadth"] = breadth
            market_cache["amount"] = amount
            market_cache["limit_stats"] = limit_stats
            market_cache["stocks"] = {
                "status": "ok" if stocks else "unavailable",
                "data": stocks,
                "message": f"个股快照共 {len(stocks)} 条" if stocks else "个股快照不可用"
            }
            market_cache["updated_at"] = now_iso()
            available = sum(market_cache[k]["status"] in ("ok", "partial") for k in ["indices", "breadth", "amount", "limit_stats"])
            market_cache["status"] = "ok" if available == 4 else ("partial" if available else "unavailable")
            market_cache["message"] = "真实行情数据已更新；缺失项目会单独标注。" + ((" 数据源提示：" + "；".join(errors[:3])) if errors else "")
    except Exception as exc:
        with cache_lock:
            market_cache["status"] = "unavailable"
            market_cache["message"] = f"行情抓取失败：{type(exc).__name__}: {str(exc)[:180]}"
            market_cache["updated_at"] = now_iso()
    finally:
        market_fetch_running = False
        market_fetch_lock.release()

def fetch_sector_data():
    """获取实时行业/概念板块快照。历史K线接口不作为当日排名使用。"""
    global sector_fetch_running, sector_last_attempt
    if not sector_fetch_lock.acquire(blocking=False):
        return
    sector_fetch_running = True
    sector_last_attempt = datetime.now(TZ).timestamp()
    try:
        import akshare as ak
        result = {"industry": [], "concept": []}
        errors = []
        for key, func_name in [
            ("industry", "stock_board_industry_name_em"),
            ("concept", "stock_board_concept_name_em"),
        ]:
            try:
                fn = getattr(ak, func_name, None)
                if not callable(fn):
                    raise RuntimeError(f"接口函数 {func_name} 不存在")
                df = fn()
                records = safe_records(df)
                if not records:
                    raise RuntimeError("接口没有返回有效记录")
                result[key] = records
            except Exception as exc:
                errors.append(f"{key}：{type(exc).__name__}: {str(exc)[:100]}")
        available = bool(result["industry"] or result["concept"])
        both = bool(result["industry"] and result["concept"])
        with cache_lock:
            market_cache["sectors"] = {
                "status": "ok" if both else ("partial" if available else "unavailable"),
                "data": result if available else None,
                "message": ("行业和概念板块快照均已获取" if both else "部分板块数据可用" if available else "板块快照不可用") + (("；" + "；".join(errors)) if errors else "")
            }
            market_cache["updated_at"] = now_iso()
    except Exception as exc:
        with cache_lock:
            market_cache["sectors"] = {"status": "unavailable", "data": None, "message": f"板块获取失败：{type(exc).__name__}: {str(exc)[:120]}"}
    finally:
        sector_fetch_running = False
        sector_fetch_lock.release()

def start_background_fetch(force=False):
    global market_last_attempt
    now = datetime.now(TZ).timestamp()
    with cache_lock:
        status = market_cache["status"]
    if not force and now - market_last_attempt < (600 if status in ("ok", "partial") else 60):
        return
    market_last_attempt = now
    threading.Thread(target=fetch_market_data, daemon=True).start()

def start_background_sector_fetch(force=False):
    global sector_last_attempt
    now = datetime.now(TZ).timestamp()
    with cache_lock:
        status = market_cache["sectors"]["status"]
    if not force and now - sector_last_attempt < (600 if status in ("ok", "partial") else 60):
        return
    sector_last_attempt = now
    threading.Thread(target=fetch_sector_data, daemon=True).start()

def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            side TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            shares INTEGER NOT NULL,
            price REAL NOT NULL,
            amount REAL NOT NULL,
            fee REAL NOT NULL
        )""")
        con.execute("""CREATE TABLE IF NOT EXISTS positions (
            code TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            shares INTEGER NOT NULL,
            avg_cost REAL NOT NULL
        )""")
        con.execute("""CREATE TABLE IF NOT EXISTS account (
            id INTEGER PRIMARY KEY CHECK(id=1),
            initial_cash REAL NOT NULL,
            cash REAL NOT NULL
        )""")
        con.execute("INSERT OR IGNORE INTO account(id, initial_cash, cash) VALUES(1, 50000, 50000)")
        con.commit()

init_db()

def quote_map():
    with cache_lock:
        rows = market_cache.get("stocks", {}).get("data") or []
    result = {}
    for row in rows:
        code = str(row.get("代码") or row.get("股票代码") or row.get("code") or "").strip()
        name = str(row.get("名称") or row.get("股票简称") or row.get("name") or code)
        price = as_float(row.get("最新价") or row.get("最新") or row.get("现价") or row.get("price"))
        change = as_float(row.get("涨跌幅") or row.get("涨幅"))
        if code and price is not None:
            result[code] = {"code": code, "name": name, "price": price, "change": change}
    return result

def account_snapshot():
    quotes = quote_map()
    with sqlite3.connect(DB_PATH) as con:
        con.row_factory = sqlite3.Row
        account = con.execute("SELECT * FROM account WHERE id=1").fetchone()
        positions = con.execute("SELECT * FROM positions ORDER BY code").fetchall()
        trades = con.execute("SELECT * FROM trades ORDER BY id DESC LIMIT 30").fetchall()
    cash = float(account["cash"])
    holdings = []
    market_value = 0.0
    cost_value = 0.0
    for p in positions:
        q = quotes.get(p["code"])
        price = q["price"] if q else None
        value = price * p["shares"] if price is not None else None
        cost = p["avg_cost"] * p["shares"]
        cost_value += cost
        if value is not None:
            market_value += value
        holdings.append({
            "code": p["code"], "name": p["name"], "shares": p["shares"],
            "avg_cost": p["avg_cost"], "latest_price": price, "market_value": value,
            "profit": (price - p["avg_cost"]) * p["shares"] if price is not None else None,
            "change_percent": ((price / p["avg_cost"]) - 1) * 100 if price is not None and p["avg_cost"] else None,
            "quote_status": "ok" if price is not None else "unavailable"
        })
    equity = cash + market_value
    initial = float(account["initial_cash"])
    profit = equity - initial
    return {
        "initial_cash": initial, "cash": cash, "market_value": market_value,
        "equity": equity, "profit": profit,
        "return_percent": profit / initial * 100 if initial else 0,
        "positions": holdings,
        "recent_trades": [dict(t) for t in trades],
        "message": "若持仓个股行情不可用，账户市值会暂时低估；本工具仅为模拟交易。"
    }

@app.get("/")
def index():
    return send_from_directory(str(BASE_DIR), "index.html")

@app.get("/api/health")
def health():
    return jsonify({"ok": True, "service": "A股每日复盘", "version": APP_VERSION, "timezone": "Asia/Shanghai", "time": now_iso()})

@app.get("/api/diagnostics")
def diagnostics():
    try:
        import akshare as ak
        version = getattr(ak, "__version__", "unknown")
        names = ["stock_zh_index_spot_em", "stock_zh_index_spot_sina", "stock_zh_a_spot_em", "stock_board_industry_name_em", "stock_board_concept_name_em"]
        funcs = {n: callable(getattr(ak, n, None)) for n in names}
    except Exception as exc:
        version = None
        funcs = {}
        error = f"{type(exc).__name__}: {str(exc)[:160]}"
        return jsonify({"app_version": APP_VERSION, "akshare_version": version, "functions": funcs, "error": error, "time": now_iso()})
    return jsonify({"app_version": APP_VERSION, "akshare_version": version, "functions": funcs, "time": now_iso()})

@app.get("/api/market")
def market():
    start_background_fetch()
    start_background_sector_fetch()
    with cache_lock:
        result = json.loads(json.dumps(market_cache, ensure_ascii=False, default=str))
    result["source"] = "AKShare"
    result["disclaimer"] = "仅展示数据源实际返回的数据。涨跌停数量为基于行情快照的估算，缺失数据不会虚构填充。"
    result["fetching"] = {"market": market_fetch_running, "sectors": sector_fetch_running}
    return jsonify(result)

@app.post("/api/refresh")
def refresh():
    start_background_fetch(force=True)
    start_background_sector_fetch(force=True)
    return jsonify({"ok": True, "message": "已请求后台刷新；数据需要等待行情接口返回。", "time": now_iso()})

@app.get("/api/sim/account")
def sim_account():
    return jsonify(account_snapshot())

@app.post("/api/sim/buy")
def sim_buy():
    body = request.get_json(silent=True) or {}
    code = str(body.get("code", "")).strip()
    try:
        shares = int(body.get("shares", 0))
    except Exception:
        shares = 0
    quotes = quote_map()
    q = quotes.get(code)
    if not q:
        return jsonify({"ok": False, "error": "没有该股票的有效实时/最近行情，请先刷新行情并核对股票代码。"}), 400
    if shares < 100 or shares % 100:
        return jsonify({"ok": False, "error": "买入股数须为100股的整数倍。"}), 400
    price = q["price"]
    amount = price * shares
    fee = max(5.0, amount * 0.0003)
    if amount > 10000:
        return jsonify({"ok": False, "error": "单只新买入金额不能超过约定的1万元仓位上限。"}), 400
    with sqlite3.connect(DB_PATH) as con:
        con.row_factory = sqlite3.Row
        acc = con.execute("SELECT * FROM account WHERE id=1").fetchone()
        old = con.execute("SELECT * FROM positions WHERE code=?", (code,)).fetchone()
        total_cost = amount + fee
        if total_cost > acc["cash"]:
            return jsonify({"ok": False, "error": "现金不足。"}), 400
        if old and (old["shares"] * old["avg_cost"] + amount) > 10000:
            return jsonify({"ok": False, "error": "该股票累计持仓成本将超过1万元上限。"}), 400
        new_cash = float(acc["cash"]) - total_cost
        con.execute("UPDATE account SET cash=? WHERE id=1", (new_cash,))
        if old:
            new_shares = int(old["shares"]) + shares
            avg = (old["avg_cost"] * old["shares"] + amount) / new_shares
            con.execute("UPDATE positions SET shares=?, avg_cost=?, name=? WHERE code=?", (new_shares, avg, q["name"], code))
        else:
            con.execute("INSERT INTO positions(code,name,shares,avg_cost) VALUES(?,?,?,?)", (code, q["name"], shares, price))
        con.execute("INSERT INTO trades(created_at,side,code,name,shares,price,amount,fee) VALUES(?,?,?,?,?,?,?,?)", (now_iso(), "BUY", code, q["name"], shares, price, amount, fee))
        con.commit()
    return jsonify({"ok": True, "message": "模拟买入已记录（不是真实委托）。", "account": account_snapshot()})

@app.post("/api/sim/sell")
def sim_sell():
    body = request.get_json(silent=True) or {}
    code = str(body.get("code", "")).strip()
    try:
        shares = int(body.get("shares", 0))
    except Exception:
        shares = 0
    quotes = quote_map()
    q = quotes.get(code)
    if not q:
        return jsonify({"ok": False, "error": "没有该股票的有效实时/最近行情，请先刷新行情。"}), 400
    if shares < 100 or shares % 100:
        return jsonify({"ok": False, "error": "卖出股数须为100股的整数倍。"}), 400
    price = q["price"]
    amount = price * shares
    fee = max(5.0, amount * 0.0003) + amount * 0.0005  # 模拟佣金+印花税，未含过户费
    with sqlite3.connect(DB_PATH) as con:
        con.row_factory = sqlite3.Row
        acc = con.execute("SELECT * FROM account WHERE id=1").fetchone()
        old = con.execute("SELECT * FROM positions WHERE code=?", (code,)).fetchone()
        if not old or shares > old["shares"]:
            return jsonify({"ok": False, "error": "持仓不足，不能卖出超过持有数量的股票。"}), 400
        con.execute("UPDATE account SET cash=? WHERE id=1", (float(acc["cash"]) + amount - fee,))
        left = old["shares"] - shares
        if left:
            con.execute("UPDATE positions SET shares=? WHERE code=?", (left, code))
        else:
            con.execute("DELETE FROM positions WHERE code=?", (code,))
        con.execute("INSERT INTO trades(created_at,side,code,name,shares,price,amount,fee) VALUES(?,?,?,?,?,?,?,?)", (now_iso(), "SELL", code, q["name"], shares, price, amount, fee))
        con.commit()
    return jsonify({"ok": True, "message": "模拟卖出已记录（不是真实委托）。", "account": account_snapshot()})

@app.get("/api/stocks")
def stocks():
    with cache_lock:
        rows = market_cache.get("stocks", {}).get("data") or []
    query = request.args.get("q", "").strip().lower()
    if query:
        rows = [r for r in rows if query in str(r.get("代码", r.get("股票代码", ""))).lower() or query in str(r.get("名称", r.get("股票简称", ""))).lower()]
    return jsonify({"status": "ok" if rows else "unavailable", "data": rows[:100], "count": len(rows), "message": "显示真实快照数据；仅返回前100条匹配结果。"})

@app.get("/api/report")
def report():
    with cache_lock:
        snapshot = json.loads(json.dumps(market_cache, ensure_ascii=False, default=str))
    sectors = snapshot.get("sectors", {}).get("data") or {}
    concept = sectors.get("concept") or []
    industry = sectors.get("industry") or []
    def field(row, keys):
        for k in keys:
            if row.get(k) not in (None, ""):
                return row[k]
        return None
    def rank(rows):
        normalized = []
        for row in rows:
            name = field(row, ["板块名称", "名称", "板块", "行业名称"])
            change = as_float(field(row, ["涨跌幅", "涨幅", "涨跌幅(%)"]))
            if name is not None and change is not None:
                normalized.append({"name": name, "change_percent": change, "leader": field(row, ["领涨股票", "领涨股"]), "leader_change": field(row, ["领涨股票-涨跌幅", "领涨股涨跌幅"])})
        return sorted(normalized, key=lambda x: x["change_percent"], reverse=True)
    top_industry, top_concept = rank(industry)[:10], rank(concept)[:10]
    return jsonify({
        "date": datetime.now(TZ).strftime("%Y-%m-%d"),
        "generated_at": now_iso(),
        "market_status": snapshot.get("status"),
        "market_message": snapshot.get("message"),
        "indices": snapshot.get("indices"),
        "breadth": snapshot.get("breadth"),
        "amount": snapshot.get("amount"),
        "limit_stats": snapshot.get("limit_stats"),
        "industry_top10": top_industry,
        "concept_top10": top_concept,
        "notes": [
            "板块排名依据接口返回的涨跌幅，盘中数据不等于收盘排名。",
            "板块领涨股不自动等同于龙一、龙二、龙三；还需核对连板高度、成交额、换手率、封板质量、题材持续性和消息来源。",
            "涨跌停数量如标注为估算，不能当作交易所官方精确统计。",
            "报告仅使用实际获取的数据，不会补造缺失行情。"
        ]
    })

@app.get("/api/test-sectors")
def test_sectors():
    # 不再逐个请求多个慢接口，避免诊断接口长时间占用服务。
    try:
        import akshare as ak
        names = ["stock_board_industry_name_em", "stock_board_concept_name_em", "stock_board_industry_index_ths", "stock_board_concept_index_ths"]
        return jsonify({name: {"exists": callable(getattr(ak, name, None))} for name in names})
    except Exception as exc:
        return jsonify({"error": f"{type(exc).__name__}: {str(exc)[:160]}"}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)

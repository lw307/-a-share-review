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

APP_VERSION = "20261021-sectorfix-v8"
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
    "sectors": {"status": "unavailable", "data": {"industry": [], "concept": []}, "message": "板块行情等待获取", "updated_at": None},
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

def normalize_stock_code(value):
    """把 sh600000/sz000001/bj920000 统一成六位数字代码。"""
    code = str(value or "").strip()
    code = re.sub(r"^(?:sh|sz|bj|SH|SZ|BJ)", "", code)
    code = re.sub(r"\.0$", "", code)
    if code.isdigit() and len(code) < 6:
        code = code.zfill(6)
    return code

def is_main_board_code(value):
    """仅允许沪深主板，排除创业板、科创板、北交所。"""
    code = normalize_stock_code(value)
    return code.startswith(("600", "601", "603", "605", "000", "001", "002", "003")) and len(code) == 6

def normalize_stock_snapshot(df, source_name):
    """Normalize supported quote-source columns to a shared schema."""
    import pandas as pd
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return None
    out = df.copy()
    aliases = {
        "代码": ["代码", "股票代码", "code", "symbol"],
        "名称": ["名称", "股票简称", "name"],
        "最新价": ["最新价", "最新", "现价", "trade", "price"],
        "涨跌幅": ["涨跌幅", "涨幅", "change_percent", "changepercent"],
        "成交额": ["成交额", "成交金额", "amount"],
        "成交量": ["成交量", "volume"],
        "今开": ["今开", "open"],
        "最高": ["最高", "high"],
        "最低": ["最低", "low"],
    }
    normalized = pd.DataFrame(index=out.index)
    for target, candidates in aliases.items():
        col = first_column(out, candidates)
        if col is not None:
            normalized[target] = out[col]
    if "代码" not in normalized.columns or "最新价" not in normalized.columns:
        return None
    normalized["代码"] = normalized["代码"].map(normalize_stock_code)
    normalized["_source"] = source_name
    return normalized

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

        # 个股快照：先尝试东方财富，失败后尝试新浪。统一列名后供统计与模拟账户使用。
        df_stocks = None
        for source_name, func_name in [
            ("东方财富", "stock_zh_a_spot_em"),
            ("新浪财经备用", "stock_zh_a_spot"),
        ]:
            try:
                fn = getattr(ak, func_name, None)
                if not callable(fn):
                    errors.append(f"{source_name}个股接口函数不存在：{func_name}")
                    continue
                raw = fn()
                candidate = normalize_stock_snapshot(raw, source_name)
                if candidate is None or candidate.empty:
                    raise RuntimeError("接口没有返回可识别的个股快照")
                df_stocks = candidate
                stocks = safe_records(candidate)
                break
            except Exception as exc:
                errors.append(f"{source_name}个股快照：{type(exc).__name__}: {str(exc)[:100]}")
        if df_stocks is None:
            errors.append("所有个股快照来源均失败；个股统计暂不可用")

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

        # 如果全市场个股快照失败，尝试用上证指数与深证成指记录中的成交额字段估算两市成交额。
        # 此值不用于替代个股快照或市场宽度统计，并明确标记来源和口径。
        if amount.get("status") == "unavailable" and unique_indices:
            major_amounts = []
            for row in unique_indices:
                name = str(row.get("名称") or row.get("指数名称") or row.get("name") or "")
                val = as_float(row.get("成交额") or row.get("成交金额") or row.get("amount"))
                if val is not None and any(k in name for k in ("上证指数", "上证综指", "深证成指")):
                    major_amounts.append((name, val))
            # Only use one Shanghai and one Shenzhen broad-market index to avoid duplicate index records.
            sh = next((v for n, v in major_amounts if "上证" in n), None)
            sz = next((v for n, v in major_amounts if "深证成指" in n), None)
            if sh is not None and sz is not None:
                amount = {
                    "status": "partial",
                    "data": {"总成交额": sh + sz, "单位": "元", "股票数": None},
                    "message": "个股快照不可用；按上证指数与深证成指行情记录中的成交额相加作为备用估算，口径可能与交易所统计有差异。"
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

def sector_frame_is_snapshot(df):
    """只接受含板块名称与涨跌幅的行情快照，不接受名称清单/历史事件表。"""
    if df is None or getattr(df, "empty", True):
        return False, "接口返回空数据"
    name_col = first_column(df, ["板块名称", "名称", "板块", "行业名称", "行业", "name"])
    change_col = first_column(df, ["涨跌幅", "涨幅", "涨跌幅(%)", "涨跌幅%", "涨跌幅（%）", "pct_change", "change_percent"])
    if not name_col:
        return False, f"缺少板块名称列；实际字段：{','.join(map(str, list(df.columns)[:12]))}"
    if not change_col:
        return False, f"缺少涨跌幅列；实际字段：{','.join(map(str, list(df.columns)[:12]))}"
    # 至少应有一定比例可转成数值，避免把新闻/事件表误当板块榜。
    valid = sum(as_float(v) is not None for v in df[change_col].head(20).tolist())
    if valid < min(3, len(df)):
        return False, "涨跌幅列没有足够的有效数值"
    return True, None

def fetch_concept_direct_em():
    """通过东方财富板块列表接口的小页快照作为备用源；仅接受真实返回的涨跌幅。"""
    import requests
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1", "pz": "40", "po": "1", "np": "1", "fltt": "2", "invt": "2",
        "fid": "f3", "fs": "m:90+t:3+f:!50",
        "fields": "f12,f14,f3,f104,f105,f128,f136"
    }
    last_error = None
    # 控制总耗时，避免 Render 免费服务的 HTTP 请求等待过久。
    for attempt in range(2):
        try:
            response = requests.get(
                url, params=params,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36", "Referer": "https://quote.eastmoney.com/center/boardlist.html"},
                timeout=(3, 6)
            )
            response.raise_for_status()
            payload = response.json()
            data = payload.get("data") or {}
            diff = data.get("diff") or []
            records = []
            for item in diff:
                name = item.get("f14")
                change = as_float(item.get("f3"))
                if not name or change is None:
                    continue
                records.append({
                    "板块代码": item.get("f12"),
                    "板块名称": name,
                    "涨跌幅": change,
                    "上涨家数": item.get("f104"),
                    "下跌家数": item.get("f105"),
                    "领涨股票": item.get("f128"),
                    "领涨股票-涨跌幅": item.get("f136"),
                    "数据来源": "东方财富概念板块列表接口（备用）"
                })
            if len(records) < 3:
                raise RuntimeError(f"返回有效板块不足3条；data.total={data.get('total')}")
            return records, None
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {str(exc)[:150]}"
            if attempt < 1:
                import time
                time.sleep(0.5)
    return [], last_error

def fetch_concept_sina():
    """使用新浪财经概念板块实时汇总作为东方财富不可用时的备用源。"""
    import akshare as ak
    fn = getattr(ak, "stock_sector_spot", None)
    if not callable(fn):
        raise RuntimeError("AKShare 中不存在 stock_sector_spot")
    df = fn(indicator="概念")
    valid, reason = sector_frame_is_snapshot(df)
    if not valid:
        raise RuntimeError(reason or "新浪概念板块返回内容未通过行情快照校验")
    name_col = first_column(df, ["板块名称", "板块", "名称", "name"])
    change_col = first_column(df, ["涨跌幅", "涨幅", "pct_change", "change_percent"])
    records = []
    for row in safe_records(df):
        name = row.get(name_col)
        change = as_float(row.get(change_col))
        if not name or change is None:
            continue
        row["板块名称"] = name
        row["涨跌幅"] = change
        # 新浪字段代表该概念当前领涨个股及其涨幅；代码保留用于主板校验。
        if row.get("领涨股票") in (None, ""):
            row["领涨股票"] = row.get("股票名称")
        if row.get("领涨股票代码") in (None, ""):
            row["领涨股票代码"] = row.get("股票代码")
        if row.get("领涨股票-涨跌幅") in (None, ""):
            row["领涨股票-涨跌幅"] = row.get("个股-涨跌幅")
        row["数据来源"] = "新浪财经概念板块实时汇总"
        records.append(row)
    if len(records) < 10:
        raise RuntimeError(f"新浪概念板块有效记录不足10条：{len(records)}")
    return records

def fetch_sector_data():
    """获取板块当日快照。行业增加同花顺实时汇总兜底；概念不拿历史事件表冒充实时排名。"""
    global sector_fetch_running, sector_last_attempt
    if not sector_fetch_lock.acquire(blocking=False):
        return
    sector_fetch_running = True
    sector_last_attempt = datetime.now(TZ).timestamp()
    try:
        import akshare as ak
        result = {"industry": [], "concept": []}
        source_used = {}
        errors = []
        sources = {
            # stock_board_industry_name_ths 只是行业名称列表，不能当行情榜；summary_ths 才是行情汇总。
            "industry": [
                ("东方财富行业快照", "stock_board_industry_name_em", None),
                ("同花顺行业实时汇总", "stock_board_industry_summary_ths", None),
            ],
            # 概念优先使用东方财富实时快照；名称清单不再作为行情来源。
            "concept": [
                ("东方财富概念实时快照", "stock_board_concept_spot_em", None),
            ],
        }
        for key, providers in sources.items():
            for source_label, func_name, args in providers:
                try:
                    fn = getattr(ak, func_name, None)
                    if not callable(fn):
                        errors.append(f"{key}/{func_name}：函数不存在")
                        continue
                    df = fn(**(args or {}))
                    valid, reason = sector_frame_is_snapshot(df)
                    if not valid:
                        raise RuntimeError(reason or "非有效板块行情快照")
                    # Normalize only key columns; preserve all original fields for the UI/report.
                    name_col = first_column(df, ["板块名称", "名称", "板块", "行业名称", "行业", "name"])
                    change_col = first_column(df, ["涨跌幅", "涨幅", "涨跌幅(%)", "涨跌幅%", "涨跌幅（%）", "pct_change", "change_percent"])
                    records = safe_records(df)
                    normalized = []
                    for row in records:
                        if row.get("板块名称") in (None, ""):
                            row["板块名称"] = row.get(name_col)
                        if row.get("涨跌幅") in (None, ""):
                            row["涨跌幅"] = row.get(change_col)
                        # Normalize optional leader aliases for the frontend.
                        if row.get("领涨股票") in (None, ""):
                            row["领涨股票"] = row.get("领涨股") or row.get("领涨股票名称")
                        if row.get("领涨股票-涨跌幅") in (None, ""):
                            row["领涨股票-涨跌幅"] = row.get("领涨股涨跌幅")
                        normalized.append(row)
                    result[key] = normalized
                    source_used[key] = source_label
                    break
                except Exception as exc:
                    errors.append(f"{key}/{func_name}：{type(exc).__name__}: {str(exc)[:100]}")
        # 东方财富失败后，先切换新浪概念实时汇总，再尝试东方财富直连接口。
        if not result["concept"]:
            try:
                result["concept"] = fetch_concept_sina()
                source_used["concept"] = "新浪财经概念板块实时汇总"
            except Exception as exc:
                errors.append(f"concept/sina_fallback：{type(exc).__name__}: {str(exc)[:120]}")
        if not result["concept"]:
            direct_records, direct_error = fetch_concept_direct_em()
            if direct_records:
                result["concept"] = direct_records
                source_used["concept"] = "东方财富概念板块直接快照备用（前40名）"
            else:
                errors.append(f"concept/direct_em_fallback：{direct_error or '无有效数据'}")

        available = bool(result["industry"] or result["concept"])
        both = bool(result["industry"] and result["concept"])
        if both:
            status = "ok"
            message = "行业和概念板块实时快照均已获取"
        elif result["industry"]:
            status = "partial"
            message = "行业板块排名已恢复；概念板块数据源当前不可用，未使用历史事件表伪造排名"
        elif result["concept"]:
            status = "partial"
            message = "概念板块排名可用；行业板块数据源当前不可用"
        else:
            status = "unavailable"
            message = "未取得有效的当日板块涨跌幅快照"
        details = []
        if source_used:
            details.append("成功来源：" + "、".join(f"{k}={v}" for k, v in source_used.items()))
        if errors:
            details.append("接口提示：" + "；".join(errors[:4]))
        with cache_lock:
            sector_updated = now_iso()
            market_cache["sectors"] = {
                "status": status,
                "data": result if available else None,
                "message": message + (("；" + "；".join(details)) if details else ""),
                "updated_at": sector_updated,
                "source_used": source_used,
                "errors": errors[:8],
            }
    except Exception as exc:
        with cache_lock:
            market_cache["sectors"] = {"status": "unavailable", "data": None, "message": f"板块获取失败：{type(exc).__name__}: {str(exc)[:120]}", "updated_at": now_iso(), "source_used": {}, "errors": [f"{type(exc).__name__}: {str(exc)[:160]}"]}
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
        code = normalize_stock_code(row.get("代码") or row.get("股票代码") or row.get("code") or "")
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
        names = [
            "stock_zh_index_spot_em", "stock_zh_index_spot_sina", "stock_zh_a_spot_em", "stock_zh_a_spot",
            "stock_board_industry_name_em", "stock_board_concept_name_em", "stock_board_industry_name_ths",
            "stock_board_concept_name_ths", "stock_board_industry_summary_ths", "stock_board_concept_spot_em",
            "stock_board_concept_index_ths", "stock_board_concept_summary_ths",
        ]
        funcs = {n: {"exists": callable(getattr(ak, n, None))} for n in names}
    except Exception as exc:
        version = None
        funcs = {}
        error = f"{type(exc).__name__}: {str(exc)[:160]}"
        return jsonify({"app_version": APP_VERSION, "akshare_version": version, "functions": funcs, "error": error, "time": now_iso()})
    with cache_lock:
        market_updated = market_cache.get("updated_at")
        sectors = json.loads(json.dumps(market_cache.get("sectors", {}), ensure_ascii=False, default=str))
        market_status = market_cache.get("status")
        market_message = market_cache.get("message")
    return jsonify({
        "app_version": APP_VERSION,
        "akshare_version": version,
        "functions": funcs,
        "cache": {
            "market_status": market_status,
            "market_updated_at": market_updated,
            "market_message": market_message,
            "sector_status": sectors.get("status"),
            "sector_updated_at": sectors.get("updated_at"),
            "sector_message": sectors.get("message"),
            "sector_source_used": sectors.get("source_used", {}),
            "sector_errors": sectors.get("errors", []),
            "market_fetch_running": market_fetch_running,
            "sector_fetch_running": sector_fetch_running,
        },
        "time": now_iso(),
    })

@app.get("/api/market")
def market():
    start_background_fetch()
    start_background_sector_fetch()
    with cache_lock:
        result = json.loads(json.dumps(market_cache, ensure_ascii=False, default=str))
    result["source"] = "AKShare（按接口可用情况尝试多数据源）"
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
    code = normalize_stock_code(body.get("code", ""))
    if not is_main_board_code(code):
        return jsonify({"ok": False, "error": "本模拟账户只允许沪深主板股票（沪市600/601/603/605、深市000/001/002/003）；不含创业板、科创板和北交所。"}), 400
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
    code = normalize_stock_code(body.get("code", ""))
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
    # 选股接口只给用户约定的沪深主板，不把创业板/科创板/北交所混入候选。
    rows = [r for r in rows if is_main_board_code(r.get("代码") or r.get("股票代码") or r.get("code"))]
    query = request.args.get("q", "").strip().lower()
    if query:
        rows = [r for r in rows if query in normalize_stock_code(r.get("代码", r.get("股票代码", ""))).lower() or query in str(r.get("名称", r.get("股票简称", ""))).lower()]
    return jsonify({"status": "ok" if rows else "unavailable", "data": rows[:100], "count": len(rows), "message": "仅展示沪深主板真实行情；已排除创业板、科创板和北交所；仅返回前100条匹配结果。"})

@app.get("/api/report")
def report():
    # Trigger background refresh if cache is empty/stale; report itself stays responsive.
    start_background_fetch()
    start_background_sector_fetch()
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
            name = field(row, ["板块名称", "名称", "板块", "行业名称", "行业", "name"])
            change = as_float(field(row, ["涨跌幅", "涨幅", "涨跌幅(%)", "涨跌幅%", "涨跌幅（%）", "pct_change", "change_percent"]))
            if name is not None and change is not None:
                normalized.append({
                    "name": name,
                    "change_percent": change,
                    "leader": field(row, ["领涨股票", "领涨股", "领涨股票名称"]),
                    "leader_change": field(row, ["领涨股票-涨跌幅", "领涨股涨跌幅"]),
                    "leader_code": field(row, ["领涨股票代码", "领涨股代码", "领涨股票-代码", "领涨股-代码"]),
                })
        return sorted(normalized, key=lambda x: x["change_percent"], reverse=True)
    # Sector leader names are not promoted as recommendations unless a stock code is present
    # and that code is confirmed to belong to the user's allowed Shanghai/Shenzhen main boards.
    def validate_leaders(rows):
        out = []
        for row in rows:
            item = dict(row)
            raw_code = field(item, ["leader_code", "领涨股票代码", "领涨股代码", "领涨股票-代码", "代码", "股票代码", "code"])
            code = normalize_stock_code(raw_code) if raw_code not in (None, "") else None
            item["leader_code"] = code
            item["leader_verified_main_board"] = bool(code and is_main_board_code(code))
            if not item["leader_verified_main_board"]:
                item["leader"] = None
                item["leader_change"] = None
            out.append(item)
        return out
    top_industry, top_concept = validate_leaders(rank(industry)[:10]), validate_leaders(rank(concept)[:10])
    def age_seconds(iso_value):
        if not iso_value:
            return None
        try:
            return max(0, int((datetime.now(TZ) - datetime.fromisoformat(iso_value)).total_seconds()))
        except Exception:
            return None
    market_updated_at = snapshot.get("updated_at")
    sector_updated_at = (snapshot.get("sectors") or {}).get("updated_at")
    market_age = age_seconds(market_updated_at)
    sector_age = age_seconds(sector_updated_at)
    freshness = {
        "market_updated_at": market_updated_at,
        "market_age_seconds": market_age,
        "market_is_stale": market_age is None or market_age > 1800,
        "sector_updated_at": sector_updated_at,
        "sector_age_seconds": sector_age,
        "sector_is_stale": sector_age is None or sector_age > 1800,
        "note": "超过30分钟的数据标记为可能过期；盘中快照不等于最终收盘数据。"
    }
    return jsonify({
        "date": datetime.now(TZ).strftime("%Y-%m-%d"),
        "generated_at": now_iso(),
        "freshness": freshness,
        "market_status": snapshot.get("status"),
        "market_message": snapshot.get("message"),
        "indices": snapshot.get("indices"),
        "breadth": snapshot.get("breadth"),
        "amount": snapshot.get("amount"),
        "limit_stats": snapshot.get("limit_stats"),
        "industry_top10": top_industry,
        "concept_top10": top_concept,
        "notes": [
            "板块排名依据通过字段校验的当日行情快照；行业板块可使用同花顺实时汇总兜底，盘中数据不等于收盘排名。",
            "若概念板块接口不可用，概念榜会明确留空；不会把同花顺历史概念事件表或名称列表误当成当日涨幅排名。",
            "板块接口返回的领涨股只有在返回股票代码且确认属于沪深主板时才显示；无法核验的领涨股会留空，避免把创业板、科创板或北交所个股混入候选。",
            "超过30分钟的数据会标记为可能过期；生成时间不代表行情时间，盘中快照不等于收盘数据。",
            "概念板块备用源必须提供有效的当日涨跌幅；同花顺名称清单或历史指数序列不会被伪装成实时概念排名。",
            "涨跌停数量如标注为估算，不能当作交易所官方精确统计。",
            "报告仅使用实际获取的数据，不会补造缺失行情。"
        ]
    })

@app.get("/api/test-sectors")
def test_sectors():
    """轻量诊断：列出板块相关接口是否存在和实际应用的优先路径。"""
    try:
        import akshare as ak
        names = [
            "stock_board_industry_name_em", "stock_board_industry_summary_ths",
            "stock_board_concept_name_em", "stock_board_concept_spot_em",
            "stock_board_industry_name_ths", "stock_board_concept_name_ths",
            "stock_board_concept_summary_ths", "stock_board_concept_index_ths", "stock_sector_spot",
        ]
        functions = {name: {"exists": callable(getattr(ak, name, None))} for name in names}
        with cache_lock:
            sector_cache = json.loads(json.dumps(market_cache.get("sectors", {}), ensure_ascii=False, default=str))
            sector_status = sector_cache.get("status")
            sector_message = sector_cache.get("message")
        return jsonify({
            "app_version": APP_VERSION,
            "functions": functions,
            "sector_cache_status": sector_status,
            "sector_cache_message": sector_message,
            "sector_updated_at": sector_cache.get("updated_at"),
            "sector_source_used": sector_cache.get("source_used", {}),
            "sector_errors": sector_cache.get("errors", []),
            "fetch_running": {"market": market_fetch_running, "sectors": sector_fetch_running},
            "strategy": {
                "industry": "东方财富行业快照优先；失败时使用同花顺行业实时汇总",
                "concept": "优先尝试 stock_board_concept_spot_em；失败后尝试新浪 stock_sector_spot(indicator=概念)；再尝试东方财富直接接口小分页。名称清单、历史指数和事件表不会被当作实时排名",
            },
            "time": now_iso(),
        })
    except Exception as exc:
        return jsonify({"app_version": APP_VERSION, "error": f"{type(exc).__name__}: {str(exc)[:160]}", "time": now_iso()}), 500


@app.get("/api/test-concept-fallback")
def test_concept_fallback():
    """手动测试东方财富概念板块直接快照备用源，不修改正式缓存。"""
    try:
        records, error = fetch_concept_direct_em()
        valid, note = sector_frame_is_snapshot(__import__("pandas").DataFrame(records)) if records else (False, error or "无数据")
        return jsonify({
            "app_version": APP_VERSION,
            "source": "Eastmoney direct concept snapshot fallback",
            "row_count": len(records),
            "snapshot_validation_passed": bool(valid),
            "columns": list(records[0].keys()) if records else [],
            "preview_first_5_rows": records[:5],
            "validation_note": note,
            "error": error,
            "warning": "本端点只做诊断，不修改正式板块缓存；当前备用源只请求涨幅榜前40条。",
            "time": now_iso(),
        })
    except Exception as exc:
        return jsonify({"app_version": APP_VERSION, "error": f"{type(exc).__name__}: {str(exc)[:180]}", "time": now_iso()}), 200


@app.get("/api/test-concept-sina")
def test_concept_sina():
    """手动测试新浪财经概念板块实时汇总；只返回诊断结果，不修改正式缓存。"""
    try:
        records = fetch_concept_sina()
        return jsonify({
            "app_version": APP_VERSION,
            "source": "Sina Finance stock_sector_spot(indicator=概念)",
            "row_count": len(records),
            "snapshot_validation_passed": len(records) >= 10 and all(as_float(x.get("涨跌幅")) is not None for x in records[:20]),
            "columns": list(records[0].keys()) if records else [],
            "preview_first_5_rows": records[:5],
            "warning": "本接口只做诊断，不修改正式板块缓存；是否接入正式排名还需核对线上返回字段和数据时效。",
            "time": now_iso(),
        })
    except Exception as exc:
        return jsonify({
            "app_version": APP_VERSION,
            "source": "Sina Finance stock_sector_spot(indicator=概念)",
            "row_count": 0,
            "snapshot_validation_passed": False,
            "error": f"{type(exc).__name__}: {str(exc)[:180]}",
            "warning": "本接口只做诊断，不修改正式板块缓存。",
            "time": now_iso(),
        }), 200


@app.get("/api/test-concept-source")
def test_concept_source():
    """手动诊断同花顺概念汇总接口；只查看字段，不把结果直接当作实时排名。"""
    try:
        import akshare as ak
        func_name = "stock_board_concept_summary_ths"
        fn = getattr(ak, func_name, None)
        if not callable(fn):
            return jsonify({"app_version": APP_VERSION, "source": func_name, "exists": False, "time": now_iso()})
        df = fn()
        columns = [str(c) for c in getattr(df, "columns", [])]
        rows = safe_records(df.head(3)) if df is not None and hasattr(df, "head") else []
        valid, reason = sector_frame_is_snapshot(df)
        return jsonify({
            "app_version": APP_VERSION,
            "source": func_name,
            "exists": True,
            "row_count": int(len(df)) if df is not None else 0,
            "columns": columns,
            "snapshot_validation_passed": bool(valid),
            "validation_note": reason or "字段检查通过；仍需人工确认其语义与时间口径后才能用于排名",
            "preview_first_3_rows": rows,
            "warning": "此接口仅用于诊断字段结构；本端点不会把返回内容直接写入板块排名。",
            "time": now_iso(),
        })
    except Exception as exc:
        return jsonify({
            "app_version": APP_VERSION,
            "source": "stock_board_concept_summary_ths",
            "error": f"{type(exc).__name__}: {str(exc)[:220]}",
            "time": now_iso(),
        }), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)

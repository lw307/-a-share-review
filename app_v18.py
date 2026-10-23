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

APP_VERSION = "20261023-mainboard-v19"
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
    "source_diagnostics": {"checked_at": None, "attempts": [], "stock_snapshot_rows": 0, "index_rows": 0},
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
        source_attempts = []
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
                    msg = f"{source_name}指数接口返回空数据"
                    errors.append(msg)
                    source_attempts.append({"category": "indices", "source": source_name, "function": func_name, "status": "empty", "row_count": 0, "error": msg})
                    continue
                code_col = first_column(df, ["代码", "指数代码", "symbol", "code"])
                name_col = first_column(df, ["名称", "指数名称", "name"])
                if not code_col and not name_col:
                    msg = f"{source_name}指数数据缺少名称/代码列"
                    errors.append(msg)
                    source_attempts.append({"category": "indices", "source": source_name, "function": func_name, "status": "invalid_schema", "row_count": len(df), "columns": list(map(str, list(df.columns)[:20])), "error": msg})
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
                source_attempts.append({"category": "indices", "source": source_name, "function": func_name, "status": "ok" if len(picked) else "no_target_rows", "row_count": len(df), "matched_rows": len(picked), "columns": list(map(str, list(df.columns)[:20]))})
            except Exception as exc:
                msg = f"{source_name}指数：{type(exc).__name__}: {str(exc)[:100]}"
                errors.append(msg)
                source_attempts.append({"category": "indices", "source": source_name, "function": func_name, "status": "error", "error": msg})

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
                    cols = list(map(str, list(raw.columns)[:20])) if raw is not None and hasattr(raw, "columns") else []
                    msg = "接口没有返回可识别的个股快照"
                    source_attempts.append({"category": "stocks", "source": source_name, "function": func_name, "status": "invalid_schema", "row_count": int(len(raw)) if raw is not None and hasattr(raw, "__len__") else 0, "columns": cols, "error": msg})
                    raise RuntimeError(msg)
                if len(candidate) < 100:
                    msg = f"快照仅返回{len(candidate)}行，低于全市场快照最低校验阈值100行"
                    source_attempts.append({"category": "stocks", "source": source_name, "function": func_name, "status": "insufficient_rows", "row_count": len(candidate), "columns": list(map(str, list(raw.columns)[:20])), "error": msg})
                    raise RuntimeError(msg)
                df_stocks = candidate
                stocks = safe_records(candidate)
                source_attempts.append({"category": "stocks", "source": source_name, "function": func_name, "status": "ok", "row_count": len(candidate), "columns": list(map(str, list(raw.columns)[:20]))})
                break
            except Exception as exc:
                msg = f"{source_name}个股快照：{type(exc).__name__}: {str(exc)[:100]}"
                errors.append(msg)
                # Avoid duplicating an already-recorded schema/row-count diagnostic.
                if not source_attempts or source_attempts[-1].get("function") != func_name or source_attempts[-1].get("category") != "stocks":
                    source_attempts.append({"category": "stocks", "source": source_name, "function": func_name, "status": "error", "error": msg})
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
            market_cache["source_diagnostics"] = {
                "checked_at": now_iso(),
                "attempts": source_attempts[:12],
                "stock_snapshot_rows": len(stocks),
                "index_rows": len(unique_indices),
                "note": "个股快照至少100行才作为全市场统计来源；接口错误、字段不匹配和样本不足分别记录。"
            }
            market_cache["updated_at"] = now_iso()
            available = sum(market_cache[k]["status"] in ("ok", "partial") for k in ["indices", "breadth", "amount", "limit_stats"])
            market_cache["status"] = "ok" if available == 4 else ("partial" if available else "unavailable")
            market_cache["message"] = "真实行情数据已更新；缺失项目会单独标注。" + ((" 数据源提示：" + "；".join(errors[:3])) if errors else "")
    except Exception as exc:
        with cache_lock:
            market_cache["status"] = "unavailable"
            market_cache["message"] = f"行情抓取失败：{type(exc).__name__}: {str(exc)[:180]}"
            market_cache["source_diagnostics"] = {"checked_at": now_iso(), "attempts": [{"category": "market", "status": "error", "error": f"{type(exc).__name__}: {str(exc)[:180]}"}], "stock_snapshot_rows": 0, "index_rows": 0}
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
       # 优先获取已经通过诊断测试的概念板块数据，
# 避免行业接口异常时，概念数据迟迟无法进入正式缓存。
sources = {
    "concept": [
        ("东方财富概念实时快照", "stock_board_concept_spot_em", None),
    ],
    "industry": [
        ("东方财富行业实时快照", "stock_board_industry_spot_em", None),
        ("同花顺行业实时汇总", "stock_board_industry_summary_ths", None),
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
        con.execute("""CREATE TABLE IF NOT EXISTS sector_daily_history (
            trade_date TEXT NOT NULL, sector_type TEXT NOT NULL, sector_name TEXT NOT NULL,
            change_percent REAL, strength_score REAL, coverage_percent REAL,
            leader_code TEXT, leader_name TEXT, PRIMARY KEY(trade_date, sector_type, sector_name)
        )""")
        con.execute("""CREATE TABLE IF NOT EXISTS stock_daily_history (
            trade_date TEXT NOT NULL, code TEXT NOT NULL, name TEXT, change_percent REAL,
            latest_price REAL, amount REAL, open_price REAL, high_price REAL, low_price REAL,
            volume REAL, turnover_rate REAL, PRIMARY KEY(trade_date, code)
        )""")
        # Schema migration for existing Render/SQLite databases; safe to run on every startup.
        existing_cols = {r[1] for r in con.execute("PRAGMA table_info(stock_daily_history)").fetchall()}
        for col, typ in (("open_price", "REAL"), ("high_price", "REAL"), ("low_price", "REAL"), ("volume", "REAL"), ("turnover_rate", "REAL")):
            if col not in existing_cols:
                con.execute(f"ALTER TABLE stock_daily_history ADD COLUMN {col} {typ}")
        con.execute("""CREATE TABLE IF NOT EXISTS account_equity_history (
            record_date TEXT PRIMARY KEY, equity REAL NOT NULL, cash REAL NOT NULL,
            market_value REAL NOT NULL, profit REAL NOT NULL, return_percent REAL NOT NULL
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
        cost = p["avg_cost"] * p["shares"]
        # If a quote is unavailable, use cost basis as a clearly marked temporary estimate;
        # never silently value the holding at zero, which would distort total account equity.
        value = price * p["shares"] if price is not None else cost
        cost_value += cost
        market_value += value
        holdings.append({
            "code": p["code"], "name": p["name"], "shares": p["shares"],
            "avg_cost": p["avg_cost"], "latest_price": price, "market_value": value,
            "profit": (price - p["avg_cost"]) * p["shares"] if price is not None else None,
            "change_percent": ((price / p["avg_cost"]) - 1) * 100 if price is not None and p["avg_cost"] else None,
            "quote_status": "ok" if price is not None else "estimated_at_cost"
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
        "message": "持仓行情不可用时，暂按持仓成本估算市值并标记为估算；账户盈亏暂不计入该持仓的实时浮动盈亏。本工具仅为模拟交易。"
    }

@app.get("/")
def index():
    response = send_from_directory(str(BASE_DIR), "index.html")
    response.headers["Cache-Control"] = "no-cache"
    return response

@app.get("/manifest.json")
def web_manifest():
    return send_from_directory(str(BASE_DIR), "manifest.json", mimetype="application/manifest+json", max_age=3600)

@app.get("/apple-touch-icon.png")
def apple_touch_icon():
    return send_from_directory(str(BASE_DIR), "apple-touch-icon.png", mimetype="image/png", max_age=86400)

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

def save_daily_research_history(trade_date, industry_rows, concept_rows, stock_rows):
    """Persist only actual returned snapshots; history is incomplete if the app was not opened on prior sessions."""
    with sqlite3.connect(DB_PATH) as con:
        for sector_type, rows in (("industry", industry_rows), ("concept", concept_rows)):
            for r in rows:
                con.execute("""INSERT OR REPLACE INTO sector_daily_history
                    (trade_date, sector_type, sector_name, change_percent, strength_score, coverage_percent, leader_code, leader_name)
                    VALUES(?,?,?,?,?,?,?,?)""", (trade_date, sector_type, r.get("name"), r.get("change_percent"), r.get("strength_score"),
                    (r.get("score_data_quality") or {}).get("coverage_percent"), r.get("leader_code"), r.get("leader")))
        # Save the main-board universe snapshot actually returned by the source, not just the top-ten watchlist.
        for stock in stock_rows:
            code = normalize_stock_code(stock.get("代码") or stock.get("股票代码") or stock.get("code"))
            name = str(stock.get("名称") or stock.get("股票简称") or stock.get("name") or "")
            change = as_float(stock.get("涨跌幅") or stock.get("涨幅") or stock.get("change_percent"))
            price = as_float(stock.get("最新价") or stock.get("最新") or stock.get("现价"))
            amount = as_float(stock.get("成交额") or stock.get("成交金额") or stock.get("amount"))
            if is_main_board_code(code) and name and change is not None:
                open_price = as_float(stock.get("今开") or stock.get("开盘") or stock.get("open"))
                high_price = as_float(stock.get("最高") or stock.get("最高价") or stock.get("high"))
                low_price = as_float(stock.get("最低") or stock.get("最低价") or stock.get("low"))
                volume = as_float(stock.get("成交量") or stock.get("成交股数") or stock.get("volume"))
                turnover = as_float(stock.get("换手率") or stock.get("turnover_rate"))
                con.execute("""INSERT OR REPLACE INTO stock_daily_history
                    (trade_date, code, name, change_percent, latest_price, amount, open_price, high_price, low_price, volume, turnover_rate)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                    (trade_date, code, name, change, price, amount, open_price, high_price, low_price, volume, turnover))
        con.commit()

def load_history(trade_date):
    with sqlite3.connect(DB_PATH) as con:
        con.row_factory = sqlite3.Row
        dates = [r[0] for r in con.execute("SELECT trade_date FROM (SELECT trade_date FROM sector_daily_history UNION SELECT trade_date FROM stock_daily_history) WHERE trade_date <= ? ORDER BY trade_date DESC LIMIT 5", (trade_date,)).fetchall()]
        sectors = [dict(r) for r in con.execute("SELECT * FROM sector_daily_history WHERE trade_date <= ? ORDER BY trade_date DESC", (trade_date,)).fetchall()]
        stocks = [dict(r) for r in con.execute("SELECT * FROM stock_daily_history WHERE trade_date <= ? ORDER BY trade_date DESC", (trade_date,)).fetchall()]
        trades = [dict(r) for r in con.execute("SELECT * FROM trades ORDER BY id DESC LIMIT 500").fetchall()]
        equity_rows = [dict(r) for r in con.execute("SELECT * FROM account_equity_history ORDER BY record_date").fetchall()]
    date_set = set(dates)
    return dates, [r for r in sectors if r["trade_date"] in date_set], [r for r in stocks if r["trade_date"] in date_set], trades, equity_rows

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
        """Normalize sector rows without inventing unavailable breadth/leader data."""
        normalized = []
        for row in rows:
            name = field(row, ["板块名称", "名称", "板块", "行业名称", "行业", "name"])
            change = as_float(field(row, ["涨跌幅", "涨幅", "涨跌幅(%)", "涨跌幅%", "涨跌幅（%）", "pct_change", "change_percent"]))
            if name is None or change is None:
                continue
            # Index/benchmark rows are not stock-sector themes and should not pollute sector rankings.
            normalized_name = str(name).strip().upper().replace(" ", "")
            if normalized_name in {"科创50", "创业板指", "上证指数", "上证50", "沪深300", "深证成指", "中证500", "中证1000", "北证50"}:
                continue
            leader_code = field(row, ["领涨股票代码", "领涨股代码", "领涨股票-代码", "股票代码", "代码", "code"])
            up_count = as_float(field(row, ["上涨家数", "上涨数量", "上涨公司数", "上涨家数(只)"]))
            down_count = as_float(field(row, ["下跌家数", "下跌数量", "下跌公司数", "下跌家数(只)"]))
            amount = as_float(field(row, ["总成交额", "成交额", "成交金额", "板块成交额"]))
            normalized.append({
                "name": str(name),
                "change_percent": change,
                "leader": field(row, ["领涨股票", "领涨股", "领涨股票名称", "股票名称"]),
                "leader_change": as_float(field(row, ["领涨股票-涨跌幅", "领涨股涨跌幅", "个股-涨跌幅"])),
                "leader_code": normalize_stock_code(leader_code) if leader_code not in (None, "") else None,
                "up_count": up_count,
                "down_count": down_count,
                "amount": amount,
                "source": field(row, ["数据来源"]) or (snapshot.get("sectors", {}).get("source_used", {}) or {}).get("concept"),
            })
        return normalized

    def theme_group(name):
        text = str(name or "")
        groups = [
            ("水产与水域", ("水产品", "水域改革", "水产", "海洋渔业")),
            ("农牧养殖", ("猪肉", "鸡肉", "养殖", "畜禽", "生猪")),
            ("软件与网络安全", ("国产软件", "信息安全", "网络安全", "信创", "软件服务", "网络游戏")),
            ("华为产业链", ("华为汽车", "华为鸿蒙", "华为概念", "华为海思", "华为算力")),
            ("通信与5G", ("5G", "宽带提速", "通信设备", "卫星通信")),
            ("军工与高端装备", ("国防军工", "军工", "航空装备", "航天装备")),
            ("核电与能源装备", ("核电", "核能", "核电核能")),
        ]
        for label, keys in groups:
            if any(k in text for k in keys):
                return label
        return text

    def score_sectors(rows):
        """Cross-sectional, explainable score. This is a same-day strength score, not a persistence prediction."""
        if not rows:
            return []
        def percentile(values, idx):
            valid = [v for v in values if v is not None]
            if len(valid) <= 1 or values[idx] is None:
                return None
            less_equal = sum(v <= values[idx] for v in valid)
            return 100.0 * (less_equal - 1) / max(1, len(valid) - 1)
        changes = [r.get("change_percent") for r in rows]
        amounts = [r.get("amount") for r in rows]
        breadths = []
        leader_changes = [r.get("leader_change") for r in rows]
        for r in rows:
            up, down = r.get("up_count"), r.get("down_count")
            breadths.append((up / (up + down) * 100) if up is not None and down is not None and up + down > 0 else None)
        for i, item in enumerate(rows):
            components = {"板块涨幅排名": percentile(changes, i)}
            if breadths[i] is not None:
                components["板块上涨广度"] = percentile(breadths, i)
            if amounts[i] is not None:
                components["成交额排名"] = percentile(amounts, i)
            if leader_changes[i] is not None:
                components["领涨股涨幅排名"] = percentile(leader_changes, i)
            weights = {"板块涨幅排名": 0.50, "板块上涨广度": 0.20, "成交额排名": 0.15, "领涨股涨幅排名": 0.15}
            active = [(weights[k], v) for k, v in components.items() if v is not None]
            active_weight = sum(w for w, _ in active)
            item["strength_score"] = round(sum(w * v for w, v in active) / active_weight, 1) if active else None
            coverage = round(active_weight / sum(weights.values()) * 100, 1)
            item["score_data_quality"] = {
                "coverage_percent": coverage,
                "available_components": list(components.keys()),
                "missing_components": [k for k in weights if k not in components],
                "status": "较完整" if coverage >= 95 else ("部分缺失" if coverage >= 70 else "数据不足"),
                "note": "评分只对可用指标按权重重新归一化；覆盖率较低时不宜与数据完整的板块直接等同比较。"
            }
            item["score_components"] = {k: round(v, 1) if v is not None else None for k, v in components.items()}
            item["theme_group"] = theme_group(item["name"])
            item["leader_verified_main_board"] = bool(item.get("leader_code") and is_main_board_code(item["leader_code"]))
            if not item["leader_verified_main_board"]:
                item["leader"] = None
                item["leader_change"] = None
            item["leader_status"] = "已核验沪深主板" if item["leader_verified_main_board"] else "未能核验为沪深主板，已隐藏"
            if item["strength_score"] is None:
                item["strength_label"] = "数据不足"
            elif item["strength_score"] >= 80:
                item["strength_label"] = "强势观察" if coverage >= 95 else "强势观察（评分部分缺失）"
            elif item["strength_score"] >= 60:
                item["strength_label"] = "相对偏强" if coverage >= 95 else "相对偏强（评分部分缺失）"
            elif item["strength_score"] >= 40:
                item["strength_label"] = "中性轮动" if coverage >= 95 else "中性轮动（评分部分缺失）"
            else:
                item["strength_label"] = "相对偏弱" if coverage >= 95 else "相对偏弱（评分部分缺失）"
            item["reason"] = "按当日板块涨幅为主，并在可用时纳入上涨广度、成交额及领涨股涨幅；分数仅用于同日相对比较。"
        return sorted(rows, key=lambda x: (x.get("strength_score") if x.get("strength_score") is not None else -1, x["change_percent"]), reverse=True)

    ranked_industry = score_sectors(rank(industry))
    ranked_concept = score_sectors(rank(concept))

    # Theme-level view: consolidate overlapping concept names so related tags do not masquerade as independent market leaders.
    grouped = {}
    for row in ranked_concept:
        group = row.get("theme_group") or row["name"]
        item = grouped.setdefault(group, {"name": group, "constituent_sectors": [], "scores": [], "changes": [], "verified_leaders": []})
        item["constituent_sectors"].append(row["name"])
        if row.get("strength_score") is not None:
            item["scores"].append(float(row["strength_score"]))
        if row.get("change_percent") is not None:
            item["changes"].append(float(row["change_percent"]))
        if row.get("leader_verified_main_board") and row.get("leader"):
            item["verified_leaders"].append({"name": row["leader"], "code": row["leader_code"], "change_percent": row.get("leader_change"), "sector": row["name"]})
    theme_strength_ranked = []
    for item in grouped.values():
        # Use the strongest constituent score rather than averaging many overlapping tags;
        # this avoids a theme being boosted merely because it has more similarly named concepts.
        item["score"] = round(max(item["scores"]), 1) if item["scores"] else None
        item["average_change_percent"] = round(sum(item["changes"]) / len(item["changes"]), 3) if item["changes"] else None
        item["constituent_count"] = len(item["constituent_sectors"])
        item["constituent_sectors"] = item["constituent_sectors"][:8]
        item["verified_leaders"] = sorted(item["verified_leaders"], key=lambda x: x.get("change_percent") if x.get("change_percent") is not None else -999, reverse=True)[:3]
        item["grouping_method"] = "组内最高单板块评分；不因概念标签数量叠加加分"
        item["interpretation"] = "归并观察值，不是独立资金流指标；概念可能重叠。组内最高分只代表最强组成板块，不代表整条主线的广度或持续性。"
        theme_strength_ranked.append(item)
    theme_strength_ranked.sort(key=lambda x: (x["score"] if x["score"] is not None else -1, x["average_change_percent"] if x["average_change_percent"] is not None else -999), reverse=True)

    # Keep legacy TOP10 sorted by raw daily change for the existing homepage;
    # the new score-sorted rankings are exposed separately for the next UI update.
    top_industry = sorted(ranked_industry, key=lambda x: x["change_percent"], reverse=True)[:10]
    top_concept = sorted(ranked_concept, key=lambda x: x["change_percent"], reverse=True)[:10]

    # Global main-board strength watchlist is explicitly NOT represented as sector dragon 2/3,
    # because the snapshot does not contain reliable sector-membership mapping.
    stock_rows = snapshot.get("stocks", {}).get("data") or []
    main_board_watchlist = []
    for stock in stock_rows:
        code = normalize_stock_code(stock.get("代码") or stock.get("股票代码") or stock.get("code"))
        name = str(stock.get("名称") or stock.get("股票简称") or stock.get("name") or "")
        change = as_float(stock.get("涨跌幅") or stock.get("涨幅") or stock.get("change_percent"))
        price = as_float(stock.get("最新价") or stock.get("最新") or stock.get("现价"))
        amount = as_float(stock.get("成交额") or stock.get("成交金额") or stock.get("amount"))
        if not is_main_board_code(code) or change is None or not name or "ST" in name.upper() or "退" in name:
            continue
        main_board_watchlist.append({
            "code": code, "name": name, "change_percent": change, "latest_price": price,
            "amount": amount, "type": "情绪强势观察（仅单日快照，非趋势确认）",
            "candidate_class": "单日强势观察",
            "reason": "仅按当前主板快照涨幅及可用成交额排序；没有足够历史K线时，不判定为趋势龙头。",
            "trend_status": "待历史K线与多日量价确认",
            "risk": "单日涨幅不能证明持续性；不因涨幅靠前直接追高，需核验板块共振、量价、位置及退出条件。"
        })
    main_board_watchlist.sort(key=lambda x: (x["change_percent"], x["amount"] or 0), reverse=True)
    main_board_watchlist = main_board_watchlist[:10]
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
    report_date = datetime.now(TZ).strftime("%Y-%m-%d")
    # Avoid turning stale quotes/weekends into fake daily history records. Holidays still require manual data-date verification.
    record_history_today = (not freshness["market_is_stale"] and datetime.now(TZ).weekday() < 5)
    if record_history_today:
        save_daily_research_history(report_date, ranked_industry, ranked_concept, stock_rows)
    history_dates, sector_history, stock_history, trade_history, equity_history = load_history(report_date)

    # Cross-day persistence is based on observed daily records only; never fill missing sessions with guesses.
    persistence_by_type = {}
    for sector_type in ("industry", "concept"):
        type_rows = [r for r in sector_history if r["sector_type"] == sector_type]
        per_name = {}
        for r in type_rows:
            item = per_name.setdefault(r["sector_name"], {"days_seen": 0, "top10_days": 0, "positive_days": 0, "changes": [], "scores": []})
            item["days_seen"] += 1
            if r.get("change_percent") is not None:
                item["changes"].append(float(r["change_percent"]))
                if float(r["change_percent"]) > 0: item["positive_days"] += 1
            if r.get("strength_score") is not None: item["scores"].append(float(r["strength_score"]))
        for d in history_dates:
            todays = sorted([r for r in type_rows if r["trade_date"] == d and r.get("strength_score") is not None], key=lambda x: x["strength_score"], reverse=True)[:10]
            for r in todays:
                per_name.setdefault(r["sector_name"], {"days_seen": 0, "top10_days": 0, "positive_days": 0, "changes": [], "scores": []})["top10_days"] += 1
        entries=[]
        for name, item in per_name.items():
            observed_days = int(item["days_seen"])
            # A one- or two-day sample is not enough to assess persistence. Keep the raw
            # diagnostics, but return no numeric score until at least three observations.
            raw_score = round((item["top10_days"] / max(1, len(history_dates))) * 70 +
                              (item["positive_days"] / max(1, observed_days)) * 30, 1)
            if observed_days < 3:
                persistence_score = None
                persistence_status = "样本不足，暂不评定" if observed_days < 3 else "初步观察"
            elif observed_days < 5:
                persistence_score = raw_score
                persistence_status = "初步观察：历史样本有限"
            else:
                persistence_score = raw_score
                persistence_status = "可评估：仍需结合市场环境判断"
            entries.append({"name": name, "observed_days": observed_days, "top10_days": item["top10_days"],
                "positive_days": item["positive_days"], "avg_change": round(sum(item["changes"])/len(item["changes"]), 2) if item["changes"] else None,
                "avg_score": round(sum(item["scores"])/len(item["scores"]), 1) if item["scores"] else None,
                "persistence_score": persistence_score, "persistence_status": persistence_status,
                "persistence_raw_score_for_diagnostics": raw_score})
        persistence_by_type[sector_type] = sorted(entries, key=lambda x: (x["persistence_score"] is not None, x["persistence_score"] if x["persistence_score"] is not None else -1, x["avg_change"] if x["avg_change"] is not None else -999), reverse=True)[:10]

    # Market emotion score with explicit data coverage; estimate-based limit statistics are marked as lower confidence.
    breadth_data = (snapshot.get("breadth") or {}).get("data") or {}
    limit_data = (snapshot.get("limit_stats") or {}).get("data") or {}
    up_n = as_float(breadth_data.get("上涨家数")); down_n = as_float(breadth_data.get("下跌家数"))
    limit_up = as_float(limit_data.get("涨停家数_估算")); limit_down = as_float(limit_data.get("跌停家数_估算"))
    index_rows = (snapshot.get("indices") or {}).get("data") or []
    def index_change(match):
        for rr in index_rows:
            nm = str(rr.get("名称") or rr.get("指数名称") or rr.get("name") or "")
            if match in nm:
                return as_float(rr.get("涨跌幅") or rr.get("涨幅") or rr.get("change_percent") or rr.get("pct_change"))
        return None
    sh_change, sz_change = index_change("上证"), index_change("深证")
    emotion_parts=[]; emotion_weights=[]
    if up_n is not None and down_n is not None and up_n+down_n>0:
        emotion_parts.append(100*up_n/(up_n+down_n)); emotion_weights.append(0.45)
    index_changes=[x for x in (sh_change, sz_change) if x is not None]
    if index_changes:
        avg_idx=sum(index_changes)/len(index_changes)
        emotion_parts.append(max(0,min(100,(avg_idx+3.0)/6.0*100))); emotion_weights.append(0.30)
    if limit_up is not None and limit_down is not None and limit_up+limit_down>0:
        emotion_parts.append(100*limit_up/(limit_up+limit_down)); emotion_weights.append(0.25)
    emotion_coverage=round(sum(emotion_weights)/1.0*100)
    emotion_score=round(sum(v*w for v,w in zip(emotion_parts,emotion_weights))/sum(emotion_weights),1) if emotion_weights else None
    breadth_ratio=(up_n/(up_n+down_n)) if up_n is not None and down_n is not None and up_n+down_n>0 else None
    market_stale=market_age is None or market_age>1800
    no_open_reasons=[]
    if emotion_score is None: no_open_reasons.append("情绪关键数据不足，暂不新开仓")
    elif emotion_score < 45: no_open_reasons.append(f"市场情绪评分仅 {emotion_score}/100，低于45分开仓观察线")
    if breadth_ratio is not None and breadth_ratio < 0.42: no_open_reasons.append(f"上涨家数占比仅 {breadth_ratio*100:.1f}%，市场赚钱效应偏弱")
    if down_n is not None and up_n is not None and down_n > up_n*1.25: no_open_reasons.append("下跌家数明显多于上涨家数")
    if sh_change is not None and sz_change is not None and sh_change < 0 and sz_change < 0 and (limit_up is not None and limit_down is not None and limit_up < limit_down*2): no_open_reasons.append("两大指数均下跌，涨停相对跌停优势不足")
    if emotion_coverage < 70: no_open_reasons.append("情绪数据覆盖率低于70%，为避免误判，明日不新开仓")
    if market_stale: no_open_reasons.append("行情超过30分钟未更新或更新时间未知，明日开盘前须重新核验")
    no_open = bool(no_open_reasons)
    emotion = {"score": emotion_score, "coverage_percent": emotion_coverage, "breadth_ratio_percent": round(breadth_ratio*100,1) if breadth_ratio is not None else None,
        "up_count": up_n, "down_count": down_n, "sh_change": sh_change, "sz_change": sz_change, "limit_up_estimate": limit_up, "limit_down_estimate": limit_down,
        "decision": "明日不开新仓" if no_open else "可观察，仍须满足个股条件", "no_open": no_open, "reasons": no_open_reasons,
        "method": "规则化情绪参考分，不是经过验证的预测模型；涨跌停数量若为估算会降低可靠性。"}

    # Provisional leader candidates use observable snapshot strength only. No false claim of sector membership.
    stock_by_code = {}
    for rr in stock_rows:
        cc=normalize_stock_code(rr.get("代码") or rr.get("股票代码") or rr.get("code"))
        nm=str(rr.get("名称") or rr.get("股票简称") or rr.get("name") or "")
        ch=as_float(rr.get("涨跌幅") or rr.get("涨幅") or rr.get("change_percent"))
        pr=as_float(rr.get("最新价") or rr.get("最新") or rr.get("现价"))
        am=as_float(rr.get("成交额") or rr.get("成交金额") or rr.get("amount"))
        if is_main_board_code(cc) and nm and ch is not None and "ST" not in nm.upper() and "退" not in nm and pr and pr>0:
            stock_by_code[cc]={"code":cc,"name":nm,"change_percent":ch,"latest_price":pr,"amount":am}
    stock_hist_by_code={}
    for r in stock_history:
        stock_hist_by_code.setdefault(r["code"],[]).append(r)
    leader_candidates=[]
    for item in stock_by_code.values():
        hist=stock_hist_by_code.get(item["code"],[])
        positive_days=sum(1 for h in hist if h.get("change_percent") is not None and h["change_percent"]>0)
        top_watch_days=sum(1 for h in hist if h.get("change_percent") is not None and h["change_percent"]>=5)
        amount_score=min(100, (math.log10(max(1,item["amount"] or 1))-4)*15) if item["amount"] else 0
        item.update({"observed_days":len(hist),"positive_days":positive_days,"strong_days":top_watch_days,
            "candidate_score":round(max(0,min(100, item["change_percent"]*4 + amount_score*0.25 + positive_days*3)),1),
            "classification":"当日涨幅/成交额强势候选；跨日记录不足时不称为趋势龙头" if len(hist)<3 else "短期强势候选；仍需人工核对K线、换手及题材带动"})
        leader_candidates.append(item)
    leader_candidates.sort(key=lambda x:(x["candidate_score"],x["change_percent"],x["amount"] or 0),reverse=True)
    leader_candidates=leader_candidates[:10]

    # v18: three transparent strategy lenses. Daily snapshots are NOT OHLCV candles;
    # therefore these are screening candidates, never confirmed breakouts or buy signals.
    strategy_models = {
        "generated_at": now_iso(),
        "data_limitations": [
            "v19 开始逐日保存真实行情快照中的开盘、最高、最低、价格、涨跌幅、成交额、成交量和换手率（数据源实际提供什么就保存什么）。",
            "数据库升级不会补造过去缺失的历史；新历史从程序成功记录的交易日开始累积，历史样本不完整时策略会主动少报或不报。",
            "快照中的最高/最低可能是盘中值，不应冒充最终日K；箱体突破、均线支撑等确认仍需完整、可靠的日线数据。",
            "候选分数用于同日筛选和复盘，不是盈利概率，也不保证次日上涨。"
        ],
        "models": []
    }
    strategy_buckets = {"趋势龙头回调": [], "箱体突破": [], "主线低位补涨": []}
    for item in stock_by_code.values():
        hist = sorted(stock_hist_by_code.get(item["code"], []), key=lambda x: x.get("trade_date", ""))
        changes = [as_float(h.get("change_percent")) for h in hist]
        changes = [v for v in changes if v is not None]
        observed = len(changes)
        pos_days = sum(v > 0 for v in changes[-5:])
        strong_days = sum(v >= 5 for v in changes[-5:])
        latest_change = item.get("change_percent")
        # These are deliberately conservative proxy scores based on available snapshots only.
        trend_score = max(0, min(100, 35 + pos_days * 9 + (12 if latest_change is not None and 0 <= latest_change <= 4 else 0) - (18 if latest_change is not None and latest_change >= 7 else 0)))
        breakout_score = max(0, min(100, 30 + (25 if latest_change is not None and latest_change >= 3 else 0) + (15 if item.get("amount") else 0) + min(20, strong_days * 5)))
        low_follow_score = max(0, min(100, 35 + (20 if latest_change is not None and -1 <= latest_change <= 3 else 0) + (10 if pos_days >= 2 else 0) + (10 if item.get("amount") else 0) - (15 if latest_change is not None and latest_change >= 7 else 0)))
        common = {"code": item["code"], "name": item["name"], "latest_price": item.get("latest_price"),
                  "change_percent": latest_change, "amount": item.get("amount"), "observed_days": observed,
                  "history_changes": changes[-5:], "confirmed_pattern": False,
                  "warning": "仅为快照代理筛选；缺少完整OHLCV/换手及板块成分映射，不能确认形态或直接据此买入。"}
        if observed >= 5 and pos_days >= 2 and latest_change is not None and -1.5 <= latest_change <= 4.5:
            strategy_buckets["趋势龙头回调"].append({**common, "score": round(trend_score, 1),
                "reason": f"最近最多5个已记录交易日中上涨{pos_days}天，当前涨幅处于温和区间；仅作为趋势回调观察，尚未核验均线与回调结构。",
                "entry_condition": "先补齐日K线；价格守住已验证的趋势支撑、回调缩量且再度放量转强后才进一步评估。",
                "invalid_condition": "跌破经K线确认的前低/趋势支撑，或板块主线明显退潮。"})
        if observed >= 5 and latest_change is not None and latest_change >= 3 and item.get("amount"):
            strategy_buckets["箱体突破"].append({**common, "score": round(breakout_score, 1),
                "reason": "当日涨幅与成交额快照较强；没有箱体高点及量能均值，故只列突破待核验，不认定已突破。",
                "entry_condition": "用至少20日真实日K线确认箱体上沿；收盘有效站上上沿且成交额/量能高于近期均值，再观察回踩承接。",
                "invalid_condition": "突破后收盘重新跌回箱体，或放量冲高回落且板块同步走弱。"})
        if observed >= 5 and latest_change is not None and -1 <= latest_change <= 3 and item.get("amount"):
            strategy_buckets["主线低位补涨"].append({**common, "score": round(low_follow_score, 1),
                "reason": "当日涨幅不极端且有成交额数据；因缺少可靠估值/阶段涨幅与板块成分映射，暂不能证明处于低位或具备补涨关系。",
                "entry_condition": "先确认所属主线仍居强势排名前列，再用多日涨幅、相对位置和量价结构排除高位滞涨股。",
                "invalid_condition": "主线跌出强势区、板块上涨广度恶化，或个股放量跌破近期结构低点。"})
    for model_name, candidates in strategy_buckets.items():
        candidates.sort(key=lambda x: (x["score"], x.get("amount") or 0), reverse=True)
        strategy_models["models"].append({"name": model_name, "status": "观察候选" if candidates else "当前无足够数据生成候选",
            "candidate_count": len(candidates), "candidates": candidates[:8],
            "score_note": "规则化代理评分，仅用于排序；不是统计学胜率或买入建议。"})

    # Trade review from recorded round trips using FIFO lots; no unrealized P&L is mixed into win rate.
    lots={}; closed=[]
    for t in sorted(trade_history,key=lambda x:x["id"]):
        code=t["code"]
        if t["side"]=="BUY": lots.setdefault(code,[]).append({"shares":int(t["shares"]),"unit_cost":(float(t["amount"])+float(t["fee"]))/max(1,int(t["shares"]))})
        elif t["side"]=="SELL":
            remain=int(t["shares"]); proceeds=(float(t["amount"])-float(t["fee"]))
            while remain>0 and lots.get(code):
                lot=lots[code][0]; take=min(remain,lot["shares"])
                cost=lot["unit_cost"]*take; pnl=proceeds*(take/max(1,int(t["shares"]))) - cost
                closed.append({"code":code,"name":t["name"],"shares":take,"pnl":pnl,"return_percent":pnl/max(1,cost)*100,"date":t["created_at"]})
                lot["shares"]-=take; remain-=take
                if lot["shares"]<=0: lots[code].pop(0)
    wins=sum(1 for x in closed if x["pnl"]>0)
    trade_review={"closed_lots":len(closed),"win_rate_percent":round(wins/len(closed)*100,1) if closed else None,
        "realized_pnl":round(sum(x["pnl"] for x in closed),2),"avg_return_percent":round(sum(x["return_percent"] for x in closed)/len(closed),2) if closed else None,
        "best_trade":max(closed,key=lambda x:x["pnl"]) if closed else None,"worst_trade":min(closed,key=lambda x:x["pnl"]) if closed else None,
        "equity_history_days":len(equity_history),"note":"只统计已完成的模拟卖出匹配批次；不含未实现盈亏。胜率样本很少时参考意义有限。"}

    # Record daily account equity for later drawdown analysis; current account endpoint stays the source of account valuation.
    try:
        acct=account_snapshot()
        if record_history_today:
            with sqlite3.connect(DB_PATH) as con:
                con.execute("INSERT OR REPLACE INTO account_equity_history(record_date,equity,cash,market_value,profit,return_percent) VALUES(?,?,?,?,?,?)",
                    (report_date, float(acct.get("equity",50000)), float(acct.get("cash",50000)), float(acct.get("market_value",0)), float(acct.get("profit",0)), float(acct.get("return_percent",0))))
                con.commit()
            equity_history.append({"record_date":report_date,"equity":acct.get("equity",50000)})
    except Exception:
        pass
    peak=None; max_drawdown=0.0
    for er in equity_history:
        eq=as_float(er.get("equity"))
        if eq is None: continue
        peak=eq if peak is None else max(peak,eq)
        if peak and eq<peak: max_drawdown=max(max_drawdown,(peak-eq)/peak*100)
    trade_review["max_drawdown_percent"] = round(max_drawdown,2) if equity_history else None
    trade_review["equity_history_days"] = len(equity_history)

    return jsonify({
        "date": report_date,
        "generated_at": now_iso(),
        "freshness": freshness,
        "market_source_diagnostics": snapshot.get("source_diagnostics") or {"checked_at": None, "attempts": [], "note": "尚无行情源诊断记录"},
        "fetch_running": {"market": market_fetch_running, "sectors": sector_fetch_running},
        "market_status": snapshot.get("status"),
        "market_message": snapshot.get("message"),
        "indices": snapshot.get("indices"),
        "breadth": snapshot.get("breadth"),
        "amount": snapshot.get("amount"),
        "limit_stats": snapshot.get("limit_stats"),
        "industry_top10": top_industry,
        "concept_top10": top_concept,
        "industry_strength_ranked": ranked_industry[:30],
        "concept_strength_ranked": ranked_concept[:30],
        "theme_strength_ranked": theme_strength_ranked[:15],
        "main_board_watchlist": main_board_watchlist,
        "main_board_watchlist_count": len(main_board_watchlist),
        "market_emotion": emotion,
        "sector_persistence": {"observed_dates": history_dates, "observation_count": len(history_dates), "recorded_today": record_history_today, "warning": "只统计应用实际记录的工作日新鲜快照；节假日仍需人工核对交易日，缺少历史记录不代表板块此前不强。持续性评分：单个板块有效观察少于3天时不提供数值评分；3-4天标记为初步观察；5天及以上才作为可评估样本，仍不构成预测保证。", "industry": persistence_by_type.get("industry", []), "concept": persistence_by_type.get("concept", [])},
        "leader_candidates": leader_candidates,
        "strategy_models": strategy_models,
        "next_day_plan": {"decision": "明日不开新仓" if no_open else "先观察再决定", "rules": ["开盘前复核市场情绪、指数及数据更新时间。", "若出现不开新仓提示，不建立新模拟仓位；只管理已有持仓风险。", "若允许观察，也必须等板块有共振、个股量价确认并设置止损；不在集合竞价盲目追高。", "计划持有约3天仅作为复盘假设，不构成必须持有的要求。"], "no_open": no_open},
        "trade_review": trade_review,
        "sector_scoring_method": {
            "title": "当日板块相对强度评分（0-100）",
            "weights": {"板块涨幅排名": "50%", "板块上涨广度排名": "20%（字段可用时）", "成交额排名": "15%（字段可用时）", "领涨股涨幅排名": "15%（字段可用时）"},
            "warning": "只比较本次接口返回的板块横截面，不代表资金流向的精确测量，也不预测未来收益。",
            "data_quality": "每个板块新增 score_data_quality.coverage_percent 和 missing_components；数据不完整时 strength_label 会注明评分部分缺失。",
            "theme_persistence": "持续性评分需要跨交易日历史数据；目前不以单日快照伪造持续性分数。",
            "theme_grouping": "v16 主线归并采用组内最高单板块评分，避免因相似概念标签较多而抬高分数；它不代表整条主线广度或持续性。",
            "leader_classification": "单日快照只生成情绪强势观察；没有历史K线与可靠成分股映射时，不冒称趋势龙头，也不虚构板块龙二/龙三。",
            "main_board_filter": "最终观察名单只保留沪市600/601/603/605和深市000/001/002/003代码，并排除ST及退市标记。",
            "v18_strategy_models": "趋势龙头回调、箱体突破、主线低位补涨三类观察模型；数据不足时不冒充形态确认。",
            "v19_history_and_validation": "扩展每日个股历史字段并兼容升级旧SQLite；三个模型统一要求至少5个实际记录交易日才展示候选，仍不等于完整日K回测。"
        },
        "dragon_analysis_status": {
            "dragon1": "仅在接口提供股票代码且代码属于沪深主板时展示经核验领涨股。",
            "dragon2_dragon3": "当前板块快照不含完整成分股映射，暂不编造龙二、龙三；全市场强势观察股另列 main_board_watchlist。"
        },
        "notes": [
            "板块排名依据通过字段校验的当日行情快照；行业板块可使用同花顺实时汇总兜底，盘中数据不等于收盘排名。",
            "若概念板块接口不可用，概念榜会明确留空；不会把同花顺历史概念事件表或名称列表误当成当日涨幅排名。",
            "板块接口返回的领涨股只有在返回股票代码且确认属于沪深主板时才显示；无法核验的领涨股会留空，避免把创业板、科创板或北交所个股混入候选。",
            "超过30分钟的数据会标记为可能过期；生成时间不代表行情时间，盘中快照不等于收盘数据。",
            "概念板块备用源必须提供有效的当日涨跌幅；同花顺名称清单或历史指数序列不会被伪装成实时概念排名。",
            "v15新增主线归并观察，用于识别重叠概念；归并分数只是组内可用板块评分均值，不代表资金流或持续性。",
            "v15过滤常见指数/基准名称，避免指数混入概念板块排名。",
            "全市场主板强势观察榜只依据主板个股快照排序，不代表其属于对应板块的龙二/龙三，也不是自动买入建议。",
            "当前数据源不含完整的板块成分股映射和跨日历史，无法可靠给出每个板块的龙二/龙三及主线持续性分数；缺失内容会明确说明。",
            "涨跌停数量如标注为估算，不能当作交易所官方精确统计。",
            "报告仅使用实际获取的数据，不会补造缺失行情。",
            "新增情绪规则：若情绪评分低于45、上涨广度偏弱、数据覆盖不足或行情过期，系统提示明日不开新仓；此为纪律提醒而非市场预测。",
            "板块持续性和个股跨日统计只基于本程序实际保存的历史快照；如果过去几天没有打开程序记录，历史样本不完整。",
            "龙头候选仅为全市场主板快照的量价强势候选；没有可靠板块成分映射时，不把个股冒称某板块龙一、龙二、龙三。"
        ]
    })

@app.get("/api/history-status")
def history_status():
    """Report actual persisted daily history coverage; never imply missing dates were backfilled."""
    with sqlite3.connect(DB_PATH) as con:
        con.row_factory = sqlite3.Row
        stock_days = con.execute("SELECT trade_date, COUNT(*) AS rows_count, COUNT(open_price) AS open_rows, COUNT(high_price) AS high_rows, COUNT(low_price) AS low_rows, COUNT(volume) AS volume_rows, COUNT(turnover_rate) AS turnover_rows FROM stock_daily_history GROUP BY trade_date ORDER BY trade_date DESC LIMIT 30").fetchall()
        total_stocks = con.execute("SELECT COUNT(*) FROM stock_daily_history").fetchone()[0]
        unique_days = con.execute("SELECT COUNT(DISTINCT trade_date) FROM stock_daily_history").fetchone()[0]
        sector_days = con.execute("SELECT COUNT(DISTINCT trade_date) FROM sector_daily_history").fetchone()[0]
    return jsonify({
        "app_version": APP_VERSION,
        "database_path": str(DB_PATH),
        "stock_history_rows": total_stocks,
        "stock_history_trade_days": unique_days,
        "sector_history_trade_days": sector_days,
        "recent_daily_coverage": [dict(r) for r in stock_days],
        "target": "建议先积累至少20个实际记录交易日；本接口只汇报数据库真实内容，不补造历史。",
        "persistence_warning": "若部署平台重启会清空本地数据库，必须将 SIM_DB_PATH 指向持久化磁盘路径，否则历史和模拟交易可能丢失。"
    })


@app.get("/api/market-diagnostics")
def market_diagnostics():
    """展示市场数据源逐项尝试结果，便于区分网络失败、字段不匹配与样本不足。"""
    with cache_lock:
        snapshot = json.loads(json.dumps(market_cache, ensure_ascii=False, default=str))
    return jsonify({
        "app_version": APP_VERSION,
        "time": now_iso(),
        "market_status": snapshot.get("status"),
        "market_updated_at": snapshot.get("updated_at"),
        "market_message": snapshot.get("message"),
        "indices": snapshot.get("indices"),
        "breadth": snapshot.get("breadth"),
        "amount": snapshot.get("amount"),
        "stocks_status": (snapshot.get("stocks") or {}).get("status"),
        "source_diagnostics": snapshot.get("source_diagnostics"),
        "fetch_running": {"market": market_fetch_running, "sectors": sector_fetch_running},
        "note": "本接口只读诊断，不会主动请求外部行情；请先打开 /api/market 或首页触发后台刷新。"
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

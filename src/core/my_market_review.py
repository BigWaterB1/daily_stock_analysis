# -*- coding: utf-8 -*-
"""
===================================
我的个性化大盘复盘模块 v2
===================================

功能：无 AI，纯数据报告，包含三部分：
  1. 市场总结：上涨/下跌/平盘家数、涨停/跌停、成交额（含与上日对比）
  2. 指数行情：上证/深证/创业板/上证50/沪深300/中证1000/微盘股
  3. 板块涨跌：同花顺行业 TOP10 涨 / TOP10 跌，每板块附带前3只股票

数据来源（免费、无需 Key）：
  - 指数：新浪财经  ak.stock_zh_index_spot_sina()
  - 微盘股：东财概念板块 → BK1158 HTTP fallback
  - 市场统计：东财  ak.stock_zh_a_spot_em() → 新浪 fallback
  - 前日成交额：ak.stock_market_trade_ma()
  - 同花顺行业：ak.stock_board_industry_summary_ths()
  - 板块成分股：ak.stock_board_industry_cons_em()（并发，失败降级 THS 领涨/领跌股）

颜色规范（中国股市惯例）：🔴 = 涨，🟢 = 跌
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Optional

import requests

from src.config import get_config
from src.notification import NotificationService
from src.search_service import SearchService
from src.analyzer import GeminiAnalyzer

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# 常量配置：需要展示的指数（新浪代码 → 显示名）
# ─────────────────────────────────────────────
INDEX_MAP = {
    'sh000001': '上证指数',
    'sz399001': '深证成指',
    'sz399006': '创业板指',
    'sh000016': '上证50',
    'sh000300': '沪深300',
    'sh000852': '中证1000',
}


# ─────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────

def _get_last_trading_date() -> str:
    """
    返回最近已收盘交易日的日期字符串（YYYY-MM-DD）。
    规则：若北京时间 < 15:00，则今日数据未就绪，取上一交易日。
    始终使用 Asia/Shanghai 时区，避免 GitHub Actions (UTC) 环境误判。
    使用新浪交易日历；失败时用工作日近似（不含节假日修正）。
    """
    import akshare as ak
    import pandas as pd
    from datetime import datetime, timedelta
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo

    tz_sh = ZoneInfo('Asia/Shanghai')
    now = datetime.now(tz_sh)
    # 15:00 前认为当日数据尚未完整，使用前一交易日
    cutoff = now.replace(hour=15, minute=0, second=0, microsecond=0)
    ref_date = now.date() if now >= cutoff else now.date() - timedelta(days=1)

    try:
        df = ak.tool_trade_date_hist_sina()
        col = df.columns[0]
        dates = pd.to_datetime(df[col]).dt.date
        past = dates[dates <= ref_date]
        if not past.empty:
            return past.max().strftime('%Y-%m-%d')
    except Exception as e:
        logger.debug(f"[交易日历] 获取失败，使用工作日近似: {e}")

    # 近似回退：找最近工作日
    for days_back in range(7):
        d = ref_date - timedelta(days=days_back)
        if d.weekday() < 5:
            return d.strftime('%Y-%m-%d')
    return ref_date.strftime('%Y-%m-%d')


def _c(text: str, pct: float) -> str:
    """用字体颜色表示涨跌（中国惯例：红=涨，绿=跌），兼容企业微信/邮件"""
    color = 'red' if pct >= 0 else 'green'
    return f'<font color="{color}">{text}</font>'


def _esc(text: str) -> str:
    """转义数据中的 markdown 特殊字符（* 会破坏粗体，如 *ST 股票名）"""
    return text.replace('*', '\\*')


# ─────────────────────────────────────────────
# 数据获取
# ─────────────────────────────────────────────

def _fetch_indices() -> list[dict]:
    """
    从新浪接口获取主要指数行情，同时附带微盘股行情。
    返回 list[dict]，每项含：name, current, change_pct, amount_yi（亿元）
    """
    import akshare as ak

    results = []

    try:
        df = ak.stock_zh_index_spot_sina()
        for code, name in INDEX_MAP.items():
            row = df[df['代码'] == code]
            if row.empty:
                continue
            row = row.iloc[0]
            amount_raw = float(row.get('成交额', 0) or 0)
            results.append({
                'name': name,
                'current': float(row.get('最新价', 0) or 0),
                'change_pct': float(row.get('涨跌幅', 0) or 0),
                'amount_yi': round(amount_raw / 1e8, 0),
            })
    except Exception as e:
        logger.warning(f"[指数] 新浪接口获取失败: {e}")

    micro = _fetch_micro_cap_index()
    if micro:
        results.append(micro)

    return results


def _fetch_micro_cap_index() -> Optional[dict]:
    """
    获取微盘股指数行情。
    优先东财概念板块搜索"微盘"，降级 BK1158 HTTP 接口。
    """
    import akshare as ak

    # 方案1：东财概念板块中查找"微盘股"
    try:
        df = ak.stock_board_concept_spot_em()
        if df is not None and not df.empty:
            mask = df['板块名称'].str.contains('微盘', na=False)
            match = df[mask]
            if not match.empty:
                row = match.iloc[0]
                return {
                    'name': '微盘股',
                    'current': float(row.get('最新价', 0) or 0),
                    'change_pct': float(row.get('涨跌幅', 0) or 0),
                    'amount_yi': round(float(row.get('成交额', 0) or 0) / 1e8, 0),
                }
    except Exception as e:
        logger.debug(f"[微盘股] 东财概念接口失败，尝试BK1158: {e}")

    # 方案2：BK1158 直接 HTTP
    return _fetch_bk1158()


def _fetch_bk1158() -> Optional[dict]:
    """
    从东财 HTTP 接口获取 BK1158(微盘股)实时行情。
    失败时静默返回 None。
    """
    url = (
        "http://push2.eastmoney.com/api/qt/stock/get"
        "?fields=f43,f57,f58,f169,f170,f60,f44,f45,f47,f48"
        "&secid=90.BK1158"
        "&ut=fa5fd1943c7b386f172d6893dbfba10b"
        "&invt=2"
    )
    try:
        resp = requests.get(url, timeout=5)
        data = resp.json().get('data', {})
        if not data:
            return None
        current = data.get('f43', 0) / 100
        change_pct = data.get('f170', 0) / 100
        amount_yi = data.get('f48', 0) / 1e8  # f48=成交额（元）
        return {
            'name': '微盘股',
            'current': round(current, 2),
            'change_pct': round(change_pct, 2),
            'amount_yi': round(amount_yi, 0),
        }
    except Exception as e:
        logger.debug(f"[微盘股] BK1158 获取失败（跳过）: {e}")
        return None


def _fetch_market_stats() -> Optional[dict]:
    """
    获取市场涨跌统计：上涨/下跌/平盘/涨停/跌停/成交额。
    优先东财全量行情，失败降级新浪。
    """
    import akshare as ak
    import pandas as pd
    import numpy as np

    def _calc(df: pd.DataFrame) -> dict:
        df = df.copy()
        close_col = next((c for c in ['最新价', 'close'] if c in df.columns), None)
        pre_col   = next((c for c in ['昨收', 'pre_close'] if c in df.columns), None)
        name_col  = next((c for c in ['名称', '股票名称'] if c in df.columns), None)
        code_col  = next((c for c in ['代码', '股票代码'] if c in df.columns), None)
        amt_col   = next((c for c in ['成交额'] if c in df.columns), None)

        if not all([close_col, pre_col, name_col, code_col]):
            return {}

        up = down = flat = lu = ld = 0
        for code, name, cur, pre, amt in zip(
            df[code_col], df[name_col], df[close_col], df[pre_col],
            df[amt_col] if amt_col else [0] * len(df)
        ):
            if pd.isna(cur) or pd.isna(pre) or pre in ['-'] or cur in ['-'] or amt == 0:
                continue
            cur, pre = float(cur), float(pre)
            if cur <= 0 or pre <= 0:
                continue

            if str(code).startswith(('688', '30')):
                ratio = 0.20
            elif 'ST' in str(name).upper():
                ratio = 0.05
            else:
                ratio = 0.10

            lup = np.floor(pre * (1 + ratio) * 100 + 0.5) / 100
            ldn = np.floor(pre * (1 - ratio) * 100 + 0.5) / 100
            tol_up = round(abs(pre * (1 + ratio) - lup), 10)
            tol_dn = round(abs(pre * (1 - ratio) - ldn), 10)

            if abs(cur - lup) <= tol_up:
                lu += 1
            if abs(cur - ldn) <= tol_dn:
                ld += 1

            if cur > pre:
                up += 1
            elif cur < pre:
                down += 1
            else:
                flat += 1

        total_amt = 0.0
        if amt_col:
            df[amt_col] = pd.to_numeric(df[amt_col], errors='coerce')
            total_amt = df[amt_col].sum() / 1e8

        return {
            'up_count': up, 'down_count': down, 'flat_count': flat,
            'limit_up_count': lu, 'limit_down_count': ld,
            'total_amount': round(total_amt, 0),
        }

    # 东财
    try:
        df = ak.stock_zh_a_spot_em()
        if df is not None and not df.empty:
            return _calc(df)
    except Exception as e:
        logger.warning(f"[市场统计] 东财接口失败: {e}，尝试新浪")

    # 新浪 fallback
    try:
        df = ak.stock_zh_a_spot()
        if df is not None and not df.empty:
            return _calc(df)
    except Exception as e:
        logger.error(f"[市场统计] 新浪接口失败: {e}")

    return None


def _fetch_prev_day_amount() -> Optional[float]:
    """
    获取上一交易日 A 股总成交额（亿元）= 上交所股票成交额 + 深交所股票成交额。
    尝试最近 5 个自然日，找到第一个有数据的交易日即返回。
    """
    import akshare as ak
    import pandas as pd
    from datetime import datetime, timedelta

    today = datetime.now().date()

    for days_back in range(1, 6):
        check_date = today - timedelta(days=days_back)
        date_str = check_date.strftime('%Y%m%d')
        sse_amt = None
        szse_amt = None

        # 上交所
        try:
            df_sse = ak.stock_sse_deal_daily(date=date_str)
            if df_sse is not None and not df_sse.empty:
                row = df_sse[df_sse.iloc[:, 0] == '成交金额']
                if not row.empty and '股票' in df_sse.columns:
                    sse_amt = pd.to_numeric(row.iloc[0]['股票'], errors='coerce')
                    # 上交所成交金额单位是亿元
        except Exception:
            pass

        # 深交所
        try:
            df_szse = ak.stock_szse_summary(date=date_str)
            if df_szse is not None and not df_szse.empty:
                row = df_szse[df_szse['证券类别'] == '股票']
                if not row.empty:
                    val = pd.to_numeric(row.iloc[0]['成交金额'], errors='coerce')
                    if not pd.isna(val):
                        szse_amt = val / 1e8  # 元 → 亿

        except Exception:
            pass

        if sse_amt and szse_amt:
            total = round(float(sse_amt) + float(szse_amt), 0)
            logger.debug(f"[成交额历史] {date_str} SSE={sse_amt:.0f}亿 SZSE={szse_amt:.0f}亿 合计={total:.0f}亿")
            return total

    logger.debug("[成交额历史] 未能获取上一交易日数据")
    return None


def _fetch_sw_sectors(top_n: int = 10) -> tuple[list[dict], list[dict]]:
    """
    获取行业实时涨跌。
    优先同花顺二级（含领涨股），最多重试 3 次；失败降级申万二级（无领涨股）。
    返回 (涨幅TOP10, 跌幅TOP10)，每项含 name, change_pct, leading_gainer,
    leading_gainer_pct, top_stocks。
    """
    import akshare as ak
    import pandas as pd
    import time

    # ── 尝试同花顺 ──────────────────────────────
    df_ths = None
    for attempt in range(3):
        try:
            df_ths = ak.stock_board_industry_summary_ths()
            if df_ths is not None and not df_ths.empty:
                break
        except Exception as e:
            logger.debug(f"[板块] THS 第{attempt+1}次失败: {e}")
        if attempt < 2:
            time.sleep(2)

    if df_ths is not None and not df_ths.empty:
        df_ths['涨跌幅'] = pd.to_numeric(df_ths['涨跌幅'], errors='coerce')
        df_ths = df_ths.dropna(subset=['涨跌幅'])

        def _safe_pct(val):
            try:
                return float(val) if val not in (None, '', '-') else None
            except (ValueError, TypeError):
                return None

        sectors = []
        for _, row in df_ths.iterrows():
            sectors.append({
                'name': str(row.get('板块', row.get('板块名称', ''))).strip(),
                'change_pct': float(row['涨跌幅']),
                'leading_gainer': str(row.get('领涨股', '') or '').strip(),
                'leading_gainer_pct': _safe_pct(row.get('领涨股-涨跌幅')),
                'top_stocks': [],
            })

    else:
        # ── 降级：申万二级 ───────────────────────
        logger.warning("[板块] THS 重试失败，降级使用申万二级行业")
        try:
            df_sw = ak.index_realtime_sw(symbol='二级行业')
            df_sw['最新价'] = pd.to_numeric(df_sw['最新价'], errors='coerce')
            df_sw['昨收盘'] = pd.to_numeric(df_sw['昨收盘'], errors='coerce')
            df_sw = df_sw.dropna(subset=['最新价', '昨收盘'])
            df_sw['change_pct'] = (df_sw['最新价'] - df_sw['昨收盘']) / df_sw['昨收盘'] * 100

            sectors = []
            for _, row in df_sw.iterrows():
                sectors.append({
                    'name': str(row['指数名称']).strip(),
                    'change_pct': round(float(row['change_pct']), 2),
                    'leading_gainer': '',
                    'leading_gainer_pct': None,
                    'top_stocks': [],
                })
        except Exception as e:
            logger.error(f"[板块] 申万二级也失败: {e}")
            return [], []

    sectors.sort(key=lambda x: x['change_pct'], reverse=True)
    top = sectors[:top_n]
    bot = list(reversed(sectors[-top_n:]))

    # 并发从东财获取每板块前3只股票
    _fill_sector_stocks(top, get_losers=False)
    _fill_sector_stocks(bot, get_losers=True)

    return top, bot


def _fill_sector_stocks(sectors: list[dict], get_losers: bool) -> None:
    """
    就地填充每个板块的 top_stocks 字段。
    get_losers=False → 取涨幅前3；get_losers=True → 取跌幅前3（最大跌幅）。
    使用并发，总超时 15 秒，失败时 top_stocks 保持空列表。
    """
    import akshare as ak
    import pandas as pd

    def fetch_one(sector_name: str) -> tuple[str, list[dict]]:
        try:
            df = ak.stock_board_industry_cons_em(symbol=sector_name)
            if df is None or df.empty:
                return sector_name, []
            pct_col  = next((c for c in ['涨跌幅', '涨跌幅(%)'] if c in df.columns), None)
            name_col = next((c for c in ['名称', '股票名称'] if c in df.columns), None)
            if not pct_col or not name_col:
                return sector_name, []
            df[pct_col] = pd.to_numeric(df[pct_col], errors='coerce')
            df = df.dropna(subset=[pct_col])
            fn = df.nsmallest if get_losers else df.nlargest
            stocks = fn(3, pct_col)[[name_col, pct_col]].rename(
                columns={name_col: 'name', pct_col: 'change_pct'}
            ).to_dict('records')
            return sector_name, stocks
        except Exception:
            return sector_name, []

    name_to_sector = {s['name']: s for s in sectors}

    try:
        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = {executor.submit(fetch_one, s['name']): s['name'] for s in sectors}
            for future in as_completed(futures, timeout=15):
                try:
                    sector_name, stocks = future.result(timeout=5)
                    if sector_name in name_to_sector:
                        name_to_sector[sector_name]['top_stocks'] = stocks
                except Exception:
                    pass
    except Exception as e:
        logger.debug(f"[板块成分股] 并发获取超时或失败: {e}")


# ─────────────────────────────────────────────
# 报告拼装
# ─────────────────────────────────────────────

def _fetch_limit_up(trade_date: str) -> list[dict]:
    """
    获取指定交易日涨停板数据（东财涨停池）。
    trade_date 格式 YYYYMMDD。
    同时尝试获取强势股池的入选理由，合并到对应股票。
    返回 list[dict]：name, code, turnover, lianban, industry, reason, seal_time
    """
    import akshare as ak
    import pandas as pd

    df = None
    try:
        df = ak.stock_zt_pool_em(date=trade_date)
    except Exception as e:
        logger.warning(f"[涨停] 东财涨停池获取失败: {e}")
        return []

    if df is None or df.empty:
        return []

    # 强势股池提供入选理由，以代码为 key 合并
    reasons: dict[str, str] = {}
    try:
        df_s = ak.stock_zt_pool_strong_em(date=trade_date)
        if df_s is not None and not df_s.empty and '入选理由' in df_s.columns:
            for _, row in df_s.iterrows():
                reasons[str(row['代码'])] = str(row.get('入选理由', '') or '')
    except Exception:
        pass

    stocks = []
    for _, row in df.iterrows():
        code = str(row['代码'])
        stocks.append({
            'name':      str(row['名称']),
            'code':      code,
            'turnover':  round(float(row.get('换手率', 0) or 0), 2),
            'lianban':   int(row.get('连板数', 1) or 1),
            'industry':  str(row.get('所属行业', '') or '').strip(),
            'reason':    reasons.get(code, ''),
            'seal_time': str(row.get('首次封板时间', '') or ''),
        })

    return stocks


def _group_limit_up(stocks: list[dict]) -> list[dict]:
    """
    将涨停股按行业分组，组内按连板数降序排列。
    每组选取最具代表性的名称：若同行业里有多只同概念的则用行业名，
    否则直接用行业名。
    返回 list[dict]：group_name, count, stocks（已排序）
    按 count 降序。
    """
    from collections import defaultdict
    groups: dict[str, list[dict]] = defaultdict(list)
    for s in stocks:
        key = s['industry'] if s['industry'] else '其他'
        groups[key].append(s)

    result = []
    for group_name, members in groups.items():
        members.sort(key=lambda x: x['lianban'], reverse=True)
        result.append({
            'group_name': group_name,
            'count': len(members),
            'stocks': members,
        })

    result.sort(key=lambda x: x['count'], reverse=True)
    return result


def _build_report(
    indices: list[dict],
    stats: Optional[dict],
    top_sectors: list[dict],
    bot_sectors: list[dict],
    prev_amount: Optional[float] = None,
    limit_up_groups: Optional[list] = None,
) -> str:
    date_str = _get_last_trading_date()
    lines = [f"## {date_str} 大盘日报\n"]

    # ── 第一部分：市场总结 ────────────────────────
    lines.append("### 一、市场总结\n")
    if stats:
        today_amt = int(stats['total_amount'])
        amt_cmp = ""
        if prev_amount and prev_amount > 0:
            diff = stats['total_amount'] - prev_amount
            diff_pct = diff / prev_amount * 100
            sign = '+' if diff >= 0 else ''
            amt_cmp = f"（较上日 {_c(f'{diff_pct:+.1f}%，{sign}{int(diff)} 亿', diff)}）"
        lines.append(
            f"上涨 **{stats['up_count']}** 家 / "
            f"下跌 **{stats['down_count']}** 家 / "
            f"平盘 **{stats['flat_count']}** 家 | "
            f"涨停 **{stats['limit_up_count']}** / "
            f"跌停 **{stats['limit_down_count']}** | "
            f"成交额 **{today_amt}** 亿{amt_cmp}\n"
        )
    else:
        lines.append("_市场统计数据暂不可用_\n")

    # ── 第二部分：指数 ───────────────────────────
    lines.append("\n### 二、指数行情\n")
    if indices:
        for idx in indices:
            pct = idx['change_pct']
            amt_str = f"{int(idx['amount_yi'])} 亿" if idx['amount_yi'] > 0 else "-"
            lines.append(
                f"**{idx['name']}**　{idx['current']:.2f}　"
                f"{_c(f'{pct:+.2f}%', pct)}　{amt_str}"
            )
        lines.append("")
    else:
        lines.append("_指数数据暂不可用_\n")

    # ── 第三部分：同花顺行业板块 ────────────────────
    lines.append("\n### 三、同花顺行业板块\n")

    def _render_sector_block(sectors: list[dict]) -> list[str]:
        block = []
        for i, s in enumerate(sectors, 1):
            pct = s['change_pct']
            line = f"{i:>2}. **{_esc(s['name'])}** {_c(f'{pct:+.2f}%', pct)}"

            stocks = s.get('top_stocks', [])
            if stocks:
                parts = []
                for st in stocks:
                    sp = st['change_pct']
                    parts.append(f"{_esc(st['name'])} {_c(f'{sp:+.1f}%', sp)}")
                line += "　|　" + "　".join(parts)
            else:
                nm = s.get('leading_gainer', '')
                p  = s.get('leading_gainer_pct')
                if nm and p is not None:
                    line += f"　|　{_esc(nm)} {_c(f'{p:+.1f}%', p)}"
            block.append(line)
        return block

    if top_sectors:
        lines.append("**涨幅 TOP 10**\n")
        lines.extend(_render_sector_block(top_sectors))
        lines.append("")

    if bot_sectors:
        lines.append("**跌幅 TOP 10**\n")
        lines.extend(_render_sector_block(bot_sectors))
        lines.append("")

    if not top_sectors and not bot_sectors:
        lines.append("_板块数据暂不可用_\n")

    # ── 第四部分：涨停分析 ───────────────────────
    lines.append("\n### 四、涨停分析\n")
    if limit_up_groups:
        total_zt = sum(g['count'] for g in limit_up_groups)
        lines.append(f"共 **{total_zt}** 只涨停，按行业分布：\n")
        for g in limit_up_groups:
            lines.append(f"**{_esc(g['group_name'])}**（{g['count']}只）")
            for s in g['stocks']:
                lb = f" {s['lianban']}连板" if s['lianban'] > 1 else ""
                reason = f" _{_esc(s['reason'])}_" if s['reason'] else ""
                lines.append(
                    f"　{_esc(s['name'])}　换手 {s['turnover']:.1f}%{lb}{reason}"
                )
            lines.append("")
    else:
        lines.append("_涨停数据暂不可用_\n")

    return "\n".join(lines)


# ─────────────────────────────────────────────
# 入口（保持与 main.py 调用接口一致）
# ─────────────────────────────────────────────

def run_market_review(
    notifier: NotificationService,
    analyzer: Optional[GeminiAnalyzer] = None,       # 本模块不使用 AI，保留参数兼容
    search_service: Optional[SearchService] = None,  # 保留参数兼容
    send_notification: bool = True,
    merge_notification: bool = False,
    override_region: Optional[str] = None,
) -> Optional[str]:
    """
    执行个性化大盘复盘（无 AI），推送到已配置渠道。
    """
    logger.info("开始执行个性化大盘复盘（my_market_review）...")

    try:
        indices      = _fetch_indices()
        stats        = _fetch_market_stats()
        prev_amount  = _fetch_prev_day_amount()
        top_s, bot_s = _fetch_sw_sectors(top_n=10)

        trade_date = _get_last_trading_date().replace('-', '')
        zt_stocks   = _fetch_limit_up(trade_date)
        zt_groups   = _group_limit_up(zt_stocks) if zt_stocks else []

        report = _build_report(indices, stats, top_s, bot_s, prev_amount, zt_groups)

        # 保存到文件
        date_str = datetime.now().strftime('%Y%m%d')
        filepath = notifier.save_report_to_file(
            report,
            f"market_review_{date_str}.md"
        )
        logger.info(f"复盘报告已保存: {filepath}")

        # 推送
        if merge_notification and send_notification:
            logger.info("合并推送模式：跳过单独推送")
        elif send_notification and notifier.is_available():
            if notifier.send(report, email_send_to_all=True):
                logger.info("大盘复盘推送成功")
            else:
                logger.warning("大盘复盘推送失败")
        elif not send_notification:
            logger.info("已跳过推送 (--no-notify)")

        return report

    except Exception as e:
        logger.error(f"个性化大盘复盘失败: {e}", exc_info=True)
        return None

# -*- coding: utf-8 -*-
"""
===================================
开盘前晨报模块
===================================

功能：每个交易日 09:00 前推送，包含：
  1. 美股三大指数（道指/纳指/标普）昨日收盘涨跌
  2. 美股11大板块 ETF 涨跌 TOP5 / BOT5
  3. 现货黄金、布伦特原油、美元指数

数据来源：新浪财经实时接口（无需 Key，免费）
颜色规范：红色 = 涨，绿色 = 跌
"""

import logging
import re
from datetime import datetime
from typing import Optional

import requests

from src.notification import NotificationService

logger = logging.getLogger(__name__)

SINA_HEADERS = {'Referer': 'http://finance.sina.com.cn', 'User-Agent': 'Mozilla/5.0'}

# 美股11大板块 ETF
US_SECTOR_ETFS = {
    'xlk':  '科技',
    'xlf':  '金融',
    'xlv':  '医疗健康',
    'xle':  '能源',
    'xli':  '工业',
    'xly':  '非必需消费',
    'xlp':  '必需消费',
    'xlu':  '公用事业',
    'xlre': '房地产',
    'xlb':  '原材料',
    'xlc':  '通信服务',
}


# ─────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────

def _c(text: str, pct: float) -> str:
    """红涨绿跌"""
    color = 'red' if pct >= 0 else 'green'
    return f'<font color="{color}">{text}</font>'


def _sina_quote(codes: list[str]) -> dict[str, list[str]]:
    """
    批量拉新浪实时行情，返回 {code: [字段列表]} 或空列表（无数据）。
    """
    url = 'http://hq.sinajs.cn/list=' + ','.join(codes)
    try:
        r = requests.get(url, headers=SINA_HEADERS, timeout=8)
        r.encoding = 'gbk'
        result = {}
        for line in r.text.splitlines():
            m = re.match(r'var hq_str_(\w+)="(.*)"', line)
            if m:
                code = m.group(1)
                fields = m.group(2).split(',')
                result[code] = fields
        return result
    except Exception as e:
        logger.warning(f"[新浪行情] 请求失败: {e}")
        return {}


# ─────────────────────────────────────────────
# 数据获取
# ─────────────────────────────────────────────

def _fetch_us_indices() -> list[dict]:
    """
    获取美股三大指数。
    新浪 gb_* 格式：名称,最新价,涨跌幅%,时间,涨跌点,开盘,...
    """
    codes = ['gb_dji', 'gb_ixic', 'gb_inx']
    display = {'gb_dji': '道琼斯', 'gb_ixic': '纳斯达克', 'gb_inx': '标普500'}
    data = _sina_quote(codes)
    results = []
    for code in codes:
        fields = data.get(code, [])
        if len(fields) < 5:
            continue
        try:
            name   = display.get(code, fields[0])
            price  = float(fields[1])
            pct    = float(fields[2])   # 涨跌幅%
            change = float(fields[4])   # 涨跌点
            results.append({'name': name, 'price': price, 'change': change, 'pct': pct})
        except (ValueError, IndexError):
            continue
    return results


def _fetch_us_sectors() -> list[dict]:
    """
    获取美股11大板块 ETF 涨跌，返回按 pct 排序的列表。
    新浪 gb_xlk 格式：名称,最新价,涨跌幅%,时间,...,昨收,...
    字段0=名称 1=最新 2=涨跌幅% 3=时间 4=涨跌点 5=开盘 ...
    """
    codes = [f'gb_{k}' for k in US_SECTOR_ETFS]
    data = _sina_quote(codes)
    results = []
    for etf_code, sector_name in US_SECTOR_ETFS.items():
        fields = data.get(f'gb_{etf_code}', [])
        if len(fields) < 3:
            continue
        try:
            price = float(fields[1])
            pct   = float(fields[2])
            results.append({'name': sector_name, 'etf': etf_code.upper(), 'price': price, 'pct': pct})
        except (ValueError, IndexError):
            continue
    results.sort(key=lambda x: x['pct'], reverse=True)
    return results


def _fetch_commodities() -> list[dict]:
    """
    获取现货黄金(XAU)、布伦特原油、美元指数。

    hf_XAU 格式：最新价,昨收,最新价2,买价,最高,最低,时间,...,名称
    hf_OIL 格式：最新价,,开盘,买价,最高,最低,时间,昨收,...,名称,成交量
    gb_usdx 格式：名称,最新,涨跌幅%,时间,...
    """
    codes = ['hf_XAU', 'hf_OIL', 'gb_usdx']
    data = _sina_quote(codes)
    results = []

    # 现货黄金
    f = data.get('hf_XAU', [])
    if len(f) >= 2:
        try:
            price    = float(f[0])
            prev     = float(f[1])
            pct      = (price - prev) / prev * 100 if prev else 0
            results.append({'name': '现货黄金', 'price': price, 'prev': prev, 'pct': round(pct, 2), 'unit': '美元/盎司'})
        except (ValueError, IndexError):
            pass

    # 布伦特原油
    f = data.get('hf_OIL', [])
    if len(f) >= 8:
        try:
            price = float(f[0])
            prev  = float(f[7])   # 昨收在第8字段
            pct   = (price - prev) / prev * 100 if prev else 0
            results.append({'name': '布伦特原油', 'price': price, 'prev': prev, 'pct': round(pct, 2), 'unit': '美元/桶'})
        except (ValueError, IndexError):
            pass

    # 美元指数（gb_usdx）
    f = data.get('gb_usdx', [])
    if len(f) >= 3:
        try:
            price = float(f[1])
            pct   = float(f[2])
            results.append({'name': '美元指数', 'price': price, 'prev': None, 'pct': pct, 'unit': ''})
        except (ValueError, IndexError):
            pass

    return results


# ─────────────────────────────────────────────
# 报告拼装
# ─────────────────────────────────────────────

def _build_pre_market_report(
    indices: list[dict],
    sectors: list[dict],
    commodities: list[dict],
) -> str:
    date_str = datetime.now().strftime('%Y-%m-%d')
    lines = [f"## {date_str} 开盘前晨报\n"]

    # ── 一、美股三大指数 ─────────────────────────
    lines.append("### 一、美股三大指数（昨日收盘）\n")
    if indices:
        for idx in indices:
            sign = '+' if idx['change'] >= 0 else ''
            change_str = f"{sign}{idx['change']:,.2f}　{sign}{idx['pct']:.2f}%"
            lines.append(
                f"**{idx['name']}**　{idx['price']:,.2f}　"
                + _c(change_str, idx['pct'])
            )
        lines.append("")
    else:
        lines.append("_指数数据暂不可用_\n")

    # ── 二、美股板块 ─────────────────────────────
    lines.append("### 二、美股板块涨跌\n")
    if sectors:
        top5 = sectors[:5]
        bot5 = sectors[-5:]
        lines.append("**涨幅 TOP 5**\n")
        for s in top5:
            pct_str = f"{s['pct']:+.2f}%"
            lines.append(f"　{s['name']}（{s['etf']}）　" + _c(pct_str, s['pct']))
        lines.append("")
        lines.append("**跌幅 TOP 5**\n")
        for s in bot5:
            pct_str = f"{s['pct']:+.2f}%"
            lines.append(f"　{s['name']}（{s['etf']}）　" + _c(pct_str, s['pct']))
        lines.append("")
    else:
        lines.append("_板块数据暂不可用_\n")

    # ── 三、大宗商品 ─────────────────────────────
    lines.append("### 三、大宗商品\n")
    if commodities:
        for c in commodities:
            unit = f"　{c['unit']}" if c['unit'] else ""
            pct_str = f"{c['pct']:+.2f}%"
            lines.append(
                f"**{c['name']}**　{c['price']:,.2f}{unit}　"
                + _c(pct_str, c['pct'])
            )
        lines.append("")
    else:
        lines.append("_大宗商品数据暂不可用_\n")

    return "\n".join(lines)


# ─────────────────────────────────────────────
# 入口
# ─────────────────────────────────────────────

def run_pre_market_report(
    notifier: NotificationService,
    send_notification: bool = True,
) -> Optional[str]:
    """执行开盘前晨报，推送到已配置渠道。"""
    logger.info("开始执行开盘前晨报...")

    try:
        indices     = _fetch_us_indices()
        sectors     = _fetch_us_sectors()
        commodities = _fetch_commodities()

        report = _build_pre_market_report(indices, sectors, commodities)

        # 保存到文件
        date_str = datetime.now().strftime('%Y%m%d')
        filepath = notifier.save_report_to_file(report, f"pre_market_{date_str}.md")
        logger.info(f"晨报已保存: {filepath}")

        # 推送
        if send_notification and notifier.is_available():
            if notifier.send(report, email_send_to_all=True):
                logger.info("晨报推送成功")
            else:
                logger.warning("晨报推送失败")
        elif not send_notification:
            logger.info("已跳过推送 (--no-notify)")

        return report

    except Exception as e:
        logger.error(f"开盘前晨报失败: {e}", exc_info=True)
        return None


from __future__ import annotations

import argparse
import json
import math
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import pandas as pd

PERIODS = ["当日", "近一周", "本月以来", "本季以来", "今年以来"]
TARGET_COMPANIES = ["易方达", "华夏", "富国", "国泰", "广发", "南方", "永赢"]
PRIMARY_CATEGORIES = [
    "核心宽基",
    "科创成长",
    "红利与SmartBeta",
    "科技TMT",
    "医药健康",
    "金融地产",
    "消费服务",
    "高端制造与周期",
    "央国企与政策主题",
    "跨境与区域市场",
    "商品与另类",
]
STATIC_TAGS = ["领先布局", "拥挤赛道", "特色布局", "空白赛道"]
DYNAMIC_TAG = "强势流入"
EMPTY_TAG_THRESHOLD = 3
DEFAULT_PERIOD = "近一周"
DEFAULT_DATA_FILENAME = "etf_competitor_matrix_data.json"
DEFAULT_HTML_FILENAME = "etf_competitor_matrix_dashboard.html"
DEFAULT_MAPPING_FILENAME = "category_mapping.json"
SOURCE_PATTERN = re.compile(r"ETF\u57fa\u91d1\u6570\u636e\u65e5\u62a5(?P<source>\d{8})_\(\u6570\u636e\u622a\u81f3(?P<data>\d{8})\)")
FIELD_RAW_CATEGORY = "\u539f\u59cb\u5206\u7c7b"
FIELD_COMPANY = "\u7ba1\u7406\u4eba"
FIELD_ETF_NAME = "ETF\u540d\u79f0"
FIELD_FUND_CODE = "\u57fa\u91d1\u4ee3\u7801"
FIELD_INDEX_SHORT = "\u6307\u6570\u7b80\u79f0"
FIELD_SCALE_RANK = "\u89c4\u6a21\u6392\u540d"
FIELD_LATEST_SCALE = "\u6700\u65b0\u89c4\u6a21"
FIELD_PRIMARY_CATEGORY = "\u4e00\u7ea7\u8d5b\u9053"
METRIC_NET_INFLOW = "\u51c0\u6d41\u5165\u4ebf\u5143"
METRIC_SCALE_CHANGE = "\u89c4\u6a21\u53d8\u5316\u4ebf\u5143"
METRIC_SCALE_CHANGE_RATE = "\u89c4\u6a21\u53d8\u5316\u7387"
METRIC_NAV_CHANGE_RATE = "\u51c0\u503c\u53d8\u5316\u7387"
METRIC_SHARE_CHANGE_RATE = "\u4efd\u989d\u53d8\u5316\u7387"

RAW_TO_PRIMARY_CATEGORY = {
    "50指数": "核心宽基",
    "A股生科,医药医疗": "医药健康",
    "H股": "跨境与区域市场",
    "MSCI中国A": "核心宽基",
    "SmartBeta-ESG": "红利与SmartBeta",
    "SmartBeta-价值": "红利与SmartBeta",
    "SmartBeta-基本面": "红利与SmartBeta",
    "SmartBeta-成长": "红利与SmartBeta",
    "SmartBeta-质量": "红利与SmartBeta",
    "TMT": "科技TMT",
    "TMT-通信": "科技TMT",
    "上证大中盘": "核心宽基",
    "东南亚": "跨境与区域市场",
    "中小": "核心宽基",
    "中证100": "核心宽基",
    "中证1000": "核心宽基",
    "中证2000": "核心宽基",
    "中证500": "核心宽基",
    "中证800": "核心宽基",
    "中证A500": "核心宽基",
    "云大算AI,XR": "科技TMT",
    "互联网": "科技TMT",
    "交运物流": "消费服务",
    "产业升级": "高端制造与周期",
    "信息": "科技TMT",
    "农林牧渔": "消费服务",
    "创业板": "科创成长",
    "创新药": "医药健康",
    "券商": "金融地产",
    "区域,政策主题": "央国企与政策主题",
    "半导体,芯片": "科技TMT",
    "双创": "科创成长",
    "国证2000": "核心宽基",
    "国防军工": "高端制造与周期",
    "基建": "高端制造与周期",
    "增强": "红利与SmartBeta",
    "大金融": "金融地产",
    "央企回报": "央国企与政策主题",
    "央企科技": "央国企与政策主题",
    "央企能源": "央国企与政策主题",
    "央国企": "央国企与政策主题",
    "家电": "消费服务",
    "恒指": "跨境与区域市场",
    "房地产": "金融地产",
    "教育,养老": "消费服务",
    "文娱旅游": "消费服务",
    "新经济": "跨境与区域市场",
    "新能源": "高端制造与周期",
    "日本": "跨境与区域市场",
    "普通红利": "红利与SmartBeta",
    "有色,稀土,黑色": "高端制造与周期",
    "期货": "商品与另类",
    "未定义分类": "核心宽基",
    "机械与制造": "高端制造与周期",
    "材料": "高端制造与周期",
    "汽车": "高端制造与周期",
    "沙特": "跨境与区域市场",
    "沪深300": "核心宽基",
    "沪港深宽基": "跨境与区域市场",
    "海外宽基": "跨境与区域市场",
    "消费": "消费服务",
    "深主板50": "核心宽基",
    "深成指": "核心宽基",
    "深证宽基": "核心宽基",
    "港股科技": "科技TMT",
    "港股红利": "红利与SmartBeta",
    "港股通宽基": "跨境与区域市场",
    "湾区": "央国企与政策主题",
    "物联网": "科技TMT",
    "环保": "高端制造与周期",
    "电力": "高端制造与周期",
    "疫苗相关": "医药健康",
    "碳中和,低碳": "高端制造与周期",
    "科创100": "科创成长",
    "科创50": "科创成长",
    "科创成长": "科创成长",
    "科技": "科技TMT",
    "红利+低波": "红利与SmartBeta",
    "红利+质量": "红利与SmartBeta",
    "综合指数": "核心宽基",
    "美股行业主题": "跨境与区域市场",
    "能化": "高端制造与周期",
    "资源": "高端制造与周期",
    "银行": "金融地产",
    "非A生科,医药医疗": "医药健康",
    "食品饮料": "消费服务",
    "黄金": "商品与另类",
    "黄金主题": "商品与另类",
}


def _safe_float(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, str):
        text = value.strip()
        if not text or text.lower() == "nan":
            return 0.0
        value = text.replace(",", "")
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return 0.0
    if math.isnan(numeric):
        return 0.0
    return numeric


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if pd.isna(value):
        return ""
    return str(value).strip()


def _format_yyyymmdd(value: str) -> str:
    return f"{value[:4]}-{value[4:6]}-{value[6:8]}"


def extract_source_metadata(source_path: str | Path) -> dict[str, str]:
    path = Path(source_path)
    metadata = {"source_filename": path.name, "source_date": "", "data_date": ""}
    match = SOURCE_PATTERN.search(path.stem)
    if match:
        metadata["source_date"] = _format_yyyymmdd(match.group("source"))
        metadata["data_date"] = _format_yyyymmdd(match.group("data"))
    return metadata


def _competition_rank(values: list[tuple[str, float]]) -> dict[str, int | None]:
    ordered = sorted(values, key=lambda item: item[1], reverse=True)
    ranks: dict[str, int | None] = {}
    previous_value: float | None = None
    previous_rank = 0
    for index, (name, value) in enumerate(ordered, start=1):
        if value <= 0:
            ranks[name] = None
            continue
        if previous_value is None or not math.isclose(value, previous_value, rel_tol=1e-9, abs_tol=1e-9):
            previous_rank = index
            previous_value = value
        ranks[name] = previous_rank
    return ranks


def map_primary_category(raw_category: str, fund_name: str = "", index_short_name: str = "") -> str:
    raw_category = (raw_category or "").strip()
    if raw_category and raw_category != "未定义分类" and raw_category in RAW_TO_PRIMARY_CATEGORY:
        return RAW_TO_PRIMARY_CATEGORY[raw_category]

    search_text = " ".join([raw_category, fund_name or "", index_short_name or ""]).lower()
    fallback_rules = [
        (["黄金", "商品", "期货"], "商品与另类"),
        (["医疗", "医药", "创新药", "生物", "疫苗"], "医药健康"),
        (["恒生", "港股", "qdii", "纳指", "海外", "日经", "沙特", "东南亚"], "跨境与区域市场"),
        (["科技", "通信", "芯片", "半导体", "卫星", "物联网", "人工智能", "互联网"], "科技TMT"),
        (["银行", "证券", "非银", "金融", "地产"], "金融地产"),
        (["红利", "低波", "价值", "成长", "质量", "esg", "基本面", "增强"], "红利与SmartBeta"),
        (["新能源", "电池", "光伏", "军工", "制造", "材料", "有色", "煤炭", "油气", "电力", "基建"], "高端制造与周期"),
        (["消费", "食品", "酒", "旅游", "家电", "养老", "教育"], "消费服务"),
        (["创业板", "科创", "双创"], "科创成长"),
    ]
    for keywords, category in fallback_rules:
        if any(keyword in search_text for keyword in keywords):
            return category
    return "核心宽基"


def _weighted_average(items: list[dict[str, Any]], metric_key: str, period: str) -> float:
    if not items:
        return 0.0
    weights = [_safe_float(item.get("最新规模")) for item in items]
    total_weight = sum(weights)
    values = [_safe_float(item["periods"][period][metric_key]) for item in items]
    if total_weight <= 0:
        return sum(values) / len(values)
    return sum(value * weight for value, weight in zip(values, weights)) / total_weight


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    normalized["原始分类"] = _safe_text(row.get("原始分类"))
    normalized["管理人"] = _safe_text(row.get("管理人"))
    normalized["ETF名称"] = _safe_text(row.get("ETF名称"))
    normalized["基金代码"] = _safe_text(row.get("基金代码"))
    normalized["指数简称"] = _safe_text(row.get("指数简称"))
    normalized["规模排名"] = _safe_text(row.get("规模排名"))
    normalized["最新规模"] = _safe_float(row.get("最新规模"))
    normalized["一级赛道"] = map_primary_category(normalized["原始分类"], normalized["ETF名称"], normalized["指数简称"])
    raw_periods = row.get("periods") or {}
    normalized["periods"] = {
        period: {
            "净流入亿元": _safe_float((raw_periods.get(period) or {}).get("净流入亿元")),
            "规模变化亿元": _safe_float((raw_periods.get(period) or {}).get("规模变化亿元")),
            "规模变化率": _safe_float((raw_periods.get(period) or {}).get("规模变化率")),
            "净值变化率": _safe_float((raw_periods.get(period) or {}).get("净值变化率")),
            "份额变化率": _safe_float((raw_periods.get(period) or {}).get("份额变化率")),
        }
        for period in PERIODS
    }
    return normalized

def build_dashboard_payload(
    rows: list[dict[str, Any]],
    source_name: str | None = None,
    data_date: str | None = None,
    default_period: str = DEFAULT_PERIOD,
    source_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_rows = [_normalize_row(row) for row in rows]
    all_by_category: dict[str, list[dict[str, Any]]] = defaultdict(list)
    all_by_company_category: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    target_by_company_category: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    target_rows: list[dict[str, Any]] = []

    for row in normalized_rows:
        category = row[FIELD_PRIMARY_CATEGORY]
        company = row[FIELD_COMPANY]
        all_by_category[category].append(row)
        all_by_company_category[(company, category)].append(row)
        if company in TARGET_COMPANIES:
            target_rows.append(row)
            target_by_company_category[(company, category)].append(row)

    max_observed_coverage = max((sum(1 for company in TARGET_COMPANIES if target_by_company_category.get((company, category))) for category in PRIMARY_CATEGORIES), default=0)
    empty_threshold = 4 if max_observed_coverage >= 4 else max(1, max_observed_coverage)

    market_context: dict[str, dict[str, Any]] = {}
    base_tag_map: dict[tuple[str, str], list[str]] = {}
    dynamic_tag_map: dict[tuple[str, str, str], list[str]] = {}
    target_stats_by_category: dict[str, dict[str, dict[str, Any]]] = {}

    for category in PRIMARY_CATEGORIES:
        market_rows = all_by_category.get(category, [])
        company_stats = {}
        for (company, cat), items in all_by_company_category.items():
            if cat != category:
                continue
            company_stats[company] = {"product_count": len(items), "total_scale": round(sum(item[FIELD_LATEST_SCALE] for item in items), 4)}
        scale_rank_map = _competition_rank([(company, stats["total_scale"]) for company, stats in company_stats.items()])
        product_rank_map = _competition_rank([(company, float(stats["product_count"])) for company, stats in company_stats.items()])
        raw_breakdown = Counter(row[FIELD_RAW_CATEGORY] for row in market_rows)
        market_context[category] = {
            "market_product_count": len(market_rows),
            "market_total_scale": round(sum(row[FIELD_LATEST_SCALE] for row in market_rows), 4),
            "manager_count": len(company_stats),
            "market_manager_count": len(company_stats),
            "target_company_count": sum(1 for company in TARGET_COMPANIES if target_by_company_category.get((company, category))),
            "top_managers": [
                {"company": company, "product_count": stats["product_count"], "total_scale": stats["total_scale"]}
                for company, stats in sorted(company_stats.items(), key=lambda item: (-item[1]["total_scale"], item[0]))[:8]
            ],
            "raw_category_breakdown": [
                {"raw_category": raw_category, "product_count": count}
                for raw_category, count in raw_breakdown.most_common()
            ],
            "company_positions": {
                company: {
                    "is_covered": company in company_stats,
                    "product_count": company_stats.get(company, {}).get("product_count", 0),
                    "total_scale": company_stats.get(company, {}).get("total_scale", 0.0),
                    "product_count_rank": product_rank_map.get(company),
                    "scale_rank": scale_rank_map.get(company),
                }
                for company in TARGET_COMPANIES
            },
        }

        target_stats = {}
        for company in TARGET_COMPANIES:
            items = target_by_company_category.get((company, category), [])
            target_stats[company] = {
                "items": items,
                "product_count": len(items),
                "total_scale": round(sum(item[FIELD_LATEST_SCALE] for item in items), 4),
                "period_metrics": {
                    period: {
                        "net_inflow": round(sum(item["periods"][period][METRIC_NET_INFLOW] for item in items), 4),
                        "scale_change": round(sum(item["periods"][period][METRIC_SCALE_CHANGE] for item in items), 4),
                        "scale_change_rate": round(_weighted_average(items, METRIC_SCALE_CHANGE_RATE, period), 6),
                        "nav_change_rate": round(_weighted_average(items, METRIC_NAV_CHANGE_RATE, period), 6),
                        "share_change_rate": round(_weighted_average(items, METRIC_SHARE_CHANGE_RATE, period), 6),
                    }
                    for period in PERIODS
                },
            }
        target_stats_by_category[category] = target_stats
        coverage_count = sum(1 for company in TARGET_COMPANIES if target_stats[company]["product_count"] > 0)
        scale_rank_target = _competition_rank([(company, stats["total_scale"]) for company, stats in target_stats.items() if stats["product_count"] > 0])
        leaders = {company for company, rank in scale_rank_target.items() if rank is not None and rank <= 2}

        for company in TARGET_COMPANIES:
            tags: list[str] = []
            if target_stats[company]["product_count"] > 0 and company in leaders:
                tags.append("\u9886\u5148\u5e03\u5c40")
            if coverage_count >= 5:
                tags.append("\u62e5\u6324\u8d5b\u9053")
            if target_stats[company]["product_count"] > 0 and coverage_count in (1, 2):
                tags.append("\u7279\u8272\u5e03\u5c40")
            if target_stats[company]["product_count"] == 0 and coverage_count >= empty_threshold:
                tags.append("\u7a7a\u767d\u8d5b\u9053")
            base_tag_map[(company, category)] = tags

        for period in PERIODS:
            inflow_rank_map = _competition_rank([(company, stats["period_metrics"][period]["net_inflow"]) for company, stats in target_stats.items() if stats["product_count"] > 0])
            for company in TARGET_COMPANIES:
                inflow = target_stats[company]["period_metrics"][period]["net_inflow"]
                dynamic_tag_map[(company, category, period)] = ["\u5f3a\u52bf\u6d41\u5165"] if inflow > 0 and inflow_rank_map.get(company) is not None and inflow_rank_map[company] <= 2 else []

    matrix = []
    for category in PRIMARY_CATEGORIES:
        peer_companies = [
            {
                "company": company,
                "is_covered": target_stats_by_category[category][company]["product_count"] > 0,
                "product_count": target_stats_by_category[category][company]["product_count"],
                "total_scale": target_stats_by_category[category][company]["total_scale"],
                "net_inflow": target_stats_by_category[category][company]["period_metrics"][default_period]["net_inflow"],
            }
            for company in TARGET_COMPANIES
        ]
        peer_companies.sort(key=lambda item: (-item["total_scale"], item["company"]))
        for company in TARGET_COMPANIES:
            items = target_by_company_category.get((company, category), [])
            raw_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for item in items:
                raw_groups[item[FIELD_RAW_CATEGORY]].append(item)
            category_breakdown = [
                {
                    "raw_category": raw_category,
                    "product_count": len(bucket),
                    "total_scale": round(sum(item[FIELD_LATEST_SCALE] for item in bucket), 4),
                    "net_inflow": round(sum(item["periods"][default_period][METRIC_NET_INFLOW] for item in bucket), 4),
                }
                for raw_category, bucket in raw_groups.items()
            ]
            category_breakdown.sort(key=lambda item: (-item["total_scale"], item["raw_category"]))
            matrix.append({
                "company": company,
                "primary_category": category,
                "is_covered": bool(items),
                "product_count": target_stats_by_category[category][company]["product_count"],
                "total_scale": target_stats_by_category[category][company]["total_scale"],
                "raw_categories": sorted(raw_groups.keys()),
                "base_tags": base_tag_map[(company, category)],
                "market_rank_by_scale": market_context[category]["company_positions"][company]["scale_rank"],
                "market_position": market_context[category]["company_positions"][company],
                "market_manager_count": market_context[category]["manager_count"],
                "period_metrics": {period: {**target_stats_by_category[category][company]["period_metrics"][period], "tags": dynamic_tag_map[(company, category, period)]} for period in PERIODS},
                "category_breakdown": category_breakdown,
                "top_products": [
                    {
                        "etf_name": item[FIELD_ETF_NAME],
                        "fund_code": item[FIELD_FUND_CODE],
                        "index_short_name": item[FIELD_INDEX_SHORT],
                        "latest_scale": round(item[FIELD_LATEST_SCALE], 4),
                        "scale_rank": item[FIELD_SCALE_RANK],
                        "raw_category": item[FIELD_RAW_CATEGORY],
                    }
                    for item in sorted(items, key=lambda row: (-row[FIELD_LATEST_SCALE], row[FIELD_ETF_NAME]))[:5]
                ],
                "peer_companies": peer_companies,
            })

    products = []
    for row in target_rows:
        company = row[FIELD_COMPANY]
        category = row[FIELD_PRIMARY_CATEGORY]
        products.append({
            "company": company,
            "fund_name": row[FIELD_ETF_NAME],
            "etf_name": row[FIELD_ETF_NAME],
            "fund_code": row[FIELD_FUND_CODE],
            "index_short_name": row[FIELD_INDEX_SHORT],
            "scale_rank": row[FIELD_SCALE_RANK],
            "latest_scale": round(row[FIELD_LATEST_SCALE], 4),
            "original_category": row[FIELD_RAW_CATEGORY],
            "raw_category": row[FIELD_RAW_CATEGORY],
            "primary_category": category,
            "structure_tags": [category, row[FIELD_RAW_CATEGORY]],
            "competition_tags": [tag for tag in base_tag_map[(company, category)] if tag != "\u7a7a\u767d\u8d5b\u9053"],
            "strong_inflow_periods": [period for period in PERIODS if dynamic_tag_map[(company, category, period)]],
            "periods": row["periods"],
            "period_metrics": {
                period: {
                    "net_inflow": row["periods"][period][METRIC_NET_INFLOW],
                    "scale_change": row["periods"][period][METRIC_SCALE_CHANGE],
                    "scale_change_rate": row["periods"][period][METRIC_SCALE_CHANGE_RATE],
                    "nav_change_rate": row["periods"][period][METRIC_NAV_CHANGE_RATE],
                    "share_change_rate": row["periods"][period][METRIC_SHARE_CHANGE_RATE],
                }
                for period in PERIODS
            },
        })

    products.sort(key=lambda item: (TARGET_COMPANIES.index(item["company"]), PRIMARY_CATEGORIES.index(item["primary_category"]), -item["latest_scale"]))
    source_metadata = source_metadata or {}
    return {
        "meta": {
            "title": "\u0045\u0054\u0046\u7ade\u4e89\u5bf9\u624b\u4ea7\u54c1\u77e9\u9635",
            "source_name": source_name or source_metadata.get("source_filename", ""),
            "source_filename": source_metadata.get("source_filename", source_name or ""),
            "source_date": source_metadata.get("source_date", ""),
            "data_date": data_date or source_metadata.get("data_date", ""),
            "target_companies": TARGET_COMPANIES,
            "default_period": default_period,
            "periods": PERIODS,
            "primary_categories": PRIMARY_CATEGORIES,
            "tags": STATIC_TAGS + [DYNAMIC_TAG],
        },
        "matrix": matrix,
        "products": products,
        "market_context": market_context,
    }


def read_etf_workbook(excel_path: str | Path) -> tuple[list[dict[str, Any]], dict[str, str]]:
    path = Path(excel_path)
    frame = pd.read_excel(path, sheet_name="全部产品", header=1)
    columns = list(frame.columns)
    category_col = columns[0]
    name_col = columns[2]
    frame[category_col] = frame[category_col].ffill()
    frame = frame[frame[name_col].notna()].copy()
    frame = frame[frame[name_col] != "所有同类产品合计"]

    unknown_categories = sorted(
        category
        for category in frame[category_col].dropna().unique().tolist()
        if category not in RAW_TO_PRIMARY_CATEGORY
    )
    if unknown_categories:
        raise ValueError(f"发现未映射分类: {', '.join(unknown_categories)}")

    metric_names = ["净流入亿元", "规模变化亿元", "规模变化率", "净值变化率", "份额变化率"]
    period_columns = {
        "当日": columns[8:13],
        "近一周": columns[13:18],
        "本月以来": columns[18:23],
        "本季以来": columns[23:28],
        "今年以来": columns[28:33],
    }

    rows = []
    for _, record in frame.iterrows():
        item = {
            "原始分类": record[columns[0]],
            "管理人": record[columns[3]],
            "ETF名称": record[columns[2]],
            "基金代码": record[columns[1]],
            "指数简称": record[columns[5]],
            "规模排名": record[columns[6]],
            "最新规模": record[columns[7]],
            "periods": {},
        }
        for period, cols in period_columns.items():
            item["periods"][period] = {
                metric_name: _safe_float(record[col]) for metric_name, col in zip(metric_names, cols)
            }
        rows.append(item)

    return rows, extract_source_metadata(path)

def render_dashboard_html(payload: dict[str, Any]) -> str:
    payload_json = json.dumps(payload, ensure_ascii=False)
    return f"""<!doctype html>
<html lang=\"zh-CN\">
<head>
  <meta charset=\"utf-8\">
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <title>ETF竞争对手产品矩阵</title>
  <style>
    :root {{ --bg:#f6efdf; --panel:rgba(255,250,241,.82); --ink:#14263d; --muted:#607086; --accent:#a56a2d; --line:rgba(20,38,61,.12); --good:#24704d; --bad:#b55345; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; background:linear-gradient(135deg,#fbf5e8,#efe1c4 48%,#f8f1e5); color:var(--ink); font-family:\"Microsoft YaHei\",\"PingFang SC\",sans-serif; }}
    .wrap {{ width:min(1460px,calc(100vw - 28px)); margin:18px auto 28px; display:grid; gap:16px; }}
    .panel {{ background:var(--panel); border:1px solid rgba(255,255,255,.45); border-radius:24px; box-shadow:0 20px 60px rgba(31,40,57,.14); }}
    .hero {{ padding:24px 28px; }}
    .eyebrow {{ font-size:12px; letter-spacing:.2em; text-transform:uppercase; color:var(--accent); margin-bottom:10px; }}
    h1 {{ margin:0; font-family:\"STSong\",\"SimSun\",serif; font-size:clamp(32px,5vw,56px); line-height:1.04; }}
    .meta,.summary,.legend,.tags,.chips {{ display:flex; flex-wrap:wrap; gap:10px; }}
    .meta {{ margin-top:14px; }}
    .meta span,.chip {{ padding:8px 12px; border-radius:999px; border:1px solid var(--line); background:rgba(255,255,255,.54); color:var(--muted); font-size:13px; }}
    .summary {{ margin-top:18px; }}
    .summary .card {{ min-width:160px; flex:1 1 0; padding:16px 18px; border-radius:20px; background:rgba(255,255,255,.5); border:1px solid var(--line); }}
    .summary .card strong {{ display:block; margin-top:8px; font-size:28px; font-family:\"Georgia\",serif; color:var(--ink); }}
    .controls {{ padding:16px 18px; display:grid; gap:12px; }}
    .row {{ display:flex; flex-wrap:wrap; gap:10px; align-items:center; }}
    .label {{ min-width:60px; color:var(--muted); font-size:13px; }}
    button,select,.upload {{ border:1px solid var(--line); background:rgba(255,255,255,.62); color:var(--ink); border-radius:999px; padding:10px 14px; font-size:13px; cursor:pointer; }}
    button.active {{ background:linear-gradient(135deg,#b67a36,#8d5524); color:#fffdf8; border-color:transparent; }}
    input[type=file] {{ display:none; }}
    .layout {{ display:grid; gap:16px; grid-template-columns:minmax(0,1.7fr) minmax(320px,.9fr); align-items:start; }}
    .matrix {{ padding:16px; overflow:hidden; }}
    .table-wrap {{ overflow:auto; border-radius:18px; border:1px solid var(--line); background:rgba(255,255,255,.42); }}
    table {{ width:100%; min-width:920px; border-collapse:separate; border-spacing:0; }}
    th,td {{ border-right:1px solid var(--line); border-bottom:1px solid var(--line); }}
    thead th {{ position:sticky; top:0; z-index:1; background:rgba(246,239,223,.95); padding:14px 12px; font-size:13px; }}
    tbody th {{ position:sticky; left:0; z-index:1; background:rgba(246,239,223,.95); padding:14px 12px; min-width:136px; text-align:left; font-size:14px; }}
    td {{ min-width:118px; height:110px; padding:0; }}
    .cell {{ width:100%; height:100%; border:0; background:transparent; padding:12px; display:grid; align-content:space-between; gap:8px; text-align:left; }}
    .cell.active {{ outline:2px solid rgba(165,106,45,.7); outline-offset:-2px; }}
    .cell-top {{ display:flex; justify-content:space-between; gap:8px; font-size:12px; }}
    .cell-scale {{ font-size:23px; font-family:\"Georgia\",serif; line-height:1; }}
    .cell-sub {{ font-size:12px; color:var(--muted); }}
    .bar {{ height:6px; border-radius:999px; background:rgba(20,38,61,.08); overflow:hidden; }}
    .bar span {{ display:block; height:100%; }}
    .tag {{ padding:4px 8px; border-radius:999px; font-size:11px; background:rgba(255,255,255,.62); border:1px solid rgba(20,38,61,.08); }}
    .detail {{ padding:20px; display:grid; gap:14px; position:sticky; top:18px; }}
    .detail h2 {{ margin:0; font-size:28px; font-family:\"STSong\",\"SimSun\",serif; }}
    .detail .stats {{ display:grid; gap:10px; grid-template-columns:repeat(auto-fit,minmax(138px,1fr)); }}
    .detail .stat {{ padding:14px; border-radius:18px; border:1px solid var(--line); background:rgba(255,255,255,.48); }}
    .detail .stat strong {{ display:block; margin-top:8px; font-size:22px; font-family:\"Georgia\",serif; }}
    .muted {{ color:var(--muted); font-size:13px; }}
    .good {{ color:var(--good); }}
    .bad {{ color:var(--bad); }}
    .products {{ display:grid; gap:10px; max-height:360px; overflow:auto; }}
    .product {{ padding:14px; border:1px solid var(--line); border-radius:18px; background:rgba(255,255,255,.48); }}
    .product strong {{ display:block; margin-bottom:4px; }}
    @media (max-width:1080px) {{ .layout {{ grid-template-columns:1fr; }} .detail {{ position:static; }} }}
  </style>
</head>
<body>
  <div class=\"wrap\">
    <section class=\"panel hero\"><div class=\"eyebrow\">竞争格局地图</div><h1>ETF竞争对手产品矩阵</h1><div class=\"meta\" id=\"meta\"></div><div class=\"summary\" id=\"summary\"></div></section>
    <section class=\"panel controls\"><div class=\"row\"><span class=\"label\">周期</span><div class=\"legend\" id=\"periods\"></div></div><div class=\"row\"><span class=\"label\">标签</span><div class=\"legend\" id=\"tag-list\"></div></div><div class=\"row\"><span class=\"label\">赛道</span><select id=\"category\"></select><label class=\"upload\" for=\"upload\">导入 JSON</label><input id=\"upload\" type=\"file\" accept=\"application/json\"></div></section>
    <section class=\"layout\"><div class=\"panel matrix\"><div class=\"table-wrap\"><table><thead id=\"thead\"></thead><tbody id=\"tbody\"></tbody></table></div></div><aside class=\"panel detail\"><div><div class=\"eyebrow\">产品线拆解</div><h2 id=\"detail-title\">选择矩阵单元格</h2><p class=\"muted\" id=\"detail-sub\">点击任一单元格查看代表 ETF、细分类与市场位置。</p></div><div class=\"stats\" id=\"detail-stats\"></div><div><div class=\"muted\">标签</div><div class=\"chips\" id=\"detail-tags\"></div></div><div><div class=\"muted\">细分类分布</div><div class=\"chips\" id=\"detail-breakdown\"></div></div><div><div class=\"muted\">代表 ETF</div><div class=\"products\" id=\"detail-products\"></div></div></aside></section>
  </div>
  <script>
    const DATA_URL = "etf_competitor_matrix_data.json";
    window.__ETF_MATRIX_DATA__ = {payload_json};
    (() => {{
      const state = {{ payload: window.__ETF_MATRIX_DATA__, period: window.__ETF_MATRIX_DATA__.meta.default_period, tag: "全部", category: "全部赛道", selectedKey: null }};
      const $ = id => document.getElementById(id);
      const refs = {{ meta: $("meta"), summary: $("summary"), periods: $("periods"), tagList: $("tag-list"), category: $("category"), thead: $("thead"), tbody: $("tbody"), detailTitle: $("detail-title"), detailSub: $("detail-sub"), detailStats: $("detail-stats"), detailTags: $("detail-tags"), detailBreakdown: $("detail-breakdown"), detailProducts: $("detail-products"), upload: $("upload") }};
      const fmt = (value, digits = 1) => Number(value || 0).toLocaleString("zh-CN", {{ minimumFractionDigits: digits, maximumFractionDigits: digits }});
      const matrixData = () => state.payload.matrix;
      const visibleCells = () => matrixData().filter(cell => {{ if (state.category !== "全部赛道" && cell.primary_category !== state.category) return false; if (state.tag === "全部") return true; if (state.tag === "强势流入") return (cell.period_metrics[state.period].tags || []).includes("强势流入"); return (cell.base_tags || []).includes(state.tag); }});
      const heat = (scale, maxScale) => {{ if (!scale || !maxScale) return "rgba(20,38,61,.05)"; const intensity = Math.min(scale / maxScale, 1); return `rgba(165,106,45,${{(0.12 + intensity * 0.55).toFixed(3)}})`; }};
      const flow = value => value >= 0 ? "linear-gradient(90deg,#2d8d5f,#8bc89a)" : "linear-gradient(90deg,#8e342d,#d48268)";
      function hero() {{
        const meta = state.payload.meta; const matrix = matrixData(); const covered = matrix.filter(x => x.is_covered).length; const totalScale = matrix.reduce((sum, x) => sum + x.total_scale, 0); const strong = matrix.filter(x => (x.period_metrics[state.period].tags || []).includes("强势流入")).length;
        refs.meta.innerHTML = ""; [`数据日期：${{meta.data_date || "未识别"}}`, `来源文件：${{meta.source_name || "内嵌数据"}}`, `默认周期：${{state.period}}`].forEach(text => {{ const el = document.createElement("span"); el.textContent = text; refs.meta.appendChild(el); }});
        refs.summary.innerHTML = ""; [["目标公司", meta.target_companies.length, "聚焦 7 家管理人"], ["纳入产品", state.payload.products.length, "主矩阵口径"], ["覆盖单元", covered, "公司 × 赛道已布局格数"], ["总规模", `${{fmt(totalScale, 1)}} 亿元`, "目标公司汇总"], ["强势流入格", strong, `${{state.period}}前二正流入`]].forEach(([k, v, note]) => {{ const card = document.createElement("div"); card.className = "card"; card.innerHTML = `<div class=\"muted\">${{k}}</div><strong>${{v}}</strong><div class=\"muted\">${{note}}</div>`; refs.summary.appendChild(card); }});
      }}
      function controls() {{
        refs.periods.innerHTML = ""; state.payload.meta.periods.forEach(period => {{ const btn = document.createElement("button"); btn.textContent = period; btn.className = period === state.period ? "active" : ""; btn.onclick = () => {{ state.period = period; hero(); controls(); drawMatrix(); drawDetail(); }}; refs.periods.appendChild(btn); }});
        refs.tagList.innerHTML = ""; ["全部", ...state.payload.meta.tags].forEach(tag => {{ const btn = document.createElement("button"); btn.textContent = tag; btn.className = tag === state.tag ? "active" : ""; btn.onclick = () => {{ state.tag = tag; controls(); drawMatrix(); }}; refs.tagList.appendChild(btn); }});
        refs.category.innerHTML = ""; ["全部赛道", ...state.payload.meta.primary_categories].forEach(category => {{ const option = document.createElement("option"); option.value = category; option.textContent = category; option.selected = category === state.category; refs.category.appendChild(option); }});
      }}
      function drawMatrix() {{
        const cells = visibleCells(); const visible = new Set(cells.map(cell => `${{cell.company}}|${{cell.primary_category}}`)); const maxScale = Math.max(...cells.map(cell => cell.total_scale), 0);
        refs.thead.innerHTML = `<tr><th>一级赛道</th>${{state.payload.meta.target_companies.map(company => `<th>${{company}}</th>`).join("")}}</tr>`; refs.tbody.innerHTML = "";
        state.payload.meta.primary_categories.forEach(category => {{ if (state.category !== "全部赛道" && category !== state.category) return; const tr = document.createElement("tr"); tr.innerHTML = `<th>${{category}}</th>`; state.payload.meta.target_companies.forEach(company => {{ const cell = matrixData().find(item => item.company === company && item.primary_category === category); const key = `${{company}}|${{category}}`; const metric = cell.period_metrics[state.period]; const td = document.createElement("td"); if (!visible.has(key)) {{ td.innerHTML = `<div class=\"cell\" style=\"opacity:.22\"><div class=\"cell-top\"><span>已隐藏</span></div></div>`; tr.appendChild(td); return; }} const magnitude = Math.min(Math.abs(metric.net_inflow) / Math.max(maxScale, 1) * 100, 100); td.innerHTML = `<button class=\"cell ${{state.selectedKey === key ? "active" : ""}}\" style=\"background:${{heat(cell.total_scale, maxScale)}}\"><div class=\"cell-top\"><span>${{cell.is_covered ? `${{cell.product_count}}只产品` : "未覆盖"}}</span><span>${{cell.market_rank_by_scale ? `规模#${{cell.market_rank_by_scale}}` : "-"}}</span></div><div><div class=\"cell-scale\">${{cell.is_covered ? fmt(cell.total_scale, 1) : "0.0"}}</div><div class=\"cell-sub\">亿元规模 / ${{state.period}}</div></div><div><div class=\"bar\"><span style=\"width:${{magnitude}}%;background:${{flow(metric.net_inflow)}}\"></span></div><div class=\"cell-sub ${{metric.net_inflow >= 0 ? "good" : "bad"}}\">${{metric.net_inflow >= 0 ? "净流入" : "净流出"}} ${{fmt(Math.abs(metric.net_inflow), 2)}} 亿元</div><div class=\"tags\">${{[...(cell.base_tags || []), ...(metric.tags || [])].slice(0,3).map(tag => `<span class=\"tag\">${{tag}}</span>`).join("")}}</div></div></button>`; td.querySelector("button").onclick = () => {{ state.selectedKey = key; drawMatrix(); drawDetail(); }}; tr.appendChild(td); }}); refs.tbody.appendChild(tr); }});
      }}
      function drawDetail() {{
        const selected = matrixData().find(item => `${{item.company}}|${{item.primary_category}}` === state.selectedKey) || matrixData().find(item => item.is_covered); if (!selected) return; state.selectedKey = `${{selected.company}}|${{selected.primary_category}}`; const metric = selected.period_metrics[state.period]; const context = state.payload.market_context[selected.primary_category]; const products = state.payload.products.filter(item => item.company === selected.company && item.primary_category === selected.primary_category).sort((a, b) => b.latest_scale - a.latest_scale);
        refs.detailTitle.textContent = `${{selected.company}} · ${{selected.primary_category}}`; refs.detailSub.textContent = selected.is_covered ? `当前共布局 ${{selected.product_count}} 只产品，${{state.period}}净流${{metric.net_inflow >= 0 ? "入" : "出"}} ${{fmt(Math.abs(metric.net_inflow), 2)}} 亿元。` : `当前未覆盖该赛道，但市场已有 ${{context.market_product_count}} 只相关 ETF。`;
        refs.detailStats.innerHTML = ""; [["公司规模", `${{fmt(selected.total_scale, 1)}} 亿元`], ["赛道排名", selected.market_rank_by_scale ? `第 ${{selected.market_rank_by_scale}} 名` : "未布局"], ["市场总规模", `${{fmt(context.market_total_scale, 1)}} 亿元`], ["市场管理人", `${{context.manager_count}} 家`]].forEach(([k, v]) => {{ const div = document.createElement("div"); div.className = "stat"; div.innerHTML = `<div class=\"muted\">${{k}}</div><strong>${{v}}</strong>`; refs.detailStats.appendChild(div); }});
        refs.detailTags.innerHTML = ""; [...selected.base_tags, ...(metric.tags || [])].forEach(tag => {{ const el = document.createElement("span"); el.className = "tag"; el.textContent = tag; refs.detailTags.appendChild(el); }}); if (!refs.detailTags.children.length) refs.detailTags.innerHTML = '<span class="chip">当前无重点标签</span>';
        refs.detailBreakdown.innerHTML = ""; const breakdown = products.reduce((acc, product) => {{ acc[product.original_category] = (acc[product.original_category] || 0) + 1; return acc; }}, {{}}); const entries = Object.entries(breakdown).sort((a, b) => b[1] - a[1]); if (!entries.length) refs.detailBreakdown.innerHTML = '<span class="chip">暂无产品明细</span>'; entries.forEach(([name, count]) => {{ const el = document.createElement("span"); el.className = "tag"; el.textContent = `${{name}} · ${{count}}`; refs.detailBreakdown.appendChild(el); }});
        refs.detailProducts.innerHTML = ""; if (!products.length) {{ refs.detailProducts.innerHTML = '<div class="muted">该公司在此赛道暂无产品。</div>'; return; }} products.forEach(product => {{ const card = document.createElement("article"); card.className = "product"; card.innerHTML = `<strong>${{product.fund_name}}</strong><div class=\"muted\">${{product.fund_code}} · ${{product.index_short_name || "指数简称缺失"}} · 规模排名 ${{product.scale_rank || "-"}}</div><div class=\"stats\" style=\"margin-top:10px;\"><div class=\"stat\"><div class=\"muted\">最新规模</div><strong>${{fmt(product.latest_scale, 1)}} 亿元</strong></div><div class=\"stat\"><div class=\"muted\">${{state.period}}净流入</div><strong class=\"${{product.periods[state.period].净流入亿元 >= 0 ? "good" : "bad"}}\">${{fmt(product.periods[state.period].净流入亿元, 2)}} 亿元</strong></div></div><div class=\"tags\" style=\"margin-top:10px;\">${{product.structure_tags.concat(product.competition_tags).concat(product.strong_inflow_periods.includes(state.period) ? ["强势流入"] : []).map(tag => `<span class=\"tag\">${{tag}}</span>`).join("")}}</div>`; refs.detailProducts.appendChild(card); }});
      }}
      refs.category.onchange = event => {{ state.category = event.target.value; drawMatrix(); }};
      refs.upload.onchange = async event => {{ const file = event.target.files?.[0]; if (!file) return; const text = await file.text(); state.payload = JSON.parse(text); state.period = state.payload.meta.default_period; state.tag = "全部"; state.category = "全部赛道"; state.selectedKey = null; hero(); controls(); drawMatrix(); drawDetail(); }};
      hero(); controls(); drawMatrix(); drawDetail();
    }})();
  </script>
</body>
</html>
"""




def load_mapping_config(mapping_path: str | Path | None = None, write_if_missing: bool = False) -> dict[str, Any]:
    path = Path(mapping_path or DEFAULT_MAPPING_FILENAME)
    config = {"primary_categories": PRIMARY_CATEGORIES, "raw_to_primary_category": RAW_TO_PRIMARY_CATEGORY}
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    if write_if_missing:
        path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    return config


def write_dashboard_artifacts(payload: dict[str, Any], output_dir: str | Path) -> dict[str, str]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    data_path = out_dir / DEFAULT_DATA_FILENAME
    html_path = out_dir / DEFAULT_HTML_FILENAME
    data_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    html_path.write_text(render_dashboard_html(payload), encoding="utf-8")
    return {"data_path": str(data_path), "html_path": str(html_path)}


def find_default_source() -> Path:
    downloads = Path.home() / "Downloads"
    exact = downloads / "ETF\u57fa\u91d1\u6570\u636e\u65e5\u62a520260306_(\u6570\u636e\u622a\u81f320260305).xlsx"
    if exact.exists():
        return exact
    candidates = sorted(downloads.glob("ETF\u57fa\u91d1\u6570\u636e\u65e5\u62a5*.xlsx"))
    if not candidates:
        raise FileNotFoundError("\u672a\u627e\u5230 ETF \u57fa\u91d1\u6570\u636e\u65e5\u62a5 Excel \u6587\u4ef6")
    return candidates[-1]


def build_from_excel(source_path: str | Path, output_dir: str | Path, mapping_path: str | Path | None = None) -> dict[str, str]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    mapping_target = Path(mapping_path) if mapping_path else output_dir / DEFAULT_MAPPING_FILENAME
    load_mapping_config(mapping_target, write_if_missing=True)
    rows, metadata = read_etf_workbook(source_path)
    payload = build_dashboard_payload(rows, source_name=metadata["source_filename"], data_date=metadata["data_date"], source_metadata=metadata)
    outputs = write_dashboard_artifacts(payload, output_dir)
    outputs["mapping_path"] = str(mapping_target)
    return outputs


def main() -> None:
    parser = argparse.ArgumentParser(description="\u6784\u5efa ETF \u7ade\u4e89\u5bf9\u624b\u4ea7\u54c1\u77e9\u9635\u9759\u6001\u7f51\u9875")
    parser.add_argument("--source", default=str(find_default_source()), help="Excel \u6570\u636e\u6e90\u8def\u5f84")
    parser.add_argument("--output-dir", default=str(Path.cwd()), help="\u8f93\u51fa\u76ee\u5f55")
    parser.add_argument("--mapping", default=None, help="\u6620\u5c04 JSON \u8def\u5f84")
    args = parser.parse_args()
    outputs = build_from_excel(args.source, args.output_dir, args.mapping)
    print(json.dumps(outputs, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

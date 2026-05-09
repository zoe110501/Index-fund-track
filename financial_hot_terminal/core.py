from __future__ import annotations

import hashlib
import math
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Iterable


SOURCE_OFFICIAL = "official"
SOURCE_MEDIA = "media"
SOURCE_SOCIAL = "social"

REVIEW_CANDIDATE = "candidate"
REVIEW_SELECTED = "selected"
REVIEW_APPROVED = "approved"
REVIEW_REJECTED = "rejected"

DISCLAIMER = "非投资建议：本页面仅用于资讯线索和材料草稿整理，不构成任何投资建议。请以原文公告和正式披露为准。"
SHANGHAI_TZ = timezone(timedelta(hours=8))

ADVICE_PATTERNS = (
    "买入",
    "卖出",
    "建仓",
    "满仓",
    "目标收益",
    "收益保证",
    "稳赚",
    "必涨",
    "推荐购买",
)

THEME_RULES = (
    ("机器人", ("机器人", "具身智能", "智能制造", "自动化")),
    ("人工智能", ("人工智能", "AI", "算力", "大模型", "芯片")),
    ("红利低波", ("红利", "低波", "分红", "央企")),
    ("中证A500", ("A500", "中证A500", "核心资产")),
    ("港股通", ("港股", "港股通", "恒生")),
    ("新能源", ("新能源", "光伏", "储能", "电池")),
)

PRODUCT_UNIVERSE = (
    {
        "name": "中证A500ETF",
        "kind": "ETF",
        "keywords": ("A500", "中证A500", "核心资产"),
        "reason": "事件可能影响宽基配置和中证A500相关产品销售问答。",
    },
    {
        "name": "机器人主题ETF",
        "kind": "ETF",
        "keywords": ("机器人", "具身智能", "智能制造", "自动化"),
        "reason": "事件与机器人产业链景气度和主题ETF关注度相关。",
    },
    {
        "name": "人工智能主题指数",
        "kind": "指数",
        "keywords": ("人工智能", "AI", "算力", "大模型", "芯片"),
        "reason": "事件与AI主题指数成分和投资者关注方向相关。",
    },
    {
        "name": "港股通互联网ETF",
        "kind": "ETF",
        "keywords": ("港股", "港股通", "恒生", "互联网"),
        "reason": "事件可能影响港股通和港股互联网产品叙事。",
    },
    {
        "name": "红利低波基金",
        "kind": "基金",
        "keywords": ("红利", "低波", "分红", "央企"),
        "reason": "事件可能进入红利低波产品的稳健配置话术。",
    },
)


@dataclass(frozen=True)
class RawItem:
    id: str
    source_id: str
    source_name: str
    source_type: str
    title: str
    url: str
    content: str
    published_at: datetime
    market: str
    social_heat: float = 0.0


@dataclass(frozen=True)
class ProductLink:
    name: str
    kind: str
    relevance_score: float
    reason: str


@dataclass(frozen=True)
class Hotspot:
    id: str
    title: str
    summary: str
    category: str
    market: str
    source_types: tuple[str, ...]
    source_names: tuple[str, ...]
    source_urls: tuple[str, ...]
    published_at: datetime
    created_at: datetime
    market_impact: float
    product_relevance: float
    verification_confidence: float
    freshness: float
    social_heat: float
    hot_score: float
    review_status: str
    verification_status: str
    risk_tags: tuple[str, ...]
    llm_model: str
    evidence: tuple[str, ...]
    product_links: tuple[ProductLink, ...] = field(default_factory=tuple)


def stable_id(*parts: str) -> str:
    payload = "|".join(part.strip().lower() for part in parts if part)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]


def calculate_hot_score(
    *,
    market_impact: float,
    product_relevance: float,
    verification_confidence: float,
    freshness: float,
    social_heat: float,
) -> float:
    score = (
        market_impact * 0.40
        + product_relevance * 0.25
        + verification_confidence * 0.20
        + freshness * 0.10
        + social_heat * 0.05
    )
    return round(max(0.0, min(score, 100.0)), 2)


def freshness_score(published_at: datetime, now: datetime) -> float:
    age_hours = max(0.0, (now - published_at).total_seconds() / 3600)
    if age_hours <= 1:
        return 100.0
    if age_hours >= 72:
        return 15.0
    return round(100 - (age_hours - 1) * (85 / 71), 2)


def compliance_violations(text: str) -> list[str]:
    return [pattern for pattern in ADVICE_PATTERNS if pattern in text]


def classify_category(text: str) -> str:
    for category, keywords in THEME_RULES:
        if any(keyword.lower() in text.lower() for keyword in keywords):
            return category
    if any(keyword in text for keyword in ("公告", "披露", "回购", "业绩")):
        return "公司公告"
    if any(keyword in text for keyword in ("基金", "ETF", "指数")):
        return "产品动态"
    return "市场动态"


def detect_risk_tags(items: Iterable[RawItem], verification_status: str, summary: str) -> tuple[str, ...]:
    tags: list[str] = []
    source_types = {item.source_type for item in items}
    if SOURCE_SOCIAL in source_types and verification_status != "已交叉验证":
        tags.append("社媒线索待核验")
    if compliance_violations(summary):
        tags.append("疑似投资建议表述")
    if not tags:
        tags.append("需引用原文核对")
    return tuple(tags)


def link_products(text: str) -> tuple[ProductLink, ...]:
    links: list[ProductLink] = []
    text_lower = text.lower()
    for product in PRODUCT_UNIVERSE:
        hits = sum(1 for keyword in product["keywords"] if keyword.lower() in text_lower)
        if hits:
            score = min(100.0, 45.0 + hits * 22.0)
            links.append(
                ProductLink(
                    name=str(product["name"]),
                    kind=str(product["kind"]),
                    relevance_score=score,
                    reason=str(product["reason"]),
                )
            )
    return tuple(sorted(links, key=lambda link: link.relevance_score, reverse=True))


def summarize_items(items: list[RawItem]) -> str:
    leading = items[0]
    text = re.sub(r"\s+", " ", leading.content or leading.title).strip()
    if len(text) > 120:
        text = text[:118].rstrip() + "..."
    return text


def estimate_market_impact(text: str, source_types: set[str]) -> float:
    score = 42.0
    high_impact = ("监管", "政策", "回购", "业绩", "并购", "停复牌", "港股通", "ETF", "指数")
    score += sum(8.0 for keyword in high_impact if keyword in text)
    if SOURCE_OFFICIAL in source_types:
        score += 12.0
    if SOURCE_SOCIAL in source_types:
        score += 5.0
    return min(score, 95.0)


def build_hotspot_from_items(
    items: list[RawItem],
    *,
    now: datetime | None = None,
    manual_review_status: str | None = None,
    llm_model: str = "rules-v1",
) -> Hotspot:
    if not items:
        raise ValueError("items is required")
    now = now or datetime.now(SHANGHAI_TZ)
    sorted_items = sorted(items, key=lambda item: item.published_at)
    source_types = {item.source_type for item in sorted_items}
    source_names = tuple(dict.fromkeys(item.source_name for item in sorted_items))
    source_urls = tuple(dict.fromkeys(item.url for item in sorted_items))
    combined_text = " ".join([item.title + " " + item.content for item in sorted_items])
    product_links = link_products(combined_text)
    verification_status = "已交叉验证" if SOURCE_SOCIAL in source_types and len(source_types - {SOURCE_SOCIAL}) > 0 else "官方/可信源"
    if source_types == {SOURCE_SOCIAL}:
        verification_status = "待二次确认"

    verification_confidence = 45.0 if verification_status == "待二次确认" else 82.0
    if verification_status == "已交叉验证":
        verification_confidence = 92.0
    published_at = max(item.published_at for item in sorted_items)
    freshness = freshness_score(published_at, now)
    social_heat = max([item.social_heat for item in sorted_items] + [0.0])
    market_impact = estimate_market_impact(combined_text, source_types)
    product_relevance = max([link.relevance_score for link in product_links] + [35.0])
    hot_score = calculate_hot_score(
        market_impact=market_impact,
        product_relevance=product_relevance,
        verification_confidence=verification_confidence,
        freshness=freshness,
        social_heat=social_heat,
    )
    summary = summarize_items(sorted_items)

    if manual_review_status:
        review_status = manual_review_status
    elif verification_status == "待二次确认":
        review_status = REVIEW_CANDIDATE
    elif hot_score >= 60:
        review_status = REVIEW_SELECTED
    else:
        review_status = REVIEW_CANDIDATE

    return Hotspot(
        id=stable_id(sorted_items[0].title, sorted_items[0].market),
        title=sorted_items[0].title,
        summary=summary,
        category=classify_category(combined_text),
        market=sorted_items[0].market,
        source_types=tuple(sorted(source_types)),
        source_names=source_names,
        source_urls=source_urls,
        published_at=published_at,
        created_at=now,
        market_impact=market_impact,
        product_relevance=product_relevance,
        verification_confidence=verification_confidence,
        freshness=freshness,
        social_heat=social_heat,
        hot_score=hot_score,
        review_status=review_status,
        verification_status=verification_status,
        risk_tags=detect_risk_tags(sorted_items, verification_status, summary),
        llm_model=llm_model,
        evidence=tuple(item.content[:180] for item in sorted_items if item.content),
        product_links=product_links,
    )


def is_selected_candidate(hotspot: Hotspot) -> bool:
    if hotspot.review_status in {REVIEW_APPROVED, REVIEW_SELECTED}:
        return True
    return False

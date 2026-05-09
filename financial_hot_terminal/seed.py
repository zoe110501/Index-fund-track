from __future__ import annotations

from datetime import datetime, timedelta

from .core import SOURCE_OFFICIAL, SOURCE_SOCIAL, RawItem, build_hotspot_from_items, stable_id
from .repository import HotspotRepository


DEMO_SOURCES = (
    {
        "id": "cninfo",
        "name": "巨潮资讯",
        "source_type": SOURCE_OFFICIAL,
        "url": "https://www.cninfo.com.cn/",
        "enabled": True,
        "cadence_minutes": 30,
        "notes": "A股法定信息披露和公告检索示例源。",
    },
    {
        "id": "sse-star",
        "name": "上交所公告",
        "source_type": SOURCE_OFFICIAL,
        "url": "https://star.sse.com.cn/disclosure/announcement/",
        "enabled": True,
        "cadence_minutes": 30,
        "notes": "沪市和科创板公告示例源。",
    },
    {
        "id": "hkexnews",
        "name": "港交所披露易",
        "source_type": SOURCE_OFFICIAL,
        "url": "https://www.hkexnews.hk/index_c.htm",
        "enabled": True,
        "cadence_minutes": 30,
        "notes": "港股上市公司公告示例源。",
    },
    {
        "id": "kol-feed",
        "name": "公开KOL/RSS示例",
        "source_type": SOURCE_SOCIAL,
        "url": "https://example.com/public-finance-feed.xml",
        "enabled": True,
        "cadence_minutes": 30,
        "notes": "仅接入公开feed；社媒线索进入精选前需要二次确认。",
    },
)


def _raw_item(
    *,
    source_id: str,
    source_name: str,
    source_type: str,
    title: str,
    content: str,
    url: str,
    published_at: datetime,
    market: str,
    social_heat: float = 0,
) -> RawItem:
    return RawItem(
        id=stable_id(source_id, url),
        source_id=source_id,
        source_name=source_name,
        source_type=source_type,
        title=title,
        url=url,
        content=content,
        published_at=published_at,
        market=market,
        social_heat=social_heat,
    )


def seed_demo_data(repository: HotspotRepository, *, now: datetime) -> None:
    for source in DEMO_SOURCES:
        repository.upsert_source(source)

    official_robot = _raw_item(
        source_id="cninfo",
        source_name="巨潮资讯",
        source_type=SOURCE_OFFICIAL,
        title="机器人产业链政策更新带动智能制造主题关注",
        content="公告和政策信息显示，机器人、具身智能与智能制造方向获得更多产业支持，相关ETF和主题指数需要更新销售问答。",
        url="https://www.cninfo.com.cn/new/disclosure/detail/demo-robot",
        published_at=now - timedelta(hours=1),
        market="A股",
    )
    social_robot = _raw_item(
        source_id="kol-feed",
        source_name="公开KOL/RSS示例",
        source_type=SOURCE_SOCIAL,
        title=official_robot.title,
        content="公开社媒讨论显示机器人主题热度上升，但需要以公告和官方披露为准。",
        url="https://example.com/public-finance-feed/robot",
        published_at=now - timedelta(minutes=35),
        market="A股",
        social_heat=76,
    )
    official_hk = _raw_item(
        source_id="hkexnews",
        source_name="港交所披露易",
        source_type=SOURCE_OFFICIAL,
        title="港股通互联网公司回购公告密集披露",
        content="多家港股通互联网公司披露回购进展，市场关注港股通互联网ETF和恒生相关产品的材料更新。",
        url="https://www.hkexnews.hk/demo-buyback",
        published_at=now - timedelta(hours=2),
        market="港股",
    )
    social_rumor = _raw_item(
        source_id="kol-feed",
        source_name="公开KOL/RSS示例",
        source_type=SOURCE_SOCIAL,
        title="未证实传闻：某AI芯片指数产品将迎来大额申购",
        content="单一社媒来源称AI芯片指数产品可能出现大额申购，尚未找到公告、基金公司或可信媒体确认。",
        url="https://example.com/public-finance-feed/unverified-ai-chip",
        published_at=now - timedelta(minutes=20),
        market="A股",
        social_heat=88,
    )

    grouped = [
        [official_robot, social_robot],
        [official_hk],
        [social_rumor],
    ]
    for items in grouped:
        for item in items:
            repository.save_raw_item(
                {
                    "id": item.id,
                    "source_id": item.source_id,
                    "source_name": item.source_name,
                    "source_type": item.source_type,
                    "title": item.title,
                    "url": item.url,
                    "content": item.content,
                    "published_at": item.published_at.isoformat(),
                    "market": item.market,
                    "social_heat": item.social_heat,
                }
            )
        repository.save_hotspot(build_hotspot_from_items(items, now=now))

    for source in DEMO_SOURCES:
        repository.add_fetch_audit(source["id"], "ok", 1, "demo seed")

    selected = repository.list_hotspots(status="selected", limit=20)
    sections = [
        {
            "name": "精选热点",
            "items": [
                {
                    "title": item["title"],
                    "summary": item["summary"],
                    "category": item["category"],
                    "market": item["market"],
                    "hot_score": item["hot_score"],
                    "source_urls": item["source_urls"],
                }
                for item in selected
            ],
        }
    ]
    repository.save_daily_report(
        date=now.strftime("%Y-%m-%d"),
        title=f"金融热点日报 {now.strftime('%Y-%m-%d')}",
        summary="自动汇总已验证或已复核的金融热点，供投研和销售支持准备材料草稿。",
        sections=sections,
    )

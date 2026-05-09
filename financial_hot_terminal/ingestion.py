from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Iterable

from .core import RawItem, SHANGHAI_TZ, build_hotspot_from_items, stable_id
from .repository import HotspotRepository
from .source_registry import SourceConfig, load_source_registry


DEFAULT_REGISTRY_PATH = Path("config/source_registry.json")


def source_to_repository_dict(source: SourceConfig) -> dict[str, object]:
    return {
        "id": source.id,
        "name": source.name,
        "source_type": source.source_type,
        "url": source.url,
        "enabled": source.enabled,
        "cadence_minutes": source.cadence_minutes,
        "notes": source.notes,
    }


def sample_items_from_source(source: SourceConfig, now: datetime) -> list[RawItem]:
    items: list[RawItem] = []
    for index, item in enumerate(source.sample_items):
        published_at = datetime.fromisoformat(item["published_at"]) if item.get("published_at") else now
        if published_at.tzinfo is None:
            published_at = published_at.replace(tzinfo=SHANGHAI_TZ)
        url = item.get("url") or f"{source.url}#sample-{index}"
        title = item["title"]
        items.append(
            RawItem(
                id=stable_id(source.id, url),
                source_id=source.id,
                source_name=source.name,
                source_type=source.source_type,
                title=title,
                url=url,
                content=item.get("content", title),
                published_at=published_at,
                market=item.get("market", "A股"),
                social_heat=float(item.get("social_heat", 0)),
            )
        )
    return items


def group_items_for_hotspots(items: Iterable[RawItem]) -> list[list[RawItem]]:
    grouped: dict[str, list[RawItem]] = defaultdict(list)
    for item in items:
        key = item.title.replace("未证实传闻：", "").strip()
        grouped[key].append(item)
    return list(grouped.values())


def run_ingestion(
    repository: HotspotRepository,
    *,
    registry_path: str | Path = DEFAULT_REGISTRY_PATH,
    now: datetime | None = None,
) -> dict[str, object]:
    now = now or datetime.now(SHANGHAI_TZ)
    sources = load_source_registry(registry_path)
    all_items: list[RawItem] = []
    for source in sources:
        repository.upsert_source(source_to_repository_dict(source))
        if not source.enabled:
            repository.add_fetch_audit(source.id, "disabled", 0, "source disabled")
            continue
        items = sample_items_from_source(source, now)
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
        repository.add_fetch_audit(source.id, "ok", len(items), "sample registry ingestion")
        all_items.extend(items)

    for group in group_items_for_hotspots(all_items):
        repository.save_hotspot(build_hotspot_from_items(group, now=now))

    selected = repository.list_hotspots(status="selected", limit=30)
    repository.save_daily_report(
        date=now.strftime("%Y-%m-%d"),
        title=f"金融热点日报 {now.strftime('%Y-%m-%d')}",
        summary="自动生成的本地金融热点日报，面向投研和销售支持材料准备。",
        sections=[
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
        ],
    )
    return {"status": "ok", "sources": len(sources), "raw_items": len(all_items), "hotspots": len(selected)}

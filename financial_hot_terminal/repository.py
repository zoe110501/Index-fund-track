from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Any

from .core import DISCLAIMER, Hotspot, ProductLink, REVIEW_APPROVED, REVIEW_SELECTED, SHANGHAI_TZ


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _loads(value: str) -> Any:
    if not value:
        return []
    return json.loads(value)


def _iso(value: datetime) -> str:
    return value.isoformat()


def _parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


class HotspotRepository:
    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection

    def upsert_source(self, source: dict[str, Any]) -> None:
        self.connection.execute(
            """
            INSERT INTO sources (id, name, source_type, url, enabled, cadence_minutes, notes)
            VALUES (:id, :name, :source_type, :url, :enabled, :cadence_minutes, :notes)
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name,
                source_type=excluded.source_type,
                url=excluded.url,
                enabled=excluded.enabled,
                cadence_minutes=excluded.cadence_minutes,
                notes=excluded.notes
            """,
            {
                "id": source["id"],
                "name": source["name"],
                "source_type": source["source_type"],
                "url": source["url"],
                "enabled": int(source.get("enabled", True)),
                "cadence_minutes": int(source.get("cadence_minutes", 30)),
                "notes": source.get("notes", ""),
            },
        )
        self.connection.commit()

    def add_fetch_audit(self, source_id: str, status: str, item_count: int, message: str = "") -> None:
        self.connection.execute(
            """
            INSERT INTO fetch_audits (source_id, fetched_at, status, item_count, message)
            VALUES (?, ?, ?, ?, ?)
            """,
            (source_id, datetime.now(SHANGHAI_TZ).isoformat(), status, item_count, message),
        )
        self.connection.commit()

    def save_raw_item(self, item: dict[str, Any]) -> None:
        self.connection.execute(
            """
            INSERT INTO raw_items (
                id, source_id, source_name, source_type, title, url, content,
                published_at, market, social_heat, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_id, url) DO UPDATE SET
                title=excluded.title,
                content=excluded.content,
                published_at=excluded.published_at,
                social_heat=excluded.social_heat
            """,
            (
                item["id"],
                item["source_id"],
                item["source_name"],
                item["source_type"],
                item["title"],
                item["url"],
                item["content"],
                item["published_at"],
                item["market"],
                float(item.get("social_heat", 0)),
                item.get("created_at") or datetime.now(SHANGHAI_TZ).isoformat(),
            ),
        )
        self.connection.commit()

    def save_hotspot(self, hotspot: Hotspot) -> None:
        self.connection.execute(
            """
            INSERT INTO hotspots (
                id, title, summary, category, market, source_types, source_names, source_urls,
                published_at, created_at, market_impact, product_relevance, verification_confidence,
                freshness, social_heat, hot_score, review_status, verification_status, risk_tags,
                llm_model, evidence
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                title=excluded.title,
                summary=excluded.summary,
                category=excluded.category,
                market=excluded.market,
                source_types=excluded.source_types,
                source_names=excluded.source_names,
                source_urls=excluded.source_urls,
                published_at=excluded.published_at,
                market_impact=excluded.market_impact,
                product_relevance=excluded.product_relevance,
                verification_confidence=excluded.verification_confidence,
                freshness=excluded.freshness,
                social_heat=excluded.social_heat,
                hot_score=excluded.hot_score,
                review_status=excluded.review_status,
                verification_status=excluded.verification_status,
                risk_tags=excluded.risk_tags,
                llm_model=excluded.llm_model,
                evidence=excluded.evidence
            """,
            (
                hotspot.id,
                hotspot.title,
                hotspot.summary,
                hotspot.category,
                hotspot.market,
                _json(list(hotspot.source_types)),
                _json(list(hotspot.source_names)),
                _json(list(hotspot.source_urls)),
                _iso(hotspot.published_at),
                _iso(hotspot.created_at),
                hotspot.market_impact,
                hotspot.product_relevance,
                hotspot.verification_confidence,
                hotspot.freshness,
                hotspot.social_heat,
                hotspot.hot_score,
                hotspot.review_status,
                hotspot.verification_status,
                _json(list(hotspot.risk_tags)),
                hotspot.llm_model,
                _json(list(hotspot.evidence)),
            ),
        )
        self.connection.execute("DELETE FROM product_links WHERE hotspot_id = ?", (hotspot.id,))
        for link in hotspot.product_links:
            self.connection.execute(
                """
                INSERT INTO product_links (hotspot_id, name, kind, relevance_score, reason)
                VALUES (?, ?, ?, ?, ?)
                """,
                (hotspot.id, link.name, link.kind, link.relevance_score, link.reason),
            )
        self.connection.commit()

    def list_hotspots(
        self,
        *,
        status: str | None = None,
        q: str | None = None,
        market: str | None = None,
        category: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        where: list[str] = []
        params: list[Any] = []
        if status == "selected":
            where.append("review_status IN (?, ?)")
            params.extend([REVIEW_SELECTED, REVIEW_APPROVED])
        elif status:
            where.append("review_status = ?")
            params.append(status)
        if q:
            where.append("(title LIKE ? OR summary LIKE ?)")
            params.extend([f"%{q}%", f"%{q}%"])
        if market:
            where.append("market = ?")
            params.append(market)
        if category:
            where.append("category = ?")
            params.append(category)
        sql = "SELECT * FROM hotspots"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY hot_score DESC, published_at DESC LIMIT ?"
        params.append(limit)
        rows = self.connection.execute(sql, params).fetchall()
        return [self._hotspot_row(row) for row in rows]

    def get_hotspot(self, hotspot_id: str) -> dict[str, Any] | None:
        row = self.connection.execute("SELECT * FROM hotspots WHERE id = ?", (hotspot_id,)).fetchone()
        if not row:
            return None
        return self._hotspot_row(row)

    def review_hotspot(self, hotspot_id: str, *, status: str, reviewer: str, comment: str = "") -> dict[str, Any] | None:
        if not self.get_hotspot(hotspot_id):
            return None
        reviewed_at = datetime.now(SHANGHAI_TZ).isoformat()
        self.connection.execute(
            "INSERT INTO reviews (hotspot_id, status, reviewer, comment, reviewed_at) VALUES (?, ?, ?, ?, ?)",
            (hotspot_id, status, reviewer, comment, reviewed_at),
        )
        self.connection.execute("UPDATE hotspots SET review_status = ? WHERE id = ?", (status, hotspot_id))
        self.connection.commit()
        return self.get_hotspot(hotspot_id)

    def save_daily_report(self, *, date: str, title: str, summary: str, sections: list[dict[str, Any]]) -> None:
        self.connection.execute(
            """
            INSERT INTO daily_reports (date, title, summary, sections, disclaimer, generated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(date) DO UPDATE SET
                title=excluded.title,
                summary=excluded.summary,
                sections=excluded.sections,
                disclaimer=excluded.disclaimer,
                generated_at=excluded.generated_at
            """,
            (date, title, summary, _json(sections), DISCLAIMER, datetime.now(SHANGHAI_TZ).isoformat()),
        )
        self.connection.commit()

    def get_daily_report(self, date: str | None = None) -> dict[str, Any] | None:
        if date:
            row = self.connection.execute("SELECT * FROM daily_reports WHERE date = ?", (date,)).fetchone()
        else:
            row = self.connection.execute("SELECT * FROM daily_reports ORDER BY date DESC LIMIT 1").fetchone()
        if not row:
            return None
        return {
            "date": row["date"],
            "title": row["title"],
            "summary": row["summary"],
            "sections": _loads(row["sections"]),
            "disclaimer": row["disclaimer"],
            "generated_at": row["generated_at"],
        }

    def list_dailies(self, limit: int = 30) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT date, title, summary, generated_at FROM daily_reports ORDER BY date DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]

    def source_status(self) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            """
            SELECT s.*, a.status latest_status, a.fetched_at latest_fetched_at, a.item_count latest_item_count, a.message latest_message
            FROM sources s
            LEFT JOIN fetch_audits a ON a.id = (
                SELECT id FROM fetch_audits WHERE source_id = s.id ORDER BY fetched_at DESC LIMIT 1
            )
            ORDER BY s.source_type, s.name
            """
        ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["enabled"] = bool(item["enabled"])
            item["latest_status"] = item.get("latest_status") or "never"
            item["latest_item_count"] = item.get("latest_item_count") or 0
            result.append(item)
        return result

    def _hotspot_row(self, row: sqlite3.Row) -> dict[str, Any]:
        product_rows = self.connection.execute(
            "SELECT name, kind, relevance_score, reason FROM product_links WHERE hotspot_id = ? ORDER BY relevance_score DESC",
            (row["id"],),
        ).fetchall()
        item = dict(row)
        item["source_types"] = _loads(item["source_types"])
        item["source_names"] = _loads(item["source_names"])
        item["source_urls"] = _loads(item["source_urls"])
        item["risk_tags"] = _loads(item["risk_tags"])
        item["evidence"] = _loads(item["evidence"])
        item["product_links"] = [dict(product) for product in product_rows]
        item["disclaimer"] = DISCLAIMER
        item["published_date"] = _parse_dt(item["published_at"]).astimezone(SHANGHAI_TZ).strftime("%Y-%m-%d")
        item["published_time"] = _parse_dt(item["published_at"]).astimezone(SHANGHAI_TZ).strftime("%H:%M")
        return item

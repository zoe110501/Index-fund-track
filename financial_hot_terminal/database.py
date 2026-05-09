from __future__ import annotations

import sqlite3
from pathlib import Path


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS sources (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    source_type TEXT NOT NULL,
    url TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    cadence_minutes INTEGER NOT NULL DEFAULT 30,
    notes TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS raw_items (
    id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    source_name TEXT NOT NULL,
    source_type TEXT NOT NULL,
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    content TEXT NOT NULL,
    published_at TEXT NOT NULL,
    market TEXT NOT NULL,
    social_heat REAL NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    UNIQUE(source_id, url)
);

CREATE TABLE IF NOT EXISTS hotspots (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    summary TEXT NOT NULL,
    category TEXT NOT NULL,
    market TEXT NOT NULL,
    source_types TEXT NOT NULL,
    source_names TEXT NOT NULL,
    source_urls TEXT NOT NULL,
    published_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    market_impact REAL NOT NULL,
    product_relevance REAL NOT NULL,
    verification_confidence REAL NOT NULL,
    freshness REAL NOT NULL,
    social_heat REAL NOT NULL,
    hot_score REAL NOT NULL,
    review_status TEXT NOT NULL,
    verification_status TEXT NOT NULL,
    risk_tags TEXT NOT NULL,
    llm_model TEXT NOT NULL,
    evidence TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS entities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    hotspot_id TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_name TEXT NOT NULL,
    FOREIGN KEY(hotspot_id) REFERENCES hotspots(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS product_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    hotspot_id TEXT NOT NULL,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    relevance_score REAL NOT NULL,
    reason TEXT NOT NULL,
    FOREIGN KEY(hotspot_id) REFERENCES hotspots(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    hotspot_id TEXT NOT NULL,
    status TEXT NOT NULL,
    reviewer TEXT NOT NULL,
    comment TEXT NOT NULL DEFAULT '',
    reviewed_at TEXT NOT NULL,
    FOREIGN KEY(hotspot_id) REFERENCES hotspots(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS daily_reports (
    date TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    summary TEXT NOT NULL,
    sections TEXT NOT NULL,
    disclaimer TEXT NOT NULL,
    generated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fetch_audits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    status TEXT NOT NULL,
    item_count INTEGER NOT NULL DEFAULT 0,
    message TEXT NOT NULL DEFAULT ''
);
"""


def open_connection(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    return connection


def initialize_database(connection: sqlite3.Connection) -> None:
    connection.executescript(SCHEMA)
    connection.commit()

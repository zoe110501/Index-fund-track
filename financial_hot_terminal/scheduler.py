from __future__ import annotations

from datetime import datetime, time

from .core import SHANGHAI_TZ


def is_ingest_window(now: datetime | None = None) -> bool:
    now = (now or datetime.now(SHANGHAI_TZ)).astimezone(SHANGHAI_TZ)
    return time(8, 0) <= now.time() <= time(23, 0)


def should_run_half_hourly(last_run: datetime | None, now: datetime | None = None) -> bool:
    now = (now or datetime.now(SHANGHAI_TZ)).astimezone(SHANGHAI_TZ)
    if not is_ingest_window(now):
        return False
    if last_run is None:
        return True
    return (now - last_run.astimezone(SHANGHAI_TZ)).total_seconds() >= 30 * 60


def is_daily_generation_time(now: datetime | None = None) -> bool:
    now = (now or datetime.now(SHANGHAI_TZ)).astimezone(SHANGHAI_TZ)
    return now.hour == 8 and now.minute >= 30

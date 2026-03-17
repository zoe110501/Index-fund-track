from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.request import Request, urlopen

from csrc_index_monitor import DEFAULT_STATE_FILE, load_state, save_state


CHINA_TZ = timezone(timedelta(hours=8))
DEFAULT_THRESHOLD_MINUTES = 90
DEFAULT_API_URL = "https://api.github.com"
DEFAULT_WORKFLOW_FILENAME = "csrc-index-monitor.yml"
MONITOR_START_HOUR = 8
MONITOR_END_HOUR = 21
MONITOR_SLOT_MINUTE = 5
WATCHDOG_START = time(8, 0)
WATCHDOG_END = time(22, 45)
DEFAULT_WATCHDOG_STATE = {
    "last_catchup_target_slot": None,
    "last_catchup_triggered_at": None,
}


@dataclass(frozen=True)
class WatchdogConfig:
    state_file_path: Path
    repository: str
    token: str
    workflow_filename: str
    api_url: str
    threshold_minutes: int


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    return datetime.fromisoformat(normalized)


def normalize_watchdog_state(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    normalized_snapshot = dict(snapshot or {})
    normalized_snapshot["records"] = normalized_snapshot.get("records") or {}
    normalized_snapshot["last_notified_event_ids"] = normalized_snapshot.get("last_notified_event_ids") or []
    watchdog_state = dict(DEFAULT_WATCHDOG_STATE)
    watchdog_state.update(normalized_snapshot.get("watchdog") or {})
    normalized_snapshot["watchdog"] = watchdog_state
    return normalized_snapshot


def is_within_watchdog_window(now: datetime) -> bool:
    local_now = now.astimezone(CHINA_TZ).timetz().replace(tzinfo=None)
    return WATCHDOG_START <= local_now <= WATCHDOG_END


def latest_expected_slot(now: datetime) -> datetime | None:
    local_now = now.astimezone(CHINA_TZ)
    if local_now.hour < MONITOR_START_HOUR:
        return None

    slot_hour = min(local_now.hour, MONITOR_END_HOUR)
    if local_now.minute < MONITOR_SLOT_MINUTE:
        slot_hour -= 1
    if slot_hour < MONITOR_START_HOUR:
        return None

    return datetime(
        local_now.year,
        local_now.month,
        local_now.day,
        slot_hour,
        MONITOR_SLOT_MINUTE,
        tzinfo=CHINA_TZ,
    )


def evaluate_watchdog(
    snapshot: dict[str, Any] | None,
    *,
    now: datetime,
    threshold_minutes: int = DEFAULT_THRESHOLD_MINUTES,
) -> dict[str, Any]:
    normalized = normalize_watchdog_state(snapshot)
    if not is_within_watchdog_window(now):
        return {"should_dispatch": False, "reason": "outside_watchdog_window", "target_slot": None}

    target_slot = latest_expected_slot(now)
    if target_slot is None:
        return {"should_dispatch": False, "reason": "before_first_slot", "target_slot": None}

    target_slot_iso = target_slot.isoformat()
    last_success_at = parse_timestamp(normalized.get("last_success_at"))
    threshold = timedelta(minutes=threshold_minutes)

    if last_success_at and last_success_at.astimezone(CHINA_TZ) >= target_slot:
        return {"should_dispatch": False, "reason": "up_to_date", "target_slot": target_slot_iso}

    staleness_anchor = last_success_at or target_slot
    if now - staleness_anchor <= threshold:
        return {"should_dispatch": False, "reason": "within_threshold", "target_slot": target_slot_iso}

    if normalized["watchdog"].get("last_catchup_target_slot") == target_slot_iso:
        return {
            "should_dispatch": False,
            "reason": "already_dispatched_for_target_slot",
            "target_slot": target_slot_iso,
        }

    return {"should_dispatch": True, "reason": "stale_within_window", "target_slot": target_slot_iso}


def record_catchup_dispatch(snapshot: dict[str, Any] | None, *, target_slot: str, triggered_at: str) -> dict[str, Any]:
    normalized = normalize_watchdog_state(snapshot)
    normalized["watchdog"] = {
        "last_catchup_target_slot": target_slot,
        "last_catchup_triggered_at": triggered_at,
    }
    return normalized


def dispatch_workflow(*, api_url: str, repository: str, workflow_filename: str, token: str) -> None:
    request = Request(
        f"{api_url}/repos/{repository}/actions/workflows/{workflow_filename}/dispatches",
        data=json.dumps({"ref": "main"}).encode("utf-8"),
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "csrc-watchdog/1.0",
        },
        method="POST",
    )
    with urlopen(request, timeout=30):
        return


def run_watchdog(
    *,
    config: WatchdogConfig,
    now: datetime | None = None,
    dispatch_func: Callable[..., None] | None = None,
) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc).replace(microsecond=0)
    dispatch_func = dispatch_func or dispatch_workflow

    snapshot = load_state(config.state_file_path)
    decision = evaluate_watchdog(snapshot, now=now, threshold_minutes=config.threshold_minutes)
    if not decision["should_dispatch"]:
        return {
            "status": "idle",
            "decision": decision,
            "state_file_path": str(config.state_file_path),
        }

    dispatch_func(
        api_url=config.api_url,
        repository=config.repository,
        workflow_filename=config.workflow_filename,
        token=config.token,
    )
    updated_snapshot = record_catchup_dispatch(
        snapshot,
        target_slot=decision["target_slot"],
        triggered_at=now.astimezone(CHINA_TZ).isoformat(),
    )
    save_state(config.state_file_path, updated_snapshot)
    return {
        "status": "dispatched",
        "decision": decision,
        "state_file_path": str(config.state_file_path),
    }


def require_env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def load_config_from_env() -> WatchdogConfig:
    return WatchdogConfig(
        state_file_path=Path(os.getenv("STATE_FILE_PATH", str(DEFAULT_STATE_FILE))),
        repository=require_env("GITHUB_REPOSITORY"),
        token=require_env("GITHUB_TOKEN"),
        workflow_filename=os.getenv("TARGET_WORKFLOW_FILENAME", DEFAULT_WORKFLOW_FILENAME),
        api_url=os.getenv("GITHUB_API_URL", DEFAULT_API_URL),
        threshold_minutes=int(os.getenv("WATCHDOG_THRESHOLD_MINUTES", str(DEFAULT_THRESHOLD_MINUTES))),
    )


def main() -> int:
    try:
        config = load_config_from_env()
        result = run_watchdog(config=config)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "error",
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                },
                ensure_ascii=False,
                indent=2,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import json
import os
import smtplib
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlencode
from urllib.request import Request, urlopen


API_URL = "https://neris.csrc.gov.cn/alappr-delare/home/approval-progress/v1/list"
DEFAULT_KEYWORD = "指数"
DEFAULT_PAGE_SIZE = 1000
DEFAULT_STATE_FILE = Path("state/csrc_index_monitor_state.json")
EVENT_ID_SEPARATOR = "|"


@dataclass(frozen=True)
class MonitorConfig:
    keyword: str
    state_file_path: Path
    smtp_host: str
    smtp_port: int
    smtp_username: str
    smtp_password: str
    alert_email_from: str
    alert_email_to: list[str]


def make_step_id(step: dict[str, Any]) -> str:
    task_name = step.get("taskName") or step.get("task_name") or ""
    fnsh_date = step.get("fnshDate") or step.get("fnsh_date") or ""
    file_code = step.get("alFileCde") or step.get("al_file_cde") or "-"
    return f"{task_name}{EVENT_ID_SEPARATOR}{fnsh_date}{EVENT_ID_SEPARATOR}{file_code}"


def normalize_step(step: dict[str, Any]) -> dict[str, str]:
    task_name = step.get("taskName") or step.get("task_name") or ""
    fnsh_date = step.get("fnshDate") or step.get("fnsh_date") or ""
    file_code = step.get("alFileCde") or step.get("al_file_cde") or "-"
    return {
        "task_name": task_name,
        "fnsh_date": fnsh_date,
        "al_file_cde": file_code,
        "step_id": make_step_id(
            {
                "taskName": task_name,
                "fnshDate": fnsh_date,
                "alFileCde": file_code,
            }
        ),
    }


def normalize_record(record: dict[str, Any]) -> dict[str, Any]:
    title = record.get("showCntnt") or record.get("title") or ""
    steps = [normalize_step(step) for step in (record.get("aprvSchdPubFlowViewResultList") or record.get("steps") or [])]
    return {
        "record_id": record.get("alAppLtCde") or record.get("record_id") or "",
        "title": title,
        "app_date": record.get("appDate") or record.get("app_date") or "",
        "steps": steps,
    }


def fetch_page_from_api(page_num: int, page_size: int, keyword: str) -> dict[str, Any]:
    params = urlencode(
        {
            "appMatrCde": "",
            "queryCondition": keyword,
            "pageNum": page_num,
            "pageSize": page_size,
        }
    )
    request = Request(
        f"{API_URL}?{params}",
        headers={
            "Accept": "application/json, text/plain, */*",
            "User-Agent": "Mozilla/5.0 (compatible; csrc-index-monitor/1.0)",
            "Referer": "https://neris.csrc.gov.cn/alappr-delare-front/home/toPubFlow",
        },
    )
    with urlopen(request, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if payload.get("code") != "0000":
        raise RuntimeError(f"CSRC API returned error: {payload.get('code')} {payload.get('message')}")
    return payload


def fetch_all_records(
    keyword: str,
    page_size: int = DEFAULT_PAGE_SIZE,
    fetch_page: Callable[[int, int, str], dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    fetch_page = fetch_page or fetch_page_from_api
    records: list[dict[str, Any]] = []
    page_num = 1

    while True:
        payload = fetch_page(page_num, page_size, keyword)
        data = payload.get("data") or {}
        raw_records = data.get("records") or []
        for raw_record in raw_records:
            normalized = normalize_record(raw_record)
            if keyword in normalized["title"]:
                records.append(normalized)

        total = int(data.get("total") or 0)
        current = int(data.get("current") or page_num)
        size = int(data.get("size") or page_size or 1)

        if total == 0 or current * size >= total or not raw_records:
            break
        page_num += 1

    return records


def event_id_for(event_type: str, record_id: str, step_id: str | None = None) -> str:
    prefix = event_type.replace("_", "-")
    if step_id:
        return f"{prefix}{EVENT_ID_SEPARATOR}{record_id}{EVENT_ID_SEPARATOR}{step_id}"
    return f"{prefix}{EVENT_ID_SEPARATOR}{record_id}"


def build_snapshot(records: list[dict[str, Any]], now_iso: str, notified_event_ids: list[str] | None = None) -> dict[str, Any]:
    snapshot_records: dict[str, dict[str, Any]] = {}
    for record in records:
        snapshot_records[record["record_id"]] = {
            "title": record["title"],
            "app_date": record["app_date"],
            "step_ids": [step["step_id"] for step in record.get("steps", [])],
        }
    return {
        "last_success_at": now_iso,
        "records": snapshot_records,
        "last_notified_event_ids": notified_event_ids or [],
    }


def diff_snapshots(old_snapshot: dict[str, Any], new_snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    old_records = old_snapshot.get("records") or {}
    new_records = new_snapshot.get("records") or {}
    already_notified = set(old_snapshot.get("last_notified_event_ids") or [])
    events: list[dict[str, Any]] = []

    for record_id, new_record in new_records.items():
        if record_id in old_records:
            continue
        event_id = event_id_for("new_record", record_id)
        if event_id in already_notified:
            continue
        events.append(
            {
                "event_type": "new_record",
                "event_id": event_id,
                "record_id": record_id,
                "title": new_record.get("title", ""),
                "app_date": new_record.get("app_date", ""),
            }
        )

    for record_id, new_record in new_records.items():
        old_record = old_records.get(record_id)
        if not old_record:
            continue
        old_step_ids = set(old_record.get("step_ids") or [])
        for step_id in new_record.get("step_ids") or []:
            if step_id in old_step_ids:
                continue
            event_id = event_id_for("new_step", record_id, step_id)
            if event_id in already_notified:
                continue
            task_name, fnsh_date, file_code = split_step_id(step_id)
            events.append(
                {
                    "event_type": "new_step",
                    "event_id": event_id,
                    "record_id": record_id,
                    "title": new_record.get("title", ""),
                    "app_date": new_record.get("app_date", ""),
                    "step_id": step_id,
                    "task_name": task_name,
                    "fnsh_date": fnsh_date,
                    "al_file_cde": file_code,
                }
            )

    return events


def split_step_id(step_id: str) -> tuple[str, str, str]:
    parts = step_id.split(EVENT_ID_SEPARATOR, 2)
    while len(parts) < 3:
        parts.append("-")
    return parts[0], parts[1], parts[2]


def format_email_summary(events: list[dict[str, Any]]) -> tuple[str, str]:
    new_records = [event for event in events if event["event_type"] == "new_record"]
    new_steps = [event for event in events if event["event_type"] == "new_step"]
    subject = f"CSRC 指数审批进展提醒：新产品 {len(new_records)} 条，新节点 {len(new_steps)} 条"

    sections: list[str] = []
    sections.append("本轮检测到以下增量：")
    sections.append("")
    sections.append(f"新产品（{len(new_records)} 条）")
    if new_records:
        for event in new_records:
            sections.append(f"- {event['title']} | 申请日期：{event['app_date']} | 记录ID：{event['record_id']}")
    else:
        sections.append("- 无")

    sections.append("")
    sections.append(f"审批新节点（{len(new_steps)} 条）")
    if new_steps:
        for event in new_steps:
            sections.append(
                f"- {event['title']} | 节点：{event['task_name']} | 完成日期：{event['fnsh_date']} | 附件代码：{event['al_file_cde']}"
            )
    else:
        sections.append("- 无")

    return subject, "\n".join(sections)


def send_email(*, config: MonitorConfig, subject: str, body: str, events: list[dict[str, Any]] | None = None) -> None:
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = config.alert_email_from
    message["To"] = ", ".join(config.alert_email_to)
    message.set_content(body)

    if config.smtp_port == 465:
        with smtplib.SMTP_SSL(config.smtp_host, config.smtp_port, timeout=30) as client:
            client.login(config.smtp_username, config.smtp_password)
            client.send_message(message)
        return

    with smtplib.SMTP(config.smtp_host, config.smtp_port, timeout=30) as client:
        client.ehlo()
        client.starttls()
        client.ehlo()
        client.login(config.smtp_username, config.smtp_password)
        client.send_message(message)


def load_state(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def save_state(path: Path, snapshot: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")


def run_monitor(
    *,
    config: MonitorConfig,
    fetch_records: Callable[[str], list[dict[str, Any]]] | None = None,
    send_email_func: Callable[..., None] | None = None,
    now_iso: str | None = None,
) -> dict[str, Any]:
    fetch_records = fetch_records or (lambda keyword: fetch_all_records(keyword))
    send_email_func = send_email_func or send_email
    now_iso = now_iso or datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    previous_snapshot = load_state(config.state_file_path)
    records = fetch_records(config.keyword)
    current_snapshot = build_snapshot(records, now_iso)

    if previous_snapshot is None:
        save_state(config.state_file_path, current_snapshot)
        return {
            "baseline_created": True,
            "event_count": 0,
            "events": [],
            "state_changed": True,
            "state_file_path": str(config.state_file_path),
        }

    events = diff_snapshots(previous_snapshot, current_snapshot)
    if not events:
        save_state(config.state_file_path, current_snapshot)
        return {
            "baseline_created": False,
            "event_count": 0,
            "events": [],
            "state_changed": True,
            "state_file_path": str(config.state_file_path),
        }

    subject, body = format_email_summary(events)
    send_email_func(config=config, subject=subject, body=body, events=events)

    notified_snapshot = build_snapshot(records, now_iso, notified_event_ids=[event["event_id"] for event in events])
    save_state(config.state_file_path, notified_snapshot)
    return {
        "baseline_created": False,
        "event_count": len(events),
        "events": events,
        "state_changed": True,
        "state_file_path": str(config.state_file_path),
        "email_subject": subject,
    }


def parse_email_recipients(raw_value: str) -> list[str]:
    recipients = [item.strip() for item in raw_value.split(",") if item.strip()]
    if not recipients:
        raise ValueError("ALERT_EMAIL_TO must contain at least one recipient")
    return recipients


def require_env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def load_config_from_env() -> MonitorConfig:
    return MonitorConfig(
        keyword=os.getenv("CSRC_QUERY_KEYWORD", DEFAULT_KEYWORD),
        state_file_path=Path(os.getenv("STATE_FILE_PATH", str(DEFAULT_STATE_FILE))),
        smtp_host=require_env("SMTP_HOST"),
        smtp_port=int(require_env("SMTP_PORT")),
        smtp_username=require_env("SMTP_USERNAME"),
        smtp_password=require_env("SMTP_PASSWORD"),
        alert_email_from=require_env("ALERT_EMAIL_FROM"),
        alert_email_to=parse_email_recipients(require_env("ALERT_EMAIL_TO")),
    )


def main() -> int:
    try:
        config = load_config_from_env()
        result = run_monitor(config=config)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        error_payload = {
            "status": "error",
            "error_type": exc.__class__.__name__,
            "message": str(exc),
        }
        print(json.dumps(error_payload, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

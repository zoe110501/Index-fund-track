from __future__ import annotations

import io
import json
import importlib
import os
import re
import smtplib
import subprocess
import sys
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from html import escape
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from xml.sax.saxutils import escape as xml_escape


API_URL = "https://neris.csrc.gov.cn/alappr-delare/home/approval-progress/v1/list"
DEFAULT_KEYWORD = "指数"
DEFAULT_PAGE_SIZE = 1000
DEFAULT_STATE_FILE = Path("state/csrc_index_monitor_state.json")
DEFAULT_REPORT_MODE = "incremental"
REPORT_MODE_INCREMENTAL = "incremental"
REPORT_MODE_DAILY_SUMMARY = "daily_summary"
EVENT_ID_SEPARATOR = "|"
SHANGHAI_TZ = timezone(timedelta(hours=8))
DEFAULT_PDF_CJK_FONT_CANDIDATES = (
    Path("C:/Windows/Fonts/simfang.ttf"),
    Path("/usr/share/fonts/opentype/noto/NotoSerifCJK-Regular.ttc"),
    Path("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"),
    Path("/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc"),
    Path("C:/Windows/Fonts/simsun.ttc"),
    Path("C:/Windows/Fonts/msyh.ttc"),
)
DEFAULT_PDF_LATIN_FONT_CANDIDATES = (
    Path("C:/Windows/Fonts/times.ttf"),
    Path("/usr/share/fonts/truetype/msttcorefonts/Times_New_Roman.ttf"),
)
DEFAULT_PDF_LATIN_BOLD_FONT_CANDIDATES = (
    Path("C:/Windows/Fonts/timesbd.ttf"),
    Path("/usr/share/fonts/truetype/msttcorefonts/Times_New_Roman_Bold.ttf"),
)
PDF_FONT_FAMILY_CJK = "IndexMonitorSimFang"
PDF_FONT_FAMILY_LATIN = "IndexMonitorTimesNewRoman"
PDF_FONT_FAMILY_LATIN_BOLD = "IndexMonitorTimesNewRomanBold"
DISPLAY_ETF_SOURCE = "交易型开放式指数证券投资基金"
DISPLAY_ETF_TARGET = "ETF"
LINKED_FUND_KEYWORD = "联接基金"
ETF_LINKED_TYPE = "ETF联接"
TITLE_PATTERN = re.compile(r"^关于(?P<manager>.+?)的《公开募集基金募集申请注册-(?P<product_name>.+?)》$")
ASCII_TEXT_PATTERN = re.compile(r"[\x00-\x7F]+")
MANAGER_SUFFIXES = (
    "基金管理有限责任公司",
    "基金管理有限公司",
    "基金管理公司",
    "基金管理",
    "基金管理股份有限公司",
    "资产管理有限责任公司",
    "资产管理有限公司",
    "资产管理公司",
    "资产管理",
    "管理有限责任公司",
    "管理有限公司",
    "股份有限公司",
    "有限责任公司",
    "有限公司",
    "公司",
)


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


def get_email_transport(config: MonitorConfig) -> str:
    return "SMTP_SSL" if config.smtp_port == 465 else "STARTTLS"


def mask_email_address(value: str) -> str:
    value = value.strip()
    if "@" not in value:
        if len(value) <= 2:
            return "*" * len(value)
        return f"{value[0]}***{value[-1]}"

    local_part, domain = value.split("@", 1)
    if not local_part:
        masked_local = "***"
    elif len(local_part) == 1:
        masked_local = f"{local_part}***"
    else:
        masked_local = f"{local_part[0]}***{local_part[-1]}"
    return f"{masked_local}@{domain}"


def email_domain(value: str) -> str:
    _, _, domain = value.strip().partition("@")
    return domain.lower()


def build_email_diagnostics(config: MonitorConfig) -> dict[str, Any]:
    username = config.smtp_username.strip().lower()
    sender = config.alert_email_from.strip().lower()
    username_domain = email_domain(config.smtp_username)
    sender_domain = email_domain(config.alert_email_from)
    warnings: list[str] = []

    if sender != username:
        warnings.append("ALERT_EMAIL_FROM does not exactly match SMTP_USERNAME.")
    if username_domain and sender_domain and username_domain != sender_domain:
        warnings.append("ALERT_EMAIL_FROM uses a different domain from SMTP_USERNAME.")

    return {
        "smtp_host": config.smtp_host,
        "smtp_port": config.smtp_port,
        "transport": get_email_transport(config),
        "smtp_username_masked": mask_email_address(config.smtp_username),
        "alert_email_from_masked": mask_email_address(config.alert_email_from),
        "alert_email_to_masked": [mask_email_address(recipient) for recipient in config.alert_email_to],
        "recipient_count": len(config.alert_email_to),
        "sender_matches_username": sender == username,
        "sender_domain_matches_username_domain": bool(username_domain and sender_domain and username_domain == sender_domain),
        "warnings": warnings,
    }


def count_events_by_type(events: list[dict[str, Any]]) -> tuple[int, int]:
    new_record_count = sum(1 for event in events if event.get("event_type") == "new_record")
    new_step_count = sum(1 for event in events if event.get("event_type") == "new_step")
    return new_record_count, new_step_count


def attach_monitor_diagnostics(
    result: dict[str, Any],
    *,
    config: MonitorConfig,
    events: list[dict[str, Any]],
    email_attempted: bool,
    email_status: str,
    report_mode: str,
    daily_baseline_path: Path,
    daily_baseline_created: bool = False,
    skipped_reason: str | None = None,
) -> dict[str, Any]:
    new_record_count, new_step_count = count_events_by_type(events)
    result["report_mode"] = report_mode
    result["new_record_count"] = new_record_count
    result["new_step_count"] = new_step_count
    result["daily_baseline_path"] = str(daily_baseline_path)
    result["daily_baseline_created"] = daily_baseline_created
    if skipped_reason:
        result["skipped_reason"] = skipped_reason
    result["email_diagnostics"] = build_email_diagnostics(config)
    result["email_delivery"] = {
        "attempted": email_attempted,
        "status": email_status,
        "recipient_count": len(config.alert_email_to),
        "transport": get_email_transport(config),
    }
    return result


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
        "step_id": make_step_id({"taskName": task_name, "fnshDate": fnsh_date, "alFileCde": file_code}),
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


def abbreviate_manager_name(name: str) -> str:
    for suffix in MANAGER_SUFFIXES:
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


def format_product_name_for_display(product_name: str) -> str:
    return product_name.replace(DISPLAY_ETF_SOURCE, DISPLAY_ETF_TARGET)


def classify_product_type(product_name: str) -> str:
    if LINKED_FUND_KEYWORD in product_name and (
        DISPLAY_ETF_SOURCE in product_name or "交易型开放式指数" in product_name or "ETF" in product_name
    ):
        return ETF_LINKED_TYPE
    if DISPLAY_ETF_SOURCE in product_name:
        return "ETF"
    return "普通指数"


def extract_display_fields(title: str) -> dict[str, str]:
    match = TITLE_PATTERN.match(title)
    if match:
        manager_raw = match.group("manager")
        product_name_raw = match.group("product_name")
        manager = abbreviate_manager_name(manager_raw)
    else:
        product_name_raw = title
        manager = ""

    return {
        "manager": manager,
        "product_name": format_product_name_for_display(product_name_raw),
        "product_type": classify_product_type(product_name_raw),
    }


def format_table(headers: list[str], rows: list[list[str]]) -> str:
    all_rows = [headers, *rows]
    widths = [max(display_width(row[index]) for row in all_rows) for index in range(len(headers))]
    lines = [format_table_row(headers, widths), format_table_separator(widths)]
    for row in rows:
        lines.append(format_table_row(row, widths))
    return "\n".join(lines)


def display_width(value: str) -> int:
    width = 0
    for char in value:
        width += 2 if unicodedata.east_asian_width(char) in {"W", "F"} else 1
    return width


def pad_cell(value: str, width: int) -> str:
    padding = width - display_width(value)
    return value + (" " * max(padding, 0))


def format_table_row(row: list[str], widths: list[int]) -> str:
    return " | ".join(pad_cell(value, widths[index]) for index, value in enumerate(row))


def format_table_separator(widths: list[int]) -> str:
    return "-+-".join("-" * width for width in widths)


def build_record_rows(events: list[dict[str, Any]]) -> list[list[str]]:
    rows: list[list[str]] = []
    for index, event in enumerate(events, start=1):
        display = extract_display_fields(event["title"])
        rows.append([str(index), display["manager"], display["product_name"], display["product_type"], event["app_date"]])
    return rows


def build_step_rows(events: list[dict[str, Any]]) -> list[list[str]]:
    rows: list[list[str]] = []
    for index, event in enumerate(events, start=1):
        display = extract_display_fields(event["title"])
        rows.append(
            [
                str(index),
                display["manager"],
                display["product_name"],
                display["product_type"],
                event["app_date"],
                event["task_name"],
                event["fnsh_date"],
            ]
        )
    return rows


def report_copy(report_mode: str) -> dict[str, str]:
    if report_mode == REPORT_MODE_DAILY_SUMMARY:
        return {
            "intro": "今日累计汇总如下：",
            "records_title": "今日新产品",
            "steps_title": "今日新增节点产品",
        }
    return {
        "intro": "本轮检测到以下增量：",
        "records_title": "新产品",
        "steps_title": "新增节点产品",
    }


def render_html_table(headers: list[str], rows: list[list[str]]) -> str:
    header_html = "".join(
        f"<th style=\"border:1px solid #1f2937;background:#1f4f82;color:#ffffff;padding:8px 12px;text-align:center;\">{escape(header)}</th>"
        for header in headers
    )
    row_html = []
    for row in rows:
        cells = "".join(f"<td style=\"border:1px solid #1f2937;padding:8px 12px;\">{escape(value)}</td>" for value in row)
        row_html.append(f"<tr>{cells}</tr>")
    return (
        "<table style=\"border-collapse:collapse;width:100%;font-family:FangSong,STFangsong,serif;margin:8px 0 16px 0;\">"
        f"<thead><tr>{header_html}</tr></thead>"
        f"<tbody>{''.join(row_html)}</tbody>"
        "</table>"
    )


def format_html_summary(events: list[dict[str, Any]], report_mode: str) -> str:
    copy = report_copy(report_mode)
    new_records = [event for event in events if event["event_type"] == "new_record"]
    new_steps = [event for event in events if event["event_type"] == "new_step"]
    return "".join(
        [
            "<div style=\"font-family:FangSong,STFangsong,serif;font-size:16px;color:#1f2937;\">",
            f"<p>{escape(copy['intro'])}</p>",
            f"<p><strong>{escape(copy['records_title'])}（{len(new_records)} 条）</strong></p>",
            render_html_table(["序号", "管理人", "产品名称", "产品类型", "上报日期"], build_record_rows(new_records)) if new_records else "<p>无</p>",
            f"<p><strong>{escape(copy['steps_title'])}（{len(new_steps)} 条）</strong></p>",
            render_html_table(["序号", "管理人", "产品名称", "产品类型", "上报日期", "最新节点", "节点日期"], build_step_rows(new_steps)) if new_steps else "<p>无</p>",
            "</div>",
        ]
    )


def format_email_summary(events: list[dict[str, Any]], report_mode: str, local_now: datetime) -> tuple[str, str]:
    new_record_count, new_step_count = count_events_by_type(events)
    if report_mode == REPORT_MODE_DAILY_SUMMARY:
        subject = f"指数基金审批日报{local_now:%Y-%m-%d}"
        body = "\n".join(
            [
                f"指数基金审批日报 {local_now:%Y-%m-%d}",
                f"今日新产品：{new_record_count} 条",
                f"今日新增节点产品：{new_step_count} 条",
                "请查看 HTML 正文和 PDF 附件获取完整汇总。",
            ]
        )
        return subject, body

    subject = f"指数基金审批进度（{local_now:%H}：00）"
    body = "\n".join(
        [
            "请查看支持 HTML 的邮件正文获取完整表格。",
            f"新产品：{new_record_count} 条",
            f"新增节点产品：{new_step_count} 条",
        ]
    )
    return subject, body


def build_pdf_lines(events: list[dict[str, Any]], local_now: datetime) -> list[str]:
    copy = report_copy(REPORT_MODE_DAILY_SUMMARY)
    new_records = [event for event in events if event["event_type"] == "new_record"]
    new_steps = [event for event in events if event["event_type"] == "new_step"]
    lines = [
        f"指数基金审批日报{local_now:%Y-%m-%d}",
        f"生成时间：{local_now:%Y-%m-%d %H:%M}",
        f"合计：新产品 {len(new_records)} 条，新增节点产品 {len(new_steps)} 条",
        "",
        f"{copy['records_title']}（{len(new_records)} 条）",
    ]
    if new_records:
        for index, event in enumerate(new_records, start=1):
            display = extract_display_fields(event["title"])
            lines.append(f"{index}. {display['manager']} | {display['product_name']} | {event['app_date']}")
    else:
        lines.append("无")

    lines.extend(["", f"{copy['steps_title']}（{len(new_steps)} 条）"])
    if new_steps:
        for index, event in enumerate(new_steps, start=1):
            display = extract_display_fields(event["title"])
            lines.append(f"{index}. {display['manager']} | {display['product_name']} | {event['task_name']} | {event['fnsh_date']}")
    else:
        lines.append("无")
    return lines


def build_pdf_table_sections(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    copy = report_copy(REPORT_MODE_DAILY_SUMMARY)
    new_records = [event for event in events if event["event_type"] == "new_record"]
    new_steps = [event for event in events if event["event_type"] == "new_step"]
    return [
        {
            "title": f"{copy['records_title']}（{len(new_records)} 条）",
            "headers": ["序号", "管理人", "产品名称", "产品类型", "上报日期"],
            "rows": build_record_rows(new_records),
            "column_widths": [70, 140, 430, 150, 160],
        },
        {
            "title": f"{copy['steps_title']}（{len(new_steps)} 条）",
            "headers": ["序号", "管理人", "产品名称", "产品类型", "上报日期", "最新节点", "节点日期"],
            "rows": build_step_rows(new_steps),
            "column_widths": [70, 120, 300, 120, 120, 210, 120],
        },
    ]


def load_fitz_module() -> Any:
    try:
        return importlib.import_module("fitz")
    except ModuleNotFoundError as exc:
        raise RuntimeError("PyMuPDF is required to generate the daily summary PDF attachment.") from exc


def load_pillow_modules() -> tuple[Any, Any, Any]:
    try:
        pillow_image = importlib.import_module("PIL.Image")
        pillow_draw = importlib.import_module("PIL.ImageDraw")
        pillow_font = importlib.import_module("PIL.ImageFont")
    except ModuleNotFoundError as exc:
        raise RuntimeError("Pillow is required to generate the daily summary PDF attachment.") from exc
    return pillow_image, pillow_draw, pillow_font


def _find_existing_font_path(candidates: list[Path], label: str) -> Path:
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise RuntimeError(f"Missing required PDF font: {label}.")


def find_pdf_font_paths() -> dict[str, Path]:
    cjk_env = os.getenv("PDF_FONT_PATH", "").strip() or os.getenv("PDF_CJK_FONT_PATH", "").strip()
    latin_env = os.getenv("PDF_LATIN_FONT_PATH", "").strip()
    latin_bold_env = os.getenv("PDF_LATIN_BOLD_FONT_PATH", "").strip()
    return {
        "cjk": _find_existing_font_path(
            ([Path(cjk_env)] if cjk_env else []) + list(DEFAULT_PDF_CJK_FONT_CANDIDATES),
            "FangSong (仿宋, simfang.ttf)",
        ),
        "latin": _find_existing_font_path(
            ([Path(latin_env)] if latin_env else []) + list(DEFAULT_PDF_LATIN_FONT_CANDIDATES),
            "Times New Roman regular (times.ttf)",
        ),
        "latin_bold": _find_existing_font_path(
            ([Path(latin_bold_env)] if latin_bold_env else []) + list(DEFAULT_PDF_LATIN_BOLD_FONT_CANDIDATES),
            "Times New Roman bold (timesbd.ttf)",
        ),
    }


def find_pdf_font_path() -> Path:
    return find_pdf_font_paths()["cjk"]


def load_reportlab_modules() -> dict[str, Any]:
    try:
        colors = importlib.import_module("reportlab.lib.colors")
        pagesizes = importlib.import_module("reportlab.lib.pagesizes")
        styles = importlib.import_module("reportlab.lib.styles")
        enums = importlib.import_module("reportlab.lib.enums")
        units = importlib.import_module("reportlab.lib.units")
        platypus = importlib.import_module("reportlab.platypus")
        pdfmetrics = importlib.import_module("reportlab.pdfbase.pdfmetrics")
        ttfonts = importlib.import_module("reportlab.pdfbase.ttfonts")
    except ModuleNotFoundError as exc:
        raise RuntimeError("ReportLab is required to generate the daily summary PDF attachment.") from exc
    return {
        "colors": colors,
        "pagesizes": pagesizes,
        "styles": styles,
        "enums": enums,
        "units": units,
        "platypus": platypus,
        "pdfmetrics": pdfmetrics,
        "ttfonts": ttfonts,
    }


def register_pdf_fonts(pdfmetrics: Any, ttfonts: Any, font_paths: dict[str, Path]) -> None:
    registered = set(pdfmetrics.getRegisteredFontNames())
    registrations = [
        (PDF_FONT_FAMILY_CJK, font_paths["cjk"]),
        (PDF_FONT_FAMILY_LATIN, font_paths["latin"]),
        (PDF_FONT_FAMILY_LATIN_BOLD, font_paths["latin_bold"]),
    ]
    for font_name, font_path in registrations:
        if font_name not in registered:
            pdfmetrics.registerFont(ttfonts.TTFont(font_name, str(font_path)))


def build_pdf_rich_text(text: str, *, latin_bold: bool = False) -> str:
    if not text:
        return ""

    latin_font = PDF_FONT_FAMILY_LATIN_BOLD if latin_bold else PDF_FONT_FAMILY_LATIN
    parts: list[str] = []
    last_index = 0
    for match in ASCII_TEXT_PATTERN.finditer(text):
        if match.start() > last_index:
            parts.append(f"<font name='{PDF_FONT_FAMILY_CJK}'>{xml_escape(text[last_index:match.start()])}</font>")
        parts.append(f"<font name='{latin_font}'>{xml_escape(match.group(0))}</font>")
        last_index = match.end()
    if last_index < len(text):
        parts.append(f"<font name='{PDF_FONT_FAMILY_CJK}'>{xml_escape(text[last_index:])}</font>")
    return "".join(parts)


def wrap_pdf_text(draw: Any, text: str, font: Any, max_width: int) -> list[str]:
    if not text:
        return [""]

    lines: list[str] = []
    current = ""
    for char in text:
        trial = f"{current}{char}"
        if current and draw.textlength(trial, font=font) > max_width:
            lines.append(current)
            current = char
        else:
            current = trial
    if current:
        lines.append(current)
    return lines


def normalized_column_widths(widths: list[int], total_width: int) -> list[int]:
    width_sum = sum(widths)
    scaled = [max(60, round(width * total_width / width_sum)) for width in widths]
    diff = total_width - sum(scaled)
    scaled[-1] += diff
    return scaled


def generate_daily_summary_pdf(events: list[dict[str, Any]], local_now: datetime) -> dict[str, Any]:
    reportlab = load_reportlab_modules()
    font_paths = find_pdf_font_paths()
    register_pdf_fonts(reportlab["pdfmetrics"], reportlab["ttfonts"], font_paths)

    colors = reportlab["colors"]
    A4 = reportlab["pagesizes"].A4
    ParagraphStyle = reportlab["styles"].ParagraphStyle
    getSampleStyleSheet = reportlab["styles"].getSampleStyleSheet
    TA_CENTER = reportlab["enums"].TA_CENTER
    TA_LEFT = reportlab["enums"].TA_LEFT
    mm = reportlab["units"].mm
    SimpleDocTemplate = reportlab["platypus"].SimpleDocTemplate
    Spacer = reportlab["platypus"].Spacer
    Paragraph = reportlab["platypus"].Paragraph
    Table = reportlab["platypus"].LongTable
    TableStyle = reportlab["platypus"].TableStyle

    buffer = io.BytesIO()
    document = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=18 * mm,
        rightMargin=18 * mm,
        topMargin=18 * mm,
        bottomMargin=16 * mm,
        title=f"指数基金审批日报 {local_now:%Y-%m-%d}",
        author="csrc_index_monitor",
        subject="指数基金审批进度日报",
    )
    content_width = document.width
    new_record_count, new_step_count = count_events_by_type(events)
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "IndexMonitorTitle",
        parent=styles["Title"],
        fontName=PDF_FONT_FAMILY_CJK,
        fontSize=19,
        leading=24,
        alignment=TA_CENTER,
        textColor=colors.HexColor("#0f172a"),
        spaceAfter=8,
    )
    summary_style = ParagraphStyle(
        "IndexMonitorSummary",
        parent=styles["Normal"],
        fontName=PDF_FONT_FAMILY_CJK,
        fontSize=10.5,
        leading=15,
        alignment=TA_CENTER,
        textColor=colors.HexColor("#334155"),
        spaceAfter=2,
    )
    intro_style = ParagraphStyle(
        "IndexMonitorIntro",
        parent=styles["Normal"],
        fontName=PDF_FONT_FAMILY_CJK,
        fontSize=11.5,
        leading=18,
        alignment=TA_LEFT,
        textColor=colors.HexColor("#1f2937"),
        spaceAfter=10,
    )
    section_style = ParagraphStyle(
        "IndexMonitorSection",
        parent=styles["Heading2"],
        fontName=PDF_FONT_FAMILY_CJK,
        fontSize=13.5,
        leading=18,
        alignment=TA_LEFT,
        textColor=colors.HexColor("#0f172a"),
        spaceBefore=10,
        spaceAfter=8,
    )
    cell_style = ParagraphStyle(
        "IndexMonitorCell",
        parent=styles["BodyText"],
        fontName=PDF_FONT_FAMILY_CJK,
        fontSize=9.5,
        leading=13,
        alignment=TA_LEFT,
        textColor=colors.HexColor("#111827"),
        wordWrap="CJK",
    )
    header_style = ParagraphStyle(
        "IndexMonitorHeader",
        parent=cell_style,
        fontSize=9.8,
        leading=12,
        alignment=TA_CENTER,
        textColor=colors.white,
    )

    story: list[Any] = [
        Paragraph(build_pdf_rich_text(f"指数基金审批日报 {local_now:%Y-%m-%d}", latin_bold=True), title_style),
        Paragraph(build_pdf_rich_text(f"生成时间：{local_now:%Y-%m-%d %H:%M}"), summary_style),
        Paragraph(build_pdf_rich_text(f"今日新产品：{new_record_count} 条"), summary_style),
        Paragraph(build_pdf_rich_text(f"今日新增节点产品：{new_step_count} 条"), summary_style),
        Spacer(1, 8),
        Paragraph(build_pdf_rich_text("今日累计汇总如下："), intro_style),
    ]

    for section in build_pdf_table_sections(events):
        story.append(Paragraph(build_pdf_rich_text(section["title"], latin_bold=True), section_style))
        column_widths = normalized_column_widths(section["column_widths"], int(content_width))
        table_rows = [
            [Paragraph(build_pdf_rich_text(header, latin_bold=True), header_style) for header in section["headers"]]
        ]
        body_rows = section["rows"] or [["无"] + [""] * (len(section["headers"]) - 1)]
        for row in body_rows:
            normalized_row = list(row) + [""] * (len(section["headers"]) - len(row))
            table_rows.append([Paragraph(build_pdf_rich_text(str(value)), cell_style) for value in normalized_row[: len(section["headers"])]])

        table = Table(table_rows, colWidths=column_widths, repeatRows=1)
        table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f4f82")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("GRID", (0, 0), (-1, -1), 0.6, colors.HexColor("#1f2937")),
                    ("BOX", (0, 0), (-1, -1), 0.8, colors.HexColor("#1f2937")),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 6),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                    ("TOPPADDING", (0, 0), (-1, 0), 7),
                    ("BOTTOMPADDING", (0, 0), (-1, 0), 7),
                    ("TOPPADDING", (0, 1), (-1, -1), 6),
                    ("BOTTOMPADDING", (0, 1), (-1, -1), 6),
                    ("BACKGROUND", (0, 1), (-1, -1), colors.white),
                    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
                ]
            )
        )
        story.append(table)
        story.append(Spacer(1, 10))

    document.build(story)
    content = buffer.getvalue()
    return {
        "filename": f"指数基金审批日报{local_now:%Y-%m-%d}.pdf",
        "content": content,
        "maintype": "application",
        "subtype": "pdf",
    }


def send_email(
    *,
    config: MonitorConfig,
    subject: str,
    body: str,
    html_body: str | None = None,
    events: list[dict[str, Any]] | None = None,
    attachments: list[dict[str, Any]] | None = None,
) -> None:
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = config.alert_email_from
    message["To"] = ", ".join(config.alert_email_to)
    message.set_content(body)
    if html_body:
        message.add_alternative(html_body, subtype="html")
    for attachment in attachments or []:
        message.add_attachment(
            attachment["content"],
            maintype=attachment.get("maintype", "application"),
            subtype=attachment.get("subtype", "octet-stream"),
            filename=attachment["filename"],
        )

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


def parse_now_iso(now_iso: str | None = None) -> datetime:
    if now_iso:
        return datetime.fromisoformat(now_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
    return datetime.now(timezone.utc).replace(microsecond=0)


def daily_baseline_path_for(state_file_path: Path, local_now: datetime) -> Path:
    return state_file_path.parent / "daily" / f"{local_now:%Y-%m-%d}.json"


def ensure_daily_baseline(path: Path, snapshot: dict[str, Any]) -> bool:
    if path.exists():
        return False
    save_state(path, snapshot)
    return True


def find_git_repo_root(start_path: Path) -> Path | None:
    for candidate in [start_path, *start_path.parents]:
        if (candidate / ".git").exists():
            return candidate
    return None


def run_git_command(
    command: list[str],
    *,
    capture_output: bool = True,
    text: bool = True,
    encoding: str = "utf-8",
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        capture_output=capture_output,
        text=text,
        encoding=encoding,
        check=check,
    )


def load_historical_daily_baseline(
    state_file_path: Path,
    local_now: datetime,
    *,
    git_runner: Callable[..., subprocess.CompletedProcess[str]] = run_git_command,
) -> dict[str, Any] | None:
    repo_root = find_git_repo_root(state_file_path.parent)
    if repo_root is None:
        return None

    try:
        state_path_in_repo = state_file_path.relative_to(repo_root).as_posix()
    except ValueError:
        return None

    day_start_local = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end_local = day_start_local + timedelta(days=1)
    since_utc = day_start_local.astimezone(timezone.utc).isoformat()
    until_utc = day_end_local.astimezone(timezone.utc).isoformat()

    try:
        log_result = git_runner(
            [
                "git",
                "-C",
                str(repo_root),
                "log",
                "--reverse",
                f"--since={since_utc}",
                f"--until={until_utc}",
                "--format=%H",
                "--",
                state_path_in_repo,
            ]
        )
    except Exception:
        return None

    commits = [line.strip() for line in log_result.stdout.splitlines() if line.strip()]
    if not commits:
        return None

    commit_hash = commits[0]
    try:
        show_result = git_runner(
            [
                "git",
                "-C",
                str(repo_root),
                "show",
                f"{commit_hash}:{state_path_in_repo}",
            ]
        )
    except Exception:
        return None

    try:
        return json.loads(show_result.stdout)
    except json.JSONDecodeError:
        return None


def snapshot_timestamp(snapshot: dict[str, Any] | None) -> datetime | None:
    if not snapshot:
        return None
    raw_value = snapshot.get("last_success_at")
    if not raw_value:
        return None
    try:
        return parse_now_iso(str(raw_value))
    except ValueError:
        return None


def load_daily_baseline_snapshot(
    daily_baseline_path: Path,
    state_file_path: Path,
    local_now: datetime,
    *,
    git_runner: Callable[..., subprocess.CompletedProcess[str]] = run_git_command,
) -> tuple[dict[str, Any] | None, str]:
    file_snapshot = load_state(daily_baseline_path)
    historical_snapshot = load_historical_daily_baseline(state_file_path, local_now, git_runner=git_runner)

    if file_snapshot and historical_snapshot:
        file_timestamp = snapshot_timestamp(file_snapshot)
        historical_timestamp = snapshot_timestamp(historical_snapshot)
        if historical_timestamp and (file_timestamp is None or historical_timestamp < file_timestamp):
            return historical_snapshot, "git_history"
        return file_snapshot, "daily_file"

    if file_snapshot:
        return file_snapshot, "daily_file"
    if historical_snapshot:
        return historical_snapshot, "git_history"
    return None, "missing"


def write_github_step_summary(result: dict[str, Any], path: Path | None = None) -> None:
    raw_summary_path = str(path) if path is not None else os.getenv("GITHUB_STEP_SUMMARY", "")
    if not raw_summary_path:
        return

    summary_path = Path(raw_summary_path)
    diagnostics = result.get("email_diagnostics") or {}
    email_delivery = result.get("email_delivery") or {}
    warnings = diagnostics.get("warnings") or []
    lines = ["## Email delivery diagnostics", ""]

    if result.get("status") == "error":
        lines.extend(
            [
                f"- Status: error ({result.get('error_type', 'UnknownError')})",
                f"- Message: {result.get('message', '')}",
            ]
        )
    else:
        lines.extend(
            [
                f"- Report mode: {result.get('report_mode', '')}",
                f"- Baseline created: {result.get('baseline_created')}",
                f"- Daily baseline created: {result.get('daily_baseline_created')}",
                f"- Daily baseline path: {result.get('daily_baseline_path', '')}",
                f"- Daily baseline source: {result.get('daily_baseline_source', '')}",
                f"- Events detected: {result.get('event_count', 0)}",
                f"- New records: {result.get('new_record_count', 0)}",
                f"- New steps: {result.get('new_step_count', 0)}",
            ]
        )
        if result.get("email_subject"):
            lines.append(f"- Email subject: {result['email_subject']}")
        if result.get("skipped_reason"):
            lines.append(f"- Skipped reason: {result['skipped_reason']}")

    if diagnostics:
        lines.extend(
            [
                f"- SMTP host: {diagnostics.get('smtp_host', '')}",
                f"- SMTP port: {diagnostics.get('smtp_port', '')}",
                f"- Transport: {diagnostics.get('transport', '')}",
                f"- SMTP username: {diagnostics.get('smtp_username_masked', '')}",
                f"- Alert from: {diagnostics.get('alert_email_from_masked', '')}",
                f"- Alert recipients ({diagnostics.get('recipient_count', 0)}): {', '.join(diagnostics.get('alert_email_to_masked') or [])}",
                f"- Sender matches username: {diagnostics.get('sender_matches_username')}",
                f"- Sender domain matches username domain: {diagnostics.get('sender_domain_matches_username_domain')}",
            ]
        )

    if email_delivery:
        lines.extend(
            [
                f"- Delivery attempted: {email_delivery.get('attempted')}",
                f"- Delivery status: {email_delivery.get('status', '')}",
            ]
        )

    if warnings:
        lines.extend(["", "### Warnings"])
        lines.extend(f"- {warning}" for warning in warnings)

    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_monitor(
    *,
    config: MonitorConfig,
    fetch_records: Callable[[str], list[dict[str, Any]]] | None = None,
    send_email_func: Callable[..., None] | None = None,
    now_iso: str | None = None,
    report_mode: str = DEFAULT_REPORT_MODE,
) -> dict[str, Any]:
    fetch_records = fetch_records or (lambda keyword: fetch_all_records(keyword))
    send_email_func = send_email_func or send_email
    now_utc = parse_now_iso(now_iso)
    now_iso = now_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    local_now = now_utc.astimezone(SHANGHAI_TZ)
    daily_baseline_path = daily_baseline_path_for(config.state_file_path, local_now)

    if report_mode == REPORT_MODE_DAILY_SUMMARY:
        daily_baseline_snapshot, daily_baseline_source = load_daily_baseline_snapshot(
            daily_baseline_path,
            config.state_file_path,
            local_now,
        )
        if daily_baseline_snapshot is None:
            return attach_monitor_diagnostics(
                {
                    "baseline_created": False,
                    "event_count": 0,
                    "events": [],
                    "state_changed": False,
                    "state_file_path": str(config.state_file_path),
                    "daily_baseline_source": daily_baseline_source,
                },
                config=config,
                events=[],
                email_attempted=False,
                email_status="skipped_missing_baseline",
                report_mode=report_mode,
                daily_baseline_path=daily_baseline_path,
                skipped_reason="missing_daily_baseline",
            )

        latest_snapshot = load_state(config.state_file_path)
        if latest_snapshot is None:
            return attach_monitor_diagnostics(
                {
                    "baseline_created": False,
                    "event_count": 0,
                    "events": [],
                    "state_changed": False,
                    "state_file_path": str(config.state_file_path),
                    "daily_baseline_source": daily_baseline_source,
                },
                config=config,
                events=[],
                email_attempted=False,
                email_status="skipped_missing_latest_state",
                report_mode=report_mode,
                daily_baseline_path=daily_baseline_path,
                skipped_reason="missing_latest_state",
            )

        events = diff_snapshots(daily_baseline_snapshot, latest_snapshot)
        if not events:
            return attach_monitor_diagnostics(
                {
                    "baseline_created": False,
                    "event_count": 0,
                    "events": [],
                    "state_changed": False,
                    "state_file_path": str(config.state_file_path),
                    "daily_baseline_source": daily_baseline_source,
                },
                config=config,
                events=[],
                email_attempted=False,
                email_status="skipped_no_changes",
                report_mode=report_mode,
                daily_baseline_path=daily_baseline_path,
                skipped_reason="no_daily_changes",
            )

        subject, body = format_email_summary(events, report_mode, local_now)
        html_body = format_html_summary(events, report_mode)
        attachments = [generate_daily_summary_pdf(events, local_now)]
        send_email_func(config=config, subject=subject, body=body, html_body=html_body, events=events, attachments=attachments)
        return attach_monitor_diagnostics(
            {
                "baseline_created": False,
                "event_count": len(events),
                "events": events,
                "state_changed": False,
                "state_file_path": str(config.state_file_path),
                "email_subject": subject,
                "daily_baseline_source": daily_baseline_source,
            },
            config=config,
            events=events,
            email_attempted=True,
            email_status="sent",
            report_mode=report_mode,
            daily_baseline_path=daily_baseline_path,
        )

    records = fetch_records(config.keyword)
    current_snapshot = build_snapshot(records, now_iso)
    baseline_snapshot = build_snapshot(records, now_iso)
    existing_daily_baseline, existing_daily_baseline_source = load_daily_baseline_snapshot(
        daily_baseline_path,
        config.state_file_path,
        local_now,
    )
    daily_baseline_created = False
    if existing_daily_baseline is None:
        save_state(daily_baseline_path, baseline_snapshot)
        daily_baseline_created = True
    elif existing_daily_baseline_source == "git_history" and not daily_baseline_path.exists():
        save_state(daily_baseline_path, existing_daily_baseline)
        daily_baseline_created = True
    previous_snapshot = load_state(config.state_file_path)
    if previous_snapshot is None:
        save_state(config.state_file_path, current_snapshot)
        return attach_monitor_diagnostics(
            {
                "baseline_created": True,
                "event_count": 0,
                "events": [],
                "state_changed": True,
                "state_file_path": str(config.state_file_path),
            },
            config=config,
            events=[],
            email_attempted=False,
            email_status="not_attempted",
            report_mode=report_mode,
            daily_baseline_path=daily_baseline_path,
            daily_baseline_created=daily_baseline_created,
        )

    events = diff_snapshots(previous_snapshot, current_snapshot)
    if not events:
        save_state(config.state_file_path, current_snapshot)
        return attach_monitor_diagnostics(
            {
                "baseline_created": False,
                "event_count": 0,
                "events": [],
                "state_changed": True,
                "state_file_path": str(config.state_file_path),
            },
            config=config,
            events=[],
            email_attempted=False,
            email_status="not_attempted",
            report_mode=report_mode,
            daily_baseline_path=daily_baseline_path,
            daily_baseline_created=daily_baseline_created,
        )

    subject, body = format_email_summary(events, report_mode, local_now)
    html_body = format_html_summary(events, report_mode)
    send_email_func(config=config, subject=subject, body=body, html_body=html_body, events=events, attachments=None)
    notified_snapshot = build_snapshot(records, now_iso, notified_event_ids=[event["event_id"] for event in events])
    save_state(config.state_file_path, notified_snapshot)
    return attach_monitor_diagnostics(
        {
            "baseline_created": False,
            "event_count": len(events),
            "events": events,
            "state_changed": True,
            "state_file_path": str(config.state_file_path),
            "email_subject": subject,
        },
        config=config,
        events=events,
        email_attempted=True,
        email_status="sent",
        report_mode=report_mode,
        daily_baseline_path=daily_baseline_path,
        daily_baseline_created=daily_baseline_created,
    )


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
    config: MonitorConfig | None = None
    try:
        config = load_config_from_env()
        report_mode = os.getenv("REPORT_MODE", DEFAULT_REPORT_MODE)
        result = run_monitor(config=config, report_mode=report_mode)
        write_github_step_summary(result)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        error_payload = {
            "status": "error",
            "error_type": exc.__class__.__name__,
            "message": str(exc),
        }
        if config is not None:
            error_payload["email_diagnostics"] = build_email_diagnostics(config)
        write_github_step_summary(error_payload)
        print(json.dumps(error_payload, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

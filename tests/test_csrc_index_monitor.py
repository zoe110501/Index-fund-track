import io
import json
import os
import ssl
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock
from urllib.error import URLError

import csrc_index_monitor as monitor


KEYWORD = "\u6307\u6570"
TASK_RECEIVE = "\u63a5\u6536\u6750\u6599"
TASK_ACCEPT = "\u53d7\u7406\u901a\u77e5"
ETF_PHRASE = "\u4ea4\u6613\u578b\u5f00\u653e\u5f0f\u6307\u6570\u8bc1\u5238\u6295\u8d44\u57fa\u91d1"


def build_title(manager: str, product_name: str) -> str:
    return f"\u5173\u4e8e{manager}\u7684\u300a\u516c\u5f00\u52df\u96c6\u57fa\u91d1\u52df\u96c6\u7533\u8bf7\u6ce8\u518c-{product_name}\u300b"


def build_step(task_name: str, fnsh_date: str, file_code: str = "-") -> dict[str, str]:
    return {
        "task_name": task_name,
        "fnsh_date": fnsh_date,
        "step_id": f"{task_name}|{fnsh_date}|{file_code}",
    }


def build_record(record_id: str, title: str, app_date: str, steps: list[dict[str, str]]) -> dict[str, object]:
    return {
        "record_id": record_id,
        "title": title,
        "app_date": app_date,
        "steps": steps,
    }


class SnapshotDiffTests(unittest.TestCase):
    def test_diff_snapshots_detects_new_record_and_new_step(self):
        old_snapshot = {
            "records": {
                "alpha": {
                    "title": "alpha",
                    "app_date": "2026-03-16",
                    "step_ids": [f"{TASK_RECEIVE}|2026-03-16|-"],
                }
            },
            "last_notified_event_ids": [],
        }
        new_snapshot = {
            "records": {
                "alpha": {
                    "title": "alpha",
                    "app_date": "2026-03-16",
                    "step_ids": [f"{TASK_RECEIVE}|2026-03-16|-", f"{TASK_ACCEPT}|2026-03-17|file-a"],
                },
                "beta": {
                    "title": "beta",
                    "app_date": "2026-03-17",
                    "step_ids": [f"{TASK_RECEIVE}|2026-03-17|-"],
                },
            }
        }

        events = monitor.diff_snapshots(old_snapshot, new_snapshot)

        self.assertEqual(len(events), 2)
        self.assertEqual(events[0]["event_type"], "new_record")
        self.assertEqual(events[0]["record_id"], "beta")
        self.assertEqual(events[1]["event_type"], "new_step")
        self.assertEqual(events[1]["step_id"], f"{TASK_ACCEPT}|2026-03-17|file-a")

    def test_diff_snapshots_skips_already_notified_event_ids(self):
        old_snapshot = {
            "records": {
                "alpha": {
                    "title": "alpha",
                    "app_date": "2026-03-16",
                    "step_ids": [f"{TASK_RECEIVE}|2026-03-16|-"],
                }
            },
            "last_notified_event_ids": [f"new-step|alpha|{TASK_ACCEPT}|2026-03-17|file-a"],
        }
        new_snapshot = {
            "records": {
                "alpha": {
                    "title": "alpha",
                    "app_date": "2026-03-16",
                    "step_ids": [f"{TASK_RECEIVE}|2026-03-16|-", f"{TASK_ACCEPT}|2026-03-17|file-a"],
                }
            }
        }

        self.assertEqual(monitor.diff_snapshots(old_snapshot, new_snapshot), [])


class FetchTests(unittest.TestCase):
    def test_fetch_all_records_reads_all_pages_and_filters_titles(self):
        page_one = {
            "code": "0000",
            "data": {
                "records": [
                    {
                        "alAppLtCde": "alpha",
                        "showCntnt": build_title("\u7532\u516c\u53f8", "\u673a\u5668\u4eba" + KEYWORD + "\u57fa\u91d1"),
                        "appDate": "2026-03-16",
                        "aprvSchdPubFlowViewResultList": [{"taskName": TASK_RECEIVE, "fnshDate": "2026-03-16", "alFileCde": None}],
                    },
                    {
                        "alAppLtCde": "ignore",
                        "showCntnt": build_title("\u4e59\u516c\u53f8", "\u666e\u901a\u80a1\u7968\u57fa\u91d1"),
                        "appDate": "2026-03-16",
                        "aprvSchdPubFlowViewResultList": [],
                    },
                ],
                "total": 3,
                "size": 2,
                "current": 1,
            },
        }
        page_two = {
            "code": "0000",
            "data": {
                "records": [
                    {
                        "alAppLtCde": "beta",
                        "showCntnt": build_title("\u4e19\u516c\u53f8", "\u4eba\u5de5\u667a\u80fd" + KEYWORD + "\u57fa\u91d1"),
                        "appDate": "2026-03-17",
                        "aprvSchdPubFlowViewResultList": [{"taskName": TASK_ACCEPT, "fnshDate": "2026-03-17", "alFileCde": "file-a"}],
                    }
                ],
                "total": 3,
                "size": 2,
                "current": 2,
            },
        }
        pages = {1: page_one, 2: page_two}

        records = monitor.fetch_all_records(KEYWORD, page_size=2, fetch_page=lambda page_num, page_size, keyword: pages[page_num])

        self.assertEqual([record["record_id"] for record in records], ["alpha", "beta"])
        self.assertEqual(records[1]["steps"][0]["task_name"], TASK_ACCEPT)

    def test_fetch_page_from_api_retries_without_ssl_verification_after_cert_failure(self):
        payload = {"code": "0000", "data": {"records": [], "total": 0, "size": 1, "current": 1}}
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = json.dumps(payload).encode("utf-8")
        response.__exit__.return_value = False

        with mock.patch(
            "csrc_index_monitor.urlopen",
            side_effect=[
                URLError(ssl.SSLCertVerificationError("certificate verify failed")),
                response,
            ],
        ) as mocked_urlopen:
            result = monitor.fetch_page_from_api(1, 1, KEYWORD)

        self.assertEqual(result, payload)
        self.assertEqual(mocked_urlopen.call_count, 2)
        self.assertNotIn("context", mocked_urlopen.call_args_list[0].kwargs)
        self.assertIn("context", mocked_urlopen.call_args_list[1].kwargs)

    def test_fetch_page_from_api_does_not_retry_non_ssl_url_errors(self):
        with mock.patch(
            "csrc_index_monitor.urlopen",
            side_effect=URLError("timed out"),
        ):
            with self.assertRaises(URLError):
                monitor.fetch_page_from_api(1, 1, KEYWORD)


class ConfigTests(unittest.TestCase):
    def test_build_email_diagnostics_flags_sender_mismatch(self):
        diagnostics = monitor.build_email_diagnostics(
            monitor.MonitorConfig(
                keyword=KEYWORD,
                state_file_path=Path("state.json"),
                smtp_host="smtp.example.com",
                smtp_port=465,
                smtp_username="mailer@example.com",
                smtp_password="secret",
                alert_email_from="alerts@other.com",
                alert_email_to=["one@example.com", "two@example.com"],
            )
        )

        self.assertEqual(diagnostics["smtp_host"], "smtp.example.com")
        self.assertEqual(diagnostics["recipient_count"], 2)
        self.assertFalse(diagnostics["sender_matches_username"])
        self.assertFalse(diagnostics["sender_domain_matches_username_domain"])
        self.assertTrue(diagnostics["warnings"])

    def test_load_config_from_env_parses_multiple_recipients(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = Path(tmpdir) / "state.json"
            env = {
                "CSRC_QUERY_KEYWORD": KEYWORD,
                "SMTP_HOST": "smtp.example.com",
                "SMTP_PORT": "465",
                "SMTP_USERNAME": "bot@example.com",
                "SMTP_PASSWORD": "secret",
                "ALERT_EMAIL_FROM": "bot@example.com",
                "ALERT_EMAIL_TO": "one@example.com, two@example.com",
                "STATE_FILE_PATH": str(state_file),
            }

            with mock.patch.dict(os.environ, env, clear=False):
                config = monitor.load_config_from_env()

            self.assertEqual(config.alert_email_to, ["one@example.com", "two@example.com"])
            self.assertEqual(config.state_file_path, state_file)


class DisplayTests(unittest.TestCase):
    def test_extract_display_fields_and_type(self):
        product_name = "\u534e\u590f\u4eba\u5de5\u667a\u80fd" + ETF_PHRASE
        display = monitor.extract_display_fields(build_title("\u534e\u590f\u57fa\u91d1\u7ba1\u7406\u6709\u9650\u516c\u53f8", product_name))

        self.assertEqual(display["manager"], "\u534e\u590f")
        self.assertEqual(display["product_type"], "ETF")
        self.assertIn("ETF", display["product_name"])

    def test_extract_display_fields_classifies_etf_linked_fund_variants(self):
        product_name = (
            "\u534e\u590f\u4e2d\u8bc1A500"
            "\u4ea4\u6613\u578b\u5f00\u653e\u5f0f\u6307\u6570\u8bc1\u5238\u6295\u8d44\u57fa\u91d1"
            "\u53d1\u8d77\u5f0f\u8054\u63a5\u57fa\u91d1"
        )

        display = monitor.extract_display_fields(
            build_title("\u534e\u590f\u57fa\u91d1\u7ba1\u7406\u6709\u9650\u516c\u53f8", product_name)
        )

        self.assertEqual(display["product_type"], "ETF\u8054\u63a5")

    def test_format_product_name_for_display_replaces_etf_phrase(self):
        formatted = monitor.format_product_name_for_display("\u534e\u590f\u4eba\u5de5\u667a\u80fd" + ETF_PHRASE)

        self.assertEqual(formatted, "\u534e\u590f\u4eba\u5de5\u667a\u80fdETF")


class IncrementalModeTests(unittest.TestCase):
    def test_incremental_bootstrap_creates_daily_baseline_without_sending_email(self):
        records = [
            build_record(
                "alpha",
                build_title("\u534e\u590f\u57fa\u91d1\u7ba1\u7406\u6709\u9650\u516c\u53f8", "\u534e\u590f\u4eba\u5de5\u667a\u80fd" + ETF_PHRASE),
                "2026-03-17",
                [build_step(TASK_RECEIVE, "2026-03-17")],
            )
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = Path(tmpdir) / "state.json"
            sent_messages = []
            result = monitor.run_monitor(
                config=monitor.MonitorConfig(
                    keyword=KEYWORD,
                    state_file_path=state_file,
                    smtp_host="smtp.example.com",
                    smtp_port=465,
                    smtp_username="bot@example.com",
                    smtp_password="secret",
                    alert_email_from="bot@example.com",
                    alert_email_to=["me@example.com"],
                ),
                fetch_records=lambda keyword: records,
                send_email_func=lambda **kwargs: sent_messages.append(kwargs),
                now_iso="2026-03-17T00:05:00Z",
            )

            self.assertEqual(sent_messages, [])
            self.assertTrue(result["baseline_created"])
            self.assertTrue(result["daily_baseline_created"])
            self.assertTrue(Path(result["daily_baseline_path"]).exists())

    def test_incremental_mode_uses_beijing_hour_subject_and_no_attachment(self):
        first_records = [
            build_record(
                "alpha",
                build_title("\u534e\u590f\u57fa\u91d1\u7ba1\u7406\u6709\u9650\u516c\u53f8", "\u534e\u590f\u4eba\u5de5\u667a\u80fd" + ETF_PHRASE),
                "2026-03-17",
                [build_step(TASK_RECEIVE, "2026-03-17")],
            )
        ]
        second_records = [
            build_record(
                "alpha",
                build_title("\u534e\u590f\u57fa\u91d1\u7ba1\u7406\u6709\u9650\u516c\u53f8", "\u534e\u590f\u4eba\u5de5\u667a\u80fd" + ETF_PHRASE),
                "2026-03-17",
                [build_step(TASK_RECEIVE, "2026-03-17"), build_step(TASK_ACCEPT, "2026-03-17", "file-a")],
            )
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = Path(tmpdir) / "state.json"
            config = monitor.MonitorConfig(
                keyword=KEYWORD,
                state_file_path=state_file,
                smtp_host="smtp.example.com",
                smtp_port=465,
                smtp_username="bot@example.com",
                smtp_password="secret",
                alert_email_from="bot@example.com",
                alert_email_to=["me@example.com"],
            )
            email_calls = []

            monitor.run_monitor(
                config=config,
                fetch_records=lambda keyword: first_records,
                send_email_func=lambda **kwargs: None,
                now_iso="2026-03-17T09:00:00Z",
            )
            with mock.patch("csrc_index_monitor.importlib.import_module", side_effect=AssertionError("fitz should not load in incremental mode")):
                result = monitor.run_monitor(
                    config=config,
                    fetch_records=lambda keyword: second_records,
                    send_email_func=lambda **kwargs: email_calls.append(kwargs),
                    now_iso="2026-03-17T10:00:00Z",
                )

            self.assertEqual(result["report_mode"], "incremental")
            self.assertEqual(result["email_subject"], "\u6307\u6570\u57fa\u91d1\u5ba1\u6279\u8fdb\u5ea6\uff0818\uff1a00\uff09")
            self.assertEqual(email_calls[0]["subject"], result["email_subject"])
            self.assertIsNone(email_calls[0].get("attachments"))
            self.assertIn("ETF", email_calls[0]["html_body"])

    def test_incremental_mode_does_not_update_latest_state_when_email_fails(self):
        first_records = [
            build_record("alpha", "alpha", "2026-03-16", [build_step(TASK_RECEIVE, "2026-03-16")])
        ]
        second_records = [
            build_record("alpha", "alpha", "2026-03-16", [build_step(TASK_RECEIVE, "2026-03-16"), build_step(TASK_ACCEPT, "2026-03-17", "file-a")])
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = Path(tmpdir) / "state.json"
            config = monitor.MonitorConfig(
                keyword=KEYWORD,
                state_file_path=state_file,
                smtp_host="smtp.example.com",
                smtp_port=465,
                smtp_username="bot@example.com",
                smtp_password="secret",
                alert_email_from="bot@example.com",
                alert_email_to=["me@example.com"],
            )

            monitor.run_monitor(
                config=config,
                fetch_records=lambda keyword: first_records,
                send_email_func=lambda **kwargs: None,
                now_iso="2026-03-16T10:00:00Z",
            )
            previous_state = state_file.read_text(encoding="utf-8")

            with self.assertRaises(RuntimeError):
                monitor.run_monitor(
                    config=config,
                    fetch_records=lambda keyword: second_records,
                    send_email_func=lambda **kwargs: (_ for _ in ()).throw(RuntimeError("smtp failed")),
                    now_iso="2026-03-17T10:00:00Z",
                )

            self.assertEqual(state_file.read_text(encoding="utf-8"), previous_state)


class DailySummaryTests(unittest.TestCase):
    def build_daily_summary_events(self, record_count: int = 1, step_count: int = 1) -> list[dict[str, str]]:
        events: list[dict[str, str]] = []
        for index in range(record_count):
            events.append(
                {
                    "event_type": "new_record",
                    "title": build_title(
                        "中银基金管理有限公司",
                        f"中银有色金属{index + 1}" + ETF_PHRASE,
                    ),
                    "app_date": "2026-03-17",
                    "record_id": f"record-{index}",
                    "event_id": f"new-record|record-{index}",
                }
            )
        for index in range(step_count):
            events.append(
                {
                    "event_type": "new_step",
                    "title": build_title(
                        "华夏基金管理有限公司",
                        f"华夏人工智能{index + 1}" + ETF_PHRASE,
                    ),
                    "app_date": "2026-03-17",
                    "record_id": f"step-{index}",
                    "event_id": f"new-step|step-{index}|x",
                    "task_name": TASK_ACCEPT,
                    "fnsh_date": "2026-03-17",
                    "al_file_cde": "x",
                }
            )
        return events

    def test_build_pdf_table_sections_matches_email_tables(self):
        events = [
            {
                "event_type": "new_record",
                "title": build_title("\u4e2d\u94f6\u57fa\u91d1\u7ba1\u7406\u6709\u9650\u516c\u53f8", "\u4e2d\u94f6\u6709\u8272\u91d1\u5c5e" + ETF_PHRASE),
                "app_date": "2026-03-17",
                "record_id": "beta",
                "event_id": "new-record|beta",
            },
            {
                "event_type": "new_step",
                "title": build_title("\u534e\u590f\u57fa\u91d1\u7ba1\u7406\u6709\u9650\u516c\u53f8", "\u534e\u590f\u4eba\u5de5\u667a\u80fd" + ETF_PHRASE),
                "app_date": "2026-03-17",
                "record_id": "alpha",
                "event_id": "new-step|alpha|x",
                "task_name": TASK_ACCEPT,
                "fnsh_date": "2026-03-17",
                "al_file_cde": "x",
            },
        ]

        sections = monitor.build_pdf_table_sections(events)

        self.assertEqual(sections[0]["headers"], ["\u5e8f\u53f7", "\u7ba1\u7406\u4eba", "\u4ea7\u54c1\u540d\u79f0", "\u4ea7\u54c1\u7c7b\u578b", "\u4e0a\u62a5\u65e5\u671f"])
        self.assertEqual(sections[0]["rows"][0][1:], ["\u4e2d\u94f6", "\u4e2d\u94f6\u6709\u8272\u91d1\u5c5eETF", "ETF", "2026-03-17"])
        self.assertEqual(sections[1]["headers"], ["\u5e8f\u53f7", "\u7ba1\u7406\u4eba", "\u4ea7\u54c1\u540d\u79f0", "\u4ea7\u54c1\u7c7b\u578b", "\u4e0a\u62a5\u65e5\u671f", "\u6700\u65b0\u8282\u70b9", "\u8282\u70b9\u65e5\u671f"])
        self.assertEqual(sections[1]["rows"][0][1:], ["\u534e\u590f", "\u534e\u590f\u4eba\u5de5\u667a\u80fdETF", "ETF", "2026-03-17", TASK_ACCEPT, "2026-03-17"])

    def test_generate_daily_summary_pdf_reports_missing_cjk_font(self):
        local_now = datetime(2026, 3, 17, 19, 30, tzinfo=monitor.SHANGHAI_TZ)

        with mock.patch(
            "csrc_index_monitor.find_pdf_font_candidates",
            side_effect=RuntimeError("Missing required PDF font: FangSong (仿宋, simfang.ttf)."),
        ):
            with self.assertRaisesRegex(RuntimeError, "FangSong"):
                monitor.generate_daily_summary_pdf([], local_now)

    def test_generate_daily_summary_pdf_reports_missing_times_new_roman_font(self):
        local_now = datetime(2026, 3, 17, 19, 30, tzinfo=monitor.SHANGHAI_TZ)

        with mock.patch(
            "csrc_index_monitor.find_pdf_font_candidates",
            side_effect=RuntimeError("Missing required PDF font: Times New Roman regular (times.ttf)."),
        ):
            with self.assertRaisesRegex(RuntimeError, "Times New Roman"):
                monitor.generate_daily_summary_pdf([], local_now)

    def test_find_pdf_font_paths_uses_linux_serif_fallbacks_when_times_is_missing(self):
        cjk_font = Path("C:/Windows/Fonts/simfang.ttf")
        liberation_regular = Path("/usr/share/fonts/truetype/liberation2/LiberationSerif-Regular.ttf")
        liberation_bold = Path("/usr/share/fonts/truetype/liberation2/LiberationSerif-Bold.ttf")

        def fake_exists(path_obj: Path) -> bool:
            return path_obj in {cjk_font, liberation_regular, liberation_bold}

        with mock.patch("pathlib.Path.exists", autospec=True, side_effect=fake_exists):
            font_paths = monitor.find_pdf_font_paths()

        self.assertEqual(font_paths["latin"], liberation_regular)
        self.assertEqual(font_paths["latin_bold"], liberation_bold)

    def test_register_pdf_fonts_falls_back_when_first_existing_font_cannot_be_loaded(self):
        cjk_bad = Path("/usr/share/fonts/opentype/noto/NotoSerifCJK-Regular.ttc")
        cjk_good = Path("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc")
        latin_regular = Path("/usr/share/fonts/truetype/liberation2/LiberationSerif-Regular.ttf")
        latin_bold = Path("/usr/share/fonts/truetype/liberation2/LiberationSerif-Bold.ttf")
        font_candidates = {
            "cjk": [cjk_bad, cjk_good],
            "latin": [latin_regular],
            "latin_bold": [latin_bold],
        }
        registered_fonts: list[tuple[str, str]] = []

        class FakePdfMetrics:
            def getRegisteredFontNames(self):
                return []

            def registerFont(self, font_obj):
                registered_fonts.append((font_obj.fontName, font_obj.path))

        class FakeTtFonts:
            class TTFError(Exception):
                pass

            class TTFont:
                def __init__(self, font_name, path):
                    if path == str(cjk_bad):
                        raise FakeTtFonts.TTFError("unsupported outlines")
                    self.fontName = font_name
                    self.path = path

        class FakeCidFonts:
            class UnicodeCIDFont:
                def __init__(self, font_name):
                    self.fontName = font_name
                    self.path = font_name

        with mock.patch("pathlib.Path.exists", autospec=True, return_value=True):
            resolved = monitor.register_pdf_fonts(FakePdfMetrics(), FakeTtFonts(), FakeCidFonts(), font_candidates)

        self.assertEqual(resolved["cjk"], monitor.PDF_FONT_FAMILY_CJK)
        self.assertEqual(resolved["latin"], monitor.PDF_FONT_FAMILY_LATIN)
        self.assertEqual(resolved["latin_bold"], monitor.PDF_FONT_FAMILY_LATIN_BOLD)
        self.assertEqual(
            registered_fonts,
            [
                (monitor.PDF_FONT_FAMILY_CJK, str(cjk_good)),
                (monitor.PDF_FONT_FAMILY_LATIN, str(latin_regular)),
                (monitor.PDF_FONT_FAMILY_LATIN_BOLD, str(latin_bold)),
            ],
        )

    def test_register_pdf_fonts_falls_back_to_builtin_cjk_font_when_all_files_fail(self):
        cjk_bad = Path("/usr/share/fonts/opentype/noto/NotoSerifCJK-Regular.ttc")
        cjk_bad_two = Path("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc")
        latin_regular = Path("/usr/share/fonts/truetype/liberation2/LiberationSerif-Regular.ttf")
        latin_bold = Path("/usr/share/fonts/truetype/liberation2/LiberationSerif-Bold.ttf")
        font_candidates = {
            "cjk": [cjk_bad, cjk_bad_two],
            "latin": [latin_regular],
            "latin_bold": [latin_bold],
        }
        registered_fonts: list[tuple[str, str]] = []

        class FakePdfMetrics:
            def getRegisteredFontNames(self):
                return []

            def registerFont(self, font_obj):
                registered_fonts.append((font_obj.fontName, font_obj.path))

        class FakeTtFonts:
            class TTFError(Exception):
                pass

            class TTFont:
                def __init__(self, font_name, path):
                    if path in {str(cjk_bad), str(cjk_bad_two)}:
                        raise FakeTtFonts.TTFError("unsupported outlines")
                    self.fontName = font_name
                    self.path = path

        class FakeCidFonts:
            class UnicodeCIDFont:
                def __init__(self, font_name):
                    self.fontName = font_name
                    self.path = font_name

        with mock.patch("pathlib.Path.exists", autospec=True, return_value=True):
            resolved = monitor.register_pdf_fonts(FakePdfMetrics(), FakeTtFonts(), FakeCidFonts(), font_candidates)

        self.assertEqual(resolved["cjk"], "STSong-Light")
        self.assertEqual(resolved["latin"], monitor.PDF_FONT_FAMILY_LATIN)
        self.assertEqual(resolved["latin_bold"], monitor.PDF_FONT_FAMILY_LATIN_BOLD)
        self.assertEqual(
            registered_fonts,
            [
                ("STSong-Light", "STSong-Light"),
                (monitor.PDF_FONT_FAMILY_LATIN, str(latin_regular)),
                (monitor.PDF_FONT_FAMILY_LATIN_BOLD, str(latin_bold)),
            ],
        )

    def test_generate_daily_summary_pdf_contains_extractable_report_text(self):
        local_now = datetime(2026, 3, 17, 19, 30, tzinfo=monitor.SHANGHAI_TZ)
        attachment = monitor.generate_daily_summary_pdf(self.build_daily_summary_events(), local_now)

        pdf = monitor.load_fitz_module().open(stream=attachment["content"], filetype="pdf")
        extracted = "\n".join(page.get_text() for page in pdf)

        self.assertIn("指数基金审批日报 2026-03-17", extracted)
        self.assertIn("今日累计汇总如下：", extracted)
        self.assertIn("今日新产品（1 条）", extracted)
        self.assertIn("今日新增节点产品（1 条）", extracted)
        self.assertIn("产品类型", extracted)

    def test_generate_daily_summary_pdf_repeats_headers_on_paginated_tables(self):
        local_now = datetime(2026, 3, 17, 19, 30, tzinfo=monitor.SHANGHAI_TZ)
        attachment = monitor.generate_daily_summary_pdf(self.build_daily_summary_events(record_count=0, step_count=60), local_now)

        pdf = monitor.load_fitz_module().open(stream=attachment["content"], filetype="pdf")

        self.assertGreaterEqual(pdf.page_count, 2)
        first_page_text = pdf[0].get_text()
        second_page_text = pdf[1].get_text()

        self.assertIn("序号", first_page_text)
        self.assertIn("最新节点", first_page_text)
        self.assertIn("序号", second_page_text)
        self.assertIn("最新节点", second_page_text)

    def test_daily_summary_sends_pdf_attachment(self):
        baseline_records = [
            build_record(
                "alpha",
                build_title("\u534e\u590f\u57fa\u91d1\u7ba1\u7406\u6709\u9650\u516c\u53f8", "\u534e\u590f\u4eba\u5de5\u667a\u80fd" + ETF_PHRASE),
                "2026-03-17",
                [build_step(TASK_RECEIVE, "2026-03-17")],
            )
        ]
        current_records = [
            build_record(
                "alpha",
                build_title("\u534e\u590f\u57fa\u91d1\u7ba1\u7406\u6709\u9650\u516c\u53f8", "\u534e\u590f\u4eba\u5de5\u667a\u80fd" + ETF_PHRASE),
                "2026-03-17",
                [build_step(TASK_RECEIVE, "2026-03-17"), build_step(TASK_ACCEPT, "2026-03-17", "file-a")],
            ),
            build_record(
                "beta",
                build_title("\u4e2d\u94f6\u57fa\u91d1\u7ba1\u7406\u6709\u9650\u516c\u53f8", "\u4e2d\u94f6\u6709\u8272\u91d1\u5c5e" + ETF_PHRASE),
                "2026-03-17",
                [build_step(TASK_RECEIVE, "2026-03-17")],
            ),
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = Path(tmpdir) / "state.json"
            baseline_path = Path(tmpdir) / "daily" / "2026-03-17.json"
            baseline_path.parent.mkdir(parents=True, exist_ok=True)
            baseline_path.write_text(
                json.dumps(monitor.build_snapshot(baseline_records, "2026-03-17T00:05:00Z"), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            state_file.write_text(
                json.dumps(monitor.build_snapshot(current_records, "2026-03-17T11:05:00Z"), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            config = monitor.MonitorConfig(
                keyword=KEYWORD,
                state_file_path=state_file,
                smtp_host="smtp.example.com",
                smtp_port=465,
                smtp_username="bot@example.com",
                smtp_password="secret",
                alert_email_from="bot@example.com",
                alert_email_to=["me@example.com"],
            )
            email_calls = []

            with mock.patch("csrc_index_monitor.fetch_all_records", side_effect=AssertionError("daily summary should reuse latest state snapshot")):
                result = monitor.run_monitor(
                    config=config,
                    send_email_func=lambda **kwargs: email_calls.append(kwargs),
                    now_iso="2026-03-17T11:30:00Z",
                    report_mode="daily_summary",
                )

            self.assertEqual(result["report_mode"], "daily_summary")
            self.assertEqual(result["email_subject"], "\u6307\u6570\u57fa\u91d1\u5ba1\u6279\u65e5\u62a52026-03-17")
            self.assertEqual(result["event_count"], 2)
            self.assertEqual(result["new_record_count"], 1)
            self.assertEqual(result["new_step_count"], 1)
            self.assertEqual(len(email_calls), 1)
            self.assertEqual(len(email_calls[0]["attachments"]), 1)
            attachment = email_calls[0]["attachments"][0]
            self.assertEqual(attachment["filename"], "\u6307\u6570\u57fa\u91d1\u5ba1\u6279\u65e5\u62a52026-03-17.pdf")
            self.assertEqual(attachment["subtype"], "pdf")
            self.assertGreater(len(attachment["content"]), 0)
            fitz = monitor.load_fitz_module()
            pdf = fitz.open(stream=attachment["content"], filetype="pdf")
            pix = pdf[0].get_pixmap()
            self.assertTrue(any(channel != 255 for channel in pix.samples))

    def test_daily_summary_skips_email_when_no_changes(self):
        records = [
            build_record("alpha", "alpha", "2026-03-17", [build_step(TASK_RECEIVE, "2026-03-17")])
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = Path(tmpdir) / "state.json"
            baseline_path = Path(tmpdir) / "daily" / "2026-03-17.json"
            baseline_path.parent.mkdir(parents=True, exist_ok=True)
            baseline_path.write_text(
                json.dumps(monitor.build_snapshot(records, "2026-03-17T00:05:00Z"), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            state_file.write_text(
                json.dumps(monitor.build_snapshot(records, "2026-03-17T11:05:00Z"), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            config = monitor.MonitorConfig(
                keyword=KEYWORD,
                state_file_path=state_file,
                smtp_host="smtp.example.com",
                smtp_port=465,
                smtp_username="bot@example.com",
                smtp_password="secret",
                alert_email_from="bot@example.com",
                alert_email_to=["me@example.com"],
            )
            email_calls = []

            with mock.patch("csrc_index_monitor.fetch_all_records", side_effect=AssertionError("daily summary should not fetch live records")):
                result = monitor.run_monitor(
                    config=config,
                    send_email_func=lambda **kwargs: email_calls.append(kwargs),
                    now_iso="2026-03-17T11:30:00Z",
                    report_mode="daily_summary",
                )

            self.assertEqual(result["email_delivery"]["status"], "skipped_no_changes")
            self.assertEqual(result["skipped_reason"], "no_daily_changes")
            self.assertEqual(email_calls, [])

    def test_daily_summary_skips_when_baseline_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = Path(tmpdir) / "state.json"
            config = monitor.MonitorConfig(
                keyword=KEYWORD,
                state_file_path=state_file,
                smtp_host="smtp.example.com",
                smtp_port=465,
                smtp_username="bot@example.com",
                smtp_password="secret",
                alert_email_from="bot@example.com",
                alert_email_to=["me@example.com"],
            )
            result = monitor.run_monitor(
                config=config,
                send_email_func=lambda **kwargs: None,
                now_iso="2026-03-17T11:30:00Z",
                report_mode="daily_summary",
            )

            self.assertEqual(result["email_delivery"]["status"], "skipped_missing_baseline")
            self.assertEqual(result["skipped_reason"], "missing_daily_baseline")

    def test_load_daily_baseline_snapshot_falls_back_to_git_history(self):
        baseline_records = [
            build_record("alpha", "alpha", "2026-03-17", [build_step(TASK_RECEIVE, "2026-03-17")])
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir) / "state-branch"
            repo_root.mkdir(parents=True, exist_ok=True)
            (repo_root / ".git").write_text("gitdir: /tmp/fake\n", encoding="utf-8")
            state_file = repo_root / "state" / "csrc_index_monitor_state.json"
            daily_path = repo_root / "state" / "daily" / "2026-03-17.json"
            expected_snapshot = monitor.build_snapshot(baseline_records, "2026-03-17T01:56:45Z")

            def fake_git_runner(command, **kwargs):
                self.assertEqual(command[0], "git")
                self.assertEqual(command[1], "-C")
                self.assertEqual(command[2], str(repo_root))
                if command[3] == "log":
                    return mock.Mock(returncode=0, stdout="3b76b95b4c80af5eb36aa4352ec7c93d7abfa545\n", stderr="")
                if command[3] == "show":
                    self.assertEqual(command[4], "3b76b95b4c80af5eb36aa4352ec7c93d7abfa545:state/csrc_index_monitor_state.json")
                    return mock.Mock(
                        returncode=0,
                        stdout=json.dumps(expected_snapshot, ensure_ascii=False),
                        stderr="",
                    )
                raise AssertionError(f"Unexpected git command: {command}")

            snapshot, source = monitor.load_daily_baseline_snapshot(
                daily_path,
                state_file,
                datetime(2026, 3, 17, 19, 30, tzinfo=monitor.SHANGHAI_TZ),
                git_runner=fake_git_runner,
            )

            self.assertEqual(source, "git_history")
            self.assertEqual(snapshot, expected_snapshot)

    def test_load_daily_baseline_snapshot_prefers_earlier_git_history_over_late_daily_file(self):
        baseline_records = [
            build_record("alpha", "alpha", "2026-03-17", [build_step(TASK_RECEIVE, "2026-03-17")])
        ]
        late_records = [
            build_record("alpha", "alpha", "2026-03-17", [build_step(TASK_RECEIVE, "2026-03-17"), build_step(TASK_ACCEPT, "2026-03-17", "file-a")])
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir) / "state-branch"
            repo_root.mkdir(parents=True, exist_ok=True)
            (repo_root / ".git").write_text("gitdir: /tmp/fake\n", encoding="utf-8")
            state_file = repo_root / "state" / "csrc_index_monitor_state.json"
            daily_path = repo_root / "state" / "daily" / "2026-03-17.json"
            daily_path.parent.mkdir(parents=True, exist_ok=True)
            late_snapshot = monitor.build_snapshot(late_records, "2026-03-17T11:50:42Z")
            expected_snapshot = monitor.build_snapshot(baseline_records, "2026-03-17T01:56:45Z")
            daily_path.write_text(json.dumps(late_snapshot, ensure_ascii=False), encoding="utf-8")

            def fake_git_runner(command, **kwargs):
                self.assertEqual(command[0], "git")
                self.assertEqual(command[1], "-C")
                self.assertEqual(command[2], str(repo_root))
                if command[3] == "log":
                    return mock.Mock(returncode=0, stdout="3b76b95b4c80af5eb36aa4352ec7c93d7abfa545\n", stderr="")
                if command[3] == "show":
                    return mock.Mock(
                        returncode=0,
                        stdout=json.dumps(expected_snapshot, ensure_ascii=False),
                        stderr="",
                    )
                raise AssertionError(f"Unexpected git command: {command}")

            snapshot, source = monitor.load_daily_baseline_snapshot(
                daily_path,
                state_file,
                datetime(2026, 3, 17, 19, 30, tzinfo=monitor.SHANGHAI_TZ),
                git_runner=fake_git_runner,
            )

            self.assertEqual(source, "git_history")
            self.assertEqual(snapshot, expected_snapshot)

    def test_load_daily_baseline_snapshot_uses_previous_day_last_state_commit(self):
        previous_day_records = [
            build_record("alpha", "alpha", "2026-03-16", [build_step(TASK_RECEIVE, "2026-03-16")])
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir) / "state-branch"
            repo_root.mkdir(parents=True, exist_ok=True)
            (repo_root / ".git").write_text("gitdir: /tmp/fake\n", encoding="utf-8")
            state_file = repo_root / "state" / "csrc_index_monitor_state.json"
            daily_path = repo_root / "state" / "daily" / "2026-03-17.json"
            expected_snapshot = monitor.build_snapshot(previous_day_records, "2026-03-16T15:59:59Z")

            def fake_git_runner(command, **kwargs):
                self.assertEqual(command[0], "git")
                self.assertEqual(command[1], "-C")
                self.assertEqual(command[2], str(repo_root))
                if command[3] == "log":
                    self.assertIn("--since=2026-03-15T16:00:00+00:00", command)
                    self.assertIn("--until=2026-03-16T16:00:00+00:00", command)
                    return mock.Mock(
                        returncode=0,
                        stdout="\n".join(
                            [
                                "1111111111111111111111111111111111111111",
                                "2222222222222222222222222222222222222222",
                            ]
                        ),
                        stderr="",
                    )
                if command[3] == "show":
                    self.assertEqual(command[4], "2222222222222222222222222222222222222222:state/csrc_index_monitor_state.json")
                    return mock.Mock(
                        returncode=0,
                        stdout=json.dumps(expected_snapshot, ensure_ascii=False),
                        stderr="",
                    )
                raise AssertionError(f"Unexpected git command: {command}")

            snapshot, source = monitor.load_daily_baseline_snapshot(
                daily_path,
                state_file,
                datetime(2026, 3, 17, 19, 30, tzinfo=monitor.SHANGHAI_TZ),
                git_runner=fake_git_runner,
            )

            self.assertEqual(source, "git_history")
            self.assertEqual(snapshot, expected_snapshot)

    def test_daily_summary_skips_when_latest_state_missing(self):
        baseline_records = [
            build_record("alpha", "alpha", "2026-03-17", [build_step(TASK_RECEIVE, "2026-03-17")])
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = Path(tmpdir) / "state.json"
            baseline_path = Path(tmpdir) / "daily" / "2026-03-17.json"
            baseline_path.parent.mkdir(parents=True, exist_ok=True)
            baseline_path.write_text(
                json.dumps(monitor.build_snapshot(baseline_records, "2026-03-17T00:05:00Z"), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            config = monitor.MonitorConfig(
                keyword=KEYWORD,
                state_file_path=state_file,
                smtp_host="smtp.example.com",
                smtp_port=465,
                smtp_username="bot@example.com",
                smtp_password="secret",
                alert_email_from="bot@example.com",
                alert_email_to=["me@example.com"],
            )

            with mock.patch("csrc_index_monitor.fetch_all_records", side_effect=AssertionError("daily summary should not fetch live records")):
                result = monitor.run_monitor(
                    config=config,
                    send_email_func=lambda **kwargs: None,
                    now_iso="2026-03-17T11:30:00Z",
                    report_mode="daily_summary",
                )

            self.assertEqual(result["email_delivery"]["status"], "skipped_missing_latest_state")
            self.assertEqual(result["skipped_reason"], "missing_latest_state")


class SuspectedWithdrawalTests(unittest.TestCase):
    def test_find_suspected_withdrawals_uses_week_without_acceptance_or_missing_without_acceptance(self):
        records_seen_before = [
            build_record(
                "suspect",
                build_title("华夏基金管理有限公司", "华夏人工智能" + ETF_PHRASE),
                "2026-03-10",
                [build_step(TASK_RECEIVE, "2026-03-10")],
            ),
            build_record(
                "accepted",
                build_title("易方达基金管理有限公司", "易方达机器人" + ETF_PHRASE),
                "2026-03-10",
                [build_step(TASK_RECEIVE, "2026-03-10"), build_step(TASK_ACCEPT, "2026-03-11", "file-a")],
            ),
            build_record(
                "recent",
                build_title("南方基金管理有限公司", "南方红利" + ETF_PHRASE),
                "2026-03-16",
                [build_step(TASK_RECEIVE, "2026-03-16")],
            ),
            build_record(
                "bond",
                build_title("广发基金管理有限公司", "广发中债短债指数证券投资基金"),
                "2026-03-10",
                [build_step(TASK_RECEIVE, "2026-03-10")],
            ),
            build_record(
                "visible",
                build_title("富国基金管理有限公司", "富国芯片" + ETF_PHRASE),
                "2026-03-10",
                [build_step(TASK_RECEIVE, "2026-03-10")],
            ),
        ]
        first_snapshot = monitor.build_snapshot(records_seen_before, "2026-03-10T02:00:00Z")
        latest_snapshot = monitor.build_snapshot(
            [records_seen_before[-1]],
            "2026-03-18T02:00:00Z",
            previous_snapshot=first_snapshot,
        )

        events = monitor.find_suspected_withdrawal_events(
            latest_snapshot,
            datetime(2026, 3, 19, 19, 30, tzinfo=monitor.SHANGHAI_TZ),
        )

        events_by_id = {event["record_id"]: event for event in events}
        self.assertEqual([event["record_id"] for event in events], ["recent", "visible", "suspect"])
        self.assertEqual(events_by_id["suspect"]["event_type"], "suspected_withdrawal")
        self.assertIn("未显示受理满 7 天", events_by_id["suspect"]["reason"])
        self.assertIn("公示列表中消失", events_by_id["suspect"]["reason"])
        self.assertEqual(events_by_id["recent"]["reason"], "疑似撤回：未显示受理，且已从公示列表中消失。")
        self.assertEqual(events_by_id["visible"]["reason"], "疑似撤回：未显示受理满 7 天。")

    def test_find_suspected_withdrawals_excludes_pre_2025_applications_and_sorts_by_app_date_desc(self):
        records_seen_before = [
            build_record(
                "old-application",
                build_title("华夏基金管理有限公司", "华夏2024人工智能" + ETF_PHRASE),
                "2024-12-31",
                [build_step(TASK_RECEIVE, "2024-12-31")],
            ),
            build_record(
                "newer-application",
                build_title("华夏基金管理有限公司", "华夏2026人工智能" + ETF_PHRASE),
                "2026-02-10",
                [build_step(TASK_RECEIVE, "2026-02-10")],
            ),
            build_record(
                "older-application",
                build_title("南方基金管理有限公司", "南方2025红利" + ETF_PHRASE),
                "2025-01-02",
                [build_step(TASK_RECEIVE, "2025-01-02")],
            ),
        ]
        first_snapshot = monitor.build_snapshot(records_seen_before, "2026-02-10T02:00:00Z")
        latest_snapshot = monitor.build_snapshot([], "2026-03-18T02:00:00Z", previous_snapshot=first_snapshot)

        events = monitor.find_suspected_withdrawal_events(
            latest_snapshot,
            datetime(2026, 3, 18, 19, 30, tzinfo=monitor.SHANGHAI_TZ),
        )

        self.assertEqual([event["record_id"] for event in events], ["newer-application", "older-application"])
        self.assertEqual([event["app_date"] for event in events], ["2026-02-10", "2025-01-02"])

    def test_week_without_acceptance_alone_triggers_suspected_withdrawal(self):
        records = [
            build_record(
                "labor-holiday",
                build_title("华夏基金管理有限公司", "华夏劳动节前人工智能" + ETF_PHRASE),
                "2026-04-30",
                [build_step(TASK_RECEIVE, "2026-04-30")],
            )
        ]
        snapshot = monitor.build_snapshot(records, "2026-05-12T02:00:00Z")

        events = monitor.find_suspected_withdrawal_events(
            snapshot,
            datetime(2026, 5, 12, 19, 30, tzinfo=monitor.SHANGHAI_TZ),
        )

        self.assertEqual([event["record_id"] for event in events], ["labor-holiday"])
        self.assertEqual(events[0]["days_without_acceptance"], 12)
        self.assertEqual(events[0]["reason"], "疑似撤回：未显示受理满 7 天。")

    def test_true_withdrawal_reminder_uses_new_acceptance_batch_window(self):
        accepted_on_day_8_before = build_record(
            "accepted-on-day-8",
            build_title("易方达基金管理有限公司", "易方达机器人" + ETF_PHRASE),
            "2026-03-10",
            [build_step(TASK_RECEIVE, "2026-03-10")],
        )
        accepted_on_day_8_now = build_record(
            "accepted-on-day-8",
            build_title("易方达基金管理有限公司", "易方达机器人" + ETF_PHRASE),
            "2026-03-10",
            [build_step(TASK_RECEIVE, "2026-03-10"), build_step(TASK_ACCEPT, "2026-03-18", "file-a")],
        )
        same_day_unaccepted = build_record(
            "same-day-unaccepted",
            build_title("华夏基金管理有限公司", "华夏同日未受理" + ETF_PHRASE),
            "2026-03-10",
            [build_step(TASK_RECEIVE, "2026-03-10")],
        )
        earlier_unaccepted = build_record(
            "earlier-unaccepted",
            build_title("南方基金管理有限公司", "南方更早未受理" + ETF_PHRASE),
            "2026-03-09",
            [build_step(TASK_RECEIVE, "2026-03-09")],
        )
        later_unaccepted = build_record(
            "later-unaccepted",
            build_title("富国基金管理有限公司", "富国更晚未受理" + ETF_PHRASE),
            "2026-03-11",
            [build_step(TASK_RECEIVE, "2026-03-11")],
        )
        already_accepted = build_record(
            "already-accepted",
            build_title("广发基金管理有限公司", "广发已受理" + ETF_PHRASE),
            "2026-03-09",
            [build_step(TASK_RECEIVE, "2026-03-09"), build_step(TASK_ACCEPT, "2026-03-18", "file-b")],
        )
        first_snapshot = monitor.build_snapshot(
            [accepted_on_day_8_before, same_day_unaccepted, earlier_unaccepted, later_unaccepted, already_accepted],
            "2026-03-17T02:00:00Z",
        )
        latest_snapshot = monitor.build_snapshot(
            [accepted_on_day_8_now, same_day_unaccepted, earlier_unaccepted, later_unaccepted, already_accepted],
            "2026-03-18T02:00:00Z",
            previous_snapshot=first_snapshot,
        )

        events = monitor.find_suspected_withdrawal_events(
            latest_snapshot,
            datetime(2026, 3, 18, 19, 30, tzinfo=monitor.SHANGHAI_TZ),
        )

        true_withdrawals = [event for event in events if event["event_type"] == "confirmed_withdrawal"]
        self.assertEqual([event["record_id"] for event in true_withdrawals], ["same-day-unaccepted"])
        self.assertTrue(all(event["event_id"].startswith("confirmed-withdrawal|") for event in true_withdrawals))
        self.assertIn("同批受理窗口", true_withdrawals[0]["reason"])
        self.assertIn("视为真正撤回", true_withdrawals[0]["reason"])

    def test_true_withdrawal_reminder_uses_first_seen_acceptance_date_not_step_finish_date(self):
        accepted_may8_before = build_record(
            "accepted-may8",
            build_title("易方达基金管理有限公司", "易方达创业板新能源ETF联接基金"),
            "2026-05-08",
            [build_step(TASK_RECEIVE, "2026-05-08")],
        )
        accepted_may8_now = build_record(
            "accepted-may8",
            build_title("易方达基金管理有限公司", "易方达创业板新能源ETF联接基金"),
            "2026-05-08",
            [build_step(TASK_RECEIVE, "2026-05-08"), build_step(TASK_ACCEPT, "2026-05-14", "file-a")],
        )
        accepted_may13_before = build_record(
            "accepted-may13",
            build_title("银华基金管理有限公司", "银华中证农业主题ETF发起式联接基金"),
            "2026-05-13",
            [build_step(TASK_RECEIVE, "2026-05-13")],
        )
        accepted_may13_now = build_record(
            "accepted-may13",
            build_title("银华基金管理有限公司", "银华中证农业主题ETF发起式联接基金"),
            "2026-05-13",
            [build_step(TASK_RECEIVE, "2026-05-13"), build_step(TASK_ACCEPT, "2026-05-14", "file-b")],
        )
        may8_unaccepted = build_record(
            "may8-unaccepted",
            build_title("永赢基金管理有限公司", "永赢中证光伏产业指数型证券投资基金"),
            "2026-05-08",
            [build_step(TASK_RECEIVE, "2026-05-08")],
        )
        may12_unaccepted = build_record(
            "may12-unaccepted",
            build_title("鹏华基金管理有限公司", "鹏华中证稀土产业ETF"),
            "2026-05-12",
            [build_step(TASK_RECEIVE, "2026-05-12")],
        )
        may13_unaccepted = build_record(
            "may13-unaccepted",
            build_title("招商基金管理有限公司", "招商国证价值100ETF"),
            "2026-05-13",
            [build_step(TASK_RECEIVE, "2026-05-13")],
        )
        outside_batch_unaccepted = build_record(
            "outside-batch-unaccepted",
            build_title("华夏基金管理有限公司", "华夏中证红利ETF"),
            "2026-05-14",
            [build_step(TASK_RECEIVE, "2026-05-14")],
        )
        first_snapshot = monitor.build_snapshot(
            [accepted_may8_before, accepted_may13_before, may8_unaccepted, may12_unaccepted, may13_unaccepted, outside_batch_unaccepted],
            "2026-05-17T02:00:00Z",
        )
        latest_snapshot = monitor.build_snapshot(
            [accepted_may8_now, accepted_may13_now, may8_unaccepted, may12_unaccepted, may13_unaccepted, outside_batch_unaccepted],
            "2026-05-18T12:00:00Z",
            previous_snapshot=first_snapshot,
        )

        events = monitor.find_suspected_withdrawal_events(
            latest_snapshot,
            datetime(2026, 5, 18, 20, 30, tzinfo=monitor.SHANGHAI_TZ),
        )

        true_withdrawals = [event for event in events if event["event_type"] == "confirmed_withdrawal"]
        self.assertEqual(
            [event["record_id"] for event in true_withdrawals],
            ["may13-unaccepted", "may12-unaccepted", "may8-unaccepted"],
        )
        self.assertIn("2026-05-08 至 2026-05-13", true_withdrawals[0]["reason"])
        self.assertNotIn("outside-batch-unaccepted", [event["record_id"] for event in true_withdrawals])

    def test_suspected_withdrawal_daily_resends_true_withdrawal_after_suspected_notice(self):
        accepted_before = build_record(
            "accepted-on-day-8",
            build_title("易方达基金管理有限公司", "易方达机器人" + ETF_PHRASE),
            "2026-03-10",
            [build_step(TASK_RECEIVE, "2026-03-10")],
        )
        accepted_now = build_record(
            "accepted-on-day-8",
            build_title("易方达基金管理有限公司", "易方达机器人" + ETF_PHRASE),
            "2026-03-10",
            [build_step(TASK_RECEIVE, "2026-03-10"), build_step(TASK_ACCEPT, "2026-03-18", "file-a")],
        )
        candidate = build_record(
            "candidate",
            build_title("华夏基金管理有限公司", "华夏已疑似通知" + ETF_PHRASE),
            "2026-03-10",
            [build_step(TASK_RECEIVE, "2026-03-10")],
        )
        first_snapshot = monitor.build_snapshot([accepted_before, candidate], "2026-03-17T02:00:00Z")
        latest_snapshot = monitor.build_snapshot(
            [accepted_now, candidate],
            "2026-03-18T02:00:00Z",
            previous_snapshot=first_snapshot,
        )
        latest_snapshot["last_notified_suspected_withdrawal_event_ids"] = [
            monitor.event_id_for("suspected_withdrawal", "candidate")
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = Path(tmpdir) / "state.json"
            state_file.write_text(json.dumps(latest_snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
            config = monitor.MonitorConfig(
                keyword=KEYWORD,
                state_file_path=state_file,
                smtp_host="smtp.example.com",
                smtp_port=465,
                smtp_username="bot@example.com",
                smtp_password="secret",
                alert_email_from="bot@example.com",
                alert_email_to=["me@example.com"],
            )
            email_calls = []

            with mock.patch(
                "csrc_index_monitor.generate_suspected_withdrawal_pdf",
                return_value={
                    "filename": "疑似撤回产品日报2026-03-18.pdf",
                    "content": b"pdf",
                    "maintype": "application",
                    "subtype": "pdf",
                },
            ):
                result = monitor.run_monitor(
                    config=config,
                    send_email_func=lambda **kwargs: email_calls.append(kwargs),
                    now_iso="2026-03-18T11:30:00Z",
                    report_mode="suspected_withdrawal_daily",
                )

            self.assertEqual(result["event_count"], 1)
            self.assertEqual(result["confirmed_withdrawal_count"], 1)
            self.assertEqual([event["event_type"] for event in email_calls[0]["events"]], ["confirmed_withdrawal"])
            saved_state = json.loads(state_file.read_text(encoding="utf-8"))
            self.assertIn(
                monitor.event_id_for("confirmed_withdrawal", "candidate"),
                saved_state["last_notified_suspected_withdrawal_event_ids"],
            )
            self.assertIn(
                monitor.event_id_for("suspected_withdrawal", "candidate"),
                saved_state["last_notified_suspected_withdrawal_event_ids"],
            )

    def test_suspected_withdrawal_daily_sends_pdf_attachment_from_latest_state(self):
        records_seen_before = [
            build_record(
                "suspect",
                build_title("华夏基金管理有限公司", "华夏人工智能" + ETF_PHRASE),
                "2026-03-10",
                [build_step(TASK_RECEIVE, "2026-03-10")],
            )
        ]
        first_snapshot = monitor.build_snapshot(records_seen_before, "2026-03-10T02:00:00Z")
        latest_snapshot = monitor.build_snapshot([], "2026-03-18T02:00:00Z", previous_snapshot=first_snapshot)

        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = Path(tmpdir) / "state.json"
            state_file.write_text(json.dumps(latest_snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
            config = monitor.MonitorConfig(
                keyword=KEYWORD,
                state_file_path=state_file,
                smtp_host="smtp.example.com",
                smtp_port=465,
                smtp_username="bot@example.com",
                smtp_password="secret",
                alert_email_from="bot@example.com",
                alert_email_to=["me@example.com"],
            )
            email_calls = []

            with (
                mock.patch("csrc_index_monitor.fetch_all_records", side_effect=AssertionError("suspected withdrawal report should reuse latest state")),
                mock.patch(
                    "csrc_index_monitor.generate_suspected_withdrawal_pdf",
                    return_value={
                        "filename": "疑似撤回产品日报2026-03-18.pdf",
                        "content": b"pdf",
                        "maintype": "application",
                        "subtype": "pdf",
                    },
                ),
            ):
                result = monitor.run_monitor(
                    config=config,
                    send_email_func=lambda **kwargs: email_calls.append(kwargs),
                    now_iso="2026-03-18T11:30:00Z",
                    report_mode="suspected_withdrawal_daily",
                )

            self.assertEqual(result["report_mode"], "suspected_withdrawal_daily")
            self.assertEqual(result["email_subject"], "指数产品疑似撤回日报2026-03-18")
            self.assertEqual(result["event_count"], 1)
            self.assertEqual(result["suspected_withdrawal_count"], 1)
            self.assertEqual(len(email_calls), 1)
            self.assertIn("疑似撤回产品", email_calls[0]["html_body"])
            self.assertEqual(email_calls[0]["attachments"][0]["subtype"], "pdf")

    def test_suspected_withdrawal_daily_sends_only_new_candidates_and_records_notified_ids(self):
        old_candidate = build_record(
            "old-candidate",
            build_title("华夏基金管理有限公司", "华夏旧候选" + ETF_PHRASE),
            "2026-03-10",
            [build_step(TASK_RECEIVE, "2026-03-10")],
        )
        new_candidate = build_record(
            "new-candidate",
            build_title("南方基金管理有限公司", "南方新候选" + ETF_PHRASE),
            "2026-03-11",
            [build_step(TASK_RECEIVE, "2026-03-11")],
        )
        first_snapshot = monitor.build_snapshot([old_candidate, new_candidate], "2026-03-11T02:00:00Z")
        snapshot = monitor.build_snapshot([], "2026-03-18T02:00:00Z", previous_snapshot=first_snapshot)
        snapshot["last_notified_suspected_withdrawal_event_ids"] = [
            monitor.event_id_for("suspected_withdrawal", "old-candidate")
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = Path(tmpdir) / "state.json"
            state_file.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
            config = monitor.MonitorConfig(
                keyword=KEYWORD,
                state_file_path=state_file,
                smtp_host="smtp.example.com",
                smtp_port=465,
                smtp_username="bot@example.com",
                smtp_password="secret",
                alert_email_from="bot@example.com",
                alert_email_to=["me@example.com"],
            )
            email_calls = []

            with mock.patch(
                "csrc_index_monitor.generate_suspected_withdrawal_pdf",
                return_value={
                    "filename": "疑似撤回产品日报2026-03-18.pdf",
                    "content": b"pdf",
                    "maintype": "application",
                    "subtype": "pdf",
                },
            ):
                result = monitor.run_monitor(
                    config=config,
                    send_email_func=lambda **kwargs: email_calls.append(kwargs),
                    now_iso="2026-03-18T11:30:00Z",
                    report_mode="suspected_withdrawal_daily",
                )

            self.assertEqual(result["event_count"], 1)
            self.assertTrue(result["state_changed"])
            self.assertEqual([event["record_id"] for event in email_calls[0]["events"]], ["new-candidate"])
            saved_state = json.loads(state_file.read_text(encoding="utf-8"))
            self.assertEqual(
                saved_state["last_notified_suspected_withdrawal_event_ids"],
                [
                    monitor.event_id_for("suspected_withdrawal", "new-candidate"),
                    monitor.event_id_for("suspected_withdrawal", "old-candidate"),
                ],
            )

    def test_suspected_withdrawal_daily_skips_when_all_candidates_already_notified(self):
        candidate = build_record(
            "candidate",
            build_title("华夏基金管理有限公司", "华夏已通知" + ETF_PHRASE),
            "2026-03-10",
            [build_step(TASK_RECEIVE, "2026-03-10")],
        )
        first_snapshot = monitor.build_snapshot([candidate], "2026-03-10T02:00:00Z")
        snapshot = monitor.build_snapshot([], "2026-03-18T02:00:00Z", previous_snapshot=first_snapshot)
        snapshot["last_notified_suspected_withdrawal_event_ids"] = [
            monitor.event_id_for("suspected_withdrawal", "candidate")
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = Path(tmpdir) / "state.json"
            state_file.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
            config = monitor.MonitorConfig(
                keyword=KEYWORD,
                state_file_path=state_file,
                smtp_host="smtp.example.com",
                smtp_port=465,
                smtp_username="bot@example.com",
                smtp_password="secret",
                alert_email_from="bot@example.com",
                alert_email_to=["me@example.com"],
            )
            email_calls = []

            result = monitor.run_monitor(
                config=config,
                send_email_func=lambda **kwargs: email_calls.append(kwargs),
                now_iso="2026-03-18T11:30:00Z",
                report_mode="suspected_withdrawal_daily",
            )

            self.assertEqual(result["email_delivery"]["status"], "skipped_no_new_suspected_withdrawals")
            self.assertEqual(result["skipped_reason"], "no_new_suspected_withdrawals")
            self.assertEqual(email_calls, [])

    def test_suspected_withdrawal_pdf_column_widths_stay_positive_on_a4_content_width(self):
        section = monitor.build_pdf_table_sections(
            [
                {
                    "event_type": "suspected_withdrawal",
                    "title": build_title("华夏基金管理有限公司", "华夏人工智能" + ETF_PHRASE),
                    "app_date": "2026-03-10",
                    "record_id": "suspect",
                    "event_id": "suspected-withdrawal|suspect",
                    "first_seen_at": "2026-03-10T02:00:00Z",
                    "last_seen_at": "2026-05-05T02:00:00Z",
                    "missing_since": "2026-05-06T02:00:00Z",
                    "days_without_acceptance": 63,
                    "reason": "疑似撤回：未显示受理满 7 天，且已从公示列表中消失。",
                }
            ],
            monitor.REPORT_MODE_SUSPECTED_WITHDRAWAL_DAILY,
        )[0]

        widths = monitor.normalized_column_widths(section["column_widths"], 493)

        self.assertEqual(sum(widths), 493)
        self.assertTrue(all(width > 0 for width in widths))

    def test_suspected_withdrawal_daily_skips_when_no_candidates(self):
        visible_record = build_record(
            "visible",
            build_title("富国基金管理有限公司", "富国芯片" + ETF_PHRASE),
            "2026-03-10",
            [build_step(TASK_RECEIVE, "2026-03-10"), build_step(TASK_ACCEPT, "2026-03-11", "file-a")],
        )
        latest_snapshot = monitor.build_snapshot([visible_record], "2026-03-18T02:00:00Z")

        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = Path(tmpdir) / "state.json"
            state_file.write_text(json.dumps(latest_snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
            config = monitor.MonitorConfig(
                keyword=KEYWORD,
                state_file_path=state_file,
                smtp_host="smtp.example.com",
                smtp_port=465,
                smtp_username="bot@example.com",
                smtp_password="secret",
                alert_email_from="bot@example.com",
                alert_email_to=["me@example.com"],
            )
            email_calls = []

            result = monitor.run_monitor(
                config=config,
                send_email_func=lambda **kwargs: email_calls.append(kwargs),
                now_iso="2026-03-18T11:30:00Z",
                report_mode="suspected_withdrawal_daily",
            )

            self.assertEqual(result["email_delivery"]["status"], "skipped_no_suspected_withdrawals")
            self.assertEqual(result["skipped_reason"], "no_suspected_withdrawals")
            self.assertEqual(result["suspected_withdrawal_count"], 0)
            self.assertEqual(email_calls, [])


class ObservabilityTests(unittest.TestCase):
    def test_emit_github_error_annotation_escapes_special_characters(self):
        buffer = io.StringIO()

        monitor.emit_github_error_annotation(
            "SMTPDataError: line 1\nline 2 with 50% payload\rline 3",
            stream=buffer,
        )

        self.assertEqual(
            buffer.getvalue(),
            "::error::SMTPDataError: line 1%0Aline 2 with 50%25 payload%0Dline 3\n",
        )

    def test_write_github_step_summary_includes_skip_reason(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            summary_path = Path(tmpdir) / "summary.md"
            result = {
                "report_mode": "daily_summary",
                "event_count": 0,
                "new_record_count": 0,
                "new_step_count": 0,
                "skipped_reason": "no_daily_changes",
                "email_delivery": {
                    "attempted": False,
                    "status": "skipped_no_changes",
                    "recipient_count": 1,
                    "transport": "SMTP_SSL",
                },
                "email_diagnostics": {
                    "smtp_host": "smtp.example.com",
                    "smtp_port": 465,
                    "transport": "SMTP_SSL",
                    "smtp_username_masked": "b***@example.com",
                    "alert_email_from_masked": "b***@example.com",
                    "alert_email_to_masked": ["m***@example.com"],
                    "recipient_count": 1,
                    "sender_matches_username": True,
                    "sender_domain_matches_username_domain": True,
                    "warnings": [],
                },
            }

            monitor.write_github_step_summary(result, summary_path)

            content = summary_path.read_text(encoding="utf-8")
            self.assertIn("daily_summary", content)
            self.assertIn("skipped_no_changes", content)
            self.assertIn("no_daily_changes", content)


if __name__ == "__main__":
    unittest.main()

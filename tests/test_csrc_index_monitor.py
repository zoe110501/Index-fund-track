import json
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

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
    def test_generate_daily_summary_pdf_reports_missing_font(self):
        local_now = datetime(2026, 3, 17, 19, 30, tzinfo=monitor.SHANGHAI_TZ)

        with mock.patch("csrc_index_monitor.find_pdf_font_path", side_effect=RuntimeError("font missing")):
            with self.assertRaisesRegex(RuntimeError, "font missing"):
                monitor.generate_daily_summary_pdf([], local_now)

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


class ObservabilityTests(unittest.TestCase):
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

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import csrc_index_monitor as monitor


FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


class StepIdTests(unittest.TestCase):
    def test_make_step_id_uses_dash_for_missing_file_code(self):
        step = {"taskName": "接收材料", "fnshDate": "2026-03-16", "alFileCde": None}
        self.assertEqual(monitor.make_step_id(step), "接收材料|2026-03-16|-")


class SnapshotDiffTests(unittest.TestCase):
    def test_diff_snapshots_detects_new_record_and_new_steps(self):
        old_snapshot = {
            "records": {
                "alpha": {
                    "title": "甲中证机器人指数证券投资基金",
                    "app_date": "2026-03-16",
                    "step_ids": ["接收材料|2026-03-16|-"],
                }
            },
            "last_notified_event_ids": [],
        }
        new_snapshot = {
            "records": {
                "alpha": {
                    "title": "甲中证机器人指数证券投资基金",
                    "app_date": "2026-03-16",
                    "step_ids": ["接收材料|2026-03-16|-", "受理通知|2026-03-17|file-a"],
                },
                "gamma": {
                    "title": "丙沪深300指数增强证券投资基金",
                    "app_date": "2026-03-17",
                    "step_ids": ["接收材料|2026-03-17|-"],
                },
            }
        }

        events = monitor.diff_snapshots(old_snapshot, new_snapshot)

        self.assertEqual(len(events), 2)
        self.assertEqual(events[0]["event_type"], "new_record")
        self.assertEqual(events[0]["record_id"], "gamma")
        self.assertEqual(events[1]["event_type"], "new_step")
        self.assertEqual(events[1]["record_id"], "alpha")
        self.assertEqual(events[1]["step_id"], "受理通知|2026-03-17|file-a")

    def test_diff_snapshots_skips_already_notified_event_ids(self):
        old_snapshot = {
            "records": {
                "alpha": {
                    "title": "甲中证机器人指数证券投资基金",
                    "app_date": "2026-03-16",
                    "step_ids": ["接收材料|2026-03-16|-"],
                }
            },
            "last_notified_event_ids": ["new-step|alpha|受理通知|2026-03-17|file-a"],
        }
        new_snapshot = {
            "records": {
                "alpha": {
                    "title": "甲中证机器人指数证券投资基金",
                    "app_date": "2026-03-16",
                    "step_ids": ["接收材料|2026-03-16|-", "受理通知|2026-03-17|file-a"],
                }
            }
        }

        self.assertEqual(monitor.diff_snapshots(old_snapshot, new_snapshot), [])


class FetchAndNormalizeTests(unittest.TestCase):
    def test_fetch_all_records_reads_all_pages_and_filters_titles(self):
        pages = {
            1: json.loads((FIXTURES_DIR / "csrc_approval_progress_page1.json").read_text(encoding="utf-8")),
            2: json.loads((FIXTURES_DIR / "csrc_approval_progress_page2.json").read_text(encoding="utf-8")),
        }

        def fake_fetch(page_num, page_size, keyword):
            self.assertEqual(page_size, 2)
            self.assertEqual(keyword, "指数")
            return pages[page_num]

        records = monitor.fetch_all_records("指数", page_size=2, fetch_page=fake_fetch)

        self.assertEqual([record["record_id"] for record in records], ["alpha", "gamma"])
        self.assertEqual(records[1]["steps"][1]["task_name"], "受理通知")

    def test_fetch_all_records_handles_missing_step_lists(self):
        page = {
            "code": "0000",
            "message": "success",
            "data": {
                "records": [
                    {
                        "alAppLtCde": "alpha",
                        "showCntnt": "关于甲公司的指数产品",
                        "appDate": "2026-03-16",
                        "aprvSchdPubFlowViewResultList": None,
                    }
                ],
                "total": 1,
                "size": 1000,
                "current": 1,
            },
        }

        records = monitor.fetch_all_records("指数", fetch_page=lambda page_num, page_size, keyword: page)

        self.assertEqual(records[0]["record_id"], "alpha")
        self.assertEqual(records[0]["steps"], [])

    def test_fetch_all_records_returns_empty_list_for_empty_response(self):
        page = {
            "code": "0000",
            "message": "success",
            "data": {"records": [], "total": 0, "size": 1000, "current": 1},
        }

        records = monitor.fetch_all_records("指数", fetch_page=lambda page_num, page_size, keyword: page)

        self.assertEqual(records, [])


class OrchestrationTests(unittest.TestCase):
    def test_run_monitor_bootstraps_without_sending_email(self):
        records = [
            {
                "record_id": "alpha",
                "title": "甲中证机器人指数证券投资基金",
                "app_date": "2026-03-16",
                "steps": [{"task_name": "接收材料", "fnsh_date": "2026-03-16", "step_id": "接收材料|2026-03-16|-"}],
            }
        ]
        sent_messages = []

        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = Path(tmpdir) / "state.json"
            result = monitor.run_monitor(
                config=monitor.MonitorConfig(
                    keyword="指数",
                    state_file_path=state_file,
                    smtp_host="smtp.example.com",
                    smtp_port=465,
                    smtp_username="bot@example.com",
                    smtp_password="secret",
                    alert_email_from="bot@example.com",
                    alert_email_to=["me@example.com"],
                ),
                fetch_records=lambda keyword: records,
                send_email_func=lambda *_args, **_kwargs: sent_messages.append("sent"),
                now_iso="2026-03-16T10:00:00Z",
            )

            self.assertTrue(state_file.exists())
            self.assertEqual(sent_messages, [])
            self.assertTrue(result["baseline_created"])
            self.assertEqual(result["events"], [])

    def test_run_monitor_sends_summary_for_incremental_changes(self):
        first_records = [
            {
                "record_id": "alpha",
                "title": "关于易方达基金管理有限公司的《公开募集基金募集申请注册-易方达中证机器人指数证券投资基金》",
                "app_date": "2026-03-16",
                "steps": [{"task_name": "接收材料", "fnsh_date": "2026-03-16", "step_id": "接收材料|2026-03-16|-"}],
            }
        ]
        second_records = [
            {
                "record_id": "alpha",
                "title": "关于易方达基金管理有限公司的《公开募集基金募集申请注册-易方达上证综指交易型开放式指数证券投资基金联接基金》",
                "app_date": "2026-03-16",
                "steps": [
                    {"task_name": "接收材料", "fnsh_date": "2026-03-16", "step_id": "接收材料|2026-03-16|-"},
                    {"task_name": "受理通知", "fnsh_date": "2026-03-17", "step_id": "受理通知|2026-03-17|file-a"},
                ],
            },
            {
                "record_id": "gamma",
                "title": "关于华夏基金管理有限公司的《公开募集基金募集申请注册-华夏创业板人工智能交易型开放式指数证券投资基金》",
                "app_date": "2026-03-17",
                "steps": [{"task_name": "接收材料", "fnsh_date": "2026-03-17", "step_id": "接收材料|2026-03-17|-"}],
            },
            {
                "record_id": "delta",
                "title": "关于国泰基金管理有限公司的《公开募集基金募集申请注册-国泰沪深300指数增强证券投资基金》",
                "app_date": "2026-03-18",
                "steps": [{"task_name": "接收材料", "fnsh_date": "2026-03-18", "step_id": "接收材料|2026-03-18|-"}],
            },
        ]
        email_calls = []

        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = Path(tmpdir) / "state.json"
            base_config = monitor.MonitorConfig(
                keyword="指数",
                state_file_path=state_file,
                smtp_host="smtp.example.com",
                smtp_port=465,
                smtp_username="bot@example.com",
                smtp_password="secret",
                alert_email_from="bot@example.com",
                alert_email_to=["me@example.com"],
            )

            monitor.run_monitor(
                config=base_config,
                fetch_records=lambda keyword: first_records,
                send_email_func=lambda *_args, **_kwargs: email_calls.append("baseline"),
                now_iso="2026-03-16T10:00:00Z",
            )

            result = monitor.run_monitor(
                config=base_config,
                fetch_records=lambda keyword: second_records,
                send_email_func=lambda **kwargs: email_calls.append(kwargs),
                now_iso="2026-03-17T10:00:00Z",
            )

            self.assertEqual(len(email_calls), 1)
            self.assertEqual(result["event_count"], 3)
            self.assertIn("新产品 2 条", email_calls[0]["subject"])
            self.assertIn("新节点 1 条", email_calls[0]["subject"])
            self.assertIn("请查看支持 HTML 的邮件正文获取完整表格。", email_calls[0]["body"])
            self.assertIn("新产品：2 条", email_calls[0]["body"])
            self.assertIn("新节点：1 条", email_calls[0]["body"])
            self.assertNotIn("本轮检测到以下增量", email_calls[0]["body"])
            self.assertNotIn("序号 | 管理人 | 产品名称", email_calls[0]["body"])
            self.assertIn("html_body", email_calls[0])
            self.assertIn("font-family: FangSong", email_calls[0]["html_body"])
            self.assertIn("<th", email_calls[0]["html_body"])
            self.assertIn("最新状态日期", email_calls[0]["html_body"])
            self.assertIn("华夏创业板人工智能交易型开放式指数证券投资基金", email_calls[0]["html_body"])

    def test_run_monitor_does_not_update_state_when_email_fails(self):
        first_records = [
            {
                "record_id": "alpha",
                "title": "甲中证机器人指数证券投资基金",
                "app_date": "2026-03-16",
                "steps": [{"task_name": "接收材料", "fnsh_date": "2026-03-16", "step_id": "接收材料|2026-03-16|-"}],
            }
        ]
        second_records = [
            {
                "record_id": "alpha",
                "title": "甲中证机器人指数证券投资基金",
                "app_date": "2026-03-16",
                "steps": [
                    {"task_name": "接收材料", "fnsh_date": "2026-03-16", "step_id": "接收材料|2026-03-16|-"},
                    {"task_name": "受理通知", "fnsh_date": "2026-03-17", "step_id": "受理通知|2026-03-17|file-a"},
                ],
            }
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = Path(tmpdir) / "state.json"
            config = monitor.MonitorConfig(
                keyword="指数",
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
                send_email_func=lambda *_args, **_kwargs: None,
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


class ConfigTests(unittest.TestCase):
    def test_build_email_diagnostics_flags_sender_mismatch(self):
        diagnostics = monitor.build_email_diagnostics(
            monitor.MonitorConfig(
                keyword="鎸囨暟",
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
        self.assertEqual(diagnostics["smtp_port"], 465)
        self.assertEqual(diagnostics["recipient_count"], 2)
        self.assertFalse(diagnostics["sender_matches_username"])
        self.assertFalse(diagnostics["sender_domain_matches_username_domain"])
        self.assertTrue(diagnostics["warnings"])
        self.assertIn("@", diagnostics["smtp_username_masked"])
        self.assertIn("@", diagnostics["alert_email_from_masked"])

    def test_load_config_from_env_parses_multiple_recipients(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = Path(tmpdir) / "state.json"
            env = {
                "CSRC_QUERY_KEYWORD": "指数",
                "SMTP_HOST": "smtp.example.com",
                "SMTP_PORT": "465",
                "SMTP_USERNAME": "bot@example.com",
                "SMTP_PASSWORD": "secret",
                "ALERT_EMAIL_FROM": "bot@example.com",
                "ALERT_EMAIL_TO": "one@example.com, two@example.com ,three@example.com",
                "STATE_FILE_PATH": str(state_file),
            }

            with mock.patch.dict(os.environ, env, clear=False):
                config = monitor.load_config_from_env()

            self.assertEqual(
                config.alert_email_to,
                ["one@example.com", "two@example.com", "three@example.com"],
            )
            self.assertEqual(config.state_file_path, state_file)


class DisplayFieldTests(unittest.TestCase):
    def test_extract_display_fields_from_raw_title(self):
        display = monitor.extract_display_fields(
            "关于易方达基金管理有限公司的《公开募集基金募集申请注册-易方达创业板新能源交易型开放式指数证券投资基金》"
        )

        self.assertEqual(display["manager"], "易方达")
        self.assertEqual(display["product_name"], "易方达创业板新能源交易型开放式指数证券投资基金")
        self.assertEqual(display["product_type"], "ETF")

    def test_format_table_aligns_columns(self):
        table = monitor.format_table(
            ["序号", "管理人", "产品类型"],
            [
                ["1", "华夏", "ETF"],
                ["2", "国泰", "普通指数"],
            ],
        )

        lines = table.splitlines()
        self.assertEqual(lines[0].count("|"), 2)
        self.assertIn("-+-", lines[1])
        self.assertEqual(lines[2].count("|"), 2)


class ObservabilityTests(unittest.TestCase):
    def test_run_monitor_returns_email_delivery_diagnostics(self):
        first_records = [
            {
                "record_id": "alpha",
                "title": "鐢蹭腑璇佹満鍣ㄤ汉鎸囨暟璇佸埜鎶曡祫鍩洪噾",
                "app_date": "2026-03-16",
                "steps": [{"task_name": "鎺ユ敹鏉愭枡", "fnsh_date": "2026-03-16", "step_id": "鎺ユ敹鏉愭枡|2026-03-16|-"}],
            }
        ]
        second_records = [
            {
                "record_id": "alpha",
                "title": "鐢蹭腑璇佹満鍣ㄤ汉鎸囨暟璇佸埜鎶曡祫鍩洪噾",
                "app_date": "2026-03-16",
                "steps": [
                    {"task_name": "鎺ユ敹鏉愭枡", "fnsh_date": "2026-03-16", "step_id": "鎺ユ敹鏉愭枡|2026-03-16|-"},
                    {"task_name": "鍙楃悊閫氱煡", "fnsh_date": "2026-03-17", "step_id": "鍙楃悊閫氱煡|2026-03-17|file-a"},
                ],
            },
            {
                "record_id": "gamma",
                "title": "涓欐勃娣?00鎸囨暟澧炲己璇佸埜鎶曡祫鍩洪噾",
                "app_date": "2026-03-17",
                "steps": [{"task_name": "鎺ユ敹鏉愭枡", "fnsh_date": "2026-03-17", "step_id": "鎺ユ敹鏉愭枡|2026-03-17|-"}],
            },
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = Path(tmpdir) / "state.json"
            config = monitor.MonitorConfig(
                keyword="鎸囨暟",
                state_file_path=state_file,
                smtp_host="smtp.example.com",
                smtp_port=465,
                smtp_username="bot@example.com",
                smtp_password="secret",
                alert_email_from="bot@example.com",
                alert_email_to=["me@example.com", "ops@example.com"],
            )

            monitor.run_monitor(
                config=config,
                fetch_records=lambda keyword: first_records,
                send_email_func=lambda **kwargs: None,
                now_iso="2026-03-16T10:00:00Z",
            )

            result = monitor.run_monitor(
                config=config,
                fetch_records=lambda keyword: second_records,
                send_email_func=lambda **kwargs: None,
                now_iso="2026-03-17T10:00:00Z",
            )

            self.assertEqual(result["event_count"], 2)
            self.assertEqual(result["new_record_count"], 1)
            self.assertEqual(result["new_step_count"], 1)
            self.assertEqual(result["email_delivery"]["status"], "sent")
            self.assertEqual(result["email_delivery"]["recipient_count"], 2)
            self.assertTrue(result["email_delivery"]["attempted"])
            self.assertEqual(result["email_diagnostics"]["smtp_host"], "smtp.example.com")
            self.assertEqual(result["email_diagnostics"]["recipient_count"], 2)

    def test_write_github_step_summary_writes_masked_delivery_details(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            summary_path = Path(tmpdir) / "summary.md"
            result = {
                "baseline_created": False,
                "event_count": 2,
                "new_record_count": 1,
                "new_step_count": 1,
                "email_delivery": {
                    "attempted": True,
                    "status": "sent",
                    "recipient_count": 2,
                    "transport": "SMTP_SSL",
                },
                "email_diagnostics": {
                    "smtp_host": "smtp.example.com",
                    "smtp_port": 465,
                    "transport": "SMTP_SSL",
                    "smtp_username_masked": "b***@example.com",
                    "alert_email_from_masked": "b***@example.com",
                    "alert_email_to_masked": ["m***@example.com", "o***@example.com"],
                    "recipient_count": 2,
                    "sender_matches_username": True,
                    "sender_domain_matches_username_domain": True,
                    "warnings": [],
                },
            }

            monitor.write_github_step_summary(result, summary_path)

            content = summary_path.read_text(encoding="utf-8")
            self.assertIn("Email delivery diagnostics", content)
            self.assertIn("smtp.example.com", content)
            self.assertIn("b***@example.com", content)
            self.assertIn("sent", content)


if __name__ == "__main__":
    unittest.main()

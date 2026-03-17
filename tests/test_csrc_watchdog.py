from datetime import datetime
import tempfile
import unittest
from pathlib import Path

import csrc_index_monitor as monitor
import csrc_watchdog as watchdog


class WatchdogDecisionTests(unittest.TestCase):
    def test_normalize_watchdog_state_defaults_for_legacy_snapshot(self):
        normalized = watchdog.normalize_watchdog_state(
            {
                "last_success_at": "2026-03-17T06:08:30Z",
                "records": {},
            }
        )

        self.assertEqual(normalized["last_success_at"], "2026-03-17T06:08:30Z")
        self.assertEqual(
            normalized["watchdog"],
            {
                "last_catchup_target_slot": None,
                "last_catchup_triggered_at": None,
            },
        )

    def test_evaluate_watchdog_triggers_when_daytime_run_is_stale(self):
        decision = watchdog.evaluate_watchdog(
            {
                "last_success_at": "2026-03-17T06:08:30Z",
                "records": {},
                "watchdog": {},
            },
            now=watchdog.parse_timestamp("2026-03-17T07:45:00Z"),
        )

        self.assertTrue(decision["should_dispatch"])
        self.assertEqual(decision["target_slot"], "2026-03-17T15:05:00+08:00")
        self.assertEqual(decision["reason"], "stale_within_window")

    def test_evaluate_watchdog_waits_until_threshold_passes(self):
        decision = watchdog.evaluate_watchdog(
            {
                "last_success_at": "2026-03-17T06:08:30Z",
                "records": {},
                "watchdog": {},
            },
            now=watchdog.parse_timestamp("2026-03-17T07:20:00Z"),
        )

        self.assertFalse(decision["should_dispatch"])
        self.assertEqual(decision["reason"], "within_threshold")

    def test_evaluate_watchdog_does_not_trigger_at_night(self):
        decision = watchdog.evaluate_watchdog(
            {
                "last_success_at": "2026-03-17T12:30:00Z",
                "records": {},
                "watchdog": {},
            },
            now=watchdog.parse_timestamp("2026-03-17T15:00:00Z"),
        )

        self.assertFalse(decision["should_dispatch"])
        self.assertEqual(decision["reason"], "outside_watchdog_window")

    def test_evaluate_watchdog_only_triggers_once_per_target_slot(self):
        snapshot = {
            "last_success_at": "2026-03-17T06:08:30Z",
            "records": {},
            "watchdog": {
                "last_catchup_target_slot": "2026-03-17T15:05:00+08:00",
                "last_catchup_triggered_at": "2026-03-17T15:45:00+08:00",
            },
        }

        decision = watchdog.evaluate_watchdog(
            snapshot,
            now=watchdog.parse_timestamp("2026-03-17T07:50:00Z"),
        )

        self.assertFalse(decision["should_dispatch"])
        self.assertEqual(decision["reason"], "already_dispatched_for_target_slot")

    def test_evaluate_watchdog_stops_after_main_workflow_succeeds(self):
        decision = watchdog.evaluate_watchdog(
            {
                "last_success_at": "2026-03-17T08:07:00Z",
                "records": {},
                "watchdog": {
                    "last_catchup_target_slot": "2026-03-17T15:05:00+08:00",
                    "last_catchup_triggered_at": "2026-03-17T15:45:00+08:00",
                },
            },
            now=watchdog.parse_timestamp("2026-03-17T08:15:00Z"),
        )

        self.assertFalse(decision["should_dispatch"])
        self.assertEqual(decision["reason"], "up_to_date")

    def test_record_catchup_dispatch_updates_watchdog_metadata(self):
        updated = watchdog.record_catchup_dispatch(
            {
                "last_success_at": "2026-03-17T06:08:30Z",
                "records": {},
            },
            target_slot="2026-03-17T15:05:00+08:00",
            triggered_at="2026-03-17T15:45:00+08:00",
        )

        self.assertEqual(
            updated["watchdog"],
            {
                "last_catchup_target_slot": "2026-03-17T15:05:00+08:00",
                "last_catchup_triggered_at": "2026-03-17T15:45:00+08:00",
            },
        )


class MonitorStateCompatibilityTests(unittest.TestCase):
    def test_run_monitor_preserves_watchdog_metadata(self):
        records = [
            {
                "record_id": "alpha",
                "title": "test-title",
                "app_date": "2026-03-17",
                "steps": [],
            }
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = Path(tmpdir) / "state.json"
            state_file.write_text(
                """
{
  "last_success_at": "2026-03-17T06:08:30Z",
  "records": {},
  "last_notified_event_ids": [],
  "watchdog": {
    "last_catchup_target_slot": "2026-03-17T15:05:00+08:00",
    "last_catchup_triggered_at": "2026-03-17T15:45:00+08:00"
  }
}
                """.strip(),
                encoding="utf-8",
            )

            monitor.run_monitor(
                config=monitor.MonitorConfig(
                    keyword="test",
                    state_file_path=state_file,
                    smtp_host="smtp.example.com",
                    smtp_port=465,
                    smtp_username="bot@example.com",
                    smtp_password="secret",
                    alert_email_from="bot@example.com",
                    alert_email_to=["me@example.com"],
                ),
                fetch_records=lambda keyword: records,
                send_email_func=lambda *_args, **_kwargs: None,
                now_iso="2026-03-17T08:02:00Z",
            )

            state_payload = monitor.load_state(state_file)

        self.assertEqual(
            state_payload["watchdog"],
            {
                "last_catchup_target_slot": "2026-03-17T15:05:00+08:00",
                "last_catchup_triggered_at": "2026-03-17T15:45:00+08:00",
            },
        )


class WatchdogRunTests(unittest.TestCase):
    def test_run_watchdog_dispatches_and_persists_metadata(self):
        dispatched = []

        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = Path(tmpdir) / "state.json"
            state_file.write_text(
                """
{
  "last_success_at": "2026-03-17T06:08:30Z",
  "records": {},
  "watchdog": {}
}
                """.strip(),
                encoding="utf-8",
            )

            result = watchdog.run_watchdog(
                config=watchdog.WatchdogConfig(
                    state_file_path=state_file,
                    repository="zoe110501/Index-fund-track",
                    token="token",
                    workflow_filename="csrc-index-monitor.yml",
                    api_url="https://api.github.com",
                    threshold_minutes=90,
                ),
                now=watchdog.parse_timestamp("2026-03-17T07:45:00Z"),
                dispatch_func=lambda **kwargs: dispatched.append(kwargs),
            )

            state_payload = monitor.load_state(state_file)

        self.assertEqual(result["status"], "dispatched")
        self.assertEqual(len(dispatched), 1)
        self.assertEqual(
            state_payload["watchdog"]["last_catchup_target_slot"],
            "2026-03-17T15:05:00+08:00",
        )

    def test_run_watchdog_skips_dispatch_when_workflow_is_healthy(self):
        dispatched = []

        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = Path(tmpdir) / "state.json"
            state_file.write_text(
                """
{
  "last_success_at": "2026-03-17T07:20:00Z",
  "records": {},
  "watchdog": {}
}
                """.strip(),
                encoding="utf-8",
            )

            result = watchdog.run_watchdog(
                config=watchdog.WatchdogConfig(
                    state_file_path=state_file,
                    repository="zoe110501/Index-fund-track",
                    token="token",
                    workflow_filename="csrc-index-monitor.yml",
                    api_url="https://api.github.com",
                    threshold_minutes=90,
                ),
                now=watchdog.parse_timestamp("2026-03-17T07:45:00Z"),
                dispatch_func=lambda **kwargs: dispatched.append(kwargs),
            )

        self.assertEqual(result["status"], "idle")
        self.assertEqual(dispatched, [])


if __name__ == "__main__":
    unittest.main()

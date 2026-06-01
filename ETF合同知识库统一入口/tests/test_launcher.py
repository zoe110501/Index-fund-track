from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import app as launcher


def make_config(*dev_paths: Path) -> launcher.ServiceConfig:
    return launcher.ServiceConfig(
        key="etf",
        title="ETF",
        subtitle="ETF",
        description="ETF",
        system_dir="etf",
        dev_paths=dev_paths,
        preferred_port=5001,
    )


def touch_app(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    (path / "app.py").write_text("# test child app\n", encoding="utf-8")


class ResolveSystemPathTests(unittest.TestCase):
    def setUp(self) -> None:
        self._old_systems_dir = launcher.SYSTEMS_DIR

    def tearDown(self) -> None:
        launcher.SYSTEMS_DIR = self._old_systems_dir

    def test_prefers_packaged_system_over_dev_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            launcher.SYSTEMS_DIR = root / "systems"
            packaged = launcher.SYSTEMS_DIR / "etf"
            workspace = root / "workspace" / "ETF合同知识库"
            desktop = root / "desktop" / "ETF合同知识库"
            for path in (packaged, workspace, desktop):
                touch_app(path)

            self.assertEqual(
                launcher.resolve_system_path(make_config(workspace, desktop)),
                packaged,
            )

    def test_uses_first_existing_dev_path_when_not_packaged(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            launcher.SYSTEMS_DIR = root / "systems"
            workspace = root / "workspace" / "ETF合同知识库"
            desktop = root / "desktop" / "ETF合同知识库"
            touch_app(workspace)
            touch_app(desktop)

            self.assertEqual(
                launcher.resolve_system_path(make_config(workspace, desktop)),
                workspace,
            )

    def test_falls_back_to_packaged_path_when_no_app_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            launcher.SYSTEMS_DIR = root / "systems"
            workspace = root / "workspace" / "ETF合同知识库"
            desktop = root / "desktop" / "ETF合同知识库"

            self.assertEqual(
                launcher.resolve_system_path(make_config(workspace, desktop)),
                launcher.SYSTEMS_DIR / "etf",
            )


class ServiceEnvironmentTests(unittest.TestCase):
    def test_linked_service_exports_packaged_template_dirs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            system_path = Path(tmp)
            summary_templates = system_path / "packaged_assets" / "product_summary_templates"
            legal_templates = system_path / "packaged_assets" / "legal_templates"
            summary_templates.mkdir(parents=True)
            legal_templates.mkdir(parents=True)

            env = launcher.service_process_env(launcher.SERVICES["linked"], system_path)

            self.assertEqual(env["PRODUCT_SUMMARY_TEMPLATE_DIR"], str(summary_templates))
            self.assertEqual(env["CONTRACT_TEMPLATE_DIR"], str(legal_templates))


class StatusApiTests(unittest.TestCase):
    def test_status_api_returns_service_entries(self) -> None:
        response = launcher.app.test_client().get("/api/status")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual({item["key"] for item in payload}, {"etf", "linked"})
        for item in payload:
            self.assertIn("path", item)
            self.assertIn("ready", item)
            self.assertIn("port", item)


if __name__ == "__main__":
    unittest.main()

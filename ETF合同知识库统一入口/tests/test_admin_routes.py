from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import app as launcher
import maintenance_config


class AdminRoutesTests(unittest.TestCase):
    def setUp(self) -> None:
        self._old_admin_config_base_dir = launcher.ADMIN_CONFIG_BASE_DIR
        self.tmp = tempfile.TemporaryDirectory()
        self.base_dir = Path(self.tmp.name)
        maintenance_config.ensure_seed_configs(base_dir=self.base_dir)
        launcher.ADMIN_CONFIG_BASE_DIR = self.base_dir
        self.client = launcher.app.test_client()

    def tearDown(self) -> None:
        launcher.ADMIN_CONFIG_BASE_DIR = self._old_admin_config_base_dir
        self.tmp.cleanup()

    def test_admin_page_loads(self) -> None:
        response = self.client.get("/admin")

        self.assertEqual(response.status_code, 200)
        self.assertIn("维护后台", response.get_data(as_text=True))

    def test_admin_config_get_endpoints_return_data(self) -> None:
        expectations = {
            "/api/admin/templates": "templates",
            "/api/admin/variables": "variables",
            "/api/admin/organizations": "organizations",
        }

        for url, key in expectations.items():
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, 200)
                payload = response.get_json()
                self.assertTrue(payload["success"])
                self.assertIn(key, payload["data"])

    def test_admin_config_post_updates_data_and_creates_backup(self) -> None:
        data = maintenance_config.load_config("variable_registry", base_dir=self.base_dir)
        data["variables"].append(
            {
                "name": "NEW_TEST_VARIABLE",
                "label": "测试变量",
                "type": "string",
                "required": False,
                "systems": ["etf"],
            }
        )

        response = self.client.post("/api/admin/variables", json=data)

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["success"])
        updated = maintenance_config.load_config("variable_registry", base_dir=self.base_dir)
        self.assertTrue(any(item["name"] == "NEW_TEST_VARIABLE" for item in updated["variables"]))
        backups = list((self.base_dir / "backups" / "maintenance-admin").rglob("variable_registry.json"))
        self.assertTrue(backups)

    def test_admin_config_post_rejects_invalid_json(self) -> None:
        response = self.client.post(
            "/api/admin/variables",
            data="{",
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertFalse(response.get_json()["success"])


if __name__ == "__main__":
    unittest.main()

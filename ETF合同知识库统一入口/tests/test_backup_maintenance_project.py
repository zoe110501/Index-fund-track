from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BACKUP_SCRIPT = PROJECT_ROOT / "scripts" / "backup_maintenance_project.py"


def load_backup_module():
    spec = importlib.util.spec_from_file_location("backup_maintenance_project", BACKUP_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError("Cannot load backup script")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_file(path: Path, text: str = "x") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


class BackupMaintenanceProjectTests(unittest.TestCase):
    def test_backup_copies_maintainable_files_and_skips_generated_dirs(self) -> None:
        backup = load_backup_module()
        with tempfile.TemporaryDirectory() as tmp:
            workspace = Path(tmp) / "workspace"
            backup_root = workspace / "ETF合同知识库统一入口" / "backups" / "maintenance-admin"

            write_file(workspace / "ETF合同知识库统一入口" / "app.py")
            write_file(workspace / "ETF合同知识库统一入口" / "templates" / "index.html")
            write_file(workspace / "ETF合同知识库统一入口" / "build" / "junk.py")
            write_file(workspace / "ETF合同知识库统一入口" / "logs" / "server.log")
            write_file(workspace / "ETF合同知识库统一入口" / "backups" / "old" / "app.py")

            write_file(workspace / "ETF合同知识库" / "app.py")
            write_file(workspace / "ETF合同知识库" / "packaged_assets" / "reference_prospectus" / "SSE_CROSS.docx")
            write_file(workspace / "ETF合同知识库" / "dist" / "old.docx")

            write_file(workspace / "ETF联接基金合同知识库" / "app.py")
            write_file(workspace / "ETF联接基金合同知识库" / "02_变量定义表.json", "{}")
            write_file(workspace / "ETF联接基金合同知识库" / "__pycache__" / "app.pyc")

            created = backup.backup_workspace(
                workspace_root=workspace,
                backup_root=backup_root,
                timestamp="20260101-010203",
            )

            self.assertTrue((created / "ETF合同知识库统一入口" / "app.py").exists())
            self.assertTrue((created / "ETF合同知识库统一入口" / "templates" / "index.html").exists())
            self.assertTrue((created / "ETF合同知识库" / "app.py").exists())
            self.assertTrue(
                (created / "ETF合同知识库" / "packaged_assets" / "reference_prospectus" / "SSE_CROSS.docx").exists()
            )
            self.assertTrue((created / "ETF联接基金合同知识库" / "02_变量定义表.json").exists())

            self.assertFalse((created / "ETF合同知识库统一入口" / "build" / "junk.py").exists())
            self.assertFalse((created / "ETF合同知识库统一入口" / "logs" / "server.log").exists())
            self.assertFalse((created / "ETF合同知识库统一入口" / "backups" / "old" / "app.py").exists())
            self.assertFalse((created / "ETF合同知识库" / "dist" / "old.docx").exists())
            self.assertFalse((created / "ETF联接基金合同知识库" / "__pycache__" / "app.pyc").exists())

    def test_backup_root_must_stay_inside_workspace(self) -> None:
        backup = load_backup_module()
        with tempfile.TemporaryDirectory() as tmp:
            workspace = Path(tmp) / "workspace"
            write_file(workspace / "ETF合同知识库统一入口" / "app.py")

            with self.assertRaises(ValueError):
                backup.backup_workspace(
                    workspace_root=workspace,
                    backup_root=Path(tmp) / "outside",
                    timestamp="20260101-010203",
                )


if __name__ == "__main__":
    unittest.main()

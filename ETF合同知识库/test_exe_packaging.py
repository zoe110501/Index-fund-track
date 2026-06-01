import importlib
import importlib.util
import tempfile
import unittest
from pathlib import Path

import app


class ExePackagingSupportTests(unittest.TestCase):
    def test_packaging_support_module_exists_and_computes_runtime_root(self):
        module_spec = importlib.util.find_spec("packaging_support")

        self.assertIsNotNone(module_spec)
        if module_spec is None:
            return

        packaging_support = importlib.import_module("packaging_support")
        runtime_root = packaging_support.compute_app_root(
            module_file=r"C:\workspace\ETF合同知识库\app.py",
            frozen=True,
            executable=r"D:\release\ETF合同知识库.exe",
        )
        source_root = packaging_support.compute_app_root(
            module_file=r"C:\workspace\ETF合同知识库\app.py",
            frozen=False,
            executable=None,
        )

        self.assertEqual(Path(r"D:\release"), runtime_root)
        self.assertEqual(Path(r"C:\workspace\ETF合同知识库"), source_root)

    def test_app_exposes_packaged_assets_directory_and_reference_defaults(self):
        self.assertTrue(hasattr(app, "PACKAGED_ASSETS_DIR"))
        if not hasattr(app, "PACKAGED_ASSETS_DIR"):
            return

        expected_packaged_dir = app.BASE_DIR / "packaged_assets"
        expected_reference_docx = expected_packaged_dir / "reference_prospectus" / "SSE_CROSS.docx"

        self.assertEqual(expected_packaged_dir, app.PACKAGED_ASSETS_DIR)
        self.assertIn(expected_reference_docx, app._reference_prospectus_docx_candidates("SSE_CROSS"))
        self.assertEqual(
            expected_packaged_dir / "review_rules" / "基金合同与招募说明书规则.xlsx",
            app.RULES_XLSX,
        )

    def test_prepare_packaged_assets_copies_first_existing_source(self):
        module_spec = importlib.util.find_spec("packaging_support")

        self.assertIsNotNone(module_spec)
        if module_spec is None:
            return

        packaging_support = importlib.import_module("packaging_support")
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "project"
            source_root = Path(tmp_dir) / "sources"
            project_root.mkdir()
            source_root.mkdir()

            existing_source = source_root / "rules.xlsx"
            existing_source.write_bytes(b"demo")

            copied_assets = packaging_support.prepare_packaged_assets(
                project_root=project_root,
                specs=[
                    packaging_support.AssetSpec(
                        key="rules",
                        destination=Path("packaged_assets/review_rules/rules.xlsx"),
                        sources=(source_root / "missing.xlsx", existing_source),
                    )
                ],
            )

            copied_target = project_root / "packaged_assets" / "review_rules" / "rules.xlsx"
            self.assertEqual(1, len(copied_assets))
            self.assertTrue(copied_target.exists())
            self.assertEqual(b"demo", copied_target.read_bytes())

    def test_build_scripts_and_spec_exist_for_directory_exe_flow(self):
        project_root = Path(app.__file__).resolve().parent

        self.assertTrue((project_root / "build_exe.ps1").exists())
        self.assertTrue((project_root / "ETF合同知识库.spec").exists())
        self.assertTrue((project_root / "启动ETF合同知识库.bat").exists())


if __name__ == "__main__":
    unittest.main()

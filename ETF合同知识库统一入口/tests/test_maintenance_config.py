from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import maintenance_config


CRITICAL_TEMPLATE_IDS = {
    "etf_contract_markdown",
    "etf_prospectus_markdown",
    "etf_variable_definition_json",
    "etf_contract_diff_table",
    "etf_prospectus_diff_table",
    "etf_entry_table",
    "etf_contract_clause_library",
    "etf_prospectus_clause_library",
    "etf_business_text_overrides",
    "etf_contract_docx_source_dir",
    "etf_contract_docx_packaged_dir",
    "etf_product_summary_docx",
    "etf_prospectus_reference_sse_cross",
    "etf_prospectus_reference_sse_single",
    "etf_prospectus_reference_sse_hk",
    "etf_prospectus_reference_szse_cross",
    "etf_prospectus_reference_szse_single",
    "etf_prospectus_reference_szse_hk",
    "etf_prospectus_reference_text_sse_cross",
    "etf_prospectus_reference_text_sse_single",
    "etf_prospectus_reference_text_sse_hk",
    "etf_prospectus_reference_text_szse_cross",
    "etf_prospectus_reference_text_szse_single",
    "etf_prospectus_reference_text_szse_hk",
    "etf_review_rules_xlsx",
    "etf_review_workbook_red_quality",
    "etf_review_workbook_linked_aviation",
    "etf_prospectus_materials_readme",
    "etf_prospectus_materials_index",
    "etf_prospectus_materials_source_mapping",
    "etf_prospectus_materials_function_mapping",
    "etf_prospectus_materials_shared_dependencies",
    "etf_prospectus_materials_editing_guide",
    "linked_contract_markdown",
    "linked_variable_definition_json",
    "linked_contract_diff_table",
    "linked_contract_clause_library",
    "linked_entry_table",
    "linked_prospectus_markdown",
    "linked_prospectus_clause_library",
    "linked_prospectus_diff_table",
    "linked_legal_templates_source_dir",
    "linked_legal_templates_packaged_dir",
    "linked_product_summary_templates_packaged_dir",
    "linked_contract_docx",
    "linked_prospectus_docx",
    "linked_product_summary_a_docx",
    "linked_product_summary_c_docx",
}


CRITICAL_VARIABLE_NAMES = {
    "FUND_NAME",
    "INDEX_NAME",
    "MANAGER_NAME",
    "MANAGER_ADDRESS",
    "MANAGER_LEGAL_REP",
    "MANAGER_INFO_VERSION",
    "COMPANY_WEBSITE",
    "FUND_MANAGER_WEBSITE",
    "SERVICE_HOTLINE",
    "FUND_MANAGER_HOTLINE",
    "CUSTODIAN_NAME",
    "CUSTODIAN_ADDRESS",
    "CUSTODIAN_HAS_OFFICE_ADDRESS",
    "CUSTODIAN_OFFICE_ADDRESS",
    "CUSTODIAN_LEGAL_REP",
    "CUSTODIAN_ESTABLISHED",
    "CUSTODIAN_APPROVAL_NO",
    "CUSTODIAN_REGISTERED_CAPITAL",
    "CUSTODIAN_ORG_FORM",
    "CUSTODIAN_CUSTODY_LICENSE",
    "CUSTODIAN_TYPE",
    "CUSTODIAN_INFO_VERSION",
    "CUSTODIAN_DEPT",
    "CUSTODIAN_PHONE",
    "CUSTODIAN_WEBSITE",
    "CUSTODIAN_INTRO",
    "CUSTODIAN_PROSPECTUS_TEXT",
    "FUND_MANAGER_NAME",
    "FUND_MANAGER_SEX",
    "FUND_MANAGER_BIO",
    "FUND_MANAGER_RESUME",
    "FUND_MANAGER_START_DATE",
    "FUND_MANAGER_SECURITIES_DATE",
    "SERVICE_ORGANIZATIONS_TEXT",
    "ACCOUNTING_FIRM_PROFILE",
    "LAW_FIRM_PROFILE",
}


class MaintenanceConfigTests(unittest.TestCase):
    def test_ensure_seed_configs_creates_required_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base_dir = Path(tmp)

            paths = maintenance_config.ensure_seed_configs(base_dir=base_dir)

            self.assertEqual(set(paths), set(maintenance_config.CONFIG_FILE_NAMES))
            for path in paths.values():
                self.assertTrue(path.exists(), path)

    def test_load_config_seeds_missing_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base_dir = Path(tmp)

            manifest = maintenance_config.load_config("template_manifest", base_dir=base_dir)

            self.assertIn("templates", manifest)
            self.assertTrue((base_dir / "config" / "template_manifest.json").exists())

    def test_save_config_writes_json_and_creates_backup(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base_dir = Path(tmp)
            maintenance_config.ensure_seed_configs(base_dir=base_dir)
            data = maintenance_config.load_config("publish_state", base_dir=base_dir)
            data["status"] = "DRAFT"

            path = maintenance_config.save_config("publish_state", data, base_dir=base_dir, backup=True)

            self.assertEqual(path, base_dir / "config" / "publish_state.json")
            self.assertEqual(maintenance_config.load_config("publish_state", base_dir=base_dir)["status"], "DRAFT")
            backups = list((base_dir / "backups" / "maintenance-admin").rglob("publish_state.json"))
            self.assertTrue(backups)

    def test_validate_config_rejects_missing_required_top_level_keys(self) -> None:
        errors = maintenance_config.validate_config("variable_registry", {"version": "1.0"})

        self.assertTrue(errors)
        self.assertIn("variables", errors[0])

    def test_template_manifest_seed_covers_known_runtime_assets(self) -> None:
        manifest = maintenance_config.seed_config("template_manifest")
        ids = {item["id"] for item in manifest["templates"]}

        self.assertFalse(CRITICAL_TEMPLATE_IDS - ids)

    def test_template_manifest_seed_has_unique_ids(self) -> None:
        manifest = maintenance_config.seed_config("template_manifest")
        ids = [item["id"] for item in manifest["templates"]]

        self.assertEqual(len(ids), len(set(ids)))

    def test_variable_registry_seed_covers_known_template_variables(self) -> None:
        registry = maintenance_config.seed_config("variable_registry")
        names = {item["name"] for item in registry["variables"]}

        self.assertFalse(CRITICAL_VARIABLE_NAMES - names)

    def test_variable_registry_seed_has_unique_names(self) -> None:
        registry = maintenance_config.seed_config("variable_registry")
        names = [item["name"] for item in registry["variables"]]

        self.assertEqual(len(names), len(set(names)))


if __name__ == "__main__":
    unittest.main()

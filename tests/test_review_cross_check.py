import sys
import tempfile
import unittest
from pathlib import Path

from openpyxl import Workbook


APP_DIR = Path(r"C:\Users\12534\Desktop\ETF合同知识库")
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

import app  # noqa: E402


class ReviewCrossCheckTests(unittest.TestCase):
    def setUp(self):
        self.client = app.app.test_client()
        self.original_rules_xlsx = app.RULES_XLSX
        self.original_candidates = list(app.REVIEW_XLSX_CANDIDATES)
        self.original_store = dict(app._review_store)
        app._review_store.clear()

    def tearDown(self):
        app.RULES_XLSX = self.original_rules_xlsx
        app.REVIEW_XLSX_CANDIDATES = list(self.original_candidates)
        app._review_store.clear()
        app._review_store.update(self.original_store)

    def _write_general_rules(self):
        wb = Workbook()
        ws = wb.active
        ws.append(("基金类型", "合同", "招募", "对应关系", "提示词", "内容完全一致", "内容有差异"))
        ws.append(("通用", "无关章节", "无关章节", "完全一致", "", "", ""))
        tmp = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
        tmp.close()
        wb.save(tmp.name)
        return Path(tmp.name)

    def _write_detail_rules(self, rows):
        wb = Workbook()
        ws = wb.active
        ws.title = "招募-合同(章节级)"
        ws.append(("招募说明书与基金合同——章节级勾稽关系", None, None, None, None, None, None))
        ws.append(("序号", "招募说明书位置", "对应基金合同位置", "关系类型", "一致性判断", "文本相似度(参考)", "详细勾稽说明"))
        for row in rows:
            ws.append(row)
        tmp = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
        tmp.close()
        wb.save(tmp.name)
        return Path(tmp.name)

    def _seed_review_store(self, contract_heading, contract_text, prospectus_heading, prospectus_text, rules_path):
        app._review_store["data"] = {
            "contract_text": contract_text,
            "contract_sections": [{"heading": contract_heading, "content": contract_text}],
            "contract_filename": "contract.docx",
            "prospectus_text": prospectus_text,
            "prospectus_sections": [{"heading": prospectus_heading, "content": prospectus_text}],
            "prospectus_filename": "prospectus.docx",
        }
        app._review_store["rules_xlsx_path"] = str(rules_path)

    def test_cross_check_prefers_uploaded_detail_rules_and_normalizes_known_self_references(self):
        general_rules = self._write_general_rules()
        detail_rules = self._write_detail_rules([
            (1, "释义", "第二部分释义", "直接对应", "基本一致", 0.996, "差异主要来自文件自指口径，属于个别表述差异。"),
        ])
        app.RULES_XLSX = general_rules
        app.REVIEW_XLSX_CANDIDATES = []

        contract_text = "\n".join([
            "在本基金合同中，除非文意另有所指，下列词语或简称具有如下含义：",
            "4、基金合同或本基金合同：指《示例基金合同》及对本基金合同的任何有效修订和补充",
            "6、招募说明书：指《示例招募说明书》及其更新",
            "67、不可抗力：指本基金合同当事人不能预见、不能避免且不能克服的客观事件。",
        ])
        prospectus_text = "\n".join([
            "在本招募说明书中，除非文意另有所指，下列词语或简称具有如下含义：",
            "4、基金合同：指《示例基金合同》及对基金合同的任何有效修订和补充",
            "6、招募说明书或本招募说明书：指《示例招募说明书》及其更新",
            "67、不可抗力：指基金合同当事人不能预见、不能避免且不能克服的客观事件。",
        ])
        self._seed_review_store("第二部分释义", contract_text, "释义", prospectus_text, detail_rules)

        response = self.client.post("/api/review/cross_check", json={"fund_type": "ETF"})

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["total_rules"], 1)
        result = payload["results"][0]
        self.assertEqual(result["status"], "pass")
        self.assertEqual(result["hunks"], [])
        self.assertIn("一致", result["message"])

    def test_cross_check_keeps_strict_diff_for_detail_rows_marked_fully_identical(self):
        general_rules = self._write_general_rules()
        detail_rules = self._write_detail_rules([
            (1, "基金合同的生效", "第五部分基金备案", "直接对应", "完全一致", 1.0, "应全文一致。"),
        ])
        app.RULES_XLSX = general_rules
        app.REVIEW_XLSX_CANDIDATES = []

        contract_text = "基金合同自获书面确认之日起生效。"
        prospectus_text = "基金合同自公告之日起生效。"
        self._seed_review_store("第五部分基金备案", contract_text, "基金合同的生效", prospectus_text, detail_rules)

        response = self.client.post("/api/review/cross_check", json={"fund_type": "ETF"})

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["total_rules"], 1)
        result = payload["results"][0]
        self.assertEqual(result["status"], "fail")
        self.assertTrue(result["hunks"])
        self.assertIn("差异", result["message"])


if __name__ == "__main__":
    unittest.main()

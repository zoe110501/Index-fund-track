import re
import tempfile
import unittest
import zipfile
from pathlib import Path

import app


class ProspectusEngineRoutingTests(unittest.TestCase):
    CHAPTER_TITLES = {
        "一": "绪言",
        "二": "释义",
        "三": "基金管理人",
        "四": "基金托管人",
        "五": "相关服务机构",
        "六": "基金的募集",
        "七": "基金合同的生效",
        "八": "基金份额折算与变更登记",
        "九": "基金份额的上市",
        "十": "基金份额的申购赎回",
        "十一": "基金的投资",
        "十二": "基金的财产",
        "十三": "基金资产估值",
        "十四": "基金的收益与分配",
        "十五": "基金的费用与税收",
        "十六": "基金的会计与审计",
        "十七": "基金的信息披露",
        "十八": "风险揭示",
        "十九": "基金合同的变更、终止和基金财产的清算",
        "二十": "基金合同的内容摘要",
        "二十一": "基金托管协议的内容摘要",
        "二十二": "基金份额持有人服务",
        "二十三": "其他应披露事项",
        "二十四": "招募说明书存放及其查阅方式",
        "二十五": "备查文件",
    }

    @classmethod
    def setUpClass(cls):
        cls.engine = app.ProspectusEngine()

    def build_form(self, **overrides):
        data = {
            "FUND_NAME": "测试交易型开放式指数证券投资基金",
            "INDEX_NAME": "测试指数",
            "EXCHANGE": "SSE",
            "MARKET_TYPE": "A_SHARE",
            "CUSTODIAN_NAME": "招商银行股份有限公司",
            "CONTRACT_DATE": "2026年1月1日",
            "FUND_MANAGER_NAME": "张三",
            "FUND_MANAGER_BIO": "张三，硕士，2020年起任基金经理。",
            "MIN_SUB_UNIT": "250,000份",
        }
        data.update(overrides)
        return data

    def chapter_heading(self, chapter_cn):
        return f"{chapter_cn}、{self.CHAPTER_TITLES[chapter_cn]}"

    def extract_chapter(self, text, chapter_cn):
        pattern = re.compile(rf"^第{chapter_cn}章[^\n]*", re.MULTILINE)
        matches = list(pattern.finditer(text))
        if matches:
            start = matches[-1].start()
            next_pattern = re.compile(r"^第[一二三四五六七八九十百]+章[^\n]*", re.MULTILINE)
            nxt = next_pattern.search(text, matches[-1].end())
            end = nxt.start() if nxt else len(text)
            return text[start:end].strip()

        lines = text.splitlines()
        heading = self.chapter_heading(chapter_cn)
        headings = {self.chapter_heading(cn) for cn in self.CHAPTER_TITLES}
        matches = [idx for idx, line in enumerate(lines) if line.strip() == heading]
        self.assertTrue(matches, f"missing chapter {chapter_cn}")
        start_idx = matches[-1]

        end_idx = len(lines)
        for idx in range(start_idx + 1, len(lines)):
            if lines[idx].strip() in headings:
                end_idx = idx
                break
        return "\n".join(lines[start_idx:end_idx]).strip()

    def extract_toc(self, text):
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        toc_idx = lines.index("目录")
        first_heading = self.chapter_heading("一")
        headings = []
        body_start = None
        for idx in range(toc_idx + 1, len(lines)):
            line = lines[idx]
            if line == first_heading and headings:
                body_start = idx
                break
            headings.append(line)
        self.assertIsNotNone(body_start, "missing formatted body start after TOC")
        return "\n".join(headings)

    def extract_preface_before_toc(self, text):
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        toc_idx = lines.index("\u76ee\u5f55")
        return "\n".join(lines[:toc_idx])

    def extract_section(self, chapter_text, section_cn):
        pattern = re.compile(rf"^{section_cn}、[^\n]*", re.MULTILINE)
        match = pattern.search(chapter_text)
        self.assertIsNotNone(match, f"missing section {section_cn}")
        next_pattern = re.compile(r"^[一二三四五六七八九十百]+、[^\n]*", re.MULTILINE)
        nxt = next_pattern.search(chapter_text, match.end())
        end = nxt.start() if nxt else len(chapter_text)
        return chapter_text[match.start():end].strip()

    def _docx_xml(self, docx_bytes):
        with tempfile.NamedTemporaryFile(suffix=".docx", delete=False) as tmp:
            tmp.write(docx_bytes)
            tmp.flush()
            with zipfile.ZipFile(tmp.name) as zf:
                return {
                    name: zf.read(name).decode("utf-8", errors="ignore")
                    for name in zf.namelist()
                    if name in {"word/document.xml", "word/settings.xml", "word/styles.xml", "word/numbering.xml"}
                    or name.startswith("word/footer")
                    or name.startswith("word/header")
                }

    def test_open_browser_helper_is_defined(self):
        self.assertTrue(callable(app.open_browser))

    def test_market_scope_defaults_to_single_for_kechuang(self):
        derived = self.engine._derive_variables(self.build_form(MARKET_TYPE="KECHUANG"))
        self.assertEqual(derived["MARKET_SCOPE"], "SINGLE_MARKET")

    def test_market_scope_defaults_to_cross_for_standard_a_share(self):
        derived = self.engine._derive_variables(self.build_form(MARKET_TYPE="A_SHARE"))
        self.assertEqual(derived["MARKET_SCOPE"], "CROSS_MARKET")

    def test_market_scope_override_is_preserved(self):
        derived = self.engine._derive_variables(self.build_form(MARKET_TYPE="A_SHARE", MARKET_SCOPE="SINGLE_MARKET"))
        self.assertEqual(derived["MARKET_SCOPE"], "SINGLE_MARKET")

    def test_product_type_defaults_to_etf(self):
        derived = self.engine._derive_variables(self.build_form())
        self.assertEqual(derived["PRODUCT_TYPE"], "ETF")

    def test_variant_key_uses_six_way_matrix(self):
        self.assertEqual(
            self.engine._get_prospectus_variant_key(self.build_form(EXCHANGE="SSE", MARKET_TYPE="KECHUANG")),
            "SSE_SINGLE",
        )
        self.assertEqual(
            self.engine._get_prospectus_variant_key(self.build_form(EXCHANGE="SSE", MARKET_TYPE="A_SHARE")),
            "SSE_CROSS",
        )
        self.assertEqual(
            self.engine._get_prospectus_variant_key(self.build_form(EXCHANGE="SZSE", MARKET_TYPE="CHUANGYE")),
            "SZSE_SINGLE",
        )
        self.assertEqual(
            self.engine._get_prospectus_variant_key(self.build_form(EXCHANGE="SZSE", MARKET_TYPE="A_SHARE")),
            "SZSE_CROSS",
        )
        self.assertEqual(
            self.engine._get_prospectus_variant_key(self.build_form(EXCHANGE="SSE", MARKET_TYPE="HK_CONNECT")),
            "SSE_HK",
        )
        self.assertEqual(
            self.engine._get_prospectus_variant_key(self.build_form(EXCHANGE="SZSE", MARKET_TYPE="HK_CONNECT")),
            "SZSE_HK",
        )

    def test_chapter_four_is_placeholder_only(self):
        text = self.engine.generate(self.build_form())
        chapter = self.extract_chapter(text, "四")
        self.assertEqual(chapter.splitlines()[1:], ["【托管人情况待填写】"])

    def test_chapter_twenty_one_keeps_titles_only(self):
        text = self.engine.generate(self.build_form())
        chapter = self.extract_chapter(text, "二十一")
        self.assertIn("一、基金托管协议当事人", chapter)
        self.assertIn("二、基金托管人对基金管理人的业务监督和核查", chapter)
        self.assertIn("三、基金管理人对基金托管人的业务核查", chapter)
        self.assertNotIn("[待填写", chapter)

    def test_chapter_ten_uses_frontend_min_sub_unit_value(self):
        text = self.engine.generate(self.build_form(MIN_SUB_UNIT="123,456份"))
        chapter = self.extract_chapter(text, "十")
        sec5 = self.extract_section(chapter, "五")
        self.assertIn("123,456份", sec5)

    def test_chapter_ten_uses_default_min_sub_unit_when_missing(self):
        form = self.build_form()
        form.pop("MIN_SUB_UNIT")
        text = self.engine.generate(form)
        chapter = self.extract_chapter(text, "十")
        sec5 = self.extract_section(chapter, "五")
        self.assertIn("1,000,000份（即100万份）", sec5)

        single_text = self.engine.generate(self.build_form(EXCHANGE="SSE", MARKET_TYPE="KECHUANG"))
        cross_text = self.engine.generate(self.build_form(EXCHANGE="SSE", MARKET_TYPE="A_SHARE"))
        single_ch6 = self.extract_section(self.extract_chapter(single_text, "六"), "七")
        cross_ch6 = self.extract_section(self.extract_chapter(cross_text, "六"), "七")
        self.assertNotEqual(single_ch6, cross_ch6)
        single_ch9 = self.extract_chapter(single_text, "九")
        cross_ch9 = self.extract_chapter(cross_text, "九")
        self.assertNotEqual(single_ch9, cross_ch9)
        single_ch10 = self.extract_section(self.extract_chapter(single_text, "十"), "四")
        cross_ch10 = self.extract_section(self.extract_chapter(cross_text, "十"), "四")
        self.assertNotEqual(single_ch10, cross_ch10)

    def test_risk_disclosure_uses_market_specific_text(self):
        hk_text = self.engine.generate(self.build_form(EXCHANGE="SSE", MARKET_TYPE="HK_CONNECT"))
        kechuang_text = self.engine.generate(self.build_form(EXCHANGE="SSE", MARKET_TYPE="KECHUANG"))
        chuangye_text = self.engine.generate(self.build_form(EXCHANGE="SZSE", MARKET_TYPE="CHUANGYE"))
        a_share_text = self.engine.generate(self.build_form(EXCHANGE="SZSE", MARKET_TYPE="A_SHARE"))

        self.assertIn("港股通机制下", self.extract_chapter(hk_text, "十八"))
        self.assertIn("科创板机制下", self.extract_chapter(kechuang_text, "十八"))
        self.assertIn("创业板机制下", self.extract_chapter(chuangye_text, "十八"))
        self.assertIn("本基金属于股票型基金", self.extract_chapter(a_share_text, "十八"))

    def test_definitions_reuse_contract_part_two_verbatim(self):
        form = self.build_form()
        variables = self.engine._extract_contract_sections(form)
        text = self.engine.generate(form)
        chapter = self.extract_chapter(text, "二")
        chapter_body = chapter.splitlines()[1:]
        while chapter_body and not chapter_body[0].strip():
            chapter_body.pop(0)
        self.assertEqual("\n".join(chapter_body).strip(), variables["CONTRACT_DEFS_TEXT"].strip())
        self.assertIn("在本基金合同中", chapter)
        self.assertNotIn("在本招募说明书中", chapter)
        self.assertIn("6、招募说明书：指《测试交易型开放式指数证券投资基金招募说明书》及其更新", chapter)

    def test_generated_prospectus_drops_template_metadata_block(self):
        text = self.engine.generate(self.build_form())
        self.assertTrue(text.startswith("测试交易型开放式指数证券投资基金招募说明书"))
        self.assertNotIn("VALUATION_TIMING_CLAUSE", text)
        self.assertNotIn("模板说明", text)
        self.assertNotIn("条件变量引用说明", text)
        self.assertNotIn("\n---\n", text)

    def test_generated_prospectus_uses_reference_style_toc_and_body_titles(self):
        text = self.engine.generate(self.build_form())
        toc = self.extract_toc(text)

        self.assertIn("一、绪言", toc)
        self.assertIn("四、基金托管人", toc)
        self.assertIn("二十一、基金托管协议的内容摘要", toc)
        self.assertNotIn("一、基金托管协议当事人", toc)
        self.assertNotIn("【托管人情况待填写】", toc)
        self.assertNotIn("第一章  绪言", text)
        self.assertIn("\n一、绪言\n", text)

    def test_generate_render_model_keeps_chapter_titles_unumbered(self):
        model = self.engine.generate_render_model(self.build_form())

        self.assertEqual(model["chapters"][0]["title"], "绪言")
        self.assertEqual(model["chapters"][1]["title"], "释义")
        self.assertNotIn("、", model["chapters"][0]["title"])
        self.assertEqual(model["toc_title"], "目录")
        self.assertEqual(model["toc_titles"][0], "绪言")

    def test_generate_prospectus_api_includes_render_model(self):
        client = app.app.test_client()
        response = client.post("/api/generate_prospectus", json=self.build_form())

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["success"])
        self.assertIn("render_model", payload)
        self.assertEqual(payload["render_model"]["chapters"][0]["title"], "绪言")
        self.assertIn("一、绪言", payload["text"])
    def test_generated_prospectus_puts_variant_specific_important_notice_before_toc(self):
        standard_text = self.engine.generate(self.build_form(EXCHANGE="SSE", MARKET_TYPE="A_SHARE"))
        kechuang_text = self.engine.generate(self.build_form(EXCHANGE="SSE", MARKET_TYPE="KECHUANG"))
        chuangye_text = self.engine.generate(self.build_form(EXCHANGE="SZSE", MARKET_TYPE="CHUANGYE"))
        hk_text = self.engine.generate(self.build_form(EXCHANGE="SSE", MARKET_TYPE="HK_CONNECT"))

        standard_preface = self.extract_preface_before_toc(standard_text)
        kechuang_preface = self.extract_preface_before_toc(kechuang_text)
        chuangye_preface = self.extract_preface_before_toc(chuangye_text)
        hk_preface = self.extract_preface_before_toc(hk_text)

        self.assertIn("\u91cd\u8981\u63d0\u793a", standard_preface)
        self.assertIn("\u91cd\u8981\u63d0\u793a", kechuang_preface)
        self.assertIn("\u91cd\u8981\u63d0\u793a", chuangye_preface)
        self.assertIn("\u91cd\u8981\u63d0\u793a", hk_preface)
        self.assertIn("\u672c\u57fa\u91d1\u6807\u7684\u6307\u6570\u4e3a\u4e2d\u8bc1\u5168\u6307\u7ea2\u5229\u8d28\u91cf\u6307\u6570", standard_preface)
        self.assertIn("\u672c\u57fa\u91d1\u6295\u8d44\u4e8e\u79d1\u521b\u677f\u80a1\u7968\uff0c\u4f1a\u9762\u4e34\u79d1\u521b\u677f\u673a\u5236\u4e0b", kechuang_preface)
        self.assertIn("\u672c\u57fa\u91d1\u6295\u8d44\u521b\u4e1a\u677f\u80a1\u7968\uff0c\u4f1a\u9762\u4e34\u521b\u4e1a\u677f\u673a\u5236\u4e0b", chuangye_preface)
        self.assertIn("\u672c\u57fa\u91d1\u8d44\u4ea7\u6295\u8d44\u4e8e\u6e2f\u80a1\uff0c\u4f1a\u9762\u4e34\u6e2f\u80a1\u901a\u673a\u5236\u4e0b", hk_preface)

    def test_reused_chapters_drop_contract_wording(self):
        text = self.engine.generate(self.build_form())
        invest = self.extract_chapter(text, "十一")
        valuation = self.extract_chapter(text, "十三")
        fees = self.extract_chapter(text, "十五")
        self.assertNotIn("本基金合同", invest)
        self.assertNotIn("本基金合同", valuation)
        self.assertNotIn("本基金合同", fees)

    def test_reused_chapter_normalizer_fixes_side_pocket_reference(self):
        normalized = self.engine._normalize_reused_prospectus_chapter(
            "实施侧袋机制期间的投资运作安排，详见招募说明书的规定。基金管理人根据本基金合同履行信息披露义务。"
        )
        self.assertIn("详见招募说明书“侧袋机制”部分的规定", normalized)
        self.assertNotIn("详见招募说明书的规定", normalized)
        self.assertNotIn("本基金合同", normalized)

    def test_risk_chapter_normalizer_uses_frontend_min_sub_unit_value(self):
        normalized = self.engine._normalize_prospectus_risk_chapter(
            "投资人按原1,000,000份申购并持有的基金份额，可能无法按照新的1,000,000份全部赎回。",
            self.build_form(MIN_SUB_UNIT="123,456份"),
        )
        self.assertIn("123,456份", normalized)
        self.assertNotIn("1,000,000份", normalized)

    def test_etf_chapter_ten_keeps_prospectus_only_sections(self):
        text = self.engine.generate(self.build_form())
        chapter = self.extract_chapter(text, "十")
        self.assertIn("四、申购和赎回的程序", chapter)
        self.assertIn("五、申购和赎回的数额限制", chapter)
        self.assertIn("对价、费用及其用途", chapter)
        self.assertIn("七、申购赎回清单的内容与格式", chapter)

    def test_etf_chapter_ten_reuses_reference_limits_text_without_markdown_table(self):
        text = self.engine.generate(self.build_form())
        chapter = self.extract_chapter(text, "\u5341")
        sec5 = self.extract_section(chapter, "\u4e94")
        self.assertIn("\u5f53\u63a5\u53d7\u7533\u8d2d\u7533\u8bf7\u5bf9\u5b58\u91cf\u57fa\u91d1\u4efd\u989d\u6301\u6709\u4eba\u5229\u76ca\u6784\u6210\u6f5c\u5728\u91cd\u5927\u4e0d\u5229\u5f71\u54cd\u65f6", sec5)
        self.assertIn("250,000\u4efd", sec5)
        self.assertNotIn("|\u9879\u76ee|\u5185\u5bb9|", sec5)
        self.assertNotIn("|---|---|", sec5)

    def test_etf_chapter_ten_sec7_keeps_reference_format_description(self):
        text = self.engine.generate(self.build_form(EXCHANGE="SSE", MARKET_TYPE="A_SHARE"))
        chapter = self.extract_chapter(text, "\u5341")
        sec7 = self.extract_section(chapter, "\u4e03")
        self.assertIn("T\u65e5\u7533\u8d2d\u8d4e\u56de\u6e05\u5355\u7684\u683c\u5f0f\u4e3e\u4f8b\u5982\u4e0b\uff1a", sec7)
        self.assertIn("\u57fa\u672c\u4fe1\u606f", sec7)
        self.assertIn("T-1\u65e5\u4fe1\u606f\u5185\u5bb9", sec7)
        self.assertIn("T\u65e5\u4fe1\u606f\u5185\u5bb9", sec7)
        self.assertIn("\u6210\u4efd\u80a1\u4fe1\u606f\u5185\u5bb9", sec7)
        self.assertNotIn("\u57fa\u672c\u4fe1\u606f/T-1\u65e5\u4fe1\u606f\u5185\u5bb9/T\u65e5\u4fe1\u606f\u5185\u5bb9/\u6210\u4efd\u80a1\u4fe1\u606f\u5185\u5bb9", sec7)

    def test_chapter_six_reuses_reference_stock_subscription_formula(self):
        text = self.engine.generate(self.build_form(EXCHANGE="SSE", MARKET_TYPE="A_SHARE", HAS_STOCK_SUBSCRIPTION=True))
        chapter = self.extract_chapter(text, "\u516d")
        sec11 = self.extract_section(chapter, "\u5341\u4e00")
        self.assertIn("\u7b2ci\u53ea\u80a1\u7968\u8ba4\u8d2d\u671f\u6700\u540e\u4e00\u65e5\u7684\u5747\u4ef7", sec11)
        self.assertIn("\u9664\u606f\u4e14\u9001\u80a1", sec11)
        self.assertIn("\u9664\u606f\u4e14\u914d\u80a1", sec11)

    def test_contract_summary_excludes_signing_page_content(self):
        variables = self.engine._extract_contract_sections(self.build_form())
        summary = variables["CONTRACT_SUMMARY_TEXT"]
        self.assertNotIn("签署页", summary)
        self.assertNotIn("本页无正文", summary)
        self.assertNotIn("（盖章）", summary)
        self.assertNotIn("（签字或盖章）", summary)
        self.assertNotIn("（签名）", summary)

    def test_etf_chapter_fourteen_keeps_distribution_conditions(self):
        text = self.engine.generate(self.build_form())
        chapter = self.extract_chapter(text, "十四")
        self.assertIn("四、收益分配条件", chapter)

    def test_contract_docx_has_body_page_numbers_starting_at_one(self):
        contract_text = app.engine.generate(self.build_form())
        xml_map = self._docx_xml(app.engine.build_docx(contract_text))
        document_xml = xml_map["word/document.xml"]
        footer_xml = "".join(v for k, v in xml_map.items() if k.startswith("word/footer"))

        self.assertIn('w:start="1"', document_xml)
        self.assertIn("<w:titlePg", document_xml)
        self.assertIn("PAGE", footer_xml)
        self.assertIn('w:jc w:val="center"', footer_xml)

    def test_prospectus_docx_has_body_page_numbers_starting_at_one(self):
        prospectus_text = app.prospectus_engine.generate(self.build_form())
        xml_map = self._docx_xml(app.prospectus_engine.build_docx_prospectus(prospectus_text))
        document_xml = xml_map["word/document.xml"]
        footer_xml = "".join(v for k, v in xml_map.items() if k.startswith("word/footer"))
        header_xml = "".join(v for k, v in xml_map.items() if k.startswith("word/header"))
        settings_xml = xml_map["word/settings.xml"]

        self.assertEqual(document_xml.count("<w:sectPr"), 1)
        self.assertIn('w:start="0"', document_xml)
        self.assertIn("<w:titlePg", document_xml)
        self.assertIn('w:top="1440"', document_xml)
        self.assertIn('w:bottom="1440"', document_xml)
        self.assertIn('w:left="1800"', document_xml)
        self.assertIn('w:right="1800"', document_xml)
        self.assertIn('w:header="851"', document_xml)
        self.assertIn('w:footer="992"', document_xml)
        self.assertIn("PAGE", footer_xml)
        self.assertIn('w:jc w:val="center"', footer_xml)
        self.assertIn("\u6d4b\u8bd5\u4ea4\u6613\u578b\u5f00\u653e\u5f0f\u6307\u6570\u8bc1\u5238\u6295\u8d44\u57fa\u91d1\u62db\u52df\u8bf4\u660e\u4e66", header_xml)
        self.assertIn('w:jc w:val="right"', header_xml)
        self.assertIn('w:pBdr', xml_map["word/styles.xml"])
        self.assertIn('w:val="single"', header_xml)
        self.assertIn('w:val="18"', footer_xml)
        self.assertIn("updateFields", settings_xml)

    def test_prospectus_docx_formats_important_notice_and_chapter_titles(self):
        prospectus_text = app.prospectus_engine.generate(self.build_form())
        xml_map = self._docx_xml(app.prospectus_engine.build_docx_prospectus(prospectus_text))
        document_xml = xml_map["word/document.xml"]
        styles_xml = xml_map["word/styles.xml"]

        self.assertRegex(document_xml, r"w:jc w:val=\"center\".{0,500}\u91cd\u8981\u63d0\u793a")
        self.assertIn("<w:t>绪言</w:t>", document_xml)
        self.assertIn('w:pStyle w:val="3"', document_xml)
        self.assertNotIn("<w:t>一、绪言</w:t>", document_xml)
        self.assertIn('w:styleId="3"', styles_xml)
        self.assertIn('w:eastAsia="黑体"', styles_xml)
        self.assertIn('w:sz w:val="32"', styles_xml)
    def test_prospectus_docx_uses_automatic_toc_field_for_top_level_headings(self):
        prospectus_text = app.prospectus_engine.generate(self.build_form())
        document_xml = self._docx_xml(app.prospectus_engine.build_docx_prospectus(prospectus_text))["word/document.xml"]

        self.assertIn('TOC \\o "1-3" \\h \\z \\u', document_xml)
        self.assertIn("更新目录后显示页码", document_xml)
        self.assertIn("<w:t>目录</w:t>", document_xml)
    def test_prospectus_docx_toc_excludes_placeholder_and_chapter_twenty_one_subitems(self):
        prospectus_text = app.prospectus_engine.generate(self.build_form())
        document_xml = self._docx_xml(app.prospectus_engine.build_docx_prospectus(prospectus_text))["word/document.xml"]

        pre_body_xml = document_xml[:document_xml.index("\u7eea\u8a00")]
        self.assertNotIn("【托管人情况待填写】", pre_body_xml)
        self.assertNotIn("基金托管协议当事人", pre_body_xml)
        self.assertNotIn("基金管理人对基金托管人的业务核查", pre_body_xml)

    def test_prospectus_docx_body_uses_reference_style_chapter_headings(self):
        prospectus_text = app.prospectus_engine.generate(self.build_form())
        xml_map = self._docx_xml(app.prospectus_engine.build_docx_prospectus(prospectus_text))
        document_xml = xml_map["word/document.xml"]
        numbering_xml = xml_map["word/numbering.xml"]

        self.assertIn(">绪言<", document_xml)
        self.assertIn(">基金托管协议的内容摘要<", document_xml)
        self.assertIn("一、基金托管协议当事人", document_xml)
        self.assertIn('w:abstractNumId w:val="1"', numbering_xml)
        self.assertIn("upperLetter", numbering_xml)
    def test_prospectus_export_validation_detects_metadata_leak(self):
        report = self.engine.validate_exportable_text(
            "VALUATION_TIMING_CLAUSE：见 06_招募说明书差异条款库.json\n---\n正式正文"
        )
        self.assertFalse(report["ok"])
        self.assertEqual(report["error_type"], "template_metadata_leaked")
        self.assertIn("VALUATION_TIMING_CLAUSE", "\n".join(report["matches"]))

    def test_prospectus_export_validation_allows_unresolved_placeholders(self):
        report = self.engine.validate_exportable_text(
            "测试招募说明书\n【待填写】\n[待填写：律师事务所名称]\n{CUSTODIAN_NAME}\n{{PLACEHOLDER}}"
        )
        self.assertTrue(report["ok"])
        self.assertEqual(report["matches"], [])

    def test_export_prospectus_docx_allows_incomplete_text(self):
        client = app.app.test_client()
        response = client.post("/api/export_prospectus_docx", json=self.build_form())
        self.assertEqual(response.status_code, 200)
        self.assertIn("application/vnd.openxmlformats-officedocument.wordprocessingml.document", response.content_type)

    def test_export_prospectus_txt_allows_incomplete_text(self):
        client = app.app.test_client()
        response = client.post("/api/export_prospectus_txt", json=self.build_form())
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/plain", response.content_type)
        self.assertIn("\u4e00\u3001\u7eea\u8a00", response.get_data(as_text=True))

    def test_prospectus_exports_keep_dynamic_chinese_fields_readable(self):
        client = app.app.test_client()
        form = self.build_form(
            FUND_NAME="测试交易型开放式指数证券投资基金",
            CUSTODIAN_NAME="招商银行股份有限公司",
            MIN_SUB_UNIT="123,456份",
        )

        txt_response = client.post("/api/export_prospectus_txt", json=form)
        self.assertEqual(txt_response.status_code, 200)
        txt_text = txt_response.get_data(as_text=True)
        self.assertIn(form["FUND_NAME"], txt_text)
        self.assertIn(form["CUSTODIAN_NAME"], txt_text)
        self.assertIn(form["MIN_SUB_UNIT"], txt_text)
        self.assertNotIn("æµ", txt_text)
        self.assertNotIn("æ", txt_text)

        docx_response = client.post("/api/export_prospectus_docx", json=form)
        self.assertEqual(docx_response.status_code, 200)
        document_xml = self._docx_xml(docx_response.data)["word/document.xml"]
        self.assertIn(form["FUND_NAME"], document_xml)
        self.assertIn(form["CUSTODIAN_NAME"], document_xml)
        self.assertIn(form["MIN_SUB_UNIT"], document_xml)
        self.assertNotIn("æµ", document_xml)
        self.assertNotIn("æ", document_xml)

    def test_index_template_exposes_dedicated_prospectus_sidebar_entry(self):
        html = Path(app.BASE_DIR / "templates" / "index.html").read_text(encoding="utf-8")
        self.assertIn('data-panel="prospectus"', html)
        self.assertIn('id="prospectus-panel"', html)
        self.assertIn("招募说明书", html)

    def test_clean_prospectus_docx_does_not_include_placeholder_markers(self):
        clean_text = "\n".join([
            "测试交易型开放式指数证券投资基金招募说明书",
            "基金管理人：南方基金管理股份有限公司",
            "基金托管人：招商银行股份有限公司",
            "2026年1月1日",
            "目录",
            "一、绪言",
            "一、绪言",
            "本招募说明书用于测试导出。",
        ])
        xml_map = self._docx_xml(app.prospectus_engine.build_docx_prospectus(clean_text))
        document_xml = xml_map["word/document.xml"]

        self.assertNotIn("VALUATION_TIMING_CLAUSE", document_xml)
        self.assertNotIn("【待填写】", document_xml)
        self.assertNotIn("[待填写", document_xml)
        self.assertNotIn("{CUSTODIAN_NAME}", document_xml)

if __name__ == "__main__":
    unittest.main()

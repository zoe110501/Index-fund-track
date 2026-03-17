from pathlib import Path

path = Path(r'D:/codex/app_work.py')
text = path.read_text(encoding='utf-8')

block1 = '''    # ── Step 1: 派生变量（委托给 ContractEngine）────────────────────────────
    def _derive_variables(self, v: dict) -> dict:
        v = engine._derive_variables(v)
        market_scope = str(v.get("MARKET_SCOPE", "") or "").strip().upper()
        if market_scope not in {"SINGLE_MARKET", "CROSS_MARKET"}:
            market_type = str(v.get("MARKET_TYPE", "") or "").strip().upper()
            if market_type in {"KECHUANG", "CHUANGYE"}:
                market_scope = "SINGLE_MARKET"
            else:
                market_scope = "CROSS_MARKET"
        v["MARKET_SCOPE"] = market_scope
        return v

    def _get_prospectus_variant_key(self, v: dict) -> str:
        v = self._derive_variables(v)
        exchange = v.get("EXCHANGE", "")
        has_hk = bool(v.get("HAS_HK_CONNECT"))
        market_scope = v.get("MARKET_SCOPE", "CROSS_MARKET")

        if has_hk:
            return "SSE_HK" if exchange == "SSE" else "SZSE_HK"
        if exchange == "SSE":
            return "SSE_SINGLE" if market_scope == "SINGLE_MARKET" else "SSE_CROSS"
        return "SZSE_SINGLE" if market_scope == "SINGLE_MARKET" else "SZSE_CROSS"

    def _get_variant_clause_bundle(self, v: dict) -> dict:
        variants = self.pro_clauses.get("PROSPECTUS_VARIANTS", {}).get("variants", {})
        key = self._get_prospectus_variant_key(v)
        return variants.get(key, variants.get("SSE_CROSS", {}))

'''

block2 = '''    # ── Step 2: 注入差异条款（合同条款 + 招募说明书专有条款）──────────────
    def _inject_clause_texts(self, v: dict) -> dict:
        v = self._derive_variables(v)
        v = engine._inject_clause_texts(v)
        has_hk = v.get("HAS_HK_CONNECT", False)
        exch_cn = v.get("EXCHANGE_NAME_CN", "证券交易所")
        variant_key = self._get_prospectus_variant_key(v)
        variant_bundle = self._get_variant_clause_bundle(v)

        vt_key = "HK_CONNECT" if has_hk else "STANDARD"
        vt_variants = self.pro_clauses["VALUATION_TIMING"]["variants"]
        valuation_text = vt_variants.get(vt_key, vt_variants["STANDARD"])["text"]
        valuation_text = valuation_text.replace("{EXCHANGE_NAME_CN}", exch_cn)
        v["VALUATION_TIMING_CLAUSE"] = valuation_text

        v["RISK_DISCLOSURE_CHUANGYE"] = self.pro_clauses["RISK_DISCLOSURE_CHUANGYE"]["variants"]["DEFAULT"]["text"]
        v["RISK_DISCLOSURE_KECHUANG"] = self.pro_clauses["RISK_DISCLOSURE_KECHUANG"]["variants"]["DEFAULT"]["text"]
        v["RISK_DISCLOSURE_HK_CONNECT"] = self.pro_clauses["RISK_DISCLOSURE_HK_CONNECT"]["variants"]["DEFAULT"]["text"]

        custodian_name = v.get("CUSTODIAN_NAME", "")
        custodian_contacts = self.pro_clauses.get("CUSTODIAN_INFO_PROSPECTUS", {}).get("custodians", {})
        info = custodian_contacts.get(custodian_name, {})
        v.setdefault("CUSTODIAN_DEPT", info.get("dept", "[待填写：托管部门名称]"))
        v.setdefault("CUSTODIAN_PHONE", info.get("phone", "[待填写：服务电话]"))
        v.setdefault("CUSTODIAN_WEBSITE", info.get("website", "[待填写：网址]"))

        v.setdefault("FUND_MANAGER_NAME", "[待填写：基金经理姓名]")
        v.setdefault("FUND_MANAGER_BIO", "[待填写：基金经理简介（学历、从业经历、任职日期等）]")
        v.setdefault("CSRC_APPROVAL_NO", "202X年X月X日证监许可〔202X〕XXXX号")
        v.setdefault("MIN_SUB_UNIT", "1,000,000份（即100万份）")

        chapter6 = variant_bundle.get("chapter_6", {})
        chapter10 = variant_bundle.get("chapter_10", {})
        v["PROSPECTUS_VARIANT_KEY"] = variant_key
        v["PROSPECTUS_CH6_SEC4"] = chapter6.get("section_4", "")
        v["PROSPECTUS_CH6_SEC7"] = chapter6.get("section_7", "")
        v["PROSPECTUS_CH6_SEC11"] = chapter6.get("section_11", "")
        v["PROSPECTUS_CH6_SEC12"] = chapter6.get("section_12", "")
        v["PROSPECTUS_CH6_SEC13"] = chapter6.get("section_13", "")
        v["PROSPECTUS_CH7_BODY"] = variant_bundle.get("chapter_7", "")
        v["PROSPECTUS_CH9_BODY"] = variant_bundle.get("chapter_9", "")
        v["PROSPECTUS_CH10_PRELUDE"] = chapter10.get("prelude", "")
        v["PROSPECTUS_CH10_SEC4"] = chapter10.get("section_4", "")
        v["PROSPECTUS_CH10_SEC7"] = chapter10.get("section_7", "")
        v["PROSPECTUS_CH21_TITLES"] = self.pro_clauses.get("CHAPTER21_TITLES", {}).get("text", "")

        risk_bodies = self.pro_clauses.get("RISK_CHAPTER_BODIES", {}).get("variants", {})
        if has_hk:
            v["PROSPECTUS_CH18_BODY"] = risk_bodies.get("HK_CONNECT", "")
        elif v.get("IS_KECHUANG"):
            v["PROSPECTUS_CH18_BODY"] = risk_bodies.get("KECHUANG", "")
        elif v.get("IS_CHUANGYE"):
            v["PROSPECTUS_CH18_BODY"] = risk_bodies.get("CHUANGYE", "")
        else:
            v["PROSPECTUS_CH18_BODY"] = risk_bodies.get("STANDARD_A", "")

        return v

'''

start1 = text.index('    # ── Step 1: 派生变量（委托给 ContractEngine）────────────────────────────')
end1 = text.index('    # ── Step 3: 从合同全文提取各关键部分（内容摘要 + 各章节来源段落）───────')
text = text[:start1] + block1 + block2 + text[end1:]

block3 = '''    def _apply_reference_fixed_content(self, text: str, v: dict) -> str:
        """
        Apply canonical fixed text from red-dividend prospectus docx.
        """
        ref = self._load_reference_fixed_content()
        if not ref:
            return text

        def ref_chapter(chap_cn: str) -> str:
            return ref.get(chap_cn, {}).get("body", "")

        def ref_section(chap_cn: str, sec_cn: str) -> str:
            return ref.get(chap_cn, {}).get("sections", {}).get(sec_cn, "")

        ch3 = ref_chapter("三")
        if ch3:
            manager_name = str(v.get("FUND_MANAGER_NAME") or "[待填写：基金经理姓名]").strip()
            manager_bio = str(v.get("FUND_MANAGER_BIO") or "[待填写：基金经理简介]").strip()
            sec_item3 = f"3、本基金的基金经理为{manager_name}。"
            if manager_bio:
                sec_item3 = f"{sec_item3}\n{manager_bio}"
            ch3 = self._replace_numbered_item_in_section(ch3, "二", "3", sec_item3)
            text = self._replace_chapter_body(text, "三", ch3)

        text = self._replace_chapter_body(text, "四", "【托管人情况待填写】")

        text = self._replace_subsection_in_chapter(text, "五", "一", ref_section("五", "一"))
        text = self._replace_subsection_in_chapter(text, "五", "二", ref_section("五", "二"))
        sec3 = ref_section("五", "三")
        if sec3:
            sec3 = re.sub(r"^经办律师[：:].*$", "经办律师：丁媛、李晓露", sec3, flags=re.MULTILINE)
        text = self._replace_subsection_in_chapter(text, "五", "三", sec3)
        text = self._replace_subsection_in_chapter(text, "五", "四", "四、审计基金财产的会计师事务所\n【待填写】")

        text = self._replace_chapter_body(text, "八", ref_chapter("八"))
        text = self._replace_chapter_body(text, "十四", ref_chapter("十四"))

        for chap_cn in ("二十二", "二十三", "二十四"):
            text = self._replace_chapter_body(text, chap_cn, ref_chapter(chap_cn))

        return text

'''
start3 = text.index('    def _apply_reference_fixed_content(self, text: str, v: dict) -> str:')
end3 = text.index('    @staticmethod\n    def _find_chapter_span')
text = text[:start3] + block3 + text[end3:]

block4 = '''    @staticmethod
    def _join_nonempty_blocks(blocks) -> str:
        return "\n\n".join(block.strip() for block in blocks if (block or "").strip())

    def _build_chapter_six_body(self, v: dict, ref: dict) -> str:
        def ref_section(sec_cn: str) -> str:
            return ref.get("六", {}).get("sections", {}).get(sec_cn, "")

        sec8 = """八、认购费用
认购费用由投资人承担，不高于0.30%，认购费率如下表所示：

|   |   |
|---|---|
|认购份额（S）|认购费率|
|S＜100万份|0.30%|
|S≥100万份|每笔500元|

基金管理人办理网下现金认购和网下股票认购不收取认购费。发售代理机构办理网上现金认购、网下现金认购、网下股票认购时可参照上述费率结构，按照不高于0.3%的标准收取一定的佣金。投资人申请重复现金认购的，须按每次认购所对应的费率档次分别计费。"""

        blocks = [
            f"本基金由基金管理人依照《基金法》、《运作办法》、《销售办法》、基金合同及其他有关规定，并经中国证监会{v.get('CSRC_APPROVAL_NO', '202X年X月X日证监许可〔202X〕XXXX号')}文注册募集。",
            "本基金为交易型开放式基金，股票型基金，基金存续期限为不定期。",
            ref_section("一"),
            ref_section("二"),
            ref_section("三"),
            v.get("PROSPECTUS_CH6_SEC4", ""),
            ref_section("五"),
            ref_section("六"),
            v.get("PROSPECTUS_CH6_SEC7", ""),
            sec8,
            ref_section("九"),
            ref_section("十"),
            v.get("PROSPECTUS_CH6_SEC11", ""),
            v.get("PROSPECTUS_CH6_SEC12", ""),
            v.get("PROSPECTUS_CH6_SEC13", ""),
        ]
        return self._join_nonempty_blocks(blocks)

    def _build_chapter_ten_sec5(self, ref: dict, v: dict) -> str:
        sec5 = ref.get("十", {}).get("sections", {}).get("五", "")
        min_sub_unit = str(v.get("MIN_SUB_UNIT") or "1,000,000份（即100万份）").strip()
        if sec5:
            sec5 = re.sub(r"目前，本基金最小申购赎回单位为[^，。]+", f"目前，本基金最小申购赎回单位为{min_sub_unit}", sec5, count=1)
            if min_sub_unit not in sec5:
                sec5 = re.sub(r"最小申购赎回单位[^。]*。", f"最小申购赎回单位为{min_sub_unit}。", sec5, count=1)
            return sec5
        return f"""五、申购和赎回的数额限制
1、投资人申购、赎回的基金份额需为最小申购赎回单位的整数倍。目前，本基金最小申购赎回单位为{min_sub_unit}，基金管理人有权对其进行调整，并在调整实施前依照《信息披露办法》的有关规定在规定媒介上公告。
2、基金管理人可以规定本基金当日申购份额及当日赎回份额上限，具体规定请参见申购赎回清单或相关公告。
3、基金管理人可根据市场情况，在法律法规允许的情况下，合理调整上述申购和赎回的数量或比例限制，并在实施前依照《信息披露办法》的有关规定在规定媒介上公告。"""

    def _build_contract_section(self, v: dict, var_name: str, sec_cn: str) -> str:
        sec_text = self._retag_subsection_number(v.get(var_name, ""), sec_cn)
        return self._ensure_subsection_heading(sec_text, sec_cn)

    def _build_chapter_ten_body(self, v: dict, ref: dict) -> str:
        blocks = []
        prelude = v.get("PROSPECTUS_CH10_PRELUDE", "")
        if prelude:
            blocks.append(prelude)
        blocks.extend([
            ref.get("十", {}).get("sections", {}).get("一", ""),
            self._build_contract_section(v, "CONTRACT_PART8_SEC2", "二"),
            self._build_contract_section(v, "CONTRACT_PART8_SEC3", "三"),
            v.get("PROSPECTUS_CH10_SEC4", ""),
            self._build_chapter_ten_sec5(ref, v),
            self._build_contract_section(v, "CONTRACT_PART8_SEC6", "六"),
            v.get("PROSPECTUS_CH10_SEC7", ""),
            self._build_contract_section(v, "CONTRACT_PART8_SEC7", "八"),
            self._build_contract_section(v, "CONTRACT_PART8_SEC8", "九"),
            self._build_contract_section(v, "CONTRACT_PART8_SEC9", "十"),
            self._build_contract_section(v, "CONTRACT_PART8_SEC10", "十一"),
            self._build_contract_section(v, "CONTRACT_PART8_SEC11", "十二"),
            self._build_contract_section(v, "CONTRACT_PART8_SEC12", "十三"),
            self._build_contract_section(v, "CONTRACT_PART8_SEC13", "十四"),
            self._build_contract_section(v, "CONTRACT_PART8_SEC14", "十五"),
        ])
        return self._join_nonempty_blocks(blocks)

    def _apply_prospectus_chapter_logic(self, text: str, v: dict) -> str:
        """
        Apply chapter-level composition rules for prospectus generation.
        """
        text = self._apply_reference_fixed_content(text, v)
        ref = self._load_reference_fixed_content()

        chapter_map = {
            "二": "CONTRACT_DEFS_TEXT",
            "十一": "CONTRACT_INVEST_TEXT",
            "十二": "CONTRACT_ASSET_TEXT",
            "十三": "CONTRACT_VALUATION_TEXT",
            "十五": "CONTRACT_FEE_TEXT",
            "十六": "CONTRACT_AUDIT_TEXT",
            "十七": "CONTRACT_DISCLOSURE_TEXT",
            "十九": "CONTRACT_TERMINATION_TEXT",
            "二十": "CONTRACT_SUMMARY_TEXT",
        }
        for chap_cn, var_name in chapter_map.items():
            text = self._replace_chapter_body(text, chap_cn, v.get(var_name, ""))

        text = self._replace_chapter_body(text, "六", self._build_chapter_six_body(v, ref))
        text = self._replace_chapter_body(text, "七", v.get("PROSPECTUS_CH7_BODY", ""))
        text = self._replace_chapter_body(text, "九", v.get("PROSPECTUS_CH9_BODY", ""))
        text = self._replace_chapter_body(text, "十", self._build_chapter_ten_body(v, ref))
        text = self._replace_chapter_body(text, "十八", v.get("PROSPECTUS_CH18_BODY", ""))
        text = self._replace_chapter_body(text, "二十一", v.get("PROSPECTUS_CH21_TITLES", ""))

        sec3 = self._ensure_subsection_heading(v.get("CONTRACT_PART18_SEC3", ""), "三")
        text = self._replace_subsection_in_chapter(text, "十四", "三", sec3)
        return text

'''
start4 = text.index('    def _trim_chapter_after_subsection(self, text: str, chapter_cn: str, keep_until_cn: str) -> str:')
end4 = text.index('    # ── Step 4-5: 条件处理 + 占位符替换（委托给 ContractEngine）───────────')
text = text[:start4] + block4 + text[end4:]

path.write_text(text, encoding='utf-8')

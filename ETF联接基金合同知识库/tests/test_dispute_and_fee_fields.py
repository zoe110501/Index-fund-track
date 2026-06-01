from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import app  # noqa: E402
from app import ContractEngine, ProspectusEngine  # noqa: E402


def _base_form() -> dict:
    return {
        "FUND_NAME": "南方中证测试交易型开放式指数证券投资基金联接基金",
        "FUND_SHORT_NAME": "南方中证测试ETF联接",
        "FUND_CODE": "019001",
        "INDEX_NAME": "中证测试指数",
        "INDEX_CODE": "000001",
        "INDEX_COMPILER": "中证指数有限公司",
        "INDEX_WEBSITE": "www.csindex.com.cn",
        "INDEX_DESCRIPTION": "中证测试指数用于测试合同生成。",
        "TARGET_ETF_NAME": "南方中证测试交易型开放式指数证券投资基金",
        "TARGET_ETF_SHORT_NAME": "南方中证测试ETF",
        "TARGET_ETF_CODE": "159999",
        "BENCHMARK": "中证测试指数收益率×95%+活期存款基准利率×5%",
        "CONTRACT_DATE": "2026年5月",
        "MANAGER_NAME": "南方基金管理股份有限公司",
        "MANAGER_ADDRESS": "深圳市福田区莲花街道益田路5999号基金大厦32-42楼",
        "MANAGER_LEGAL_REP": "周易",
        "MANAGER_ESTABLISHED": "1998年3月6日",
        "MANAGER_APPROVAL_NO": "中国证监会证监基字[1998]4号",
        "MANAGER_ORG_FORM": "股份有限公司",
        "MANAGER_REGISTERED_CAPITAL": "3.6172亿元人民币",
        "CUSTODIAN_NAME": "中国建设银行股份有限公司",
        "CUSTODIAN_TYPE": "商业银行",
        "CUSTODIAN_ADDRESS": "北京市西城区金融大街25号",
        "CUSTODIAN_LEGAL_REP": "张金良",
        "CUSTODIAN_ESTABLISHED": "2004年9月17日",
        "CUSTODIAN_APPROVAL_NO": "中国银监会银监复〔2004〕143号",
        "CUSTODIAN_ORG_FORM": "股份有限公司",
        "CUSTODIAN_REGISTERED_CAPITAL": "人民币2500.11亿元",
        "CUSTODIAN_CUSTODY_LICENSE": "证监基字〔1998〕12号",
        "MGMT_FEE_RATE": "0.15",
        "CUSTODY_FEE_RATE": "0.05",
        "SALES_SERVICE_FEE_RATE": "0.20",
        "DISTRIBUTION_FREQ": "QUARTERLY",
        "MGMT_FEE_PAYMENT_METHOD": "CONSULT",
        "CUSTODY_FEE_PAYMENT_METHOD": "CONSULT",
        "TARGET_ETF_RATIO_MIN": "90",
    }


def _contract_summary_blocks(contract_text: str) -> dict[str, str]:
    sections = app._split_contract_sections(contract_text)
    summary = next(sec for sec in sections if "基金合同内容摘要" in sec["heading"])
    return {
        block["heading"]: block["body"]
        for block in app._split_summary_top_blocks(summary["content"])
    }


def test_contract_template_summary_uses_generation_placeholders():
    template_text = app.TEMPLATE_MD.read_text(encoding="utf-8")
    summary_start = template_text.rindex("第二十四部分  基金合同内容摘要")
    signature_start = template_text.index("（本页无正文", summary_start)
    summary_template = template_text[summary_start:signature_start]

    for placeholder in [
        "{CONTRACT_SUMMARY_PARTIES}",
        "{CONTRACT_SUMMARY_MEETING}",
        "{CONTRACT_SUMMARY_DISTRIBUTION}",
        "{CONTRACT_SUMMARY_FEES}",
        "{CONTRACT_SUMMARY_INVESTMENT}",
        "{CONTRACT_SUMMARY_VALUATION}",
        "{CONTRACT_SUMMARY_CHANGE_TERMINATION}",
        "{CONTRACT_SUMMARY_DISPUTE}",
        "{CONTRACT_SUMMARY_EFFECT}",
    ]:
        assert placeholder in summary_template


def test_contract_dispute_resolution_venue_can_use_beijing_cietac():
    contract = ContractEngine().generate(
        {
            **_base_form(),
            "DISPUTE_RESOLUTION_VENUE": "BJ_CIETAC",
        }
    )

    assert "将争议提交中国国际经济贸易仲裁委员会" in contract
    assert "仲裁地点为北京市，仲裁裁决" in contract
    assert "将争议提交深圳国际仲裁院" not in contract
    assert "{DISPUTE_RESOLUTION_CLAUSE}" not in contract


def test_contract_fee_payment_accepts_editable_verbatim_text():
    mgmt_payment = (
        "基金管理费每日计提，逐日累计至每月月末，按月支付。"
        "由测试托管人按人工编辑后的管理费表述支付。"
    )
    custody_payment = (
        "基金托管费每日计提，逐日累计至每月月末，按月支付，"
        "由测试托管人按人工编辑后的托管费表述支取。"
    )
    contract = ContractEngine().generate(
        {
            **_base_form(),
            "MGMT_FEE_PAYMENT_METHOD": mgmt_payment,
            "CUSTODY_FEE_PAYMENT_METHOD": custody_payment,
        }
    )

    assert "由测试托管人按人工编辑后的管理费表述支付" in contract
    assert "由测试托管人按人工编辑后的托管费表述支取" in contract
    assert "按月支付。基金管理费每日计提" not in contract
    assert "按月支付，基金托管费每日计提" not in contract


def test_contract_generated_clause_texts_are_loaded_from_clause_library():
    engine = ContractEngine()
    for clause_key in [
        "INITIATING_FUND_DEFINITIONS",
        "INVESTMENT_SCOPE_TEXT",
        "COMPONENT_STOCK_STRATEGY",
        "RISK_RETURN_CHARACTERISTICS",
    ]:
        assert clause_key in engine.clauses

    form = {
        **_base_form(),
        "HAS_HK_CONNECT": True,
        "INDEX_NAME": "港股通测试指数",
        "FUND_NAME": "南方港股通测试交易型开放式指数证券投资基金联接基金",
    }
    variables = engine._inject_clause_texts(engine._derive_variables(form))
    assert variables["INVESTMENT_SCOPE_TEXT"] == engine.clauses["INVESTMENT_SCOPE_TEXT"]["variants"]["HK_CONNECT"]["text"]


def test_variables_endpoint_updates_schema_variable_definitions(monkeypatch):
    with tempfile.TemporaryDirectory(dir=ROOT) as tmpdir:
        schema_path = Path(tmpdir) / "schema.json"
        schema_path.write_text(
            json.dumps(
                {
                    "version": "test",
                    "groups": {
                        "basic": {
                            "label": "基本变量",
                            "variables": [
                                {
                                    "name": "OLD_FLAG",
                                    "type": "boolean",
                                    "label": "旧开关",
                                    "default": False,
                                    "usage": "旧用途",
                                }
                            ],
                        }
                    },
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        monkeypatch.setattr(app, "SCHEMA_JSON", schema_path)

        client = app.app.test_client()
        list_response = client.get("/api/variables")
        assert list_response.status_code == 200
        list_payload = list_response.get_json()
        assert list_payload["count"] == 1
        assert list_payload["condition_count"] == 1
        assert list_payload["items"][0]["key"] == "OLD_FLAG"

        update_response = client.put(
            "/api/variables/OLD_FLAG",
            json={
                "key": "NEW_FLAG",
                "label": "新开关",
                "type": "boolean",
                "default": True,
                "group": "basic",
                "usage": "新用途",
            },
        )
        assert update_response.status_code == 200

        create_response = client.post(
            "/api/variables",
            json={
                "key": "CUSTOM_TEXT",
                "label": "自定义文本",
                "type": "textarea",
                "default": "默认文本",
                "group": "custom",
                "group_label": "自定义变量",
            },
        )
        assert create_response.status_code == 200

        delete_response = client.delete("/api/variables/CUSTOM_TEXT")
        assert delete_response.status_code == 200

        saved = json.loads(schema_path.read_text(encoding="utf-8"))
    variables = saved["groups"]["basic"]["variables"]
    assert variables[0]["name"] == "NEW_FLAG"
    assert variables[0]["label"] == "新开关"
    assert variables[0]["default"] is True
    assert variables[0]["usage"] == "新用途"
    assert saved["groups"]["custom"]["variables"] == []


def test_index_template_includes_variable_library_panel():
    html = (ROOT / "templates" / "index.html").read_text(encoding="utf-8")

    assert 'data-panel="variables"' in html
    assert 'id="variable-library-rows"' in html
    assert "/api/variables" in html
    assert "openVariableEditor" in html
    assert 'id="variable-type-hint"' in html
    assert "VARIABLE_TYPE_HINTS" in html
    assert "applyVariableTypeHints" in html
    assert "SHOW_CUSTOM_NOTICE" in html
    assert 'id="variable-template-snippet"' in html
    assert "canonicalVariableType" in html
    assert "buildVariableTemplateSuggestion" in html
    assert 'option value="condition_boolean"' not in html
    assert 'option value="direct_input"' not in html
    assert 'option value="direct_input_enum"' not in html
    assert 'option value="derived_clause_placeholder"' not in html


def test_schema_defaults_are_available_to_generation_templates(monkeypatch):
    with tempfile.TemporaryDirectory(dir=ROOT) as tmpdir:
        tmp_path = Path(tmpdir)
        template_path = tmp_path / "template.md"
        schema_path = tmp_path / "schema.json"
        base_template = app.TEMPLATE_MD.read_text(encoding="utf-8")
        template_path.write_text(
            base_template
            + "\n\n"
            + (
                "schema variable: {CUSTOM_NOTICE}\n"
                "{{IF SHOW_CUSTOM_NOTICE}}schema condition enabled{{ENDIF}}\n"
                "{{IF_NOT SHOW_CUSTOM_NOTICE}}schema condition disabled{{ENDIF}}\n"
            ),
            encoding="utf-8",
        )
        schema_path.write_text(
            json.dumps(
                {
                    "version": "test",
                    "groups": {
                        "custom_generation": {
                            "label": "Custom generation",
                            "variables": [
                                {
                                    "name": "CUSTOM_NOTICE",
                                    "type": "string",
                                    "default": "default-from-schema",
                                },
                                {
                                    "name": "SHOW_CUSTOM_NOTICE",
                                    "type": "boolean",
                                    "default": True,
                                },
                            ],
                        }
                    },
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        monkeypatch.setattr(app, "TEMPLATE_MD", template_path)
        monkeypatch.setattr(app, "SCHEMA_JSON", schema_path)

        contract = ContractEngine().generate(_base_form())
        overridden = ContractEngine().generate(
            {
                **_base_form(),
                "CUSTOM_NOTICE": "manual-value",
                "SHOW_CUSTOM_NOTICE": False,
            }
        )

    assert "schema variable: default-from-schema" in contract
    assert "schema condition enabled" in contract
    assert "schema condition disabled" not in contract
    assert "schema variable: manual-value" in overridden
    assert "schema condition enabled" not in overridden
    assert "schema condition disabled" in overridden


def test_distribution_method_metadata_and_ui_match_reference_clause():
    expected = "现金分红与红利再投资（默认现金分红）"
    schema = json.loads((ROOT / "02_变量定义表.json").read_text(encoding="utf-8"))
    distribution_field = next(
        field
        for group in schema["groups"].values()
        for field in group.get("variables", [])
        if field.get("name") == "DISTRIBUTION_METHOD"
    )
    html = (ROOT / "templates" / "index.html").read_text(encoding="utf-8")
    entry_table = (ROOT / "05_要素录入表.md").read_text(encoding="utf-8")
    contract = ContractEngine().generate(_base_form())

    assert distribution_field["default"] == expected
    assert "红利再投资" in distribution_field["note"]
    assert f'value="{expected}"' in html
    assert f"data.DISTRIBUTION_METHOD = '{expected}';" in html
    assert "现金分红（固定）" not in html
    assert expected in entry_table
    assert "固定：现金分红" not in entry_table
    assert "收益分配方式分两种：现金分红与红利再投资" in contract


def test_contract_summary_is_rendered_from_contract_body_without_placeholders():
    contract = ContractEngine().generate(_base_form())
    blocks = _contract_summary_blocks(contract)

    expected_headings = [
        "一、基金合同当事人的权利、义务",
        "二、基金份额持有人大会召集、议事及表决的程序和规则",
        "三、基金收益分配原则、执行方式",
        "四、基金费用与税收",
        "五、基金财产的投资范围和投资限制",
        "六、基金资产净值的计算方法和公告方式",
        "七、基金合同的变更、终止与基金财产的清算",
        "八、争议解决方式",
        "九、基金合同的效力",
    ]

    assert "{CONTRACT_SUMMARY_" not in contract
    assert "CONTRACT_SUMMARY_LEGACY_DISABLED" not in contract
    assert list(blocks) == expected_headings
    assert "（一）基金管理人的权利与义务" in blocks["一、基金合同当事人的权利、义务"]
    assert "目标ETF基金份额持有人大会" in blocks["二、基金份额持有人大会召集、议事及表决的程序和规则"]
    assert "实施侧袋机制期间基金份额持有人大会的特殊约定" in blocks["二、基金份额持有人大会召集、议事及表决的程序和规则"]
    assert "下列第（七）条规定程序确定和公布监票人" in blocks["二、基金份额持有人大会召集、议事及表决的程序和规则"]
    assert "下列第七条规定程序确定和公布监票人" not in blocks["二、基金份额持有人大会召集、议事及表决的程序和规则"]


def test_revision_workbench_summary_rerender_drops_legacy_template_blocks():
    contract = ContractEngine().generate(_base_form())
    summary_text, report = app._render_review_contract_summary_from_text(contract)

    assert report["status"] == "generated"
    assert "{{IF" not in summary_text
    assert "{{ENDIF}}" not in summary_text
    assert "CONTRACT_SUMMARY_LEGACY_DISABLED" not in summary_text
    assert "{CONTRACT_SUMMARY_" not in summary_text


def test_contract_summary_syncs_fee_distribution_and_dispute_fields():
    contract = ContractEngine().generate(
        {
            **_base_form(),
            "MGMT_FEE_RATE": "0.31",
            "CUSTODY_FEE_RATE": "0.09",
            "SALES_SERVICE_FEE_RATE": "0.42",
            "DISTRIBUTION_FREQ": "MONTHLY",
            "DISPUTE_RESOLUTION_VENUE": "BJ_CIETAC",
        }
    )
    blocks = _contract_summary_blocks(contract)

    fees = blocks["四、基金费用与税收"]
    assert "0.31%" in fees
    assert "0.09%" in fees
    assert "0.42%" in fees
    assert "每月评估是否进行收益分配" in blocks["三、基金收益分配原则、执行方式"]
    assert "中国国际经济贸易仲裁委员会" in blocks["八、争议解决方式"]
    assert "仲裁地点为北京市" in blocks["八、争议解决方式"]


def test_contract_summary_keeps_existing_selective_excerpt_scope():
    contract = ContractEngine().generate(_base_form())
    blocks = _contract_summary_blocks(contract)

    distribution = blocks["三、基金收益分配原则、执行方式"]
    fees = blocks["四、基金费用与税收"]
    investment = blocks["五、基金财产的投资范围和投资限制"]
    valuation = blocks["六、基金资产净值的计算方法和公告方式"]

    assert "（一）基金收益分配原则" in distribution
    assert "（三）收益分配方案的确定、公告与实施" in distribution
    assert "基金收益分配中发生的费用" not in distribution
    assert "（一）基金费用的种类" in fees
    assert "（二）基金费用计提方法、计提标准和支付方式" in fees
    assert "不列入基金费用的项目" not in fees
    assert "（一）投资范围" in investment
    assert "（二）投资限制" in investment
    assert "三、投资策略" not in investment
    assert "基金管理人应每个估值日对基金资产估值" in valuation
    assert "估值错误的处理" not in valuation


def test_builtin_summary_rules_match_generated_contract_sections():
    contract = ContractEngine().generate(_base_form())
    sections = app._split_contract_sections(contract)

    for rule in app.BUILTIN_LINKED_FUND_SUMMARY_RULES:
        target = app._locate_review_rule_target(sections, rule["contract_pos"])
        assert target["matched"], rule["contract_pos"]


def test_prospectus_contract_block_extracts_summary_descendants():
    prospectus_engine = ProspectusEngine()
    text = (
        "一、基金合同当事人的权利、义务\n\n"
        "（一）基金管理人的权利与义务\n\n"
        "1、根据《基金法》，基金管理人的权利包括但不限于：\n\n"
        "（1）依法募集资金；\n\n"
        "2、根据《基金法》，基金管理人的义务包括但不限于：\n\n"
        "（1）办理基金备案手续；\n\n"
        "（二）基金托管人的权利与义务\n\n"
        "1、根据《基金法》，基金托管人的权利包括但不限于：\n\n"
        "（1）获得基金托管费；\n"
    )

    block = prospectus_engine._extract_contract_block(text, "（一）基金管理人的权利与义务")

    assert "1、根据《基金法》，基金管理人的权利包括但不限于：" in block
    assert "（1）依法募集资金；" in block
    assert "2、根据《基金法》，基金管理人的义务包括但不限于：" in block
    assert "（1）办理基金备案手续；" in block
    assert "基金托管人的权利与义务" not in block


def test_prospectus_contract_summary_includes_parties_full_body():
    prospectus = ProspectusEngine().generate(_base_form())
    summary_start = prospectus.rindex("十九、基金合同的内容摘要")
    summary_end = prospectus.index("二十、基金托管协议的内容摘要", summary_start)
    summary = prospectus[summary_start:summary_end]

    assert "（一）基金管理人的权利与义务" in summary
    assert "1、根据《基金法》、《运作办法》及其他有关规定，基金管理人的权利包括但不限于：" in summary
    assert "（1）依法募集资金；" in summary
    assert "2、根据《基金法》、《运作办法》及其他有关规定，基金管理人的义务包括但不限于：" in summary
    assert "（二）基金托管人的权利与义务" in summary
    assert "1、根据《基金法》、《运作办法》及其他有关规定，基金托管人的权利包括但不限于：" in summary
    assert "（1）自《基金合同》生效之日起，依法律法规和《基金合同》的规定安全保管基金财产；" in summary
    assert "下列第（七）条规定程序确定和公布监票人" in summary
    assert "下列第七条规定程序确定和公布监票人" not in summary

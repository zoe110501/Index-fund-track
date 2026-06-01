from __future__ import annotations

import json
import shutil
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR_NAME = "config"

CONFIG_FILE_NAMES = {
    "template_manifest": "template_manifest.json",
    "variable_registry": "variable_registry.json",
    "organization_master_data": "organization_master_data.json",
    "publish_state": "publish_state.json",
}

REQUIRED_TOP_LEVEL_KEYS = {
    "template_manifest": ("version", "templates"),
    "variable_registry": ("version", "variables"),
    "organization_master_data": ("version", "organizations"),
    "publish_state": ("version", "status"),
}


SEED_CONFIGS: dict[str, dict[str, Any]] = {
    "template_manifest": {
        "version": "1.0",
        "last_updated": "2026-05-20",
        "templates": [
            {
                "id": "etf_contract_markdown",
                "system": "etf",
                "kind": "text_template",
                "label": "ETF 基金合同正文模板",
                "path": "../ETF合同知识库/01_基金合同模板.md",
                "managed_path": "",
                "required_variables": ["FUND_NAME", "CUSTODIAN_NAME", "MANAGER_NAME"],
            },
            {
                "id": "etf_prospectus_markdown",
                "system": "etf",
                "kind": "text_template",
                "label": "ETF 招募说明书正文模板",
                "path": "../ETF合同知识库/02_招募说明书模板.md",
                "managed_path": "",
                "required_variables": ["FUND_NAME", "CUSTODIAN_NAME", "MANAGER_NAME"],
            },
            {
                "id": "etf_variable_definition_json",
                "system": "etf",
                "kind": "variable_definition",
                "label": "ETF 变量定义表",
                "path": "../ETF合同知识库/03_变量定义表.json",
                "managed_path": "",
                "required_variables": [],
            },
            {
                "id": "etf_contract_diff_table",
                "system": "etf",
                "kind": "clause_mapping",
                "label": "ETF 基金合同差异条款匹配表",
                "path": "../ETF合同知识库/04_基金合同差异条款匹配表.md",
                "managed_path": "",
                "required_variables": [],
            },
            {
                "id": "etf_prospectus_diff_table",
                "system": "etf",
                "kind": "clause_mapping",
                "label": "ETF 招募说明书差异条款映射表",
                "path": "../ETF合同知识库/05_招募说明书差异条款映射表.md",
                "managed_path": "",
                "required_variables": [],
            },
            {
                "id": "etf_entry_table",
                "system": "etf",
                "kind": "input_schema",
                "label": "ETF 要素录入表",
                "path": "../ETF合同知识库/06_要素录入表.md",
                "managed_path": "",
                "required_variables": [],
            },
            {
                "id": "etf_contract_clause_library",
                "system": "etf",
                "kind": "clause_library",
                "label": "ETF 基金合同差异条款原文库",
                "path": "../ETF合同知识库/07_基金合同差异条款原文库.json",
                "managed_path": "",
                "required_variables": [],
            },
            {
                "id": "etf_prospectus_clause_library",
                "system": "etf",
                "kind": "clause_library",
                "label": "ETF 招募说明书差异条款库",
                "path": "../ETF合同知识库/08_招募说明书差异条款库.json",
                "managed_path": "",
                "required_variables": [],
            },
            {
                "id": "etf_business_text_overrides",
                "system": "etf",
                "kind": "business_text_overrides",
                "label": "ETF 业务正文覆盖表",
                "path": "../ETF合同知识库/09_业务正文覆盖.json",
                "managed_path": "",
                "required_variables": [],
            },
            {
                "id": "etf_contract_docx_source_dir",
                "system": "etf",
                "kind": "docx_template_source_dir",
                "label": "ETF 基金合同 DOCX 格式底稿来源目录",
                "path": "%USERPROFILE%/Desktop/基金合同",
                "managed_path": "",
                "selection_rule": "包含“基金合同”，排除“联接”“产品资料概要”“公告”。",
                "required_variables": [],
            },
            {
                "id": "etf_contract_docx_packaged_dir",
                "system": "etf",
                "kind": "docx_template_packaged_dir",
                "label": "ETF 基金合同 DOCX 格式底稿发布目录",
                "path": "../ETF合同知识库/packaged_assets/contract_templates",
                "managed_path": "",
                "selection_rule": "运行时会从该目录选择包含“基金合同”的 DOCX；多份存在时当前代码取第一份。",
                "required_variables": [],
            },
            {
                "id": "etf_product_summary_docx",
                "system": "etf",
                "kind": "docx_template",
                "label": "ETF 产品资料概要 DOCX 底稿",
                "path": "../ETF合同知识库/packaged_assets/product_summary/南方国证石油天然气交易型开放式指数证券投资基金基金产品资料概要.docx",
                "managed_path": "",
                "required_variables": ["FUND_NAME", "CUSTODIAN_NAME", "MANAGER_NAME"],
            },
            {
                "id": "etf_prospectus_reference_sse_cross",
                "system": "etf",
                "kind": "prospectus_reference_docx",
                "label": "ETF 招募说明书参考底稿 SSE_CROSS（本地优先）",
                "path": "../ETF合同知识库/manual_regression_20260310_201555/SSE_CROSS.docx",
                "managed_path": "../ETF合同知识库/packaged_assets/reference_prospectus/SSE_CROSS.docx",
                "variant": "SSE_CROSS",
                "required_variables": [],
            },
            {
                "id": "etf_prospectus_reference_sse_single",
                "system": "etf",
                "kind": "prospectus_reference_docx",
                "label": "ETF 招募说明书参考底稿 SSE_SINGLE（本地优先）",
                "path": "../ETF合同知识库/manual_regression_20260310_201555/SSE_SINGLE.docx",
                "managed_path": "../ETF合同知识库/packaged_assets/reference_prospectus/SSE_SINGLE.docx",
                "variant": "SSE_SINGLE",
                "required_variables": [],
            },
            {
                "id": "etf_prospectus_reference_sse_hk",
                "system": "etf",
                "kind": "prospectus_reference_docx",
                "label": "ETF 招募说明书参考底稿 SSE_HK（本地优先）",
                "path": "../ETF合同知识库/manual_regression_20260310_201555/SSE_HK.docx",
                "managed_path": "../ETF合同知识库/packaged_assets/reference_prospectus/SSE_HK.docx",
                "variant": "SSE_HK",
                "required_variables": [],
            },
            {
                "id": "etf_prospectus_reference_szse_cross",
                "system": "etf",
                "kind": "prospectus_reference_docx",
                "label": "ETF 招募说明书参考底稿 SZSE_CROSS（本地优先）",
                "path": "../ETF合同知识库/manual_regression_20260310_201555/SZSE_CROSS.docx",
                "managed_path": "../ETF合同知识库/packaged_assets/reference_prospectus/SZSE_CROSS.docx",
                "variant": "SZSE_CROSS",
                "required_variables": [],
            },
            {
                "id": "etf_prospectus_reference_szse_single",
                "system": "etf",
                "kind": "prospectus_reference_docx",
                "label": "ETF 招募说明书参考底稿 SZSE_SINGLE（本地优先）",
                "path": "../ETF合同知识库/manual_regression_20260310_201555/SZSE_SINGLE.docx",
                "managed_path": "../ETF合同知识库/packaged_assets/reference_prospectus/SZSE_SINGLE.docx",
                "variant": "SZSE_SINGLE",
                "required_variables": [],
            },
            {
                "id": "etf_prospectus_reference_szse_hk",
                "system": "etf",
                "kind": "prospectus_reference_docx",
                "label": "ETF 招募说明书参考底稿 SZSE_HK（本地优先）",
                "path": "../ETF合同知识库/manual_regression_20260310_201555/SZSE_HK.docx",
                "managed_path": "../ETF合同知识库/packaged_assets/reference_prospectus/SZSE_HK.docx",
                "variant": "SZSE_HK",
                "required_variables": [],
            },
            {
                "id": "etf_prospectus_reference_text_sse_cross",
                "system": "etf",
                "kind": "prospectus_reference_text",
                "label": "ETF 招募说明书参考抽取文本 SSE_CROSS",
                "path": "../ETF合同知识库/manual_regression_20260310_201555/SSE_CROSS.txt",
                "managed_path": "",
                "variant": "SSE_CROSS",
                "required_variables": [],
            },
            {
                "id": "etf_prospectus_reference_text_sse_single",
                "system": "etf",
                "kind": "prospectus_reference_text",
                "label": "ETF 招募说明书参考抽取文本 SSE_SINGLE",
                "path": "../ETF合同知识库/manual_regression_20260310_201555/SSE_SINGLE.txt",
                "managed_path": "",
                "variant": "SSE_SINGLE",
                "required_variables": [],
            },
            {
                "id": "etf_prospectus_reference_text_sse_hk",
                "system": "etf",
                "kind": "prospectus_reference_text",
                "label": "ETF 招募说明书参考抽取文本 SSE_HK",
                "path": "../ETF合同知识库/manual_regression_20260310_201555/SSE_HK.txt",
                "managed_path": "",
                "variant": "SSE_HK",
                "required_variables": [],
            },
            {
                "id": "etf_prospectus_reference_text_szse_cross",
                "system": "etf",
                "kind": "prospectus_reference_text",
                "label": "ETF 招募说明书参考抽取文本 SZSE_CROSS",
                "path": "../ETF合同知识库/manual_regression_20260310_201555/SZSE_CROSS.txt",
                "managed_path": "",
                "variant": "SZSE_CROSS",
                "required_variables": [],
            },
            {
                "id": "etf_prospectus_reference_text_szse_single",
                "system": "etf",
                "kind": "prospectus_reference_text",
                "label": "ETF 招募说明书参考抽取文本 SZSE_SINGLE",
                "path": "../ETF合同知识库/manual_regression_20260310_201555/SZSE_SINGLE.txt",
                "managed_path": "",
                "variant": "SZSE_SINGLE",
                "required_variables": [],
            },
            {
                "id": "etf_prospectus_reference_text_szse_hk",
                "system": "etf",
                "kind": "prospectus_reference_text",
                "label": "ETF 招募说明书参考抽取文本 SZSE_HK",
                "path": "../ETF合同知识库/manual_regression_20260310_201555/SZSE_HK.txt",
                "managed_path": "",
                "variant": "SZSE_HK",
                "required_variables": [],
            },
            {
                "id": "etf_review_rules_xlsx",
                "system": "etf",
                "kind": "review_rules_workbook",
                "label": "ETF 合同与招募说明书复核规则",
                "path": "../ETF合同知识库/packaged_assets/review_rules/基金合同与招募说明书规则.xlsx",
                "managed_path": "",
                "required_variables": [],
            },
            {
                "id": "etf_review_workbook_red_quality",
                "system": "etf",
                "kind": "review_workbook",
                "label": "ETF 红利质量勾稽关系工作簿",
                "path": "../ETF合同知识库/packaged_assets/review_workbooks/南方中证全指红利质量ETF_勾稽关系整理.xlsx",
                "managed_path": "",
                "required_variables": [],
            },
            {
                "id": "etf_review_workbook_linked_aviation",
                "system": "etf",
                "kind": "review_workbook",
                "label": "ETF 通用航空联接勾稽关系工作簿",
                "path": "../ETF合同知识库/packaged_assets/review_workbooks/南方中证通用航空主题ETF发起式联接基金_勾稽关系整理.xlsx",
                "managed_path": "",
                "required_variables": [],
            },
            {
                "id": "etf_prospectus_materials_index",
                "system": "etf",
                "kind": "prospectus_material",
                "label": "ETF 招募说明书参考素材索引",
                "path": "../ETF合同知识库/prospectus_materials/01_参考素材索引.json",
                "managed_path": "",
                "required_variables": [],
            },
            {
                "id": "etf_prospectus_materials_readme",
                "system": "etf",
                "kind": "prospectus_material",
                "label": "ETF 招募说明书素材库说明",
                "path": "../ETF合同知识库/prospectus_materials/00_素材库说明.md",
                "managed_path": "",
                "required_variables": [],
            },
            {
                "id": "etf_prospectus_materials_source_mapping",
                "system": "etf",
                "kind": "prospectus_material",
                "label": "ETF 招募说明书表述来源映射表",
                "path": "../ETF合同知识库/prospectus_materials/02_表述来源映射表.md",
                "managed_path": "",
                "required_variables": [],
            },
            {
                "id": "etf_prospectus_materials_function_mapping",
                "system": "etf",
                "kind": "prospectus_material",
                "label": "ETF 招募说明书函数映射表",
                "path": "../ETF合同知识库/prospectus_materials/03_函数映射表.json",
                "managed_path": "",
                "required_variables": [],
            },
            {
                "id": "etf_prospectus_materials_shared_dependencies",
                "system": "etf",
                "kind": "prospectus_material",
                "label": "ETF 招募说明书共享依赖清单",
                "path": "../ETF合同知识库/prospectus_materials/04_共享依赖清单.md",
                "managed_path": "",
                "required_variables": [],
            },
            {
                "id": "etf_prospectus_materials_editing_guide",
                "system": "etf",
                "kind": "prospectus_material",
                "label": "ETF 招募说明书素材修改操作指引",
                "path": "../ETF合同知识库/prospectus_materials/05_修改操作指引.md",
                "managed_path": "",
                "required_variables": [],
            },
            {
                "id": "linked_contract_markdown",
                "system": "linked",
                "kind": "text_template",
                "label": "ETF联接 基金合同正文模板",
                "path": "../ETF联接基金合同知识库/01_通用模板.md",
                "managed_path": "",
                "required_variables": ["FUND_NAME", "CUSTODIAN_NAME", "MANAGER_NAME"],
            },
            {
                "id": "linked_variable_definition_json",
                "system": "linked",
                "kind": "variable_definition",
                "label": "ETF联接 变量定义表",
                "path": "../ETF联接基金合同知识库/02_变量定义表.json",
                "managed_path": "",
                "required_variables": [],
            },
            {
                "id": "linked_contract_diff_table",
                "system": "linked",
                "kind": "clause_mapping",
                "label": "ETF联接 基金合同差异条款匹配表",
                "path": "../ETF联接基金合同知识库/03_差异条款匹配表.md",
                "managed_path": "",
                "required_variables": [],
            },
            {
                "id": "linked_contract_clause_library",
                "system": "linked",
                "kind": "clause_library",
                "label": "ETF联接 基金合同差异条款原文库",
                "path": "../ETF联接基金合同知识库/04_差异条款原文库.json",
                "managed_path": "",
                "required_variables": [],
            },
            {
                "id": "linked_entry_table",
                "system": "linked",
                "kind": "input_schema",
                "label": "ETF联接 要素录入表",
                "path": "../ETF联接基金合同知识库/05_要素录入表.md",
                "managed_path": "",
                "required_variables": [],
            },
            {
                "id": "linked_prospectus_markdown",
                "system": "linked",
                "kind": "text_template",
                "label": "ETF联接 招募说明书正文模板",
                "path": "../ETF联接基金合同知识库/07_招募说明书模板.md",
                "managed_path": "",
                "required_variables": ["FUND_NAME", "CUSTODIAN_NAME", "MANAGER_NAME"],
            },
            {
                "id": "linked_prospectus_clause_library",
                "system": "linked",
                "kind": "clause_library",
                "label": "ETF联接 招募说明书差异条款库",
                "path": "../ETF联接基金合同知识库/08_招募说明书差异条款库.json",
                "managed_path": "",
                "required_variables": [],
            },
            {
                "id": "linked_prospectus_diff_table",
                "system": "linked",
                "kind": "clause_mapping",
                "label": "ETF联接 招募说明书差异条款映射表",
                "path": "../ETF联接基金合同知识库/09_招募说明书差异条款映射表.md",
                "managed_path": "",
                "required_variables": [],
            },
            {
                "id": "linked_legal_templates_source_dir",
                "system": "linked",
                "kind": "docx_template_source_dir",
                "label": "ETF联接 法律文件 DOCX 底稿来源目录",
                "path": "%USERPROFILE%/Desktop/联接基金法律文件",
                "managed_path": "",
                "selection_rule": "合同、招募说明书、产品资料概要当前底稿均来自该目录。",
                "required_variables": [],
            },
            {
                "id": "linked_legal_templates_packaged_dir",
                "system": "linked",
                "kind": "docx_template_packaged_dir",
                "label": "ETF联接 合同/招募说明书 DOCX 底稿发布目录",
                "path": "../ETF联接基金合同知识库/packaged_assets/legal_templates",
                "managed_path": "",
                "selection_rule": "运行时优先读取该目录，缺失时回退到桌面“联接基金法律文件”。",
                "required_variables": [],
            },
            {
                "id": "linked_product_summary_templates_packaged_dir",
                "system": "linked",
                "kind": "docx_template_packaged_dir",
                "label": "ETF联接 产品资料概要 DOCX 底稿发布目录",
                "path": "../ETF联接基金合同知识库/packaged_assets/product_summary_templates",
                "managed_path": "",
                "selection_rule": "运行时优先读取该目录，缺失时回退到桌面“联接基金法律文件”。",
                "required_variables": [],
            },
            {
                "id": "linked_contract_docx",
                "system": "linked",
                "kind": "docx_template",
                "label": "ETF联接 基金合同 DOCX 格式底稿",
                "path": "%USERPROFILE%/Desktop/联接基金法律文件/1、南方中证全指农牧渔交易型开放式指数证券投资基金发起式联接基金基金合同（草案）.docx",
                "managed_path": "../ETF联接基金合同知识库/packaged_assets/legal_templates/1、南方中证全指农牧渔交易型开放式指数证券投资基金发起式联接基金基金合同（草案）.docx",
                "required_variables": ["FUND_NAME", "CUSTODIAN_NAME", "MANAGER_NAME"],
            },
            {
                "id": "linked_prospectus_docx",
                "system": "linked",
                "kind": "docx_template",
                "label": "ETF联接 招募说明书 DOCX 格式底稿",
                "path": "%USERPROFILE%/Desktop/联接基金法律文件/2、南方中证全指农牧渔交易型开放式指数证券投资基金发起式联接基金招募说明书（草案）.docx",
                "managed_path": "../ETF联接基金合同知识库/packaged_assets/legal_templates/2、南方中证全指农牧渔交易型开放式指数证券投资基金发起式联接基金招募说明书（草案）.docx",
                "required_variables": ["FUND_NAME", "CUSTODIAN_NAME", "MANAGER_NAME"],
            },
            {
                "id": "linked_product_summary_a_docx",
                "system": "linked",
                "kind": "docx_template",
                "label": "ETF联接 产品资料概要 A类 DOCX 底稿",
                "path": "%USERPROFILE%/Desktop/联接基金法律文件/4、南方中证全指农牧渔交易型开放式指数证券投资基金发起式联接基金（A类份额）基金产品资料概要.docx",
                "managed_path": "../ETF联接基金合同知识库/packaged_assets/product_summary_templates/4、南方中证全指农牧渔交易型开放式指数证券投资基金发起式联接基金（A类份额）基金产品资料概要.docx",
                "required_variables": ["FUND_NAME", "CUSTODIAN_NAME", "MANAGER_NAME"],
            },
            {
                "id": "linked_product_summary_c_docx",
                "system": "linked",
                "kind": "docx_template",
                "label": "ETF联接 产品资料概要 C类 DOCX 底稿",
                "path": "%USERPROFILE%/Desktop/联接基金法律文件/5、南方中证全指农牧渔交易型开放式指数证券投资基金联接基金（C类份额）基金产品资料概要.docx",
                "managed_path": "../ETF联接基金合同知识库/packaged_assets/product_summary_templates/5、南方中证全指农牧渔交易型开放式指数证券投资基金联接基金（C类份额）基金产品资料概要.docx",
                "required_variables": ["FUND_NAME", "CUSTODIAN_NAME", "MANAGER_NAME"],
            },
        ],
    },
    "variable_registry": {
        "version": "1.0",
        "last_updated": "2026-05-20",
        "variables": [
            {"name": "FUND_NAME", "label": "基金全名", "type": "string", "required": True, "systems": ["etf", "linked"]},
            {"name": "FUND_SHORT_NAME", "label": "基金简称", "type": "string", "required": False, "systems": ["etf", "linked"]},
            {"name": "FUND_CODE", "label": "基金代码", "type": "string", "required": False, "systems": ["etf", "linked"]},
            {"name": "INDEX_NAME", "label": "标的指数名称", "type": "string", "required": True, "systems": ["etf", "linked"]},
            {"name": "MANAGER_NAME", "label": "基金管理人名称", "type": "organization_ref", "required": True, "systems": ["etf", "linked"]},
            {"name": "MANAGER_ADDRESS", "label": "基金管理人住所", "type": "string", "required": True, "systems": ["etf", "linked"]},
            {"name": "MANAGER_LEGAL_REP", "label": "基金管理人法定代表人", "type": "string", "required": True, "systems": ["etf", "linked"]},
            {"name": "MANAGER_INFO_VERSION", "label": "基金管理人信息版本", "type": "string", "required": False, "systems": ["linked"]},
            {"name": "COMPANY_WEBSITE", "label": "基金管理人网站", "type": "string", "required": False, "systems": ["etf"]},
            {"name": "FUND_MANAGER_WEBSITE", "label": "基金管理人网站（产品概要别名）", "type": "string", "required": False, "systems": ["etf"]},
            {"name": "SERVICE_HOTLINE", "label": "客服电话", "type": "string", "required": False, "systems": ["etf"]},
            {"name": "FUND_MANAGER_HOTLINE", "label": "客服电话（产品概要别名）", "type": "string", "required": False, "systems": ["etf"]},
            {"name": "CUSTODIAN_NAME", "label": "基金托管人名称", "type": "organization_ref", "required": True, "systems": ["etf", "linked"]},
            {"name": "CUSTODIAN_ADDRESS", "label": "基金托管人住所", "type": "string", "required": True, "systems": ["etf", "linked"]},
            {"name": "CUSTODIAN_HAS_OFFICE_ADDRESS", "label": "托管人是否有办公地址", "type": "boolean", "required": False, "systems": ["etf", "linked"]},
            {"name": "CUSTODIAN_OFFICE_ADDRESS", "label": "基金托管人办公地址", "type": "string", "required": False, "systems": ["etf", "linked"]},
            {"name": "CUSTODIAN_LEGAL_REP", "label": "基金托管人法定代表人", "type": "string", "required": True, "systems": ["etf", "linked"]},
            {"name": "CUSTODIAN_ESTABLISHED", "label": "基金托管人成立日期", "type": "string", "required": False, "systems": ["linked"]},
            {"name": "CUSTODIAN_APPROVAL_NO", "label": "基金托管人批准设立文号", "type": "string", "required": False, "systems": ["linked"]},
            {"name": "CUSTODIAN_REGISTERED_CAPITAL", "label": "基金托管人注册资本", "type": "string", "required": False, "systems": ["linked"]},
            {"name": "CUSTODIAN_ORG_FORM", "label": "基金托管人组织形式", "type": "string", "required": False, "systems": ["linked"]},
            {"name": "CUSTODIAN_CUSTODY_LICENSE", "label": "基金托管资格批文", "type": "string", "required": False, "systems": ["linked"]},
            {"name": "CUSTODIAN_TYPE", "label": "托管人类型", "type": "enum", "required": False, "systems": ["etf", "linked"]},
            {"name": "CUSTODIAN_INFO_VERSION", "label": "基金托管人信息版本", "type": "string", "required": False, "systems": ["linked"]},
            {"name": "CUSTODIAN_DEPT", "label": "基金托管部门", "type": "string", "required": False, "systems": ["etf"]},
            {"name": "CUSTODIAN_PHONE", "label": "基金托管部门联系电话", "type": "string", "required": False, "systems": ["etf"]},
            {"name": "CUSTODIAN_WEBSITE", "label": "基金托管人网站", "type": "string", "required": False, "systems": ["etf"]},
            {"name": "CUSTODIAN_INTRO", "label": "基金托管人简介", "type": "text", "required": False, "systems": ["etf"]},
            {"name": "CUSTODIAN_PROSPECTUS_TEXT", "label": "基金托管人招募说明书正文", "type": "text", "required": False, "systems": ["linked"]},
            {"name": "MGMT_FEE_PAYMENT_METHOD", "label": "管理费划款方式", "type": "text", "required": False, "systems": ["linked"]},
            {"name": "CUSTODY_FEE_PAYMENT_METHOD", "label": "托管费划款方式", "type": "text", "required": False, "systems": ["linked"]},
            {"name": "FUND_MANAGER_NAME", "label": "基金经理姓名", "type": "string", "required": False, "systems": ["etf", "linked"]},
            {"name": "FUND_MANAGER_SEX", "label": "基金经理性别称谓", "type": "enum", "required": False, "systems": ["etf"]},
            {"name": "FUND_MANAGER_BIO", "label": "基金经理简介", "type": "text", "required": False, "systems": ["etf"]},
            {"name": "FUND_MANAGER_RESUME", "label": "基金经理简历全文", "type": "text", "required": False, "systems": ["linked"]},
            {"name": "FUND_MANAGER_START_DATE", "label": "开始担任本基金基金经理日期", "type": "string", "required": False, "systems": ["linked"]},
            {"name": "FUND_MANAGER_SECURITIES_DATE", "label": "基金经理证券从业日期", "type": "string", "required": False, "systems": ["linked"]},
            {"name": "SERVICE_ORGANIZATIONS_TEXT", "label": "相关服务机构全文", "type": "text", "required": False, "systems": ["linked"]},
            {"name": "ACCOUNTING_FIRM_PROFILE", "label": "会计师事务所简介", "type": "text", "required": False, "systems": ["etf", "linked"]},
            {"name": "LAW_FIRM_PROFILE", "label": "律师事务所简介", "type": "text", "required": False, "systems": ["etf", "linked"]},
        ],
    },
    "organization_master_data": {
        "version": "1.0",
        "last_updated": "2026-05-20",
        "organizations": {
            "managers": [
                {
                    "name": "南方基金管理股份有限公司",
                    "address": "深圳市福田区莲花街道益田路5999号基金大厦32-42楼",
                    "legal_representative": "周易",
                    "established": "1998年3月6日",
                    "registered_capital": "3.6172亿元人民币",
                    "website": "www.nffund.com",
                    "service_hotline": "400-889-8899",
                }
            ],
            "custodians": [
                {
                    "name": "中国工商银行股份有限公司",
                    "type": "商业银行",
                    "address": "北京市西城区复兴门内大街55号",
                    "legal_representative": "廖林",
                    "established": "1984年1月1日",
                    "registered_capital": "人民币35,640,625.71万元",
                    "custody_license": "中国证监会和中国人民银行证监基字【1998】3号",
                    "mgmt_fee_payment_method": "AUTO_V2",
                    "custody_fee_payment_method": "AUTO_V2",
                },
                {
                    "name": "招商银行股份有限公司",
                    "type": "商业银行",
                    "address": "",
                    "legal_representative": "",
                    "established": "",
                    "registered_capital": "",
                    "custody_license": "",
                    "mgmt_fee_payment_method": "",
                    "custody_fee_payment_method": "",
                },
            ],
            "accounting_firms": [
                {"name": "", "profile": "", "certified_accountants": []}
            ],
            "law_firms": [
                {"name": "", "profile": "", "lawyers": []}
            ],
        },
    },
    "publish_state": {
        "version": "1.0",
        "status": "DRAFT",
        "published_at": "",
        "published_by": "",
        "validation": {"errors": [], "warnings": []},
    },
}


def now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S-%f")


def get_base_dir(base_dir: str | Path | None = None) -> Path:
    return Path(base_dir).resolve() if base_dir is not None else BASE_DIR


def get_config_dir(base_dir: str | Path | None = None) -> Path:
    return get_base_dir(base_dir) / CONFIG_DIR_NAME


def get_config_path(name: str, base_dir: str | Path | None = None) -> Path:
    if name not in CONFIG_FILE_NAMES:
        raise KeyError(f"Unknown config: {name}")
    return get_config_dir(base_dir) / CONFIG_FILE_NAMES[name]


def validate_config(name: str, data: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    for key in REQUIRED_TOP_LEVEL_KEYS.get(name, ()):
        if key not in data:
            errors.append(f"Missing top-level key: {key}")
    return errors


def seed_config(name: str) -> dict[str, Any]:
    if name not in SEED_CONFIGS:
        raise KeyError(f"Unknown seed config: {name}")
    return deepcopy(SEED_CONFIGS[name])


def write_json_atomic(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temp_path.replace(path)


def backup_existing_config(path: Path, base_dir: Path) -> Path | None:
    if not path.exists():
        return None
    backup_path = (
        base_dir
        / "backups"
        / "maintenance-admin"
        / now_stamp()
        / "config"
        / path.name
    )
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, backup_path)
    return backup_path


def save_config(
    name: str,
    data: dict[str, Any],
    base_dir: str | Path | None = None,
    backup: bool = True,
) -> Path:
    errors = validate_config(name, data)
    if errors:
        raise ValueError("; ".join(errors))
    resolved_base_dir = get_base_dir(base_dir)
    path = get_config_path(name, resolved_base_dir)
    if backup:
        backup_existing_config(path, resolved_base_dir)
    write_json_atomic(path, data)
    return path


def ensure_seed_configs(base_dir: str | Path | None = None) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    for name in CONFIG_FILE_NAMES:
        path = get_config_path(name, base_dir)
        if not path.exists():
            write_json_atomic(path, seed_config(name))
        paths[name] = path
    return paths


def load_config(name: str, base_dir: str | Path | None = None) -> dict[str, Any]:
    path = get_config_path(name, base_dir)
    if not path.exists():
        ensure_seed_configs(base_dir)
    data = json.loads(path.read_text(encoding="utf-8"))
    errors = validate_config(name, data)
    if errors:
        raise ValueError(f"Invalid {name}: {'; '.join(errors)}")
    return data

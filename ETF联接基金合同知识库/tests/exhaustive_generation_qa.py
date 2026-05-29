"""Exhaustive generation QA for ETF linked fund legal documents.

This script exercises the discrete UI/backend condition space and verifies that
the generated contract, prospectus, and product summaries do not contain obvious
omissions, unresolved placeholders, or mutually conflicting conditional clauses.
"""

from __future__ import annotations

import argparse
import io
import itertools
import re
import sys
import time
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app import ContractEngine, ProductSummaryEngine, ProspectusEngine, _generation_quality_check  # noqa: E402


_WORKER_ENGINES: tuple[ContractEngine, ProspectusEngine, ProductSummaryEngine] | None = None


PAYMENT_METHODS = ("CONSULT", "AUTO", "AUTO_V2", "AUTO_CHECK")
DISTRIBUTION_FREQS = ("QUARTERLY", "MONTHLY")
RISK_SUMMARY_STYLES = ("市场风险", "系统性风险和非系统性风险")

SPECIFIC_RISK_TITLES = (
    "投资股指期货的风险",
    "本基金投资资产支持证券的风险",
    "本基金参与转融通证券出借业务的风险",
    "投资于存托凭证的风险",
    "投资于目标ETF带来的风险",
)


@dataclass(frozen=True)
class Scenario:
    id: str
    fund_core: str
    short_core: str
    index_name: str
    index_code: str
    index_compiler: str
    market_scope: str
    single_exchange: str
    has_hk: bool
    is_chuangye: bool
    is_kechuang: bool


@dataclass(frozen=True)
class CustodianCase:
    id: str
    name: str
    custodian_type: str


SCENARIOS = (
    Scenario(
        id="cross_non_hk",
        fund_core="南方中证全指质量交易型开放式指数证券投资基金",
        short_core="南方中证全指质量ETF联接",
        index_name="中证全指质量指数",
        index_code="930001",
        index_compiler="中证指数有限公司",
        market_scope="跨市场",
        single_exchange="",
        has_hk=False,
        is_chuangye=False,
        is_kechuang=False,
    ),
    Scenario(
        id="single_sh",
        fund_core="南方上证测试交易型开放式指数证券投资基金",
        short_core="南方上证测试ETF联接",
        index_name="上证测试指数",
        index_code="000001",
        index_compiler="中证指数有限公司",
        market_scope="单市场",
        single_exchange="上交所",
        has_hk=False,
        is_chuangye=False,
        is_kechuang=False,
    ),
    Scenario(
        id="single_sz",
        fund_core="南方深证测试交易型开放式指数证券投资基金",
        short_core="南方深证测试ETF联接",
        index_name="深证测试指数",
        index_code="399001",
        index_compiler="深圳证券信息有限公司",
        market_scope="单市场",
        single_exchange="深交所",
        has_hk=False,
        is_chuangye=False,
        is_kechuang=False,
    ),
    Scenario(
        id="hk_connect",
        fund_core="南方中证港股通测试交易型开放式指数证券投资基金",
        short_core="南方中证港股通测试ETF联接",
        index_name="中证港股通测试指数",
        index_code="931001",
        index_compiler="中证指数有限公司",
        market_scope="跨市场",
        single_exchange="",
        has_hk=True,
        is_chuangye=False,
        is_kechuang=False,
    ),
    Scenario(
        id="chuangye",
        fund_core="南方创业板测试交易型开放式指数证券投资基金",
        short_core="南方创业板测试ETF联接",
        index_name="创业板测试指数",
        index_code="399002",
        index_compiler="深圳证券信息有限公司",
        market_scope="单市场",
        single_exchange="深交所",
        has_hk=False,
        is_chuangye=True,
        is_kechuang=False,
    ),
    Scenario(
        id="kechuang",
        fund_core="南方上证科创板测试交易型开放式指数证券投资基金",
        short_core="南方上证科创板测试ETF联接",
        index_name="上证科创板测试指数",
        index_code="000688",
        index_compiler="中证指数有限公司",
        market_scope="单市场",
        single_exchange="上交所",
        has_hk=False,
        is_chuangye=False,
        is_kechuang=True,
    ),
)

CUSTODIANS = (
    CustodianCase("bank_consult", "中国工商银行股份有限公司", "商业银行"),
    CustodianCase("bank_auto", "中国建设银行股份有限公司", "商业银行"),
    CustodianCase("securities_consult", "国泰海通证券股份有限公司", "证券公司"),
    CustodianCase("securities_instruction", "中信证券股份有限公司", "证券公司"),
)


def build_case(
    scenario: Scenario,
    custodian: CustodianCase,
    initiating: bool,
    distribution_freq: str,
    mgmt_payment: str,
    custody_payment: str,
    risk_summary_style: str,
) -> dict:
    suffix = "发起式联接基金" if initiating else "联接基金"
    fund_name = f"{scenario.fund_core}{suffix}"
    target_etf_name = scenario.fund_core
    return {
        "FUND_NAME": fund_name,
        "FUND_SHORT_NAME": scenario.short_core,
        "INDEX_NAME": scenario.index_name,
        "INDEX_CODE": scenario.index_code,
        "INDEX_COMPILER": scenario.index_compiler,
        "INDEX_WEBSITE": "www.csindex.com.cn"
        if scenario.index_compiler == "中证指数有限公司"
        else "www.cnindex.com.cn",
        "INDEX_DESCRIPTION": f"{scenario.index_name}用于测试不同市场条件下的指数描述。",
        "TARGET_ETF_NAME": target_etf_name,
        "TARGET_ETF_SHORT_NAME": scenario.short_core.replace("联接", ""),
        "BENCHMARK": f"{scenario.index_name}收益率×95%+活期存款基准利率×5%",
        "MANAGER_NAME": "南方基金管理股份有限公司",
        "MANAGER_ADDRESS": "深圳市福田区莲花街道益田路5999号基金大厦32-42楼",
        "MANAGER_LEGAL_REP": "周易",
        "MANAGER_ESTABLISHED": "1998年3月6日",
        "MANAGER_APPROVAL_NO": "中国证监会证监基字[1998]4号",
        "MANAGER_ORG_FORM": "股份有限公司",
        "MANAGER_REGISTERED_CAPITAL": "3.6172亿元人民币",
        "CUSTODIAN_NAME": custodian.name,
        "CUSTODIAN_TYPE": custodian.custodian_type,
        "CUSTODIAN_ADDRESS": "测试托管人注册地址",
        "CUSTODIAN_LEGAL_REP": "测试法定代表人",
        "CUSTODIAN_ESTABLISHED": "2000年1月1日",
        "CUSTODIAN_APPROVAL_NO": "测试批准文号",
        "CUSTODIAN_ORG_FORM": "股份有限公司",
        "CUSTODIAN_REGISTERED_CAPITAL": "100亿元人民币",
        "CUSTODIAN_CUSTODY_LICENSE": "证监许可〔2020〕1号",
        "HAS_HK_CONNECT": scenario.has_hk,
        "IS_INITIATING_FUND": initiating,
        "IS_CHUANGYE_FUND": scenario.is_chuangye,
        "IS_KECHUANG_FUND": scenario.is_kechuang,
        "MARKET_SCOPE": scenario.market_scope,
        "SINGLE_MARKET_EXCHANGE": scenario.single_exchange,
        "DISTRIBUTION_FREQ": distribution_freq,
        "MGMT_FEE_PAYMENT_METHOD": mgmt_payment,
        "CUSTODY_FEE_PAYMENT_METHOD": custody_payment,
        "RISK_SUMMARY_STYLE": risk_summary_style,
        "MGMT_FEE_RATE": "0.15",
        "CUSTODY_FEE_RATE": "0.05",
        "SALES_SERVICE_FEE_RATE": "0.20",
        "TARGET_ETF_RATIO_MIN": "90",
        "FUND_CODE_A": "019001",
        "FUND_CODE_C": "019002",
        "CONTRACT_DATE": "2026年3月",
        "COVER_DATE": "2026年3月",
        "REGISTRATION_APPROVAL_DATE": "2026年3月1日",
        "REGISTRATION_APPROVAL_NO": "证监许可〔2026〕1000号",
        "REGISTRATION_APPROVAL_YEAR": "2026",
        "REGISTRATION_APPROVAL_NUMBER": "1000",
        "FUND_MANAGER_NAME": "测试基金经理",
        "FUND_MANAGER_RESUME": "测试基金经理，具备基金从业资格，具有多年证券投资研究经验。",
        "SERVICE_ORGANIZATIONS_TEXT": "销售机构、登记机构、律师事务所及会计师事务所等相关服务机构信息已完整填写。",
        "OTHER_DISCLOSURE_TEXT": "本基金无其它应披露事项。",
        "CUSTODIAN_PROSPECTUS_TEXT": f"基金托管人\n一、基金托管人情况\n名称：{custodian.name}\n二、主要人员情况\n托管业务人员配备完整。\n三、证券投资基金托管情况\n托管业务运行正常。\n四、基金托管人的内部控制制度\n内部控制制度健全。\n五、基金托管人对基金管理人运作基金进行监督的方法和程序\n托管人依法监督基金投资运作。",
        "ACCOUNTING_FIRM_PROFILE": "四、审计基金财产的会计师事务所\n名称：测试会计师事务所\n经办注册会计师：测试人员。",
        "CUSTODY_AGREEMENT_SUMMARY_TEXT": "一、基金托管协议当事人\n基金管理人与基金托管人信息完整。\n二、基金托管人对基金管理人的业务监督和核查\n基金托管人依法履行监督职责。\n三、基金管理人对基金托管人的业务核查\n基金管理人依法进行核查。\n四、基金财产的保管\n基金财产独立保管。\n五、基金资产净值计算和会计核算\n基金资产净值依法计算。\n六、基金份额持有人名册的保管\n名册依法保管。\n七、适用法律与争议解决方式\n适用中华人民共和国法律。\n八、托管协议的变更、终止与基金财产的清算\n依法办理。",
    }


def iter_cases():
    for scenario, initiating, custodian, distribution, mgmt, custody, risk_style in itertools.product(
        SCENARIOS,
        (False, True),
        CUSTODIANS,
        DISTRIBUTION_FREQS,
        PAYMENT_METHODS,
        PAYMENT_METHODS,
        RISK_SUMMARY_STYLES,
    ):
        yield {
            "id": "|".join(
                [
                    scenario.id,
                    "init" if initiating else "standard",
                    custodian.id,
                    distribution,
                    f"mgmt={mgmt}",
                    f"custody={custody}",
                    risk_style,
                ]
            ),
            "scenario": scenario,
            "initiating": initiating,
            "custodian": custodian,
            "data": build_case(scenario, custodian, initiating, distribution, mgmt, custody, risk_style),
        }


def unresolved_tokens(text: str) -> list[str]:
    patterns = [
        r"\{\{\s*[^}]+?\s*\}\}",
        r"\{\{\s*(?:IF|ENDIF|ELSE)",
        r"JINJA_VAR_\d+",
        r"\{[A-Z][A-Z0-9_]{2,}\}",
        r"�",
    ]
    found: list[str] = []
    for pattern in patterns:
        for match in re.finditer(pattern, text):
            token = match.group(0)
            if token not in found:
                found.append(token)
    return found[:10]


def require(condition: bool, failures: list[str], message: str) -> None:
    if not condition:
        failures.append(message)


def section_between(text: str, start: str, end: str | None = None) -> str:
    start_idx = text.find(start)
    if start_idx == -1:
        return ""
    start_idx += len(start)
    if end is None:
        return text[start_idx:]
    end_idx = text.find(end, start_idx)
    if end_idx == -1:
        return text[start_idx:]
    return text[start_idx:end_idx]


def validate_common(doc_name: str, text: str, data: dict, failures: list[str]) -> None:
    require(bool(text and len(text) > 1000), failures, f"{doc_name}: 输出过短或为空")
    tokens = unresolved_tokens(text)
    require(not tokens, failures, f"{doc_name}: 存在未解析占位符/异常字符 {tokens}")
    quality = _generation_quality_check(text, doc_name)
    require(
        not quality["issues"],
        failures,
        f"{doc_name}: 生成后条件邻近文本检查发现问题 {quality['issues'][:5]}",
    )
    for value_key in ("FUND_NAME", "INDEX_NAME", "TARGET_ETF_NAME", "CUSTODIAN_NAME"):
        value = data[value_key]
        require(value in text, failures, f"{doc_name}: 未出现设置值 {value_key}={value}")


def validate_condition_conflicts(doc_name: str, text: str, data: dict, failures: list[str]) -> None:
    is_hk = bool(data["HAS_HK_CONNECT"])
    is_chuangye = bool(data["IS_CHUANGYE_FUND"])
    is_kechuang = bool(data["IS_KECHUANG_FUND"])
    is_special = is_hk or is_chuangye or is_kechuang

    if is_hk:
        require("港股通" in text, failures, f"{doc_name}: 港股通基金未出现港股通条款")
        require("创业板机制下" not in text, failures, f"{doc_name}: 港股通基金混入创业板机制风险")
        require("科创板机制下" not in text, failures, f"{doc_name}: 港股通基金混入科创板机制风险")
    elif is_chuangye:
        require("创业板" in text, failures, f"{doc_name}: 创业板基金未出现创业板条款")
        require("港股通机制" not in text, failures, f"{doc_name}: 创业板基金混入港股通机制风险")
        require("科创板机制下" not in text, failures, f"{doc_name}: 创业板基金混入科创板机制风险")
    elif is_kechuang:
        require("科创板" in text, failures, f"{doc_name}: 科创板基金未出现科创板条款")
        require("港股通机制" not in text, failures, f"{doc_name}: 科创板基金混入港股通机制风险")
        require("创业板机制下" not in text, failures, f"{doc_name}: 科创板基金混入创业板机制风险")
    elif not is_special:
        require("港股通机制" not in text, failures, f"{doc_name}: 普通非港股通基金混入港股通机制风险")
        require("创业板机制下" not in text, failures, f"{doc_name}: 普通基金混入创业板机制风险")
        require("科创板机制下" not in text, failures, f"{doc_name}: 普通基金混入科创板机制风险")

    if data["IS_INITIATING_FUND"]:
        require("发起式" in text, failures, f"{doc_name}: 发起式设置未体现")
        require("发起资金" in text or doc_name == "产品资料概要", failures, f"{doc_name}: 发起式基金缺少发起资金表述")
    else:
        require("发起资金" not in text, failures, f"{doc_name}: 普通基金混入发起资金条款")


def validate_payment_method(text: str, data: dict, failures: list[str]) -> None:
    expected = {
        "CONSULT": "协商一致",
        "AUTO": "自动",
        "AUTO_V2": "自动",
        "AUTO_CHECK": "核对",
    }
    for key, label in (
        ("MGMT_FEE_PAYMENT_METHOD", "管理费"),
        ("CUSTODY_FEE_PAYMENT_METHOD", "托管费"),
    ):
        enum_value = data[key]
        keyword = expected[enum_value]
        require(keyword in text, failures, f"基金合同: {label}划款方式 {enum_value} 未体现关键词“{keyword}”")


def validate_product_summary(text: str, data: dict, failures: list[str]) -> None:
    parts = text.split("\n\n---\n\n")
    require(len(parts) == 2, failures, "产品资料概要: 未生成 A/C 两类份额")
    for idx, part in enumerate(parts, start=1):
        share = "A" if idx == 1 else "C"
        require("### （一）风险揭示" in part, failures, f"产品资料概要{share}: 缺少风险揭示小节")
        require("一）本基金特有的风险" in part, failures, f"产品资料概要{share}: 缺少一）本基金特有的风险")
        require("二）市场风险" in part, failures, f"产品资料概要{share}: 缺少二）市场风险")
        specific = section_between(part, "一）本基金特有的风险", "二）市场风险")
        market = section_between(part, "二）市场风险", "三）管理风险")
        require("标的指数" in specific or "目标ETF" in specific, failures, f"产品资料概要{share}: 特有风险未体现联接基金核心风险")
        for title in SPECIFIC_RISK_TITLES:
            require(title not in market, failures, f"产品资料概要{share}: 市场风险混入特有风险“{title}”")


def validate_docx_bytes(label: str, docx_bytes: bytes, failures: list[str]) -> None:
    require(len(docx_bytes) > 1000, failures, f"{label}: DOCX 输出过短")
    try:
        with zipfile.ZipFile(io.BytesIO(docx_bytes)) as archive:
            names = set(archive.namelist())
            require("word/document.xml" in names, failures, f"{label}: DOCX 缺少 word/document.xml")
    except zipfile.BadZipFile:
        failures.append(f"{label}: DOCX 不是有效 zip/docx 文件")


def validate_docx_bundle(label: str, bundle_bytes: bytes, failures: list[str]) -> None:
    require(len(bundle_bytes) > 1000, failures, f"{label}: ZIP 输出过短")
    try:
        with zipfile.ZipFile(io.BytesIO(bundle_bytes)) as bundle:
            docx_names = [name for name in bundle.namelist() if name.endswith(".docx")]
            require(len(docx_names) == 2, failures, f"{label}: 未包含 A/C 两个 DOCX")
            for name in docx_names:
                with zipfile.ZipFile(io.BytesIO(bundle.read(name))) as docx:
                    require("word/document.xml" in set(docx.namelist()), failures, f"{label}: {name} 缺少 word/document.xml")
    except zipfile.BadZipFile:
        failures.append(f"{label}: 不是有效 zip 文件")


def run_case(case: dict, engines: tuple[ContractEngine, ProspectusEngine, ProductSummaryEngine], docx: bool) -> list[str]:
    contract_engine, prospectus_engine, product_summary_engine = engines
    data = case["data"]
    failures: list[str] = []

    contract = contract_engine.generate(data)
    prospectus = prospectus_engine.generate(data)
    summary_bundle = product_summary_engine.generate_bundle({**data, "PROSPECTUS_TEXT": prospectus})
    summary = summary_bundle["text"]

    validate_common("基金合同", contract, data, failures)
    validate_common("招募说明书", prospectus, data, failures)
    validate_common("产品资料概要", summary, data, failures)
    validate_condition_conflicts("基金合同", contract, data, failures)
    validate_condition_conflicts("招募说明书", prospectus, data, failures)
    validate_payment_method(contract, data, failures)
    validate_product_summary(summary, data, failures)

    if docx:
        validate_docx_bytes("基金合同DOCX", contract_engine.build_docx(contract), failures)
        validate_docx_bytes("招募说明书DOCX", prospectus_engine.build_docx(prospectus), failures)
        validate_docx_bundle(
            "产品资料概要DOCX包",
            product_summary_engine.build_docx_bundle(summary_bundle["render_models"]),
            failures,
        )

    return failures


def init_worker() -> None:
    global _WORKER_ENGINES
    _WORKER_ENGINES = (ContractEngine(), ProspectusEngine(), ProductSummaryEngine())


def run_case_worker(case: dict) -> tuple[str, list[str]]:
    global _WORKER_ENGINES
    if _WORKER_ENGINES is None:
        init_worker()
    assert _WORKER_ENGINES is not None
    try:
        return case["id"], run_case(case, _WORKER_ENGINES, False)
    except Exception as exc:  # noqa: BLE001 - report the exact case.
        return case["id"], [f"生成异常: {type(exc).__name__}: {exc}"]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-failures", type=int, default=50)
    parser.add_argument(
        "--docx",
        choices=("none", "smoke", "all"),
        default="smoke",
        help="DOCX validation mode. smoke checks the first case per scenario/custodian.",
    )
    parser.add_argument("--limit", type=int, default=0, help="Limit the number of cases checked; 0 means all.")
    parser.add_argument("--workers", type=int, default=1, help="Parallel workers for text-only validation.")
    parser.add_argument(
        "--smoke-only",
        action="store_true",
        help="Run only the first case for each scenario/custodian group; useful with --docx smoke.",
    )
    args = parser.parse_args()

    cases = list(iter_cases())
    engines = (ContractEngine(), ProspectusEngine(), ProductSummaryEngine())
    failures: list[tuple[str, str]] = []
    docx_smoked: set[tuple[str, str]] = set()
    started = time.time()

    if args.smoke_only:
        seen_groups: set[tuple[str, str]] = set()
        selected_cases = []
        for case in cases:
            group = (case["scenario"].id, case["custodian"].id)
            if group in seen_groups:
                continue
            seen_groups.add(group)
            selected_cases.append(case)
    else:
        selected_cases = cases
    selected_cases = selected_cases[: args.limit] if args.limit else selected_cases

    if args.workers > 1 and args.docx == "none":
        checked = 0
        with ProcessPoolExecutor(max_workers=args.workers, initializer=init_worker) as executor:
            futures = [executor.submit(run_case_worker, case) for case in selected_cases]
            for future in as_completed(futures):
                checked += 1
                case_id, case_failures = future.result()
                for failure in case_failures:
                    failures.append((case_id, failure))
                    if len(failures) >= args.max_failures:
                        break
                if checked % 100 == 0:
                    elapsed = time.time() - started
                    print(f"checked {checked}/{len(selected_cases)} cases in {elapsed:.1f}s", flush=True)
                if len(failures) >= args.max_failures:
                    for pending in futures:
                        pending.cancel()
                    break
        index = checked
    else:
        for index, case in enumerate(selected_cases, start=1):
            scenario_id = case["scenario"].id
            custodian_id = case["custodian"].id
            do_docx = args.docx == "all"
            if args.docx == "smoke" and (scenario_id, custodian_id) not in docx_smoked:
                do_docx = True
                docx_smoked.add((scenario_id, custodian_id))
            try:
                case_failures = run_case(case, engines, do_docx)
            except Exception as exc:  # noqa: BLE001 - report the exact case and continue.
                case_failures = [f"生成异常: {type(exc).__name__}: {exc}"]

            for failure in case_failures:
                failures.append((case["id"], failure))
                if len(failures) >= args.max_failures:
                    break
            if len(failures) >= args.max_failures:
                break

            if index % 100 == 0:
                elapsed = time.time() - started
                print(f"checked {index}/{len(selected_cases)} cases in {elapsed:.1f}s", flush=True)

    if not selected_cases:
        index = 0

    elapsed = time.time() - started
    print(f"\nTotal cases planned: {len(cases)}")
    print(f"Cases checked: {index}")
    print(f"DOCX smoke groups checked: {len(docx_smoked)}")
    print(f"Failures: {len(failures)}")
    print(f"Elapsed: {elapsed:.1f}s")

    if failures:
        print("\nFirst failures:")
        for case_id, failure in failures[: args.max_failures]:
            print(f"- [{case_id}] {failure}")
        return 1

    print("\nAll exhaustive generation QA checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

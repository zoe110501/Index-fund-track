"""Regression QA for the legal-file review workflow.

The script exercises the real Flask review endpoints with in-memory DOCX files.
It intentionally injects issues that the review feature must surface in a
machine-readable way so the UI/report can display them reliably.
"""

from __future__ import annotations

import io
import sys
from pathlib import Path

import pytest
from docx import Document


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app import _review_store, _strip_contract_signing_page_text, app  # noqa: E402


@pytest.fixture()
def client():
    with app.test_client() as test_client:
        yield test_client


def _docx_bytes(lines: list[str]) -> io.BytesIO:
    doc = Document()
    for line in lines:
        doc.add_paragraph(line)
    buf = io.BytesIO()
    doc.save(buf)
    buf.seek(0)
    return buf


def _upload(client, contract_lines: list[str], prospectus_lines: list[str] | None = None):
    _review_store.clear()
    data = {
        "contract": (_docx_bytes(contract_lines), "测试ETF联接基金基金合同.docx"),
    }
    if prospectus_lines is not None:
        data["prospectus"] = (_docx_bytes(prospectus_lines), "测试ETF联接基金招募说明书.docx")
    resp = client.post("/api/review/upload", data=data, content_type="multipart/form-data")
    payload = resp.get_json()
    assert resp.status_code == 200, payload
    assert payload["success"] is True, payload
    return payload


def _get(client, path: str):
    resp = client.post(path, json={})
    payload = resp.get_json()
    assert resp.status_code == 200, payload
    assert payload["success"] is True, payload
    return payload


def _find_by_text(items: list[dict], needle: str) -> dict:
    for item in items:
        blob = " ".join(str(v) for v in item.values())
        if needle in blob:
            return item
    raise AssertionError(f"未找到包含 {needle!r} 的复核结果")


def test_format_check_detects_template_and_duplicate_issues(client):
    contract = [
        "测试ETF联接基金 基金合同",
        "第一部分 前言",
        "本基金合同存在未闭合括号（需要被检查出来",
        "{{IF COND_SHOULD_BE_REMOVED}}",
        "一、重复标题",
        "一、重复标题",
        "这是重复段落。",
        "这是重复段落。",
    ]
    _upload(client, contract)
    data = _get(client, "/api/review/format_check")
    issue_types = {item.get("type") for item in data["issues"]}
    assert "unclosed_bracket" in issue_types, data
    assert "unresolved_condition_tag" in issue_types, data
    assert "adjacent_duplicate_heading" in issue_types, data
    assert "adjacent_duplicate_text" in issue_types, data
    assert all(item.get("description") for item in data["issues"]), data


def test_format_check_allows_repeated_boilerplate_across_subsections(client):
    repeated_clause = (
        "基金管理人可在法律法规允许的情况下，对上述原则进行调整。"
        "基金管理人必须在新规则开始实施前依照《信息披露办法》的有关规定在规定媒介上公告。"
    )
    contract = [
        "测试ETF联接基金 基金合同",
        "第一部分 前言",
        "三、申购与赎回的原则",
        "5、办理申购、赎回业务时，应当遵循基金份额持有人利益优先原则。",
        repeated_clause,
        "四、申购与赎回的程序",
        "3、申购和赎回申请的确认",
        "基金销售机构对申购、赎回申请的受理并不代表申请一定成功。",
        repeated_clause,
    ]
    _upload(client, contract)
    data = _get(client, "/api/review/format_check")
    issue_types = {item.get("type") for item in data["issues"]}
    assert "near_duplicate_paragraph" not in issue_types, data


def test_format_check_bracket_issue_includes_context_and_column(client):
    contract = [
        "测试ETF联接基金 基金合同",
        "第一部分 前言",
        "这一行有多余右括号)需要定位",
    ]
    _upload(client, contract)
    data = _get(client, "/api/review/format_check")
    issue = _find_by_text(data["issues"], "多余的闭合括号")
    assert issue["type"] == "extra_close_bracket", issue
    assert issue["line"] == 2, issue
    assert issue["column"] == 10, issue
    assert "这一行有多余右括号" in issue["text"], issue
    assert any("<mark>)</mark>" in ctx.get("html", "") for ctx in issue["context"]), issue


def test_format_check_allows_inline_numbered_brackets(client):
    contract = [
        "测试ETF联接基金 基金合同",
        "第一部分 前言",
        "64、特定资产：包括：（一）无可参考的活跃市场价格且采用估值技术仍导致公允价值存在重大不确定性的资产；（二）按摊余成本计量且计提资产减值准备仍导致资产价值存在重大不确定性的资产；（三）其他资产价值存在重大不确定性的资产",
        "基金管理人可以根据前述“（1）全额赎回”或“（2）部分延期赎回”的约定方式办理。",
    ]
    _upload(client, contract)
    data = _get(client, "/api/review/format_check")
    issue_types = {item.get("type") for item in data["issues"]}
    assert "extra_close_bracket" not in issue_types, data
    assert "unclosed_bracket" not in issue_types, data


def test_format_check_isolated_bracket_context_skips_blank_lines(client):
    _review_store.clear()
    _review_store["data"] = {
        "contract_text": "\n".join([
            "第一部分 前言",
            "前一段用于定位。",
            "",
            "",
            "(",
            "",
            "",
            "后一段用于定位。",
        ]),
    }
    data = _get(client, "/api/review/format_check")
    issue = _find_by_text(data["issues"], "未闭合的括号")
    context_text = "\n".join(ctx.get("text", "") for ctx in issue.get("context", []))
    assert "前一段用于定位。" in context_text, issue
    assert "后一段用于定位。" in context_text, issue
    assert issue.get("section", {}).get("text") == "第一部分 前言", issue


def test_format_check_ignores_signing_page_bracket_split_by_page(client):
    _review_store.clear()
    body_lines = ["第一部分 前言"] + [f"这是正文第{i}段。" for i in range(1, 40)]
    _review_store["data"] = {
        "contract_text": "\n".join(body_lines + [
            "",
            "(",
            "本页无正文，为《测试ETF联接基金基金合同（草案）》签字页）",
            "",
            "基金管理人：测试基金管理有限公司（法人盖章）",
        ]),
    }
    stripped = _strip_contract_signing_page_text(_review_store["data"]["contract_text"])
    assert "本页无正文" not in stripped
    assert not stripped.rstrip().endswith("("), stripped
    data = _get(client, "/api/review/format_check")
    issue_types = {item.get("type") for item in data["issues"]}
    assert "unclosed_bracket" not in issue_types, data


def test_consistency_check_returns_display_ready_issue_fields(client):
    contract = [
        "测试ETF联接基金 基金合同",
        "第一部分 前言",
        "基金管理人：甲方基金管理有限公司",
        "基金管理人：乙方基金管理有限公司",
    ]
    _upload(client, contract)
    data = _get(client, "/api/review/consistency_check")
    issue = _find_by_text(data["issues"], "基金管理人名称不一致")
    assert issue["severity"] == "error", issue
    assert issue.get("description"), issue
    assert issue.get("is_problem") is True, issue


def test_cross_check_detects_contract_prospectus_fee_mismatch(client):
    contract = [
        "测试ETF联接基金 基金合同",
        "第十五部分 基金费用与税收",
        "1、基金管理人的管理费",
        "本基金的管理费按前一日基金资产净值的0.50%年费率计提。",
        "2、基金托管人的托管费",
        "本基金的托管费按前一日基金资产净值的0.10%年费率计提。",
    ]
    prospectus = [
        "测试ETF联接基金 招募说明书",
        "第十一部分 费用与税收",
        "1、基金管理人的管理费",
        "本基金的管理费按前一日基金资产净值的0.80%年费率计提。",
        "2、基金托管人的托管费",
        "本基金的托管费按前一日基金资产净值的0.10%年费率计提。",
    ]
    _upload(client, contract, prospectus)
    data = _get(client, "/api/review/cross_check")
    issue = _find_by_text(data["results"], "基金费用与税收")
    assert issue["severity"] in ("error", "warning"), issue
    assert issue["status"] in ("error", "warning"), issue
    assert issue["is_problem"] is True, issue
    assert "0.50%" in issue["message"] and "0.80%" in issue["message"], issue
    assert issue.get("hunks") or issue.get("diffs"), issue


def test_summary_cross_check_returns_problem_list_and_report_data(client):
    contract = [
        "测试ETF联接基金 基金合同",
        "第二十四部分 基金合同的内容摘要",
        "四、基金费用与税收",
        "本基金的管理费按0.50%年费率计提。",
    ]
    prospectus = [
        "测试ETF联接基金 招募说明书",
        "第十七部分 基金合同的内容摘要",
        "四、基金费用与税收",
        "本基金的管理费按0.80%年费率计提。",
    ]
    _upload(client, contract, prospectus)
    data = _get(client, "/api/review/summary_cross_check")
    assert data["problem_count"] >= 1, data
    issue = _find_by_text(data["results"], "四、基金费用与税收")
    assert issue["is_problem"] is True, issue
    assert issue["severity"] in ("error", "warning"), issue
    assert "0.50%" in issue["message"] or "0.80%" in issue["message"], issue

    report = client.post(
        "/api/review/report",
        json={
            "fund_name": "测试ETF联接基金",
            "cross_check": [],
            "summary_cross_check": data,
            "format_issues": [],
            "consistency_issues": [],
        },
    )
    html = report.get_data(as_text=True)
    assert report.status_code == 200, html[:500]
    assert "四、基金费用与税收" in html, html[:1000]
    assert "0.50%" in html or "0.80%" in html, html[:1000]


def test_review_detail_dom_has_only_valid_result_cards(client):
    html = (ROOT / "templates" / "index.html").read_text(encoding="utf-8")
    marker = '<div id="review-detail-area"'
    start = html.find(marker)
    assert start >= 0, "未找到复核详情区"
    end = html.find('</div><!-- /review-panel -->', start)
    assert end > start, "复核详情区未在 review-panel 内闭合"
    detail_html = html[start:end]

    expected_ids = {
        "rv-format-detail",
        "rv-consistency-detail",
        "rv-summary-cross-detail",
        "rv-cross-detail",
        "rv-ai-detail",
    }
    found_card_ids = set()
    for card_id in expected_ids:
        if f'id="{card_id}"' in detail_html:
            found_card_ids.add(card_id)
    assert found_card_ids == expected_ids, found_card_ids
    assert 'id="rv-summary-body"' not in detail_html, "存在孤立的摘要详情 body，会破坏复核详情区 DOM"


def main() -> int:
    tests = [
        test_format_check_detects_template_and_duplicate_issues,
        test_format_check_allows_repeated_boilerplate_across_subsections,
        test_format_check_bracket_issue_includes_context_and_column,
        test_format_check_allows_inline_numbered_brackets,
        test_format_check_isolated_bracket_context_skips_blank_lines,
        test_format_check_ignores_signing_page_bracket_split_by_page,
        test_consistency_check_returns_display_ready_issue_fields,
        test_cross_check_detects_contract_prospectus_fee_mismatch,
        test_summary_cross_check_returns_problem_list_and_report_data,
        test_review_detail_dom_has_only_valid_result_cards,
    ]
    failures = 0
    with app.test_client() as client:
        for test in tests:
            try:
                test(client)
                print(f"PASS {test.__name__}")
            except Exception as exc:
                failures += 1
                print(f"FAIL {test.__name__}: {exc}")
    if failures:
        print(f"review_feature_qa failed: {failures}/{len(tests)}")
        return 1
    print(f"review_feature_qa passed: {len(tests)}/{len(tests)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

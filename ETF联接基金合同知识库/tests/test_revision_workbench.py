from __future__ import annotations

import io
import json
import sys
import zipfile
from pathlib import Path

from werkzeug.datastructures import MultiDict


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import app  # noqa: E402


W_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"


def _p(inner: str) -> str:
    return f"<w:p>{inner}</w:p>"


def _r(text: str) -> str:
    return f"<w:r><w:t>{text}</w:t></w:r>"


def _ins(text: str) -> str:
    return f'<w:ins w:author="Llinks" w:date="2026-05-09T09:54:00Z"><w:r><w:t>{text}</w:t></w:r></w:ins>'


def _delete(text: str) -> str:
    return f'<w:del w:author="Llinks" w:date="2026-05-09T09:54:00Z"><w:r><w:delText>{text}</w:delText></w:r></w:del>'


def _minimal_docx(paragraphs: list[str], comments_xml: str = "") -> bytes:
    document_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        f'<w:document xmlns:w="{W_NS}"><w:body>'
        + "".join(paragraphs)
        + "</w:body></w:document>"
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("[Content_Types].xml", "<Types/>")
        zf.writestr("word/document.xml", document_xml)
        if comments_xml:
            zf.writestr("word/comments.xml", comments_xml)
    return buf.getvalue()


def _tc(*paragraphs: str) -> str:
    return "<w:tc>" + "".join(paragraphs) + "</w:tc>"


def _tr(*cells: str) -> str:
    return "<w:tr>" + "".join(cells) + "</w:tr>"


def _tbl(*rows: str) -> str:
    return "<w:tbl>" + "".join(rows) + "</w:tbl>"


def _document_xml(docx_bytes: bytes) -> str:
    with zipfile.ZipFile(io.BytesIO(docx_bytes), "r") as zf:
        return zf.read("word/document.xml").decode("utf-8", errors="ignore")


def _settings_xml(docx_bytes: bytes) -> str:
    with zipfile.ZipFile(io.BytesIO(docx_bytes), "r") as zf:
        return zf.read("word/settings.xml").decode("utf-8", errors="ignore")


def test_revision_workbench_template_contains_review_author_control():
    index_html = (ROOT / "templates" / "index.html").read_text(encoding="utf-8")

    assert "let reviewRevisionAuthor" in index_html
    assert "revision-author-input" in index_html
    assert "review_author" in index_html


def test_revision_document_view_groups_modify_pairs_and_contract_summary_scope():
    docx_bytes = _minimal_docx([
        _p(_r("第一部分  前言")),
        _p(_delete("旧表述") + _ins("新表述")),
        _p(_r("第二十四部分  基金合同内容摘要")),
        _p(_ins("摘要新增内容")),
    ])

    document = app._build_revision_document_view(docx_bytes, document_kind="contract", filename="contract.docx")

    assert document["revision_counts"]["contract_body"] == 1
    assert document["revision_counts"]["contract_summary"] == 1
    assert document["revisions"][0]["revision_type"] == "modify"
    assert document["revisions"][0]["old_text"] == "旧表述"
    assert document["revisions"][0]["new_text"] == "新表述"
    assert document["revisions"][1]["document_scope"] == "contract_summary"


def test_revision_workbench_upload_classifies_prospectus_summary_but_not_custody_summary():
    prospectus_bytes = _minimal_docx([
        _p(_r("基金合同内容摘要")),
        _p(_ins("招募内合同摘要新增")),
        _p(_r("基金托管协议内容摘要")),
        _p(_ins("托管协议摘要新增")),
    ])
    client = app.app.test_client()
    response = client.post(
        "/api/revision_workbench/upload",
        data={
            "prospectus_revision_docx": (io.BytesIO(prospectus_bytes), "prospectus.docx"),
        },
        content_type="multipart/form-data",
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["revision_counts"]["prospectus_contract_summary"] == 1
    assert payload["revision_counts"]["prospectus_body"] == 1
    scopes = [item["document_scope"] for item in payload["revision_items"]]
    assert scopes == ["prospectus_contract_summary", "prospectus_body"]


def test_revision_workbench_upload_accepts_docx_without_revision_marks():
    contract_bytes = _minimal_docx([
        _p(_r("第一部分  前言")),
        _p(_r("这是一份没有 Word 修订痕迹的普通合同文件。")),
    ])
    client = app.app.test_client()

    response = client.post(
        "/api/revision_workbench/upload",
        data={
            "contract_revision_docx": (io.BytesIO(contract_bytes), "contract.docx"),
        },
        content_type="multipart/form-data",
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["success"] is True
    assert payload["revision_total"] == 0
    assert payload["revision_items"] == []
    assert payload["documents"][0]["document_kind"] == "contract"


def test_revision_workbench_template_is_dedicated_page_and_keeps_file_review():
    html = (ROOT / "templates" / "index.html").read_text(encoding="utf-8")

    assert 'data-panel="revision-workbench"' in html
    assert 'id="revision-workbench-panel"' in html
    assert 'id="review-panel"' in html
    assert "/api/revision_workbench/upload" in html
    assert "startRevisionWorkbench" in html
    assert "revision-reader-layout" in html
    assert "toggleSelectCurrentRevisions" in html
    assert "/api/revision_workbench/export_final" in html
    assert "let reviewRevisionEdits = {}" in html
    assert "revision-edit-box" in html
    assert "revision.context" in html
    assert "mode: 'paragraph'" in html
    assert "revision-edit-paragraph" in html
    assert "revision_edits" in html
    assert "已编辑" in html


def test_revision_workbench_template_supports_custody_and_batch_product_summary_uploads():
    html = (ROOT / "templates" / "index.html").read_text(encoding="utf-8")

    assert 'id="revision-custody-file"' in html
    assert "custody_agreement" in html
    assert 'id="revision-product-summary-file" accept=".docx" multiple' in html
    assert "for (const file of productSummaryInput.files)" in html


def test_revision_workbench_upload_accepts_custody_and_multiple_product_summaries():
    custody_bytes = _minimal_docx([
        _p(_r("第一章 基金托管协议当事人")),
        _p(_ins("托管协议新增文字")),
    ])
    summary_a_bytes = _minimal_docx([
        _p(_r("A类份额基金产品资料概要")),
        _p(_ins("A类概要新增文字")),
    ])
    summary_c_bytes = _minimal_docx([
        _p(_r("C类份额基金产品资料概要")),
        _p(_ins("C类概要新增文字")),
    ])

    client = app.app.test_client()
    response = client.post(
        "/api/revision_workbench/upload",
        data=MultiDict([
            ("custody_agreement_revision_docx", (io.BytesIO(custody_bytes), "托管协议.docx")),
            ("product_summary_revision_docx", (io.BytesIO(summary_a_bytes), "产品资料概要_A.docx")),
            ("product_summary_revision_docx", (io.BytesIO(summary_c_bytes), "产品资料概要_C.docx")),
        ]),
        content_type="multipart/form-data",
    )

    assert response.status_code == 200
    payload = response.get_json()
    documents = payload["documents"]
    assert any(document["document_kind"] == "custody_agreement" for document in documents)
    product_docs = [document for document in documents if document["document_kind"].startswith("product_summary")]
    assert len(product_docs) == 2
    revision_ids = [item["id"] for item in payload["revision_items"]]
    assert len(revision_ids) == len(set(revision_ids))
    assert payload["revision_counts"]["custody_agreement"] == 1


def test_revision_decisions_apply_edit_to_modify_redline():
    docx_bytes = _minimal_docx([
        _p(_delete("old fee") + _ins("new fee")),
    ])
    document = app._build_revision_document_view(docx_bytes, document_kind="contract", filename="contract.docx")
    revision = document["revisions"][0]

    final_bytes, report = app._apply_review_revision_decisions_to_docx(
        docx_bytes,
        document_kind="contract",
        filename="contract.docx",
        decisions={revision["id"]: "accept"},
        revision_edits={revision["id"]: {"text": "edited fee"}},
    )

    xml = _document_xml(final_bytes)
    assert "old fee" in xml
    assert "edited fee" in xml
    assert "new fee" not in xml
    assert report["edited_count"] == 1


def test_revision_decisions_apply_paragraph_edit_to_modify_redline():
    docx_bytes = _minimal_docx([
        _p(_r("before ") + _delete("old fee") + _ins("new fee") + _r(" after")),
    ])
    document = app._build_revision_document_view(docx_bytes, document_kind="contract", filename="contract.docx")
    revision = document["revisions"][0]

    final_bytes, report = app._apply_review_revision_decisions_to_docx(
        docx_bytes,
        document_kind="contract",
        filename="contract.docx",
        decisions={revision["id"]: "accept"},
        revision_edits={revision["id"]: {"text": "before edited paragraph after", "mode": "paragraph"}},
    )

    xml = _document_xml(final_bytes)
    assert "<w:del" in xml
    assert "<w:ins" in xml
    assert "old fee" in xml
    assert "edited paragraph" in xml
    assert "new fee" not in xml
    assert report["edited_count"] == 1


def test_revision_decisions_apply_edit_to_insert_and_delete_redlines():
    insert_bytes = _minimal_docx([_p(_ins("insert original"))])
    insert_doc = app._build_revision_document_view(insert_bytes, document_kind="contract", filename="contract.docx")
    insert_revision = insert_doc["revisions"][0]
    insert_final, insert_report = app._apply_review_revision_decisions_to_docx(
        insert_bytes,
        document_kind="contract",
        filename="contract.docx",
        decisions={insert_revision["id"]: "accept"},
        revision_edits={insert_revision["id"]: {"text": "insert edited"}},
    )

    delete_bytes = _minimal_docx([_p(_delete("delete original"))])
    delete_doc = app._build_revision_document_view(delete_bytes, document_kind="contract", filename="contract.docx")
    delete_revision = delete_doc["revisions"][0]
    delete_final, delete_report = app._apply_review_revision_decisions_to_docx(
        delete_bytes,
        document_kind="contract",
        filename="contract.docx",
        decisions={delete_revision["id"]: "accept"},
        revision_edits={delete_revision["id"]: {"text": "delete replacement"}},
    )

    insert_xml = _document_xml(insert_final)
    delete_xml = _document_xml(delete_final)
    assert "insert edited" in insert_xml
    assert "insert original" not in insert_xml
    assert "delete original" in delete_xml
    assert "delete replacement" in delete_xml
    assert "<w:ins" in delete_xml
    assert insert_report["edited_count"] == 1
    assert delete_report["edited_count"] == 1


def test_revision_decisions_ignore_edit_when_rejected():
    docx_bytes = _minimal_docx([
        _p(_delete("old fee") + _ins("new fee")),
    ])
    document = app._build_revision_document_view(docx_bytes, document_kind="contract", filename="contract.docx")
    revision = document["revisions"][0]

    final_bytes, report = app._apply_review_revision_decisions_to_docx(
        docx_bytes,
        document_kind="contract",
        filename="contract.docx",
        decisions={revision["id"]: "reject"},
        revision_edits={revision["id"]: {"text": "rejected edit"}},
    )

    xml = _document_xml(final_bytes)
    assert "old fee" in xml
    assert "new fee" not in xml
    assert "rejected edit" not in xml
    assert report["edited_count"] == 0


def test_contract_summary_redline_renumbers_equal_following_items():
    docx_bytes = _minimal_docx([
        _p(_r("第二十四部分  基金合同内容摘要")),
        _p(_r("一、基金合同当事人的权利、义务")),
        _p(_r("（8）返还在基金交易过程中因任何原因获得的不当得利；")),
        _p(_r("（9）法律法规及中国证监会规定的和《基金合同》约定的其他义务。")),
        _p(_r("第二十五部分  其他事项")),
    ])

    updated, report = app._replace_docx_section_content_with_redline(
        docx_bytes,
        is_heading=app._contract_summary_heading_match,
        is_stop_heading=app._contract_summary_stop_heading_match,
        new_text=(
            "第二十四部分  基金合同内容摘要\n"
            "一、基金合同当事人的权利、义务\n"
            "（8）返还在基金交易过程中因任何原因获得的不当得利；\n"
            "（9）发起资金提供方持有认购的基金份额不少于3年；\n"
            "（10）法律法规及中国证监会规定的和《基金合同》约定的其他义务。"
        ),
        report_label="基金合同第 24 部分摘要",
        review_author="产品部审核",
    )

    xml = _document_xml(updated)
    assert report["status"] == "redlined"
    assert "发起资金提供方持有认购的基金份额不少于3年" in xml
    assert "<w:delText>（9）</w:delText>" in xml
    assert "<w:t>（10）</w:t>" in xml


def test_revision_workbench_export_final_returns_zip_for_decided_revisions():
    contract_bytes = _minimal_docx([
        _p(_r("第一部分  前言")),
        _p(_delete("旧合同文字") + _ins("新合同文字")),
    ])
    prospectus_bytes = _minimal_docx([
        _p(_r("一、绪言")),
        _p(_ins("新增招募文字")),
    ])

    client = app.app.test_client()
    upload = client.post(
        "/api/revision_workbench/upload",
        data={
            "contract_revision_docx": (io.BytesIO(contract_bytes), "contract.docx"),
            "prospectus_revision_docx": (io.BytesIO(prospectus_bytes), "prospectus.docx"),
        },
        content_type="multipart/form-data",
    )
    assert upload.status_code == 200
    payload = upload.get_json()
    decisions = {item["id"]: "accept" for item in payload["revision_items"]}
    first_revision_id = next(item["id"] for item in payload["revision_items"] if item["revision_type"] in {"insert", "modify", "delete"})

    response = client.post(
        "/api/revision_workbench/export_final",
        json={
            "decisions": decisions,
            "revision_edits": {first_revision_id: {"text": "Manual linked export edit"}},
            "review_author": "产品部审核",
            "form_data": {},
            "session_operations": [],
        },
    )

    assert response.status_code == 200
    assert response.mimetype == "application/zip"
    with zipfile.ZipFile(io.BytesIO(response.data), "r") as zf:
        names = set(zf.namelist())
        json_payloads = [json.loads(zf.read(name).decode("utf-8")) for name in names if name.endswith(".json")]
        decision_record = next(item for item in json_payloads if "decisions" in item)
        decision_csv = next(zf.read(name).decode("utf-8-sig") for name in names if name.endswith(".csv"))
    assert "基金合同.docx" in names
    assert "招募说明书.docx" in names
    assert "审核决策记录.json" in names
    assert "同步报告.json" in names

    assert decision_record["edited_count"] == 1
    assert decision_record["review_author"] == "产品部审核"
    assert decision_record["revision_edits"][first_revision_id]["text"] == "Manual linked export edit"
    assert "edited_text" in decision_csv.splitlines()[0]
    assert "Manual linked export edit" in decision_csv


def test_revision_finalizer_marks_docx_fields_for_update(monkeypatch):
    monkeypatch.setenv("DISABLE_WORD_FIELD_UPDATE", "1")
    finalized = app._finalize_review_docx_for_export(_minimal_docx([_p(_r("目录")), _p(_r("1"))]))

    settings_xml = _settings_xml(finalized)
    assert "updateFields" in settings_xml
    assert 'w:val="true"' in settings_xml


def test_sync_review_custody_summary_redlines_prospectus_summary():
    custody_bytes = _minimal_docx([
        _p(_r("第一章 基金托管协议当事人")),
        _p(_r("托管协议当事人最终正文。")),
        _p(_r("第二章 基金托管人对基金管理人的监督和核查")),
        _p(_r("监督核查最终正文。")),
    ])
    prospectus_bytes = _minimal_docx([
        _p(_r("基金托管协议的内容摘要")),
        _p(_r("旧托管协议摘要正文。")),
        _p(_r("基金份额持有人服务")),
    ])

    updated, report = app._sync_review_custody_summary_redlines(custody_bytes, prospectus_bytes)

    xml = _document_xml(updated)
    assert report["summary"]["source_type"] == "custody_agreement"
    assert report["apply"]["status"] == "redlined"
    assert "旧托管协议摘要正文" in xml
    assert "托管协议当事人最终正文" in xml
    assert "<w:del" in xml
    assert "<w:ins" in xml


def test_export_final_outputs_each_uploaded_linked_product_summary_share(monkeypatch):
    monkeypatch.setenv("DISABLE_WORD_FIELD_UPDATE", "1")
    contract_bytes = _minimal_docx([
        _p(_r("第一部分  前言")),
        _p(_ins("合同新增文字")),
    ])
    prospectus_bytes = _minimal_docx([
        _p(_r("一、绪言")),
        _p(_ins("招募新增文字")),
    ])
    summary_a_bytes = _minimal_docx([
        _p(_r("A类份额基金产品资料概要")),
        _p(_r("A类概要正文。")),
    ])
    summary_c_bytes = _minimal_docx([
        _p(_r("C类份额基金产品资料概要")),
        _p(_r("C类概要正文。")),
    ])

    client = app.app.test_client()
    upload = client.post(
        "/api/revision_workbench/upload",
        data=MultiDict([
            ("contract_revision_docx", (io.BytesIO(contract_bytes), "contract.docx")),
            ("prospectus_revision_docx", (io.BytesIO(prospectus_bytes), "prospectus.docx")),
            ("product_summary_revision_docx", (io.BytesIO(summary_a_bytes), "产品资料概要_A.docx")),
            ("product_summary_revision_docx", (io.BytesIO(summary_c_bytes), "产品资料概要_C.docx")),
        ]),
        content_type="multipart/form-data",
    )
    assert upload.status_code == 200
    payload = upload.get_json()
    decisions = {item["id"]: "accept" for item in payload["revision_items"]}

    response = client.post(
        "/api/revision_workbench/export_final",
        json={"decisions": decisions, "revision_edits": {}, "form_data": {}, "session_operations": []},
    )

    assert response.status_code == 200
    with zipfile.ZipFile(io.BytesIO(response.data), "r") as zf:
        names = set(zf.namelist())
        sync_report = json.loads(zf.read("同步报告.json").decode("utf-8"))

    assert "产品资料概要-A类.docx" in names
    assert "产品资料概要-C类.docx" in names
    assert len(sync_report["product_summaries"]) == 2


def test_review_sync_line_rows_supports_minimal_redline_normalizer():
    rows = app._review_sync_line_rows("1、 本基金合同约定\n\n|---|---|")

    assert rows == [{
        "line": "1、 本基金合同约定",
        "compare": "本基金合同约定",
        "lineno": 1,
    }]


def test_review_template_source_sync_applies_prospectus_postprocess_before_redline():
    contract = _minimal_docx([
        _p(_r("第二部分  释义")),
        _p(_r("在本基金合同中，除非文意另有所指，下列词语或简称具有如下含义：")),
        _p(_r("1、基金合同或本基金合同：指更新后的基金合同文本")),
        _p(_r("第三部分  基金合同的基本情况")),
    ])
    prospectus = _minimal_docx([
        _p(_r("第一部分  释义")),
        _p(_r("在本招募说明书中，除非文意另有所指，下列词语或简称具有如下含义：")),
        _p(_r("1、基金合同：指旧基金合同文本")),
        _p(_r("第二部分  基金管理人")),
    ])

    updated, report = app._sync_review_direct_contract_sources_redlines(contract, prospectus)

    xml = _document_xml(updated)
    assert report["updated_count"] == 1
    assert report["updated"][0]["source"] == "prospectus_template"
    assert "在本招募说明书中" in xml
    assert "基金合同或本基金合同" not in xml
    assert "旧基金合同文本" in xml
    assert "更新后的基金合同文本" in xml
    assert "<w:del" in xml
    assert "<w:ins" in xml


def test_review_direct_sync_uses_matched_subheading_body_only():
    contract = _minimal_docx([
        _p(_r("第十五部分  基金费用与税收")),
        _p(_r("一、基金费用的种类")),
        _p(_r("本基金管理费率为0.20%，托管费率为0.05%。")),
        _p(_r("二、基金费用计提方法、计提标准和支付方式")),
        _p(_r("计提方法合同正文。")),
        _p(_r("第十六部分  基金的收益与分配")),
    ])
    prospectus = _minimal_docx([
        _p(_r("第十一部分  费用与税收")),
        _p(_r("一、基金费用的种类")),
        _p(_r("本基金管理费率为0.15%，托管费率为0.05%。")),
        _p(_r("二、基金费用计提方法、计提标准和支付方式")),
        _p(_r("计提方法招募正文。")),
        _p(_r("招募说明书额外计提说明。")),
        _p(_r("第十二部分  基金的收益与分配")),
    ])
    rules = [{
        "contract_locator": "第十五部分 基金费用与税收 / 一、基金费用的种类",
        "prospectus_locator": "第十一部分 费用与税收 / 一、基金费用的种类",
        "contract_section_name": "第十五部分 基金费用与税收",
        "prospectus_section_name": "第十一部分 费用与税收",
        "relation": "直接对应",
        "consistency": "完全一致",
    }]

    updated, report = app._sync_review_direct_contract_sources_redlines(contract, prospectus, rules=rules)

    xml = _document_xml(updated)
    assert report["updated_count"] == 1
    assert "<w:delText>0.15%</w:delText>" in xml
    assert "<w:t>0.20%</w:t>" in xml
    assert "招募说明书额外计提说明" in xml
    assert "<w:delText>招募说明书额外计提说明。</w:delText>" not in xml


def test_review_direct_sync_redlines_number_prefix_only_changes():
    prospectus = _minimal_docx([
        _p(_r("第十二部分  基金的收益与分配")),
        _p(_r("4、每一基金份额享有同等分配权；")),
        _p(_r("第十三部分  基金的会计与审计")),
    ])

    updated, report = app._redline_docx_paragraph_span_to_text(
        prospectus,
        "4、每一基金份额享有同等分配权；",
        "1、每一基金份额享有同等分配权；",
        report_label="收益分配原则",
    )

    xml = _document_xml(updated)
    assert report["status"] == "redlined"
    assert "<w:delText>4、</w:delText>" in xml
    assert "<w:t>1、</w:t>" in xml


def test_export_final_syncs_product_summary_from_updated_prospectus():
    contract_bytes = _minimal_docx([
        _p(_ins("测试修订")),
        _p(_r("第十一部分  基金的投资")),
        _p(_r("一、投资目标")),
        _p(_r("本基金通过投资于目标ETF，紧密跟踪标的指数，追求与业绩比较基准相似的回报。")),
        _p(_r("二、投资范围")),
        _p(_r("最终投资范围包含转融通证券出借业务。")),
        _p(_r("三、投资策略")),
        _p(_r("策略正文。")),
        _p(_r("六、风险收益特征")),
        _p(_r("最终风险收益特征。")),
        _p(_r("七、基金管理人代表基金行使股东或债权人权利的处理原则及方法")),
        _p(_r("第十二部分  基金的财产")),
        _p(_r("基金财产正文。")),
    ])
    prospectus_bytes = _minimal_docx([
        _p(_r("第八部分  基金的投资")),
        _p(_r("一、投资目标")),
        _p(_r("本基金通过投资于目标ETF，紧密跟踪标的指数，追求与业绩比较基准相似的回报。")),
        _p(_r("二、投资范围")),
        _p(_r("旧投资范围。")),
        _p(_r("三、投资策略")),
        _p(_r("策略正文。")),
        _p(_r("六、风险收益特征")),
        _p(_r("旧风险收益特征。")),
        _p(_r("七、基金管理人代表基金行使股东或债权人权利的处理原则及方法")),
        _p(_r("第九部分  基金的财产")),
        _p(_r("基金财产正文。")),
    ])
    product_summary_bytes = _minimal_docx([
        _p(_r("产品资料概要")),
        _tbl(
            _tr(_tc(_p(_r("项目"))), _tc(_p(_r("内容")))),
            _tr(_tc(_p(_r("投资范围"))), _tc(_p(_r("旧投资范围。")))),
            _tr(_tc(_p(_r("风险收益特征"))), _tc(_p(_r("旧风险收益特征。")))),
        ),
    ])

    client = app.app.test_client()
    upload = client.post(
        "/api/revision_workbench/upload",
        data={
            "contract_revision_docx": (io.BytesIO(contract_bytes), "contract.docx"),
            "prospectus_revision_docx": (io.BytesIO(prospectus_bytes), "prospectus.docx"),
            "product_summary_revision_docx": (io.BytesIO(product_summary_bytes), "product_summary_A.docx"),
        },
        content_type="multipart/form-data",
    )
    assert upload.status_code == 200
    payload = upload.get_json()
    decisions = {item["id"]: "accept" for item in payload["revision_items"]}

    response = client.post(
        "/api/revision_workbench/export_final",
        json={
            "decisions": decisions,
            "revision_edits": {},
            "form_data": {
                "FUND_NAME": "南方测试ETF联接基金",
                "INDEX_NAME": "测试指数",
                "TARGET_ETF_NAME": "南方测试ETF",
                "FUND_SHORT_NAME": "南方测试ETF联接",
            },
            "session_operations": [],
        },
    )

    assert response.status_code == 200
    with zipfile.ZipFile(io.BytesIO(response.data), "r") as zf:
        product_xml = _document_xml(zf.read("产品资料概要.docx"))
        sync_report = json.loads(zf.read("同步报告.json").decode("utf-8"))

    assert "旧投资范围" in product_xml
    assert "最终投资范围包含转融通证券出借业务" in product_xml
    assert "旧风险收益特征" in product_xml
    assert "最终风险收益特征" in product_xml
    assert "<w:del" in product_xml
    assert "<w:ins" in product_xml
    assert sync_report["product_summary"]["status"] == "synced_from_prospectus"


def test_sync_product_summary_uses_linked_prospectus_fee_and_risk_chapters():
    final_prospectus_bytes = _minimal_docx([
        _p(_r("第十一部分  费用与税收")),
        _p(_r("一、基金费用的种类")),
        _p(_r("1、基金管理人的管理费")),
        _p(_r("2、基金托管人的托管费")),
        _p(_r("3、基金上市费及年费")),
        _p(_r("二、基金费用计提方法、计提标准和支付方式")),
        _p(_r("第十二部分  基金的收益与分配")),
        _p(_r("第十五部分  风险揭示")),
        _p(_r("一、市场风险")),
        _p(_r("1、政策风险")),
        _p(_r("二、管理风险")),
        _p(_r("管理风险正文。")),
        _p(_r("四、本基金特有的风险")),
        _p(_r("1、目标ETF风险：最终目标ETF风险。")),
        _p(_r("五、其他风险")),
        _p(_r("第十六部分  基金合同的变更、终止与基金财产的清算")),
    ])
    product_summary_bytes = _minimal_docx([
        _p(_r("产品资料概要")),
        _tbl(
            _tr(_tc(_p(_r("项目"))), _tc(_p(_r("内容"))), _tc(_p(_r("收取方")))),
            _tr(_tc(_p(_r("其他费用"))), _tc(_p(_r("旧其他费用。"))), _tc(_p(_r("旧其他费用。")))),
        ),
        _p(_r("四、风险揭示与重要提示")),
        _p(_r("（一）风险揭示")),
        _p(_r("旧风险揭示。")),
        _p(_r("（二）重要提示")),
        _p(_r("重要提示正文。")),
    ])

    updated, report = app._sync_review_product_summary_from_prospectus_redlines(
        product_summary_bytes,
        final_prospectus_bytes,
        form_data={
            "FUND_NAME": "南方测试ETF联接基金",
            "INDEX_NAME": "测试指数",
            "TARGET_ETF_NAME": "南方测试ETF",
            "FUND_SHORT_NAME": "南方测试ETF联接",
        },
        filename="product_summary_A.docx",
    )

    xml = _document_xml(updated)
    assert report["status"] == "synced_from_prospectus"
    assert "其他费用" in report["updated_fields"]
    assert "风险揭示" in report["updated_fields"]
    assert "旧其他费用" in xml
    assert "基金上市费及年费" in xml
    assert "旧风险揭示" in xml
    assert "最终目标ETF风险" in xml
    assert "<w:del" in xml
    assert "<w:ins" in xml

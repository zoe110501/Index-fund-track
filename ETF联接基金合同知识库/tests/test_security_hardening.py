from __future__ import annotations

import io
import shutil
import sys
from pathlib import Path

import pytest
from docx import Document


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import app as app_module  # noqa: E402

from app import app  # noqa: E402


@pytest.fixture()
def client():
    app.config["TESTING"] = True
    with app.test_client() as test_client:
        yield test_client


def _docx_file(lines: list[str], filename: str = "测试基金合同.docx"):
    doc = Document()
    for line in lines:
        doc.add_paragraph(line)
    stream = io.BytesIO()
    doc.save(stream)
    stream.seek(0)
    return stream, filename


def test_review_report_sanitizes_client_supplied_html(client):
    response = client.post(
        "/api/review/report",
        json={
            "fund_name": "测试基金",
            "cross_check": [
                {
                    "severity": "error",
                    "status": "error",
                    "message": "恶意 HTML 应被清理",
                    "rule": {"contract_chapter": "合同", "prospectus_chapter": "招募"},
                    "hunks": [
                        {
                            "contract_ln": 1,
                            "prospectus_ln": 1,
                            "contract_html": '<img src=x onerror="alert(1)"><del>保留差异</del>',
                            "prospectus_html": "<script>alert(2)</script><ins>保留新增</ins>",
                        }
                    ],
                }
            ],
        },
    )

    html = response.get_data(as_text=True).lower()
    assert response.status_code == 200
    assert "<img" not in html
    assert "onerror" not in html
    assert "alert(1)" not in html
    assert "alert(2)" not in html
    assert "<del>保留差异</del>" in response.get_data(as_text=True)
    assert "<ins>保留新增</ins>" in response.get_data(as_text=True)


def test_file_editor_lists_only_explicit_knowledge_files(client):
    response = client.get("/api/files")
    names = {item["name"] for item in response.get_json()}

    assert "README.md" not in names
    assert "branch_logic_audit_resolved.md" not in names
    assert "模板修改说明.json" not in names
    assert "01_通用模板.md" in names


def test_file_editor_rejects_non_allowlisted_file(client, monkeypatch):
    temp_base = ROOT / "outputs" / "_security_test_tmp"
    if temp_base.exists():
        shutil.rmtree(temp_base)
    temp_base.mkdir(parents=True)
    try:
        monkeypatch.setattr(app_module, "BASE_DIR", temp_base)
        (temp_base / "README.md").write_text("# should stay untouched", encoding="utf-8")

        response = client.post(
            "/api/files/README.md",
            json={"content": "# should not be writable"},
        )

        assert response.status_code == 403
        assert (temp_base / "README.md").read_text(encoding="utf-8") == "# should stay untouched"
    finally:
        shutil.rmtree(temp_base, ignore_errors=True)


def test_clause_library_flattens_editable_and_readonly_entries(client):
    response = client.get("/api/clause_library")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["success"] is True

    editable = next(
        item
        for item in payload["entries"]
        if item["document_type"] == "contract"
        and item["clause_key"] == "MGMT_FEE_PAYMENT"
        and item["field_key"] == "text"
    )
    assert editable["document_label"] == "基金合同"
    assert editable["readonly"] is False
    assert editable["path_id"]
    assert editable["applicability"]
    assert editable["applicability"] in editable["search_text"]

    readonly = next(
        item
        for item in payload["entries"]
        if item["document_type"] == "prospectus"
        and item["clause_key"] == "PROSPECTUS_VARIANTS"
    )
    assert readonly["readonly"] is True
    assert "只读" in readonly["readonly_reason"]


def test_clause_library_save_updates_only_target_text_field(client, monkeypatch):
    temp_base = ROOT / "outputs" / "_clause_library_test_tmp"
    if temp_base.exists():
        shutil.rmtree(temp_base)
    temp_base.mkdir(parents=True)
    try:
        monkeypatch.setattr(app_module, "BASE_DIR", temp_base)
        contract_path = temp_base / "04_差异条款原文库.json"
        prospectus_path = temp_base / "08_招募说明书差异条款库.json"
        contract_path.write_text(
            '{"version":"test","clauses":{"MGMT_FEE_PAYMENT":{"description":"管理费测试","variants":{"CONSULT":{"condition":"测试条件","text":"保存前正文"}}}}}',
            encoding="utf-8",
        )
        prospectus_path.write_text('{"version":"test","clauses":{}}', encoding="utf-8")

        list_response = client.get("/api/clause_library")
        assert list_response.status_code == 200
        entry = next(item for item in list_response.get_json()["entries"] if item["field_key"] == "text")

        save_response = client.post(
            "/api/clause_library",
            json={"path_id": entry["path_id"], "content": "保存后正文"},
        )

        assert save_response.status_code == 200
        assert save_response.get_json()["success"] is True
        saved = contract_path.read_text(encoding="utf-8")
        assert "保存后正文" in saved
        assert "测试条件" in saved
    finally:
        shutil.rmtree(temp_base, ignore_errors=True)


def test_review_upload_returns_isolated_job_ids(client):
    first = client.post(
        "/api/review/upload",
        data={"contract": _docx_file(["第一只测试基金基金合同", "第一份正文"])},
        content_type="multipart/form-data",
    ).get_json()
    second = client.post(
        "/api/review/upload",
        data={"contract": _docx_file(["第二只测试基金基金合同", "第二份正文"])},
        content_type="multipart/form-data",
    ).get_json()

    assert first["job_id"] != second["job_id"]

    first_text = client.get(f"/api/review/get_text?job_id={first['job_id']}").get_json()
    second_text = client.get(f"/api/review/get_text?job_id={second['job_id']}").get_json()

    assert "第一份正文" in first_text["contract_text"]
    assert "第二份正文" in second_text["contract_text"]


def test_ai_review_requires_explicit_external_consent(client):
    upload = client.post(
        "/api/review/upload",
        data={"contract": _docx_file(["测试基金基金合同", "基金的基本情况", "正文"])},
        content_type="multipart/form-data",
    ).get_json()

    response = client.post("/api/review/ai_check", json={"job_id": upload["job_id"]})

    assert response.status_code == 403
    assert response.get_json()["error"]


def test_frontend_does_not_interpolate_raw_names_into_inner_html():
    index_html = (ROOT / "templates" / "index.html").read_text(encoding="utf-8")

    assert "<strong>${name}</strong>" not in index_html


def test_frontend_accounting_firm_dropdown_has_builtin_profiles():
    index_html = (ROOT / "templates" / "index.html").read_text(encoding="utf-8")

    assert 'id="ACCOUNTING_FIRM_CHOICE"' in index_html
    assert "ACCOUNTING_FIRM_PROFILES" in index_html
    assert "德勤华永会计师事务所（特殊普通合伙）" in index_html
    assert "安永华明会计师事务所（特殊普通合伙）" in index_html
    assert "容诚会计师事务所（特殊普通合伙）" in index_html
    assert "经办注册会计师：汪芳、陈思雨" in index_html
    assert "签章注册会计师：高鹤、邓雯" in index_html
    assert "经办注册会计师：陈熹、成磊" in index_html
    assert 'value="OTHER"' in index_html
    assert "手动编辑" in index_html


def test_frontend_exposes_form_history_controls():
    index_html = (ROOT / "templates" / "index.html").read_text(encoding="utf-8")

    assert 'id="form-history-panel"' in index_html
    assert 'id="form-history-list"' in index_html
    assert "FORM_AUTODRAFT_STORAGE_KEY" in index_html
    assert "FORM_HISTORY_STORAGE_KEY" in index_html
    assert "saveCurrentFormHistory" in index_html
    assert "restoreSelectedFormHistory" in index_html
    assert "deleteSelectedFormHistory" in index_html
    assert "clearFormHistory" in index_html
    assert "applyFormData" in index_html

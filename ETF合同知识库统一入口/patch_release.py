from __future__ import annotations

import re
import sys
from pathlib import Path


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8", newline="")


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if old not in text:
        if new in text:
            return text
        raise RuntimeError(f"Patch anchor not found: {label}")
    return text.replace(old, new, 1)


def replace_one_of(text: str, old_values: tuple[str, ...], new: str, label: str) -> str:
    if new in text:
        return text
    for old in old_values:
        if old in text:
            return text.replace(old, new, 1)
    raise RuntimeError(f"Patch anchor not found: {label}")


def replace_between(text: str, start_marker: str, end_marker: str, replacement: str, label: str) -> str:
    start = text.find(start_marker)
    if start == -1:
        if replacement.strip() in text:
            return text
        raise RuntimeError(f"Patch start not found: {label}")
    end = text.find(end_marker, start)
    if end == -1:
        raise RuntimeError(f"Patch end not found: {label}")
    return text[:start] + replacement.rstrip() + text[end:]


def patch_linked_app(path: Path) -> None:
    text = read_text(path)

    text = replace_one_of(
        text,
        (
            '''        _template_dir = os.environ.get(
            "CONTRACT_TEMPLATE_DIR",
            str(Path.home() / "Desktop" / "联接基金法律文件"),
        )
''',
            '''        _packaged_contract_template_dir = Path(__file__).resolve().parent / "packaged_assets" / "legal_templates"
        _template_dir = os.environ.get("CONTRACT_TEMPLATE_DIR")
        if not _template_dir:
            _template_dir = (
                str(_packaged_contract_template_dir)
                if _packaged_contract_template_dir.exists()
                else str(Path.home() / "Desktop" / "联接基金法律文件")
            )
''',
        ),
        '''        _packaged_contract_template_dir = Path(__file__).resolve().parent / "packaged_assets" / "legal_templates"
        _template_dir = os.environ.get("CONTRACT_TEMPLATE_DIR") or str(_packaged_contract_template_dir)
''',
        "linked contract template directory",
    )

    text = replace_one_of(
        text,
        (
            '''    @staticmethod
    def _template_dir():
        configured = os.environ.get("PRODUCT_SUMMARY_TEMPLATE_DIR")
        if configured:
            return Path(configured)
        return Path.home() / "Desktop" / "联接基金法律文件"
''',
            '''    @staticmethod
    def _template_dir():
        configured = os.environ.get("PRODUCT_SUMMARY_TEMPLATE_DIR")
        if configured:
            return Path(configured)
        packaged = Path(__file__).resolve().parent / "packaged_assets" / "product_summary_templates"
        if packaged.exists():
            return packaged
        return Path.home() / "Desktop" / "联接基金法律文件"
''',
        ),
        '''    @staticmethod
    def _template_dir():
        configured = os.environ.get("PRODUCT_SUMMARY_TEMPLATE_DIR")
        if configured:
            return Path(configured)
        packaged = Path(__file__).resolve().parent / "packaged_assets" / "product_summary_templates"
        if packaged.exists():
            return packaged
        return Path(".")
''',
        "linked product summary template directory",
    )

    text = replace_once(
        text,
        '''        text = product_summary_engine.generate(form_data)
        quality = _generation_quality_check(text, "产品资料概要")
        return jsonify({"success": True, "text": text, "quality_check": quality})
''',
        '''        bundle = product_summary_engine.generate_bundle(form_data)
        quality = _generation_quality_check(bundle["text"], "产品资料概要")
        return jsonify({"success": True, **bundle, "quality_check": quality})
''',
        "linked product summary API render model response",
    )

    write_text(path, text)


PRODUCT_SUMMARY_CSS = r'''
.product-summary-render {
  max-width: 980px;
  margin: 0 auto;
  color: var(--text);
  white-space: normal;
}
.product-summary-doc {
  background: #fff;
  border: 1px solid #d8e0ea;
  border-radius: 8px;
  padding: 24px;
  box-shadow: 0 8px 22px rgba(30,42,56,0.08);
}
.product-summary-doc + .product-summary-doc { margin-top: 18px; }
.product-summary-cover {
  text-align: center;
  border-bottom: 1px solid var(--border);
  margin-bottom: 18px;
  padding-bottom: 14px;
}
.product-summary-title {
  font-size: 20px;
  font-weight: 700;
  line-height: 1.45;
  margin-bottom: 10px;
}
.product-summary-cover-meta {
  color: var(--muted);
  font-size: 12.5px;
  line-height: 1.8;
}
.product-summary-section { margin-top: 22px; }
.product-summary-section-title {
  color: var(--accent);
  font-size: 15px;
  font-weight: 700;
  margin-bottom: 10px;
  padding-left: 10px;
  border-left: 3px solid var(--gold);
}
.product-summary-subsection { margin-top: 14px; }
.product-summary-subsection-title {
  font-size: 13.5px;
  font-weight: 700;
  color: var(--text);
  margin-bottom: 8px;
}
.product-summary-table-wrap {
  margin: 10px 0 14px;
  overflow-x: auto;
  border: 1px solid #dce3ec;
  border-radius: 6px;
}
.product-summary-table {
  width: 100%;
  min-width: 620px;
  border-collapse: collapse;
  font-size: 12.5px;
  background: #fff;
}
.product-summary-table th,
.product-summary-table td {
  border: 1px solid #dce3ec;
  padding: 8px 10px;
  line-height: 1.65;
  vertical-align: top;
  text-align: left;
}
.product-summary-table th {
  background: #eef4fb;
  color: var(--accent);
  font-weight: 700;
}
.product-summary-text p {
  margin: 0 0 9px;
  line-height: 1.85;
  text-align: justify;
}
.product-summary-text .is-subheading {
  font-weight: 700;
  color: var(--text);
  margin-top: 12px;
}
.product-summary-note {
  color: var(--muted);
  font-size: 12px;
  line-height: 1.75;
  margin-top: 8px;
}
@media (max-width: 700px) {
  .product-summary-doc { padding: 16px; }
  .product-summary-title { font-size: 17px; }
  .product-summary-table { min-width: 560px; font-size: 12px; }
}
'''


PRODUCT_SUMMARY_HELPERS = r'''
function renderProductSummaryTable(rows) {
  const normalizedRows = (rows || [])
    .map(row => (row || []).map(cell => String(cell || '').trim()));
  if (!normalizedRows.length) return '';
  const colCount = Math.max(...normalizedRows.map(row => row.length));
  const firstRow = normalizedRows[0] || [];
  const useHeader = firstRow.some(cell => /费用类型|费用类别|收费方式|收费方|备注/.test(cell));
  const renderCell = (cell, tag) => `<${tag}>${escHtml(cell)}</${tag}>`;
  const renderRow = (row, tag) => {
    const cells = [];
    for (let i = 0; i < colCount; i += 1) cells.push(renderCell(row[i] || '', tag));
    return `<tr>${cells.join('')}</tr>`;
  };
  const rowsHtml = normalizedRows.map((row, index) => renderRow(row, useHeader && index === 0 ? 'th' : 'td')).join('');
  return `<div class="product-summary-table-wrap"><table class="product-summary-table"><tbody>${rowsHtml}</tbody></table></div>`;
}

function isProductSummarySubheading(line) {
  return /^[一二三四五六七八九十]+[）)、]/.test(line) || /^（[一二三四五六七八九十]+）/.test(line);
}

function renderProductSummaryTextBlock(text) {
  const lines = String(text || '').split(/\n+/).map(line => line.trim()).filter(Boolean);
  if (!lines.length) return '';
  return `<div class="product-summary-text">${lines.map(line => {
    const cls = isProductSummarySubheading(line) ? ' class="is-subheading"' : '';
    return `<p${cls}>${escHtml(line)}</p>`;
  }).join('')}</div>`;
}

function renderProductSummarySectionBody(section) {
  if (!section || typeof section !== 'object') return '';
  if (section.type === 'table') return renderProductSummaryTable(section.rows || []);
  if (section.type === 'text') return renderProductSummaryTextBlock(section.content || '');
  if (section.type === 'mixed') {
    return (section.subsections || []).map(sub => {
      const title = sub.title ? `<h4 class="product-summary-subsection-title">${escHtml(sub.title)}</h4>` : '';
      const body = sub.type === 'table'
        ? renderProductSummaryTable(sub.rows || [])
        : renderProductSummaryTextBlock(sub.content || '');
      const note = sub.note ? `<div class="product-summary-note">${String(sub.note).split(/\n+/).filter(Boolean).map(line => escHtml(line)).join('<br>')}</div>` : '';
      return `<section class="product-summary-subsection">${title}${body}${note}</section>`;
    }).join('');
  }
  return '';
}

function renderProductSummaryModel(model) {
  const fundName = model && model.fund_name ? model.fund_name : 'ETF联接基金';
  const manager = model && model.fund_manager ? model.fund_manager : '详见招募说明书';
  const custodian = model && model.custodian_name ? model.custodian_name : '详见招募说明书';
  const sections = (model && Array.isArray(model.sections)) ? model.sections : [];
  const title = `${fundName}基金产品资料概要`;
  const sectionHtml = sections.map(section => `
    <section class="product-summary-section">
      <h3 class="product-summary-section-title">${escHtml(section.title || '')}</h3>
      ${renderProductSummarySectionBody(section)}
    </section>`).join('');
  return `
    <article class="product-summary-doc">
      <div class="product-summary-cover">
        <div class="product-summary-title">${escHtml(title)}</div>
        <div class="product-summary-cover-meta">基金管理人：${escHtml(manager)}　基金托管人：${escHtml(custodian)}</div>
      </div>
      ${sectionHtml}
    </article>`;
}

function buildProductSummaryHtml(textValue, result) {
  const models = Array.isArray(result.render_models) && result.render_models.length
    ? result.render_models
    : (result.render_model ? [result.render_model] : []);
  if (!models.length) {
    return `<div class="product-summary-render"><article class="product-summary-doc">${renderProductSummaryTextBlock(textValue)}</article></div>`;
  }
  return `<div class="product-summary-render">${models.map(model => renderProductSummaryModel(model)).join('')}</div>`;
}

function renderProductSummaryOutput(result) {
  const text = result.text || '';
  const out = document.getElementById('productsummary-output');
  out.innerHTML = buildProductSummaryHtml(text, result);
  renderGenerationQuality('productsummary-output', result.quality_check);
  lastProductSummaryText = text;
  document.getElementById('product-summary-char-count').textContent = '共 ' + text.length.toLocaleString() + ' 字';
  document.getElementById('productsummary-result').style.display = 'block';
  document.getElementById('btn-product-summary-docx').disabled = false;
  document.getElementById('btn-product-summary-txt').disabled = false;
}
'''


NEW_GENERATE_PRODUCT_SUMMARY = r'''
async function generateProductSummary() {
  const data = collectFormData();

  const btn = document.getElementById('btn-gen-product-summary');
  const origHTML = btn.innerHTML;
  btn.innerHTML = '<span class="loader"></span> 生成中...';
  btn.disabled = true;

  try {
    await refreshProspectusForProductSummary(data);
    const requestData = buildProductSummaryRequestData(data);
    lastProductSummaryFormData = requestData;

    const r = await fetch('/api/generate_product_summary', {
      method: 'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify(requestData)
    });
    const result = await r.json();
    if (result.success) {
      renderProductSummaryOutput(result);
      document.getElementById('productsummary-result').scrollIntoView({behavior:'smooth', block:'start'});
      showToast('产品资料概要已生成');
    } else {
      showToast('生成失败：' + result.error, true);
    }
  } catch(e) {
    showToast('网络错误：' + e.message, true);
  } finally {
    btn.innerHTML = origHTML;
    btn.disabled = false;
  }
}
'''


def patch_linked_template(path: Path) -> None:
    text = read_text(path)
    if ".product-summary-render" not in text:
        text = replace_once(
            text,
            ".quick-start-card .card-body { display: flex; flex-wrap: wrap; align-items: center; gap: 10px 12px; }\n",
            PRODUCT_SUMMARY_CSS + "\n.quick-start-card .card-body { display: flex; flex-wrap: wrap; align-items: center; gap: 10px 12px; }\n",
            "linked product summary CSS",
        )
    if "function renderProductSummaryModel(" not in text:
        text = replace_once(
            text,
            "async function generateProductSummary() {\n",
            PRODUCT_SUMMARY_HELPERS + "\nasync function generateProductSummary() {\n",
            "linked product summary JS helpers",
        )
    text = replace_between(
        text,
        "async function generateProductSummary() {\n",
        "\nasync function exportProductSummaryDocx() {",
        NEW_GENERATE_PRODUCT_SUMMARY,
        "linked generateProductSummary function",
    )
    write_text(path, text)


def patch_etf_app(path: Path) -> None:
    text = read_text(path)
    text = replace_once(
        text,
        '''        template_candidates = [
            Path(r"C:\\Users\\12534\\Downloads\\南方中证智能制造主题交易型开放式指数证券投资基金基金合同 (1).docx"),
            Path(r"C:\\Users\\12534\\Downloads\\3、南方创业板成长ETF-基金合同.docx"),
        ]
''',
        '''        packaged_contract_template_dir = BASE_DIR / "packaged_assets" / "contract_templates"
        template_candidates = []
        if packaged_contract_template_dir.is_dir():
            template_candidates = [
                p for p in packaged_contract_template_dir.iterdir()
                if p.suffix.lower() == ".docx" and "基金合同" in p.name
            ]
''',
        "ETF contract page-style template candidates",
    )
    write_text(path, text)


def patch_etf_packaging_support(path: Path) -> None:
    text = read_text(path)
    text = replace_between(
        text,
        "LEGACY_REFERENCE_PROSPECTUS_SOURCE_MAP = {",
        "\n\nPRODUCT_SUMMARY_TEMPLATE_FILENAME",
        '''LEGACY_REFERENCE_PROSPECTUS_SOURCE_MAP = {
    key: (PACKAGED_REFERENCE_PROSPECTUS_DIR / filename,)
    for key, filename in REFERENCE_PROSPECTUS_VARIANT_FILENAMES.items()
}''',
        "ETF legacy reference prospectus paths",
    )
    text = replace_between(
        text,
        "LEGACY_PRODUCT_SUMMARY_TEMPLATE_CANDIDATES = (",
        "\n\nRULES_XLSX_FILENAME",
        '''LEGACY_PRODUCT_SUMMARY_TEMPLATE_CANDIDATES = (
    PACKAGED_PRODUCT_SUMMARY_DIR / PRODUCT_SUMMARY_TEMPLATE_FILENAME,
)''',
        "ETF legacy product summary path",
    )
    text = replace_between(
        text,
        "LEGACY_RULES_XLSX_CANDIDATES = (",
        "\n\nREVIEW_WORKBOOK_FILENAMES",
        '''LEGACY_RULES_XLSX_CANDIDATES = (
    PACKAGED_REVIEW_RULES_DIR / RULES_XLSX_FILENAME,
)''',
        "ETF legacy rules workbook paths",
    )
    text = replace_between(
        text,
        "LEGACY_REVIEW_XLSX_CANDIDATES = tuple(",
        "\n\n\n@dataclass",
        '''LEGACY_REVIEW_XLSX_CANDIDATES = tuple(
    PACKAGED_REVIEW_WORKBOOKS_DIR / name for name in REVIEW_WORKBOOK_FILENAMES
)''',
        "ETF legacy review workbook paths",
    )
    write_text(path, text)


def sanitize_distribution_text_paths(release_dir: Path) -> None:
    text_suffixes = {
        ".bat",
        ".cmd",
        ".css",
        ".csv",
        ".html",
        ".js",
        ".json",
        ".md",
        ".ps1",
        ".py",
        ".txt",
        ".toml",
        ".yaml",
        ".yml",
    }
    legacy_posix = "C:" + "/" + "Users" + "/" + "12534"
    legacy_win = "C:" + "\\" + "Users" + "\\" + "12534"
    replacements = [
        (f"{legacy_posix}/Desktop/ETF合同知识库/", "systems/etf/"),
        (f"{legacy_posix}/Desktop/ETF合同知识库", "systems/etf"),
        (f"{legacy_win}\\Desktop\\ETF合同知识库\\", r"systems\etf\\"),
        (f"{legacy_win}\\Desktop\\ETF合同知识库", r"systems\etf"),
        (f"{legacy_posix}/Desktop/ETF联接基金合同知识库/", "systems/linked/"),
        (f"{legacy_posix}/Desktop/ETF联接基金合同知识库", "systems/linked"),
        (f"{legacy_win}\\Desktop\\ETF联接基金合同知识库\\", r"systems\linked\\"),
        (f"{legacy_win}\\Desktop\\ETF联接基金合同知识库", r"systems\linked"),
        (f"{legacy_posix}/Desktop/基金合同与招募说明书规则.xlsx", "systems/etf/packaged_assets/review_rules/基金合同与招募说明书规则.xlsx"),
        (f"{legacy_win}\\Desktop\\基金合同与招募说明书规则.xlsx", r"systems\etf\packaged_assets\review_rules\基金合同与招募说明书规则.xlsx"),
        (f"{legacy_posix}/Desktop/基金合同/", "systems/etf/packaged_assets/contract_templates/"),
        (f"{legacy_posix}/Desktop/基金合同", "systems/etf/packaged_assets/contract_templates"),
        (f"{legacy_win}\\Desktop\\基金合同\\", r"systems\etf\packaged_assets\contract_templates\\"),
        (f"{legacy_win}\\Desktop\\基金合同", r"systems\etf\packaged_assets\contract_templates"),
        (f"{legacy_posix}/Downloads/", "external_samples/"),
        (f"{legacy_win}\\Downloads\\", r"external_samples\\"),
        (f"{legacy_win}\\Downloads", "external_samples"),
    ]

    changed_count = 0
    for path in release_dir.rglob("*"):
        if not path.is_file() or path.suffix.lower() not in text_suffixes:
            continue
        try:
            original = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        updated = original
        for old, new in replacements:
            updated = updated.replace(old, new)
        if updated != original:
            write_text(path, updated)
            changed_count += 1
    if changed_count:
        print(f"Sanitized local absolute paths in {changed_count} text files.")


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    if len(args) != 1:
        print("Usage: python patch_release.py <release-dir>", file=sys.stderr)
        return 2

    release_dir = Path(args[0]).resolve()
    etf_dir = release_dir / "systems" / "etf"
    linked_dir = release_dir / "systems" / "linked"

    patch_etf_app(etf_dir / "app.py")
    patch_etf_packaging_support(etf_dir / "packaging_support.py")
    sanitize_distribution_text_paths(release_dir)

    print("Release runtime patches applied.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

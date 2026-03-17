import json
from pathlib import Path
from docx import Document

BASE = Path(r"C:\Users\12534\Desktop\基金合同")
JSON_PATH = Path(r"D:\codex\06_招募说明书差异条款库_work.json")
DOCS = {
    "SSE_SINGLE": BASE / "南方上证科创板人工智能交易型开放式指数证券投资基金招募说明书.docx",
    "SSE_CROSS": BASE / "南方中证全指红利质量交易型开放式指数证券投资基金招募说明书.docx",
    "SZSE_SINGLE": BASE / "南方创业板中盘200交易型开放式指数证券投资基金招募说明书.docx",
    "SZSE_CROSS": BASE / "南方中证电池主题交易型开放式指数证券投资基金招募说明书.docx",
    "SSE_HK": BASE / "南方中证港股通互联网交易型开放式指数证券投资基金招募说明书.docx",
    "SZSE_HK": BASE / "南方中证港股通50交易型开放式指数证券投资基金招募说明书.docx",
}
CN = {
    1: "一", 2: "二", 3: "三", 4: "四", 5: "五", 6: "六", 7: "七", 8: "八", 9: "九", 10: "十",
    11: "十一", 12: "十二", 13: "十三", 14: "十四", 15: "十五", 16: "十六", 17: "十七", 18: "十八",
    19: "十九", 20: "二十", 21: "二十一", 22: "二十二", 23: "二十三", 24: "二十四", 25: "二十五",
}
TOP_SECTIONS = tuple(CN.values())


def split_sections(body: str) -> dict:
    sections = {}
    current = None
    buff = []
    for line in (body or "").splitlines():
        if any(line.startswith(f"{cn}、") for cn in TOP_SECTIONS):
            if current:
                sections[current] = "\n".join(buff).strip()
            current = line.split("、", 1)[0]
            buff = [line]
        elif current:
            buff.append(line)
    if current:
        sections[current] = "\n".join(buff).strip()
    return sections


def split_prelude_and_sections(body: str):
    lines = (body or "").splitlines()
    prelude = []
    idx = 0
    while idx < len(lines) and not any(lines[idx].startswith(f"{cn}、") for cn in TOP_SECTIONS):
        if lines[idx].strip():
            prelude.append(lines[idx].strip())
        idx += 1
    return "\n".join(prelude).strip(), split_sections("\n".join(lines[idx:]))


def extract_doc(path: Path) -> dict:
    doc = Document(str(path))
    paras = list(doc.paragraphs)
    starts = []
    for i, p in enumerate(paras):
        txt = (p.text or "").strip()
        if not txt:
            continue
        style = ""
        try:
            style = p.style.name or ""
        except Exception:
            style = ""
        sl = style.lower()
        if ("heading 2" in sl) or ("标题 2" in style) or ("标题2" in style):
            starts.append(i)
    data = {}
    for idx, start in enumerate(starts):
        chap = CN.get(idx + 1)
        if not chap:
            continue
        end = starts[idx + 1] if idx + 1 < len(starts) else len(paras)
        body_lines = []
        for p in paras[start + 1:end]:
            line = (p.text or "").strip()
            if line:
                body_lines.append(line)
        body = "\n".join(body_lines).strip()
        prelude, sections = split_prelude_and_sections(body)
        data[chap] = {
            "title": (paras[start].text or "").strip(),
            "body": body,
            "prelude": prelude,
            "sections": sections,
        }
    return data

with JSON_PATH.open(encoding="utf-8") as f:
    payload = json.load(f)
clauses = payload.setdefault("clauses", {})
variant_payload = {}
for key, path in DOCS.items():
    ref = extract_doc(path)
    variant_payload[key] = {
        "chapter_6": {
            "section_4": ref.get("六", {}).get("sections", {}).get("四", ""),
            "section_7": ref.get("六", {}).get("sections", {}).get("七", ""),
            "section_11": ref.get("六", {}).get("sections", {}).get("十一", ""),
            "section_12": ref.get("六", {}).get("sections", {}).get("十二", ""),
            "section_13": ref.get("六", {}).get("sections", {}).get("十三", ""),
        },
        "chapter_7": ref.get("七", {}).get("body", ""),
        "chapter_9": ref.get("九", {}).get("body", ""),
        "chapter_10": {
            "prelude": ref.get("十", {}).get("prelude", ""),
            "section_4": ref.get("十", {}).get("sections", {}).get("四", ""),
            "section_7": ref.get("十", {}).get("sections", {}).get("七", ""),
        },
        "chapter_18": ref.get("十八", {}).get("body", ""),
    }

clauses["PROSPECTUS_VARIANTS"] = {
    "description": "六类产品参考条款矩阵（由样本招募说明书抽取）",
    "variants": variant_payload,
}
clauses["CHAPTER21_TITLES"] = {
    "description": "第二十一章仅保留标题骨架",
    "text": "一、基金托管协议当事人\n二、基金托管人对基金管理人的业务监督和核查\n三、基金管理人对基金托管人的业务核查\n四、基金财产的保管\n五、基金资产净值计算和会计核算\n六、基金份额持有人名册的保管\n七、适用法律与争议解决方式\n八、基金托管协议的变更、终止与基金财产的清算"
}
clauses["RISK_CHAPTER_BODIES"] = {
    "description": "第十八章整章风险揭示固定版本",
    "variants": {
        "STANDARD_A": variant_payload["SSE_CROSS"]["chapter_18"],
        "KECHUANG": variant_payload["SSE_SINGLE"]["chapter_18"],
        "CHUANGYE": variant_payload["SZSE_SINGLE"]["chapter_18"],
        "HK_CONNECT": variant_payload["SSE_HK"]["chapter_18"],
    },
}
payload["last_updated"] = "2026-03-06"
with JSON_PATH.open("w", encoding="utf-8") as f:
    json.dump(payload, f, ensure_ascii=False, indent=2)

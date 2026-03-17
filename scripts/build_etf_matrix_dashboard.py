from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from etf_matrix import build_dashboard_payload, read_etf_workbook, render_dashboard_html


def main() -> int:
    parser = argparse.ArgumentParser(description="Build ETF competitor matrix dashboard assets.")
    parser.add_argument("excel_path", help="Path to the ETF Excel workbook")
    parser.add_argument("--json-out", default="D:/codex/etf_competitor_matrix_data.json")
    parser.add_argument("--html-out", default="D:/codex/etf_competitor_matrix.html")
    args = parser.parse_args()

    excel_path = Path(args.excel_path)
    rows, data_date = read_etf_workbook(excel_path)
    payload = build_dashboard_payload(rows, source_name=excel_path.name, data_date=data_date, default_period="近一周")

    json_out = Path(args.json_out)
    html_out = Path(args.html_out)
    json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    html_out.write_text(render_dashboard_html(payload), encoding="utf-8")

    print(f"JSON: {json_out}")
    print(f"HTML: {html_out}")
    print(f"Products: {len(payload['products'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

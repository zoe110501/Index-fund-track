from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app import ProductSummaryEngine  # noqa: E402


def test_specific_risk_uses_full_prospectus_section():
    prospectus_text = """
十七、风险揭示
一、市场风险
1、政策风险。
四、本基金特有的风险
1、标的指数的风险：第一句会被旧逻辑压缩。
这里是必须完整保留的第二段风险说明。
（1）这里是必须完整保留的子项说明。
2、投资于目标ETF带来的风险：目标ETF风险第一句。
这里是目标ETF风险的补充说明，也必须保留。
五、其他风险
这里不应进入特有风险。
十八、基金合同的变更、终止和基金财产的清算
"""

    specific_risk = ProductSummaryEngine._build_specific_risk_summary(prospectus_text)

    assert specific_risk == (
        "1、标的指数的风险：第一句会被旧逻辑压缩。\n"
        "这里是必须完整保留的第二段风险说明。\n"
        "（1）这里是必须完整保留的子项说明。\n"
        "2、投资于目标ETF带来的风险：目标ETF风险第一句。\n"
        "这里是目标ETF风险的补充说明，也必须保留。"
    )
    assert "这里不应进入特有风险" not in specific_risk

import json

import pytest

from financial_hot_terminal.llm import parse_llm_json
from financial_hot_terminal.source_registry import load_source_registry


def test_llm_json_parser_validates_required_shape():
    payload = parse_llm_json(
        json.dumps(
            {
                "summary": "机器人主题热度上升。",
                "category": "机器人",
                "market_impact": 70,
                "product_relevance": 80,
                "recommendation_reason": "适合进入材料草稿。",
                "risk_tags": ["需引用原文核对"],
            },
            ensure_ascii=False,
        )
    )

    assert payload["market_impact"] == 70.0
    assert payload["risk_tags"] == ["需引用原文核对"]


def test_llm_json_parser_rejects_missing_fields():
    with pytest.raises(ValueError):
        parse_llm_json('{"summary":"missing"}')


def test_source_registry_loads_sample_sources(tmp_path):
    registry = tmp_path / "sources.json"
    registry.write_text(
        json.dumps(
            {
                "sources": [
                    {
                        "id": "official",
                        "name": "官方源",
                        "source_type": "official",
                        "url": "https://example.com",
                        "sample_items": [],
                    }
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    sources = load_source_registry(registry)

    assert sources[0].id == "official"
    assert sources[0].cadence_minutes == 30

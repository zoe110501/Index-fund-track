from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any
from urllib.request import Request, urlopen


REQUIRED_ANALYSIS_FIELDS = {
    "summary",
    "category",
    "market_impact",
    "product_relevance",
    "recommendation_reason",
    "risk_tags",
}


@dataclass(frozen=True)
class OpenAICompatibleConfig:
    base_url: str
    api_key: str
    model: str
    timeout_seconds: int = 30

    @classmethod
    def from_env(cls) -> "OpenAICompatibleConfig | None":
        api_key = os.getenv("FINHOT_OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY")
        if not api_key:
            return None
        return cls(
            base_url=(os.getenv("FINHOT_OPENAI_BASE_URL") or "https://api.openai.com/v1").rstrip("/"),
            api_key=api_key,
            model=os.getenv("FINHOT_OPENAI_MODEL") or "gpt-4.1-mini",
            timeout_seconds=int(os.getenv("FINHOT_OPENAI_TIMEOUT", "30")),
        )


def validate_llm_analysis(payload: dict[str, Any]) -> dict[str, Any]:
    missing = REQUIRED_ANALYSIS_FIELDS - set(payload)
    if missing:
        raise ValueError(f"LLM analysis missing fields: {', '.join(sorted(missing))}")
    payload["market_impact"] = float(payload["market_impact"])
    payload["product_relevance"] = float(payload["product_relevance"])
    payload["risk_tags"] = list(payload.get("risk_tags") or [])
    return payload


def parse_llm_json(content: str) -> dict[str, Any]:
    stripped = content.strip()
    if stripped.startswith("```"):
        stripped = stripped.strip("`")
        stripped = stripped.removeprefix("json").strip()
    return validate_llm_analysis(json.loads(stripped))


def analyze_with_openai_compatible(config: OpenAICompatibleConfig, *, title: str, content: str) -> dict[str, Any]:
    prompt = (
        "你是金融投研支持助手。请只返回JSON，字段包括summary、category、market_impact、"
        "product_relevance、recommendation_reason、risk_tags。不得输出买卖建议或收益承诺。\n\n"
        f"标题：{title}\n内容：{content}"
    )
    body = json.dumps(
        {
            "model": config.model,
            "messages": [
                {"role": "system", "content": "输出严格JSON，用于资讯线索整理，非投资建议。"},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.2,
        },
        ensure_ascii=False,
    ).encode("utf-8")
    request = Request(
        f"{config.base_url}/chat/completions",
        data=body,
        headers={
            "Authorization": f"Bearer {config.api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urlopen(request, timeout=config.timeout_seconds) as response:
        payload = json.loads(response.read().decode("utf-8"))
    content = payload["choices"][0]["message"]["content"]
    return parse_llm_json(content)

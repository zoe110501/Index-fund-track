from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .core import SOURCE_MEDIA, SOURCE_OFFICIAL, SOURCE_SOCIAL


VALID_SOURCE_TYPES = {SOURCE_OFFICIAL, SOURCE_MEDIA, SOURCE_SOCIAL}


@dataclass(frozen=True)
class SourceConfig:
    id: str
    name: str
    source_type: str
    url: str
    enabled: bool = True
    cadence_minutes: int = 30
    notes: str = ""
    sample_items: tuple[dict[str, Any], ...] = field(default_factory=tuple)


def load_source_registry(path: str | Path) -> list[SourceConfig]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    sources = payload.get("sources", [])
    parsed: list[SourceConfig] = []
    for source in sources:
        source_type = source.get("source_type")
        if source_type not in VALID_SOURCE_TYPES:
            raise ValueError(f"Invalid source_type for {source.get('id')}: {source_type}")
        parsed.append(
            SourceConfig(
                id=source["id"],
                name=source["name"],
                source_type=source_type,
                url=source["url"],
                enabled=bool(source.get("enabled", True)),
                cadence_minutes=int(source.get("cadence_minutes", 30)),
                notes=source.get("notes", ""),
                sample_items=tuple(source.get("sample_items", [])),
            )
        )
    return parsed

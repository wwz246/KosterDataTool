from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ParsedPreview:
    file_type: str
    delimiter: str
    modeCols: int
    kept_ratio: float
    max_cycle: int | None
    warnings: list[str]

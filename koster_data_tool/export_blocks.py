from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Block3Header:
    h1: list[str]
    h2: list[str]
    h3: list[str]
    data: list[list[float]]
    warnings: list[str] = field(default_factory=list)

from __future__ import annotations

import re

CYCLE_STANDALONE_RE = re.compile(r"^\s*(\d+)\s*CYCLE\s*$", re.IGNORECASE)
CYCLE_TAIL_RE = re.compile(r"(\d+)\s*CYCLE\s*$", re.IGNORECASE)


def extract_k_cycle_markers(raw_text: str) -> list[dict]:
    marker_events: list[dict] = []
    for raw_idx_1based, raw_line in enumerate(raw_text.splitlines(), start=1):
        line = raw_line[1:] if raw_line.startswith("\ufeff") else raw_line

        standalone_match = CYCLE_STANDALONE_RE.match(line)
        if standalone_match:
            marker_events.append({"rawLineIndex": raw_idx_1based, "k": int(standalone_match.group(1)), "isStandalone": True})
            continue

        tail_match = CYCLE_TAIL_RE.search(line)
        if tail_match:
            marker_events.append({"rawLineIndex": raw_idx_1based, "k": int(tail_match.group(1)), "isStandalone": False})

    return marker_events


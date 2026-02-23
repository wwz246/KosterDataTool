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


def estimate_max_cycle(
    file_type: str,
    has_cycle_col: bool,
    cycle_values: list[int] | None,
    marker_events: list[dict],
    has_data_after_last_marker: bool,
) -> int:
    if file_type not in {"CV", "GCD"}:
        raise ValueError("file_type must be CV or GCD")

    if file_type == "GCD":
        if not has_cycle_col or not cycle_values:
            raise ValueError("E9007: missing Cycle column for GCD")
        return max(cycle_values)

    if not marker_events:
        return 1

    n_max = max(int(event["k"]) for event in marker_events)
    return n_max + 1 if has_data_after_last_marker else n_max

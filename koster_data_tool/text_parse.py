from __future__ import annotations

import re
from collections import Counter

NUMERIC_TOKEN_RE = re.compile(r"^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$")
CYCLE_STANDALONE_RE = re.compile(r"^\s*(\d+)\s*CYCLE\s*$", re.IGNORECASE)
CYCLE_TAIL_RE = re.compile(r"(\d+)\s*CYCLE\s*$", re.IGNORECASE)


def preclean_lines(raw_text: str) -> tuple[list[str], list[dict]]:
    clean_lines: list[str] = []
    marker_events: list[dict] = []

    for raw_idx, raw_line in enumerate(raw_text.splitlines()):
        line = raw_line[1:] if raw_line.startswith("\ufeff") else raw_line

        if line.startswith("CSStudioFile,") or "H4sIA" in line:
            continue

        standalone_match = CYCLE_STANDALONE_RE.match(line)
        if standalone_match:
            marker_events.append({"rawLineIndex": raw_idx, "k": int(standalone_match.group(1)), "isStandalone": True})
            continue

        tail_match = CYCLE_TAIL_RE.search(line)
        if tail_match:
            marker_events.append({"rawLineIndex": raw_idx, "k": int(tail_match.group(1)), "isStandalone": False})
            line = line[: tail_match.start()].rstrip()

        if line.strip():
            clean_lines.append(line)

    return clean_lines, marker_events


def _split_by_delimiter(line: str, delimiter: str) -> list[str]:
    if delimiter in {"\t", ",", ";"}:
        parts = line.split(delimiter)
    else:
        parts = re.split(delimiter, line)
    return [p.strip() for p in parts if p.strip()]


def detect_delimiter_and_rows(lines: list[str]) -> dict:
    if not lines:
        raise ValueError("delimiter detection failed: empty lines")

    candidates = ["\t", ",", ";", r"\s{2,}", r"\s+"]
    results: list[dict] = []

    for idx, delimiter in enumerate(candidates):
        if delimiter == r"\s+" and any(r["valid"] for r in results):
            break

        num_cols: list[int] = []
        for line in lines:
            tokens = _split_by_delimiter(line, delimiter)
            numeric_cols = sum(1 for token in tokens if NUMERIC_TOKEN_RE.match(token))
            num_cols.append(numeric_cols)

        mode_cols, _ = Counter(num_cols).most_common(1)[0]
        kept_lines = [line for line, count in zip(lines, num_cols) if count == mode_cols]
        kept_count = len(kept_lines)
        kept_ratio = kept_count / len(lines)
        valid = kept_ratio >= 0.8 and mode_cols > 0

        results.append(
            {
                "delimiter": delimiter,
                "modeCols": mode_cols,
                "kept_ratio": kept_ratio,
                "kept_lines": kept_lines,
                "dropped_count": len(lines) - kept_count,
                "valid": valid,
                "priority": idx,
                "kept_count": kept_count,
            }
        )

    valid_results = [r for r in results if r["valid"]]
    if not valid_results:
        raise ValueError("delimiter detection failed: no valid candidate")

    best = sorted(
        valid_results,
        key=lambda r: (-r["kept_count"], -r["kept_ratio"], -r["modeCols"], r["priority"]),
    )[0]
    return {
        "delimiter": best["delimiter"],
        "modeCols": best["modeCols"],
        "kept_ratio": best["kept_ratio"],
        "kept_lines": best["kept_lines"],
        "dropped_count": best["dropped_count"],
    }


def estimate_max_cycle(
    file_type: str,
    has_cycle_col: bool,
    cycle_values: list[int] | None,
    marker_events: list[dict],
    has_data_after_last_marker: bool,
) -> int:
    if file_type not in {"CV", "GCD"}:
        raise ValueError("file_type must be CV or GCD")

    if file_type == "GCD" and has_cycle_col:
        if not cycle_values:
            raise ValueError("cycle_values required when has_cycle_col is True")
        return max(cycle_values)

    if not marker_events:
        return 1

    n_max = max(int(event["k"]) for event in marker_events)
    return n_max + 1 if has_data_after_last_marker else n_max

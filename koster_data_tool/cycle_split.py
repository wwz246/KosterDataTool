from __future__ import annotations

from dataclasses import dataclass


@dataclass
class CycleSplitResult:
    file_type: str
    method: str
    max_cycle: int | None
    cycles: dict[int, list[int]]
    warnings: list[str]


def split_cycles(
    file_type: str,
    has_cycle_col: bool,
    cycle_values: list[int] | None,
    kept_raw_line_indices: list[int],
    marker_events: list[dict],
) -> CycleSplitResult:
    ftype = file_type.upper()
    warnings: list[str] = []

    if ftype == "EIS":
        return CycleSplitResult(file_type=ftype, method="none", max_cycle=None, cycles={}, warnings=warnings)

    if ftype == "GCD":
        if not has_cycle_col or not cycle_values:
            raise ValueError("E9007: missing Cycle column for GCD")
        max_cycle = max(cycle_values)
        cycles: dict[int, list[int]] = {}
        for i, cyc in enumerate(cycle_values):
            k = int(cyc)
            if k <= 0:
                continue
            cycles.setdefault(k, []).append(i)
        return CycleSplitResult(file_type=ftype, method="cycle_col", max_cycle=max_cycle, cycles=cycles, warnings=warnings)

    n_rows = len(kept_raw_line_indices)
    if n_rows == 0:
        return CycleSplitResult(file_type=ftype, method="k_cycle", max_cycle=1, cycles={1: []}, warnings=warnings)

    def _pos(raw_line_index: int) -> int | None:
        pos = None
        for idx, ridx in enumerate(kept_raw_line_indices):
            if ridx <= raw_line_index:
                pos = idx
            else:
                break
        return pos

    latest_marker_by_k: dict[int, dict] = {}
    for event in marker_events:
        k = int(event.get("k", 0))
        if k <= 0:
            continue
        prev = latest_marker_by_k.get(k)
        if prev is None or int(event.get("rawLineIndex", -1)) > int(prev.get("rawLineIndex", -1)):
            latest_marker_by_k[k] = event

    end_idx_by_k: dict[int, int] = {}
    for k in sorted(latest_marker_by_k):
        raw_idx = int(latest_marker_by_k[k]["rawLineIndex"])
        p = _pos(raw_idx)
        if p is None:
            warnings.append(f"k={k} 标记在首数据行之前，已忽略")
            continue
        end_idx_by_k[k] = p

    clamped_end_idx_by_k: dict[int, int] = {}
    prev_end = -1
    for k in sorted(end_idx_by_k):
        cur = end_idx_by_k[k]
        if cur <= prev_end:
            warnings.append(f"k={k} 标记不单调，已夹紧")
            cur = prev_end
        clamped_end_idx_by_k[k] = cur
        prev_end = cur

    if not clamped_end_idx_by_k:
        return CycleSplitResult(file_type=ftype, method="k_cycle", max_cycle=1, cycles={1: list(range(n_rows))}, warnings=warnings)

    n_max = max(clamped_end_idx_by_k)
    last_end = clamped_end_idx_by_k[n_max]
    has_data_after_last_marker = last_end < n_rows - 1
    max_cycle = n_max + 1 if has_data_after_last_marker else n_max

    cycles: dict[int, list[int]] = {}
    end_idx = dict(clamped_end_idx_by_k)
    for k in range(1, n_max + 1):
        k_end = end_idx.get(k, end_idx[k - 1] if k > 1 else -1)
        if k == 1:
            start = 0
        else:
            start = end_idx[k - 1] + 1
        cycles[k] = [] if k_end < start else list(range(start, k_end + 1))

    if max_cycle == n_max + 1:
        tail_start = end_idx[n_max] + 1
        cycles[max_cycle] = list(range(tail_start, n_rows)) if tail_start < n_rows else []

    return CycleSplitResult(file_type=ftype, method="k_cycle", max_cycle=max_cycle, cycles=cycles, warnings=warnings)


def select_cycle_indices(file_type: str, split_result: CycleSplitResult, n_cycle: int) -> list[int]:
    if n_cycle <= 0:
        raise ValueError("n_cycle out of range")

    ftype = file_type.upper()
    if split_result.max_cycle is None:
        raise ValueError("n_cycle out of range")

    if n_cycle > split_result.max_cycle:
        raise ValueError("n_cycle out of range")

    if ftype in {"CV", "GCD"}:
        return list(split_result.cycles.get(n_cycle, []))

    raise ValueError("n_cycle out of range")

from __future__ import annotations

import re
from pathlib import Path

from .colmap import _append_run_report, parse_file_for_cycles, read_and_map_file
from .cycle_split import select_cycle_indices, split_cycles
from .export_blocks import Block3Header
from .gcd_segment import calc_m_active_g, decide_main_order, drop_first_cycle_reverse_segment, segment_one_cycle


def _extract_label_num(file_path: str, prefix: str) -> str:
    m = re.match(rf"^{prefix}-([+-]?\d+(?:\.\d+)?)\.txt$", Path(file_path).name, re.IGNORECASE)
    if not m:
        raise ValueError(f"文件名必须为 {prefix}-<num>.txt")
    return m.group(1)


def export_cv_block(
    file_path: str,
    n_cv: int,
    a_geom_cm2: float,
    m_pos_mg: float,
    m_neg_mg: float,
    p_active_pct: float,
    logger,
    run_report_path: str,
) -> Block3Header:
    speed = _extract_label_num(file_path, "CV")
    m_active = calc_m_active_g(m_pos_mg, m_neg_mg, p_active_pct)
    mapping, series, kept_raw_line_indices, marker_events, has_cycle_col, cycle_values = parse_file_for_cycles(
        file_path=file_path,
        file_type="CV",
        a_geom_cm2=a_geom_cm2,
        v_start=None,
        v_end=None,
        logger=logger,
        run_report_path=run_report_path,
    )
    split_result = split_cycles("CV", has_cycle_col, cycle_values, kept_raw_line_indices, marker_events)
    idxs = select_cycle_indices("CV", split_result, n_cv)
    if "E" not in series:
        raise ValueError("CV 缺少电压列")
    if "I" in series:
        cur = [series["I"][i] for i in idxs]
    elif "j" in series:
        cur = [series["j"][i] * a_geom_cm2 for i in idxs]
    else:
        raise ValueError("CV 缺少电流列")
    voltage = [series["E"][i] for i in idxs]
    i_sp = [x / m_active for x in cur]
    return Block3Header(
        h1=["Voltage", "Specific Current"],
        h2=["V", "A/g"],
        h3=["", f"{speed} mV/s"],
        data=[voltage, i_sp],
        warnings=[*mapping.warnings, *split_result.warnings],
    )


def export_gcd_block(
    file_path: str,
    n_gcd: int,
    logger,
    run_report_path: str,
) -> Block3Header:
    density = _extract_label_num(file_path, "GCD")
    mapping, series, kept_raw_line_indices, marker_events, has_cycle_col, cycle_values = parse_file_for_cycles(
        file_path=file_path,
        file_type="GCD",
        a_geom_cm2=1.0,
        v_start=None,
        v_end=None,
        logger=logger,
        run_report_path=run_report_path,
    )
    split_result = split_cycles("GCD", has_cycle_col, cycle_values, kept_raw_line_indices, marker_events)
    idxs = select_cycle_indices("GCD", split_result, n_gcd)
    if not idxs:
        return Block3Header(["Time", "Voltage"], ["s", "V"], ["", f"{density} A/g"], [[], []], [*mapping.warnings, *split_result.warnings])

    step = [int(round(series["Step"][i])) for i in idxs] if "Step" in series else None
    i_data = [series["I"][i] for i in idxs] if "I" in series else ([series["j"][i] for i in idxs] if "j" in series else [0.0 for _ in idxs])
    v_min, v_max = min(series["E"][i] for i in idxs), max(series["E"][i] for i in idxs)
    seg = segment_one_cycle(
        [series["t"][i] for i in idxs],
        [series["E"][i] for i in idxs],
        i_data,
        step,
        v_min,
        v_max,
        float(density),
        1.0,
    )
    seg.cycle_k = n_gcd

    if n_gcd == 1 and (split_result.max_cycle or 0) >= 2:
        all_segs = []
        for k in range(1, (split_result.max_cycle or 0) + 1):
            k_idxs = split_result.cycles.get(k, [])
            if not k_idxs:
                continue
            k_step = [int(round(series["Step"][i])) for i in k_idxs] if "Step" in series else None
            k_i_data = [series["I"][i] for i in k_idxs] if "I" in series else ([series["j"][i] for i in k_idxs] if "j" in series else [0.0 for _ in k_idxs])
            k_v_min, k_v_max = min(series["E"][i] for i in k_idxs), max(series["E"][i] for i in k_idxs)
            kk = segment_one_cycle([series["t"][i] for i in k_idxs], [series["E"][i] for i in k_idxs], k_i_data, k_step, k_v_min, k_v_max, float(density), 1.0)
            kk.cycle_k = k
            all_segs.append(kk)
        if all_segs:
            order = decide_main_order(all_segs)
            seg = drop_first_cycle_reverse_segment(seg, order)

    valid_local_idx: list[int] = []
    for s in seg.segments:
        valid_local_idx.extend(list(range(s.start, s.end + 1)))
    valid_local_idx = sorted(set(valid_local_idx))
    if not valid_local_idx:
        valid_local_idx = list(range(len(idxs)))

    out_t = [series["t"][idxs[i]] for i in valid_local_idx]
    out_v = [series["E"][idxs[i]] for i in valid_local_idx]
    t0 = out_t[0]
    out_t = [t - t0 for t in out_t]

    return Block3Header(
        h1=["Time", "Voltage"],
        h2=["s", "V"],
        h3=["", f"{density} A/g"],
        data=[out_t, out_v],
        warnings=[*mapping.warnings, *split_result.warnings, *seg.warnings],
    )


def export_eis_block(
    file_path: str,
    a_geom_cm2: float,
    logger,
    run_report_path: str,
) -> Block3Header:
    num = _extract_label_num(file_path, "EIS")
    mapping, series = read_and_map_file(
        file_path=file_path,
        file_type="EIS",
        a_geom_cm2=a_geom_cm2,
        v_start=None,
        v_end=None,
        logger=logger,
        run_report_path=run_report_path,
    )
    if "Zre" not in series or "Zim" not in series:
        raise ValueError("EIS 缺少 Zre/Zim 列")
    zre = list(series["Zre"])
    nzim = [-x for x in series["Zim"]]
    if any(x > 0 for x in series["Zim"]):
        msg = "EIS 导出按口径输出 -Z''"
        logger.info(msg, file=file_path)
        _append_run_report(run_report_path, msg)
    return Block3Header(
        h1=["Z'", "-Z''"],
        h2=["ohm", "ohm"],
        h3=["", f"EIS-{num}"],
        data=[zre, nzim],
        warnings=[*mapping.warnings],
    )

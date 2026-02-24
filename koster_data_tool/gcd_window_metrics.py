from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from pathlib import Path
from statistics import median

from .colmap import _append_run_report, parse_file_for_cycles
from .run_report import report_error, report_warning
from .cycle_split import split_cycles
from .gcd_segment import calc_m_active_g, decide_main_order, drop_first_cycle_reverse_segment, segment_one_cycle


@dataclass
class WindowTrace:
    t: list[float]
    E: list[float]
    I: list[float] | None
    Q: list[float] | None
    start_included_by_interp: bool
    end_included_by_interp: bool
    warnings: list[str]


@dataclass
class SegmentEndpoints:
    t_start_raw: float
    E_start_raw: float
    I_start_raw: float | None
    t_end_raw: float
    E_end_raw: float
    I_end_raw: float | None
    t_start_win: float | None
    E_start_win: float | None
    I_start_win: float | None
    t_end_win: float | None
    E_end_win: float | None
    I_end_win: float | None
    warnings: list[str]


@dataclass
class GcdCycleMetrics:
    cycle_k: int
    ok_window: bool
    delta_t: float | None
    delta_t_samp: float | None
    delta_q_chg: float | None
    delta_q_dis: float | None
    delta_q_eff_chg: float | None
    delta_q_eff_dis: float | None
    delta_q_source: str | None
    delta_v_noir: float
    delta_v_eff_chg: float | None
    delta_v_eff_dis: float | None
    r_drop: float | None
    r_turn: float | None
    warnings: list[str] = field(default_factory=list)


@dataclass
class GcdConditionMetrics:
    file_path: str
    j_label: float
    main_order: str
    n_gcd: int
    representative_cycle_ok: bool
    cycles: dict[int, GcdCycleMetrics]
    fatal_error: str | None
    warnings: list[str] = field(default_factory=list)


def _interp_pair(i: int, target_v: float, t: list[float], E: list[float], I: list[float] | None, Q: list[float] | None) -> tuple[float, float, float | None, float | None]:
    e0, e1 = E[i], E[i + 1]
    de = e1 - e0
    if abs(de) < 1e-15:
        raise ValueError("电压窗截取失败: 插值分母为0")
    alpha = (target_v - e0) / de
    t0 = t[i] + alpha * (t[i + 1] - t[i])
    ii = None if I is None else I[i] + alpha * (I[i + 1] - I[i])
    qq = None if Q is None else Q[i] + alpha * (Q[i + 1] - Q[i])
    return t0, target_v, ii, qq


def _interp_pair_global(g_i: int, target_v: float, global_t: list[float], global_E: list[float], global_I: list[float] | None) -> tuple[float, float, float | None]:
    e0, e1 = global_E[g_i], global_E[g_i + 1]
    de = e1 - e0
    if abs(de) < 1e-15:
        raise ValueError("电压窗截取失败: 插值分母为0")
    alpha = (target_v - e0) / de
    t0 = global_t[g_i] + alpha * (global_t[g_i + 1] - global_t[g_i])
    ii = None if global_I is None else global_I[g_i] + alpha * (global_I[g_i + 1] - global_I[g_i])
    return t0, target_v, ii


def _crosses(a: float, b: float, target: float, upward: bool) -> bool:
    eps = 1e-12
    if upward:
        return (a <= target <= b + eps) or (abs(a - target) <= eps) or (abs(b - target) <= eps)
    return (b - eps <= target <= a) or (abs(a - target) <= eps) or (abs(b - target) <= eps)


def _find_event_indices(E: list[float], target: float, upward: bool) -> list[int]:
    out: list[int] = []
    for i in range(len(E) - 1):
        if _crosses(E[i], E[i + 1], target, upward):
            out.append(i)
    return out


def _find_bracket_in_global(global_E: list[float], center_global_idx: int, target: float, upward: bool) -> int | None:
    if len(global_E) < 2:
        return None
    lo = max(0, center_global_idx - 5)
    hi = min(len(global_E) - 2, center_global_idx + 5)
    candidates = [i for i in range(lo, hi + 1) if _crosses(global_E[i], global_E[i + 1], target, upward)]
    if not candidates:
        return None
    candidates.sort(key=lambda i: (abs(i - center_global_idx), i))
    return candidates[0]


def _local_index_by_global_pair(seg_global_indices: list[int], g_pair: int) -> int | None:
    for i in range(len(seg_global_indices) - 1):
        if seg_global_indices[i] == g_pair and seg_global_indices[i + 1] == g_pair + 1:
            return i
    return None


def clip_segment_by_voltage_window(
    t: list[float], E: list[float], I: list[float] | None, Q: list[float] | None,
    v_start: float, v_end: float,
    direction: str,
    global_t: list[float], global_E: list[float], global_I: list[float] | None,
    seg_global_indices: list[int]
) -> WindowTrace:
    warnings: list[str] = []
    if len(t) < 2 or len(E) < 2 or v_start >= v_end:
        return WindowTrace([], [], None if I is None else [], None if Q is None else [], False, False, ["电压窗截取失败"])

    if direction == "charge":
        start_target, end_target = v_start, v_end
        upward_start, upward_end = True, True
        start_pick, end_pick = "last", "first"
    else:
        start_target, end_target = v_end, v_start
        upward_start, upward_end = False, False
        start_pick, end_pick = "last", "first"

    start_events = _find_event_indices(E, start_target, upward=upward_start)
    start_i_local: int | None = None
    start_global_pair: int | None = None
    start_uses_global = False
    if start_events:
        start_i_local = start_events[-1] if start_pick == "last" else start_events[0]
    else:
        g_center_start = seg_global_indices[0]
        start_global_pair = _find_bracket_in_global(global_E, g_center_start, start_target, upward_start)
        if start_global_pair is None:
            return WindowTrace([], [], None if I is None else [], None if Q is None else [], False, False, ["电压窗截取失败"])
        start_i_local = _local_index_by_global_pair(seg_global_indices, start_global_pair)
        start_uses_global = True

    all_end_events = _find_event_indices(E, end_target, upward=upward_end)
    if start_i_local is not None:
        end_events = [i for i in all_end_events if i >= start_i_local]
    else:
        end_events = all_end_events

    end_i_local: int | None = None
    end_global_pair: int | None = None
    end_uses_global = False
    if end_events:
        end_i_local = end_events[0] if end_pick == "first" else end_events[-1]
    else:
        g_center_end = seg_global_indices[-1]
        end_global_pair = _find_bracket_in_global(global_E, g_center_end, end_target, upward_end)
        if end_global_pair is None:
            return WindowTrace([], [], None if I is None else [], None if Q is None else [], False, False, ["电压窗截取失败"])
        end_i_local = _local_index_by_global_pair(seg_global_indices, end_global_pair)
        end_uses_global = True

    w_t: list[float] = []
    w_E: list[float] = []
    w_I: list[float] | None = [] if I is not None else None
    w_Q: list[float] | None = [] if Q is not None else None

    def _push(pt_t: float, pt_e: float, pt_i: float | None, pt_q: float | None) -> None:
        if w_t and abs(w_t[-1] - pt_t) <= 1e-12 and abs(w_E[-1] - pt_e) <= 1e-12:
            return
        w_t.append(pt_t)
        w_E.append(pt_e)
        if w_I is not None:
            w_I.append(0.0 if pt_i is None else pt_i)
        if w_Q is not None:
            w_Q.append(0.0 if pt_q is None else pt_q)

    try:
        if start_uses_global:
            assert start_global_pair is not None
            st, se, si = _interp_pair_global(start_global_pair, start_target, global_t, global_E, global_I)
            sq = None
            if start_i_local is not None:
                _, _, _, sq = _interp_pair(start_i_local, start_target, t, E, I, Q)
        else:
            assert start_i_local is not None
            st, se, si, sq = _interp_pair(start_i_local, start_target, t, E, I, Q)

        if end_uses_global:
            assert end_global_pair is not None
            et, ee, ei = _interp_pair_global(end_global_pair, end_target, global_t, global_E, global_I)
            eq = None
            if end_i_local is not None:
                _, _, _, eq = _interp_pair(end_i_local, end_target, t, E, I, Q)
        else:
            assert end_i_local is not None
            et, ee, ei, eq = _interp_pair(end_i_local, end_target, t, E, I, Q)
    except ValueError:
        return WindowTrace([], [], None if I is None else [], None if Q is None else [], False, False, ["电压窗截取失败"])

    start_loop_idx = 0 if start_i_local is None else (start_i_local + 1)
    end_loop_idx = (len(t) - 1) if end_i_local is None else end_i_local
    if et <= st + 1e-15 or end_loop_idx < start_loop_idx - 1:
        return WindowTrace([], [], None if I is None else [], None if Q is None else [], False, False, ["电压窗截取失败"])

    _push(st, se, si, sq)
    for i in range(start_loop_idx, end_loop_idx + 1):
        _push(t[i], E[i], None if I is None else I[i], None if Q is None else Q[i])
    _push(et, ee, ei, eq)

    if len(w_t) < 2:
        warnings.append("电压窗截取失败")
        return WindowTrace([], [], None if I is None else [], None if Q is None else [], False, False, warnings)
    return WindowTrace(w_t, w_E, w_I, w_Q, True, True, warnings)


def _integrate_mAh(t: list[float], I: list[float], start_interval: int = 0) -> float:
    if len(t) < 2:
        return math.nan
    s = 0.0
    for i in range(start_interval, len(t) - 1):
        dt = t[i + 1] - t[i]
        if dt <= 0:
            continue
        s += 0.5 * (I[i] + I[i + 1]) * dt
    return abs(s / 3.6)


def _clean_time_series(t: list[float], E: list[float], I: list[float] | None, Q: list[float] | None) -> tuple[list[float], list[float], list[float] | None, list[float] | None]:
    rows = sorted(enumerate(t), key=lambda x: (x[1], x[0]))
    out_t: list[float] = []
    out_E: list[float] = []
    out_I: list[float] | None = [] if I is not None else None
    out_Q: list[float] | None = [] if Q is not None else None
    seen: set[float] = set()
    for idx, tt in rows:
        if tt in seen:
            continue
        seen.add(tt)
        out_t.append(tt)
        out_E.append(E[idx])
        if out_I is not None and I is not None:
            out_I.append(I[idx])
        if out_Q is not None and Q is not None:
            out_Q.append(Q[idx])
    return out_t, out_E, out_I, out_Q


def compute_one_cycle_metrics(
    cycle_k: int,
    seg1_raw: dict, seg2_raw: dict,
    main_order: str,
    v_start: float, v_end: float,
    a_geom: float,
    m_active_g: float,
    k_factor: float | None,
    output_type: str,
    prefer_current_source: dict,
    logger, run_report_path: str,
    file_path: str,
) -> GcdCycleMetrics:
    warnings: list[str] = []
    delta_v_noir = v_end - v_start
    if output_type == "Csp" and (k_factor is None or k_factor <= 0):
        raise ValueError("Csp 模式下 k_factor 必须>0")
    if output_type == "Qsp" and k_factor is not None:
        raise ValueError("Qsp 模式下不得传 k_factor")

    wt1 = clip_segment_by_voltage_window(**seg1_raw)
    wt2 = clip_segment_by_voltage_window(**seg2_raw)
    warnings.extend(wt1.warnings)
    warnings.extend(wt2.warnings)

    if not wt1.t or not wt2.t:
        return GcdCycleMetrics(cycle_k, False, None, None, None, None, None, None, None, delta_v_noir, None, None, None, None, warnings)

    t1, E1, I1, Q1 = _clean_time_series(wt1.t, wt1.E, wt1.I, wt1.Q)
    t2, E2, I2, Q2 = _clean_time_series(wt2.t, wt2.E, wt2.I, wt2.Q)

    dt_all = [t1[i + 1] - t1[i] for i in range(len(t1) - 1) if t1[i + 1] - t1[i] > 0] + [t2[i + 1] - t2[i] for i in range(len(t2) - 1) if t2[i + 1] - t2[i] > 0]
    delta_t = (t1[-1] - t1[0]) + (t2[-1] - t2[0])
    delta_t_samp = median(dt_all) if dt_all else None

    source = None
    if prefer_current_source.get("I") and I1 is not None and I2 is not None:
        source = "I"
        dq1 = _integrate_mAh(t1, I1, 0)
        dq2 = _integrate_mAh(t2, I2, 0)
        dq1_eff = _integrate_mAh(t1, I1, 1) if len(t1) >= 3 else math.nan
        dq2_eff = _integrate_mAh(t2, I2, 1) if len(t2) >= 3 else math.nan
    elif prefer_current_source.get("j") and I1 is not None and I2 is not None:
        source = "j"
        dq1 = _integrate_mAh(t1, I1, 0)
        dq2 = _integrate_mAh(t2, I2, 0)
        dq1_eff = _integrate_mAh(t1, I1, 1) if len(t1) >= 3 else math.nan
        dq2_eff = _integrate_mAh(t2, I2, 1) if len(t2) >= 3 else math.nan
    elif Q1 is not None and Q2 is not None and len(Q1) >= 2 and len(Q2) >= 2:
        source = "capacity"
        dq1 = abs(Q1[-1] - Q1[0])
        dq2 = abs(Q2[-1] - Q2[0])
        dq1_eff = math.nan if len(Q1) < 3 else abs(Q1[-1] - Q1[1])
        dq2_eff = math.nan if len(Q2) < 3 else abs(Q2[-1] - Q2[1])
        line = report_warning(run_report_path, "W5101", "容量差分算ΔQ", file_path=file_path, cycle=cycle_k)
        warnings.append(line)
        logger.warning(line, code="W5101", cycle_k=cycle_k, file_path=file_path, delta_q_source="capacity")
    else:
        line = report_error(run_report_path, "E5102", "缺电流且缺容量", file_path=file_path, cycle=cycle_k)
        warnings.append(line)
        logger.error(line, code="E5102", cycle_k=cycle_k, file_path=file_path)
        return GcdCycleMetrics(cycle_k, True, delta_t, delta_t_samp, None, None, None, None, None, delta_v_noir, None, None, None, None, warnings)

    if len(t1) < 3 or len(t2) < 3:
        w = f"W5201 窗口点数不足，ΔQ_eff/ΔV_eff 为 NaN file_path={file_path} cycle={cycle_k}"
        warnings.append(w)
        logger.warning(w, code="W5201", file_path=file_path, cycle=cycle_k)
        _append_run_report(run_report_path, w)
    dv1_eff = math.nan if len(E1) < 3 else abs(E1[-1] - E1[1])
    dv2_eff = math.nan if len(E2) < 3 else abs(E2[-1] - E2[1])

    ep1 = SegmentEndpoints(
        t_start_raw=seg1_raw["t"][0], E_start_raw=seg1_raw["E"][0], I_start_raw=None if seg1_raw["I"] is None else seg1_raw["I"][0],
        t_end_raw=seg1_raw["t"][-1], E_end_raw=seg1_raw["E"][-1], I_end_raw=None if seg1_raw["I"] is None else seg1_raw["I"][-1],
        t_start_win=t1[0], E_start_win=E1[0], I_start_win=None if I1 is None else I1[0],
        t_end_win=t1[-1], E_end_win=E1[-1], I_end_win=None if I1 is None else I1[-1], warnings=[]
    )
    ep2 = SegmentEndpoints(
        t_start_raw=seg2_raw["t"][0], E_start_raw=seg2_raw["E"][0], I_start_raw=None if seg2_raw["I"] is None else seg2_raw["I"][0],
        t_end_raw=seg2_raw["t"][-1], E_end_raw=seg2_raw["E"][-1], I_end_raw=None if seg2_raw["I"] is None else seg2_raw["I"][-1],
        t_start_win=t2[0], E_start_win=E2[0], I_start_win=None if I2 is None else I2[0],
        t_end_win=t2[-1], E_end_win=E2[-1], I_end_win=None if I2 is None else I2[-1], warnings=[]
    )

    r_drop = abs(ep1.E_end_raw - ep2.E_start_raw)
    i1 = ep1.I_end_raw
    i2 = ep2.I_start_raw
    if i1 is None and seg1_raw["I"]:
        i1 = median(seg1_raw["I"])
    if i2 is None and seg2_raw["I"]:
        i2 = median(seg2_raw["I"])
    if i1 is None or i2 is None:
        r_turn = math.nan
        line = report_warning(run_report_path, "W1103", "缺电流无法算R_turn", file_path=file_path, cycle=cycle_k)
        warnings.append(line)
        logger.warning(line, code="W1103", file_path=file_path, cycle=cycle_k)
    else:
        di_turn = abs(i2 - i1)
        r_turn = math.nan if di_turn <= 0 else r_drop / di_turn

    if main_order == "Charge→Discharge":
        dq_chg, dq_dis = dq1, dq2
        dq_eff_chg, dq_eff_dis = dq1_eff, dq2_eff
        dv_eff_chg, dv_eff_dis = dv1_eff, dv2_eff
    else:
        dq_chg, dq_dis = dq2, dq1
        dq_eff_chg, dq_eff_dis = dq2_eff, dq1_eff
        dv_eff_chg, dv_eff_dis = dv2_eff, dv1_eff

    if m_active_g <= 0:
        raise ValueError("m_active 必须>0")

    _qsp_chg = dq_chg / m_active_g
    _qsp_dis = dq_dis / m_active_g
    _ce = 100.0 * (dq_dis / dq_chg) if dq_chg > 0 else math.nan
    if output_type == "Csp":
        k = float(k_factor or 1.0)
        _ = (dq_chg * 3.6) / (delta_v_noir * m_active_g) * k
        if not math.isnan(dq_eff_chg) and not math.isnan(dv_eff_chg):
            _ = (dq_eff_chg * 3.6) / (dv_eff_chg * m_active_g) * k

    return GcdCycleMetrics(cycle_k, True, delta_t, delta_t_samp, dq_chg, dq_dis, dq_eff_chg, dq_eff_dis, source, delta_v_noir, dv_eff_chg, dv_eff_dis, r_drop, r_turn, warnings)


def compute_gcd_file_metrics(
    file_path: str,
    root_params: dict,
    battery_params: dict,
    logger, run_report_path: str
) -> GcdConditionMetrics:
    fp = Path(file_path)
    m = re.match(r"^GCD-([+-]?\d+(?:\.\d+)?)\.txt$", fp.name, re.IGNORECASE)
    if not m:
        raise ValueError("文件名必须为 GCD-<num>.txt")
    j_label = float(m.group(1))

    v_start = float(root_params["v_start"])
    v_end = float(root_params["v_end"])
    a_geom = float(root_params["a_geom"])
    output_type = root_params.get("output_type", "Csp")
    k_factor = root_params.get("k_factor")
    n_gcd = int(root_params.get("n_gcd", 1))

    m_active_g = calc_m_active_g(float(battery_params.get("m_pos", 0.0)), float(battery_params.get("m_neg", 0.0)), float(battery_params.get("p_active", 100.0)))

    _mapping, series, kept_raw_line_indices, marker_events, has_cycle_col, cycle_values = parse_file_for_cycles(
        file_path=file_path,
        file_type="GCD",
        a_geom_cm2=a_geom,
        v_start=v_start,
        v_end=v_end,
        logger=logger,
        run_report_path=run_report_path,
    )
    split_result = split_cycles("GCD", has_cycle_col, cycle_values, kept_raw_line_indices, marker_events)
    max_cycle = split_result.max_cycle or 0

    def _build_seg_current(idxs: list[int]) -> list[float]:
        if "I" in series:
            return [series["I"][i] for i in idxs]
        if "Step" in series and idxs:
            step_vals = [int(round(series["Step"][i])) for i in idxs]
            e_vals = [series["E"][i] for i in idxs]
            out = [0.0 for _ in idxs]
            s = 0
            for i in range(1, len(idxs) + 1):
                if i == len(idxs) or step_vals[i] != step_vals[i - 1]:
                    d = e_vals[i - 1] - e_vals[s]
                    val = 1.0 if d >= 0 else -1.0
                    for j in range(s, i):
                        out[j] = val
                    s = i
            return out
        return [1.0 for _ in idxs]

    all_cycle_segments = []
    per_cycle_indices: dict[int, list[int]] = {}
    for k in range(1, max_cycle + 1):
        idxs = split_result.cycles.get(k, [])
        per_cycle_indices[k] = idxs
        if not idxs:
            continue
        step = [int(round(series["Step"][i])) for i in idxs] if "Step" in series else None
        seg = segment_one_cycle(
            [series["t"][i] for i in idxs],
            [series["E"][i] for i in idxs],
            _build_seg_current(idxs),
            step,
            v_start,
            v_end,
            j_label,
            m_active_g,
        )
        seg.cycle_k = k
        all_cycle_segments.append(seg)

    if max_cycle >= 2 and all_cycle_segments:
        order_info = decide_main_order(all_cycle_segments)
        main_order = order_info.order
    else:
        main_order = "Charge→Discharge"

    cycles: dict[int, GcdCycleMetrics] = {}
    file_warnings: list[str] = []
    fatal_error = None
    prefer_source = {"I": "I" in series, "j": "j" in series}

    for cyc in all_cycle_segments:
        k = cyc.cycle_k
        segs = cyc
        if k == 1 and max_cycle >= 2:
            segs = drop_first_cycle_reverse_segment(cyc, type("obj", (), {"order": main_order})())

        desired = ["charge", "discharge"] if main_order == "Charge→Discharge" else ["discharge", "charge"]
        chosen = []
        for want in desired:
            found = next((s for s in segs.segments if s.kind == want and s not in chosen), None)
            if found is not None:
                chosen.append(found)

        if len(chosen) < 2:
            cycles[k] = GcdCycleMetrics(k, False, None, None, None, None, None, None, None, v_end - v_start, None, None, None, None, ["电压窗截取失败"])
            continue

        idxs = per_cycle_indices[k]

        def _mk_seg_raw(seg, kind: str) -> dict:
            loc_idxs = list(range(seg.start, seg.end + 1))
            gidx = [idxs[i] for i in loc_idxs]
            q_series = None
            if kind == "charge" and "Q_chg" in series:
                q_series = [series["Q_chg"][g] for g in gidx]
            elif kind == "discharge" and "Q_dis" in series:
                q_series = [series["Q_dis"][g] for g in gidx]
            return {
                "t": [series["t"][g] for g in gidx],
                "E": [series["E"][g] for g in gidx],
                "I": [series["I"][g] for g in gidx] if "I" in series else None,
                "Q": q_series,
                "v_start": v_start,
                "v_end": v_end,
                "direction": kind,
                "global_t": series["t"],
                "global_E": series["E"],
                "global_I": series["I"] if "I" in series else None,
                "seg_global_indices": gidx,
            }

        seg1_raw = _mk_seg_raw(chosen[0], desired[0])
        seg2_raw = _mk_seg_raw(chosen[1], desired[1])
        cm = compute_one_cycle_metrics(k, seg1_raw, seg2_raw, main_order, v_start, v_end, a_geom, m_active_g, k_factor, output_type, prefer_source, logger, run_report_path, file_path)
        cycles[k] = cm
        if any("E5102" in w for w in cm.warnings):
            fatal_error = f"E5102 缺电流且缺容量列，ΔQ 无法计算 file_path={file_path} n_gcd={n_gcd}"
        if not cm.ok_window and k != n_gcd:
            line = report_warning(
                run_report_path,
                "W5204",
                "电压窗截取失败",
                file_path=file_path,
                cycle=k,
                V_start=v_start,
                V_end=v_end,
            )
            file_warnings.append(line)
            logger.warning(line, code="W5204", file_path=file_path, cycle=k, V_start=v_start, V_end=v_end)

    rep_ok = bool(cycles.get(n_gcd) and cycles[n_gcd].ok_window)
    if not rep_ok:
        fatal_error = report_error(
            run_report_path,
            "E5201",
            "选定圈无法按电压窗截取",
            file_path=file_path,
            cycle=n_gcd,
            V_start=v_start,
            V_end=v_end,
        )
        logger.error(fatal_error, code="E5201", file_path=file_path, cycle=n_gcd, V_start=v_start, V_end=v_end)

    return GcdConditionMetrics(file_path=file_path, j_label=j_label, main_order=main_order, n_gcd=n_gcd, representative_cycle_ok=rep_ok, cycles=cycles, fatal_error=fatal_error, warnings=file_warnings)

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from pathlib import Path

from .colmap import _append_run_report
from .export_blocks import Block3Header
from .gcd_segment import calc_m_active_g
from .gcd_window_metrics import compute_gcd_file_metrics


@dataclass
class RateBlock:
    rate: Block3Header
    retention: Block3Header
    warnings: list[str] = field(default_factory=list)


def _gcd_label(path: str) -> float:
    m = re.match(r"^GCD-([+-]?\d+(?:\.\d+)?)\.txt$", Path(path).name, re.IGNORECASE)
    if not m:
        raise ValueError("文件名必须为 GCD-<num>.txt")
    return float(m.group(1))


def _calc_csp(delta_q_mAh: float | None, delta_v: float, m_active_g: float, k_factor: float) -> float:
    if delta_q_mAh is None or m_active_g <= 0 or delta_v <= 0:
        return math.nan
    return (delta_q_mAh * 3.6) / (delta_v * m_active_g) * k_factor


def build_rate_and_retention_for_battery(
    gcd_files: list[str],
    n_gcd: int,
    output_type: str,
    root_params: dict,
    battery_params: dict,
    logger,
    run_report_path: str,
) -> RateBlock:
    rows = sorted(gcd_files, key=_gcd_label)
    m_active_g = calc_m_active_g(float(battery_params.get("m_pos", 0.0)), float(battery_params.get("m_neg", 0.0)), float(battery_params.get("p_active", 100.0)))
    delta_v = float(root_params["v_end"]) - float(root_params["v_start"])
    k_factor = float(root_params.get("k_factor")) if root_params.get("k_factor") is not None else None

    if output_type == "Csp" and (k_factor is None or k_factor <= 0):
        raise ValueError("Csp 模式下 k_factor 必填且>0")

    out_cols: list[list[float]]
    warnings: list[str] = []
    if output_type == "Csp":
        out_cols = [[] for _ in range(5)]
        h1 = [
            "Current density",
            "Specific capacitance",
            "Specific capacitance",
            "R↓",
            "R_turn",
        ]
        h2 = ["A/g", "F/g", "F/g", "V", "ohm"]
        h3 = ["", "不扣IR", "有效值", "", ""]
    else:
        out_cols = [[] for _ in range(2)]
        h1 = ["Current density", "Specific capacity"]
        h2 = ["A/g", "mAh/g"]
        h3 = ["", ""]

    metric_cols: list[list[float]] = [[] for _ in range(len(out_cols) - 1)]

    for fp in rows:
        result = compute_gcd_file_metrics(
            file_path=fp,
            root_params={**root_params, "n_gcd": n_gcd, "output_type": output_type},
            battery_params=battery_params,
            logger=logger,
            run_report_path=run_report_path,
        )
        rep = result.cycles.get(n_gcd)
        j = _gcd_label(fp)
        out_cols[0].append(j)

        if rep is None:
            vals = [math.nan for _ in metric_cols]
        elif output_type == "Csp":
            if result.main_order == "Charge→Discharge":
                dq_rep = rep.delta_q_dis
                dq_eff_rep = rep.delta_q_eff_dis
            else:
                dq_rep = rep.delta_q_chg
                dq_eff_rep = rep.delta_q_eff_chg
            vals = [
                _calc_csp(dq_rep, delta_v, m_active_g, float(k_factor)),
                _calc_csp(dq_eff_rep, delta_v, m_active_g, float(k_factor)) if dq_eff_rep is not None else math.nan,
                rep.r_drop if rep.r_drop is not None else math.nan,
                rep.r_turn if rep.r_turn is not None else math.nan,
            ]
        else:
            qsp_dis = (rep.delta_q_dis / m_active_g) if (rep.delta_q_dis is not None and m_active_g > 0) else math.nan
            vals = [qsp_dis]

        for i, v in enumerate(vals, start=1):
            out_cols[i].append(v)
            metric_cols[i - 1].append(v)

    retention_cols: list[list[float]] = [list(out_cols[0])] + [[] for _ in metric_cols]
    for ci, col in enumerate(metric_cols, start=1):
        if not col:
            retention_cols[ci] = []
            continue
        x0 = col[0]
        x1 = col[-1]
        if math.isnan(x0) or x0 <= 0:
            msg = f"W1304:保持率基准无效，列={ci}"
            logger.warning(msg, code="W1304")
            _append_run_report(run_report_path, msg)
            warnings.append(msg)
            retention_cols[ci] = [math.nan for _ in col]
        else:
            value = 100.0 * x1 / x0
            retention_cols[ci] = [value for _ in col]

    rate_block = Block3Header(h1=h1, h2=h2, h3=h3, data=out_cols, warnings=warnings)
    retention_block = Block3Header(h1=h1, h2=["%" if i > 0 else u for i, u in enumerate(h2)], h3=h3, data=retention_cols, warnings=warnings)
    return RateBlock(rate=rate_block, retention=retention_block, warnings=warnings)

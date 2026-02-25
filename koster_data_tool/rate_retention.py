from __future__ import annotations

import math
import re
from decimal import Decimal, ROUND_HALF_UP
from dataclasses import dataclass, field
from pathlib import Path

from .colmap import _append_run_report
from .export_blocks import Block3Header
from .gcd_segment import calc_m_active_g
from .gcd_window_metrics import compute_gcd_file_metrics
from .run_report import report_warning


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


def _calc_csp(delta_q_mAh: float | None, delta_v: float | None, m_active_g: float, k_factor: float) -> float:
    if delta_q_mAh is None or delta_v is None or m_active_g <= 0 or not math.isfinite(delta_v) or delta_v <= 0:
        return math.nan
    return (delta_q_mAh * 3.6) / (delta_v * m_active_g) * k_factor


def _round_half_up(value: float, ndigits: int) -> float:
    q = Decimal("1") if ndigits == 0 else Decimal("1." + ("0" * ndigits))
    return float(Decimal(str(value)).quantize(q, rounding=ROUND_HALF_UP))


def _round_metric_by_semantic(metric_key: str, value: float) -> float:
    if not math.isfinite(value):
        return value
    if metric_key in {"csp_noir", "csp_eff"}:
        return _round_half_up(value, 0)
    if metric_key in {"qsp", "r_drop", "r_turn"}:
        return _round_half_up(value, 2)
    return value


def build_rate_and_retention_for_battery(
    gcd_files: list[str],
    n_gcd: int,
    output_type: str,
    root_params: dict,
    battery_params: dict,
    logger,
    run_report_path: str,
    csp_column_choice: str | None = None,
    compact_rate_columns: bool = False,
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
            "Specific capacitance (noIR)",
            "Specific capacitance (eff)",
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

    metric_keys = ["csp_noir", "csp_eff", "r_drop", "r_turn"] if output_type == "Csp" else ["qsp"]
    metric_cols: list[list[float]] = [[] for _ in range(len(metric_keys))]

    for fp in rows:
        result = compute_gcd_file_metrics(
            file_path=fp,
            root_params={**root_params, "n_gcd": n_gcd, "output_type": output_type},
            battery_params=battery_params,
            logger=logger,
            run_report_path=run_report_path,
        )
        rep = result.cycles.get(result.n_gcd)
        j = _gcd_label(fp)
        out_cols[0].append(j)

        if rep is None:
            vals = [math.nan for _ in metric_cols]
        elif output_type == "Csp":
            if result.main_order == "Charge→Discharge":
                dq_rep = rep.delta_q_dis
                dq_eff_rep = rep.delta_q_eff_dis
                dv_eff_rep = rep.delta_v_eff_dis
            else:
                dq_rep = rep.delta_q_chg
                dq_eff_rep = rep.delta_q_eff_chg
                dv_eff_rep = rep.delta_v_eff_chg
            csp_eff = _calc_csp(dq_eff_rep, dv_eff_rep, m_active_g, float(k_factor))
            if math.isnan(csp_eff):
                msg = f"W5202 Csp(eff) 无法计算 file={fp} cycle={result.n_gcd}"
                logger.warning(msg, code="W5202", file_path=fp, cycle=result.n_gcd)
                _append_run_report(run_report_path, msg)
                warnings.append(msg)
            vals = [
                _calc_csp(dq_rep, delta_v, m_active_g, float(k_factor)),
                csp_eff,
                rep.r_drop if rep.r_drop is not None else math.nan,
                rep.r_turn if rep.r_turn is not None else math.nan,
            ]
        else:
            qsp_dis = (rep.delta_q_dis / m_active_g) if (rep.delta_q_dis is not None and m_active_g > 0) else math.nan
            vals = [qsp_dis]

        for i, v in enumerate(vals, start=1):
            rounded_v = _round_metric_by_semantic(metric_keys[i - 1], v)
            out_cols[i].append(rounded_v)
            metric_cols[i - 1].append(rounded_v)
    if compact_rate_columns and output_type == "Csp":
        chosen = csp_column_choice if csp_column_choice in {"csp_noir", "csp_eff"} else "csp_noir"
        chosen_idx = 1 if chosen == "csp_noir" else 2
        out_cols = [out_cols[0], out_cols[chosen_idx]]
        metric_cols = [metric_cols[chosen_idx - 1]]
        metric_keys = [chosen]
        if chosen == "csp_noir":
            h1 = ["Current density", "Specific capacitance (noIR)"]
            h2 = ["A/g", "F/g"]
            h3 = ["", "不扣IR"]
        else:
            h1 = ["Current density", "Specific capacitance (eff)"]
            h2 = ["A/g", "F/g"]
            h3 = ["", "有效值"]

    retention_row: list[str] = ["保持率"] + ["" for _ in range(len(out_cols) - 1)]
    retention_metric_keys = {"qsp", "csp_noir", "csp_eff"}
    for out_idx in range(1, len(out_cols)):
        metric_key = metric_keys[out_idx - 1] if out_idx - 1 < len(metric_keys) else ""
        if metric_key not in retention_metric_keys:
            continue
        col = metric_cols[out_idx - 1] if out_idx - 1 < len(metric_cols) else []
        if not col:
            continue
        x0 = col[0]
        x1 = col[-1]
        if math.isnan(x0) or x0 <= 0:
            msg = report_warning(run_report_path, "W1304", "Retention 基准X0缺失或<=0")
            logger.warning(msg, code="W1304")
            warnings.append(msg)
            retention_row[out_idx] = "NA"
            continue
        value = _round_half_up(100.0 * x1 / x0, 2)
        retention_row[out_idx] = f"{value:.2f}%"

    for ci in range(len(out_cols)):
        out_cols[ci].append("")
    for ci in range(len(out_cols)):
        out_cols[ci].append(retention_row[ci] if ci < len(retention_row) else "")

    rate_block = Block3Header(h1=h1, h2=h2, h3=h3, data=out_cols, warnings=warnings)
    retention_block = Block3Header(h1=[], h2=[], h3=[], data=[], warnings=warnings)
    return RateBlock(rate=rate_block, retention=retention_block, warnings=warnings)

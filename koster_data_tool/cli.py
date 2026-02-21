from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import threading
from pathlib import Path

from openpyxl import load_workbook

from .bootstrap import init_run_context
from .colmap import parse_file_for_cycles, read_and_map_file
from .curve_export import export_cv_block, export_eis_block, export_gcd_block
from .export_pipeline import run_full_export
from .cycle_split import select_cycle_indices, split_cycles
from .gcd_segment import calc_m_active_g, decide_main_order, drop_first_cycle_reverse_segment, segment_one_cycle
from .gcd_window_metrics import compute_gcd_file_metrics
from .gui import run_gui
from .rate_retention import build_rate_and_retention_for_battery
from .scanner import scan_root
from .state_store import write_last_root


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="KosterDataTool")
    p.add_argument("--root", type=str, default="", help="根目录绝对路径或相对路径")
    p.add_argument("--no-gui", action="store_true", help="以 CLI 形态运行")
    p.add_argument("--selftest", action="store_true", help="运行自检")
    p.add_argument("--scan-only", action="store_true", help="仅扫描并打印摘要")
    p.add_argument("--parse-one", type=str, default="", help="解析单个文件并做列映射/单位换算")
    p.add_argument("--curve-one", type=str, default="", help="导出单个曲线数据块")
    p.add_argument("--split-one", type=str, default="", help="解析并执行分圈+选圈")
    p.add_argument("--gcd-seg-one", type=str, default="", help="单文件执行 GCD 分段")
    p.add_argument("--n-cycle", type=int, default=1, help="代表圈序号")
    p.add_argument("--a-geom", type=float, default=1.0, help="几何面积 cm^2")
    p.add_argument("--m-pos", type=float, default=0.0, help="正极质量 mg")
    p.add_argument("--m-neg", type=float, default=0.0, help="负极质量 mg")
    p.add_argument("--p-active", type=float, default=100.0, help="活性物比例 %")
    p.add_argument("--v-start", type=float, default=None, help="起始电压")
    p.add_argument("--v-end", type=float, default=None, help="终止电压")
    p.add_argument("--gcd-metrics-one", type=str, default="", help="单文件执行 GCD 指标计算")
    p.add_argument("--output-type", type=str, choices=["Csp", "Qsp"], default="Csp", help="输出类型")
    p.add_argument("--export", action="store_true", help="执行端到端导出")
    p.add_argument("--params-json", type=str, default="", help="每电池参数 JSON 文件")
    p.add_argument("--k-factor", type=float, default=None, help="Csp 的 K 系数")
    p.add_argument("--n-gcd", type=int, default=1, help="代表圈序号（GCD 指标）")
    p.add_argument("--rate-selftest", action="store_true", help="打印 Step8 的 Rate/Retention 自检摘要")
    return p


def _write_sample_cv(path: Path) -> None:
    path.write_text(
        "0.10\t0.20\t0.30\n"
        "0.20\t0.30\t0.40 1 CYCLE\n"
        "3 CYCLE\n"
        "0.30\t0.40\t0.50\n"
        "0.40\t0.50\t0.60 2 CYCLE\n"
        "0.50\t0.60\t0.70\n",
        encoding="utf-8",
    )


def _write_sample_gcd(path: Path) -> None:
    path.write_text(
        "Time,Voltage,Current,Step,Cycle\n"
        "0,3.20,-0.5,1,1\n"
        "1,3.10,-0.5,1,1\n"
        "2,3.10,0.5,2,1\n"
        "3,3.20,0.5,2,1\n"
        "4,3.30,0.5,2,1\n"
        "5,3.20,-0.5,3,1\n"
        "6,3.10,-0.5,3,1\n"
        "7,3.00,-0.5,3,1\n"
        "8,3.00,0.5,4,2\n"
        "9,3.10,0.5,4,2\n"
        "10,3.20,0.5,4,2\n"
        "11,3.10,-0.5,5,2\n"
        "12,3.00,-0.5,5,2\n"
        "13,2.90,-0.5,5,2\n",
        encoding="utf-8",
    )


def _write_sample_gcd_no_cycle(path: Path) -> None:
    path.write_text(
        "Time;Voltage;Current\n"
        "0;3.1;0.5\n"
        "1;3.2;0.5 1 CYCLE\n"
        "2 CYCLE\n"
        "2;3.3;0.5\n",
        encoding="utf-8",
    )


def _write_sample_eis(path: Path) -> None:
    path.write_text("1,2,3\n2,3,4\n", encoding="utf-8")


def _write_sample_cv_units(path: Path) -> None:
    path.write_text(
        "时间(s)\t电压(V)\t电流(mA)\tStep\n"
        "0\t3.00\t10\t1\n"
        "1\t3.10\t20\t1\n",
        encoding="utf-8",
    )


def _write_sample_gcd_units(path: Path) -> None:
    path.write_text(
        "Time(s),Voltage(V),j(mA/cm2),Cycle\n"
        "0,3.20,5,1\n"
        "1,3.30,10,1\n",
        encoding="utf-8",
    )


def _write_sample_eis_units(path: Path) -> None:
    path.write_text(
        "Freq(Hz),Z'(Ohm·cm2),Z''(Ohm·cm2)\n"
        "1,10,4\n"
        "2,12,6\n",
        encoding="utf-8",
    )


def _write_sample_eis_no_header_bad(path: Path) -> None:
    path.write_text("1,2,3\n2,3,4\n3,4,5\n", encoding="utf-8")


def _write_sample_cv_cycle_rules(path: Path) -> None:
    path.write_text(
        "2 CYCLE\n"
        "1 CYCLE\n"
        "0.10\t0.20\t0.30\n"
        "0.20\t0.30\t0.40 1 CYCLE\n"
        "0.30\t0.40\t0.50\n",
        encoding="utf-8",
    )


def _write_sample_gcd_cycle_col(path: Path) -> None:
    path.write_text(
        "Time,Voltage,Current,Cycle\n"
        "0,3.1,0.5,1\n"
        "1,3.2,0.5,1\n"
        "2,3.3,0.5,2\n"
        "3,3.4,0.5,3\n",
        encoding="utf-8",
    )


def _write_sample_gcd_metrics(path: Path) -> None:
    path.write_text(
        "Time,Voltage,Current,Step,Cycle,Q_chg,Q_dis\n"
        "0,2.4,1,1,1,0.00,0.00\n"
        "1,3.0,1,1,1,0.28,0.00\n"
        "2,4.4,1,1,1,0.56,0.00\n"
        "3,4.3,-1,2,1,0.56,0.05\n"
        "4,3.5,-1,2,1,0.56,0.32\n"
        "5,2.3,-1,2,1,0.56,0.58\n"
        "6,2.4,1,3,2,0.00,0.00\n"
        "7,3.1,1,3,2,0.30,0.00\n"
        "8,4.3,1,3,2,0.57,0.00\n"
        "9,4.3,-1,4,2,0.57,0.07\n"
        "10,3.6,-1,4,2,0.57,0.34\n"
        "11,2.3,-1,4,2,0.57,0.61\n",
        encoding="utf-8",
    )


def _write_sample_gcd_capacity_only(path: Path) -> None:
    path.write_text(
        "Time,Voltage,Step,Cycle,Q_chg,Q_dis\n"
        "0,2.4,1,1,0.00,0.00\n"
        "1,3.0,1,1,0.28,0.00\n"
        "2,4.4,1,1,0.56,0.00\n"
        "3,4.3,2,1,0.56,0.05\n"
        "4,3.5,2,1,0.56,0.32\n"
        "5,2.3,2,1,0.56,0.58\n",
        encoding="utf-8",
    )


def _write_sample_eis_cycle_none(path: Path) -> None:
    path.write_text("Freq,Zre,Zim\n1,2,3\n2,3,4\n3,4,5\n", encoding="utf-8")


def _write_step8_cv(path: Path) -> None:
    path.write_text(
        "Time(s),Voltage(V),j(mA/cm2),Cycle\n"
        "0,1.0,10,1\n"
        "1,1.1,-20,1\n"
        "2,1.2,30,1\n"
        "3,1.3,-40,1\n",
        encoding="utf-8",
    )


def _write_step8_gcd(path: Path) -> None:
    path.write_text(
        "Time(s),Voltage(V),Current(A),Step,Cycle\n"
        "0,3.00,-0.2,1,1\n"
        "1,2.90,-0.2,1,1\n"
        "2,2.90,0.0,2,1\n"
        "3,2.90,0.0,2,1\n"
        "4,2.95,0.2,3,1\n"
        "5,3.05,0.2,3,1\n"
        "6,3.15,0.2,3,1\n"
        "7,3.20,0.2,4,2\n"
        "8,3.30,0.2,4,2\n"
        "9,3.40,0.2,4,2\n"
        "10,3.30,-0.2,5,2\n"
        "11,3.20,-0.2,5,2\n"
        "12,3.10,-0.2,5,2\n",
        encoding="utf-8",
    )


def _write_step8_eis(path: Path) -> None:
    path.write_text(
        "Freq(Hz),Z'(Ohm·cm2),Z''(Ohm·cm2)\n"
        "1,10,4\n"
        "2,12,6\n",
        encoding="utf-8",
    )


def _write_step8_rate_good(path: Path, current: float, dq_dis_end: float) -> None:
    path.write_text(
        "Time,Voltage,Current,Step,Cycle,Q_chg,Q_dis\n"
        f"0,2.4,0,1,1,0.00,0.00\n"
        f"1,3.0,{current},1,1,0.28,0.00\n"
        f"2,4.3,{current},1,1,0.57,0.00\n"
        f"3,4.3,{-current},2,1,0.57,0.07\n"
        f"4,3.6,{-current},2,1,0.57,0.34\n"
        f"5,2.3,{-current},2,1,0.57,{dq_dis_end}\n",
        encoding="utf-8",
    )


def _write_step8_rate_bad(path: Path, current: float) -> None:
    path.write_text(
        "Time,Voltage,Current,Step,Cycle,Q_chg,Q_dis\n"
        f"0,2.4,0,1,1,0.00,0.00\n"
        f"1,3.0,0,1,1,0.00,0.00\n"
        f"2,4.3,0,1,1,0.00,0.00\n"
        f"3,4.3,0,2,1,0.00,0.00\n"
        f"4,3.6,0,2,1,0.00,0.00\n"
        f"5,2.3,0,2,1,0.00,0.00\n",
        encoding="utf-8",
    )


def _create_selftest_tree(base_root: Path) -> tuple[Path, Path]:
    if base_root.exists():
        shutil.rmtree(base_root)
    base_root.mkdir(parents=True, exist_ok=True)

    struct_a = base_root / "structure_a_root"
    struct_a.mkdir(parents=True, exist_ok=True)
    _write_sample_cv(struct_a / "CV-1.txt")
    _write_sample_gcd(struct_a / "GCD-0.5.txt")
    _write_sample_gcd_no_cycle(struct_a / "GCD-2.txt")
    _write_sample_eis(struct_a / "EIS-1.txt")
    _write_sample_cv_units(struct_a / "CV-2.txt")
    _write_sample_gcd_units(struct_a / "GCD-3.txt")
    _write_sample_eis_units(struct_a / "EIS-4.txt")
    _write_sample_eis_no_header_bad(struct_a / "EIS-5.txt")
    _write_sample_cv_cycle_rules(struct_a / "CV-10.txt")
    _write_sample_gcd_cycle_col(struct_a / "GCD-10.txt")
    _write_sample_eis_cycle_none(struct_a / "EIS-10.txt")
    _write_sample_gcd_metrics(struct_a / "GCD-1.txt")
    _write_sample_gcd_capacity_only(struct_a / "GCD-4.txt")

    step8_root = base_root / "step8"
    step8_root.mkdir(parents=True, exist_ok=True)
    _write_step8_cv(step8_root / "CV-5.txt")
    _write_step8_gcd(step8_root / "GCD-0.5.txt")
    _write_step8_eis(step8_root / "EIS-1.txt")
    rate_ok = step8_root / "battery_rate_ok"
    rate_ok.mkdir(parents=True, exist_ok=True)
    _write_step8_rate_good(rate_ok / "GCD-0.5.txt", 0.5, 0.58)
    _write_step8_rate_good(rate_ok / "GCD-1.txt", 1.0, 0.50)
    rate_bad = step8_root / "battery_rate_bad"
    rate_bad.mkdir(parents=True, exist_ok=True)
    _write_step8_rate_bad(rate_bad / "GCD-0.5.txt", 0.5)
    _write_step8_rate_bad(rate_bad / "GCD-1.txt", 1.0)

    struct_b = base_root / "structure_b_root"
    for bat in ("Battery_A", "Battery_B"):
        bat_dir = struct_b / bat
        bat_dir.mkdir(parents=True, exist_ok=True)
        _write_sample_cv(bat_dir / "CV-1.txt")
        _write_sample_gcd(bat_dir / "GCD-2.txt")
        _write_sample_eis(bat_dir / "EIS-0.2.txt")

    deep_file = struct_b / "Battery_A" / "deep_l2" / "deep_l3" / "too_deep.txt"
    deep_file.parent.mkdir(parents=True, exist_ok=True)
    deep_file.write_text("should appear in skipped report\n", encoding="utf-8")
    return struct_a, struct_b


def _estimate_cycle_from_file(file_path: Path, file_type: str, logger, run_report_path: str) -> int:
    _mapping, _series, kept_raw_line_indices, marker_events, has_cycle_col, cycle_values = parse_file_for_cycles(
        file_path=str(file_path),
        file_type=file_type,
        a_geom_cm2=1.0,
        v_start=None,
        v_end=None,
        logger=logger,
        run_report_path=run_report_path,
    )
    split_result = split_cycles(file_type, has_cycle_col, cycle_values, kept_raw_line_indices, marker_events)
    if split_result.max_cycle is None:
        raise ValueError("max_cycle is None")
    return split_result.max_cycle


def _run_split_one(ctx, logger, split_one: str, n_cycle: int, a_geom: float, v_start: float | None, v_end: float | None) -> int:
    fpath = Path(split_one).expanduser().resolve()
    m = re.match(r"^(CV|GCD|EIS)-", fpath.name, re.IGNORECASE)
    if not m:
        raise ValueError("--split-one 文件名必须以 CV-/GCD-/EIS- 开头")
    file_type = m.group(1).upper()

    _mapping, _series, kept_raw_line_indices, marker_events, has_cycle_col, cycle_values = parse_file_for_cycles(
        file_path=str(fpath),
        file_type=file_type,
        a_geom_cm2=a_geom,
        v_start=v_start,
        v_end=v_end,
        logger=logger,
        run_report_path=str(ctx.report_path),
    )
    split_result = split_cycles(file_type, has_cycle_col, cycle_values, kept_raw_line_indices, marker_events)
    selected = select_cycle_indices(file_type, split_result, n_cycle)

    print(f"file_type={file_type}")
    print(f"split_method={split_result.method}")
    print(f"max_cycle={split_result.max_cycle}")
    print(f"selected_n={n_cycle}")
    print(f"selected_row_count={len(selected)}")
    print(f"warnings={split_result.warnings}")
    print(f"run_report_path={ctx.report_path}")
    return 0


def _run_gcd_seg_one(ctx, logger, args) -> int:
    fpath = Path(args.gcd_seg_one).expanduser().resolve()
    m = re.match(r"^GCD-([+-]?\d+(?:\.\d+)?)\.txt$", fpath.name, re.IGNORECASE)
    if not m:
        raise ValueError("--gcd-seg-one 文件名必须为 GCD-<num>.txt")
    j_label = float(m.group(1))

    _mapping, series, kept_raw_line_indices, marker_events, has_cycle_col, cycle_values = parse_file_for_cycles(
        file_path=str(fpath),
        file_type="GCD",
        a_geom_cm2=args.a_geom,
        v_start=args.v_start,
        v_end=args.v_end,
        logger=logger,
        run_report_path=str(ctx.report_path),
    )
    split_result = split_cycles("GCD", has_cycle_col, cycle_values, kept_raw_line_indices, marker_events)
    row_indices = select_cycle_indices("GCD", split_result, args.n_cycle)
    t = [series["t"][i] for i in row_indices]
    E = [series["E"][i] for i in row_indices]
    I = [series["I"][i] for i in row_indices]
    step = [int(round(series["Step"][i])) for i in row_indices] if "Step" in series else None

    m_active = calc_m_active_g(args.m_pos, args.m_neg, args.p_active)
    cycle_seg = segment_one_cycle(t, E, I, step, args.v_start, args.v_end, j_label, m_active)
    cycle_seg.cycle_k = args.n_cycle

    all_cycles = []
    max_cycle = split_result.max_cycle or 0
    for k in range(1, max_cycle + 1):
        idxs = split_result.cycles.get(k, [])
        if not idxs:
            continue
        kk_t = [series["t"][i] for i in idxs]
        kk_E = [series["E"][i] for i in idxs]
        kk_I = [series["I"][i] for i in idxs]
        kk_step = [int(round(series["Step"][i])) for i in idxs] if "Step" in series else None
        seg_k = segment_one_cycle(kk_t, kk_E, kk_I, kk_step, args.v_start, args.v_end, j_label, m_active)
        seg_k.cycle_k = k
        all_cycles.append(seg_k)

    main_order = None
    adjusted_cycle1 = None
    if max_cycle >= 2:
        main_order = decide_main_order(all_cycles)
        cycle1 = next((x for x in all_cycles if x.cycle_k == 1), None)
        if cycle1 is not None:
            adjusted_cycle1 = drop_first_cycle_reverse_segment(cycle1, main_order)

    print(f"J_label={j_label}")
    print(f"m_active={m_active}")
    print(f"cycle_k={cycle_seg.cycle_k}")
    print(f"segment_count={len(cycle_seg.segments)}")
    for s in cycle_seg.segments:
        print(
            "segment="
            f"({s.kind},{s.start},{s.end},{s.t_start},{s.t_end},{s.I_med},{s.deltaE_end})"
        )
    if main_order is not None:
        print(f"main_order={main_order.order}")
        print(f"main_order_decided_from={main_order.decided_from}")
        print(f"main_order_warnings={main_order.warnings}")
    if adjusted_cycle1 is not None:
        print(f"cycle1_after_drop_segment_count={len(adjusted_cycle1.segments)}")
        print(f"cycle1_after_drop_warnings={adjusted_cycle1.warnings}")
    print(f"warnings={cycle_seg.warnings}")
    print(f"run_report_path={ctx.report_path}")
    return 0


def _selftest(ctx, logger) -> int:
    temp_root = ctx.paths.temp_dir / f"run_{ctx.run_id}" / "selftest_root"
    struct_a, struct_b = _create_selftest_tree(temp_root)

    logger.info("selftest: start", root=str(struct_b))
    print(f"SELFTEST_ROOT={struct_b}")

    assert _estimate_cycle_from_file(struct_a / "CV-1.txt", "CV", logger, str(ctx.report_path)) == 4, "CV-1.txt maxCycle assertion failed"
    assert _estimate_cycle_from_file(struct_a / "GCD-0.5.txt", "GCD", logger, str(ctx.report_path)) == 2, "GCD-1.txt maxCycle assertion failed"
    assert _estimate_cycle_from_file(struct_a / "GCD-2.txt", "GCD", logger, str(ctx.report_path)) == 3, "GCD-2.txt maxCycle assertion failed"

    cv_map, cv_series = read_and_map_file(
        file_path=str(struct_a / "CV-2.txt"),
        file_type="CV",
        a_geom_cm2=1.0,
        v_start=2.5,
        v_end=4.2,
        logger=logger,
        run_report_path=str(ctx.report_path),
    )
    assert cv_map.unit.get("I") == "A", "CV-2 I unit must be A"
    assert abs(cv_series["I"][0] - 0.01) < 1e-12 and abs(cv_series["I"][1] - 0.02) < 1e-12, "CV-2 I mA->A failed"

    gcd_map, gcd_series = read_and_map_file(
        file_path=str(struct_a / "GCD-3.txt"),
        file_type="GCD",
        a_geom_cm2=2.0,
        v_start=2.5,
        v_end=4.2,
        logger=logger,
        run_report_path=str(ctx.report_path),
    )
    assert gcd_map.unit.get("j") == "A/cm2", "GCD-3 j unit must be A/cm2"
    assert gcd_map.unit.get("I") == "A", "GCD-3 derived I unit must be A"
    assert abs(gcd_series["I"][0] - 0.01) < 1e-12 and abs(gcd_series["I"][1] - 0.02) < 1e-12, "GCD-3 I=j*A failed"

    _, eis_series = read_and_map_file(
        file_path=str(struct_a / "EIS-4.txt"),
        file_type="EIS",
        a_geom_cm2=2.0,
        v_start=None,
        v_end=None,
        logger=logger,
        run_report_path=str(ctx.report_path),
    )
    assert abs(eis_series["Zre"][0] - 5.0) < 1e-12 and abs(eis_series["Zim"][1] - 3.0) < 1e-12, "EIS-4 area normalization failed"

    failed = False
    try:
        read_and_map_file(
            file_path=str(struct_a / "EIS-5.txt"),
            file_type="EIS",
            a_geom_cm2=2.0,
            v_start=None,
            v_end=None,
            logger=logger,
            run_report_path=str(ctx.report_path),
        )
    except ValueError as e:
        failed = "E6101" in str(e)
    assert failed, "EIS-5 must fail with E6101"
    assert "E6101" in Path(ctx.report_path).read_text(encoding="utf-8"), "run_report must contain E6101"

    _m_cv10, _s_cv10, cv10_kept, cv10_markers, cv10_has_cycle_col, cv10_cycle_values = parse_file_for_cycles(
        file_path=str(struct_a / "CV-10.txt"),
        file_type="CV",
        a_geom_cm2=1.0,
        v_start=2.5,
        v_end=4.2,
        logger=logger,
        run_report_path=str(ctx.report_path),
    )
    cv10_split = split_cycles("CV", cv10_has_cycle_col, cv10_cycle_values, cv10_kept, cv10_markers)
    assert cv10_split.method == "k_cycle", "CV-10.txt method assertion failed"
    assert cv10_split.max_cycle == 2, "CV-10.txt maxCycle assertion failed"
    assert len(cv10_split.cycles.get(1, [])) == 2, "CV-10.txt cycle#1 size assertion failed"
    assert cv10_split.warnings, "CV-10.txt warnings assertion failed"

    _m_gcd10, _s_gcd10, gcd10_kept, gcd10_markers, gcd10_has_cycle_col, gcd10_cycle_values = parse_file_for_cycles(
        file_path=str(struct_a / "GCD-10.txt"),
        file_type="GCD",
        a_geom_cm2=1.0,
        v_start=2.5,
        v_end=4.2,
        logger=logger,
        run_report_path=str(ctx.report_path),
    )
    gcd10_split = split_cycles("GCD", gcd10_has_cycle_col, gcd10_cycle_values, gcd10_kept, gcd10_markers)
    assert gcd10_split.method == "cycle_col", "GCD-10.txt method assertion failed"
    for k, idxs in gcd10_split.cycles.items():
        for i in idxs:
            assert gcd10_cycle_values is not None and gcd10_cycle_values[i] == k, "GCD-10.txt cycle membership assertion failed"

    _m_eis10, _s_eis10, eis10_kept, eis10_markers, eis10_has_cycle_col, eis10_cycle_values = parse_file_for_cycles(
        file_path=str(struct_a / "EIS-10.txt"),
        file_type="EIS",
        a_geom_cm2=1.0,
        v_start=None,
        v_end=None,
        logger=logger,
        run_report_path=str(ctx.report_path),
    )
    eis10_split = split_cycles("EIS", eis10_has_cycle_col, eis10_cycle_values, eis10_kept, eis10_markers)
    assert eis10_split.method == "none", "EIS-10.txt method assertion failed"
    assert eis10_split.cycles == {}, "EIS-10.txt cycles assertion failed"

    try:
        _run_split_one(ctx, logger, str(struct_a / "CV-10.txt"), 1, 1.0, 2.5, 4.2)
    except Exception as exc:  # noqa: BLE001
        raise AssertionError(f"CV-10.txt split_one assertion failed: {exc}") from exc

    _mapping, series, kept_raw_line_indices, marker_events, has_cycle_col, cycle_values = parse_file_for_cycles(
        file_path=str(struct_a / "GCD-0.5.txt"),
        file_type="GCD",
        a_geom_cm2=1.0,
        v_start=2.5,
        v_end=4.2,
        logger=logger,
        run_report_path=str(ctx.report_path),
    )
    split_result = split_cycles("GCD", has_cycle_col, cycle_values, kept_raw_line_indices, marker_events)
    m_active = calc_m_active_g(10, 0, 90)
    seg_cycles = []
    for k in sorted(split_result.cycles):
        idxs = split_result.cycles[k]
        seg_k = segment_one_cycle(
            [series["t"][i] for i in idxs],
            [series["E"][i] for i in idxs],
            [series["I"][i] for i in idxs],
            [int(round(series["Step"][i])) for i in idxs],
            2.5,
            4.2,
            0.5,
            m_active,
        )
        seg_k.cycle_k = k
        seg_cycles.append(seg_k)
    order = decide_main_order(seg_cycles)
    assert order.order == "Charge→Discharge", "main order assertion failed"
    cycle1 = next(c for c in seg_cycles if c.cycle_k == 1)
    after_drop = drop_first_cycle_reverse_segment(cycle1, order)
    assert len(after_drop.segments) == len(cycle1.segments) - 1, "drop reverse segment assertion failed"

    cmd = [
        "python",
        "main.py",
        "--no-gui",
        "--gcd-seg-one",
        str(struct_a / "GCD-0.5.txt"),
        "--m-pos",
        "10",
        "--m-neg",
        "0",
        "--p-active",
        "90",
        "--v-start",
        "2.5",
        "--v-end",
        "4.2",
        "--n-cycle",
        "1",
    ]
    run = subprocess.run(cmd, cwd=str(ctx.paths.program_dir), check=False, capture_output=True, text=True)
    assert run.returncode == 0, f"gcd-seg-one cli assertion failed: {run.stderr}"

    metrics = compute_gcd_file_metrics(
        file_path=str(struct_a / "GCD-1.txt"),
        root_params={"v_start": 2.5, "v_end": 4.2, "a_geom": 1.0, "output_type": "Csp", "k_factor": 1.0, "n_gcd": 1},
        battery_params={"m_pos": 10.0, "m_neg": 0.0, "p_active": 90.0},
        logger=logger,
        run_report_path=str(ctx.report_path),
    )
    assert metrics.fatal_error != "E5201:代表圈电压窗截取失败", "代表圈不得触发 E5201"
    metrics_capacity = compute_gcd_file_metrics(
        file_path=str(struct_a / "GCD-4.txt"),
        root_params={"v_start": 2.5, "v_end": 4.2, "a_geom": 1.0, "output_type": "Csp", "k_factor": 1.0, "n_gcd": 1},
        battery_params={"m_pos": 10.0, "m_neg": 0.0, "p_active": 90.0},
        logger=logger,
        run_report_path=str(ctx.report_path),
    )
    assert metrics_capacity.cycles[1].delta_q_source == "capacity", "W5101 样例应为 capacity 源"
    ce_raw = 100 * (metrics.cycles[1].delta_q_dis / metrics.cycles[1].delta_q_chg)
    ce_rounded_proxy = 100 * (round(metrics.cycles[1].delta_q_dis, 2) / round(metrics.cycles[1].delta_q_chg, 2))
    assert abs(ce_raw - ce_rounded_proxy) > 1e-6, "CE 必须用未取整 ΔQ"

    result = scan_root(
        root_path=str(struct_b),
        program_dir=str(ctx.paths.program_dir),
        run_id=ctx.run_id,
        cancel_flag=threading.Event(),
        progress_cb=None,
    )
    skipped_report = Path(result.skipped_report_path)
    assert skipped_report.exists(), "skipped report must exist"
    assert skipped_report.read_text(encoding="utf-8").strip(), "skipped report must be non-empty"

    step8_root = temp_root / "step8"
    cv_block = export_cv_block(
        file_path=str(step8_root / "CV-5.txt"),
        n_cv=1,
        a_geom_cm2=2.0,
        m_pos_mg=10.0,
        m_neg_mg=0.0,
        p_active_pct=90.0,
        logger=logger,
        run_report_path=str(ctx.report_path),
    )
    m_active = calc_m_active_g(10.0, 0.0, 90.0)
    expected = [0.01 * 2.0 / m_active, -0.02 * 2.0 / m_active]
    assert cv_block.data[1][0] > 0 and cv_block.data[1][1] < 0, "CV 导出电流应保留正负号"
    assert abs(cv_block.data[1][0] - expected[0]) < 1e-12 and abs(cv_block.data[1][1] - expected[1]) < 1e-12, "CV 导出需按 j*A_geom/m_active"

    gcd_block = export_gcd_block(str(step8_root / "GCD-0.5.txt"), 1, logger, str(ctx.report_path))
    assert gcd_block.data[0][0] == 0.0, "GCD 导出时间需圈内归零"
    assert len(gcd_block.data[0]) < 7, "GCD 导出应剔除静置段"

    eis_block = export_eis_block(str(step8_root / "EIS-1.txt"), 2.0, logger, str(ctx.report_path))
    assert eis_block.data[1][0] < 0, "EIS 导出第二列需为 -Z''"
    assert abs(eis_block.data[0][0] - 5.0) < 1e-12, "EIS Ohm·cm2 需按面积换算"

    rate_ok = build_rate_and_retention_for_battery(
        gcd_files=[str(step8_root / "battery_rate_ok" / "GCD-0.5.txt"), str(step8_root / "battery_rate_ok" / "GCD-1.txt")],
        n_gcd=1,
        output_type="Qsp",
        root_params={"a_geom": 1.0, "v_start": 2.5, "v_end": 4.2},
        battery_params={"m_pos": 10.0, "m_neg": 0.0, "p_active": 90.0},
        logger=logger,
        run_report_path=str(ctx.report_path),
    )
    assert len(rate_ok.rate.data[0]) >= 2, "Rate 需至少两个工况"
    assert not any("W1304" in w for w in rate_ok.warnings), "正常样例不应触发 W1304"

    rate_bad = build_rate_and_retention_for_battery(
        gcd_files=[str(step8_root / "battery_rate_bad" / "GCD-0.5.txt"), str(step8_root / "battery_rate_bad" / "GCD-1.txt")],
        n_gcd=1,
        output_type="Qsp",
        root_params={"a_geom": 1.0, "v_start": 2.5, "v_end": 4.2},
        battery_params={"m_pos": 10.0, "m_neg": 0.0, "p_active": 90.0},
        logger=logger,
        run_report_path=str(ctx.report_path),
    )
    assert any("W1304" in w for w in rate_bad.warnings), "X0<=0 应触发 W1304"

    # step9: 全链路导出回归
    before = {p.name for p in struct_b.iterdir() if p.is_file()}
    export_cmd = ["python", "main.py", "--no-gui", "--root", str(struct_b), "--export", "--output-type", "Csp"]
    run_export = subprocess.run(export_cmd, cwd=str(ctx.paths.program_dir), check=False, capture_output=True, text=True)
    assert run_export.returncode == 0, f"export cli assertion failed: {run_export.stderr}\n{run_export.stdout}"
    after = {p.name for p in struct_b.iterdir() if p.is_file()}
    diff = sorted(after - before)
    assert len(diff) == 2 and all(x.endswith('.xlsx') for x in diff), f"root 新增文件应仅2个xlsx: {diff}"

    # 报告/日志必须在 program_dir/KosterData
    for line in run_export.stdout.splitlines():
        if any(line.startswith(k) for k in ("run_report_path=", "log_path=", "jsonl_log_path=")):
            path = line.split("=", 1)[1].strip()
            assert "KosterData" in Path(path).parts, f"非KosterData路径: {path}"

    xlsx_paths = [struct_b / name for name in diff]
    battery_wb = None
    electrode_wb = None
    for xp in xlsx_paths:
        wb = load_workbook(xp)
        if "参数汇总" in wb.sheetnames:
            battery_wb = wb
        if "Rate" in wb.sheetnames:
            electrode_wb = wb
    assert battery_wb is not None and electrode_wb is not None, "应同时生成极片级与电池级"

    assert "参数汇总" in battery_wb.sheetnames, "电池级应有参数汇总"
    for b in scan_root(str(struct_b), str(ctx.paths.program_dir), ctx.run_id, threading.Event(), None).batteries:
        assert b.name in battery_wb.sheetnames, f"电池级缺少sheet:{b.name}"

    assert "Rate" in electrode_wb.sheetnames, "极片级缺少Rate"
    assert any(n.startswith("CV-") for n in electrode_wb.sheetnames), "极片级缺少CV sheet"
    assert any(n.startswith("GCD-") for n in electrode_wb.sheetnames), "极片级缺少GCD sheet"
    assert any(n.startswith("EIS-") for n in electrode_wb.sheetnames), "极片级缺少EIS sheet"

    ps = battery_wb["参数汇总"]
    texts = [str(ps.cell(row=r, column=1).value or "") for r in range(1, min(120, ps.max_row + 1))]
    idx_param = next(i for i, t in enumerate(texts, start=1) if t == "电池名")
    idx_detail = next(i for i, t in enumerate(texts, start=1) if t == "电池名" and i > idx_param)
    assert idx_detail - idx_param > 5, "参数表与逐圈结果表间必须有5空行"


    rate_ws = electrode_wb["Rate"]
    # 第3行：每个电池块首列空，其余列为电池名
    battery_names = [b.name for b in sorted(scan_root(str(struct_b), str(ctx.paths.program_dir), ctx.run_id, threading.Event(), None).batteries, key=lambda x: x.name)]
    col = 1
    for name in battery_names:
        assert (rate_ws.cell(row=3, column=col).value or "") == "", "Rate 第3行块首列应为空"
        assert (rate_ws.cell(row=3, column=col + 1).value or "") == name, "Rate 第3行应填电池名"
        col += 5
    # Rate 与 Retention 间应有1空行
    rate_data_last = 4
    while rate_ws.cell(row=rate_data_last, column=1).value not in (None, ""):
        rate_data_last += 1
    assert rate_ws.cell(row=rate_data_last, column=1).value in (None, ""), "Rate 后需空行"

    # 参数汇总逐圈表覆盖 max k，不可跳过缺圈
    header_row = idx_detail
    detail_rows = [r for r in range(header_row + 1, ps.max_row + 1) if ps.cell(row=r, column=1).value]
    assert detail_rows, "参数汇总逐圈明细不能为空"

    # CLI 输出应含失败/告警数量
    assert "failures=" in run_export.stdout and "warnings=" in run_export.stdout, "导出CLI需输出失败/告警数量"
    logger.info("selftest: ok", structure_a=str(struct_a), structure_b=str(struct_b))
    return 0


def _run_scan_only(ctx, root_arg: str) -> int:
    if not root_arg:
        raise ValueError("--scan-only 需要同时传入 --root <dir>")

    root = Path(root_arg).expanduser().resolve()
    result = scan_root(
        root_path=str(root),
        program_dir=str(ctx.paths.program_dir),
        run_id=ctx.run_id,
        cancel_flag=threading.Event(),
        progress_cb=None,
    )
    write_last_root(ctx.paths.state_dir, root)

    print(f"structure={result.structure}")
    print(f"batteries={len(result.batteries)}")
    print(f"recognized_file_count={result.recognized_file_count}")
    print(f"skipped_report_path={result.skipped_report_path}")
    print(f"run_report_path={ctx.report_path}")
    return 0


def _run_gcd_metrics_one(ctx, logger, args) -> int:
    fpath = Path(args.gcd_metrics_one).expanduser().resolve()
    metrics = compute_gcd_file_metrics(
        file_path=str(fpath),
        root_params={
            "v_start": args.v_start,
            "v_end": args.v_end,
            "a_geom": args.a_geom,
            "output_type": args.output_type,
            "k_factor": args.k_factor,
            "n_gcd": args.n_gcd,
        },
        battery_params={"m_pos": args.m_pos, "m_neg": args.m_neg, "p_active": args.p_active},
        logger=logger,
        run_report_path=str(ctx.report_path),
    )
    rep = metrics.cycles.get(metrics.n_gcd)
    ce = None
    if rep and rep.delta_q_chg and rep.delta_q_dis:
        ce = 100.0 * rep.delta_q_dis / rep.delta_q_chg
    print(f"file_path={metrics.file_path}")
    print(f"j_label={metrics.j_label}")
    print(f"main_order={metrics.main_order}")
    print(f"fatal_error={metrics.fatal_error}")
    print(f"representative_cycle_ok={metrics.representative_cycle_ok}")
    if rep is not None:
        print(f"rep_delta_t={rep.delta_t}")
        print(f"rep_delta_q_chg={rep.delta_q_chg}")
        print(f"rep_delta_q_dis={rep.delta_q_dis}")
        print(f"rep_ce={ce}")
        print(f"rep_r_turn={rep.r_turn}")
        print(f"rep_warnings={rep.warnings}")
    print(f"warnings={metrics.warnings}")
    print(f"run_report_path={ctx.report_path}")
    return 0



def _run_curve_one(ctx, logger, args) -> int:
    fpath = Path(args.curve_one).expanduser().resolve()
    m = re.match(r"^(CV|GCD|EIS)-", fpath.name, re.IGNORECASE)
    if not m:
        raise ValueError("--curve-one 文件名必须以 CV-/GCD-/EIS- 开头")
    ftype = m.group(1).upper()
    if ftype == "CV":
        block = export_cv_block(str(fpath), args.n_cycle, args.a_geom, args.m_pos, args.m_neg, args.p_active, logger, str(ctx.report_path))
    elif ftype == "GCD":
        block = export_gcd_block(str(fpath), args.n_cycle, logger, str(ctx.report_path))
    else:
        block = export_eis_block(str(fpath), args.a_geom, logger, str(ctx.report_path))

    preview = []
    rows = len(block.data[0]) if block.data else 0
    for r in range(min(3, rows)):
        preview.append([col[r] for col in block.data])
    print(f"file_type={ftype}")
    print(f"columns={block.h1}")
    print(f"data_rows={rows}")
    print(f"preview={preview}")
    print(f"warnings={block.warnings}")
    print(f"run_report_path={ctx.report_path}")
    return 0


def _run_rate_selftest(ctx, logger, args) -> int:
    temp_root = ctx.paths.temp_dir / f"run_{ctx.run_id}" / "selftest_root"
    _create_selftest_tree(temp_root)
    bat = temp_root / "step8" / "battery_rate_ok"
    gcd_files = sorted(str(x) for x in bat.glob("GCD-*.txt"))
    block = build_rate_and_retention_for_battery(
        gcd_files=gcd_files,
        n_gcd=args.n_cycle,
        output_type=args.output_type if args.output_type else "Csp",
        root_params={"a_geom": args.a_geom, "v_start": args.v_start if args.v_start is not None else 2.5, "v_end": args.v_end if args.v_end is not None else 4.2, "k_factor": args.k_factor if args.k_factor is not None else 1.0},
        battery_params={"m_pos": args.m_pos, "m_neg": args.m_neg, "p_active": args.p_active},
        logger=logger,
        run_report_path=str(ctx.report_path),
    )
    print(f"rate_rows={len(block.rate.data[0]) if block.rate.data else 0}")
    print(f"retention_rows={len(block.retention.data[0]) if block.retention.data else 0}")
    print(f"triggered_W1304={any('W1304' in w for w in block.warnings)}")
    print(f"run_report_path={ctx.report_path}")
    return 0


def _default_params_for_scan(scan_result, output_type: str) -> dict:
    battery_params = {}
    for b in scan_result.batteries:
        battery_params[b.name] = {
            "m_pos": 10.0,
            "m_neg": 0.0,
            "p_active": 90.0,
            "k": 1.0,
            "n_cv": 1,
            "n_gcd": 1,
            "v_start": 2.5,
            "v_end": 4.2,
            "main_order": "先充后放",
        }
    return {
        "a_geom": 1.0,
        "output_type": output_type,
        "export_battery_workbook": True,
        "battery_params": battery_params,
    }


def _load_or_default_params(args, scan_result):
    if args.params_json:
        return json.loads(Path(args.params_json).read_text(encoding="utf-8"))
    return _default_params_for_scan(scan_result, args.output_type)


def _run_export(ctx, logger, args) -> int:
    if not args.root:
        raise ValueError("--export 需要 --root")
    root = Path(args.root).expanduser().resolve()
    scan_result = scan_root(str(root), str(ctx.paths.program_dir), ctx.run_id, threading.Event(), None)
    write_last_root(ctx.paths.state_dir, root)
    params = _load_or_default_params(args, scan_result)
    params["a_geom"] = args.a_geom
    params["output_type"] = args.output_type
    selections = {
        "batteries": [b.name for b in scan_result.batteries],
        "cv_nums": [str(x) for x in scan_result.available_cv[:1]],
        "gcd_nums": [str(x) for x in scan_result.available_gcd[:1]],
        "eis_nums": [str(scan_result.available_eis[-1])] if scan_result.available_eis else [],
    }

    result = run_full_export(str(root), scan_result, params, selections, ctx, logger, None)
    print(f"electrode_path={result['electrode_path']}")
    print(f"battery_path={result['battery_path']}")
    print(f"run_report_path={result['run_report_path']}")
    print(f"log_path={result['log_path']}")
    print(f"jsonl_log_path={result['jsonl_log_path']}")
    print(f"failures={len(result.get('failures', []))}")
    print(f"warnings={len(result.get('warnings', []))}")
    return 0

def _run_cli(args) -> int:
    ctx, logger = init_run_context()
    logger.info("mode", mode="CLI")

    if args.selftest:
        return _selftest(ctx, logger)

    if args.export:
        return _run_export(ctx, logger, args)

    if args.scan_only:
        return _run_scan_only(ctx, args.root)

    if args.curve_one:
        return _run_curve_one(ctx, logger, args)

    if args.rate_selftest:
        return _run_rate_selftest(ctx, logger, args)

    if args.split_one:
        return _run_split_one(ctx, logger, args.split_one, args.n_cycle, args.a_geom, args.v_start, args.v_end)

    if args.gcd_seg_one:
        return _run_gcd_seg_one(ctx, logger, args)

    if args.gcd_metrics_one:
        return _run_gcd_metrics_one(ctx, logger, args)

    if args.parse_one:
        fpath = Path(args.parse_one).expanduser().resolve()
        m = re.match(r"^(CV|GCD|EIS)-", fpath.name, re.IGNORECASE)
        if not m:
            raise ValueError("--parse-one 文件名必须以 CV-/GCD-/EIS- 开头")
        file_type = m.group(1).upper()
        mapping, _series = read_and_map_file(
            file_path=str(fpath),
            file_type=file_type,
            a_geom_cm2=args.a_geom,
            v_start=args.v_start,
            v_end=args.v_end,
            logger=logger,
            run_report_path=str(ctx.report_path),
        )
        print(f"file_type={mapping.file_type}")
        print(f"delimiter={mapping.delimiter}")
        print(f"modeCols={mapping.modeCols}")
        print(f"kept_ratio={mapping.kept_ratio}")
        print(f"no_header={mapping.no_header}")
        print(f"col_index={mapping.col_index}")
        print(f"unit={mapping.unit}")
        print(f"warnings={mapping.warnings}")
        print(f"run_report_path={ctx.report_path}")
        return 0

    print("CLI ready. Try --scan-only or --selftest.")
    print(f"LOG_TEXT={ctx.text_log_path}")
    print(f"LOG_JSONL={ctx.jsonl_log_path}")
    print(f"REPORT={ctx.report_path}")
    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.no_gui:
        return _run_cli(args)

    return run_gui()

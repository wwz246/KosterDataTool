from __future__ import annotations

import argparse
import re
import shutil
import threading
from pathlib import Path

from .bootstrap import init_run_context
from .colmap import parse_file_for_cycles, read_and_map_file
from .cycle_split import select_cycle_indices, split_cycles
from .gui import run_gui
from .scanner import scan_root


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="KosterDataTool")
    p.add_argument("--root", type=str, default="", help="根目录绝对路径或相对路径")
    p.add_argument("--no-gui", action="store_true", help="以 CLI 形态运行")
    p.add_argument("--selftest", action="store_true", help="运行自检")
    p.add_argument("--scan-only", action="store_true", help="仅扫描并打印摘要")
    p.add_argument("--parse-one", type=str, default="", help="解析单个文件并做列映射/单位换算")
    p.add_argument("--split-one", type=str, default="", help="解析并执行分圈+选圈")
    p.add_argument("--n-cycle", type=int, default=1, help="代表圈序号")
    p.add_argument("--a-geom", type=float, default=1.0, help="几何面积 cm^2")
    p.add_argument("--v-start", type=float, default=None, help="起始电压")
    p.add_argument("--v-end", type=float, default=None, help="终止电压")
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
        "Time,Voltage,Current,Cycle\n0,3.1,0.5,1\n1,3.2,0.5,3\n2,3.3,0.5,2\n3,3.4,0.5,3\n",
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


def _write_sample_eis_cycle_none(path: Path) -> None:
    path.write_text("Freq,Zre,Zim\n1,2,3\n2,3,4\n3,4,5\n", encoding="utf-8")


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


def _selftest(ctx, logger) -> int:
    temp_root = ctx.paths.temp_dir / f"run_{ctx.run_id}" / "selftest_root"
    struct_a, struct_b = _create_selftest_tree(temp_root)

    logger.info("selftest: start", root=str(struct_b))
    print(f"SELFTEST_ROOT={struct_b}")

    assert _estimate_cycle_from_file(struct_a / "CV-1.txt", "CV", logger, str(ctx.report_path)) == 4, "CV-1.txt maxCycle assertion failed"
    assert _estimate_cycle_from_file(struct_a / "GCD-0.5.txt", "GCD", logger, str(ctx.report_path)) == 3, "GCD-1.txt maxCycle assertion failed"
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

    print(f"structure={result.structure}")
    print(f"batteries={len(result.batteries)}")
    print(f"recognized_file_count={result.recognized_file_count}")
    print(f"skipped_report_path={result.skipped_report_path}")
    print(f"run_report_path={ctx.report_path}")
    return 0


def _run_cli(args) -> int:
    ctx, logger = init_run_context()
    logger.info("mode", mode="CLI")

    if args.selftest:
        return _selftest(ctx, logger)

    if args.scan_only:
        return _run_scan_only(ctx, args.root)

    if args.split_one:
        return _run_split_one(ctx, logger, args.split_one, args.n_cycle, args.a_geom, args.v_start, args.v_end)

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

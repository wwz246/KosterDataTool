from __future__ import annotations

import argparse
import re
import shutil
import threading
from pathlib import Path

from .bootstrap import init_run_context
from .gui import run_gui
from .scanner import scan_root
from .text_parse import detect_delimiter_and_rows, estimate_max_cycle, preclean_lines


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="KosterDataTool")
    p.add_argument("--root", type=str, default="", help="根目录绝对路径或相对路径")
    p.add_argument("--no-gui", action="store_true", help="以 CLI 形态运行")
    p.add_argument("--selftest", action="store_true", help="运行自检")
    p.add_argument("--scan-only", action="store_true", help="仅扫描并打印摘要")
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


def _estimate_cycle_from_file(file_path: Path, file_type: str) -> int:
    raw_text = file_path.read_text(encoding="utf-8")
    clean_lines, marker_events = preclean_lines(raw_text)
    try:
        detection = detect_delimiter_and_rows(clean_lines)
    except ValueError:
        detection = detect_delimiter_and_rows(clean_lines[1:]) if len(clean_lines) > 1 else detect_delimiter_and_rows(clean_lines)

    has_cycle_col = False
    cycle_values: list[int] = []
    if file_type == "GCD":
        cycle_idx = None
        for line in clean_lines:
            if detection["delimiter"] in {"\t", ",", ";"}:
                tokens = [t.strip() for t in line.split(detection["delimiter"]) if t.strip()]
            else:
                tokens = [t.strip() for t in re.split(detection["delimiter"], line) if t.strip()]

            if cycle_idx is None:
                cycle_idx = next((i for i, tk in enumerate(tokens) if tk.lower() == "cycle"), None)
                if cycle_idx is None:
                    continue
                has_cycle_col = True
                continue
            if cycle_idx < len(tokens) and re.match(r"^[+-]?\d+$", tokens[cycle_idx]):
                cycle_values.append(int(tokens[cycle_idx]))

    has_data_after_last_marker = False
    if marker_events:
        last_marker_index = max(event["rawLineIndex"] for event in marker_events)
        kept_set = set(detection["kept_lines"])
        for raw_idx, raw_line in enumerate(raw_text.splitlines()):
            if raw_idx <= last_marker_index:
                continue
            if raw_line in kept_set:
                has_data_after_last_marker = True
                break

    return estimate_max_cycle(
        file_type=file_type,
        has_cycle_col=has_cycle_col,
        cycle_values=cycle_values if has_cycle_col else None,
        marker_events=marker_events,
        has_data_after_last_marker=has_data_after_last_marker,
    )


def _selftest(ctx, logger) -> int:
    temp_root = ctx.paths.temp_dir / f"run_{ctx.run_id}" / "selftest_root"
    struct_a, struct_b = _create_selftest_tree(temp_root)

    logger.info("selftest: start", root=str(struct_b))
    print(f"SELFTEST_ROOT={struct_b}")

    assert _estimate_cycle_from_file(struct_a / "CV-1.txt", "CV") == 4, "CV-1.txt maxCycle assertion failed"
    assert _estimate_cycle_from_file(struct_a / "GCD-0.5.txt", "GCD") == 3, "GCD-1.txt maxCycle assertion failed"
    assert _estimate_cycle_from_file(struct_a / "GCD-2.txt", "GCD") == 3, "GCD-2.txt maxCycle assertion failed"

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

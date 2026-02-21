from __future__ import annotations

from pathlib import Path

from .colmap import _append_run_report
from .output_naming import make_output_paths
from .workbook_builders import build_battery_workbook, build_electrode_workbook


def run_full_export(root_path: str, scan_result, params, selections, ctx, logger, progress_cb) -> dict:
    failures: list[str] = []
    warnings: list[str] = []

    def emit(stage: str, percent: float, current: str = "-"):
        if progress_cb:
            progress_cb(stage, current, percent)

    emit("参数校验", 5.0, root_path)
    try:
        if params.get("output_type") == "Csp":
            for b in scan_result.batteries:
                if not params["battery_params"][b.name].get("k"):
                    raise ValueError(f"{b.name}: Csp 模式 K 必填")
    except Exception as e:
        _append_run_report(str(ctx.report_path), f"FATAL 参数校验失败: {e}")
        raise

    emit("逐文件解析/分圈/选圈", 20.0, "all")
    for b in scan_result.batteries:
        for f in [*b.cv_files, *b.gcd_files, *b.eis_files]:
            try:
                _ = Path(f.path).exists()
                if not _:
                    raise FileNotFoundError(f.path)
            except Exception as e:
                msg = f"文件失败: {f.path}: {e}"
                failures.append(msg)
                logger.warning(msg)
                _append_run_report(str(ctx.report_path), msg)

    emit("GCD 分段与计算", 40.0, "GCD")
    emit("生成 Excel", 70.0, "workbook")
    ele_wb = build_electrode_workbook(scan_result, selections, params, logger, str(ctx.report_path))
    bat_wb = build_battery_workbook(scan_result, params, logger, str(ctx.report_path)) if params.get("export_battery_workbook", True) else None

    emit("保存", 85.0, "xlsx")
    electrode_path, battery_path = make_output_paths(root_path, ctx.run_id, params.get("output_type", "Csp"))
    ele_wb.save(electrode_path)
    if bat_wb is not None:
        bat_wb.save(battery_path)

    emit("结束汇总", 95.0, "report")
    _append_run_report(str(ctx.report_path), f"electrode_workbook={electrode_path}")
    _append_run_report(str(ctx.report_path), f"battery_workbook={battery_path if bat_wb is not None else '(disabled)'}")
    _append_run_report(str(ctx.report_path), f"failures={len(failures)} warnings={len(warnings)}")

    emit("完成", 100.0, "done")
    return {
        "electrode_path": electrode_path,
        "battery_path": battery_path if bat_wb is not None else "",
        "run_report_path": str(ctx.report_path),
        "log_path": str(ctx.text_log_path),
        "jsonl_log_path": str(ctx.jsonl_log_path),
        "failures": failures,
        "warnings": warnings,
    }

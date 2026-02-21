from __future__ import annotations

from pathlib import Path

from .colmap import _append_run_report
from .output_naming import make_output_paths
from .param_validation import validate_battery_row, validate_global
from .run_report import report_error
from .workbook_builders import build_battery_workbook, build_electrode_workbook


def _collect_report_messages(report_path: Path) -> tuple[list[str], list[str]]:
    failures: list[str] = []
    warnings: list[str] = []
    if not report_path.exists():
        return failures, warnings
    for raw in report_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("W"):
            warnings.append(line)
        elif line.startswith("E") or line.startswith("文件失败:") or " 文件失败 " in line:
            failures.append(line)
    return failures, warnings



def run_full_export(root_path: str, scan_result, params, selections, ctx, logger, progress_cb) -> dict:
    failures: list[str] = []
    warnings: list[str] = []

    def emit(stage: str, percent: float, current: str = "-"):
        if progress_cb:
            progress_cb(stage, current, percent)

    emit("参数校验", 5.0, root_path)
    output_type = params.get("output_type", "Csp")
    try:
        a_geom = float(params.get("a_geom", 0))
    except Exception:
        a_geom = 0
    global_errors = validate_global(output_type=output_type, a_geom=a_geom)
    row_errors: list[str] = []
    for b in scan_result.batteries:
        bp = params.get("battery_params", {}).get(b.name, {})
        errs = validate_battery_row(
            output_type=output_type,
            m_pos=bp.get("m_pos"),
            m_neg=bp.get("m_neg"),
            p_active=bp.get("p_active"),
            k=bp.get("k"),
            n_cv=bp.get("n_cv"),
            n_gcd=bp.get("n_gcd"),
            v_start=bp.get("v_start"),
            v_end=bp.get("v_end"),
            cv_max=b.cv_max_cycle,
            gcd_max=b.gcd_max_cycle,
        )
        for field, reason in errs.items():
            row_errors.append(f"{b.name}.{field}: {reason}")

    if global_errors or row_errors:
        all_errors = [*global_errors, *row_errors]
        for item in all_errors:
            _append_run_report(str(ctx.report_path), f"FATAL 参数校验失败: {item}")
        logger.error("参数校验失败，禁止导出", errors=all_errors, stage="validation")
        raise ValueError("参数校验失败，详见 run_report.txt 与 jsonl")

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

    try:
        emit("生成 Excel", 70.0, "workbook")
        ele_wb = build_electrode_workbook(scan_result, selections, params, logger, str(ctx.report_path))
        bat_wb = build_battery_workbook(scan_result, params, logger, str(ctx.report_path)) if params.get("export_battery_workbook", True) else None
    except Exception as exc:
        line = report_error(str(ctx.report_path), "E9001", "生成 Excel 失败", error=str(exc))
        logger.exception(line, code="E9001", stage="build_excel", exc=exc)
        agg_failures, agg_warnings = _collect_report_messages(Path(ctx.report_path))
        emit("结束弹窗（失败/告警清单）", 100.0, "failed")
        return {
            "electrode_path": "",
            "battery_path": "",
            "run_report_path": str(ctx.report_path),
            "log_path": str(ctx.text_log_path),
            "jsonl_log_path": str(ctx.jsonl_log_path),
            "skipped_paths_path": str(ctx.paths.reports_dir / f"skipped_paths-{ctx.run_id}.txt"),
            "failures": list(dict.fromkeys([*failures, *agg_failures])),
            "warnings": list(dict.fromkeys([*warnings, *agg_warnings])),
        }

    try:
        emit("保存", 85.0, "xlsx")
        electrode_path, battery_path = make_output_paths(root_path, ctx.run_id, params.get("output_type", "Csp"))
        ele_wb.save(electrode_path)
        if bat_wb is not None:
            bat_wb.save(battery_path)
    except Exception as exc:
        line = report_error(str(ctx.report_path), "E9002", "保存失败", error=str(exc))
        logger.exception(line, code="E9002", stage="save", exc=exc)
        agg_failures, agg_warnings = _collect_report_messages(Path(ctx.report_path))
        emit("结束弹窗（失败/告警清单）", 100.0, "failed")
        return {
            "electrode_path": "",
            "battery_path": "",
            "run_report_path": str(ctx.report_path),
            "log_path": str(ctx.text_log_path),
            "jsonl_log_path": str(ctx.jsonl_log_path),
            "skipped_paths_path": str(ctx.paths.reports_dir / f"skipped_paths-{ctx.run_id}.txt"),
            "failures": list(dict.fromkeys([*failures, *agg_failures])),
            "warnings": list(dict.fromkeys([*warnings, *agg_warnings])),
        }

    _append_run_report(str(ctx.report_path), f"electrode_workbook={electrode_path}")
    _append_run_report(str(ctx.report_path), f"battery_workbook={battery_path if bat_wb is not None else '(disabled)'}")
    agg_failures, agg_warnings = _collect_report_messages(Path(ctx.report_path))
    failures = list(dict.fromkeys([*failures, *agg_failures]))
    warnings = list(dict.fromkeys([*warnings, *agg_warnings]))
    _append_run_report(str(ctx.report_path), f"failures={len(failures)} warnings={len(warnings)}")

    emit("结束弹窗（失败/告警清单）", 100.0, "done")
    return {
        "electrode_path": electrode_path,
        "battery_path": battery_path if bat_wb is not None else "",
        "run_report_path": str(ctx.report_path),
        "log_path": str(ctx.text_log_path),
        "jsonl_log_path": str(ctx.jsonl_log_path),
        "skipped_paths_path": str(ctx.paths.reports_dir / f"skipped_paths-{ctx.run_id}.txt"),
        "failures": failures,
        "warnings": warnings,
    }

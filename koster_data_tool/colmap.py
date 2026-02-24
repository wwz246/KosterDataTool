from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path

from .fixed_tab_reader import CYCLE_TAIL_RE, read_fixed_tab_table, tokens_to_float_matrix
from .run_report import report_error
from .text_parse import extract_k_cycle_markers


@dataclass
class ColumnMapping:
    file_type: str
    delimiter: str
    modeCols: int
    kept_ratio: float
    no_header: bool
    col_index: dict[str, int]
    unit: dict[str, str]
    warnings: list[str]


def normalize_header_token(s: str) -> tuple[str, str]:
    token = unicodedata.normalize("NFKC", s or "").strip()
    m = re.search(r"[\(\[（【]\s*([^\)\]）】]+)\s*[\)\]）】]", token)
    unit_raw = m.group(1).strip() if m else ""
    if m:
        token = token[: m.start()] + token[m.end() :]
    name_norm = re.sub(r"[\s_\-\(\)\[\]\{\}/\\·*'\"]+", "", token.lower())
    return name_norm, unit_raw


def _is_density_unit(unit: str) -> bool:
    u = unicodedata.normalize("NFKC", unit).lower().replace("²", "2")
    return "/cm2" in u or "/cm^2" in u or "/mm2" in u or "/m2" in u


def map_columns_from_header(header_tokens: list[str]) -> tuple[dict[str, int], dict[str, str]]:
    col_index: dict[str, int] = {}
    unit: dict[str, str] = {}

    for i, token in enumerate(header_tokens):
        name_norm, unit_raw = normalize_header_token(token)
        nn = name_norm.lower()
        if not nn:
            continue

        mapped = None
        if "z''" in token.lower() or "zim" in nn:
            mapped = "Zim"
        elif "z'" in token.lower() or "zre" in nn:
            mapped = "Zre"
        elif nn in {"frequency", "freq", "频率"} or "frequency" in nn or "freq" in nn or "频率" in nn:
            mapped = "Freq"
        elif nn in {"time", "时间", "t"} or "time" in nn or "时间" in nn:
            mapped = "t"
        elif nn in {"voltage", "电压", "e", "potential"} or "voltage" in nn or "potential" in nn or "电压" in nn:
            mapped = "E"
        elif nn in {"step", "工步"} or "step" in nn or "工步" in nn:
            mapped = "Step"
        elif nn == "cycle" or "cycle" in nn:
            mapped = "Cycle"
        elif "chargecapacity" in nn or nn == "qchg":
            mapped = "Q_chg"
        elif "dischargecapacity" in nn or nn == "qdis":
            mapped = "Q_dis"
        elif "currentdensity" in nn or "电流密度" in nn or nn == "j":
            mapped = "j"
        elif nn in {"current", "电流", "i"} or "current" in nn or "电流" in nn:
            mapped = "j" if _is_density_unit(unit_raw) else "I"

        if mapped and mapped not in col_index:
            col_index[mapped] = i
            if unit_raw:
                unit[mapped] = unit_raw

    return col_index, unit


def _norm_unit(u: str) -> str:
    return unicodedata.normalize("NFKC", u or "").strip().lower().replace("ω", "ohm").replace("Ω", "ohm").replace("²", "2")


def _area_unit_to_cm2(u: str) -> float:
    if "cm2" in u or "cm^2" in u:
        return 1.0
    if "mm2" in u or "mm^2" in u:
        return 0.01
    if re.search(r"(^|[^c])m2", u) or re.search(r"(^|[^c])m\^2", u):
        return 10000.0
    return 1.0


def convert_units(col_index: dict[str, int], unit_raw: dict[str, str], data_cols: list[list[float]], a_geom_cm2: float) -> tuple[dict[str, str], dict[str, list[float]], list[str]]:
    unit_norm: dict[str, str] = {}
    series: dict[str, list[float]] = {}
    warnings: list[str] = []

    for key, idx in col_index.items():
        if idx >= len(data_cols):
            continue
        vals = list(data_cols[idx])
        u_raw = unit_raw.get(key, "")
        u = _norm_unit(u_raw)

        if key == "t":
            factor = 1.0
            if u.startswith("ms"):
                factor = 1e-3
            elif u.startswith("min"):
                factor = 60.0
            elif u.startswith("h"):
                factor = 3600.0
            series[key] = [v * factor for v in vals]
            unit_norm[key] = "s"
        elif key == "E":
            series[key] = vals
            unit_norm[key] = "V"
        elif key == "Freq":
            series[key] = vals
            unit_norm[key] = "Hz"
        elif key in {"I", "j"}:
            is_density = (key == "j") or _is_density_unit(u)
            if "ma" in u:
                base = [v / 1000.0 for v in vals]
            elif "ua" in u or "μa" in u:
                base = [v / 1_000_000.0 for v in vals]
            else:
                base = vals
            if is_density:
                area_factor = _area_unit_to_cm2(u)
                j_vals = [v / area_factor for v in base]
                series["j"] = j_vals
                unit_norm["j"] = "A/cm2"
                if "I" not in series:
                    series["I"] = [v * a_geom_cm2 for v in j_vals]
                    unit_norm["I"] = "A"
            else:
                series["I"] = base
                unit_norm["I"] = "A"
        elif key in {"Q_chg", "Q_dis"}:
            if "ah" in u and "mah" not in u and "uah" not in u and "μah" not in u:
                out = [v * 1000.0 for v in vals]
            elif "uah" in u or "μah" in u:
                out = [v / 1000.0 for v in vals]
            else:
                out = vals
            series[key] = out
            unit_norm[key] = "mAh"
        elif key in {"Zre", "Zim"}:
            if "cm2" in u or "cm^2" in u or "mm2" in u or "m2" in u:
                area_factor = _area_unit_to_cm2(u)
                vals = [v * area_factor / a_geom_cm2 for v in vals]
            series[key] = vals
            unit_norm[key] = "ohm"
        else:
            series[key] = vals

    for key in ("Step", "Cycle"):
        if key in col_index and key not in series:
            series[key] = list(data_cols[col_index[key]])

    return unit_norm, series, warnings


def _append_run_report(run_report_path: str, text: str) -> None:
    Path(run_report_path).parent.mkdir(parents=True, exist_ok=True)
    with Path(run_report_path).open("a", encoding="utf-8") as f:
        f.write(text + "\n")


def _extract_cycle_values(series: dict[str, list[float]]) -> tuple[bool, list[int] | None]:
    if "Cycle" not in series:
        return False, None
    vals: list[int] = []
    for v in series["Cycle"]:
        if abs(v - round(v)) > 1e-6:
            return True, None
        vals.append(int(round(v)))
    return True, vals


def _raise_with_report(code: str, message: str, file_path: str, logger, run_report_path: str) -> None:
    line = report_error(run_report_path, code, message, file=file_path)
    logger.error(line, code=code, file_path=file_path)
    raise ValueError(message)


def _enforce_required_columns(file_type: str, col_index: dict[str, int], file_path: str, logger, run_report_path: str) -> None:
    ftype = file_type.upper()
    if ftype == "CV":
        missing: list[str] = []
        if "t" not in col_index:
            missing.append("Time")
        if "E" not in col_index:
            missing.append("Voltage")
        if "I" not in col_index and "j" not in col_index:
            missing.append("Current|Current density")
        if missing:
            _raise_with_report("E9006", f"E9006: missing required columns for CV missing={','.join(missing)} file={file_path}", file_path, logger, run_report_path)
    elif ftype == "GCD":
        missing: list[str] = []
        if "t" not in col_index:
            missing.append("Time")
        if "E" not in col_index:
            missing.append("Voltage")
        if "Cycle" not in col_index:
            missing.append("Cycle")
        if missing:
            _raise_with_report("E9007", f"E9007: missing required columns for GCD missing={','.join(missing)} file={file_path}", file_path, logger, run_report_path)
    elif ftype == "EIS":
        missing: list[str] = []
        if "Freq" not in col_index:
            missing.append("Freq")
        if "Zre" not in col_index:
            missing.append("Z'")
        if "Zim" not in col_index:
            missing.append("Z''")
        if missing:
            _raise_with_report("E9008", f"E9008: missing required columns for EIS missing={','.join(missing)} file={file_path}", file_path, logger, run_report_path)


def parse_file_for_cycles(file_path: str, file_type: str, a_geom_cm2: float, v_start: float | None, v_end: float | None, logger, run_report_path: str) -> tuple[ColumnMapping, dict[str, list[float]], list[int], list[dict], bool, list[int] | None]:
    del v_start, v_end
    try:
        raw_text = Path(file_path).read_text(encoding="utf-8")
    except Exception as exc:
        _raise_with_report("E6001", f"E6001 文件读取失败（UTF-8） file={file_path}", file_path, logger, run_report_path)
        raise exc

    marker_events = extract_k_cycle_markers(raw_text)
    try:
        header, rows_tokens = read_fixed_tab_table(file_path)
    except ValueError as exc:
        msg = str(exc)
        code = msg.split(":", 1)[0] if msg.startswith("E") else "E9004"
        _raise_with_report(code, msg, file_path, logger, run_report_path)

    col_index, unit_raw = map_columns_from_header(header)
    _enforce_required_columns(file_type, col_index, file_path, logger, run_report_path)

    try:
        if file_type.upper() == "EIS":
            required_cols = ["Freq", "Zre", "Zim"]
            data_matrix = []
            for row_idx, row_tokens in enumerate(rows_tokens, start=3):
                row_vals: list[float] = [0.0 for _ in header]
                for col_key in required_cols:
                    col_idx = col_index[col_key]
                    token = row_tokens[col_idx]
                    token_clean = CYCLE_TAIL_RE.sub("", token).strip()
                    col_name = header[col_idx] if col_idx < len(header) else f"col#{col_idx}"
                    if token_clean == "":
                        raise ValueError(
                            f"E9005: non-numeric token file={file_path} row_index={row_idx} col_name={col_name} token={token}"
                        )
                    try:
                        row_vals[col_idx] = float(token_clean)
                    except ValueError as exc:
                        raise ValueError(
                            f"E9005: non-numeric token file={file_path} row_index={row_idx} col_name={col_name} token={token}"
                        ) from exc
                data_matrix.append(row_vals)
        else:
            data_matrix = tokens_to_float_matrix(header, rows_tokens, file_path=file_path)
    except ValueError as exc:
        _raise_with_report("E9005", str(exc), file_path, logger, run_report_path)

    data_cols = [] if not data_matrix else [[row[i] for row in data_matrix] for i in range(len(data_matrix[0]))]
    unit_norm, series, convert_warnings = convert_units(col_index, unit_raw, data_cols, a_geom_cm2)
    kept_raw_line_indices = [j + 3 for j in range(len(data_matrix))]

    mapping = ColumnMapping(
        file_type=file_type.upper(),
        delimiter="\\t",
        modeCols=len(header),
        kept_ratio=1.0,
        no_header=False,
        col_index=col_index,
        unit=unit_norm,
        warnings=list(convert_warnings),
    )
    has_cycle_col, cycle_values = _extract_cycle_values(series)
    return mapping, series, kept_raw_line_indices, marker_events, has_cycle_col, cycle_values


def read_and_map_file(file_path: str, file_type: str, a_geom_cm2: float, v_start: float | None, v_end: float | None, logger, run_report_path: str) -> tuple[ColumnMapping, dict[str, list[float]]]:
    mapping, series, _kept_raw_line_indices, _marker_events, _has_cycle_col, _cycle_values = parse_file_for_cycles(
        file_path=file_path,
        file_type=file_type,
        a_geom_cm2=a_geom_cm2,
        v_start=v_start,
        v_end=v_end,
        logger=logger,
        run_report_path=run_report_path,
    )
    return mapping, series

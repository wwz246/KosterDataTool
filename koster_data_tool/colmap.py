from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path

from .text_parse import detect_delimiter_and_rows_indexed, preclean_indexed_lines
from .run_report import report_error, report_warning


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


def find_header_row(raw_lines: list[str]) -> int | None:
    for idx, line in enumerate(raw_lines):
        hit = 0
        lower = unicodedata.normalize("NFKC", line).lower()
        if re.search(r"time|时间|\bt\b", lower):
            hit += 1
        if re.search(r"voltage|电压|\be\b|potential", lower):
            hit += 1
        if re.search(r"current\s*density|电流密度|\bj\b", lower):
            hit += 1
        if re.search(r"current|电流|\bi\b", lower):
            hit += 1
        if re.search(r"step|工步", lower):
            hit += 1
        if re.search(r"cycle", lower):
            hit += 1
        if "z'" in lower or "zre" in lower:
            hit += 1
        if "z''" in lower or "zim" in lower:
            hit += 1
        if hit >= 2:
            return idx
    return None


def _split(line: str, delimiter: str) -> list[str]:
    if delimiter in {"\t", ",", ";"}:
        return [t.strip() for t in line.split(delimiter)]
    return [t.strip() for t in re.split(delimiter, line)]


def _is_density_unit(unit: str) -> bool:
    u = unicodedata.normalize("NFKC", unit).lower().replace("²", "2")
    return "/cm2" in u or "/cm^2" in u or "/mm2" in u or "/m2" in u


def map_columns_from_header(file_type: str, header_line: str, delimiter: str) -> tuple[dict[str, int], dict[str, str]]:
    col_index: dict[str, int] = {}
    unit: dict[str, str] = {}

    tokens = _split(header_line, delimiter)
    for i, token in enumerate(tokens):
        name_norm, unit_raw = normalize_header_token(token)
        nn = name_norm.lower()
        if not nn:
            continue

        mapped = None
        if "z''" in token.lower() or "zim" in nn:
            mapped = "Zim"
        elif "z'" in token.lower() or "zre" in nn:
            mapped = "Zre"
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


def _is_mostly_monotonic(col: list[float]) -> bool:
    if len(col) < 3:
        return True
    bad = sum(1 for a, b in zip(col, col[1:]) if b < a - 1e-12)
    return bad / (len(col) - 1) <= 0.05


def infer_columns_no_header(file_type: str, data_cols: list[list[float]], v_start: float | None, v_end: float | None) -> tuple[dict[str, int], list[str]]:
    col_index: dict[str, int] = {}
    warnings: list[str] = []

    if not data_cols:
        return col_index, ["W6102:no numeric columns"]

    spans = [(max(c) - min(c)) if c else 0.0 for c in data_cols]
    mono_candidates = [i for i, c in enumerate(data_cols) if _is_mostly_monotonic(c)]
    if mono_candidates:
        t_idx = max(mono_candidates, key=lambda i: spans[i])
        col_index["t"] = t_idx
        warnings.append(f"W6102:infer t->col#{t_idx} by monotonic+max-span")

    vlo = (v_start - 1.0) if v_start is not None else 0.0
    vhi = (v_end + 1.0) if v_end is not None else 5.0
    v_candidates = []
    for i, c in enumerate(data_cols):
        if not c:
            continue
        ratio = sum(1 for x in c if vlo <= x <= vhi) / len(c)
        v_candidates.append((ratio, -abs((sum(c) / len(c)) - (vlo + vhi) / 2), i))
    if v_candidates:
        e_idx = sorted(v_candidates, reverse=True)[0][2]
        if sorted(v_candidates, reverse=True)[0][0] >= 0.8:
            col_index["E"] = e_idx
            warnings.append(f"W6102:infer E->col#{e_idx} by voltage window [{vlo},{vhi}]")

    for i, c in enumerate(data_cols):
        if i in col_index.values() or not c:
            continue
        med = sorted(c)[len(c) // 2]
        mad = sorted(abs(x - med) for x in c)[len(c) // 2]
        if mad <= max(1e-6, abs(med) * 0.1):
            col_index["I"] = i
            warnings.append(f"W6102:infer I->col#{i} by quasi-constant segments")
            break

    discrete = []
    for i, c in enumerate(data_cols):
        if i in col_index.values() or not c:
            continue
        if all(abs(x - round(x)) < 1e-6 for x in c):
            vals = [int(round(x)) for x in c]
            uniq = len(set(vals))
            if uniq <= max(20, len(vals) // 5):
                discrete.append((i, vals))
    if discrete:
        step_i, step_vals = discrete[0]
        col_index["Step"] = step_i
        warnings.append(f"W6102:infer Step->col#{step_i} by discrete piecewise-constant integers")
        for i, vals in discrete[1:]:
            if min(vals) <= 1 and max(vals) > 1:
                col_index["Cycle"] = i
                warnings.append(f"W6102:infer Cycle->col#{i} by repeating ascending integer pattern")
                break

    if file_type == "EIS":
        if len(data_cols) != 2:
            return {}, ["E6101:no-header EIS mapping is unstable (need exactly 2 columns for Zre/Zim)"]
        col_index = {"Zre": 0, "Zim": 1}
        warnings.append("W6102:infer EIS no-header by 2-column fallback Zre/Zim")

    return col_index, warnings


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
            warnings.append(f"W6201:column out of range for {key}")
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
            if "ohm" not in u and u:
                warnings.append(f"W6202:unrecognized impedance unit for {key}: {u_raw}")
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


def parse_file_for_cycles(file_path: str, file_type: str, a_geom_cm2: float, v_start: float | None, v_end: float | None, logger, run_report_path: str) -> tuple[ColumnMapping, dict[str,list[float]], list[int], list[dict], bool, list[int] | None]:
    raw_text = None
    for enc in ("utf-8-sig", "utf-8", "gbk", "latin-1"):
        try:
            raw_text = Path(file_path).read_text(encoding=enc)
            break
        except Exception:
            continue
    if raw_text is None:
        err = f"E6001 文件读取失败（编码不支持） file={file_path}"
        line = report_error(run_report_path, "E6001", "文件读取失败（编码不支持）", file=file_path)
        logger.error(line, code="E6001", file_path=file_path)
        raise ValueError(err)
    raw_lines = raw_text.splitlines()
    indexed_lines, marker_events = preclean_indexed_lines(raw_text)
    try:
        detection = detect_delimiter_and_rows_indexed(indexed_lines)
    except ValueError:
        trimmed = indexed_lines[1:] if len(indexed_lines) > 1 else indexed_lines
        detection = detect_delimiter_and_rows_indexed(trimmed)

    warnings: list[str] = []
    if detection["dropped_count"] > 0:
        line = report_warning(run_report_path, "W6101", "丢弃非众数列行", count=detection["dropped_count"], file=file_path)
        warnings.append(line)
        logger.warning(line, code="W6101", file=file_path, dropped_count=detection["dropped_count"])

    kept_rows = detection["kept_rows"]
    kept_raw_line_indices = [raw_idx for raw_idx, _ in kept_rows]
    data_matrix = [row for _, row in kept_rows]
    data_cols = [] if not data_matrix else [[row[i] for row in data_matrix] for i in range(len(data_matrix[0]))]

    header_idx = find_header_row(raw_lines)
    no_header = header_idx is None
    if header_idx is not None:
        col_index, unit_raw = map_columns_from_header(file_type, raw_lines[header_idx], detection["delimiter"])
    else:
        unit_raw = {}
        col_index, infer_warnings = infer_columns_no_header(file_type, data_cols, v_start, v_end)
        warnings.extend(infer_warnings)
        logger.info("no-header inference reasons", file=file_path, reasons=infer_warnings)
        if any(w.startswith("E6101") for w in infer_warnings):
            err = next(w for w in infer_warnings if w.startswith("E6101"))
            line = report_error(run_report_path, "E6101", err, file=file_path)
            logger.error(line, code="E6101", file=file_path)
            raise ValueError(err)

    unit_norm, series, convert_warnings = convert_units(col_index, unit_raw, data_cols, a_geom_cm2)
    warnings.extend(convert_warnings)

    mapping_desc = ", ".join(f"col#{v}->{k}" for k, v in sorted(col_index.items(), key=lambda kv: kv[1]))
    if no_header:
        logger.info("no-header mapping", file=file_path, mapping=mapping_desc)
    else:
        logger.info("header mapping", file=file_path, mapping=mapping_desc)
    logger.info("column mapping detail", file=file_path, no_header=no_header, warnings=warnings)

    mapping = ColumnMapping(
        file_type=file_type,
        delimiter=detection["delimiter"],
        modeCols=detection["modeCols"],
        kept_ratio=detection["kept_ratio"],
        no_header=no_header,
        col_index=col_index,
        unit=unit_norm,
        warnings=warnings,
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


def _to_data_cols(lines: list[str], delimiter: str, mode_cols: int) -> list[list[float]]:
    rows: list[list[float]] = []
    for line in lines:
        tokens = _split(line, delimiter)
        if len(tokens) != mode_cols:
            continue
        try:
            rows.append([float(t) for t in tokens])
        except ValueError:
            continue
    if not rows:
        return []
    return [[row[i] for row in rows] for i in range(len(rows[0]))]

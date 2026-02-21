from __future__ import annotations

import os
import re
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional


NUMERIC_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")
FILE_RE = re.compile(r"^(CV|GCD|EIS)-([+-]?(?:\d+(?:\.\d*)?|\.\d+))\.txt$", re.IGNORECASE)
CYCLE_MARK_RE = re.compile(r"(\d+)\s*CYCLE\s*$", re.IGNORECASE)


@dataclass
class RecognizedFile:
    file_type: str
    num: float
    path: str


@dataclass
class BatteryScan:
    name: str
    base_dir: str
    cv_files: list[RecognizedFile]
    gcd_files: list[RecognizedFile]
    eis_files: list[RecognizedFile]
    cv_max_cycle: Optional[int]
    gcd_max_cycle: Optional[int]


@dataclass
class ScanResult:
    root_path: str
    structure: str
    batteries: list[BatteryScan]
    available_cv: list[float]
    available_gcd: list[float]
    available_eis: list[float]
    recognized_file_count: int
    skipped_dir_count: int
    skipped_file_count: int
    skipped_report_path: str


def _is_number_token(token: str) -> bool:
    return bool(NUMERIC_RE.match(token.strip()))


def _split_tokens(line: str) -> list[str]:
    if "\t" in line:
        return [t.strip() for t in line.split("\t") if t.strip()]
    if "," in line:
        return [t.strip() for t in line.split(",") if t.strip()]
    if ";" in line:
        return [t.strip() for t in line.split(";") if t.strip()]
    if re.search(r"\s{2,}", line):
        return [t.strip() for t in re.split(r"\s{2,}", line) if t.strip()]
    return [t.strip() for t in line.split() if t.strip()]


def _strip_cycle_marker_tail(line: str) -> tuple[str, Optional[int]]:
    s = line.rstrip()
    m = CYCLE_MARK_RE.search(s)
    if not m:
        return s, None
    return s[: m.start()].rstrip(), int(m.group(1))


def _has_valid_data_row(line: str) -> bool:
    stripped, _ = _strip_cycle_marker_tail(line)
    if not stripped:
        return False
    tokens = _split_tokens(stripped)
    numeric_count = sum(1 for t in tokens if _is_number_token(t))
    return numeric_count >= 3


def _max_cycle_from_markers(content: str) -> Optional[int]:
    lines = content.splitlines()
    markers: list[tuple[int, int]] = []
    for i, line in enumerate(lines):
        _, mk = _strip_cycle_marker_tail(line)
        if mk is not None and mk > 0:
            markers.append((i, mk))

    if not lines:
        return None
    if not markers:
        return 1

    last_idx, n_max = max(markers, key=lambda x: x[1])
    has_data_after = any(_has_valid_data_row(line) for line in lines[last_idx + 1 :])
    return n_max + 1 if has_data_after else n_max


def _max_cycle_from_gcd_cycle_column(content: str) -> Optional[int]:
    lines = [ln for ln in content.splitlines() if ln.strip()]
    if not lines:
        return None

    cycle_col: Optional[int] = None
    max_cycle: Optional[int] = None

    for line in lines:
        tokens = _split_tokens(line)
        if not tokens:
            continue
        if cycle_col is None:
            for idx, token in enumerate(tokens):
                if token.strip().lower() == "cycle":
                    cycle_col = idx
                    break
            if cycle_col is None:
                continue
            # header line consumed
            continue

        if cycle_col >= len(tokens):
            continue
        tk = tokens[cycle_col]
        if _is_number_token(tk):
            v = int(float(tk))
            max_cycle = v if max_cycle is None else max(max_cycle, v)

    return max_cycle


def _detect_file(file_name: str, abs_path: Path) -> Optional[RecognizedFile]:
    m = FILE_RE.match(file_name)
    if not m:
        return None
    return RecognizedFile(file_type=m.group(1).upper(), num=float(m.group(2)), path=str(abs_path.resolve()))


def _sort_recognized(files: list[RecognizedFile]) -> list[RecognizedFile]:
    return sorted(files, key=lambda x: x.num)


def _safe_read_text(path: Path) -> Optional[str]:
    for enc in ("utf-8", "gbk", "latin-1"):
        try:
            return path.read_text(encoding=enc)
        except Exception:
            continue
    return None


def _collect_skipped_deep_paths(root_path: Path) -> tuple[int, int, list[str]]:
    skipped_dirs = 0
    skipped_files = 0
    skipped_paths: list[str] = []

    for cur, dirs, _files in os.walk(root_path, topdown=True):
        cur_path = Path(cur)
        rel_parts = cur_path.relative_to(root_path).parts
        cur_depth = len(rel_parts)

        to_prune: list[str] = []
        for d in list(dirs):
            sub_path = cur_path / d
            sub_depth = cur_depth + 1
            if sub_depth > 2:
                skipped_dirs += 1
                skipped_paths.append(str(sub_path.resolve()))
                for nested_cur, nested_dirs, nested_files in os.walk(sub_path):
                    for nd in nested_dirs:
                        skipped_dirs += 1
                        skipped_paths.append(str((Path(nested_cur) / nd).resolve()))
                    for nf in nested_files:
                        skipped_files += 1
                        skipped_paths.append(str((Path(nested_cur) / nf).resolve()))
                to_prune.append(d)

        if to_prune:
            dirs[:] = [d for d in dirs if d not in to_prune]

    return skipped_dirs, skipped_files, skipped_paths


def scan_root(
    root_path: str,
    program_dir: str,
    run_id: str,
    cancel_flag: threading.Event | None,
    progress_cb: Callable[[str, str, float, int, int, int, int], None] | None,
) -> ScanResult:
    root = Path(root_path).expanduser().resolve()
    reports_dir = Path(program_dir).resolve() / "KosterData" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    skipped_report_path = reports_dir / f"skipped_paths-{run_id}.txt"

    def emit(stage: str, current: str, percent: float, bcnt: int, rcnt: int, sdcnt: int, sfcnt: int) -> None:
        if progress_cb:
            progress_cb(stage, current, max(0.0, min(100.0, percent)), bcnt, rcnt, sdcnt, sfcnt)

    skipped_dir_count, skipped_file_count, skipped_paths = _collect_skipped_deep_paths(root)
    skipped_report_path.write_text("\n".join(skipped_paths), encoding="utf-8")

    recognized_count = 0
    batteries: list[BatteryScan] = []

    root_files = [p for p in root.iterdir() if p.is_file()]
    root_recognized = [rf for p in root_files if (rf := _detect_file(p.name, p))]
    structure = "A" if root_recognized else "B"

    if structure == "A":
        cv_files = _sort_recognized([f for f in root_recognized if f.file_type == "CV"])
        gcd_files = _sort_recognized([f for f in root_recognized if f.file_type == "GCD"])
        eis_files = _sort_recognized([f for f in root_recognized if f.file_type == "EIS"])
        recognized_count += len(root_recognized)

        cv_cycles: list[int] = []
        gcd_cycles: list[int] = []
        for file_obj in cv_files:
            if cancel_flag and cancel_flag.is_set():
                break
            txt = _safe_read_text(Path(file_obj.path))
            mc = _max_cycle_from_markers(txt) if txt is not None else None
            if mc is not None:
                cv_cycles.append(mc)
        for file_obj in gcd_files:
            if cancel_flag and cancel_flag.is_set():
                break
            txt = _safe_read_text(Path(file_obj.path))
            if txt is None:
                continue
            mc = _max_cycle_from_gcd_cycle_column(txt)
            if mc is None:
                mc = _max_cycle_from_markers(txt)
            if mc is not None:
                gcd_cycles.append(mc)

        batteries.append(
            BatteryScan(
                name=root.name,
                base_dir=str(root),
                cv_files=cv_files,
                gcd_files=gcd_files,
                eis_files=eis_files,
                cv_max_cycle=max(cv_cycles) if cv_cycles else None,
                gcd_max_cycle=max(gcd_cycles) if gcd_cycles else None,
            )
        )
        emit("扫描中", root.name, 100.0, len(batteries), recognized_count, skipped_dir_count, skipped_file_count)
    else:
        bat_dirs = [p for p in root.iterdir() if p.is_dir()]
        total = len(bat_dirs) if bat_dirs else 1
        for i, bat_dir in enumerate(sorted(bat_dirs, key=lambda x: x.name.lower()), start=1):
            if cancel_flag and cancel_flag.is_set():
                break
            recognized: list[RecognizedFile] = []
            for f in bat_dir.iterdir():
                if not f.is_file():
                    continue
                r = _detect_file(f.name, f)
                if r:
                    recognized.append(r)

            cv_files = _sort_recognized([f for f in recognized if f.file_type == "CV"])
            gcd_files = _sort_recognized([f for f in recognized if f.file_type == "GCD"])
            eis_files = _sort_recognized([f for f in recognized if f.file_type == "EIS"])
            recognized_count += len(recognized)

            cv_cycles: list[int] = []
            gcd_cycles: list[int] = []
            for file_obj in cv_files:
                if cancel_flag and cancel_flag.is_set():
                    break
                txt = _safe_read_text(Path(file_obj.path))
                mc = _max_cycle_from_markers(txt) if txt is not None else None
                if mc is not None:
                    cv_cycles.append(mc)
            for file_obj in gcd_files:
                if cancel_flag and cancel_flag.is_set():
                    break
                txt = _safe_read_text(Path(file_obj.path))
                if txt is None:
                    continue
                mc = _max_cycle_from_gcd_cycle_column(txt)
                if mc is None:
                    mc = _max_cycle_from_markers(txt)
                if mc is not None:
                    gcd_cycles.append(mc)

            batteries.append(
                BatteryScan(
                    name=bat_dir.name,
                    base_dir=str(bat_dir.resolve()),
                    cv_files=cv_files,
                    gcd_files=gcd_files,
                    eis_files=eis_files,
                    cv_max_cycle=max(cv_cycles) if cv_cycles else None,
                    gcd_max_cycle=max(gcd_cycles) if gcd_cycles else None,
                )
            )
            emit(
                "扫描中",
                bat_dir.name,
                i * 100.0 / total,
                len(batteries),
                recognized_count,
                skipped_dir_count,
                skipped_file_count,
            )

    available_cv = sorted({f.num for b in batteries for f in b.cv_files})
    available_gcd = sorted({f.num for b in batteries for f in b.gcd_files})
    available_eis = sorted({f.num for b in batteries for f in b.eis_files})

    emit("完成", root.name, 100.0, len(batteries), recognized_count, skipped_dir_count, skipped_file_count)
    return ScanResult(
        root_path=str(root),
        structure=structure,
        batteries=batteries,
        available_cv=available_cv,
        available_gcd=available_gcd,
        available_eis=available_eis,
        recognized_file_count=recognized_count,
        skipped_dir_count=skipped_dir_count,
        skipped_file_count=skipped_file_count,
        skipped_report_path=str(skipped_report_path.resolve()),
    )

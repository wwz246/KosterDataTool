from __future__ import annotations

import os
import re
import uuid
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

Logger = Callable[[str], None]


@dataclass
class RenameIssue:
    level: str
    message: str


_NUMBER_RE = re.compile(r"\d+(?:\.\d+)?")
_PATTERN_EIS_DONE = re.compile(r"^eis-(\d+)\.txt$", re.IGNORECASE)
_PATTERN_PREFIX_DONE = {
    "CV": re.compile(r"^cv-(\d+(?:\.\d+)?)\.txt$", re.IGNORECASE),
    "GCD": re.compile(r"^gcd-(\d+(?:\.\d+)?)\.txt$", re.IGNORECASE),
}

_CV_UNIT_RE = re.compile(
    r"(?P<num>\d+(?:\.\d+)?)\s*(?:"
    r"m\s*v\s*(?:/\s*s|(?:\*|·)?\s*s\s*(?:\^?\s*-?1|[-−⁻]?1)|ps|/\s*sec)|"
    r"v\s*(?:/\s*s|(?:\*|·)?\s*s\s*(?:\^?\s*-?1|[-−⁻]?1)|ps|/\s*sec)"
    r")",
    re.IGNORECASE,
)
_GCD_UNIT_RE = re.compile(
    r"(?P<num>\d+(?:\.\d+)?)\s*(?:"
    r"m?\s*a\s*(?:/\s*g|(?:\*|·)?\s*g\s*(?:\^?\s*-?1|[-−⁻]?1)|g\s*-?1)"
    r")",
    re.IGNORECASE,
)


def run_rename(root_dir: Path, logger: Logger | None = None) -> tuple[str, bool]:
    root_dir = root_dir.expanduser().resolve()
    issues: list[RenameIssue] = []
    renamed_lines: list[str] = []
    fallback_to_mtime = False

    def log_issue(level: str, message: str) -> None:
        issues.append(RenameIssue(level=level, message=message))
        if logger is not None:
            logger(message)

    txt_files = [p for p in root_dir.rglob("*") if p.is_file() and p.suffix.lower() == ".txt"]
    file_type_map: dict[Path, str] = {}
    for path in txt_files:
        lower_name = path.name.lower()
        if "eis" in lower_name:
            file_type_map[path] = "EIS"
        elif "cv" in lower_name:
            file_type_map[path] = "CV"
        elif "gcd" in lower_name:
            file_type_map[path] = "GCD"

    eis_by_dir: dict[Path, list[Path]] = defaultdict(list)
    cv_by_dir: dict[Path, list[Path]] = defaultdict(list)
    gcd_by_dir: dict[Path, list[Path]] = defaultdict(list)
    for path, file_type in file_type_map.items():
        if file_type == "EIS":
            eis_by_dir[path.parent].append(path)
        elif file_type == "CV":
            cv_by_dir[path.parent].append(path)
        elif file_type == "GCD":
            gcd_by_dir[path.parent].append(path)

    if os.name != "nt":
        fallback_to_mtime = True
        log_issue("INFO", "当前系统非 Windows，EIS 排序从创建时间回退为修改时间（mtime）。")

    for folder, files in eis_by_dir.items():
        _rename_eis_in_folder(folder, files, renamed_lines, log_issue, fallback_to_mtime)

    for folder, files in cv_by_dir.items():
        hint = _build_side_hint(files, "cv")
        for file_path in files:
            _rename_cv_or_gcd(file_path, "CV", hint, renamed_lines, log_issue)

    for folder, files in gcd_by_dir.items():
        hint = _build_side_hint(files, "gcd")
        for file_path in files:
            _rename_cv_or_gcd(file_path, "GCD", hint, renamed_lines, log_issue)

    summary_lines: list[str] = [f"根目录: {root_dir}"]
    if renamed_lines:
        summary_lines.append("\n【成功重命名】")
        summary_lines.extend(renamed_lines)
    if issues:
        summary_lines.append("\n【冲突/跳过/异常/提示】")
        summary_lines.extend(f"[{item.level}] {item.message}" for item in issues)
    if not renamed_lines and not issues:
        summary_lines.append("未发现可处理的 txt 文件。")

    has_conflicts = bool(issues)
    return "\n".join(summary_lines), has_conflicts


def _rename_eis_in_folder(
    folder: Path,
    files: list[Path],
    renamed_lines: list[str],
    log_issue: Callable[[str, str], None],
    fallback_to_mtime: bool,
) -> None:
    if all(_PATTERN_EIS_DONE.match(f.name) for f in files):
        return

    def sort_key(p: Path):
        try:
            if not fallback_to_mtime:
                return os.path.getctime(p)
            return p.stat().st_mtime
        except Exception:
            return p.stat().st_mtime

    sorted_files = sorted(files, key=sort_key)
    targets = {src: folder / f"EIS-{idx}.txt" for idx, src in enumerate(sorted_files, start=1)}
    source_set = {f.resolve() for f in sorted_files}

    for src, dst in targets.items():
        if dst.exists() and dst.resolve() not in source_set:
            log_issue("冲突", f"EIS 整文件夹跳过：{folder}，目标已存在且不在重命名集合内 -> {dst}")
            return

    tmp_map: dict[Path, Path] = {}
    for src in sorted_files:
        tmp_path = folder / f"TMP_{uuid.uuid4().hex}.txt"
        try:
            src.rename(tmp_path)
            tmp_map[src] = tmp_path
        except Exception as exc:
            log_issue("异常", f"EIS 第一阶段失败：{src} -> {tmp_path}，原因：{exc}")
            for old, tmp in tmp_map.items():
                if tmp.exists() and not old.exists():
                    try:
                        tmp.rename(old)
                    except Exception as rollback_exc:
                        log_issue("异常", f"EIS 回滚失败：{tmp} -> {old}，原因：{rollback_exc}")
            return

    for src, tmp_path in tmp_map.items():
        final_path = targets[src]
        if final_path.exists():
            log_issue("冲突", f"EIS 第二阶段冲突，跳过：{tmp_path} -> {final_path}（目标已存在）")
            _restore_tmp(tmp_path, src, log_issue)
            continue
        try:
            tmp_path.rename(final_path)
            renamed_lines.append(f"{src} -> {final_path}")
        except Exception as exc:
            log_issue("异常", f"EIS 第二阶段失败：{tmp_path} -> {final_path}，原因：{exc}")
            _restore_tmp(tmp_path, src, log_issue)


def _restore_tmp(tmp_path: Path, old_path: Path, log_issue: Callable[[str, str], None]) -> None:
    if not tmp_path.exists() or old_path.exists():
        return
    try:
        tmp_path.rename(old_path)
    except Exception as exc:
        log_issue("异常", f"恢复原名失败：{tmp_path} -> {old_path}，原因：{exc}")


def _rename_cv_or_gcd(
    file_path: Path,
    kind: str,
    hint_side: str | None,
    renamed_lines: list[str],
    log_issue: Callable[[str, str], None],
) -> None:
    done_match = _PATTERN_PREFIX_DONE[kind].match(file_path.name)
    if done_match:
        number = done_match.group(1)
    else:
        number = _extract_number(file_path.stem, kind, hint_side)
    if number is None:
        log_issue("跳过", f"{kind} 未提取到数字，跳过：{file_path}")
        return

    target = file_path.with_name(f"{kind}-{number}.txt")
    if target == file_path:
        return

    tmp_path = file_path.with_name(f"TMP_{uuid.uuid4().hex}.txt")
    if target.exists():
        log_issue("冲突", f"{kind} 目标已存在，跳过：{file_path} -> {target}")
        return

    try:
        file_path.rename(tmp_path)
    except Exception as exc:
        log_issue("异常", f"{kind} 第一阶段失败：{file_path} -> {tmp_path}，原因：{exc}")
        return

    if target.exists():
        log_issue("冲突", f"{kind} 第二阶段冲突，跳过：{tmp_path} -> {target}")
        _restore_tmp(tmp_path, file_path, log_issue)
        return
    try:
        tmp_path.rename(target)
        renamed_lines.append(f"{file_path} -> {target}")
    except Exception as exc:
        log_issue("异常", f"{kind} 第二阶段失败：{tmp_path} -> {target}，原因：{exc}")
        _restore_tmp(tmp_path, file_path, log_issue)


def _extract_number(stem: str, kind: str, hint_side: str | None) -> str | None:
    key = kind.lower()
    lower = stem.lower()
    key_idx = lower.find(key)
    if key_idx < 0:
        return None

    unit_match = _extract_by_unit(stem, kind, key_idx)
    if unit_match is not None:
        return unit_match
    return _extract_by_numbers_fallback(stem, key_idx, kind, hint_side)


def _extract_by_unit(stem: str, kind: str, key_idx: int) -> str | None:
    matcher = _CV_UNIT_RE if kind == "CV" else _GCD_UNIT_RE
    best = None
    best_distance = None
    for match in matcher.finditer(stem):
        distance = abs(match.start() - key_idx)
        if best_distance is None or distance < best_distance:
            best_distance = distance
            best = match.group("num")
    return best


def _extract_by_numbers_fallback(stem: str, key_idx: int, kind: str, hint_side: str | None) -> str | None:
    numbers = [(m.group(), m.start(), m.end()) for m in _NUMBER_RE.finditer(stem)]
    if not numbers:
        return None

    if hint_side in {"LEFT", "RIGHT"}:
        near = _nearest_number_on_side(numbers, key_idx, hint_side)
        if near is not None:
            return near[0]

    best = None
    best_score = None
    for num, start, end in numbers:
        score = abs(start - key_idx)
        tail = stem[end:]
        if re.match(r"^\s*[Vv]", tail):
            score += 2000
        is_decimal = "." in num
        if kind == "CV" and is_decimal:
            score += 1000
        if kind == "GCD" and not is_decimal:
            score += 800
        if best_score is None or score < best_score:
            best_score = score
            best = num
    return best


def _nearest_number_on_side(numbers: list[tuple[str, int, int]], key_idx: int, side: str) -> tuple[str, int, int] | None:
    candidates = []
    if side == "LEFT":
        candidates = [n for n in numbers if n[2] <= key_idx]
        if not candidates:
            return None
        return max(candidates, key=lambda x: x[1])
    candidates = [n for n in numbers if n[1] >= key_idx]
    if not candidates:
        return None
    return min(candidates, key=lambda x: x[1])


def _build_side_hint(files: list[Path], keyword: str) -> str | None:
    left_vals = []
    right_vals = []
    for file_path in files:
        stem = file_path.stem
        idx = stem.lower().find(keyword)
        if idx < 0:
            continue
        nums = [(m.group(), m.start(), m.end()) for m in _NUMBER_RE.finditer(stem)]
        left = _nearest_number_on_side(nums, idx, "LEFT")
        right = _nearest_number_on_side(nums, idx, "RIGHT")
        if left is not None:
            left_vals.append(left[0])
        if right is not None:
            right_vals.append(right[0])

    if left_vals and len(set(left_vals)) == 1 and right_vals and len(set(right_vals)) > 1:
        return "RIGHT"
    if right_vals and len(set(right_vals)) == 1 and left_vals and len(set(left_vals)) > 1:
        return "LEFT"
    return None

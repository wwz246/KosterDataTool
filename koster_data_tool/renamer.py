from __future__ import annotations

import csv
import json
import os
import re
import uuid
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable

Logger = Callable[[str], None]


@dataclass
class RenameIssue:
    level: str
    message: str


@dataclass
class ExtractNumberResult:
    number: str | None
    extract_method: str | None = None
    unit_match_text: str | None = None
    candidate_numbers: list[dict[str, int | str]] | None = None
    hint_side: str | None = None
    chosen_score: int | None = None


@dataclass
class RenameEvent:
    ts: str
    kind: str
    src: str
    dst: str
    stage: str
    status: str
    reason_code: str
    exception_type: str = ""
    exception_msg: str = ""
    winerror: str = ""
    extracted_number: str = ""
    extract_method: str = ""
    unit_match_text: str = ""
    candidate_numbers: str = ""
    hint_side: str = ""
    chosen_score: str = ""


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
    r"m?\s*a\s*(?:"
    r"(?:/|\*|·)\s*g(?:\s*(?:\^\s*-?1|[-−⁻]?1))?"
    r"|\s*g\s*(?:\^\s*-?1|[-−⁻]?1)"
    r"|\s*g"
    r")"
    r")",
    re.IGNORECASE,
)

_REASON_SUGGESTIONS = {
    "TARGET_EXISTS": "建议动作：目标文件已存在，先清理重复命名文件或调整命名规则后重试。",
    "STAGE1_RENAME_FAIL": "建议动作：文件可能被占用或无权限，关闭占用程序并检查读写权限后重试。",
    "STAGE2_RENAME_FAIL": "建议动作：第二阶段改名失败，检查目标路径权限与文件锁定状态。",
    "ROLLBACK_FAIL": "建议动作：回滚失败，请根据日志中的 tmp/src 路径手工恢复文件名。",
    "UNKNOWN": "建议动作：查看异常类型与错误信息，按系统报错定位具体问题。",
}


def run_rename(root_dir: Path, logger: Logger | None = None, progress_cb: Callable[[int, int, str], None] | None = None) -> tuple[str, bool]:
    root_dir = root_dir.expanduser().resolve()
    issues: list[RenameIssue] = []
    renamed_lines: list[str] = []
    events: list[RenameEvent] = []
    fallback_to_mtime = False

    def record_event(
        kind: str,
        src: Path,
        dst: Path | None,
        stage: str,
        status: str,
        reason_code: str,
        extract: ExtractNumberResult | None = None,
        exc: Exception | None = None,
    ) -> None:
        winerror = ""
        if exc is not None and hasattr(exc, "winerror") and getattr(exc, "winerror") is not None:
            winerror = str(getattr(exc, "winerror"))
        candidate_numbers = ""
        if extract is not None and extract.candidate_numbers is not None:
            candidate_numbers = json.dumps(extract.candidate_numbers, ensure_ascii=False)
        events.append(
            RenameEvent(
                ts=datetime.now().isoformat(timespec="seconds"),
                kind=kind,
                src=str(src),
                dst=str(dst) if dst is not None else "",
                stage=stage,
                status=status,
                reason_code=reason_code,
                exception_type=type(exc).__name__ if exc is not None else "",
                exception_msg=str(exc) if exc is not None else "",
                winerror=winerror,
                extracted_number=(extract.number if (extract and extract.number is not None) else ""),
                extract_method=(extract.extract_method if (extract and extract.extract_method) else ""),
                unit_match_text=(extract.unit_match_text if (extract and extract.unit_match_text) else ""),
                candidate_numbers=candidate_numbers,
                hint_side=(extract.hint_side if (extract and extract.hint_side) else ""),
                chosen_score=(str(extract.chosen_score) if (extract and extract.chosen_score is not None) else ""),
            )
        )

    def log_issue(level: str, message: str) -> None:
        issues.append(RenameIssue(level=level, message=message))
        if logger is not None:
            logger(message)

    txt_files = [p for p in root_dir.rglob("*") if p.is_file() and p.suffix.lower() == ".txt"]
    total_targets = 0
    processed = 0

    def report_progress(current: Path) -> None:
        nonlocal processed
        processed += 1
        if progress_cb is not None:
            progress_cb(processed, total_targets, str(current))

    file_type_map: dict[Path, str] = {}
    for path in txt_files:
        lower_name = path.name.lower()
        if "eis" in lower_name:
            file_type_map[path] = "EIS"
        elif "cv" in lower_name:
            file_type_map[path] = "CV"
        elif "gcd" in lower_name:
            file_type_map[path] = "GCD"

    total_targets = len(file_type_map)

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
        _rename_eis_in_folder(folder, files, renamed_lines, log_issue, fallback_to_mtime, report_progress, record_event)

    for folder, files in cv_by_dir.items():
        hint = _build_side_hint(files, "cv")
        for file_path in files:
            _rename_cv_or_gcd(file_path, "CV", hint, renamed_lines, log_issue, record_event)
            report_progress(file_path)

    for folder, files in gcd_by_dir.items():
        hint = _build_side_hint(files, "gcd")
        for file_path in files:
            _rename_cv_or_gcd(file_path, "GCD", hint, renamed_lines, log_issue, record_event)
            report_progress(file_path)

    csv_path, jsonl_path = _write_events(root_dir, events)
    if logger is not None:
        logger(f"rename logs written: csv={csv_path}, jsonl={jsonl_path}")

    stats = {
        "OK": sum(1 for e in events if e.status == "OK"),
        "SKIP": sum(1 for e in events if e.status == "SKIP"),
        "CONFLICT": sum(1 for e in events if e.status == "CONFLICT"),
        "ERROR": sum(1 for e in events if e.status == "ERROR"),
    }
    problem_codes = {e.reason_code for e in events if e.status in {"CONFLICT", "ERROR"}}

    summary_lines: list[str] = [f"根目录: {root_dir}"]
    summary_lines.append(
        f"统计: 成功={stats['OK']} 跳过={stats['SKIP']} 冲突={stats['CONFLICT']} 异常={stats['ERROR']}"
    )
    summary_lines.append(f"日志文件: CSV={csv_path}")
    summary_lines.append(f"日志文件: JSONL={jsonl_path}")
    if problem_codes:
        summary_lines.append("建议动作:")
        for code in sorted(problem_codes):
            summary_lines.append(f"- {code}: {_REASON_SUGGESTIONS.get(code, _REASON_SUGGESTIONS['UNKNOWN'])}")
    if renamed_lines:
        summary_lines.append("\n【成功重命名】")
        summary_lines.extend(renamed_lines)
    if issues:
        summary_lines.append("\n【冲突/跳过/异常/提示】")
        summary_lines.extend(f"[{item.level}] {item.message}" for item in issues)
    if not renamed_lines and not issues:
        summary_lines.append("未发现可处理的 txt 文件。")

    has_conflicts = stats["CONFLICT"] > 0 or stats["ERROR"] > 0
    return "\n".join(summary_lines), has_conflicts


def _write_events(root_dir: Path, events: list[RenameEvent]) -> tuple[Path, Path]:
    logs_dir = root_dir / "_rename_logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = logs_dir / f"rename_{stamp}.csv"
    jsonl_path = logs_dir / f"rename_{stamp}.jsonl"

    fields = list(RenameEvent.__dataclass_fields__.keys())
    with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for event in events:
            writer.writerow(asdict(event))

    with jsonl_path.open("w", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(asdict(event), ensure_ascii=False) + "\n")
    return csv_path, jsonl_path


def _rename_eis_in_folder(
    folder: Path,
    files: list[Path],
    renamed_lines: list[str],
    log_issue: Callable[[str, str], None],
    fallback_to_mtime: bool,
    progress_cb: Callable[[Path], None],
    record_event: Callable[[str, Path, Path | None, str, str, str, ExtractNumberResult | None, Exception | None], None],
) -> None:
    if all(_PATTERN_EIS_DONE.match(f.name) for f in files):
        for f in files:
            record_event("EIS", f, f, "plan", "SKIP", "UNKNOWN")
            progress_cb(f)
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
        record_event("EIS", src, dst, "plan", "OK", "UNKNOWN")
        if dst.exists() and dst.resolve() not in source_set:
            log_issue("冲突", f"EIS 整文件夹跳过：{folder}，目标已存在且不在重命名集合内 -> {dst}")
            for f in files:
                record_event("EIS", f, targets.get(f), "plan", "CONFLICT", "TARGET_EXISTS")
                progress_cb(f)
            return

    tmp_map: dict[Path, Path] = {}
    for src in sorted_files:
        tmp_path = folder / f"TMP_{uuid.uuid4().hex}.txt"
        try:
            src.rename(tmp_path)
            tmp_map[src] = tmp_path
            record_event("EIS", src, tmp_path, "stage1_tmp", "OK", "UNKNOWN")
        except Exception as exc:
            log_issue("异常", f"EIS 第一阶段失败：{src} -> {tmp_path}，原因：{exc}")
            record_event("EIS", src, tmp_path, "stage1_tmp", "ERROR", "STAGE1_RENAME_FAIL", exc=exc)
            for old, tmp in tmp_map.items():
                if tmp.exists() and not old.exists():
                    try:
                        tmp.rename(old)
                        record_event("EIS", tmp, old, "rollback", "OK", "UNKNOWN")
                    except Exception as rollback_exc:
                        log_issue("异常", f"EIS 回滚失败：{tmp} -> {old}，原因：{rollback_exc}")
                        record_event("EIS", tmp, old, "rollback", "ERROR", "ROLLBACK_FAIL", exc=rollback_exc)
            for pending in sorted_files[len(tmp_map):]:
                progress_cb(pending)
            for done in tmp_map:
                progress_cb(done)
            return

    for src, tmp_path in tmp_map.items():
        final_path = targets[src]
        if final_path.exists():
            log_issue("冲突", f"EIS 第二阶段冲突，跳过：{tmp_path} -> {final_path}（目标已存在）")
            record_event("EIS", tmp_path, final_path, "stage2_final", "CONFLICT", "TARGET_EXISTS")
            _restore_tmp(tmp_path, src, log_issue, "EIS", record_event)
            progress_cb(src)
            continue
        try:
            tmp_path.rename(final_path)
            renamed_lines.append(f"{src} -> {final_path}")
            record_event("EIS", tmp_path, final_path, "stage2_final", "OK", "UNKNOWN")
        except Exception as exc:
            log_issue("异常", f"EIS 第二阶段失败：{tmp_path} -> {final_path}，原因：{exc}")
            record_event("EIS", tmp_path, final_path, "stage2_final", "ERROR", "STAGE2_RENAME_FAIL", exc=exc)
            _restore_tmp(tmp_path, src, log_issue, "EIS", record_event)
        finally:
            progress_cb(src)


def _restore_tmp(
    tmp_path: Path,
    old_path: Path,
    log_issue: Callable[[str, str], None],
    kind: str,
    record_event: Callable[[str, Path, Path | None, str, str, str, ExtractNumberResult | None, Exception | None], None],
) -> None:
    if not tmp_path.exists() or old_path.exists():
        return
    try:
        tmp_path.rename(old_path)
        record_event(kind, tmp_path, old_path, "rollback", "OK", "UNKNOWN")
    except Exception as exc:
        log_issue("异常", f"恢复原名失败：{tmp_path} -> {old_path}，原因：{exc}")
        record_event(kind, tmp_path, old_path, "rollback", "ERROR", "ROLLBACK_FAIL", exc=exc)


def _rename_cv_or_gcd(
    file_path: Path,
    kind: str,
    hint_side: str | None,
    renamed_lines: list[str],
    log_issue: Callable[[str, str], None],
    record_event: Callable[[str, Path, Path | None, str, str, str, ExtractNumberResult | None, Exception | None], None],
) -> None:
    extract: ExtractNumberResult
    done_match = _PATTERN_PREFIX_DONE[kind].match(file_path.name)
    if done_match:
        extract = ExtractNumberResult(number=done_match.group(1), extract_method="done_match", hint_side=hint_side)
    else:
        extract = _extract_number(file_path.stem, kind, hint_side)
    number = extract.number
    if number is None:
        log_issue("跳过", f"{kind} 未提取到数字，跳过：{file_path}")
        record_event(kind, file_path, None, "plan", "SKIP", "NO_NUMBER", extract=extract)
        return

    target = file_path.with_name(f"{kind}-{number}.txt")
    record_event(kind, file_path, target, "plan", "OK", "UNKNOWN", extract=extract)
    if target == file_path:
        record_event(kind, file_path, target, "plan", "SKIP", "UNKNOWN", extract=extract)
        return

    tmp_path = file_path.with_name(f"TMP_{uuid.uuid4().hex}.txt")
    if target.exists():
        log_issue("冲突", f"{kind} 目标已存在，跳过：{file_path} -> {target}")
        record_event(kind, file_path, target, "plan", "CONFLICT", "TARGET_EXISTS", extract=extract)
        return

    try:
        file_path.rename(tmp_path)
        record_event(kind, file_path, tmp_path, "stage1_tmp", "OK", "UNKNOWN", extract=extract)
    except Exception as exc:
        log_issue("异常", f"{kind} 第一阶段失败：{file_path} -> {tmp_path}，原因：{exc}")
        record_event(kind, file_path, tmp_path, "stage1_tmp", "ERROR", "STAGE1_RENAME_FAIL", extract=extract, exc=exc)
        return

    if target.exists():
        log_issue("冲突", f"{kind} 第二阶段冲突，跳过：{tmp_path} -> {target}")
        record_event(kind, tmp_path, target, "stage2_final", "CONFLICT", "TARGET_EXISTS", extract=extract)
        _restore_tmp(tmp_path, file_path, log_issue, kind, record_event)
        return
    try:
        tmp_path.rename(target)
        renamed_lines.append(f"{file_path} -> {target}")
        record_event(kind, tmp_path, target, "stage2_final", "OK", "UNKNOWN", extract=extract)
    except Exception as exc:
        log_issue("异常", f"{kind} 第二阶段失败：{tmp_path} -> {target}，原因：{exc}")
        record_event(kind, tmp_path, target, "stage2_final", "ERROR", "STAGE2_RENAME_FAIL", extract=extract, exc=exc)
        _restore_tmp(tmp_path, file_path, log_issue, kind, record_event)


def _extract_number(stem: str, kind: str, hint_side: str | None) -> ExtractNumberResult:
    key = kind.lower()
    lower = stem.lower()
    key_idx = lower.find(key)
    if key_idx < 0:
        return ExtractNumberResult(number=None, hint_side=hint_side)

    unit_match = _extract_by_unit(stem, kind, key_idx)
    if unit_match is not None:
        return ExtractNumberResult(
            number=unit_match.group("num"),
            extract_method="unit_regex",
            unit_match_text=unit_match.group(0),
            hint_side=hint_side,
        )
    return _extract_by_numbers_fallback(stem, key_idx, kind, hint_side)


def _extract_by_unit(stem: str, kind: str, key_idx: int) -> re.Match[str] | None:
    matcher = _CV_UNIT_RE if kind == "CV" else _GCD_UNIT_RE
    best = None
    best_distance = None
    for match in matcher.finditer(stem):
        distance = abs(match.start() - key_idx)
        if best_distance is None or distance < best_distance:
            best_distance = distance
            best = match
    return best


def _extract_by_numbers_fallback(stem: str, key_idx: int, kind: str, hint_side: str | None) -> ExtractNumberResult:
    numbers = [(m.group(), m.start(), m.end()) for m in _NUMBER_RE.finditer(stem)]
    candidate_numbers = [{"value": n, "start": s, "end": e} for n, s, e in numbers]
    if not numbers:
        return ExtractNumberResult(
            number=None,
            extract_method="fallback_numbers",
            candidate_numbers=candidate_numbers,
            hint_side=hint_side,
        )

    if hint_side in {"LEFT", "RIGHT"}:
        near = _nearest_number_on_side(numbers, key_idx, hint_side)
        if near is not None:
            return ExtractNumberResult(
                number=near[0],
                extract_method="fallback_numbers",
                candidate_numbers=candidate_numbers,
                hint_side=hint_side,
                chosen_score=abs(near[1] - key_idx),
            )

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
    return ExtractNumberResult(
        number=best,
        extract_method="fallback_numbers",
        candidate_numbers=candidate_numbers,
        hint_side=hint_side,
        chosen_score=best_score,
    )


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

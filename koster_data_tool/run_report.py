from __future__ import annotations

from pathlib import Path


def _format_line(code: str, message: str, **kv) -> str:
    if not code or code[0] not in {"W", "E"}:
        raise ValueError("code must start with W or E")
    extras = " ".join(f"{k}={v}" for k, v in kv.items())
    return f"{code} {message}" + (f" {extras}" if extras else "")


def _append_line(run_report_path: str, line: str) -> None:
    p = Path(run_report_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def report_warning(run_report_path: str, code: str, message: str, **kv) -> str:
    line = _format_line(code, message, **kv)
    _append_line(run_report_path, line)
    return line


def report_error(run_report_path: str, code: str, message: str, **kv) -> str:
    line = _format_line(code, message, **kv)
    _append_line(run_report_path, line)
    return line

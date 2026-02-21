from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple

from .paths import get_program_dir
from .logging_utils import DualLogger


FATAL_NOT_WRITABLE_MESSAGE = "程序所在文件夹不可写，请将程序放到可写目录（例如桌面/文档）后重试"


@dataclass(frozen=True)
class AppPaths:
    program_dir: Path
    kosterdata_dir: Path
    config_dir: Path
    state_dir: Path
    logs_dir: Path
    reports_dir: Path
    cache_dir: Path
    temp_dir: Path


@dataclass(frozen=True)
class RunContext:
    run_id: str
    paths: AppPaths
    text_log_path: Path
    jsonl_log_path: Path
    report_path: Path


def make_run_id(now: Optional[datetime] = None) -> str:
    dt = now or datetime.now()
    return dt.strftime("%Y%m%d_%H%M%S")


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def ensure_program_dir_writable(program_dir: Path) -> None:
    test_path = program_dir / ".__koster_write_test__"
    try:
        with test_path.open("w", encoding="utf-8") as f:
            f.write("ok")
        test_path.unlink(missing_ok=True)
    except Exception as e:
        raise PermissionError(FATAL_NOT_WRITABLE_MESSAGE) from e


def build_app_paths(program_dir: Path) -> AppPaths:
    kosterdata_dir = program_dir / "KosterData"
    return AppPaths(
        program_dir=program_dir,
        kosterdata_dir=kosterdata_dir,
        config_dir=kosterdata_dir / "config",
        state_dir=kosterdata_dir / "state",
        logs_dir=kosterdata_dir / "logs",
        reports_dir=kosterdata_dir / "reports",
        cache_dir=kosterdata_dir / "cache",
        temp_dir=kosterdata_dir / "temp",
    )


def create_runtime_dirs(paths: AppPaths) -> None:
    _ensure_dir(paths.config_dir)
    _ensure_dir(paths.state_dir)
    _ensure_dir(paths.logs_dir)
    _ensure_dir(paths.reports_dir)
    _ensure_dir(paths.cache_dir)
    _ensure_dir(paths.temp_dir)


def _read_text(p: Path) -> Optional[str]:
    try:
        return p.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None


def _write_text(p: Path, s: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(s, encoding="utf-8")


def cleanup_if_due(paths: AppPaths, logger: Optional[DualLogger] = None) -> None:
    last_cleanup_path = paths.state_dir / "last_cleanup.txt"
    today = datetime.now().date()
    last_str = _read_text(last_cleanup_path)
    if not last_str:
        _write_text(last_cleanup_path, today.isoformat())
        if logger:
            logger.info("cleanup: first run, created last_cleanup.txt", date=today.isoformat())
        return

    try:
        last_date = datetime.strptime(last_str, "%Y-%m-%d").date()
    except Exception:
        last_date = today - timedelta(days=30)

    if (today - last_date).days < 30:
        if logger:
            logger.info("cleanup: not due", last=last_str, today=today.isoformat())
        return

    cutoff_ts = (datetime.now() - timedelta(days=30)).timestamp()
    targets = [paths.logs_dir, paths.reports_dir, paths.cache_dir, paths.temp_dir]

    deleted_files = 0
    deleted_dirs = 0

    for base in targets:
        if not base.exists():
            continue
        for file_path in base.rglob("*"):
            if file_path.is_file():
                try:
                    if file_path.stat().st_mtime < cutoff_ts:
                        file_path.unlink(missing_ok=True)
                        deleted_files += 1
                except Exception as e:
                    if logger:
                        logger.warning("cleanup: failed to delete file", path=str(file_path), error=str(e))
        for dir_path in sorted([p for p in base.rglob("*") if p.is_dir()], key=lambda x: len(str(x)), reverse=True):
            try:
                if not any(dir_path.iterdir()):
                    dir_path.rmdir()
                    deleted_dirs += 1
            except Exception:
                pass

    _write_text(last_cleanup_path, today.isoformat())
    if logger:
        logger.info("cleanup: done", deleted_files=deleted_files, deleted_dirs=deleted_dirs, date=today.isoformat())


def init_run_context() -> Tuple[RunContext, DualLogger]:
    program_dir = get_program_dir()
    ensure_program_dir_writable(program_dir)

    paths = build_app_paths(program_dir)
    create_runtime_dirs(paths)

    run_id = make_run_id()
    text_log_path = paths.logs_dir / f"run_{run_id}.log"
    jsonl_log_path = paths.logs_dir / f"run_{run_id}.jsonl"
    report_path = paths.reports_dir / f"run_{run_id}_report.txt"
    report_path.touch(exist_ok=True)

    logger = DualLogger(text_log_path=text_log_path, jsonl_log_path=jsonl_log_path)
    logger.info("startup", run_id=run_id, program_dir=str(program_dir), mode="unknown")

    cleanup_if_due(paths, logger=logger)

    ctx = RunContext(
        run_id=run_id,
        paths=paths,
        text_log_path=text_log_path,
        jsonl_log_path=jsonl_log_path,
        report_path=report_path,
    )
    return ctx, logger

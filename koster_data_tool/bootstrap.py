from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple

from . import __version__
from .logging_utils import DualLogger
from .paths import get_program_dir


FATAL_NOT_WRITABLE_MESSAGE = "数据目录不可写，请设置 KOSTERDATA_HOME 到可写目录后重试"


@dataclass(frozen=True)
class AppPaths:
    program_dir: Path
    kosterdata_dir: Path
    config_dir: Path
    state_dir: Path
    output_dir: Path


@dataclass(frozen=True)
class RunContext:
    run_id: str
    paths: AppPaths
    text_log_path: Path
    report_path: Path
    run_output_path: Path
    run_temp_dir: Path


def make_run_id(now: Optional[datetime] = None) -> str:
    dt = now or datetime.now()
    return dt.strftime("%Y%m%d_%H%M%S")


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _ensure_writable(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)
    probe = p / ".__koster_write_test__.txt"
    try:
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
    except Exception as e:
        raise PermissionError(FATAL_NOT_WRITABLE_MESSAGE) from e


def get_data_root() -> Path:
    env_home = os.environ.get("KOSTERDATA_HOME", "").strip()
    if env_home:
        return Path(env_home).expanduser().resolve()
    home = Path.home()
    docs = home / "Documents"
    if docs.exists():
        return docs.resolve()
    return home.resolve()


def build_app_paths(program_dir: Path) -> AppPaths:
    kosterdata_dir = get_data_root() / "KosterData"
    return AppPaths(
        program_dir=program_dir,
        kosterdata_dir=kosterdata_dir,
        config_dir=kosterdata_dir / "config",
        state_dir=kosterdata_dir / "state",
        output_dir=kosterdata_dir / "output",
    )


def create_runtime_dirs(paths: AppPaths) -> None:
    _ensure_writable(paths.kosterdata_dir)
    _ensure_dir(paths.config_dir)
    _ensure_dir(paths.state_dir)
    _ensure_dir(paths.output_dir)


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
            logger.info("cleanup: first run", date=today.isoformat())
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
    deleted_files = 0

    for file_path in paths.output_dir.glob("run_*.txt"):
        try:
            if file_path.stat().st_mtime < cutoff_ts:
                file_path.unlink(missing_ok=True)
                deleted_files += 1
        except Exception as e:
            if logger:
                logger.warning("cleanup: failed", path=str(file_path), error=str(e))

    _write_text(last_cleanup_path, today.isoformat())
    if logger:
        logger.info("cleanup: done", deleted_files=deleted_files, date=today.isoformat())


def load_optional_config(paths: AppPaths, logger: Optional[DualLogger] = None) -> dict[str, str]:
    cfg = paths.config_dir / "config.txt"
    out: dict[str, str] = {}
    if not cfg.exists():
        if logger:
            logger.info("config: default (no config.txt)", path=str(cfg))
        return out
    for line in cfg.read_text(encoding="utf-8", errors="ignore").splitlines():
        t = line.strip()
        if not t or t.startswith("#"):
            continue
        if "=" in t:
            k, v = t.split("=", 1)
        elif ":" in t:
            k, v = t.split(":", 1)
        else:
            continue
        out[k.strip()] = v.strip()
    if logger:
        logger.info("config: loaded", path=str(cfg), keys=list(out.keys()))
    return out


def init_run_context() -> Tuple[RunContext, DualLogger]:
    program_dir = get_program_dir()
    paths = build_app_paths(program_dir)
    create_runtime_dirs(paths)

    run_id = make_run_id()
    run_output_path = paths.output_dir / f"run_{run_id}.txt"
    report_path = paths.output_dir / f"run_{run_id}_report.txt"
    text_log_path = paths.output_dir / f"run_{run_id}_log.txt"
    report_path.touch(exist_ok=True)
    run_output_path.touch(exist_ok=True)
    run_temp_dir = paths.output_dir / f"run_{run_id}_tmp"
    run_temp_dir.mkdir(parents=True, exist_ok=True)

    logger = DualLogger(text_log_path=text_log_path)
    logger.info("startup", run_id=run_id, program_dir=str(program_dir), data_root=str(paths.kosterdata_dir), mode="unknown", version=__version__)

    cleanup_if_due(paths, logger=logger)
    _ = load_optional_config(paths, logger=logger)

    ctx = RunContext(
        run_id=run_id,
        paths=paths,
        text_log_path=text_log_path,
        report_path=report_path,
        run_output_path=run_output_path,
        run_temp_dir=run_temp_dir,
    )
    return ctx, logger

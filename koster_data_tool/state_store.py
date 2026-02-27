from __future__ import annotations

import json
from pathlib import Path


_PREFS_FILE = "koster_dir_prefs.json"


def _prefs_path(program_dir: Path) -> Path:
    return program_dir / _PREFS_FILE


def read_last_root(program_dir: Path) -> Path | None:
    cfg_path = _prefs_path(program_dir)
    try:
        data = json.loads(cfg_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except Exception:
        return None
    value = data.get("last_root_dir")
    if not value:
        return None
    try:
        return Path(value).expanduser().resolve()
    except Exception:
        return None


def write_last_root(program_dir: Path, root_path: Path) -> None:
    cfg_path = _prefs_path(program_dir)
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    data = {"last_root_dir": str(root_path.expanduser().resolve())}
    cfg_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def resolve_initial_dir_from_last_root(program_dir: Path, fallback_dir: Path) -> Path:
    last_root = read_last_root(program_dir)
    if last_root is not None:
        parent = last_root.parent
        if parent.exists():
            return parent
        if last_root.exists():
            return last_root
    home = Path.home()
    if home.exists():
        return home
    return fallback_dir

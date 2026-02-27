from __future__ import annotations

import json
from pathlib import Path


def read_last_root(state_dir: Path) -> Path | None:
    target = state_dir / "last_root.txt"
    try:
        content = target.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None
    if not content:
        return None
    try:
        return Path(content).expanduser().resolve()
    except Exception:
        return None


def write_last_root(state_dir: Path, root_path: Path) -> None:
    target = state_dir / "last_root.txt"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(str(root_path.expanduser().resolve()), encoding="utf-8")


def read_last_dir(program_dir: Path) -> Path | None:
    cfg_path = program_dir / "koster_dir_prefs.json"
    try:
        data = json.loads(cfg_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except Exception:
        return None
    value = data.get("last_dir")
    if not value:
        return None
    try:
        return Path(value).expanduser().resolve()
    except Exception:
        return None


def write_last_dir(program_dir: Path, dir_path: Path) -> None:
    cfg_path = program_dir / "koster_dir_prefs.json"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    data = {"last_dir": str(dir_path.expanduser().resolve())}
    cfg_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

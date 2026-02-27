from __future__ import annotations

from pathlib import Path


_PREFS_FILE = "koster_dir_prefs.txt"


def _prefs_path(state_dir: Path) -> Path:
    return state_dir / _PREFS_FILE


def _parse_kv(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw in text.splitlines():
        t = raw.strip()
        if not t or t.startswith("#") or "=" not in t:
            continue
        k, v = t.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def _dump_kv(data: dict[str, str]) -> str:
    return "\n".join(f"{k}={v}" for k, v in data.items()) + "\n"


def read_last_root(state_dir: Path) -> Path | None:
    cfg_path = _prefs_path(state_dir)
    try:
        data = _parse_kv(cfg_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except Exception:
        return None
    value = data.get("last_root_dir", "")
    if not value:
        return None
    try:
        return Path(value).expanduser().resolve()
    except Exception:
        return None


def write_last_root(state_dir: Path, root_path: Path) -> None:
    cfg_path = _prefs_path(state_dir)
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    data = {}
    if cfg_path.exists():
        try:
            data = _parse_kv(cfg_path.read_text(encoding="utf-8"))
        except Exception:
            data = {}
    data["last_root_dir"] = str(root_path.expanduser().resolve())
    cfg_path.write_text(_dump_kv(data), encoding="utf-8")


def resolve_initial_dir_from_last_root(state_dir: Path, fallback_dir: Path) -> Path:
    last_root = read_last_root(state_dir)
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

from __future__ import annotations

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

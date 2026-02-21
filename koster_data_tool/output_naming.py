from __future__ import annotations

from pathlib import Path


def _dedup_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suf = path.suffix
    idx = 1
    while True:
        cand = path.with_name(f"{stem}_{idx}{suf}")
        if not cand.exists():
            return cand
        idx += 1


def make_output_paths(root_path: str, run_id: str, output_type: str) -> tuple[str, str]:
    root = Path(root_path).expanduser().resolve()
    name = root.name
    t = "Csp" if output_type == "Csp" else "Qsp"
    p1 = root / f"{name}-极片级-{t}-{run_id}.xlsx"
    p2 = root / f"{name}-电池级-{t}-{run_id}.xlsx"
    p1 = _dedup_path(p1)
    p2 = _dedup_path(p2)
    if p1 == p2:
        p2 = _dedup_path(root / f"{name}-电池级-{t}-{run_id}_1.xlsx")
    return str(p1), str(p2)

from __future__ import annotations

import json
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


def _now_iso_local() -> str:
    # 使用本地时间（UI/用户更直观）；保留 ISO 格式
    return datetime.now().astimezone().isoformat(timespec="seconds")


@dataclass
class DualLogger:
    text_log_path: Path
    jsonl_log_path: Path

    def _write_text(self, line: str) -> None:
        self.text_log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.text_log_path.open("a", encoding="utf-8", newline="\n") as f:
            f.write(line + "\n")

    def _write_jsonl(self, obj: Dict[str, Any]) -> None:
        self.jsonl_log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.jsonl_log_path.open("a", encoding="utf-8", newline="\n") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")

    def log(self, level: str, message: str, **fields: Any) -> None:
        ts = _now_iso_local()
        self._write_text(f"{ts}\t{level}\t{message}")
        payload: Dict[str, Any] = {"ts": ts, "level": level, "msg": message}
        if fields:
            payload.update(fields)
        self._write_jsonl(payload)

    def info(self, message: str, **fields: Any) -> None:
        self.log("INFO", message, **fields)

    def warning(self, message: str, **fields: Any) -> None:
        self.log("WARNING", message, **fields)

    def error(self, message: str, **fields: Any) -> None:
        self.log("ERROR", message, **fields)

    def exception(self, message: str, exc: BaseException, **fields: Any) -> None:
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        self.log("EXCEPTION", message, traceback=tb, **fields)

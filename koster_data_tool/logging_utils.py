from __future__ import annotations

import traceback
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


def _now_iso_local() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


@dataclass
class DualLogger:
    text_log_path: Path

    def _write_text(self, line: str) -> None:
        self.text_log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.text_log_path.open("a", encoding="utf-8", newline="\n") as f:
            f.write(line + "\n")

    def log(self, level: str, message: str, **fields: Any) -> None:
        ts = _now_iso_local()
        extra = ""
        if fields:
            extra = "\t" + "\t".join(f"{k}={v}" for k, v in fields.items())
        self._write_text(f"{ts}\t{level}\t{message}{extra}")

    def info(self, message: str, **fields: Any) -> None:
        self.log("INFO", message, **fields)

    def warning(self, message: str, **fields: Any) -> None:
        self.log("WARNING", message, **fields)

    def error(self, message: str, **fields: Any) -> None:
        self.log("ERROR", message, **fields)

    def exception(self, message: str, exc: BaseException, **fields: Any) -> None:
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        self.log("EXCEPTION", message, traceback=tb, **fields)

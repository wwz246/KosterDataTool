from __future__ import annotations

import argparse
import sys
from pathlib import Path
import tempfile

from .bootstrap import init_run_context
from .gui import run_gui


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="KosterDataTool")
    p.add_argument("--root", type=str, default="", help="根目录绝对路径或相对路径")
    p.add_argument("--no-gui", action="store_true", help="以 CLI 形态运行")
    p.add_argument("--selftest", action="store_true", help="运行自检")
    return p


def _selftest(root_arg: str, logger) -> int:
    # 本步骤自检仅验证：启动目录、日志、temp 写入。后续步骤再补全“读表→分段→计算→导出”的链路自检。
    if root_arg:
        root = Path(root_arg).expanduser().resolve()
        root.mkdir(parents=True, exist_ok=True)
    else:
        root = Path(tempfile.mkdtemp(prefix="koster_selftest_root_")).resolve()

    logger.info("selftest: start", root=str(root))
    print(f"SELFTEST_ROOT={root}")
    logger.info("selftest: ok")
    return 0


def _run_cli(args) -> int:
    ctx, logger = init_run_context()
    logger.info("mode", mode="CLI")

    if args.selftest:
        return _selftest(args.root, logger)

    # 解析/扫描/导出将在后续步骤实现
    print("CLI skeleton ready. Use --selftest to verify runtime dirs and logs.")
    print(f"LOG_TEXT={ctx.text_log_path}")
    print(f"LOG_JSONL={ctx.jsonl_log_path}")
    print(f"REPORT={ctx.report_path}")
    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.no_gui:
        return _run_cli(args)

    # GUI 模式
    return run_gui()

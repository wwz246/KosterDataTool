from __future__ import annotations

import sys
from pathlib import Path


def get_program_dir() -> Path:
    """
    program_dir = 程序(exe)所在文件夹。
    - PyInstaller frozen: sys.executable 的父目录
    - 开发态: 仓库根目录（main.py 所在目录）
    """
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    # koster_data_tool/paths.py -> koster_data_tool -> repo_root
    return Path(__file__).resolve().parent.parent


def resource_path(relative_path: str) -> Path:
    """
    兼容 PyInstaller onefile/onedir 的资源路径解析：
    - onefile: sys._MEIPASS 指向临时解压目录
    - 其他: program_dir
    """
    if hasattr(sys, "_MEIPASS"):
        return Path(getattr(sys, "_MEIPASS")).resolve() / relative_path
    return get_program_dir() / relative_path

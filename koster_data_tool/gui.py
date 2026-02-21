from __future__ import annotations

import sys
import tkinter as tk
from tkinter import messagebox

from .bootstrap import init_run_context, FATAL_NOT_WRITABLE_MESSAGE


WINDOW_TITLE = "科斯特工作站电化学数据处理"


def run_gui() -> int:
    try:
        ctx, logger = init_run_context()
        logger.info("mode", mode="GUI")
    except PermissionError as e:
        # program_dir 不可写：必须立即终止并弹窗明确提示
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror(WINDOW_TITLE, str(e) if str(e) else FATAL_NOT_WRITABLE_MESSAGE)
        root.destroy()
        return 2
    except Exception as e:
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror(WINDOW_TITLE, f"启动失败：{e}")
        root.destroy()
        return 2

    app = tk.Tk()
    app.title(WINDOW_TITLE)
    app.geometry("900x600")

    # 仅创建空壳窗口：后续步骤再加入“两步页面/进度区/表格/按钮”等完整 UI
    label = tk.Label(app, text="GUI skeleton ready. 后续步骤将实现两步页面与导出逻辑。")
    label.pack(padx=20, pady=20, anchor="w")

    app.mainloop()
    return 0

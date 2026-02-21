from __future__ import annotations

import os
import queue
import subprocess
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

from .bootstrap import FATAL_NOT_WRITABLE_MESSAGE, init_run_context
from .scanner import ScanResult, scan_root

WINDOW_TITLE = "科斯特工作站电化学数据处理"


class App:
    def __init__(self, root: tk.Tk, ctx):
        self.root = root
        self.ctx = ctx
        self.scan_result: ScanResult | None = None
        self.selected_root: Path | None = None
        self.cancel_event = threading.Event()
        self.scan_thread: threading.Thread | None = None
        self.msg_q: queue.Queue = queue.Queue()

        self.stage_var = tk.StringVar(value="待机")
        self.current_var = tk.StringVar(value="-")
        self.percent_var = tk.StringVar(value="0.0")
        self.battery_count_var = tk.StringVar(value="0")
        self.recognized_count_var = tk.StringVar(value="0")
        self.skipped_dir_var = tk.StringVar(value="0")
        self.skipped_file_var = tk.StringVar(value="0")
        self.root_path_var = tk.StringVar(value="未选择")

        self.output_type_var = tk.StringVar(value="Csp（F/g）")
        self.a_geom_var = tk.StringVar(value="1")
        self.export_book_var = tk.BooleanVar(value=True)
        self.open_folder_var = tk.BooleanVar(value=True)

        self._build_ui()
        self._load_default_open_dir()
        self._poll_queue()

    def _build_ui(self) -> None:
        self.root.title(WINDOW_TITLE)
        self.root.geometry("980x700")

        top = ttk.Frame(self.root, padding=10)
        top.pack(fill="x")
        ttk.Label(top, text=WINDOW_TITLE, font=("Arial", 16, "bold")).pack(anchor="w")
        ttk.Label(top, text="步骤 1：根目录选择+扫描（可取消）    |    步骤 2：展示扫描结果+全局选项").pack(anchor="w", pady=(4, 0))

        self.notebook = ttk.Notebook(self.root)
        self.page1 = ttk.Frame(self.notebook, padding=10)
        self.page2 = ttk.Frame(self.notebook, padding=10)
        self.notebook.add(self.page1, text="步骤 1")
        self.notebook.add(self.page2, text="步骤 2")
        self.notebook.pack(fill="both", expand=True)

        self._build_step1()
        self._build_step2()

        bottom = ttk.Frame(self.root, padding=10)
        bottom.pack(fill="x")
        left = ttk.Frame(bottom)
        left.pack(side="left", fill="x", expand=True)

        ttk.Label(left, text="阶段:").grid(row=0, column=0, sticky="w")
        ttk.Label(left, textvariable=self.stage_var).grid(row=0, column=1, sticky="w", padx=(5, 15))
        ttk.Label(left, text="当前对象:").grid(row=0, column=2, sticky="w")
        ttk.Label(left, textvariable=self.current_var).grid(row=0, column=3, sticky="w", padx=(5, 15))
        ttk.Label(left, text="百分比:").grid(row=0, column=4, sticky="w")
        ttk.Label(left, textvariable=self.percent_var).grid(row=0, column=5, sticky="w", padx=(5, 15))

        ttk.Label(left, text="已识别电池数:").grid(row=1, column=0, sticky="w", pady=(5, 0))
        ttk.Label(left, textvariable=self.battery_count_var).grid(row=1, column=1, sticky="w", pady=(5, 0))
        ttk.Label(left, text="已识别文件数:").grid(row=1, column=2, sticky="w", pady=(5, 0))
        ttk.Label(left, textvariable=self.recognized_count_var).grid(row=1, column=3, sticky="w", pady=(5, 0))
        ttk.Label(left, text="已跳过目录数:").grid(row=1, column=4, sticky="w", pady=(5, 0))
        ttk.Label(left, textvariable=self.skipped_dir_var).grid(row=1, column=5, sticky="w", pady=(5, 0))
        ttk.Label(left, text="已跳过文件数:").grid(row=1, column=6, sticky="w", pady=(5, 0), padx=(10, 0))
        ttk.Label(left, textvariable=self.skipped_file_var).grid(row=1, column=7, sticky="w", pady=(5, 0))

    def _build_step1(self) -> None:
        actions = ttk.Frame(self.page1)
        actions.pack(fill="x")
        ttk.Button(actions, text="科斯特数据处理", command=self.choose_root).pack(side="left")
        self.start_scan_btn = ttk.Button(actions, text="开始扫描", command=self.start_scan, state="disabled")
        self.start_scan_btn.pack(side="left", padx=8)
        self.cancel_scan_btn = ttk.Button(actions, text="取消扫描", command=self.cancel_scan, state="disabled")
        self.cancel_scan_btn.pack(side="left")
        ttk.Button(actions, text="打开跳过清单", command=self.open_skipped_dir).pack(side="left", padx=8)

        path_frame = ttk.LabelFrame(self.page1, text="已选根目录", padding=8)
        path_frame.pack(fill="x", pady=(10, 0))
        ttk.Label(path_frame, textvariable=self.root_path_var).pack(anchor="w")

    def _build_step2(self) -> None:
        options = ttk.LabelFrame(self.page2, text="全局选项", padding=10)
        options.pack(fill="x")

        ttk.Label(options, text="输出类型").grid(row=0, column=0, sticky="w")
        ttk.Radiobutton(options, text="Csp（F/g）", variable=self.output_type_var, value="Csp（F/g）").grid(row=0, column=1, sticky="w")
        ttk.Radiobutton(options, text="Qsp（mAh/g）", variable=self.output_type_var, value="Qsp（mAh/g）").grid(row=0, column=2, sticky="w")

        ttk.Label(options, text="几何面积 A_geom（cm²）").grid(row=1, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(options, textvariable=self.a_geom_var, width=10).grid(row=1, column=1, sticky="w", pady=(8, 0))
        ttk.Checkbutton(options, text="是否输出电池级工作簿", variable=self.export_book_var).grid(row=2, column=0, columnspan=2, sticky="w", pady=(8, 0))
        ttk.Checkbutton(options, text="完成后打开输出文件夹", variable=self.open_folder_var).grid(row=3, column=0, columnspan=2, sticky="w")
        ttk.Button(options, text="打开运行报告", command=self.open_run_report_dir).grid(row=0, column=3, padx=(20, 0), sticky="e")
        ttk.Button(options, text="打开跳过清单", command=self.open_skipped_dir).grid(row=1, column=3, padx=(20, 0), sticky="e")

        center = ttk.LabelFrame(self.page2, text="扫描结果", padding=10)
        center.pack(fill="both", expand=True, pady=(10, 0))
        self.tree = ttk.Treeview(center, columns=("name", "cv", "gcd"), show="headings", height=8)
        self.tree.heading("name", text="电池名")
        self.tree.heading("cv", text="CV 最大圈数")
        self.tree.heading("gcd", text="GCD 最大圈数")
        self.tree.pack(fill="x")

        self.available_cv_var = tk.StringVar(value="CV: ")
        self.available_gcd_var = tk.StringVar(value="GCD: ")
        self.available_eis_var = tk.StringVar(value="EIS: ")
        ttk.Label(center, textvariable=self.available_cv_var).pack(anchor="w", pady=(8, 0))
        ttk.Label(center, textvariable=self.available_gcd_var).pack(anchor="w")
        ttk.Label(center, textvariable=self.available_eis_var).pack(anchor="w")

        btns = ttk.Frame(self.page2)
        btns.pack(fill="x", pady=(10, 0))
        ttk.Button(btns, text="返回上一步", command=lambda: self.notebook.select(self.page1)).pack(side="left")
        ttk.Button(btns, text="清空缓存", command=self.reset_step2_controls).pack(side="left", padx=8)
        ttk.Button(btns, text="确定导出", command=self.export_placeholder).pack(side="left")

    def _load_default_open_dir(self) -> None:
        last_root_file = self.ctx.paths.state_dir / "last_root.txt"
        self.default_open_dir = self.ctx.paths.program_dir
        if last_root_file.exists():
            txt = last_root_file.read_text(encoding="utf-8").strip()
            p = Path(txt)
            if p.exists():
                self.default_open_dir = p.parent

    def choose_root(self) -> None:
        chosen = filedialog.askdirectory(initialdir=str(self.default_open_dir))
        if not chosen:
            return
        self.selected_root = Path(chosen).resolve()
        self.root_path_var.set(str(self.selected_root))
        self.start_scan_btn.configure(state="normal")

    def start_scan(self) -> None:
        if not self.selected_root:
            return
        self.ctx.paths.state_dir.mkdir(parents=True, exist_ok=True)
        (self.ctx.paths.state_dir / "last_root.txt").write_text(str(self.selected_root), encoding="utf-8")
        self.cancel_event.clear()
        self.start_scan_btn.configure(state="disabled")
        self.cancel_scan_btn.configure(state="normal")

        self.scan_thread = threading.Thread(target=self._scan_worker, daemon=True)
        self.scan_thread.start()

    def cancel_scan(self) -> None:
        self.cancel_event.set()

    def _scan_worker(self) -> None:
        def progress(stage, current, percent, battery_count, recognized_file_count, skipped_dir_count, skipped_file_count):
            self.msg_q.put(("progress", (stage, current, percent, battery_count, recognized_file_count, skipped_dir_count, skipped_file_count)))

        result = scan_root(
            root_path=str(self.selected_root),
            program_dir=str(self.ctx.paths.program_dir),
            run_id=self.ctx.run_id,
            cancel_flag=self.cancel_event,
            progress_cb=progress,
        )
        self.msg_q.put(("done", result))

    def _poll_queue(self) -> None:
        try:
            while True:
                kind, payload = self.msg_q.get_nowait()
                if kind == "progress":
                    stage, current, percent, bc, rc, sd, sf = payload
                    self.stage_var.set(stage)
                    self.current_var.set(current)
                    self.percent_var.set(f"{percent:.1f}%")
                    self.battery_count_var.set(str(bc))
                    self.recognized_count_var.set(str(rc))
                    self.skipped_dir_var.set(str(sd))
                    self.skipped_file_var.set(str(sf))
                elif kind == "done":
                    self.scan_result = payload
                    self.fill_step2_result(payload)
                    self.cancel_scan_btn.configure(state="disabled")
                    self.start_scan_btn.configure(state="normal")
                    self.notebook.select(self.page2)
        except queue.Empty:
            pass
        self.root.after(100, self._poll_queue)

    def fill_step2_result(self, result: ScanResult) -> None:
        self.tree.delete(*self.tree.get_children())
        for b in result.batteries:
            cv = "-" if b.cv_max_cycle is None else str(b.cv_max_cycle)
            gcd = "-" if b.gcd_max_cycle is None else str(b.gcd_max_cycle)
            self.tree.insert("", "end", values=(b.name, cv, gcd))
        self.available_cv_var.set("CV: " + ", ".join(str(x) for x in result.available_cv))
        self.available_gcd_var.set("GCD: " + ", ".join(str(x) for x in result.available_gcd))
        self.available_eis_var.set("EIS: " + ", ".join(str(x) for x in result.available_eis))

    def open_skipped_dir(self) -> None:
        skipped = self.ctx.paths.reports_dir / f"skipped_paths-{self.ctx.run_id}.txt"
        target_dir = skipped.parent
        self._open_directory(target_dir, fallback_file=skipped)

    def open_run_report_dir(self) -> None:
        self._open_directory(self.ctx.report_path.parent, fallback_file=self.ctx.report_path)

    def _open_directory(self, dir_path: Path, fallback_file: Path) -> None:
        try:
            if os.name == "nt":
                os.startfile(str(dir_path))
            elif os.name == "posix":
                subprocess.Popen(["xdg-open", str(dir_path)])
            else:
                raise RuntimeError("unsupported platform")
        except Exception:
            messagebox.showinfo(WINDOW_TITLE, f"无法打开目录，请手动查看：\n{fallback_file}")

    def reset_step2_controls(self) -> None:
        self.output_type_var.set("Csp（F/g）")
        self.a_geom_var.set("1")
        self.export_book_var.set(True)
        self.open_folder_var.set(True)

    def export_placeholder(self) -> None:
        messagebox.showinfo(WINDOW_TITLE, "导出功能将在后续步骤实现")


def run_gui() -> int:
    try:
        ctx, logger = init_run_context()
        logger.info("mode", mode="GUI")
    except PermissionError as e:
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

    app_root = tk.Tk()
    App(app_root, ctx)
    app_root.mainloop()
    return 0

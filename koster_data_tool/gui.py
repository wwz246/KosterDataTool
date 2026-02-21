from __future__ import annotations

import json
import os
import queue
import subprocess
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

from .bootstrap import FATAL_NOT_WRITABLE_MESSAGE, init_run_context
from .export_pipeline import run_full_export
from .scanner import ScanResult, scan_root

WINDOW_TITLE = "科斯特工作站电化学数据处理"


class App:
    def __init__(self, root: tk.Tk, ctx, logger):
        self.root = root
        self.ctx = ctx
        self.logger = logger
        self.scan_result: ScanResult | None = None
        self.selected_root: Path | None = None
        self.cancel_event = threading.Event()
        self.scan_thread: threading.Thread | None = None
        self.export_thread: threading.Thread | None = None
        self.msg_q: queue.Queue = queue.Queue()
        self.last_scan_root: str | None = None

        self.stage_var = tk.StringVar(value="待机")
        self.current_var = tk.StringVar(value="-")
        self.percent_var = tk.StringVar(value="0.0")
        self.root_path_var = tk.StringVar(value="未选择")

        self.output_type_var = tk.StringVar(value="Csp")
        self.a_geom_var = tk.StringVar(value="1")
        self.export_book_var = tk.BooleanVar(value=True)
        self.open_folder_var = tk.BooleanVar(value=True)

        self._build_ui()
        self._load_default_open_dir()
        self._poll_queue()

    def _build_ui(self) -> None:
        self.root.title(WINDOW_TITLE)
        self.root.geometry("1180x760")

        top = ttk.Frame(self.root, padding=10)
        top.pack(fill="x")
        ttk.Label(top, text=WINDOW_TITLE, font=("Arial", 16, "bold")).pack(anchor="w")

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
        ttk.Label(bottom, text="阶段:").pack(side="left")
        ttk.Label(bottom, textvariable=self.stage_var).pack(side="left", padx=(4, 12))
        ttk.Label(bottom, text="当前对象:").pack(side="left")
        ttk.Label(bottom, textvariable=self.current_var).pack(side="left", padx=(4, 12))
        ttk.Label(bottom, text="百分比:").pack(side="left")
        ttk.Label(bottom, textvariable=self.percent_var).pack(side="left", padx=(4, 0))

    def _build_step1(self) -> None:
        actions = ttk.Frame(self.page1)
        actions.pack(fill="x")
        ttk.Button(actions, text="科斯特数据处理", command=self.choose_root).pack(side="left")
        self.start_scan_btn = ttk.Button(actions, text="开始扫描", command=self.start_scan, state="disabled")
        self.start_scan_btn.pack(side="left", padx=8)

        path_frame = ttk.LabelFrame(self.page1, text="已选根目录", padding=8)
        path_frame.pack(fill="x", pady=(10, 0))
        ttk.Label(path_frame, textvariable=self.root_path_var).pack(anchor="w")

    def _build_step2(self) -> None:
        options = ttk.LabelFrame(self.page2, text="全局选项", padding=10)
        options.pack(fill="x")
        ttk.Label(options, text="输出类型").grid(row=0, column=0, sticky="w")
        ttk.Radiobutton(options, text="Csp", variable=self.output_type_var, value="Csp", command=self._on_output_type_change).grid(row=0, column=1, sticky="w")
        ttk.Radiobutton(options, text="Qsp", variable=self.output_type_var, value="Qsp", command=self._on_output_type_change).grid(row=0, column=2, sticky="w")
        ttk.Label(options, text="A_geom").grid(row=1, column=0, sticky="w")
        ttk.Entry(options, textvariable=self.a_geom_var, width=10).grid(row=1, column=1, sticky="w")
        ttk.Checkbutton(options, text="是否输出电池级工作簿", variable=self.export_book_var).grid(row=2, column=0, columnspan=2, sticky="w")
        ttk.Checkbutton(options, text="完成后打开输出文件夹", variable=self.open_folder_var).grid(row=3, column=0, columnspan=2, sticky="w")
        ttk.Button(options, text="打开运行报告", command=self.open_run_report_dir).grid(row=0, column=3, padx=(20, 0), sticky="e")

        self.sub_notebook = ttk.Notebook(self.page2)
        self.param_page = ttk.Frame(self.sub_notebook, padding=8)
        self.filter_page = ttk.Frame(self.sub_notebook, padding=8)
        self.sub_notebook.add(self.param_page, text="参数表")
        self.sub_notebook.add(self.filter_page, text="极片级筛选")
        self.sub_notebook.pack(fill="both", expand=True, pady=(8, 0))

        self.param_tree = ttk.Treeview(
            self.param_page,
            columns=("name", "cvmax", "gcdmax", "m_pos", "m_neg", "p_active", "k", "n_cv", "n_gcd", "v_start", "v_end"),
            show="headings",
            height=10,
        )
        for c, t in [
            ("name", "电池名"), ("cvmax", "CV最大圈数"), ("gcdmax", "GCD最大圈数"), ("m_pos", "m_pos"), ("m_neg", "m_neg"),
            ("p_active", "p_active"), ("k", "K"), ("n_cv", "N_CV"), ("n_gcd", "N_GCD"), ("v_start", "V_start"), ("v_end", "V_end"),
        ]:
            self.param_tree.heading(c, text=t)
            self.param_tree.column(c, width=100, anchor="center")
        self.param_tree.pack(fill="both", expand=True)
        self.param_tree.bind("<Double-1>", self._edit_param_cell)

        sel_frame = ttk.Frame(self.filter_page)
        sel_frame.pack(fill="both", expand=True)
        self.bat_list = tk.Listbox(sel_frame, selectmode="extended", exportselection=False)
        self.cv_list = tk.Listbox(sel_frame, selectmode="extended", exportselection=False)
        self.gcd_list = tk.Listbox(sel_frame, selectmode="extended", exportselection=False)
        self.eis_list = tk.Listbox(sel_frame, selectmode="extended", exportselection=False)
        for i, (w, title) in enumerate([(self.bat_list, "电池"), (self.cv_list, "CV"), (self.gcd_list, "GCD"), (self.eis_list, "EIS")]):
            col = ttk.Frame(sel_frame)
            col.grid(row=0, column=i, sticky="nsew", padx=8)
            ttk.Label(col, text=title).pack(anchor="w")
            w.pack(fill="both", expand=True)
            sel_frame.columnconfigure(i, weight=1)

        btns = ttk.Frame(self.page2)
        btns.pack(fill="x", pady=(10, 0))
        ttk.Button(btns, text="返回上一步", command=self._back_to_step1).pack(side="left")
        ttk.Button(btns, text="清空缓存", command=self._clear_cache).pack(side="left", padx=8)
        ttk.Button(btns, text="确定导出", command=self._confirm_export).pack(side="left")

    def _on_output_type_change(self):
        # Qsp 下隐藏 K（在参数收集时固定为1）
        pass

    def _load_default_open_dir(self) -> None:
        self.default_open_dir = self.ctx.paths.program_dir

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
        self.cancel_event.clear()
        self.scan_thread = threading.Thread(target=self._scan_worker, daemon=True)
        self.scan_thread.start()

    def _scan_worker(self) -> None:
        result = scan_root(str(self.selected_root), str(self.ctx.paths.program_dir), self.ctx.run_id, self.cancel_event, None)
        self.msg_q.put(("done", result))

    def _poll_queue(self) -> None:
        try:
            while True:
                kind, payload = self.msg_q.get_nowait()
                if kind == "done":
                    self.scan_result = payload
                    self._fill_step2(payload)
                    self.notebook.select(self.page2)
                elif kind == "progress":
                    stage, current, percent = payload
                    self.stage_var.set(stage)
                    self.current_var.set(current)
                    self.percent_var.set(f"{percent:.1f}%")
                elif kind == "export_done":
                    self._on_export_done(payload)
        except queue.Empty:
            pass
        self.root.after(120, self._poll_queue)

    def _fill_step2(self, result: ScanResult):
        self.param_tree.delete(*self.param_tree.get_children())
        for b in sorted(result.batteries, key=lambda x: x.name):
            self.param_tree.insert("", "end", values=(b.name, b.cv_max_cycle or 1, b.gcd_max_cycle or 1, 10, 0, 90, 1, 1, 1, 2.5, 4.2))
        self._init_filter_lists(result)
        self._load_cache_or_keep()

    def _init_filter_lists(self, result: ScanResult):
        for lb in (self.bat_list, self.cv_list, self.gcd_list, self.eis_list):
            lb.delete(0, "end")
        for b in sorted(result.batteries, key=lambda x: x.name):
            self.bat_list.insert("end", b.name)
        for v in result.available_cv:
            self.cv_list.insert("end", str(v))
        for v in result.available_gcd:
            self.gcd_list.insert("end", str(v))
        for v in result.available_eis:
            self.eis_list.insert("end", str(v))
        self.bat_list.select_set(0, "end")
        if self.cv_list.size() > 0:
            self.cv_list.select_set(0)
        if self.gcd_list.size() > 0:
            self.gcd_list.select_set(0)
        if self.eis_list.size() > 0:
            self.eis_list.select_set(self.eis_list.size() - 1)

    def _edit_param_cell(self, event):
        iid = self.param_tree.identify_row(event.y)
        col = self.param_tree.identify_column(event.x)
        if not iid or not col:
            return
        ci = int(col[1:]) - 1
        if ci <= 2:
            return
        x, y, w, h = self.param_tree.bbox(iid, col)
        old_vals = list(self.param_tree.item(iid, "values"))
        editor = ttk.Entry(self.param_tree)
        editor.place(x=x, y=y, width=w, height=h)
        editor.insert(0, old_vals[ci])
        editor.focus_set()

        def save(_e=None):
            old_vals[ci] = editor.get()
            self.param_tree.item(iid, values=old_vals)
            editor.destroy()

        editor.bind("<Return>", save)
        editor.bind("<FocusOut>", save)

    def _collect_params(self):
        out = {"a_geom": float(self.a_geom_var.get()), "output_type": self.output_type_var.get(), "export_battery_workbook": bool(self.export_book_var.get()), "battery_params": {}}
        first_error = None
        for iid in self.param_tree.get_children():
            vals = list(self.param_tree.item(iid, "values"))
            try:
                bp = {
                    "m_pos": float(vals[3]), "m_neg": float(vals[4]), "p_active": float(vals[5]),
                    "k": float(vals[6]) if self.output_type_var.get() == "Csp" else 1.0,
                    "n_cv": int(vals[7]), "n_gcd": int(vals[8]), "v_start": float(vals[9]), "v_end": float(vals[10]),
                }
                bp["main_order"] = "先充后放"
                out["battery_params"][str(vals[0])] = bp
            except Exception:
                first_error = str(vals[0])
                break
        if first_error:
            raise ValueError(f"参数非法：{first_error}")
        return out

    def _collect_selections(self):
        return {
            "batteries": [self.bat_list.get(i) for i in self.bat_list.curselection()],
            "cv_nums": [self.cv_list.get(i) for i in self.cv_list.curselection()],
            "gcd_nums": [self.gcd_list.get(i) for i in self.gcd_list.curselection()],
            "eis_nums": [self.eis_list.get(i) for i in self.eis_list.curselection()],
        }

    def _cache_path(self):
        return self.ctx.paths.cache_dir / "ui_cache.json"

    def _load_cache_or_keep(self):
        cp = self._cache_path()
        if not cp.exists() or not self.selected_root:
            return
        obj = json.loads(cp.read_text(encoding="utf-8"))
        cur = obj.get(str(self.selected_root))
        if not cur:
            return
        vals = cur.get("rows", [])
        for iid, row in zip(self.param_tree.get_children(), vals):
            self.param_tree.item(iid, values=row)

    def _save_cache(self):
        if not self.selected_root:
            return
        cp = self._cache_path()
        cp.parent.mkdir(parents=True, exist_ok=True)
        obj = {}
        if cp.exists():
            obj = json.loads(cp.read_text(encoding="utf-8"))
        rows = [list(self.param_tree.item(iid, "values")) for iid in self.param_tree.get_children()]
        obj[str(self.selected_root)] = {"rows": rows}
        cp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

    def _clear_cache(self):
        cp = self._cache_path()
        if cp.exists():
            cp.unlink()
        messagebox.showinfo(WINDOW_TITLE, "缓存已清空")

    def _back_to_step1(self):
        self._save_cache()
        self.notebook.select(self.page1)

    def _confirm_export(self):
        if not self.scan_result or not self.selected_root:
            return
        try:
            params = self._collect_params()
        except Exception as e:
            messagebox.showerror(WINDOW_TITLE, str(e))
            return
        sels = self._collect_selections()

        def progress(stage, current, percent):
            self.msg_q.put(("progress", (stage, current, percent)))

        def worker():
            try:
                result = run_full_export(str(self.selected_root), self.scan_result, params, sels, self.ctx, self.logger, progress)
                self.msg_q.put(("export_done", result))
            except Exception as e:
                self.msg_q.put(("export_done", {"error": str(e)}))

        self.export_thread = threading.Thread(target=worker, daemon=True)
        self.export_thread.start()

    def _on_export_done(self, result: dict):
        if "error" in result:
            messagebox.showerror(WINDOW_TITLE, result["error"])
            return
        lines = [
            f"根目录: {self.selected_root}",
            f"极片级: {Path(result['electrode_path']).name}",
            f"电池级: {Path(result['battery_path']).name if result['battery_path'] else '(disabled)'}",
            f"运行报告: {result['run_report_path']}",
            f"日志: {result['log_path']}",
            "失败/告警(前50):",
        ]
        for x in (result.get("failures", []) + result.get("warnings", []))[:50]:
            lines.append(f"- {x}")
        messagebox.showinfo(WINDOW_TITLE, "\n".join(lines))
        if self.open_folder_var.get() and self.selected_root:
            self._open_directory(self.selected_root, Path(result["electrode_path"]))

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

    def open_run_report_dir(self) -> None:
        self._open_directory(self.ctx.report_path.parent, self.ctx.report_path)


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
    App(app_root, ctx, logger)
    app_root.mainloop()
    return 0

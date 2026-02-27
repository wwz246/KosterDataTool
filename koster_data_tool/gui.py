from __future__ import annotations

import json
import os
import queue
import re
import subprocess
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

from .bootstrap import FATAL_NOT_WRITABLE_MESSAGE, init_run_context
from .canvas_table import CanvasTable
from .export_pipeline import run_full_export
from .param_visibility import get_visible_param_columns, get_visible_param_fields
from .param_validation import coerce_int_strict, validate_battery_row, validate_global
from .renamer import run_rename
from .scanner import ScanResult, scan_root
from .state_store import resolve_initial_dir_from_last_root, write_last_root

WINDOW_TITLE = "电化学数据处理"


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
        self.default_row_values = {"m_pos": 10, "m_neg": 0, "p_active": 90, "k": 1, "n_cv": 1, "n_gcd": 1, "v_start": 2.5, "v_end": 4.2}

        self.stage_var = tk.StringVar(value="待机")
        self.current_var = tk.StringVar(value="-")
        self.percent_var = tk.StringVar(value="0.0")
        self.battery_count_var = tk.StringVar(value="0")
        self.recognized_file_count_var = tk.StringVar(value="0")
        self.skipped_dir_count_var = tk.StringVar(value="0")
        self.skipped_file_count_var = tk.StringVar(value="0")
        self.progress_value_var = tk.DoubleVar(value=0.0)
        self.root_path_var = tk.StringVar(value="未选择")

        self.output_type_var = tk.StringVar(value="Csp")
        self.electrode_rate_csp_col_var = tk.StringVar(value="csp_noir")
        self.a_geom_var = tk.StringVar(value="1")
        self.export_book_var = tk.BooleanVar(value=True)
        self.open_folder_var = tk.BooleanVar(value=True)
        self.cv_current_unit_var = tk.StringVar(value="A/g")
        self._blink_job = None
        self._blink_on = False
        self.param_columns = ["name", "cvmax", "gcdmax", "m_pos", "m_neg", "p_active", "k", "n_cv", "n_gcd", "v_start", "v_end"]
        self.editable_column_keys = ["m_pos", "m_neg", "p_active", "k", "n_cv", "n_gcd", "v_start", "v_end"]
        self.readonly_column_keys = {"name", "cvmax", "gcdmax"}
        self.param_table: CanvasTable | None = None
        self.file_type_presence = {"cv": False, "gcd": False, "eis": False}
        self.filter_tab_visible = True
        self._final_stage_seen = False
        self._pending_export_result: dict | None = None

        self._build_ui()
        self._load_default_open_dir()
        self._poll_queue()

    def _build_ui(self) -> None:
        self.root.title(WINDOW_TITLE)
        self.root.geometry("1180x760")
        self.root.minsize(1080, 680)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

        self.main_frame = ttk.Frame(self.root)
        self.main_frame.pack(fill="both", expand=True)

        top = ttk.Frame(self.main_frame, padding=10)
        top.pack(fill="x")
        ttk.Label(top, text=WINDOW_TITLE, font=("Arial", 16, "bold")).pack(anchor="w")

        self.page_stack = ttk.Frame(self.main_frame)
        self.page_stack.pack(fill="both", expand=True)
        self.page1 = ttk.Frame(self.page_stack, padding=10)
        self.page2 = ttk.Frame(self.page_stack, padding=10)

        self._build_step1()
        self._build_step2()
        self._show_step(1)

        self.bottom_bar = ttk.Frame(self.root, padding=10)
        self.bottom_bar.pack(side="bottom", fill="x")

        bottom = self.bottom_bar
        ttk.Label(bottom, text="阶段:").pack(side="left")
        ttk.Label(bottom, textvariable=self.stage_var).pack(side="left", padx=(4, 12))
        ttk.Label(bottom, text="当前对象:").pack(side="left")
        ttk.Label(bottom, textvariable=self.current_var).pack(side="left", padx=(4, 12))
        ttk.Label(bottom, text="百分比:").pack(side="left")
        ttk.Label(bottom, textvariable=self.percent_var).pack(side="left", padx=(4, 0))
        ttk.Label(bottom, text=" 电池数:").pack(side="left", padx=(12, 0))
        ttk.Label(bottom, textvariable=self.battery_count_var).pack(side="left")
        ttk.Label(bottom, text=" 识别文件数:").pack(side="left", padx=(12, 0))
        ttk.Label(bottom, textvariable=self.recognized_file_count_var).pack(side="left")
        ttk.Label(bottom, text=" 跳过目录数:").pack(side="left", padx=(12, 0))
        ttk.Label(bottom, textvariable=self.skipped_dir_count_var).pack(side="left")
        ttk.Label(bottom, text=" 跳过文件数:").pack(side="left", padx=(12, 0))
        ttk.Label(bottom, textvariable=self.skipped_file_count_var).pack(side="left")
        ttk.Progressbar(bottom, orient="horizontal", mode="determinate", maximum=100, variable=self.progress_value_var, length=220).pack(side="left", padx=(12, 0))

    def _on_close(self) -> None:
        self.root.destroy()

    def _build_step1(self) -> None:
        actions = ttk.Frame(self.page1)
        actions.pack(fill="x")
        ttk.Button(actions, text="科斯特数据处理", command=self.choose_root).pack(side="left")
        self.start_scan_btn = ttk.Button(actions, text="开始扫描", command=self.start_scan, state="disabled")
        self.start_scan_btn.pack(side="left", padx=8)
        self.cancel_scan_btn = ttk.Button(actions, text="取消扫描", command=self.cancel_scan, state="disabled")
        self.cancel_scan_btn.pack(side="left")
        ttk.Button(actions, text="打开跳过清单", command=self.open_skipped_list_dir).pack(side="left", padx=8)
        ttk.Button(actions, text="科斯特重命名", command=self.run_koster_rename).pack(side="right", padx=(24, 0))

        path_frame = ttk.LabelFrame(self.page1, text="科斯特已选根目录", padding=8)
        path_frame.pack(fill="x", pady=(10, 0))
        ttk.Label(path_frame, textvariable=self.root_path_var).pack(anchor="w")

    def _build_step2(self) -> None:
        options = ttk.LabelFrame(self.page2, text="全局选项", padding=10)
        options.pack(fill="x")
        ttk.Label(self.page2, text="提示：R_turn 不是 RΩ，也不是 EIS 的 Rs；它是换向点表观ESR(DC)，用于工程对比。", foreground="#8a4b00").pack(anchor="w", pady=(6, 6))
        self.output_type_label = ttk.Label(options, text="输出类型")
        self.output_type_label.grid(row=0, column=0, sticky="w")
        self.output_type_rb_csp = ttk.Radiobutton(options, text="Csp", variable=self.output_type_var, value="Csp", command=self._on_output_type_change)
        self.output_type_rb_csp.grid(row=0, column=1, sticky="w")
        self.output_type_rb_qsp = ttk.Radiobutton(options, text="Qsp", variable=self.output_type_var, value="Qsp", command=self._on_output_type_change)
        self.output_type_rb_qsp.grid(row=0, column=2, sticky="w")
        self.electrode_rate_col_frame = ttk.Frame(options)
        self.electrode_rate_col_frame.grid(row=0, column=4, padx=(16, 0), sticky="w")
        ttk.Label(self.electrode_rate_col_frame, text="极片级 Rate(Csp) 输出列").pack(side="left")
        ttk.Radiobutton(self.electrode_rate_col_frame, text="比电容扣电压", variable=self.electrode_rate_csp_col_var, value="csp_eff").pack(side="left", padx=(6, 0))
        ttk.Radiobutton(self.electrode_rate_col_frame, text="比电容不扣电压", variable=self.electrode_rate_csp_col_var, value="csp_noir").pack(side="left", padx=(6, 0))
        self.a_geom_label = ttk.Label(options, text="A_geom")
        self.a_geom_label.grid(row=1, column=0, sticky="w")
        self.a_geom_entry = ttk.Entry(options, textvariable=self.a_geom_var, width=10)
        self.a_geom_entry.grid(row=1, column=1, sticky="w")
        self.cv_unit_label = ttk.Label(options, text="CV电流单位")
        self.cv_unit_label.grid(row=2, column=0, sticky="w", pady=(6, 0))
        self.cv_unit_rb_ag = ttk.Radiobutton(options, text="A/g", variable=self.cv_current_unit_var, value="A/g", command=self._on_cv_unit_change)
        self.cv_unit_rb_ag.grid(row=2, column=1, sticky="w", pady=(6, 0))
        self.cv_unit_rb_a = ttk.Radiobutton(options, text="A", variable=self.cv_current_unit_var, value="A", command=self._on_cv_unit_change)
        self.cv_unit_rb_a.grid(row=2, column=2, sticky="w", pady=(6, 0))
        self.cv_unit_rb_ma = ttk.Radiobutton(options, text="mA", variable=self.cv_current_unit_var, value="mA", command=self._on_cv_unit_change)
        self.cv_unit_rb_ma.grid(row=2, column=3, sticky="w", pady=(6, 0))
        ttk.Checkbutton(options, text="是否输出电池级工作簿", variable=self.export_book_var).grid(row=3, column=0, columnspan=2, sticky="w")
        ttk.Checkbutton(options, text="完成后打开输出文件夹", variable=self.open_folder_var).grid(row=4, column=0, columnspan=2, sticky="w")
        ttk.Button(options, text="打开运行报告", command=self.open_run_report_dir).grid(row=0, column=3, padx=(20, 0), sticky="e")
        ttk.Button(options, text="打开跳过清单", command=self.open_skipped_list_dir).grid(row=1, column=3, padx=(20, 0), sticky="e")

        self.sub_notebook = ttk.Notebook(self.page2)
        self.param_page = ttk.Frame(self.sub_notebook, padding=8)
        self.filter_page = ttk.Frame(self.sub_notebook, padding=8)
        self.sub_notebook.add(self.param_page, text="参数表")
        self.sub_notebook.add(self.filter_page, text="极片级筛选")
        self.sub_notebook.pack(fill="both", expand=True, pady=(8, 0))

        fill_frame = ttk.Frame(self.param_page)
        fill_frame.pack(fill="x", pady=(0, 6))
        ttk.Label(fill_frame, text="批量输入").pack(side="left")
        self.param_fill_var = tk.StringVar(value="")
        ttk.Entry(fill_frame, textvariable=self.param_fill_var, width=22).pack(side="left", padx=6)
        ttk.Button(fill_frame, text="单值填充", command=self._fill_single_value).pack(side="left", padx=(0, 6))
        ttk.Button(fill_frame, text="多值粘贴", command=self._paste_multi_value).pack(side="left")
        ttk.Label(
            self.param_page,
            text="质量口径提示：半电池其中一侧质量请填 0；质量均为不含集流体的活性层质量。",
            foreground="#444444",
        ).pack(anchor="w", pady=(0, 4))
        ttk.Label(
            self.param_page,
            text="K 为换算系数：K=1 按整体器件直接计算；K=2 按两电极/单侧口径进行一次换算；K=4（推荐默认）按对称器件换算到单电极材料口径。不确定时先填 4。",
            foreground="#1f4e79",
        ).pack(anchor="w", pady=(0, 6))

        table_wrap = ttk.Frame(self.param_page)
        table_wrap.pack(fill="both", expand=True)

        self.param_table = CanvasTable(
            table_wrap,
            columns=self._build_table_columns(),
            rows=[],
            readonly_cols=self.readonly_column_keys,
        )
        self.param_table.pack(fill="both", expand=True)

        sel_frame = ttk.Frame(self.filter_page)
        sel_frame.pack(fill="both", expand=True)
        self.bat_list = None
        self.cv_list = None
        self.gcd_list = None
        self.eis_list = None
        for i, (attr_name, title) in enumerate(
            [("bat_list", "电池"), ("cv_list", "CV"), ("gcd_list", "GCD"), ("eis_list", "EIS")]
        ):
            col = ttk.Frame(sel_frame)
            col.grid(row=0, column=i, sticky="nsew", padx=8)
            ttk.Label(col, text=title).pack(anchor="w")
            listbox = tk.Listbox(col, selectmode="extended", exportselection=False)
            ys = ttk.Scrollbar(col, orient="vertical", command=listbox.yview)
            listbox.configure(yscrollcommand=ys.set)
            listbox.pack(side="left", fill="both", expand=True)
            ys.pack(side="right", fill="y")
            setattr(self, attr_name, listbox)
            sel_frame.columnconfigure(i, weight=1)

        btns = ttk.Frame(self.page2)
        btns.pack(fill="x", pady=(10, 0))
        ttk.Button(btns, text="返回上一步", command=self._back_to_step1).pack(side="left")
        ttk.Button(btns, text="清空缓存", command=self._clear_cache).pack(side="left", padx=8)
        ttk.Button(btns, text="确定导出", command=self._confirm_export).pack(side="left")

    def _visible_param_fields(self) -> list[str]:
        return get_visible_param_fields(
            self.file_type_presence,
            self.output_type_var.get(),
            self.cv_current_unit_var.get(),
        )

    def _build_table_columns(self) -> list[dict]:
        return get_visible_param_columns(
            self.file_type_presence,
            self.output_type_var.get(),
            self.cv_current_unit_var.get(),
        )

    def _refresh_option_visibility(self) -> None:
        has_cv = self.file_type_presence.get("cv", False)
        has_gcd = self.file_type_presence.get("gcd", False)

        if has_gcd:
            self.output_type_label.grid()
            self.output_type_rb_csp.grid()
            self.output_type_rb_qsp.grid()
        else:
            self.output_type_label.grid_remove()
            self.output_type_rb_csp.grid_remove()
            self.output_type_rb_qsp.grid_remove()
            self.output_type_var.set("Csp")

        if has_gcd and self.output_type_var.get() == "Csp":
            if self.electrode_rate_csp_col_var.get() not in {"csp_noir", "csp_eff"}:
                self.electrode_rate_csp_col_var.set("csp_noir")
            self.electrode_rate_col_frame.grid()
        else:
            self.electrode_rate_col_frame.grid_remove()

        if has_cv:
            self.cv_unit_label.grid()
            self.cv_unit_rb_ag.grid()
            self.cv_unit_rb_a.grid()
            self.cv_unit_rb_ma.grid()
        else:
            self.cv_unit_label.grid_remove()
            self.cv_unit_rb_ag.grid_remove()
            self.cv_unit_rb_a.grid_remove()
            self.cv_unit_rb_ma.grid_remove()
            self.cv_current_unit_var.set("A/g")

        if has_cv or has_gcd:
            self.a_geom_label.grid()
            self.a_geom_entry.grid()
        else:
            self.a_geom_label.grid_remove()
            self.a_geom_entry.grid_remove()

    def _show_step(self, step: int) -> None:
        self.page1.pack_forget()
        self.page2.pack_forget()
        if step == 1:
            self.page1.pack(fill="both", expand=True)
        else:
            self.page2.pack(fill="both", expand=True)

    def _on_output_type_change(self):
        if self.param_table is None:
            return
        if self.output_type_var.get() == "Qsp":
            for row in self.param_table.rows:
                row["k"] = 1
        self._refresh_option_visibility()
        self.param_table.set_columns(self._build_table_columns())
        self._refresh_error_states()

    def _on_cv_unit_change(self):
        if self.param_table is None:
            return
        self._refresh_option_visibility()
        self.param_table.set_columns(self._build_table_columns())
        self._refresh_error_states()

    def _load_default_open_dir(self) -> None:
        self.default_open_dir = resolve_initial_dir_from_last_root(
            self.ctx.paths.program_dir,
            self.ctx.paths.program_dir,
        )

    def choose_root(self) -> None:
        chosen = filedialog.askdirectory(initialdir=str(self.default_open_dir))
        if not chosen:
            return
        new_root = Path(chosen).resolve()
        if self.selected_root is not None and new_root != self.selected_root:
            self._drop_cache_for_root(str(self.selected_root))
        self.selected_root = new_root
        self.root_path_var.set(str(self.selected_root))
        self.start_scan_btn.configure(state="normal")
        write_last_root(self.ctx.paths.program_dir, self.selected_root)
        self.default_open_dir = resolve_initial_dir_from_last_root(self.ctx.paths.program_dir, self.ctx.paths.program_dir)

    def run_koster_rename(self) -> None:
        chosen = filedialog.askdirectory(initialdir=str(self.default_open_dir))
        if not chosen:
            return
        selected_dir = Path(chosen).resolve()
        write_last_root(self.ctx.paths.program_dir, selected_dir)
        self.default_open_dir = resolve_initial_dir_from_last_root(self.ctx.paths.program_dir, self.ctx.paths.program_dir)
        summary_text, has_conflicts = run_rename(
            selected_dir,
            logger=lambda m: self.logger.info("koster_rename", message=m),
        )
        if has_conflicts:
            self._show_rename_log(summary_text)
            return
        messagebox.showinfo("科斯特重命名", summary_text)

    def _show_rename_log(self, content: str) -> None:
        win = tk.Toplevel(self.root)
        win.title("科斯特重命名日志")
        win.geometry("960x520")

        frame = ttk.Frame(win, padding=10)
        frame.pack(fill="both", expand=True)
        txt = tk.Text(frame, wrap="word")
        ybar = ttk.Scrollbar(frame, orient="vertical", command=txt.yview)
        txt.configure(yscrollcommand=ybar.set)
        txt.pack(side="left", fill="both", expand=True)
        ybar.pack(side="right", fill="y")
        txt.insert("1.0", content)

        btns = ttk.Frame(win, padding=(10, 0, 10, 10))
        btns.pack(fill="x")

        def copy_to_clipboard() -> None:
            self.root.clipboard_clear()
            self.root.clipboard_append(content)

        ttk.Button(btns, text="复制到剪贴板", command=copy_to_clipboard).pack(side="left")
        ttk.Button(btns, text="关闭", command=win.destroy).pack(side="right")

    def start_scan(self) -> None:
        if not self.selected_root:
            return
        self._reset_scan_result_state()
        self.cancel_event.clear()
        self.start_scan_btn.configure(state="disabled")
        self.cancel_scan_btn.configure(state="normal")
        self.scan_thread = threading.Thread(target=self._scan_worker, daemon=True)
        self.scan_thread.start()

    def cancel_scan(self) -> None:
        self.cancel_event.set()
        self.cancel_scan_btn.configure(state="disabled")

    def _scan_worker(self) -> None:
        def progress_cb(stage: str, current: str, percent: float, bcnt: int, rcnt: int, sdcnt: int, sfcnt: int) -> None:
            self.msg_q.put(("scan_progress", (stage, current, percent, bcnt, rcnt, sdcnt, sfcnt)))

        result = scan_root(str(self.selected_root), str(self.ctx.paths.program_dir), self.ctx.run_id, self.cancel_event, progress_cb)
        self.msg_q.put(("done", result))

    def _reset_scan_result_state(self) -> None:
        self.scan_result = None
        if self.param_table is not None:
            self.param_table.set_rows([])
        for lb in (self.bat_list, self.cv_list, self.gcd_list, self.eis_list):
            lb.delete(0, "end")
        self.stage_var.set("待机")
        self.current_var.set("-")
        self.percent_var.set("0.0%")
        self.battery_count_var.set("0")
        self.recognized_file_count_var.set("0")
        self.skipped_dir_count_var.set("0")
        self.skipped_file_count_var.set("0")
        self.progress_value_var.set(0.0)

    def _poll_queue(self) -> None:
        try:
            while True:
                kind, payload = self.msg_q.get_nowait()
                if kind == "done":
                    self.scan_result = payload
                    self._log_recognized_files(payload)
                    self._fill_step2(payload)
                    self._show_step(2)
                    self.start_scan_btn.configure(state="normal")
                    self.cancel_scan_btn.configure(state="disabled")
                elif kind == "scan_progress":
                    stage, current, percent, bcnt, rcnt, sdcnt, sfcnt = payload
                    self.stage_var.set(stage)
                    self.current_var.set(current)
                    self.percent_var.set(f"{percent:.1f}%")
                    self.battery_count_var.set(str(bcnt))
                    self.recognized_file_count_var.set(str(rcnt))
                    self.skipped_dir_count_var.set(str(sdcnt))
                    self.skipped_file_count_var.set(str(sfcnt))
                    self.progress_value_var.set(percent)
                elif kind == "progress":
                    stage, current, percent = payload
                    self.stage_var.set(stage)
                    self.current_var.set(current)
                    self.percent_var.set(f"{percent:.1f}%")
                    self.progress_value_var.set(percent)
                    if stage == "结束弹窗（失败/告警清单）" and abs(percent - 100.0) < 1e-9:
                        self._final_stage_seen = True
                        if self._pending_export_result is not None:
                            self._on_export_done(self._pending_export_result)
                            self._pending_export_result = None
                elif kind == "export_done":
                    if self._final_stage_seen:
                        self._on_export_done(payload)
                    else:
                        self._pending_export_result = payload
        except queue.Empty:
            pass
        self.root.after(120, self._poll_queue)

    def _log_recognized_files(self, result: ScanResult) -> None:
        for ignored_dir in result.ignored_invalid_dirs:
            self.logger.info("目录已忽略：无有效电化学数据", dir_path=ignored_dir)
        for battery in result.batteries:
            for file_type, files in (("CV", battery.cv_files), ("GCD", battery.gcd_files), ("EIS", battery.eis_files)):
                for rf in files:
                    self.logger.info(
                        "recognized file",
                        battery=battery.name,
                        file_type=file_type,
                        num=rf.num,
                        path=str(Path(rf.path).resolve()),
                    )

    def _fill_step2(self, result: ScanResult):
        self.file_type_presence = {
            "cv": bool(result.available_cv),
            "gcd": bool(result.available_gcd),
            "eis": bool(result.available_eis),
        }
        rows = []
        for b in sorted(result.batteries, key=lambda x: x.name):
            rows.append(
                {
                    "name": b.name,
                    "cvmax": b.cv_max_cycle if b.cv_max_cycle is not None else "-",
                    "gcdmax": b.gcd_max_cycle if b.gcd_max_cycle is not None else "-",
                    "m_pos": self.default_row_values["m_pos"],
                    "m_neg": self.default_row_values["m_neg"],
                    "p_active": self.default_row_values["p_active"],
                    "k": self.default_row_values["k"],
                    "n_cv": self.default_row_values["n_cv"],
                    "n_gcd": self.default_row_values["n_gcd"],
                    "v_start": self.default_row_values["v_start"],
                    "v_end": self.default_row_values["v_end"],
                }
            )
        self._refresh_option_visibility()
        if self.param_table is not None:
            self.param_table.set_rows(rows)
            self.param_table.set_columns(self._build_table_columns())
        only_eis = self.file_type_presence["eis"] and not self.file_type_presence["cv"] and not self.file_type_presence["gcd"]
        tab_ids = set(self.sub_notebook.tabs())
        page_id = str(self.param_page)
        if only_eis and page_id in tab_ids:
            self.sub_notebook.forget(self.param_page)
        elif not only_eis and page_id not in tab_ids:
            self.sub_notebook.insert(0, self.param_page, text="参数表")
        self._init_filter_lists(result)
        self._load_cache_or_keep()
        self._on_output_type_change()
        self._refresh_error_states()

    def _init_filter_lists(self, result: ScanResult):
        show_filter = result.structure == "B" and len(result.batteries) > 1
        if show_filter and not self.filter_tab_visible:
            self.sub_notebook.add(self.filter_page, text="极片级筛选")
            self.filter_tab_visible = True
        elif not show_filter and self.filter_tab_visible:
            self.sub_notebook.forget(self.filter_page)
            self.filter_tab_visible = False
        for lb in (self.bat_list, self.cv_list, self.gcd_list, self.eis_list):
            lb.delete(0, "end")
        for b in sorted(result.batteries, key=lambda x: x.name):
            self.bat_list.insert("end", b.name)
        for v in sorted(result.available_cv):
            self.cv_list.insert("end", str(v))
        for v in sorted(result.available_gcd):
            self.gcd_list.insert("end", str(v))
        for v in sorted(result.available_eis):
            self.eis_list.insert("end", str(v))
        self.bat_list.select_set(0, "end")
        if self.cv_list.size() > 0:
            self.cv_list.select_set(0)
        if self.gcd_list.size() > 0:
            self.gcd_list.select_set(0)
        if self.eis_list.size() > 0:
            self.eis_list.select_set(self.eis_list.size() - 1)

    def _parse_matrix(self, text: str) -> list[list[str]]:
        lines = [ln for ln in re.split(r"\r?\n", text) if ln != ""]
        return [ln.split("\t") for ln in lines]

    def _fill_single_value(self):
        if self.param_table is None:
            return
        self.param_table.fill_selection(self.param_fill_var.get())
        self._refresh_error_states()

    def _paste_multi_value(self, _event=None):
        if self.param_table is None:
            return "break"
        try:
            text = self.root.clipboard_get().strip()
        except Exception:
            return "break"
        matrix = self._parse_matrix(text)
        self.param_table.paste_matrix(matrix)
        self._refresh_error_states()
        return "break"

    def _refresh_error_states(self):
        if self.param_table is None:
            return
        self.param_table.invalid_cells.clear()
        for row_idx, row in enumerate(self.param_table.rows):
            cv_max = coerce_int_strict(str(row.get("cvmax", "")))
            gcd_max = coerce_int_strict(str(row.get("gcdmax", "")))
            row_errors = validate_battery_row(
                output_type=self.output_type_var.get(),
                has_cv=self.file_type_presence.get("cv", False),
                has_gcd=self.file_type_presence.get("gcd", False),
                cv_current_unit=self.cv_current_unit_var.get(),
                m_pos=row.get("m_pos", ""),
                m_neg=row.get("m_neg", ""),
                p_active=row.get("p_active", ""),
                k=row.get("k", ""),
                n_cv=row.get("n_cv", ""),
                n_gcd=row.get("n_gcd", ""),
                v_start=row.get("v_start", ""),
                v_end=row.get("v_end", ""),
                cv_max=cv_max,
                gcd_max=gcd_max,
            )
            col_map = {"m_pos": "m_pos", "m_neg": "m_neg", "p_active": "p_active", "k": "k", "n_cv": "n_cv", "n_gcd": "n_gcd", "v_start": "v_start", "v_end": "v_end"}
            for key, msgs in row_errors.items():
                if key in col_map:
                    self.param_table.set_invalid((row_idx, col_map[key]), "；".join(msgs))
        self.param_table.redraw()

    def _validate_all_rows(self):
        first_row = 0 if self.param_table and self.param_table.rows else None
        try:
            a_geom = float(self.a_geom_var.get())
        except Exception:
            return {"global": ["A_geom 必须 > 0"]}, ((first_row, "m_pos") if first_row is not None else None)
        output_type = self.output_type_var.get()
        global_errors = validate_global(output_type=output_type, a_geom=a_geom)
        if global_errors:
            return {"global": global_errors}, ((first_row, "m_pos") if first_row is not None else None)

        if self.param_table is None:
            return {}, None
        for row_idx, row in enumerate(self.param_table.rows):
            cv_max = coerce_int_strict(str(row.get("cvmax", "")))
            gcd_max = coerce_int_strict(str(row.get("gcdmax", "")))
            row_errors = validate_battery_row(
                output_type=output_type,
                has_cv=self.file_type_presence.get("cv", False),
                has_gcd=self.file_type_presence.get("gcd", False),
                cv_current_unit=self.cv_current_unit_var.get(),
                m_pos=row.get("m_pos", ""),
                m_neg=row.get("m_neg", ""),
                p_active=row.get("p_active", ""),
                k=row.get("k", ""),
                n_cv=row.get("n_cv", ""),
                n_gcd=row.get("n_gcd", ""),
                v_start=row.get("v_start", ""),
                v_end=row.get("v_end", ""),
                cv_max=cv_max,
                gcd_max=gcd_max,
            )
            if row_errors:
                order = ["m_pos", "m_neg", "p_active", "k", "n_cv", "n_gcd", "v_start", "v_end"]
                first_field = next((f for f in order if f in row_errors), next(iter(row_errors)))
                return row_errors, (row_idx, first_field)
        return {}, None

    def _collect_params(self):
        out = {
            "a_geom": float(self.a_geom_var.get()),
            "output_type": self.output_type_var.get(),
            "export_battery_workbook": bool(self.export_book_var.get()),
            "electrode_rate_csp_column": self.electrode_rate_csp_col_var.get() if self.output_type_var.get() == "Csp" else None,
            "cv_current_unit": self.cv_current_unit_var.get(),
            "battery_params": {},
        }
        first_error = None
        if self.param_table is None:
            return out
        visible_fields = set(self._visible_param_fields())
        for row in self.param_table.rows:
            try:
                bp = {
                    "m_pos": float(row.get("m_pos", 1.0)) if "m_pos" in visible_fields else 1.0,
                    "m_neg": float(row.get("m_neg", 0.0)) if "m_neg" in visible_fields else 0.0,
                    "p_active": float(row.get("p_active", 100.0)) if "p_active" in visible_fields else 100.0,
                    "k": float(row.get("k", 1.0)) if "k" in visible_fields else 1.0,
                    "n_cv": int(row.get("n_cv", 1)) if self.file_type_presence.get("cv", False) else 1,
                    "n_gcd": int(row.get("n_gcd", 1)) if self.file_type_presence.get("gcd", False) else 1,
                    "v_start": float(row.get("v_start", 2.5)) if self.file_type_presence.get("gcd", False) else 2.5,
                    "v_end": float(row.get("v_end", 4.2)) if self.file_type_presence.get("gcd", False) else 4.2,
                }
                bp["main_order"] = "先充后放"
                out["battery_params"][str(row["name"])] = bp
            except Exception:
                first_error = str(row.get("name", ""))
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

    def _drop_cache_for_root(self, root_key: str) -> None:
        cp = self._cache_path()
        if not cp.exists():
            return
        try:
            obj = json.loads(cp.read_text(encoding="utf-8"))
        except Exception:
            cp.unlink(missing_ok=True)
            return
        if root_key in obj:
            obj.pop(root_key, None)
            cp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

    def _load_cache_or_keep(self):
        cp = self._cache_path()
        if not cp.exists() or not self.selected_root:
            return
        obj = json.loads(cp.read_text(encoding="utf-8"))
        cur = obj.get(str(self.selected_root))
        if not cur:
            return
        vals = cur.get("rows", [])
        col_choice = cur.get("electrode_rate_csp_column")
        if col_choice in {"csp_noir", "csp_eff"}:
            self.electrode_rate_csp_col_var.set(col_choice)
        if self.param_table is None:
            return
        for row_idx, row in enumerate(vals):
            if row_idx >= len(self.param_table.rows):
                break
            self.param_table.rows[row_idx].update(row)
        self.param_table.redraw()

    def _save_cache(self):
        if not self.selected_root:
            return
        cp = self._cache_path()
        cp.parent.mkdir(parents=True, exist_ok=True)
        obj = {}
        if cp.exists():
            obj = json.loads(cp.read_text(encoding="utf-8"))
        rows = self.param_table.rows if self.param_table is not None else []
        obj[str(self.selected_root)] = {
            "rows": rows,
            "electrode_rate_csp_column": self.electrode_rate_csp_col_var.get(),
        }
        cp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

    def _clear_cache(self):
        cp = self._cache_path()
        if cp.exists():
            cp.unlink()
        if self.scan_result is not None:
            self._fill_step2(self.scan_result)
        self._refresh_error_states()
        messagebox.showinfo(WINDOW_TITLE, "缓存已清空，并恢复为扫描默认值")

    def _back_to_step1(self):
        self._save_cache()
        self._show_step(1)

    def _confirm_export(self):
        if not self.scan_result or not self.selected_root:
            return
        errors, first_cell = self._validate_all_rows()
        if errors:
            if first_cell is not None:
                row_idx, col_key = first_cell
                if self.param_table is not None:
                    self.param_table.scroll_to_cell(row_idx, col_key)
                    self.param_table.focus_cell(row_idx, col_key)
            messagebox.showerror(WINDOW_TITLE, "参数存在错误，已定位到第一个错误单元格")
            return
        try:
            params = self._collect_params()
        except Exception as e:
            messagebox.showerror(WINDOW_TITLE, str(e))
            return
        sels = self._collect_selections()

        def progress(stage, current, percent):
            self.msg_q.put(("progress", (stage, current, percent)))

        self._final_stage_seen = False
        self._pending_export_result = None

        def worker():
            try:
                result = run_full_export(str(self.selected_root), self.scan_result, params, sels, self.ctx, self.logger, progress)
                self.msg_q.put(("export_done", result))
            except Exception as e:
                self.logger.exception("gui export worker failed", exc=e)
                self.msg_q.put(("export_done", {"error": str(e)}))

        self.export_thread = threading.Thread(target=worker, daemon=True)
        self.export_thread.start()

    def _on_export_done(self, result: dict):
        if "error" in result:
            messagebox.showerror(WINDOW_TITLE, result["error"])
            return
        report_lines = []
        try:
            report_lines = Path(result["run_report_path"]).read_text(encoding="utf-8", errors="ignore").splitlines()
        except Exception:
            report_lines = []
        rep_failures = [x for x in report_lines if x.startswith("E") or x.startswith("文件失败:") or " 文件失败 " in x]
        rep_warnings = [x for x in report_lines if x.startswith("W")]
        merged = list(dict.fromkeys([*(result.get("failures", []) or []), *(result.get("warnings", []) or []), *rep_failures, *rep_warnings]))
        skipped_file = self.ctx.paths.reports_dir / f"skipped_paths-{self.ctx.run_id}.txt"
        lines = [
            f"根目录: {self.selected_root}",
            f"极片级: {Path(result['electrode_path']).name}",
            f"电池级: {Path(result['battery_path']).name if result['battery_path'] else '(disabled)'}",
            f"运行报告: {result['run_report_path']}",
            f"日志: {result['log_path']}",
            f"skipped_paths: {skipped_file}",
            "失败/告警(前50，完整见运行报告):",
        ]
        for x in merged[:50]:
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

    def open_skipped_list_dir(self) -> None:
        skipped_file = self.ctx.paths.reports_dir / f"skipped_paths-{self.ctx.run_id}.txt"
        self._open_directory(skipped_file.parent, skipped_file)


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

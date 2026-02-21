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
from .export_pipeline import run_full_export
from .param_validation import coerce_int_strict, validate_battery_row, validate_global
from .scanner import ScanResult, scan_root
from .state_store import read_last_root, write_last_root

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
        self.a_geom_var = tk.StringVar(value="1")
        self.export_book_var = tk.BooleanVar(value=True)
        self.open_folder_var = tk.BooleanVar(value=True)
        self._blink_job = None
        self._blink_on = False
        self.param_columns = ["name", "cvmax", "gcdmax", "m_pos", "m_neg", "p_active", "k", "n_cv", "n_gcd", "v_start", "v_end"]
        self.editable_columns = {3, 4, 5, 6, 7, 8, 9, 10}
        self.selected_cells: set[tuple[str, int]] = set()
        self.drag_start_cell: tuple[str, int] | None = None
        self.error_cells: dict[tuple[str, int], str] = {}
        self.undo_snapshot: dict[tuple[str, int], str] | None = None
        self.tooltip: tk.Toplevel | None = None
        self.tooltip_var = tk.StringVar(value="")
        self.filter_tab_visible = True

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
        ttk.Label(bottom, text=" 电池数:").pack(side="left", padx=(12, 0))
        ttk.Label(bottom, textvariable=self.battery_count_var).pack(side="left")
        ttk.Label(bottom, text=" 识别文件数:").pack(side="left", padx=(12, 0))
        ttk.Label(bottom, textvariable=self.recognized_file_count_var).pack(side="left")
        ttk.Label(bottom, text=" 跳过目录数:").pack(side="left", padx=(12, 0))
        ttk.Label(bottom, textvariable=self.skipped_dir_count_var).pack(side="left")
        ttk.Label(bottom, text=" 跳过文件数:").pack(side="left", padx=(12, 0))
        ttk.Label(bottom, textvariable=self.skipped_file_count_var).pack(side="left")
        ttk.Progressbar(bottom, orient="horizontal", mode="determinate", maximum=100, variable=self.progress_value_var, length=220).pack(side="left", padx=(12, 0))

    def _build_step1(self) -> None:
        actions = ttk.Frame(self.page1)
        actions.pack(fill="x")
        ttk.Button(actions, text="科斯特数据处理", command=self.choose_root).pack(side="left")
        self.start_scan_btn = ttk.Button(actions, text="开始扫描", command=self.start_scan, state="disabled")
        self.start_scan_btn.pack(side="left", padx=8)
        self.cancel_scan_btn = ttk.Button(actions, text="取消扫描", command=self.cancel_scan, state="disabled")
        self.cancel_scan_btn.pack(side="left")
        ttk.Button(actions, text="打开跳过清单", command=self.open_skipped_list_dir).pack(side="left", padx=8)

        path_frame = ttk.LabelFrame(self.page1, text="已选根目录", padding=8)
        path_frame.pack(fill="x", pady=(10, 0))
        ttk.Label(path_frame, textvariable=self.root_path_var).pack(anchor="w")

    def _build_step2(self) -> None:
        options = ttk.LabelFrame(self.page2, text="全局选项", padding=10)
        options.pack(fill="x")
        ttk.Label(self.page2, text="提示：R_turn 不是 RΩ，也不是 EIS 的 Rs；它是换向点表观ESR(DC)，用于工程对比。", foreground="#8a4b00").pack(anchor="w", pady=(6, 6))
        ttk.Label(options, text="输出类型").grid(row=0, column=0, sticky="w")
        ttk.Radiobutton(options, text="Csp", variable=self.output_type_var, value="Csp", command=self._on_output_type_change).grid(row=0, column=1, sticky="w")
        ttk.Radiobutton(options, text="Qsp", variable=self.output_type_var, value="Qsp", command=self._on_output_type_change).grid(row=0, column=2, sticky="w")
        ttk.Label(options, text="A_geom").grid(row=1, column=0, sticky="w")
        ttk.Entry(options, textvariable=self.a_geom_var, width=10).grid(row=1, column=1, sticky="w")
        ttk.Checkbutton(options, text="是否输出电池级工作簿", variable=self.export_book_var).grid(row=2, column=0, columnspan=2, sticky="w")
        ttk.Checkbutton(options, text="完成后打开输出文件夹", variable=self.open_folder_var).grid(row=3, column=0, columnspan=2, sticky="w")
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
        ).pack(anchor="w", pady=(0, 6))

        tree_wrap = ttk.Frame(self.param_page)
        tree_wrap.pack(fill="both", expand=True)

        self.param_tree = ttk.Treeview(
            tree_wrap,
            columns=tuple(self.param_columns),
            show="headings",
            height=10,
        )
        for c, t in [
            ("name", "电池名"), ("cvmax", "CV最大圈数"), ("gcdmax", "GCD最大圈数"), ("m_pos", "m_pos(mg)"), ("m_neg", "m_neg(mg)"),
            ("p_active", "p_active(%)"), ("k", "K(—)"), ("n_cv", "N_CV"), ("n_gcd", "N_GCD"), ("v_start", "V_start(V)"), ("v_end", "V_end(V)"),
        ]:
            self.param_tree.heading(c, text=t)
            self.param_tree.column(c, width=100, anchor="center")
        self.param_tree.tag_configure("row_error", background="#ffeaea")
        py = ttk.Scrollbar(tree_wrap, orient="vertical", command=self.param_tree.yview)
        px = ttk.Scrollbar(tree_wrap, orient="horizontal", command=self.param_tree.xview)
        self.param_tree.configure(yscrollcommand=py.set, xscrollcommand=px.set)
        self.param_tree.grid(row=0, column=0, sticky="nsew")
        py.grid(row=0, column=1, sticky="ns")
        px.grid(row=1, column=0, sticky="ew")
        tree_wrap.columnconfigure(0, weight=1)
        tree_wrap.rowconfigure(0, weight=1)
        self.param_tree.bind("<Double-1>", self._edit_param_cell)
        self.param_tree.bind("<Button-1>", self._on_tree_click)
        self.param_tree.bind("<Shift-Button-1>", self._on_tree_shift_click)
        self.param_tree.bind("<B1-Motion>", self._on_tree_drag)
        self.param_tree.bind("<Motion>", self._on_tree_hover)
        self.param_tree.bind("<Leave>", lambda _e: self._hide_tooltip())
        self.param_tree.bind("<Control-c>", self._copy_selection)
        self.param_tree.bind("<Control-v>", self._paste_multi_value)
        self.param_tree.bind("<Control-z>", self._undo_last_edit)

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
            ys = ttk.Scrollbar(col, orient="vertical", command=w.yview)
            w.configure(yscrollcommand=ys.set)
            w.pack(side="left", fill="both", expand=True)
            ys.pack(side="right", fill="y")
            sel_frame.columnconfigure(i, weight=1)

        btns = ttk.Frame(self.page2)
        btns.pack(fill="x", pady=(10, 0))
        ttk.Button(btns, text="返回上一步", command=self._back_to_step1).pack(side="left")
        ttk.Button(btns, text="清空缓存", command=self._clear_cache).pack(side="left", padx=8)
        ttk.Button(btns, text="确定导出", command=self._confirm_export).pack(side="left")

    def _on_output_type_change(self):
        output_type = self.output_type_var.get()
        display_cols = list(self.param_columns)
        if output_type == "Qsp":
            display_cols.remove("k")
        self.param_tree.configure(displaycolumns=display_cols)
        for iid in self.param_tree.get_children():
            vals = list(self.param_tree.item(iid, "values"))
            if output_type == "Qsp":
                vals[6] = 1
            self.param_tree.item(iid, values=vals)
        self._refresh_error_states()


    def _load_default_open_dir(self) -> None:
        last_root = read_last_root(self.ctx.paths.state_dir)
        if last_root is not None:
            self.default_open_dir = last_root.parent
            return
        self.default_open_dir = self.ctx.paths.program_dir

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
        write_last_root(self.ctx.paths.state_dir, self.selected_root)
        self.default_open_dir = self.selected_root.parent

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
        self.param_tree.delete(*self.param_tree.get_children())
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
                    self._fill_step2(payload)
                    self.notebook.select(self.page2)
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
                elif kind == "export_done":
                    self._on_export_done(payload)
        except queue.Empty:
            pass
        self.root.after(120, self._poll_queue)

    def _fill_step2(self, result: ScanResult):
        self.param_tree.delete(*self.param_tree.get_children())
        self.selected_cells.clear()
        self.error_cells.clear()
        for b in sorted(result.batteries, key=lambda x: x.name):
            self.param_tree.insert("", "end", values=(b.name, b.cv_max_cycle if b.cv_max_cycle is not None else "-", b.gcd_max_cycle if b.gcd_max_cycle is not None else "-", self.default_row_values["m_pos"], self.default_row_values["m_neg"], self.default_row_values["p_active"], self.default_row_values["k"], self.default_row_values["n_cv"], self.default_row_values["n_gcd"], self.default_row_values["v_start"], self.default_row_values["v_end"]))
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

    def _cell_from_event(self, event) -> tuple[str, int] | None:
        iid = self.param_tree.identify_row(event.y)
        col = self.param_tree.identify_column(event.x)
        if not iid or not col:
            return None
        return iid, int(col[1:]) - 1

    def _select_rectangle(self, start: tuple[str, int], end: tuple[str, int]) -> None:
        rows = list(self.param_tree.get_children())
        s_row, s_col = start
        e_row, e_col = end
        if s_row not in rows or e_row not in rows:
            return
        rs, re = sorted([rows.index(s_row), rows.index(e_row)])
        cs, ce = sorted([s_col, e_col])
        self.selected_cells = {(rows[r], c) for r in range(rs, re + 1) for c in range(cs, ce + 1)}
        self.param_tree.selection_set(rows[rs: re + 1])

    def _on_tree_click(self, event):
        cell = self._cell_from_event(event)
        if not cell:
            return
        self.drag_start_cell = cell
        self.selected_cells = {cell}

    def _on_tree_shift_click(self, event):
        cell = self._cell_from_event(event)
        if not cell:
            return
        anchor = self.drag_start_cell or cell
        self._select_rectangle(anchor, cell)
        return "break"

    def _on_tree_drag(self, event):
        if not self.drag_start_cell:
            return
        cell = self._cell_from_event(event)
        if not cell:
            return
        self._select_rectangle(self.drag_start_cell, cell)

    def _on_tree_hover(self, event):
        cell = self._cell_from_event(event)
        if not cell or cell not in self.error_cells:
            self._hide_tooltip()
            return
        self._show_tooltip(event.x_root + 12, event.y_root + 12, self.error_cells[cell])

    def _show_tooltip(self, x: int, y: int, text: str):
        if self.tooltip is None:
            self.tooltip = tk.Toplevel(self.root)
            self.tooltip.wm_overrideredirect(True)
            tk.Label(self.tooltip, textvariable=self.tooltip_var, relief="solid", borderwidth=1, background="#fff8dc").pack()
        self.tooltip_var.set(text)
        self.tooltip.geometry(f"+{x}+{y}")
        self.tooltip.deiconify()

    def _hide_tooltip(self):
        if self.tooltip is not None:
            self.tooltip.withdraw()

    def _copy_selection(self, _event=None):
        if not self.selected_cells:
            return "break"
        rows = list(self.param_tree.get_children())
        row_idxs = sorted({rows.index(iid) for iid, _c in self.selected_cells if iid in rows})
        col_idxs = sorted({c for _iid, c in self.selected_cells})
        lines = []
        for r in row_idxs:
            iid = rows[r]
            vals = list(self.param_tree.item(iid, "values"))
            lines.append("\t".join(str(vals[c]) for c in col_idxs))
        self.root.clipboard_clear()
        self.root.clipboard_append("\n".join(lines))
        return "break"

    def _parse_matrix(self, text: str) -> list[list[str]]:
        lines = [ln for ln in re.split(r"\r?\n", text) if ln != ""]
        return [ln.split("\t") for ln in lines]

    def _apply_cell_updates(self, updates: dict[tuple[str, int], str], save_undo: bool = True) -> None:
        if not updates:
            return
        if save_undo:
            self.undo_snapshot = {(iid, c): str(self.param_tree.item(iid, "values")[c]) for iid, c in updates}
        for (iid, c), nv in updates.items():
            vals = list(self.param_tree.item(iid, "values"))
            vals[c] = nv
            if c == 6 and self.output_type_var.get() == "Qsp":
                continue
            self.param_tree.item(iid, values=vals)
        self._refresh_error_states()

    def _fill_single_value(self):
        value = self.param_fill_var.get()
        targets = {(iid, c) for iid, c in self.selected_cells if c in self.editable_columns and not (c == 6 and self.output_type_var.get() == "Qsp")}
        updates = {k: value for k in targets}
        self._apply_cell_updates(updates)

    def _paste_multi_value(self, _event=None):
        text = self.param_fill_var.get().strip()
        if not text:
            try:
                text = self.root.clipboard_get()
            except Exception:
                text = ""
        matrix = self._parse_matrix(text)
        if not matrix:
            return "break"
        rows = list(self.param_tree.get_children())
        if not rows:
            return "break"
        if self.selected_cells:
            start_row_idx = min(rows.index(iid) for iid, _c in self.selected_cells if iid in rows)
            start_col = min(c for _iid, c in self.selected_cells)
        else:
            start_row_idx, start_col = 0, 3
        updates: dict[tuple[str, int], str] = {}
        for dr, line in enumerate(matrix):
            rr = start_row_idx + dr
            if rr >= len(rows):
                break
            for dc, value in enumerate(line):
                cc = start_col + dc
                if cc >= len(self.param_columns):
                    break
                if cc not in self.editable_columns:
                    continue
                if cc == 6 and self.output_type_var.get() == "Qsp":
                    continue
                updates[(rows[rr], cc)] = value
        self._apply_cell_updates(updates)
        return "break"

    def _undo_last_edit(self, _event=None):
        if self.undo_snapshot:
            self._apply_cell_updates(self.undo_snapshot, save_undo=False)
            self.undo_snapshot = None
        return "break"

    def _refresh_error_states(self):
        self.error_cells.clear()
        for iid in self.param_tree.get_children():
            vals = list(self.param_tree.item(iid, "values"))
            cv_max = coerce_int_strict(str(vals[1]))
            gcd_max = coerce_int_strict(str(vals[2]))
            row_errors = validate_battery_row(
                output_type=self.output_type_var.get(),
                m_pos=vals[3],
                m_neg=vals[4],
                p_active=vals[5],
                k=vals[6],
                n_cv=vals[7],
                n_gcd=vals[8],
                v_start=vals[9],
                v_end=vals[10],
                cv_max=cv_max,
                gcd_max=gcd_max,
            )
            if row_errors:
                self.param_tree.item(iid, tags=("row_error",))
                col_map = {"m_pos": 3, "m_neg": 4, "p_active": 5, "k": 6, "n_cv": 7, "n_gcd": 8, "v_start": 9, "v_end": 10}
                for k, msgs in row_errors.items():
                    if k in col_map:
                        self.error_cells[(iid, col_map[k])] = "；".join(msgs)
            else:
                self.param_tree.item(iid, tags=())

    def _edit_param_cell(self, event):
        iid = self.param_tree.identify_row(event.y)
        col = self.param_tree.identify_column(event.x)
        if not iid or not col:
            return
        ci = int(col[1:]) - 1
        if ci <= 2:
            return
        if ci == 6 and self.output_type_var.get() == "Qsp":
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
            self._refresh_error_states()
            editor.destroy()

        editor.bind("<Return>", save)
        editor.bind("<FocusOut>", save)

    def _set_focus_cell(self, iid: str, column_index: int) -> None:
        col = f"#{column_index + 1}"
        self.param_tree.focus(iid)
        self.param_tree.selection_set(iid)
        self.param_tree.focus_set()
        self.param_tree.see(iid)
        self.param_tree.update_idletasks()
        bbox = self.param_tree.bbox(iid, col)
        if bbox:
            x, y, w, h = bbox
            self.param_tree.event_generate("<Button-1>", x=x + max(2, w // 3), y=y + max(2, h // 2))

    def _blink_error_cell(self, iid: str, column_index: int) -> None:
        if self._blink_job is not None:
            self.root.after_cancel(self._blink_job)
            self._blink_job = None
        self._blink_on = False

        def toggle(count: int = 0):
            self._set_focus_cell(iid, column_index)
            self._blink_on = not self._blink_on
            if self._blink_on:
                self.param_tree.tag_configure("error_focus", background="#ffe1e1")
                self.param_tree.item(iid, tags=("error_focus",))
            else:
                self.param_tree.item(iid, tags=())
            if count < 3:
                self._blink_job = self.root.after(180, lambda: toggle(count + 1))
            else:
                self.param_tree.item(iid, tags=("error_focus",))
                self._blink_job = None

        toggle()

    def _validate_all_rows(self):
        first_iid = next(iter(self.param_tree.get_children()), None)
        try:
            a_geom = float(self.a_geom_var.get())
        except Exception:
            return {"global": ["A_geom 必须 > 0"]}, ((first_iid, 3) if first_iid else None)
        output_type = self.output_type_var.get()
        global_errors = validate_global(output_type=output_type, a_geom=a_geom)
        if global_errors:
            return {"global": global_errors}, ((first_iid, 3) if first_iid else None)

        for iid in self.param_tree.get_children():
            vals = list(self.param_tree.item(iid, "values"))
            cv_max = coerce_int_strict(str(vals[1]))
            gcd_max = coerce_int_strict(str(vals[2]))
            row_errors = validate_battery_row(
                output_type=output_type,
                m_pos=vals[3],
                m_neg=vals[4],
                p_active=vals[5],
                k=vals[6],
                n_cv=vals[7],
                n_gcd=vals[8],
                v_start=vals[9],
                v_end=vals[10],
                cv_max=cv_max,
                gcd_max=gcd_max,
            )
            if row_errors:
                order = ["m_pos", "m_neg", "p_active", "k", "n_cv", "n_gcd", "v_start", "v_end"]
                col_idx = {"m_pos": 3, "m_neg": 4, "p_active": 5, "k": 6, "n_cv": 7, "n_gcd": 8, "v_start": 9, "v_end": 10}
                first_field = next((f for f in order if f in row_errors), next(iter(row_errors)))
                return row_errors, (iid, col_idx[first_field])
        return {}, None

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
        if self.scan_result is not None:
            self._fill_step2(self.scan_result)
        self.undo_snapshot = None
        self.error_cells.clear()
        self.selected_cells.clear()
        self._refresh_error_states()
        messagebox.showinfo(WINDOW_TITLE, "缓存已清空，并恢复为扫描默认值")

    def _back_to_step1(self):
        self._save_cache()
        self.notebook.select(self.page1)

    def _confirm_export(self):
        if not self.scan_result or not self.selected_root:
            return
        errors, first_cell = self._validate_all_rows()
        if errors:
            if first_cell is not None:
                iid, col_idx = first_cell
                self.param_tree.see(iid)
                self._blink_error_cell(iid, col_idx)
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
            "失败/告警(前50):",
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

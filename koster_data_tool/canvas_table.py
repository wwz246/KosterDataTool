from __future__ import annotations

import re
import tkinter as tk
from tkinter import ttk


class CanvasTable(ttk.Frame):
    def __init__(self, parent, columns: list[dict], rows: list[dict], readonly_cols: set[str]):
        super().__init__(parent)
        self.columns = list(columns)
        self.rows = [dict(r) for r in rows]
        self.readonly_cols = set(readonly_cols)
        self.row_height = 24
        self.header_height = 28
        self._undo_snapshot: dict[tuple[int, str], str] | None = None
        self._selection: set[tuple[int, str]] = set()
        self._active_cell: tuple[int, str] | None = None
        self._anchor_cell: tuple[int, str] | None = None
        self._drag_start_cell: tuple[int, str] | None = None
        self._drag_start_xy = (0, 0)
        self._drag_threshold = 6
        self.invalid_cells: dict[tuple[int, str], str] = {}
        self._tooltip: tk.Toplevel | None = None
        self._tooltip_var = tk.StringVar(value="")
        self._editor: ttk.Entry | None = None
        self._editor_window = None
        self.on_data_changed = None

        self.header_canvas = tk.Canvas(self, height=self.header_height, bg="#f5f5f5", highlightthickness=0)
        self.body_canvas = tk.Canvas(self, bg="white", highlightthickness=0)
        self.v_scroll = ttk.Scrollbar(self, orient="vertical", command=self.yview)
        self.h_scroll = ttk.Scrollbar(self, orient="horizontal", command=self.xview)

        self.header_canvas.grid(row=0, column=0, sticky="ew")
        self.body_canvas.grid(row=1, column=0, sticky="nsew")
        self.v_scroll.grid(row=1, column=1, sticky="ns")
        self.h_scroll.grid(row=2, column=0, sticky="ew")
        self.grid_rowconfigure(1, weight=1)
        self.grid_columnconfigure(0, weight=1)

        self.body_canvas.configure(xscrollcommand=self._on_xscroll, yscrollcommand=self.v_scroll.set)

        self.body_canvas.bind("<Button-1>", self._on_click)
        self.body_canvas.bind("<Control-Button-1>", self._on_ctrl_click)
        self.body_canvas.bind("<Shift-Button-1>", self._on_shift_click)
        self.body_canvas.bind("<B1-Motion>", self._on_drag)
        self.body_canvas.bind("<ButtonRelease-1>", self._on_release)
        self.body_canvas.bind("<Double-Button-1>", self._on_double_click)
        self.body_canvas.bind("<Motion>", self._on_motion)
        self.body_canvas.bind("<Leave>", lambda _e: self._hide_tooltip())
        self.body_canvas.bind("<Control-c>", self._on_copy)
        self.body_canvas.bind("<Control-v>", self._on_paste)
        self.body_canvas.bind("<Control-z>", self._on_undo)
        self.body_canvas.bind("<MouseWheel>", self._on_mousewheel)
        self.body_canvas.bind("<Button-4>", lambda _e: self.yview("scroll", -1, "units"))
        self.body_canvas.bind("<Button-5>", lambda _e: self.yview("scroll", 1, "units"))

        self.body_canvas.focus_set()
        self.redraw()

    def set_columns(self, columns):
        self.columns = list(columns)
        valid_keys = {c["key"] for c in self.columns}
        self._selection = {(r, c) for (r, c) in self._selection if c in valid_keys and r < len(self.rows)}
        if self._active_cell and self._active_cell[1] not in valid_keys:
            self._active_cell = None
        self.redraw()

    def set_rows(self, rows):
        self.rows = [dict(r) for r in rows]
        self._selection = {(r, c) for (r, c) in self._selection if r < len(self.rows)}
        if self._active_cell and self._active_cell[0] >= len(self.rows):
            self._active_cell = None
        self.redraw()

    def get_value(self, row_idx, col_key):
        if row_idx < 0 or row_idx >= len(self.rows):
            return ""
        return str(self.rows[row_idx].get(col_key, ""))

    def set_value(self, row_idx, col_key, value_str):
        if row_idx < 0 or row_idx >= len(self.rows):
            return
        self.rows[row_idx][col_key] = value_str
        if self.on_data_changed:
            self.on_data_changed()
        self.redraw()

    def set_invalid(self, cell, message):
        self.invalid_cells[cell] = message
        self.redraw()

    def clear_invalid(self, cell):
        self.invalid_cells.pop(cell, None)
        self.redraw()

    def focus_cell(self, row_idx, col_key):
        self._active_cell = (row_idx, col_key)
        self._anchor_cell = self._active_cell
        self._selection = {(row_idx, col_key)}
        self.scroll_to_cell(row_idx, col_key)
        self.redraw()

    def get_selection_cells(self):
        return set(self._selection)

    def get_selection_bbox(self):
        if not self._selection:
            return (0, 0, 0, 0)
        rows = [r for r, _ in self._selection]
        cols = [self._col_index(k) for _, k in self._selection]
        return min(rows), min(cols), max(rows), max(cols)

    def scroll_to_cell(self, row_idx, col_key):
        cidx = self._col_index(col_key)
        x0, y0, x1, y1 = self._cell_rect(row_idx, cidx)
        vw = max(self.body_canvas.winfo_width(), 1)
        vh = max(self.body_canvas.winfo_height(), 1)
        total_w = max(self._total_width(), 1)
        total_h = max(len(self.rows) * self.row_height, 1)
        sx0 = self.body_canvas.canvasx(0)
        sy0 = self.body_canvas.canvasy(0)
        if x0 < sx0:
            self.body_canvas.xview_moveto(x0 / total_w)
        elif x1 > sx0 + vw:
            self.body_canvas.xview_moveto(max(0, (x1 - vw) / total_w))
        if y0 < sy0:
            self.body_canvas.yview_moveto(y0 / total_h)
        elif y1 > sy0 + vh:
            self.body_canvas.yview_moveto(max(0, (y1 - vh) / total_h))

    def xview(self, *args):
        self.body_canvas.xview(*args)
        self.header_canvas.xview(*args)

    def yview(self, *args):
        self.body_canvas.yview(*args)

    def redraw(self):
        self.header_canvas.delete("all")
        self.body_canvas.delete("all")
        total_w = self._total_width()
        total_h = len(self.rows) * self.row_height
        self.header_canvas.configure(scrollregion=(0, 0, total_w, self.header_height))
        self.body_canvas.configure(scrollregion=(0, 0, total_w, total_h))

        x = 0
        for col in self.columns:
            w = col.get("width", 100)
            self.header_canvas.create_rectangle(x, 0, x + w, self.header_height, fill="#f0f0f0", outline="#c8c8c8")
            self.header_canvas.create_text(x + 4, self.header_height // 2, text=col.get("title", ""), anchor="w")
            x += w

        for r in range(len(self.rows)):
            y0 = r * self.row_height
            y1 = y0 + self.row_height
            x = 0
            for cidx, col in enumerate(self.columns):
                key = col["key"]
                w = col.get("width", 100)
                fill = "#ffffff"
                if (r, key) in self._selection:
                    fill = "#eaf3ff"
                self.body_canvas.create_rectangle(x, y0, x + w, y1, fill=fill, outline="#dddddd")
                v = str(self.rows[r].get(key, ""))
                self.body_canvas.create_text(x + 4, y0 + self.row_height // 2, text=v, anchor="w")
                if (r, key) in self.invalid_cells:
                    self.body_canvas.create_rectangle(x + 1, y0 + 1, x + w - 1, y1 - 1, outline="#cf0000", width=2)
                if self._active_cell == (r, key):
                    self.body_canvas.create_rectangle(x + 1, y0 + 1, x + w - 1, y1 - 1, outline="#1f9d45", width=3)
                x += w

    def _total_width(self):
        return sum(c.get("width", 100) for c in self.columns)

    def _col_index(self, col_key):
        for i, col in enumerate(self.columns):
            if col["key"] == col_key:
                return i
        return 0

    def _xy_to_cell(self, x, y):
        cx = self.body_canvas.canvasx(x)
        cy = self.body_canvas.canvasy(y)
        if cx < 0 or cy < 0:
            return None
        row = int(cy // self.row_height)
        if row < 0 or row >= len(self.rows):
            return None
        acc = 0
        for idx, col in enumerate(self.columns):
            w = col.get("width", 100)
            if acc <= cx < acc + w:
                return (row, idx)
            acc += w
        return None

    def _cell_rect(self, row, cidx):
        x0 = sum(self.columns[i].get("width", 100) for i in range(cidx))
        x1 = x0 + self.columns[cidx].get("width", 100)
        y0 = row * self.row_height
        y1 = y0 + self.row_height
        return x0, y0, x1, y1

    def _set_rect_selection(self, start, end):
        r0, c0 = start
        r1, c1 = end
        rlo, rhi = min(r0, r1), max(r0, r1)
        clo, chi = min(c0, c1), max(c0, c1)
        self._selection.clear()
        for r in range(rlo, rhi + 1):
            for c in range(clo, chi + 1):
                self._selection.add((r, self.columns[c]["key"]))

    def _on_click(self, event):
        self.body_canvas.focus_set()
        cell = self._xy_to_cell(event.x, event.y)
        if cell is None:
            return "break"
        r, c = cell
        key = self.columns[c]["key"]
        self._selection = {(r, key)}
        self._active_cell = (r, key)
        self._anchor_cell = (r, c)
        self._drag_start_cell = (r, c)
        self._drag_start_xy = (event.x, event.y)
        self.redraw()
        return "break"

    def _on_ctrl_click(self, event):
        self.body_canvas.focus_set()
        cell = self._xy_to_cell(event.x, event.y)
        if cell is None:
            return "break"
        r, c = cell
        key = self.columns[c]["key"]
        target = (r, key)
        if target in self._selection:
            self._selection.remove(target)
        else:
            self._selection.add(target)
        self._active_cell = target
        self._anchor_cell = (r, c)
        self.redraw()
        return "break"

    def _on_shift_click(self, event):
        cell = self._xy_to_cell(event.x, event.y)
        if cell is None:
            return "break"
        if self._anchor_cell is None:
            return self._on_click(event)
        self._set_rect_selection(self._anchor_cell, cell)
        r, c = cell
        self._active_cell = (r, self.columns[c]["key"])
        self.redraw()
        return "break"

    def _on_drag(self, event):
        if self._drag_start_cell is None:
            return "break"
        x0, y0 = self._drag_start_xy
        if abs(event.x - x0) + abs(event.y - y0) < self._drag_threshold:
            return "break"
        cell = self._xy_to_cell(event.x, event.y)
        if cell is None:
            return "break"
        self._set_rect_selection(self._drag_start_cell, cell)
        r, c = cell
        self._active_cell = (r, self.columns[c]["key"])
        self.redraw()
        return "break"

    def _on_release(self, _event):
        self._drag_start_cell = None
        return "break"

    def _on_double_click(self, event):
        cell = self._xy_to_cell(event.x, event.y)
        if cell is None:
            return "break"
        r, c = cell
        key = self.columns[c]["key"]
        if key in self.readonly_cols:
            return "break"
        self.focus_cell(r, key)
        self._open_editor(r, c)
        return "break"

    def _open_editor(self, row, cidx):
        if self._editor is not None:
            self._close_editor(save=True)
        key = self.columns[cidx]["key"]
        x0, y0, x1, y1 = self._cell_rect(row, cidx)
        entry = ttk.Entry(self.body_canvas)
        entry.insert(0, self.get_value(row, key))
        self._editor = entry
        self._editor_window = self.body_canvas.create_window(x0 + 1, y0 + 1, anchor="nw", width=x1 - x0 - 2, height=y1 - y0 - 2, window=entry)
        entry.focus_set()

        def save(_e=None):
            self.set_value(row, key, entry.get())
            self._close_editor(save=False)
            return "break"

        def cancel(_e=None):
            self._close_editor(save=False)
            self.redraw()
            return "break"

        entry.bind("<Return>", save)
        entry.bind("<Escape>", cancel)
        entry.bind("<FocusOut>", save)

    def _close_editor(self, save=False):
        if self._editor is None:
            return
        if self._editor_window is not None:
            self.body_canvas.delete(self._editor_window)
        self._editor.destroy()
        self._editor = None
        self._editor_window = None

    def _on_motion(self, event):
        cell = self._xy_to_cell(event.x, event.y)
        if cell is None:
            self._hide_tooltip()
            return
        r, c = cell
        key = self.columns[c]["key"]
        msg = self.invalid_cells.get((r, key))
        if not msg:
            self._hide_tooltip()
            return
        self._show_tooltip(event.x_root + 12, event.y_root + 12, msg)

    def _show_tooltip(self, x, y, text):
        if self._tooltip is None:
            self._tooltip = tk.Toplevel(self)
            self._tooltip.overrideredirect(True)
            tk.Label(self._tooltip, textvariable=self._tooltip_var, relief="solid", borderwidth=1, background="#fff8dc").pack()
        self._tooltip_var.set(text)
        self._tooltip.geometry(f"+{x}+{y}")
        self._tooltip.deiconify()

    def _hide_tooltip(self):
        if self._tooltip is not None:
            self._tooltip.withdraw()

    def _selected_matrix(self):
        if not self._selection:
            return []
        r0, c0, r1, c1 = self.get_selection_bbox()
        out = []
        for r in range(r0, r1 + 1):
            row = []
            for c in range(c0, c1 + 1):
                row.append(self.get_value(r, self.columns[c]["key"]))
            out.append(row)
        return out

    def _on_copy(self, _event=None):
        matrix = self._selected_matrix()
        if not matrix:
            return "break"
        text = "\n".join("\t".join(r) for r in matrix)
        self.clipboard_clear()
        self.clipboard_append(text)
        return "break"

    def _parse_matrix(self, text):
        lines = [ln for ln in re.split(r"\r?\n", text) if ln != ""]
        return [ln.split("\t") for ln in lines]

    def paste_matrix(self, matrix):
        if not matrix or not self.rows or not self.columns:
            return
        r0, c0, _, _ = self.get_selection_bbox()
        updates: dict[tuple[int, str], str] = {}
        for dr, line in enumerate(matrix):
            rr = r0 + dr
            if rr >= len(self.rows):
                break
            for dc, value in enumerate(line):
                cc = c0 + dc
                if cc >= len(self.columns):
                    break
                key = self.columns[cc]["key"]
                if key in self.readonly_cols:
                    continue
                updates[(rr, key)] = value
        self._apply_updates(updates)

    def _on_paste(self, _event=None):
        try:
            text = self.clipboard_get().strip()
        except Exception:
            return "break"
        if not text:
            return "break"
        self.paste_matrix(self._parse_matrix(text))
        return "break"

    def _apply_updates(self, updates: dict[tuple[int, str], str], save_undo=True):
        if not updates:
            return
        if save_undo:
            self._undo_snapshot = {k: self.get_value(*k) for k in updates}
        for (r, k), v in updates.items():
            self.rows[r][k] = v
        if self.on_data_changed:
            self.on_data_changed()
        self.redraw()

    def fill_selection(self, value: str):
        updates = {(r, c): value for (r, c) in self._selection if c not in self.readonly_cols}
        self._apply_updates(updates)

    def _on_undo(self, _event=None):
        if self._undo_snapshot:
            self._apply_updates(self._undo_snapshot, save_undo=False)
            self._undo_snapshot = None
        return "break"

    def _on_xscroll(self, first, last):
        self.h_scroll.set(first, last)
        self.header_canvas.xview_moveto(first)

    def _on_mousewheel(self, event):
        delta = -1 if event.delta > 0 else 1
        self.yview("scroll", delta, "units")
        return "break"

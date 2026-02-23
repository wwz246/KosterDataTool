from __future__ import annotations

import re

CYCLE_TAIL_RE = re.compile(r"\s+\d+\s*CYCLE\s*$", re.IGNORECASE)


def read_fixed_tab_table(file_path: str) -> tuple[list[str], list[list[str]]]:
    raw_text = open(file_path, "r", encoding="utf-8").read()
    lines = raw_text.splitlines()
    if len(lines) < 3:
        raise ValueError("E9003: file too short (<3 lines)")

    header_line = lines[1]
    data_lines = lines[2:]
    header = header_line.rstrip("\n\r").split("\t")

    rows_tokens: list[list[str]] = []
    for idx, line in enumerate(data_lines, start=3):
        if line.strip() == "":
            continue
        tokens = line.rstrip("\n\r").split("\t")
        if len(tokens) != len(header):
            raise ValueError(
                f"E9004: inconsistent column count file={file_path} header_cols={len(header)} bad_row_index={idx}"
            )
        rows_tokens.append(tokens)

    return header, rows_tokens


def tokens_to_float_matrix(header: list[str], rows_tokens: list[list[str]], file_path: str = "") -> list[list[float]]:
    matrix: list[list[float]] = []
    fp = file_path or "<unknown>"
    for row_idx, row_tokens in enumerate(rows_tokens, start=3):
        row_vals: list[float] = []
        for col_idx, token in enumerate(row_tokens):
            token_clean = CYCLE_TAIL_RE.sub("", token).strip()
            if token_clean == "":
                col_name = header[col_idx] if col_idx < len(header) else f"col#{col_idx}"
                raise ValueError(
                    f"E9005: non-numeric token file={fp} row_index={row_idx} col_name={col_name} token={token}"
                )
            try:
                row_vals.append(float(token_clean))
            except ValueError as exc:
                col_name = header[col_idx] if col_idx < len(header) else f"col#{col_idx}"
                raise ValueError(
                    f"E9005: non-numeric token file={fp} row_index={row_idx} col_name={col_name} token={token}"
                ) from exc
        matrix.append(row_vals)
    return matrix

from __future__ import annotations

from collections.abc import Mapping

PARAM_COLUMNS = [
    {"key": "name", "title": "电池名", "width": 140},
    {"key": "cvmax", "title": "CV最大圈数", "width": 110},
    {"key": "gcdmax", "title": "GCD最大圈数", "width": 120},
    {"key": "m_pos", "title": "m_pos(mg)", "width": 100},
    {"key": "m_neg", "title": "m_neg(mg)", "width": 100},
    {"key": "p_active", "title": "p_active(%)", "width": 100},
    {"key": "k", "title": "K(—)", "width": 90},
    {"key": "n_cv", "title": "N_CV", "width": 90},
    {"key": "n_gcd", "title": "N_GCD", "width": 90},
    {"key": "v_start", "title": "V_start(V)", "width": 100},
    {"key": "v_end", "title": "V_end(V)", "width": 100},
]


def get_visible_param_fields(file_type_presence: Mapping[str, bool], output_type: str, cv_current_unit: str) -> list[str]:
    has_cv = bool(file_type_presence.get("cv"))
    has_gcd = bool(file_type_presence.get("gcd"))

    if not has_cv and not has_gcd:
        return []

    show_mass_related = has_gcd or (has_cv and cv_current_unit == "A/g")
    show_k = has_gcd and output_type == "Csp"

    visible: list[str] = ["name"]
    if has_cv:
        visible.extend(["cvmax", "n_cv"])
    if has_gcd:
        visible.extend(["gcdmax", "n_gcd", "v_start", "v_end"])
    if show_mass_related:
        visible.extend(["m_pos", "m_neg", "p_active"])
    if show_k:
        visible.append("k")

    return [c["key"] for c in PARAM_COLUMNS if c["key"] in set(visible)]


def get_visible_param_columns(file_type_presence: Mapping[str, bool], output_type: str, cv_current_unit: str) -> list[dict]:
    visible_keys = set(get_visible_param_fields(file_type_presence, output_type, cv_current_unit))
    return [c for c in PARAM_COLUMNS if c["key"] in visible_keys]

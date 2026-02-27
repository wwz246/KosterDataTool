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
    hidden: set[str] = set()
    has_cv = bool(file_type_presence.get("cv"))
    has_gcd = bool(file_type_presence.get("gcd"))

    if not has_cv:
        hidden.update({"cvmax", "n_cv"})
    if not has_gcd:
        hidden.update({"gcdmax", "n_gcd", "v_start", "v_end"})
    if output_type == "Qsp" or cv_current_unit in {"A", "mA"}:
        hidden.add("k")
    if cv_current_unit in {"A", "mA"}:
        hidden.update({"m_pos", "m_neg", "p_active"})

    return [c["key"] for c in PARAM_COLUMNS if c["key"] not in hidden]


def get_visible_param_columns(file_type_presence: Mapping[str, bool], output_type: str, cv_current_unit: str) -> list[dict]:
    visible_keys = set(get_visible_param_fields(file_type_presence, output_type, cv_current_unit))
    return [c for c in PARAM_COLUMNS if c["key"] in visible_keys]

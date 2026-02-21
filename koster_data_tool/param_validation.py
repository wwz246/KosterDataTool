from __future__ import annotations


def validate_global(output_type: str, a_geom: float) -> list[str]:
    errors: list[str] = []
    if output_type not in {"Csp", "Qsp"}:
        errors.append("output_type 必须为 Csp 或 Qsp")
    if not isinstance(a_geom, (int, float)) or a_geom <= 0:
        errors.append("A_geom 必须 > 0")
    return errors


def coerce_int_strict(x: str) -> int | None:
    if isinstance(x, bool):
        return None
    if isinstance(x, int):
        return x
    if not isinstance(x, str):
        return None
    s = x.strip()
    if not s:
        return None
    if s[0] in {"+", "-"}:
        if len(s) == 1 or not s[1:].isdigit():
            return None
    elif not s.isdigit():
        return None
    return int(s)


def validate_battery_row(
    output_type,
    m_pos,
    m_neg,
    p_active,
    k,
    n_cv,
    n_gcd,
    v_start,
    v_end,
    cv_max,
    gcd_max,
) -> dict[str, str]:
    errors: dict[str, str] = {}

    try:
        m_pos_f = float(m_pos)
    except Exception:
        m_pos_f = None
    try:
        m_neg_f = float(m_neg)
    except Exception:
        m_neg_f = None

    if m_pos_f is None or m_pos_f < 0:
        errors["m_pos"] = "m_pos 必须 >= 0"
    if m_neg_f is None or m_neg_f < 0:
        errors["m_neg"] = "m_neg 必须 >= 0"
    if m_pos_f is not None and m_neg_f is not None and m_pos_f + m_neg_f <= 0:
        errors["m_pos"] = "m_pos+m_neg 必须 > 0"
        errors["m_neg"] = "m_pos+m_neg 必须 > 0"

    try:
        p_active_f = float(p_active)
        if not (10 < p_active_f <= 100):
            errors["p_active"] = "p_active 必须满足 10 < p_active <= 100"
    except Exception:
        errors["p_active"] = "p_active 必须满足 10 < p_active <= 100"

    n_cv_i = n_cv if isinstance(n_cv, int) else coerce_int_strict(str(n_cv))
    n_gcd_i = n_gcd if isinstance(n_gcd, int) else coerce_int_strict(str(n_gcd))

    if n_cv_i is None or n_cv_i <= 0:
        errors["n_cv"] = "N_CV 必须为正整数"
    if n_gcd_i is None or n_gcd_i <= 0:
        errors["n_gcd"] = "N_GCD 必须为正整数"

    if cv_max is not None and n_cv_i is not None and n_cv_i > int(cv_max):
        errors["n_cv"] = f"N_CV 必须 <= CV 最大圈数({int(cv_max)})"
    if gcd_max is not None and n_gcd_i is not None and n_gcd_i > int(gcd_max):
        errors["n_gcd"] = f"N_GCD 必须 <= GCD 最大圈数({int(gcd_max)})"

    try:
        v_start_f = float(v_start)
        v_end_f = float(v_end)
        if not (v_start_f < v_end_f):
            errors["v_start"] = "V_start 必须 < V_end"
            errors["v_end"] = "V_start 必须 < V_end"
    except Exception:
        errors["v_start"] = "V_start 必须 < V_end"

    if output_type == "Csp":
        try:
            k_f = float(k)
            if k_f <= 0:
                errors["k"] = "Csp 模式 K 必填且 > 0"
        except Exception:
            errors["k"] = "Csp 模式 K 必填且 > 0"

    return errors

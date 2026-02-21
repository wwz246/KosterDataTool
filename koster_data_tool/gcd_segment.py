from __future__ import annotations

from dataclasses import dataclass, field
from statistics import median


@dataclass
class GcdSegment:
    start: int
    end: int
    t_start: float
    t_end: float
    I_med: float
    E_start: float
    E_end: float
    deltaE_end: float
    kind: str
    warnings: list[str] = field(default_factory=list)


@dataclass
class GcdCycleSegments:
    cycle_k: int
    segments: list[GcdSegment]
    dropped_rest_count: int
    warnings: list[str] = field(default_factory=list)


@dataclass
class BatteryMainOrder:
    order: str
    decided_from: str
    warnings: list[str] = field(default_factory=list)


def _sign(x: float, eps: float = 0.0) -> int:
    if x > eps:
        return 1
    if x < -eps:
        return -1
    return 0


def calc_m_active_g(m_pos_mg: float, m_neg_mg: float, p_active_pct: float) -> float:
    if m_pos_mg < 0:
        raise ValueError("m_pos 非法")
    if m_neg_mg < 0:
        raise ValueError("m_neg 非法")
    if p_active_pct <= 0 or p_active_pct > 100:
        raise ValueError("p_active 非法")
    m_basis = m_pos_mg + m_neg_mg
    return (m_basis * p_active_pct / 100.0) / 1000.0


def _build_step_candidates(step: list[int]) -> list[tuple[int, int]]:
    if not step:
        return []
    out: list[tuple[int, int]] = []
    s = 0
    for i in range(1, len(step)):
        if step[i] != step[i - 1]:
            out.append((s, i - 1))
            s = i
    out.append((s, len(step) - 1))
    return out


def _sort_dedup_by_t(t: list[float], E: list[float], I: list[float], Step: list[int] | None) -> tuple[list[float], list[float], list[float], list[int] | None]:
    rows = sorted(enumerate(zip(t, E, I, Step if Step is not None else [None] * len(t))), key=lambda x: (x[1][0], x[0]))
    out_t: list[float] = []
    out_E: list[float] = []
    out_I: list[float] = []
    out_S: list[int] | None = [] if Step is not None else None
    seen_t: set[float] = set()
    for _idx, (tt, ee, ii, ss) in rows:
        if tt in seen_t:
            continue
        seen_t.add(tt)
        out_t.append(tt)
        out_E.append(ee)
        out_I.append(ii)
        if out_S is not None:
            out_S.append(int(ss) if ss is not None else 0)
    return out_t, out_E, out_I, out_S


def _build_current_candidates(t: list[float], I: list[float], epsI: float) -> list[tuple[int, int]]:
    tags = [_sign(v, epsI) for v in I]
    raw: list[tuple[int, int, int]] = []
    s = 0
    for i in range(1, len(tags)):
        if tags[i] != tags[i - 1]:
            raw.append((s, i - 1, tags[i - 1]))
            s = i
    raw.append((s, len(tags) - 1, tags[-1]))

    valid: list[tuple[int, int, int]] = []
    for a, b, sg in raw:
        iabs_med = median([abs(x) for x in I[a : b + 1]])
        if iabs_med > epsI and sg != 0:
            valid.append((a, b, _sign(median(I[a : b + 1]))))

    if len(t) >= 2:
        dt = median([max(0.0, t[i + 1] - t[i]) for i in range(len(t) - 1)])
    else:
        dt = 0.0
    merged: list[tuple[int, int, int]] = []
    i = 0
    while i < len(valid):
        a, b, sg = valid[i]
        j = i + 1
        while j < len(valid):
            na, nb, nsg = valid[j]
            if nsg != sg:
                break
            rest_dur = max(0.0, t[na] - t[b])
            if rest_dur <= 3 * dt:
                b = nb
                j += 1
                continue
            break
        merged.append((a, b, sg))
        i = j
    return [(a, b) for a, b, _ in merged]


def _make_segment(a: int, b: int, t: list[float], E: list[float], I: list[float], epsI: float, epsV: float) -> tuple[GcdSegment | None, bool]:
    I_seg = I[a : b + 1]
    E_seg = E[a : b + 1]
    t_seg = t[a : b + 1]
    iabs_med = median([abs(x) for x in I_seg])
    if iabs_med <= epsI:
        return None, True

    i_med = median(I_seg)
    delta = E_seg[-1] - E_seg[0]
    kind = "platform"
    sV = 0
    if abs(delta) >= epsV:
        sV = _sign(delta)
    else:
        if len(E_seg) >= 2:
            sV = _sign(median([E_seg[i + 1] - E_seg[i] for i in range(len(E_seg) - 1)]))
        kind = "platform"

    if sV > 0:
        kind = "charge"
    elif sV < 0:
        kind = "discharge"
    elif iabs_med > epsI:
        kind = "platform"

    warnings: list[str] = []
    sI = _sign(i_med)
    if sI != 0:
        oppose = sum(1 for x in I_seg if _sign(x, epsI) == -sI)
        if oppose / len(I_seg) > 0.05:
            warnings.append("段内电流符号不稳定")

    if len(E_seg) >= 3:
        ds = [_sign(E_seg[i + 1] - E_seg[i], 1e-12) for i in range(len(E_seg) - 1)]
        non_zero = [x for x in ds if x != 0]
        flips = sum(1 for i in range(1, len(non_zero)) if non_zero[i] != non_zero[i - 1])
        if non_zero and flips / max(1, len(non_zero) - 1) > 0.3:
            warnings.append("段内电压不单调")

    seg = GcdSegment(
        start=a,
        end=b,
        t_start=t_seg[0],
        t_end=t_seg[-1],
        I_med=i_med,
        E_start=E_seg[0],
        E_end=E_seg[-1],
        deltaE_end=delta,
        kind=kind,
        warnings=warnings,
    )
    return seg, False


def segment_one_cycle(t: list[float], E: list[float], I: list[float], Step: list[int] | None, V_start: float, V_end: float, J_label_A_per_g: float, m_active_g: float) -> GcdCycleSegments:
    if not (len(t) == len(E) == len(I)):
        raise ValueError("t/E/I length mismatch")
    if len(t) == 0:
        return GcdCycleSegments(cycle_k=0, segments=[], dropped_rest_count=0, warnings=[])

    t2, E2, I2, S2 = _sort_dedup_by_t(t, E, I, Step)
    epsI = max(1e-9, 1e-3 * abs(J_label_A_per_g * m_active_g))
    epsV = max(1e-3, 1e-3 * (V_end - V_start))

    if S2 is not None:
        candidates = _build_step_candidates(S2)
    else:
        candidates = _build_current_candidates(t2, I2, epsI)

    dropped = 0
    cycle_warnings: list[str] = []
    segs: list[GcdSegment] = []
    for a, b in candidates:
        seg, is_rest = _make_segment(a, b, t2, E2, I2, epsI, epsV)
        if is_rest:
            dropped += 1
            continue
        if seg is not None:
            segs.append(seg)

    platforms = [i for i, s in enumerate(segs) if s.kind == "platform"]
    for idx in reversed(platforms):
        prev_i = idx - 1
        while prev_i >= 0 and segs[prev_i].kind == "platform":
            prev_i -= 1
        next_i = idx + 1
        while next_i < len(segs) and segs[next_i].kind == "platform":
            next_i += 1
        if prev_i >= 0:
            segs[idx].kind = segs[prev_i].kind
        elif next_i < len(segs):
            segs[idx].kind = segs[next_i].kind
        else:
            cycle_warnings.append("充放电判定不稳")
            pos = [s for s in segs if s.I_med > 0]
            neg = [s for s in segs if s.I_med < 0]
            if pos or neg:
                score_pos = sum(1 for s in pos if s.deltaE_end > 0) + sum(1 for s in neg if s.deltaE_end < 0)
                score_neg = sum(1 for s in pos if s.deltaE_end < 0) + sum(1 for s in neg if s.deltaE_end > 0)
                if score_pos == score_neg:
                    cycle_warnings.append("正负映射不稳，默认正->charge 负->discharge")
                    map_pos_charge = True
                else:
                    map_pos_charge = score_pos > score_neg
                for s in segs:
                    if s.I_med > 0:
                        s.kind = "charge" if map_pos_charge else "discharge"
                    elif s.I_med < 0:
                        s.kind = "discharge" if map_pos_charge else "charge"
            break

    segs = [s for s in segs if s.kind in {"charge", "discharge", "platform"}]
    return GcdCycleSegments(cycle_k=0, segments=segs, dropped_rest_count=dropped, warnings=cycle_warnings)


def decide_main_order(cycle_segments: list[GcdCycleSegments]) -> BatteryMainOrder:
    first_kinds: list[str] = []
    for cyc in cycle_segments:
        if cyc.cycle_k < 2:
            continue
        first = next((s.kind for s in cyc.segments if s.kind in {"charge", "discharge"}), None)
        if first:
            first_kinds.append(first)

    chg = sum(1 for k in first_kinds if k == "charge")
    dis = sum(1 for k in first_kinds if k == "discharge")
    if chg > dis:
        return BatteryMainOrder(order="Charge→Discharge", decided_from="vote", warnings=[])
    if dis > chg:
        return BatteryMainOrder(order="Discharge→Charge", decided_from="vote", warnings=[])

    warnings = ["顺序不稳定"]
    cyc2 = next((c for c in cycle_segments if c.cycle_k == 2), None)
    first2 = None if cyc2 is None else next((s.kind for s in cyc2.segments if s.kind in {"charge", "discharge"}), None)
    if first2 == "discharge":
        order = "Discharge→Charge"
    else:
        order = "Charge→Discharge"
    return BatteryMainOrder(order=order, decided_from="cycle2_fallback", warnings=warnings)


def drop_first_cycle_reverse_segment(cycle1: GcdCycleSegments, main_order: BatteryMainOrder) -> GcdCycleSegments:
    if cycle1.cycle_k != 1 or not cycle1.segments:
        return cycle1
    first_kind = "charge" if main_order.order.startswith("Charge") else "discharge"
    first_seg = cycle1.segments[0]
    t0 = min(s.t_start for s in cycle1.segments)
    t_last = max(s.t_end for s in cycle1.segments)
    if first_seg.kind != first_kind and first_seg.start == 0 and first_seg.t_end <= t0 + 0.3 * (t_last - t0):
        new_segs = cycle1.segments[1:]
        return GcdCycleSegments(
            cycle_k=cycle1.cycle_k,
            segments=new_segs,
            dropped_rest_count=cycle1.dropped_rest_count,
            warnings=[*cycle1.warnings, "首圈反向段已剔除"],
        )
    return cycle1

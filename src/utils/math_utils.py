import numpy as np
from decimal import Decimal, getcontext, ROUND_DOWN

# повысим точность Decimal для финансовых расчётов
getcontext().prec = 42


def compute_quantiles(arr, low, high):
    a = np.asarray(arr, dtype=float)
    q_low = float(np.nanquantile(a, low))
    q_high = float(np.nanquantile(a, high))
    return q_low, q_high


def log_diff_series(b_close, y_close):
    import numpy as _np
    b = _np.asarray(b_close, dtype=float)
    y = _np.asarray(y_close, dtype=float)
    return _np.log(b) - _np.log(y)


def decimal_floor_to_step(value, step):
    """
    Точное округление вниз (floor) до ближайшего количества, кратного step.

    Работает с Decimal и избегает преобразований через float.
    Если step == 0 — возвращается исходное значение как float.
    """
    try:
        v = Decimal(str(value))
        s = Decimal(str(step))
    except Exception:
        return float(value)
    if s == 0:
        return float(v)
    # количество шагов вниз
    try:
        qty_steps = (v / s).quantize(Decimal('1'), rounding=ROUND_DOWN)
        res = (qty_steps * s).normalize()
        return float(res)
    except Exception:
        # fallback
        try:
            qty = (v // s) * s
            return float(qty)
        except Exception:
            return float(v)


def normalize_ts(ts):
    """
    Нормализовать метку времени в миллисекунды.

    Вход может быть в секундах (10^9) или миллисекундах (10^12+).
    """
    t = int(ts)
    if t < 1_000_000_000_000:
        return int(t * 1000)
    return int(t)

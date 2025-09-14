import pytest
from src.utils.math_utils import compute_quantiles


def test_quantiles_simple():
    arr = [1,2,3,4,5]
    ql, qh = compute_quantiles(arr, 0.2, 0.8)
    assert ql == pytest.approx(1.8)
    assert qh == pytest.approx(4.2)

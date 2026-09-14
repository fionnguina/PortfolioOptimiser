"""COM handle release — the 0x800706ba teardown faults.

The engine's live pipeline runs at MODULE scope, so xlwings/COM handles stay
bound in globals long after their `with xw.App(...)` block exits. The
interpreter then finalises those proxies at shutdown, against an Excel server
that is already gone, and each one emits a `Windows fatal exception:
0x800706ba` block. They land AFTER the health summary, so the engine cannot
count them and they never reach run.log — only the wrapper's post-run scan
sees them.

The first fix listed four names by hand. It caught three handles and left 22 of
26 faults, because `opt`, `co`, `chart_obj`, the `.api` Range proxies and two
`WScript.Shell` dispatches were never in the list. Matching on TYPE is what
makes this survive the next sheet someone adds.
"""
from __future__ import annotations

import pytest

from conftest import extract_funcs


@pytest.fixture(scope="module")
def eng():
    return extract_funcs("_is_com_handle")


def _fake(module: str, name: str):
    """An object whose TYPE reports the given module/name — which is all the
    detector is allowed to look at."""
    return type(name, (), {"__module__": module})()


class _Exploding:
    """A dead COM proxy: touching any attribute raises, exactly as 0x800706ba
    does. Detection must therefore never dereference the object."""
    __module__ = "xlwings.main"
    __qualname__ = "Book"

    def __getattr__(self, item):
        raise OSError("RPC server unavailable")


_Exploding.__name__ = "Book"


def test_com_owning_handles_are_detected(eng):
    f = eng["_is_com_handle"]
    assert f(_fake("xlwings.main", "Book"))
    assert f(_fake("xlwings.main", "App"))
    assert f(_fake("xlwings.main", "Range"))
    assert f(_fake("xlwings.main", "Chart"))
    assert f(_fake("xlwings._xlwindows", "Anything"))   # the COM impl layer
    assert f(_fake("win32com.client.dynamic", "CDispatch"))
    assert f(_fake("pythoncom", "PyIDispatch"))


def test_xlwings_helper_classes_are_NOT_com(eng):
    """The regression that made the first sweep useless.

    Matching the whole xlwings package reported 563 "handles" of which 550
    were these — plain Python helpers owning no Excel interface. Dropping them
    inflated the released count and told us nothing about the fault total.
    """
    f = eng["_is_com_handle"]
    for n in ("ObjectHandleIcons", "Engine", "Engines", "VerticalExpander",
              "HorizontalExpander", "TableExpander", "Converter"):
        assert not f(_fake("xlwings.utils", n)), n
        assert not f(_fake("xlwings.main", n)), n


def test_plain_values_are_not_touched(eng):
    """`.api.Left` returns a float; dropping ordinary globals would be a bug."""
    f = eng["_is_com_handle"]
    for v in (1.0, 0, "sheet", None, [1, 2], {"a": 1}, (3, 4), True):
        assert not f(v), v


def test_detection_never_dereferences_the_object(eng):
    """A dead proxy raises on attribute access — the very fault being fixed."""
    f = eng["_is_com_handle"]
    assert f(_Exploding()) is True


def test_pandas_and_numpy_are_not_com(eng):
    import pandas as pd
    import numpy as np
    f = eng["_is_com_handle"]
    assert not f(pd.DataFrame({"a": [1]}))
    assert not f(pd.Series([1.0]))
    assert not f(np.float64(1.0))

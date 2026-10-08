"""brrrrr: SQL for tick data, over files, object stores and DataFrames.

    import brrrrr

    bars = brrrrr.sql('''
        SELECT time_bucket('1m', ts) AS minute, symbol,
               first(price, ts) AS open, max(price) AS high, min(price) AS low,
               last(price, ts) AS close, sum(size) AS volume
        FROM 's3://my-bucket/trades/*.parquet'
        GROUP BY minute, symbol
    ''')
    bars.pl()        # a Polars DataFrame (or .df() for pandas, .arrow() for pyarrow)

A DataFrame in a variable is a table by its name: ``brrrrr.sql("SELECT * FROM trades")``
finds ``trades`` in the caller's scope (a Polars or pandas DataFrame, a pyarrow Table).
"""
from __future__ import annotations

import inspect
import re
from typing import Any, Optional

from ._brrrrr import Connection as _Connection
from ._brrrrr import Error, Result, __version__

__all__ = ["Connection", "Error", "Result", "connect", "sql", "__version__"]

_NO_TABLE = re.compile(r"no table ([A-Za-z_][A-Za-z0-9_]*)")


def _arrow_of(obj: Any) -> Any:
    """`obj` as something with __arrow_c_stream__ (a pyarrow Table for older pandas), or None."""
    if hasattr(obj, "__arrow_c_stream__"):
        return obj
    try:  # pandas < 2.2: through pyarrow
        import pandas

        if isinstance(obj, pandas.DataFrame):
            import pyarrow

            return pyarrow.Table.from_pandas(obj, preserve_index=False)
    except ImportError:
        pass
    return None


class Connection:
    """A session: tables named over files, object stores and DataFrames, and views.

    ``threads`` caps the threads a query runs on (by default, every CPU's).
    """

    def __init__(self, threads: Optional[int] = None) -> None:
        self._c = _Connection(threads)

    def sql(self, query: str, _frame: Any = None) -> Result:
        """Runs a statement (or several, ``;``-separated): the last one's result.

        A table the session does not know is looked up in the caller's variables: a DataFrame
        or Arrow table there is queried by its name.
        """
        frame = _frame or inspect.currentframe().f_back  # type: ignore[union-attr]
        return self._run(query, lambda name: _lookup(frame, name))

    def _run(self, query: str, find: Any) -> Result:
        """Runs ``query``; a table it does not know is ``find(name)``: Arrow data, or None."""
        borrowed = []
        try:
            for _ in range(32):
                try:
                    return self._c.execute(query)
                except Error as e:
                    m = _NO_TABLE.search(str(e))
                    name = m and m.group(1)
                    data = name and find(name)
                    if data is None or name in borrowed:
                        raise
                    self._c.register_arrow(name, data)
                    borrowed.append(name)
            raise Error("too many DataFrames looked up")
        finally:
            for name in borrowed:
                self._c.unregister(name)

    execute = sql

    def register(self, name: str, data: Any) -> "Connection":
        """Names a table: a path, glob, directory or URL (``'s3://b/trades/'``), or a DataFrame
        or Arrow table (held in memory)."""
        if isinstance(data, str):
            self._c.register_location(name, data)
        else:
            arrow = _arrow_of(data)
            if arrow is None:
                raise TypeError(f"{type(data).__name__}: register a path, a pyarrow Table, or a Polars or pandas DataFrame")
            self._c.register_arrow(name, arrow)
        return self

    def unregister(self, name: str) -> None:
        self._c.unregister(name)

    def close(self) -> None:
        pass

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


def load_ipython_extension(ipython: Any) -> None:
    """``%load_ext brrrrr``: the ``%sql`` and ``%%sql`` magics (``brrrrr.magic``)."""
    from .magic import load

    load(ipython)


def _lookup(frame: Any, name: str) -> Any:
    while frame is not None:
        for scope in (frame.f_locals, frame.f_globals):
            if name in scope:
                found = _arrow_of(scope[name])
                if found is not None:
                    return found
        frame = frame.f_back
    return None


_default: Optional[Connection] = None


def connect(threads: Optional[int] = None) -> Connection:
    """A new session."""
    return Connection(threads)


def sql(query: str) -> Result:
    """Runs a statement on the default session: ``brrrrr.sql("FROM 'trades.parquet' LIMIT 5")``."""
    global _default
    if _default is None:
        _default = Connection()
    return _default.sql(query, _frame=inspect.currentframe().f_back)  # type: ignore[union-attr]


def _need(module: str, extra: str) -> Any:
    """``module``, or an ImportError saying how to install it."""
    try:
        return __import__(module)
    except ImportError as e:
        raise ImportError(f"{module} is not installed: pip install {module} (or pip install 'brrrrr[{extra}]')") from e


def _arrow(self: Result) -> Any:
    """The result as a pyarrow Table."""
    return _need("pyarrow", "arrow").table(self)


def _pl(self: Result) -> Any:
    """The result as a Polars DataFrame."""
    return _need("polars", "polars").DataFrame(self)


def _df(self: Result) -> Any:
    """The result as a pandas DataFrame."""
    _need("pandas", "pandas")
    return _arrow(self).to_pandas()


def _show(self: Result, max_rows: int = 40) -> None:
    """Prints the result as a table."""
    print(self.table(max_rows))


def _html(self: Result, max_rows: int = 20) -> str:
    """The result as an HTML table, for notebooks: the first rows, and how many there are."""
    import html

    if self.message and not self.columns:
        return f"<pre>{html.escape(self.message)}</pre>"
    head = "".join(f"<th>{html.escape(c)}</th>" for c in self.columns)
    rows = self.fetchall()
    body = "".join(
        "<tr>" + "".join(f"<td>{'' if v is None else html.escape(str(v))}</td>" for v in r) + "</tr>"
        for r in rows[:max_rows]
    )
    more = f" ({max_rows} shown)" if len(rows) > max_rows else ""
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table><p>{len(rows)} rows{more}</p>"


Result._repr_html_ = _html  # type: ignore[attr-defined]
Result.arrow = _arrow  # type: ignore[attr-defined]
Result.pl = _pl  # type: ignore[attr-defined]
Result.df = _df  # type: ignore[attr-defined]
Result.show = _show  # type: ignore[attr-defined]

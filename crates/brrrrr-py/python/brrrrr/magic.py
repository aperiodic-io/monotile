"""``%sql`` and ``%%sql`` in IPython and Jupyter, after ``%load_ext brrrrr``.

    %%sql -o bars
    SELECT time_bucket('1m', ts) AS minute, symbol, last(price, ts) AS close
    FROM trades GROUP BY minute, symbol

A DataFrame in the notebook is a table by its name; the answer shows as a table, and ``-o name``
also keeps it in the notebook as a DataFrame (Polars if installed, else pandas). One session per
notebook: its views and named tables last from cell to cell.
"""
from __future__ import annotations

import sys
from typing import Any, Optional

from IPython.core.magic import Magics, line_cell_magic, magics_class
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring

from . import Connection, Error, _arrow_of


def frame_of(r: Any) -> Any:
    """A result as a Polars DataFrame, or pandas without Polars."""
    try:
        import polars  # noqa: F401
    except ImportError:
        return r.df()
    return r.pl()


@magics_class
class SqlMagics(Magics):
    def __init__(self, shell: Any) -> None:
        super().__init__(shell)
        self.con = Connection()

    @magic_arguments()
    @argument("-o", "--out", metavar="NAME", help="keep the answer in the notebook as a DataFrame named NAME")
    @argument("query", nargs="*", help="the SQL (or the cell's, with %%sql)")
    @line_cell_magic
    def sql(self, line: str, cell: Optional[str] = None) -> Any:
        """Runs brrrrr SQL; the notebook's DataFrames are tables by their names."""
        args = parse_argstring(self.sql, line)
        query = cell if cell is not None else " ".join(args.query)
        if not query.strip():
            print("%sql SELECT ...  or a %%sql cell", file=sys.stderr)
            return None
        ns = self.shell.user_ns
        try:
            r = self.con._run(query, lambda name: _arrow_of(ns[name]) if name in ns else None)
        except Error as e:
            # the reason, not a traceback through the magic
            print(f"error: {e}", file=sys.stderr)
            return None
        if args.out:
            ns[args.out] = frame_of(r)
        return r


def load(ipython: Any) -> None:
    ipython.register_magics(SqlMagics(ipython))

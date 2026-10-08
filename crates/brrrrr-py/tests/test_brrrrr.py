"""brrrrr from Python: SQL over files and DataFrames, results to pyarrow, Polars and pandas."""
import datetime
import os

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

import brrrrr

TRADES = """ts,symbol,price,size
2024-01-01T00:00:10,A,100,1
2024-01-01T00:00:20,B,50,2
2024-01-01T00:00:30,A,101,2
2024-01-01T00:01:05,A,99,1
"""


@pytest.fixture()
def trades_csv(tmp_path):
    p = tmp_path / "trades.csv"
    p.write_text(TRADES)
    return str(p)


def test_sql_over_a_file_gives_rows(trades_csv):
    r = brrrrr.sql(f"SELECT symbol, count(*) AS n, sum(size) AS v FROM '{trades_csv}' GROUP BY symbol ORDER BY symbol")
    assert r.columns == ["symbol", "n", "v"]
    assert r.fetchall() == [("A", 3, 4), ("B", 1, 2)]
    assert len(r) == 2
    assert "symbol" in repr(r)


def test_results_go_to_pyarrow_polars_and_pandas(trades_csv):
    r = brrrrr.sql(f"SELECT time_bucket('1m', ts) AS m, symbol, last(price, ts) AS close FROM '{trades_csv}' GROUP BY m, symbol ORDER BY m, symbol")
    t = r.arrow()
    assert isinstance(t, pa.Table) and t.num_rows == 3
    assert t.schema.field("m").type == pa.timestamp("us", tz="UTC")
    df = r.pl()
    assert isinstance(df, pl.DataFrame) and df["close"].to_list() == [101, 50, 99]
    p = r.df()
    assert isinstance(p, pd.DataFrame) and list(p["symbol"]) == ["A", "B", "A"]
    # any Arrow consumer, without pyarrow in between
    assert pl.from_arrow(t).shape == (3, 3)


def test_times_come_back_as_aware_datetimes(trades_csv):
    (ts,) = brrrrr.sql(f"SELECT max(ts) AS t FROM '{trades_csv}'").fetchone()
    assert ts == datetime.datetime(2024, 1, 1, 0, 1, 5, tzinfo=datetime.timezone.utc)


def test_dataframes_in_variables_are_tables():
    trades = pl.DataFrame({"symbol": ["A", "B", "A"], "price": [1.0, 2.0, 3.0]})
    assert brrrrr.sql("SELECT symbol, sum(price) AS s FROM trades GROUP BY symbol ORDER BY symbol").fetchall() == [("A", 4.0), ("B", 2.0)]
    quotes = pd.DataFrame({"symbol": ["A"], "bid": [0.5]})
    r = brrrrr.sql("SELECT t.symbol, q.bid FROM trades t JOIN quotes q ON t.symbol = q.symbol ORDER BY t.price")
    assert r.fetchall() == [("A", 0.5), ("A", 0.5)]
    numbers = pa.table({"x": [1, 2, 3]})
    assert brrrrr.sql("SELECT sum(x) AS s FROM numbers").fetchone() == (6,)


def test_a_connection_keeps_its_tables_and_views(trades_csv):
    con = brrrrr.connect(threads=2)
    con.register("trades", trades_csv)
    con.sql("CREATE VIEW a AS SELECT * FROM trades WHERE symbol = 'A'")
    assert con.sql("SELECT count(*) AS n FROM a").fetchone() == (3,)
    con.register("extra", pa.table({"symbol": ["A"], "venue": ["X"]}))
    assert con.sql("SELECT e.venue, count(*) AS n FROM trades t JOIN extra e ON t.symbol = e.symbol GROUP BY e.venue").fetchall() == [("X", 3)]
    names = [row[0] for row in con.sql("SHOW TABLES").fetchall()]
    assert names == ["trades", "extra", "a"]


def test_errors_say_what_to_do(trades_csv):
    with pytest.raises(brrrrr.Error, match="unknown column nope"):
        brrrrr.sql(f"SELECT nope FROM '{trades_csv}'")
    with pytest.raises(brrrrr.Error, match="no table not_a_variable"):
        brrrrr.sql("SELECT * FROM not_a_variable")


def test_write_and_copy(tmp_path, trades_csv):
    out = str(tmp_path / "out.parquet")
    brrrrr.sql(f"SELECT * FROM '{trades_csv}'").write(out)
    assert pa.parquet.read_table(out).num_rows == 4 if hasattr(pa, "parquet") else os.path.exists(out)
    r = brrrrr.sql(f"COPY (FROM '{trades_csv}') TO '{tmp_path / 'c.csv'}'")
    assert r.message == f"4 rows written to {tmp_path / 'c.csv'}"
    assert brrrrr.sql(f"SELECT count(*) FROM '{tmp_path / 'c.csv'}'").fetchone() == (4,)


def test_a_result_shows_as_html_in_a_notebook(trades_csv):
    r = brrrrr.sql(f"SELECT symbol, count(*) AS n FROM '{trades_csv}' GROUP BY symbol ORDER BY symbol")
    html = r._repr_html_()
    assert html.startswith("<table><thead><tr><th>symbol</th><th>n</th></tr>") and "rows" in html


def test_the_sql_magic_runs_cells_over_the_notebooks_dataframes():
    shell_module = pytest.importorskip("IPython.core.interactiveshell")
    ip = shell_module.InteractiveShell.instance()
    ip.run_line_magic("load_ext", "brrrrr")
    ip.user_ns["trades"] = pl.DataFrame({"symbol": ["A", "B", "A"], "price": [1.0, 2.0, 3.0]})
    r = ip.run_cell_magic("sql", "-o totals", "SELECT symbol, sum(price) AS total FROM trades GROUP BY symbol ORDER BY symbol")
    assert r.fetchall() == [("A", 4.0), ("B", 2.0)]
    assert "<table>" in r._repr_html_()
    assert isinstance(ip.user_ns["totals"], pl.DataFrame) and ip.user_ns["totals"]["total"].to_list() == [4.0, 2.0]
    # a line, a view that lasts to the next cell, a pandas frame, and an error's reason
    ip.user_ns["quotes"] = pd.DataFrame({"symbol": ["A"], "bid": [0.5]})
    ip.run_line_magic("sql", "CREATE VIEW bids AS SELECT symbol, bid FROM quotes")
    assert ip.run_line_magic("sql", "SELECT count(*) AS n FROM bids").fetchall() == [(1,)]
    assert ip.run_cell_magic("sql", "", "SELECT nope FROM trades") is None

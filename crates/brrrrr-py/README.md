# brrrrr for Python

SQL for tick data: query Parquet, CSV and JSON on disk or in S3, GCS and Azure, and DataFrames in
memory. Bars, as-of joins, VWAP, rolling windows; results straight to Polars, pandas and pyarrow.

```bash
pip install brrrrr              # results as tables and Python rows
pip install 'brrrrr[polars]'    # and as Polars DataFrames; [pandas] and [arrow] for those
```

```python
import brrrrr

bars = brrrrr.sql("""
    SELECT time_bucket('1m', ts) AS minute, symbol,
           first(price, ts) AS open, max(price) AS high, min(price) AS low,
           last(price, ts) AS close, sum(size) AS volume
    FROM 's3://my-bucket/trades/*.parquet'
    GROUP BY minute, symbol
""").pl()
```

A DataFrame in a variable is a table by its name:

```python
import polars as pl
trades = pl.read_parquet("trades.parquet")
quotes = pl.read_parquet("quotes.parquet")
brrrrr.sql("""
    SELECT t.ts, t.symbol, t.price, q.bid, q.ask
    FROM trades t ASOF JOIN quotes q ON t.symbol = q.symbol AND t.ts >= q.ts
""").df()
```

In Jupyter, `%load_ext brrrrr`, then `%%sql` cells. On the command line, `brrrrr-sql` opens a
shell, or runs one statement: `brrrrr-sql "FROM 'trades.csv' LIMIT 5"`.

Documentation: https://aperiodic-io.github.io/monotile/docs/python.html

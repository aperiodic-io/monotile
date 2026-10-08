"""The queries of queries.sql as a Polars user writes them: lazy scans of the Parquet files,
`group_by_dynamic` for time buckets, `join_asof` for as-of joins, `shift().over()` for lags.

    python3 polars_queries.py NAME DATA_DIR      (POLARS_MAX_THREADS sets the threads)

Prints the answer's row count, as bench.py checks it against brrrrr's.
"""
import sys

import polars as pl

name, d = sys.argv[1], sys.argv[2]
# the files are in time order, as gen.py writes them: said once, so that windows need no sort
trades = pl.scan_parquet(f"{d}/trades.parquet").with_columns(pl.col("ts").set_sorted())
quotes = pl.scan_parquet(f"{d}/quotes.parquet").with_columns(pl.col("ts").set_sorted())
mid = (pl.col("bid") + pl.col("ask")) / 2


def asof():
    # the last quote of the trade's symbol at or before it; trades with none are left out (ASOF JOIN)
    return trades.join_asof(quotes, on="ts", by="symbol", strategy="backward").drop_nulls("bid")


queries = {
    "count": lambda: trades.select(n=pl.len()),
    "per_symbol": lambda: trades.group_by("symbol")
    .agg(n=pl.len(), volume=pl.col("size").sum(), avg_price=pl.col("price").mean(), high=pl.col("price").max())
    .sort("volume", descending=True)
    .head(10),
    "ohlcv_1m": lambda: trades.group_by_dynamic("ts", every="1m", group_by="symbol")
    .agg(
        open=pl.col("price").first(),
        high=pl.col("price").max(),
        low=pl.col("price").min(),
        close=pl.col("price").last(),
        volume=pl.col("size").sum(),
    )
    .sort("ts", "symbol")
    .head(5),
    "vwap_1h": lambda: trades.group_by_dynamic("ts", every="1h", group_by="symbol")
    .agg(vwap=(pl.col("price") * pl.col("size")).sum() / pl.col("size").sum())
    .sort("ts", "symbol")
    .head(5),
    "filter_buys": lambda: trades.filter((pl.col("side") == "buy") & (pl.col("size") > 10))
    .group_by("symbol")
    .agg(n=pl.len())
    .sort("n", descending=True)
    .head(5),
    "spread_bps": lambda: quotes.group_by("symbol")
    .agg(spread_bps=((pl.col("ask") - pl.col("bid")) / mid).mean() * 10000)
    .sort("spread_bps", descending=True)
    .head(5),
    "asof_trades_quotes": lambda: asof().select(n=pl.len(), avg_vs_mid=(pl.col("price") - mid).mean()),
    "tca_per_symbol": lambda: asof()
    .group_by("symbol")
    .agg(n=pl.len(), avg_vs_mid=(pl.col("price") - mid).mean())
    .sort("n", descending=True)
    .head(5),
    "rolling_return": lambda: trades.with_columns(r=pl.col("price") / pl.col("price").shift(1).over("symbol") - 1)
    .group_by("symbol")
    .agg(mean_return=pl.col("r").mean())
    .sort("symbol")
    .head(5),
}
print(queries[name]().collect().height)

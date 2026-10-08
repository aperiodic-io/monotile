# brrrrr sql against DuckDB, Polars and ClickHouse

The ad-hoc queries of `queries.sql` over a synthetic day of market data, each run as a user runs
it: a fresh process per query, timed whole, with its peak memory.

| engine | how a query runs | the queries |
| --- | --- | --- |
| brrrrr | `brrrrr sql "..."` | `queries.sql` |
| DuckDB | `python -c "duckdb.sql(...)"` | `queries.sql`, with DuckDB's spellings (`bench.py`, `duck_sql`) |
| Polars | `python polars_queries.py NAME DIR`: lazy scans, `group_by_dynamic`, `join_asof`, `shift().over()` | `polars_queries.py` |
| ClickHouse | `clickhouse local --query "..."` over the Parquet files | `clickhouse_queries.sql`: `toStartOfMinute`, `argMin`/`argMax`, `ASOF JOIN`, `lagInFrame` |

Each engine's answer is checked to have brrrrr's row count. Every engine gets the same threads
(`--threads`).

## Input
`gen.py`: one day of 10M trades and 20M quotes over 100 symbols, activity skewed (a few symbols
trade most), in time order, Parquet with zstd: `trades.parquet` (ts, symbol, price, size, side)
and `quotes.parquet` (ts, symbol, bid, ask, bid_size, ask_size).

```bash
docker build -t brrrrr-bench-py bench/sql
docker run --rm -v $PWD/target-bench:/data -v $PWD/bench/sql:/b brrrrr-bench-py python3 /b/gen.py /data
cargo build --release -p brrrrr
docker run --rm --cpus 8 -v $PWD/target-bench:/data -v $PWD/bench/sql:/b -v $PWD/target/release:/bin2:ro \
  brrrrr-bench-py python3 /b/bench.py /bin2/brrrrr /data --threads 8 --reps 3 --json /b/results.json
```

`--engines brrrrr,duckdb` runs a subset; `--only ohlcv_1m` one query. The image pins DuckDB, Polars
and ClickHouse (`Dockerfile`).

`results.json` feeds the website's benchmarks page (`website/build.py`).

## Results
brrrrr against DuckDB (Polars and ClickHouse were added since: their run is to come). 16 vCPU AMD
EPYC Rome at 2.0 GHz, 8 CPUs to each process, files in the page cache, DuckDB 1.5.5, medians of
three:

| query | brrrrr s | DuckDB s | speed-up | brrrrr MiB | DuckDB MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| count | 0.112 | 0.215 | 1.91x | 22 | 53 |
| per_symbol | 0.349 | 0.333 | 0.95x | 310 | 102 |
| ohlcv_1m | 0.528 | 0.827 | 1.57x | 388 | 127 |
| vwap_1h | 0.389 | 0.646 | 1.66x | 366 | 113 |
| filter_buys | 0.321 | 0.247 | 0.77x | 296 | 76 |
| spread_bps | 0.527 | 0.393 | 0.75x | 425 | 126 |
| asof_trades_quotes | 3.200 | 1.922 | 0.60x | 830 | 3037 |
| tca_per_symbol | 1.657 | 2.029 | 1.22x | 1381 | 2979 |
| rolling_return | 0.742 | 0.807 | 1.09x | 318 | 814 |

- **Faster:** time buckets (bars, VWAP) stream through the table in time order; an as-of join
  grouped per symbol runs per symbol side by side, in half DuckDB's memory.
- **Slower, and more memory:** plain group-bys, filters and arithmetic over every row
  (`per_symbol`, `filter_buys`, `spread_bps`): the rows are read into the streaming engine's
  form, where DuckDB aggregates its vectors in place.
- **Slower:** a global aggregate after an as-of join (`asof_trades_quotes`), which cannot be split
  by symbol.

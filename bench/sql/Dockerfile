# DuckDB, Polars and pyarrow (the data, and the same queries in DuckDB and Polars), and
# ClickHouse's single binary (clickhouse-local) from its official image
#   docker build -t brrrrr-bench-py bench/sql
FROM clickhouse/clickhouse-server:25.8 AS clickhouse
FROM python:3.14-slim
RUN pip install --no-cache-dir duckdb==1.5.5 polars==1.34.0 pyarrow numpy pytz
COPY --from=clickhouse /usr/bin/clickhouse /usr/local/bin/clickhouse

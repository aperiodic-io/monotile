"""``brrrrr-sql``: an SQL shell over files and object stores, or one statement from the command
line (``brrrrr-sql "FROM 'trades.parquet' LIMIT 5"``). The native ``brrrrr`` binary has the
same SQL and more (the live engine); this one comes with ``pip install brrrrr``."""
import sys
import time

from . import Connection, Error, __version__


def main() -> int:
    args = sys.argv[1:]
    fmt = "table"
    if "--csv" in args:
        args.remove("--csv")
        fmt = "csv"
    if args and args[0] in ("-h", "--help"):
        print('brrrrr-sql ["SQL" | -f FILE] [--csv]: runs the statements, or with neither opens the shell')
        return 0
    if len(args) == 2 and args[0] in ("-f", "--file"):
        try:
            args = [open(args[1]).read()]
        except OSError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
    con = Connection()
    if args:
        try:
            r = con.sql(" ".join(args))
        except Error as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        if r.message:
            print(r.message, file=sys.stderr)
        elif fmt == "csv":
            import csv

            w = csv.writer(sys.stdout)
            w.writerow(r.columns)
            w.writerows(r.fetchall())
        else:
            print(r.table())
        return 0
    _line_editing(con)
    print(f"brrrrr {__version__} · SQL over files, object stores and DataFrames · .help for help, Ctrl-D to leave")
    buf = ""
    while True:
        try:
            line = input("brrrrr> " if not buf else "   ...> ")
        except EOFError:
            print()
            return 0
        except KeyboardInterrupt:
            print()
            buf = ""
            continue
        if not buf and line.strip().startswith("."):
            cmd = line.split()
            if cmd[0] in (".quit", ".exit", ".q"):
                return 0
            if cmd[0] == ".tables":
                line = "SHOW TABLES;"
            elif cmd[0] == ".read" and len(cmd) > 1:
                try:
                    line = open(cmd[1]).read()
                except OSError as e:
                    print(f"error: {e}", file=sys.stderr)
                    continue
            else:
                print(HELP)
                continue
        buf += line + "\n"
        if not line.rstrip().endswith(";"):
            continue
        start = time.perf_counter()
        try:
            r = con.sql(buf)
            print(r.message if r.message and not r.columns else r.table())
            print(f"({time.perf_counter() - start:.3f} s)", file=sys.stderr)
        except Error as e:
            print(f"error: {e}", file=sys.stderr)
        except KeyboardInterrupt:
            print("canceled", file=sys.stderr)
        buf = ""


HELP = """A statement ends with ;  and may span lines. Tab completes keywords, functions and tables.
  FROM 'trades.parquet' LIMIT 5;
  SELECT time_bucket('1m', ts) AS minute, symbol, last(price, ts) AS close
  FROM 'trades.parquet' GROUP BY minute, symbol;
  SELECT t.ts, t.price, q.bid, q.ask
  FROM 'trades.parquet' t ASOF JOIN 'quotes.parquet' q ON t.symbol = q.symbol AND t.ts >= q.ts;
  CREATE VIEW bars AS SELECT ...;   SHOW TABLES;   DESCRIBE 'trades.parquet';
Commands: .tables  .read FILE  .help  .quit      More: https://aperiodic-io.github.io/monotile/docs/"""

WORDS = """SELECT FROM WHERE GROUP BY ORDER HAVING LIMIT AS ON AND OR NOT IN IS NULL LIKE BETWEEN CASE WHEN
THEN ELSE END JOIN LEFT ASOF UNION ALL DISTINCT WITH OVER PARTITION INTERVAL DESC ASC CREATE VIEW TABLE
DROP COPY TO DESCRIBE SHOW TABLES EXPLAIN""".split()
FUNCTIONS = """avg count count_if first last max min sum median stddev vwap twap time_bucket date_trunc
to_timestamp epoch_ms lag lead row_number rank dense_rank percent_rank cume_dist ntile arg_max arg_min quantile_cont coalesce round abs ln sqrt
read_csv read_parquet read_json""".split()


def _line_editing(con: Connection) -> None:
    """Line editing, history and Tab completion of keywords, functions and the session's tables,
    where readline is (not on Windows' plain Python)."""
    try:
        import readline
    except ImportError:
        return

    def complete(text: str, state: int):
        if state == 0:
            try:
                tables = [r[0] for r in con.sql("SHOW TABLES").fetchall()]
            except Error:
                tables = []
            upper = text != text.lower() or not text
            words = [w if upper else w.lower() for w in WORDS] + [f + "(" for f in FUNCTIONS] + tables
            complete.found = [w for w in words if w.lower().startswith(text.lower())]
        return complete.found[state] if state < len(complete.found) else None

    readline.set_completer(complete)
    readline.set_completer_delims(" \t\n,()=<>;")
    readline.parse_and_bind("bind ^I rl_complete" if "libedit" in (readline.__doc__ or "") else "tab: complete")


if __name__ == "__main__":
    sys.exit(main())

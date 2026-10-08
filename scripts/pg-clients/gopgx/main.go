package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
)

func main() {
	ctx := context.Background()
	dsn := "postgres://brrrrr@127.0.0.1:5433/brrrrr"
	if len(os.Args) > 1 {
		dsn = "postgres://brrrrr:" + os.Args[1] + "@127.0.0.1:5433/brrrrr"
	}
	check := func(name string, f func() (any, error)) {
		v, err := f()
		if err != nil {
			fmt.Printf("FAIL | %s | %v\n", name, err)
		} else {
			s := fmt.Sprintf("%#v", v)
			if len(s) > 200 {
				s = s[:200]
			}
			fmt.Printf("ok | %s | %s\n", name, s)
		}
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		fmt.Println("FAIL | connect |", err)
		return
	}
	rows := func(sql string, args ...any) func() (any, error) {
		return func() (any, error) {
			r, err := conn.Query(ctx, sql, args...)
			if err != nil {
				return nil, err
			}
			defer r.Close()
			var out [][]any
			for r.Next() {
				v, err := r.Values()
				if err != nil {
					return nil, err
				}
				out = append(out, v)
			}
			return out, r.Err()
		}
	}
	check("simple", rows("SELECT * FROM bars ORDER BY minute, symbol LIMIT 2"))
	check("types", func() (any, error) {
		var m time.Time
		var s string
		var o float64
		var v int64
		err := conn.QueryRow(ctx, "SELECT minute, symbol, open, volume FROM bars LIMIT 1").Scan(&m, &s, &o, &v)
		return []any{m, s, o, v}, err
	})
	check("param text", rows("SELECT count(*) FROM trades WHERE symbol = $1", "BTC"))
	check("param int", rows("SELECT symbol, price FROM trades WHERE size > $1 LIMIT 2", 0))
	check("param float", rows("SELECT count(*) FROM trades WHERE price > $1", 101.5))
	check("param timestamp", rows("SELECT count(*) FROM trades WHERE ts >= $1", time.Date(2024, 1, 2, 9, 35, 0, 0, time.UTC)))
	check("null", rows("SELECT NULL AS n, 1 AS one"))
	check("error", rows("SELECT nope FROM trades"))
	check("after error", rows("SELECT 1"))
	check("simple protocol", func() (any, error) {
		return rows("SELECT count(*) FROM trades WHERE symbol = $1", pgx.QueryExecModeSimpleProtocol, "ETH")()
	})
	for _, m := range []pgx.QueryExecMode{pgx.QueryExecModeExec, pgx.QueryExecModeSimpleProtocol} {
		check(fmt.Sprint(m, " text"), rows("SELECT count(*) FROM trades WHERE symbol = $1", m, "BTC"))
		check(fmt.Sprint(m, " int"), rows("SELECT symbol, price FROM trades WHERE size > $1 LIMIT 2", m, 0))
		check(fmt.Sprint(m, " float"), rows("SELECT count(*) FROM trades WHERE price > $1", m, 101.5))
		check(fmt.Sprint(m, " timestamp"), rows("SELECT count(*) FROM trades WHERE ts >= $1", m, time.Date(2024, 1, 2, 9, 35, 0, 0, time.UTC)))
		check(fmt.Sprint(m, " result types"), rows("SELECT minute, symbol, open, volume FROM bars WHERE symbol = $1 LIMIT 1", m, "BTC"))
	}
	check("exec mode describe", func() (any, error) {
		return rows("SELECT count(*) FROM trades WHERE price > $1", pgx.QueryExecModeDescribeExec, 101.5)()
	})
}

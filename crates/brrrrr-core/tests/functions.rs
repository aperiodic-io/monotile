//! Scalar expressions outside the aggregates: `pow` outside the range ported from musl, negative
//! literals, qualified columns, three-valued comparisons, `null_if` and `format_datetime`.
use brrrrr_core::expr::{Compiler, Scope};
use brrrrr_core::sql::parse_expr;
use brrrrr_core::value::Value;

#[test]
fn pow_outside_the_ported_range_uses_the_host_libm() {
    use brrrrr_core::pow::pow;
    for (x, y) in [
        (0.0, 1.5),
        (-2.0, 2.0),
        (f64::INFINITY, 1.5),
        (f64::NAN, 2.0),
        (5e-324, 1.5),
        (1e200, 2.0),
        (1e-200, 2.0),
        (2.0, 1e-30),
        (2.0, 1e30),
    ] {
        let (got, want) = (pow(x, y), x.powf(y));
        assert!(
            got.to_bits() == want.to_bits() || (got.is_nan() && want.is_nan()),
            "pow({x}, {y}) = {got}, host {want}"
        );
    }
    assert_eq!(pow(1.0 + 1e-17, 1.5), 1.0, "a result within 2^-54 of 1 rounds to 1");
}

fn eval(expr: &str, row: &[Value]) -> Result<Value, String> {
    let scope = Scope { cols: vec![(Some("s".into()), "x".into())], types: vec![] };
    let f = Compiler::new(&scope).compile(&parse_expr(expr).map_err(|e| e.to_string())?)?;
    Ok(f(row))
}

/// A minus sign on a numeric literal is part of the literal: `-18446744073709551615` does not
/// fit in 64 bits either way, so it is a Float64, not the wrapped negation of a UInt64. On
/// anything else the minus is evaluated, and NULL stays NULL.
#[test]
fn a_negative_literal_is_one_number_and_other_negations_are_evaluated() {
    let x = [Value::F64(2.5)];
    assert_eq!(eval("-18446744073709551615", &x), Ok(Value::F64(-18446744073709551615.0)));
    assert_eq!(eval("-9223372036854775808", &x), Ok(Value::Int(i64::MIN)));
    assert_eq!(eval("-NULL", &x), Ok(Value::Null));
    assert_eq!(eval("-x", &x), Ok(Value::F64(-2.5)));
    // a comparison is ClickHouse's UInt8 1 or 0: negated, -1 or 0
    assert_eq!(eval("-(x > 1)", &x).unwrap().f64(), Some(-1.0));
    assert_eq!(eval("-(x > 3)", &x).unwrap().f64(), Some(0.0));
}

#[test]
fn column_references_have_at_most_one_qualifier() {
    let x = [Value::F64(2.5)];
    assert_eq!(eval("s.x", &x), Ok(Value::F64(2.5)));
    let err = eval("db.s.x", &x).unwrap_err();
    assert!(err.contains("unsupported expression"), "{err}");
}

/// Comparisons of two computed values take the general path (a column against a constant is
/// read in place): the same three-valued answers, NULL on either side giving NULL.
#[test]
fn comparisons_of_computed_values_are_three_valued() {
    let x = [Value::F64(2.5)];
    for (e, want) in [
        ("x + 0 = 2.5", Value::Bool(true)),
        ("x + 0 != 2.5", Value::Bool(false)),
        ("x + 0 = 3", Value::Bool(false)),
        ("x + 0 != 3", Value::Bool(true)),
        ("x < x + 1", Value::Bool(true)),
        ("x + 1 <= x", Value::Bool(false)),
        ("x + 0 < NULL", Value::Null),
        ("NULL >= x + 0", Value::Null),
        ("x + 0 = NULL", Value::Null),
    ] {
        assert_eq!(eval(e, &x), Ok(want), "{e}");
    }
}

/// `null_if` against a constant compares in place; against a computed value it takes the
/// general path. Both give NULL exactly when the two are equal.
#[test]
fn null_if_is_null_exactly_when_its_arguments_are_equal() {
    let x = [Value::F64(2.5)];
    for (e, want) in [
        ("null_if(x, 2.5)", Value::Null),
        ("null_if(x, 3)", Value::F64(2.5)),
        ("null_if(x, x + 0)", Value::Null),
        ("null_if(x, x + 1)", Value::F64(2.5)),
        ("null_if(x, NULL)", Value::F64(2.5)),
        ("null_if(x + 0, NULL + x)", Value::F64(2.5)),
    ] {
        assert_eq!(eval(e, &x), Ok(want), "{e}");
    }
}

/// `%`, ClickHouse's modulo: a returns pipeline counts a trade's return in a window only
/// where the previous trade's `t - t % width` is that window's start, on times in microseconds.
/// Integers stay exact integers and take the dividend's sign; an integer `%` by zero, which
/// ClickHouse raises, is NULL (a stream cannot fail); a float takes the remainder of truncated
/// division.
#[test]
fn modulo_finds_a_times_window_and_takes_the_dividends_sign() {
    let x = [Value::F64(7.5)];
    for (e, want) in [
        ("1790000099999999 % 60000000", Value::Int(59_999_999)),
        ("1790000099999999 - 1790000099999999 % 60000000", Value::Int(1_790_000_040_000_000)),
        ("1790000100000000 - 1790000100000000 % 60000000", Value::Int(1_790_000_100_000_000)),
        ("-7 % 3", Value::Int(-1)),
        ("7 % -3", Value::Int(1)),
        ("7 % 0", Value::Null),
        ("x % 2", Value::F64(1.5)),
        ("-x % 2", Value::F64(-1.5)),
        ("x % (x - 5)", Value::F64(0.0)),
        ("x % NULL", Value::Null),
    ] {
        assert_eq!(eval(e, &x), Ok(want), "{e}");
    }
    let Ok(Value::F64(nan)) = eval("x % 0.0", &x) else { panic!("x % 0.0 is a float") };
    assert!(nan.is_nan(), "a float % 0 is NaN, not an error");
}

/// `sqrt`: returns' realized volatility, SQRT(SUM(r^2)), a Float64 whatever the argument's type;
/// NaN below 0 as in ClickHouse, NULL for NULL.
#[test]
fn sqrt_is_the_float64_square_root() {
    let x = [Value::F64(6.25)];
    assert_eq!(eval("sqrt(x)", &x), Ok(Value::F64(2.5)));
    assert_eq!(eval("sqrt(9)", &x), Ok(Value::F64(3.0)));
    assert_eq!(eval("sqrt(NULL)", &x), Ok(Value::Null));
    let Ok(Value::F64(nan)) = eval("sqrt(x - 7)", &x) else { panic!("sqrt is a Float64") };
    assert!(nan.is_nan(), "sqrt of a negative is NaN, not an error");
    assert!(eval("sqrt(x, x)", &x).unwrap_err().contains("expects 1 arguments"));
}

/// `format_datetime` keeps the text of the last day it formatted (a window's rows share one): a
/// row of another day, before or after it, NULL in between, or a day before 1970, is its own day.
#[test]
fn format_datetime_formats_each_rows_day_however_the_days_follow() {
    let scope = Scope { cols: vec![(None, "t".into())], types: vec![] };
    let f = Compiler::new(&scope).compile(&parse_expr("format_datetime(t, 'd=%Y%m%d')").unwrap()).unwrap();
    let day = 86_400_000_000;
    let t0 = 1_790_000_000_000_000 / day * day; // 2026-09-21 00:00 UTC
    for (t, want) in [
        (Value::Time(t0), Value::Str("d=20260921".into())),
        (Value::Time(t0 + day - 1), Value::Str("d=20260921".into())),
        (Value::Time(t0 + day), Value::Str("d=20260922".into())),
        (Value::Null, Value::Null),
        (Value::Time(t0 - 1), Value::Str("d=20260920".into())),
        (Value::Time(t0), Value::Str("d=20260921".into())),
        (Value::Time(-1), Value::Str("d=19691231".into())),
        (Value::Time(0), Value::Str("d=19700101".into())),
    ] {
        assert_eq!(f(std::slice::from_ref(&t)), want, "{t:?}");
    }
}

/// The start of a period that begins before the earliest time (`i64::MIN` µs) is that time for
/// the streaming functions, and NULL for the ad-hoc ones: subtracting the offset into the period
/// used to overflow and panic.
#[test]
fn the_start_of_a_period_before_the_earliest_time_does_not_overflow() {
    let min = [Value::Time(i64::MIN)];
    for f in ["to_start_of_day", "to_start_of_hour", "to_start_of_minute"] {
        assert_eq!(eval(&format!("{f}(x)"), &min).unwrap(), Value::Time(i64::MIN), "{f}");
    }
    for unit in ["second", "day", "week", "month", "quarter", "year"] {
        assert_eq!(eval(&format!("date_trunc('{unit}', x)"), &min).unwrap(), Value::Null, "{unit}");
    }
    assert_eq!(eval("time_bucket('1m', x)", &min).unwrap(), Value::Null);
    assert_eq!(eval("to_start_of_day(x)", &[Value::Time(86_400_000_001)]).unwrap(), Value::Time(86_400_000_000));
}

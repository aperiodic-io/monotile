//! SQL types and ClickHouse conversion semantics. MV results are cast to the
//! target stream's declared column types on insert, so these rules decide
//! the bytes a sink writes.
use brrrrr_core::value::{Type, Value};

/// Every column type of the example pipelines (fixtures/pipelines) must parse.
#[test]
fn every_pipeline_column_type_parses() {
    let mut seen = 0;
    for entry in std::fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/../../fixtures/pipelines")).unwrap() {
        let sql = std::fs::read_to_string(entry.unwrap().path()).unwrap();
        for line in sql.lines() {
            let l = line.trim().trim_end_matches(',');
            // column definitions look like "<ident> <type>[ MATERIALIZED ...]"
            let mut parts = l.splitn(2, ' ');
            let (Some(name), Some(rest)) = (parts.next(), parts.next()) else { continue };
            if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') || name.is_empty() {
                continue;
            }
            let ty = rest.split(" MATERIALIZED").next().unwrap().trim();
            let looks_like_type =
                ["int", "uint", "float", "string", "low_cardinality", "nullable", "datetime", "bool", "map(", "array("]
                    .iter()
                    .any(|p| ty.starts_with(p));
            if looks_like_type && !ty.contains(' ') || ty.starts_with("map(") {
                Type::parse(ty).unwrap_or_else(|e| panic!("{ty}: {e}"));
                seen += 1;
            }
        }
    }
    assert!(seen > 300, "only {seen} column types found");
}

#[test]
fn parses_types() {
    use Type::*;
    let cases = [
        ("int64", Int(64)),
        ("uint8", UInt(8)),
        ("float32", F32),
        ("float64", F64),
        ("bool", Bool),
        ("string", Str),
        ("low_cardinality(string)", Str),
        ("nullable(float32)", Nullable(Box::new(F32))),
        ("datetime64(6)", Time(6)),
        ("datetime64", Time(3)),
        ("datetime", Time(0)),
        ("array(float64)", Array(Box::new(F64))),
        ("map(string, string)", Map(Box::new(Str), Box::new(Str))),
    ];
    for (s, t) in cases {
        assert_eq!(Type::parse(s).unwrap(), t, "{s}");
    }
    // `abc64` must not be read as an integer width just because of its digits
    for bad in ["decimal(10,2)", "intx", "map(string)", "datetime64(x)", "nullable", "abc64", "xyz8"] {
        assert!(Type::parse(bad).is_err(), "{bad} should be rejected");
    }
    // an unclosed parenthesis is part of the name, not an argument
    for bad in ["nullable(float64", "datetime64("] {
        assert_eq!(Type::parse(bad), Err(format!("unknown type {bad}")));
    }
}

#[test]
fn casts_follow_clickhouse() {
    use Type::*;
    // float -> int truncates toward zero; integers wrap to the target width
    assert_eq!(Value::F64(-2.9).cast(&Int(32)), Value::Int(-2));
    assert_eq!(Value::Int(1 << 31).cast(&Int(32)), Value::Int(-(1 << 31)));
    assert_eq!(Value::Int(300).cast(&UInt(8)), Value::UInt(44));
    assert_eq!(Value::Int(-1).cast(&UInt(64)), Value::UInt(u64::MAX));
    // NULL into a non-nullable column inserts the default; nullable keeps NULL
    assert_eq!(Value::Null.cast(&F32), Value::F32(0.0));
    assert_eq!(Value::Null.cast(&Str), Value::Str("".into()));
    assert_eq!(Value::Null.cast(&Nullable(Box::new(F32))), Value::Null);
    assert_eq!(Value::Null.cast(&Time(6)), Value::Time(0));
    assert_eq!(Value::Null.cast(&Array(Box::new(F64))), Value::Array(vec![].into()));
    // float64 -> float32 rounds to nearest
    assert_eq!(Value::F64(0.1).cast(&F32), Value::F32(0.1f32));
    assert_eq!(Value::F64(1.5).cast(&Nullable(Box::new(F64))), Value::F64(1.5));
    // numbers <-> strings
    assert_eq!(Value::F64(1.5).cast(&Str), Value::Str("1.5".into()));
    assert_eq!(Value::Str("42".into()).cast(&Int(64)), Value::Int(42));
    assert_eq!(Value::Str("4.5".into()).cast(&F64), Value::F64(4.5));
    assert_eq!(Value::Str("4.5".into()).cast(&F32), Value::F32(4.5));
    assert_eq!(Value::Str("7".into()).cast(&UInt(16)), Value::UInt(7));
    assert_eq!(Value::Str("true".into()).cast(&Bool), Value::Bool(true));
    assert_eq!(Value::Str("2025-09-01".into()).cast(&Time(6)), Value::Time(1_756_684_800_000_000));
    assert_eq!(Value::Str("x".into()).cast(&Str), Value::Str("x".into()));
    assert_eq!(Value::Int(2).cast(&Bool), Value::Bool(true));
    assert_eq!(Value::Int(5).cast(&Time(6)), Value::Time(5));
    assert_eq!(
        Value::Array(vec![Value::Int(1)].into()).cast(&Array(Box::new(F64))),
        Value::Array(vec![Value::F64(1.0)].into())
    );
    assert_eq!(Value::Str("a".into()).cast(&Map(Box::new(Str), Box::new(Str))), Value::Str("a".into()));
}

#[test]
fn accessors_and_defaults() {
    assert_eq!(Value::UInt(3).f64(), Some(3.0));
    assert_eq!(Value::Bool(true).f64(), Some(1.0));
    assert_eq!(Value::Str("x".into()).f64(), None);
    assert_eq!(Value::F32(2.5).i64(), Some(2));
    assert_eq!(Value::F64(-2.5).i64(), Some(-2));
    assert_eq!(Value::UInt(9).i64(), Some(9));
    assert_eq!(Value::Bool(true).i64(), Some(1));
    assert_eq!(Value::Str("x".into()).i64(), None);
    assert_eq!(Value::Str("x".into()).str(), Some("x"));
    assert_eq!(Value::Int(1).str(), None);
    assert!(Value::Null.is_null() && !Value::Int(0).is_null());
    assert_eq!(Type::Nullable(Box::new(Type::F32)).base(), &Type::F32);
    assert_eq!(Type::Bool.default_value(), Value::Bool(false));
    assert_eq!(Type::UInt(8).default_value(), Value::UInt(0));
    assert_eq!(Type::F64.default_value(), Value::F64(0.0));
    assert_eq!(Type::Int(8).default_value(), Value::Int(0));
}

/// Inserts skip the cast for values that already conform: that shortcut must never change a
/// value the cast would have changed.
#[test]
fn a_conforming_value_is_what_the_cast_returns() {
    let values = [
        Value::Null,
        Value::Bool(true),
        Value::Int(-5),
        Value::Int(1 << 40),
        Value::Int(i64::MIN),
        Value::UInt(300),
        Value::UInt(u64::MAX),
        Value::F32(1.5),
        Value::F64(-0.0),
        Value::F64(f64::NAN),
        Value::F64(1e300),
        Value::Str("7".into()),
        Value::Time(1_788_220_800_000_000),
        Value::Array(vec![Value::F64(1.0), Value::F64(2.5)].into()),
        Value::Array(vec![Value::Int(1)].into()),
    ];
    let types = [
        "bool",
        "int8",
        "int32",
        "int64",
        "uint8",
        "uint64",
        "float32",
        "float64",
        "string",
        "datetime64(6)",
        "nullable(float64)",
        "nullable(int32)",
        "array(float64)",
        "map(string, string)",
    ];
    let (mut conforming, mut total) = (0, 0);
    for t in types.map(|t| Type::parse(t).unwrap()) {
        for v in &values {
            total += 1;
            if v.conforms(&t) {
                conforming += 1;
                assert_eq!(format!("{:?}", v.cast(&t)), format!("{v:?}"), "{v:?} as {t:?}");
                if let (Value::F64(a), Value::F64(b)) = (v, v.cast(&t)) {
                    assert_eq!(a.to_bits(), b.to_bits(), "{v:?} as {t:?}");
                }
            }
            assert_eq!(format!("{:?}", v.clone().cast_into(&t)), format!("{:?}", v.cast(&t)), "{v:?} as {t:?}");
        }
    }
    assert!(conforming > 15 && conforming < total, "{conforming} of {total}");
    // the shortcut is taken for every representation inserts produce
    for (v, t, want) in [
        (Value::Null, "nullable(float64)", true),
        (Value::F64(1.5), "nullable(float64)", true),
        (Value::Int(1), "nullable(float64)", false),
        (Value::UInt(255), "uint8", true),
        (Value::UInt(256), "uint8", false),
        (Value::Int(-129), "int8", false),
        (Value::Array(vec![Value::F64(1.0)].into()), "array(float64)", true),
        (Value::Array(vec![Value::Int(1)].into()), "array(float64)", false),
        (Value::Null, "float64", false),
    ] {
        assert_eq!(v.conforms(&Type::parse(t).unwrap()), want, "{v:?} as {t}");
    }
}

/// Floats into unsigned integers as C++ casts compile on x86-64: [2^63, 2^64) converts exactly
/// (it used to go through a saturating i64: 1e19 came out as 2^63 - 1), and NaN or 2^64 and more
/// give 0, as `cvttsd2si` of `x - 2^63` does.
#[test]
fn floats_cast_to_unsigned_integers_as_x86_does() {
    let u64t = Type::UInt(64);
    for (f, want) in [
        (1e19, 10_000_000_000_000_000_000u64),
        (9_223_372_036_854_775_808.0, 1 << 63),
        (1.8e19, 18_000_000_000_000_000_000),
        (2e19, 0),
        (f64::NAN, 0),
        (f64::INFINITY, 0),
        (-1.5, u64::MAX),
        (f64::NEG_INFINITY, 1 << 63),
        (42.9, 42),
    ] {
        assert_eq!(Value::F64(f).cast(&u64t), Value::UInt(want), "{f}");
    }
    let u32t = Type::UInt(32);
    // through a 64-bit cvttsd2si: 2^63 and more are out of its range (0 once wrapped to 32 bits)
    for (f, want) in [
        (4_294_967_296.0, 0u64),
        (-1.0, 4_294_967_295),
        (f64::NAN, 0),
        (f64::INFINITY, 0),
        (7.5, 7),
        (1e19, 0),
        (9_223_372_036_854_775_808.0, 0),
    ] {
        assert_eq!(Value::F64(f).cast(&u32t), Value::UInt(want), "{f}");
    }
    // into Int32 through a 32-bit cvttsd2si, exact up to its range and i32::MIN past it
    let i32t = Type::Int(32);
    for (f, want) in [
        (-2_147_483_647.5, -2_147_483_647i64),
        (-2_147_483_648.9, i32::MIN as i64),
        (-2_147_483_649.0, i32::MIN as i64),
    ] {
        assert_eq!(Value::F64(f).cast(&i32t), Value::Int(want), "{f}");
    }
    assert_eq!(Value::F32(3.9).cast(&u64t), Value::UInt(3));
}

/// A float into a datetime truncates like one into Int64, NaN and out of range included.
#[test]
fn floats_cast_to_datetimes_as_to_int64() {
    for f in [1.5e15, -2.5, f64::NAN, f64::INFINITY, 1e30] {
        let Value::Int(i) = Value::F64(f).cast(&Type::Int(64)) else { panic!() };
        assert_eq!(Value::F64(f).cast(&Type::Time(6)), Value::Time(i), "{f}");
    }
    assert_eq!(Value::F64(f64::NAN).cast(&Type::Time(6)), Value::Time(i64::MIN));
}

#[test]
fn integer_widths_and_datetime_precisions_outside_brrrrrs_range_are_refused() {
    for ok in
        ["int8", "Int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64", "datetime64(6)", "datetime64(0)"]
    {
        assert!(Type::parse(ok).is_ok(), "{ok}");
    }
    for (bad, why) in [
        ("int0", "unknown type int0"),
        ("int7", "unknown type int7"),
        ("uint300", "unknown type uint300"),
        ("int128", "64-bit"),
        ("uint256", "64-bit"),
        ("datetime64(9)", "microseconds"),
    ] {
        let err = Type::parse(bad).unwrap_err();
        assert!(err.contains(why), "{bad}: {err}");
    }
}

/// A datetime that cannot be read whole is not one: its seconds used to become 0 when they
/// could not be read (`03Z`), and months, days and hours out of range were accepted.
#[test]
fn only_whole_valid_datetimes_parse() {
    use brrrrr_core::value::parse_datetime;
    let at = |y: i64, m: i64, d: i64, s: i64| parse_datetime(&format!("{y}-{m:02}-{d:02}")).unwrap() + s * 1_000_000;
    assert_eq!(parse_datetime("2024-02-29 01:02"), Some(at(2024, 2, 29, 3720)));
    assert_eq!(parse_datetime("2024-02-29 01"), Some(at(2024, 2, 29, 3600)));
    assert_eq!(parse_datetime("2024-02-29T23:59:59.5"), Some(at(2024, 2, 29, 86_399) + 500_000));
    // ISO 8601's zone: Z, or an offset from UTC
    for (zoned, utc) in [
        ("2025-09-01T01:02:03Z", "2025-09-01 01:02:03"),
        ("2025-09-01 01:02:03+00:00", "2025-09-01 01:02:03"),
        ("2025-09-01T02:32:03.5+01:30", "2025-09-01 01:02:03.5"),
        ("2025-08-31T20:02:03-0500", "2025-09-01 01:02:03"),
        ("2025-09-01 03:02:03+02", "2025-09-01 01:02:03"),
    ] {
        assert_eq!(parse_datetime(zoned), parse_datetime(utc), "{zoned}");
    }
    for bad in [
        "2025-09-01T01:02:03+1",
        "2025-09-01T01:02:03+01:0x",
        "2025-09-01T01:02:03 UTC",
        "2025-09-01 01:xx:30",
        "2025-09-01 01:02:x",
        "2025-13-01",
        "2025-00-10",
        "2025-02-30",
        "2023-02-29",
        "2025-09-32",
        "2025-09-01 24:00:00",
        "2025-09-01 00:60:00",
        "2025-09-01 00:00:60",
        "2025-09-01 00:00:00.5x",
        "2025-09-31",
        "2025-04-31",
        "1900-02-29",
        "2025-01-00",
    ] {
        assert_eq!(parse_datetime(bad), None, "{bad}");
    }
    // the last day of every month, leap days of years divisible by 4 and by 400
    for ok in ["2025-01-31", "2025-02-28", "2025-03-31", "2025-06-30", "2025-11-30", "2025-12-31", "2000-02-29"] {
        assert!(parse_datetime(ok).is_some(), "{ok}");
    }
}

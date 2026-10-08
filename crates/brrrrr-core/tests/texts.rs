//! The texts every sink row is built of against how they were built before: `to_string`
//! and `concat` through a `String` copied into a value, `format_datetime` through `format!`, and
//! a sink's headers computed into the row as a map and read back.
use brrrrr_core::expr::{format_datetime, Compiler, Scope};
use brrrrr_core::format::{to_text, write_text};
use brrrrr_core::sql::parse_expr;
use brrrrr_core::value::{civil, Type, Value};
use proptest::prelude::*;

/// Any value: NULL, NaN of either sign, the zeros and infinities, integers near 0, near 2^53 and
/// anywhere, times anywhere and in 1970-2100, floats, strings with characters JSON escapes, and
/// arrays of them.
fn any_value() -> impl Strategy<Value = Value> {
    let two53 = 1i64 << 53;
    let scalar = prop_oneof![
        Just(Value::Null),
        prop_oneof![Just(f64::NAN), Just(-f64::NAN), Just(0.0), Just(-0.0), Just(f64::INFINITY), Just(-f64::INFINITY)]
            .prop_map(Value::F64),
        prop_oneof![Just(f32::NAN), Just(-f32::NAN), Just(-0.0), Just(f32::INFINITY)].prop_map(Value::F32),
        any::<bool>().prop_map(Value::Bool),
        prop_oneof![-3i64..3, any::<i64>(), two53 - 3..two53 + 3].prop_map(Value::Int),
        prop_oneof![0u64..3, any::<u64>(), two53 as u64 - 3..two53 as u64 + 3].prop_map(Value::UInt),
        prop_oneof![-3i64..3, any::<i64>(), 0i64..4_102_444_800_000_000].prop_map(Value::Time),
        prop_oneof![
            (-12i32..12).prop_map(|q| q as f64 / 4.0),
            any::<f64>(),
            (two53 - 3..two53 + 3).prop_map(|i| i as f64)
        ]
        .prop_map(Value::F64),
        prop_oneof![(-12i32..12).prop_map(|q| q as f32 / 4.0), any::<f32>()].prop_map(Value::F32),
        "[ab|é/\"]{0,40}".prop_map(|s| Value::Str(s.into())),
    ];
    scalar.prop_recursive(2, 12, 3, |inner| proptest::collection::vec(inner, 0..3).prop_map(|v| Value::Array(v.into())))
}

fn old_to_string(v: &Value) -> Value {
    if v.is_null() {
        Value::Null
    } else {
        Value::Str(to_text(v).into())
    }
}

fn old_concat(args: &[Value]) -> Value {
    let mut s = String::new();
    for v in args {
        match v {
            Value::Null => return Value::Null,
            Value::Str(x) => s.push_str(x),
            v => write_text(&mut s, v),
        }
    }
    Value::Str(s.into())
}

fn old_format_datetime(us: i64, fmt: &str) -> String {
    let (y, m, d) = civil(us.div_euclid(86_400_000_000));
    let mut parts = fmt.split('%');
    let mut out = parts.next().unwrap_or_default().to_string();
    for p in parts {
        out.push_str(&match &p[..1] {
            "Y" => format!("{y:04}"),
            "m" => format!("{m:02}"),
            _ => format!("{d:02}"),
        });
        out.push_str(&p[1..]);
    }
    out
}

/// The headers the engine read from a sink row's `_tp_message_headers` map (`engine::headers`).
fn old_headers(v: &Value) -> Headers {
    match v {
        Value::Array(kv) => match &kv[..] {
            [Value::Array(k), Value::Array(v)] => {
                k.iter().zip(v.iter()).map(|(k, v)| (to_text(k), to_text(v))).collect()
            }
            _ => vec![],
        },
        _ => vec![],
    }
}

fn scope() -> Scope {
    Scope::new((0..5).map(|i| format!("c{i}")))
}

fn eval(sql: &str, row: &[Value]) -> Value {
    Compiler::new(&scope()).compile(&parse_expr(sql).unwrap()).unwrap()(row)
}

/// A Kafka message's headers.
type Headers = Vec<(String, String)>;

/// `_tp_message_headers` as computed into the row and read back before, and as built now.
fn headers(sql: &str, row: &[Value]) -> (Headers, Headers) {
    let old = old_headers(&eval(sql, row).cast_into(&Type::parse("map(string, string)").unwrap()));
    let build = Compiler::new(&scope()).headers(&parse_expr(sql).unwrap()).unwrap();
    (old, build.expect("a header expression of the usual shape")(row))
}

/// The usual sink headers (fixtures/pipelines/bars.sql's), over columns `c0..c3`.
const USUAL: &str = "cast((['dedup-key'], [concat(to_string(c0), '|', 'trade_size', '|', \
                    to_string(c1), '|', c2, '|', c3)]), 'map(string, string)')";

proptest! {
    #![proptest_config(ProptestConfig::with_cases(4096))]

    #[test]
    fn to_string_concat_and_casts_to_string_are_as_before(row in proptest::collection::vec(any_value(), 5)) {
        prop_assert_eq!(eval("to_string(c0)", &row), old_to_string(&row[0]));
        let nested = eval("concat(to_string(c0), '|', c1, to_string(c2), concat(c3, to_string(c4)))", &row);
        let inner = old_concat(&[row[3].clone(), old_to_string(&row[4])]);
        let s = |x: &str| Value::Str(x.into());
        let args = [old_to_string(&row[0]), s("|"), row[1].clone(), old_to_string(&row[2]), inner];
        prop_assert_eq!(nested, old_concat(&args));
        prop_assert_eq!(eval("concat(c0)", &row), old_concat(&row[..1]));
        let old_cast = if row[0].is_null() { s("") } else { s(&to_text(&row[0])) };
        prop_assert_eq!(eval("cast(c0, 'string')", &row), old_cast);
    }

    #[test]
    fn format_datetime_is_as_before(us in prop_oneof![any::<i64>(), 0i64..4_102_444_800_000_000], f in 0usize..4) {
        let fmt = ["%Y-%m-%d", "%Y%m%d", "day %d of %m, %Y.", "%d"][f];
        prop_assert_eq!(format_datetime(us, fmt), old_format_datetime(us, fmt));
        let sql = format!("format_datetime(c0, '{fmt}')");
        prop_assert_eq!(eval(&sql, &[Value::Time(us)]), Value::Str(old_format_datetime(us, fmt).into()));
    }

    #[test]
    fn sink_headers_built_from_the_row_are_as_before(row in proptest::collection::vec(any_value(), 5)) {
        let (old, new) = headers(USUAL, &row);
        prop_assert_eq!(old, new);
        // more pairs than values, any item as a key, a key and a value of every kind
        let wide = "cast((['k', to_string(c0), c1, concat(c2, 'x')], [concat(to_string(c0), '|', c1, '|', \
                    to_string(c2), c3), c4, to_string(c3)]), 'map(string, string)')";
        let (old, new) = headers(wide, &row);
        prop_assert_eq!(old, new);
    }
}

/// The usual dedup key, `"NULL"` for a NULL one (as the map's NULL value read), and the
/// expressions `Compiler::headers` leaves to the general path.
#[test]
fn sink_headers_of_the_usual_shape_only_are_built_from_the_row() {
    let s = |x: &str| Value::Str(x.into());
    let row = [Value::Int(1_759_503_600_000_000), Value::Int(2), s("BTCUSDT"), s("15s"), Value::Null];
    let (old, new) = headers(USUAL, &row);
    assert_eq!(new, [("dedup-key".into(), "1759503600000000|trade_size|2|BTCUSDT|15s".into())]);
    assert_eq!(old, new);
    let null = [Value::Null, Value::Int(2), s("BTCUSDT"), s("15s"), Value::Null];
    assert_eq!(headers(USUAL, &null).1, [("dedup-key".into(), "NULL".into())]);
    for other in [
        "c0",
        "cast(c0, 'map(string, string)')",
        "cast((['a'], [c0]), 'string')",
        "cast((c0, c1), 'map(string, string)')",
        "cast((['a'], [c0], ['b']), 'map(string, string)')",
        "cast(if(c1 > 0, (['a'], [c0]), (['a'], ['b'])), 'map(string, string)')",
        "CAST((['a'], [c0]) AS string)",
    ] {
        let built = Compiler::new(&scope()).headers(&parse_expr(other).unwrap()).unwrap();
        assert!(built.is_none(), "{other}");
    }
}

/// `concat(to_string(x), ..)` reads `x` in place, and only that: a `to_string` that is not a
/// plain call (one with OVER) is compiled as written, and so are another call in a `concat`
/// (`null_if`) and a `to_string` in another function.
#[test]
fn only_a_plain_to_string_in_a_concat_is_read_in_place() {
    let row = [Value::Int(12_345), Value::Null, Value::Null, Value::Null, Value::Null];
    assert_eq!(eval("concat(to_string(c0), '|')", &row), Value::Str("12345|".into()));
    assert_eq!(eval("concat(null_if(c0, 12345), '|')", &row), Value::Null);
    assert_eq!(eval("null_if(to_string(c0), '12345')", &row), Value::Null);
    let over = parse_expr("concat(to_string(c0) OVER (), '|')").unwrap();
    assert!(Compiler::new(&scope()).compile(&over).is_err(), "to_string is no window function");
}

/// A year outside 0-9999 keeps `format!`'s layout: five digits, or a sign.
#[test]
fn a_year_past_four_digits_is_written_whole() {
    let year_10000 = 253_402_300_800_000_000; // 10000-01-01
    assert_eq!(format_datetime(year_10000, "%Y-%m-%d"), "10000-01-01");
    let year_minus_1 = -62_198_755_200_000_000; // -0001-01-01
    for us in [year_10000, year_minus_1] {
        assert_eq!(format_datetime(us, "%Y-%m-%d"), old_format_datetime(us, "%Y-%m-%d"));
    }
}

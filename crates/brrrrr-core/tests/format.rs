//! Exact text output. Every metric message a sink writes is JSON written by this code; a
//! single differing digit is a different message.
use brrrrr_core::format::{self, float, json_row, to_text};
use brrrrr_core::value::{parse_datetime, Type, Value};
use proptest::prelude::*;

fn fmt(x: f64, f32: bool) -> String {
    let mut s = String::new();
    float(&mut s, x, f32);
    s
}

/// Rust's Display rounds exact ties up; ClickHouse's layout (Dragonbox) rounds to even. This
/// single case broke a third of the messages of a prototype.
#[test]
fn exact_ties_round_to_even_like_dragonbox() {
    assert_eq!(fmt(2966317.25, true), "2966317.2");
    let tie: f32 = "2966317.25".parse().unwrap();
    assert_ne!(format!("{tie}"), "2966317.2", "std Display differs: the reason this module exists");
}

#[test]
fn layout_edges() {
    let cases: &[(f64, bool, &str)] = &[
        (0.0, false, "0"),
        (-0.0, false, "-0"),
        (1e21, false, "1e21"),
        (1.5e21, false, "1.5e21"),
        (9.99e20, false, "999000000000000000000"),
        (1e-6, false, "0.000001"),
        (1e-7, false, "1e-7"),
        (1.25e-7, false, "1.25e-7"),
        (1e16, false, "10000000000000000"),
        (9007199254740992.0, false, "9007199254740992"),
        (16777216.0, true, "16777216"),
        (16777218.0, true, "16777218"),
        (f64::NAN, false, "null"),
        (f64::INFINITY, true, "null"),
        (-1.5, false, "-1.5"),
        (0.1, true, "0.1"),
    ];
    for (x, f32, want) in cases {
        assert_eq!(fmt(*x, *f32), *want, "{x} f32={f32}");
    }
}

proptest! {
    /// Shortest round-trip: parsing the text gives back exactly the same f64.
    #[test]
    fn f64_round_trips(bits in any::<u64>()) {
        let x = f64::from_bits(bits);
        prop_assume!(x.is_finite());
        let s = fmt(x, false);
        prop_assert_eq!(s.parse::<f64>().unwrap().to_bits(), x.to_bits(), "{}", s);
        prop_assert!(!s.contains("e+") && !s.ends_with(".0") && !s.contains("e0"), "{}", s);
    }

    #[test]
    fn f32_round_trips(bits in any::<u32>()) {
        let x = f32::from_bits(bits);
        prop_assume!(x.is_finite());
        let s = fmt(x as f64, true);
        prop_assert_eq!(s.parse::<f32>().unwrap().to_bits(), x.to_bits(), "{}", s);
    }

    /// Integral values print without a fraction, exactly as integers do.
    #[test]
    fn integral_values_print_as_integers(i in -(1i64 << 52)..(1i64 << 52)) {
        prop_assert_eq!(fmt(i as f64, false), i.to_string());
    }
}

#[test]
fn json_strings_escape_slash_quote_backslash_and_controls() {
    let cols = vec![("s".to_string(), Type::Str)];
    let mut out = String::new();
    json_row(&mut out, &cols, &[Value::Str("a/b\"c\\d\ne\u{1}".into())]);
    assert_eq!(out, "{\"s\":\"a\\/b\\\"c\\\\d\\ne\\u0001\"}\n");
}

/// Every ASCII character and the JavaScript line separators, as ClickHouse's JSONEachRow writes
/// them (`SELECT char(number) FROM system.numbers LIMIT 128 FORMAT JSONEachRow`).
#[test]
fn json_string_escaping_of_every_ascii_character() {
    let mut escaped: std::collections::HashMap<u32, String> =
        [(8, "\\b"), (9, "\\t"), (10, "\\n"), (12, "\\f"), (13, "\\r"), (34, "\\\""), (47, "\\/"), (92, "\\\\")]
            .into_iter()
            .map(|(c, e)| (c, e.to_string()))
            .collect();
    for c in (0..32).filter(|c| ![8, 9, 10, 12, 13].contains(c)) {
        escaped.insert(c, format!("\\u{c:04X}")); // upper-case hex: \u000B, not \u000b
    }
    let cols = vec![("s".to_string(), Type::Str)];
    for c in 0..128u32 {
        let ch = char::from_u32(c).unwrap();
        let mut out = String::new();
        json_row(&mut out, &cols, &[Value::Str(ch.to_string().into())]);
        let want = escaped.get(&c).cloned().unwrap_or_else(|| ch.to_string());
        assert_eq!(out, format!("{{\"s\":\"{want}\"}}\n"), "character {c}");
    }
    let mut out = String::new();
    json_row(&mut out, &cols, &[Value::Str("a\u{2028}b\u{2029}cé😀 ".into())]);
    assert_eq!(out, "{\"s\":\"a\\u2028b\\u2029cé😀 \"}\n");
}

#[test]
fn to_text_of_float32_is_its_shortest_digits_and_non_finite_are_words() {
    assert_eq!(to_text(&Value::F32(0.1)), "0.1");
    assert_eq!(to_text(&Value::F32(16_777_216.0)), "16777216");
    assert_eq!(to_text(&Value::F32(f32::INFINITY)), "inf");
    assert_eq!(to_text(&Value::F32(f32::NEG_INFINITY)), "-inf");
    assert_eq!(to_text(&Value::F32(f32::NAN)), "nan");
    assert_eq!(to_text(&Value::F64(f64::NEG_INFINITY)), "-inf");
}

/// `civil` against a naive calendar walked day by day from 0000-03-01 to year 4160:
/// covers the 400/100/4-year leap rules that dates near today never exercise.
#[test]
fn civil_dates_match_a_naive_gregorian_calendar() {
    let leap = |y: i64| y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let len = |y: i64, m: u32| match m {
        2 if leap(y) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    let (mut y, mut m, mut d) = (0i64, 3u32, 1u32);
    for days in -719_468..800_000 {
        assert_eq!(brrrrr_core::value::civil(days), (y, m, d), "day {days}");
        d += 1;
        if d > len(y, m) {
            (d, m) = (1, m + 1);
            if m > 12 {
                (m, y) = (1, y + 1);
            }
        }
    }
}

#[test]
fn json_row_uses_declared_order_types_and_trailing_newline() {
    let cols = vec![
        ("exchange".to_string(), Type::Int(64)),
        ("x".to_string(), Type::Nullable(Box::new(Type::F32))),
        ("t".to_string(), Type::Time(6)),
        ("a".to_string(), Type::Array(Box::new(Type::F64))),
    ];
    let mut out = String::new();
    json_row(
        &mut out,
        &cols,
        &[Value::Int(1), Value::Null, Value::Time(1_756_684_800_000_001), Value::Array(vec![Value::F64(1.5)].into())],
    );
    assert_eq!(out, "{\"exchange\":1,\"x\":null,\"t\":\"2025-09-01 00:00:00.000001\",\"a\":[1.5]}\n");
}

#[test]
fn text_of_values() {
    assert_eq!(to_text(&Value::F64(f64::NAN)), "nan");
    assert_eq!(to_text(&Value::F32(f32::NEG_INFINITY)), "-inf");
    assert_eq!(to_text(&Value::F64(f64::INFINITY)), "inf");
    assert_eq!(to_text(&Value::Int(-3)), "-3");
    assert_eq!(to_text(&Value::UInt(3)), "3");
    assert_eq!(to_text(&Value::Bool(true)), "true");
    assert_eq!(to_text(&Value::Null), "NULL");
    assert_eq!(to_text(&Value::Time(0)), "1970-01-01 00:00:00.000000");
    assert_eq!(to_text(&Value::Array(vec![Value::Int(1), Value::Str("x".into())].into())), "[1,\"x\"]");
}

#[test]
fn datetimes() {
    let mut s = String::new();
    format::datetime(&mut s, -1, 3);
    assert_eq!(s, "1969-12-31 23:59:59.999");
    // a `datetime` (precision 0) has no fraction, not even its point
    s.clear();
    format::datetime(&mut s, 1_756_684_800_999_999, 0);
    assert_eq!(s, "2025-09-01 00:00:00");
    for (text, us) in [
        ("2025-09-01 00:00:00", 1_756_684_800_000_000i64),
        ("2000-02-29 12:34:56.789", 951_827_696_789_000),
        ("1970-01-01", 0),
        ("2025-09-01T01:02:03.000004", 1_756_688_523_000_004),
    ] {
        assert_eq!(parse_datetime(text), Some(us), "{text}");
    }
    assert_eq!(parse_datetime("garbage"), None);
}

proptest! {
    #[test]
    fn datetime_round_trips(us in -4_000_000_000_000_000i64..4_000_000_000_000_000i64) {
        let mut s = String::new();
        format::datetime(&mut s, us, 6);
        prop_assert_eq!(parse_datetime(&s), Some(us), "{}", s);
    }
}

/// The formatting as it was before it was made allocation-free, kept as the reference the
/// faster code must match byte for byte: every metric message is this text.
mod before {
    use brrrrr_core::value::{civil, Type, Value};

    pub fn float(out: &mut String, x: f64, f32: bool) {
        if !x.is_finite() {
            return out.push_str("null");
        }
        let limit = if f32 { 16_777_216.0 } else { 9_007_199_254_740_992.0 };
        if x == x.trunc() && x.abs() < limit {
            let mut b = itoa::Buffer::new();
            return out.push_str(if x == 0.0 && x.is_sign_negative() { "-0" } else { b.format(x as i64) });
        }
        let mut buf = ryu::Buffer::new();
        let s = if f32 { buf.format_finite(x as f32) } else { buf.format_finite(x) };
        let (neg, s) = s.strip_prefix('-').map_or((false, s), |r| (true, r));
        let (mant, exp) = s.split_once('e').map_or((s, 0), |(m, e)| (m, e.parse::<i32>().unwrap_or(0)));
        let (int, frac) = mant.split_once('.').unwrap_or((mant, ""));
        let digits = format!("{int}{frac}");
        let lead = digits.len() - digits.trim_start_matches('0').len();
        let digits = digits[lead..].trim_end_matches('0');
        let point = int.len() as i32 + exp - lead as i32;
        if neg {
            out.push('-');
        }
        if (-6..=20).contains(&(point - 1)) {
            if point <= 0 {
                out.push_str("0.");
                out.extend(std::iter::repeat_n('0', (-point) as usize));
                out.push_str(digits);
            } else if point as usize >= digits.len() {
                out.push_str(digits);
                out.extend(std::iter::repeat_n('0', point as usize - digits.len()));
            } else {
                out.push_str(&digits[..point as usize]);
                out.push('.');
                out.push_str(&digits[point as usize..]);
            }
        } else {
            out.push_str(&digits[..1]);
            if digits.len() > 1 {
                out.push('.');
                out.push_str(&digits[1..]);
            }
            out.push('e');
            out.push_str(itoa::Buffer::new().format(point - 1));
        }
    }

    pub fn datetime(out: &mut String, us: i64, precision: u8) {
        let (secs, frac) = (us.div_euclid(1_000_000), us.rem_euclid(1_000_000));
        let (y, m, d) = civil(secs.div_euclid(86_400));
        let sod = secs.rem_euclid(86_400);
        out.push_str(&format!("{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}", sod / 3600, sod / 60 % 60, sod % 60));
        if precision > 0 {
            out.push_str(&format!(".{:06}", frac)[..1 + precision.min(6) as usize]);
        }
    }

    pub fn string(out: &mut String, s: &str) {
        out.push('"');
        for c in s.chars() {
            match c {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '/' => out.push_str("\\/"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                '\u{8}' => out.push_str("\\b"),
                '\u{c}' => out.push_str("\\f"),
                c if (c as u32) < 0x20 || c == '\u{2028}' || c == '\u{2029}' => {
                    out.push_str(&format!("\\u{:04X}", c as u32))
                }
                c => out.push(c),
            }
        }
        out.push('"');
    }

    pub fn json(out: &mut String, v: &Value, t: &Type) {
        match (v, t.base()) {
            (Value::Null, _) => out.push_str("null"),
            (Value::F32(f), _) => float(out, *f as f64, true),
            (Value::F64(f), _) => float(out, *f, false),
            (Value::Str(s), _) => string(out, s),
            (Value::Time(us), Type::Time(p)) => {
                out.push('"');
                datetime(out, *us, *p);
                out.push('"');
            }
            (Value::Time(us), _) => {
                out.push('"');
                datetime(out, *us, 6);
                out.push('"');
            }
            (Value::Array(a), t) => {
                let inner = if let Type::Array(i) = t { i.as_ref() } else { t };
                out.push('[');
                for (i, v) in a.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    json(out, v, inner);
                }
                out.push(']');
            }
            (v, _) => out.push_str(&brrrrr_core::format::to_text(v)),
        }
    }

    pub fn json_row(out: &mut String, cols: &[(String, Type)], row: &[Value]) {
        out.push('{');
        for (i, ((name, t), v)) in cols.iter().zip(row).enumerate() {
            if i > 0 {
                out.push(',');
            }
            string(out, name);
            out.push(':');
            json(out, v, t);
        }
        out.push_str("}\n");
    }
}

fn written(f: impl FnOnce(&mut String)) -> String {
    let mut s = String::from("prefix:");
    f(&mut s);
    s
}

/// Text that exercises every escape: mostly plain, then `"`, `\\`, `/`, every control
/// character, U+2028/U+2029 and their neighbours (all start with 0xE2), DEL, NEL, a BOM, other
/// non-ASCII and emoji.
fn any_text() -> impl Strategy<Value = String> {
    let special = prop_oneof![
        Just('"'),
        Just('\\'),
        Just('/'),
        (0u32..0x20).prop_map(|c| char::from_u32(c).unwrap()),
        (0x2000u32..0x2100).prop_map(|c| char::from_u32(c).unwrap()),
        Just('\u{7f}'),
        Just('\u{85}'),
        Just('\u{feff}'),
        Just('é'),
        Just('😀'),
    ];
    let c = prop_oneof![3 => proptest::char::range('a', 'z'), 2 => special, 1 => any::<char>()];
    proptest::collection::vec(c, 0..24).prop_map(|cs| cs.into_iter().collect())
}

/// Any value of a type a sink writes, as the engine holds it.
fn any_value() -> impl Strategy<Value = (Value, Type)> {
    prop_oneof![
        any::<u64>().prop_map(|b| (Value::F64(f64::from_bits(b)), Type::F64)),
        any::<u32>().prop_map(|b| (Value::F32(f32::from_bits(b)), Type::F32)),
        any::<i64>().prop_map(|i| (Value::Int(i), Type::Int(64))),
        any::<u64>().prop_map(|u| (Value::UInt(u), Type::Int(64))),
        any::<bool>().prop_map(|b| (Value::Bool(b), Type::Bool)),
        any_text().prop_map(|s| (Value::Str(s.into()), Type::Str)),
        (-62_135_596_800_000_000i64..253_402_300_800_000_000i64, 0u8..=9)
            .prop_map(|(us, p)| (Value::Time(us), Type::Time(p))),
        Just((Value::Null, Type::F64)),
        proptest::collection::vec(any::<u64>(), 0..4).prop_map(|v| {
            (
                Value::Array(v.into_iter().map(|b| Value::F64(f64::from_bits(b))).collect()),
                Type::Array(Box::new(Type::F64)),
            )
        }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(4096))]

    /// Every f64 and f32 bit pattern (subnormals, NaNs, infinities, both zeros, integral and
    /// not) is written as before, after whatever the string held.
    #[test]
    fn floats_are_written_as_before(b in any::<u64>(), b32 in any::<u32>()) {
        let x = f64::from_bits(b);
        prop_assert_eq!(written(|s| float(s, x, false)), written(|s| before::float(s, x, false)));
        let y = f32::from_bits(b32) as f64;
        prop_assert_eq!(written(|s| float(s, y, true)), written(|s| before::float(s, y, true)));
    }

    /// Floats near the notation's edges and with many digits: around powers of ten, where
    /// fixed notation turns scientific, and the shortest digits are longest.
    #[test]
    fn floats_near_powers_of_ten_are_written_as_before(e in -330i32..310, m in 1.0f64..10.0, ulps in -3i64..4) {
        let x = f64::from_bits((m * 10f64.powi(e)).to_bits().wrapping_add_signed(ulps));
        prop_assert_eq!(written(|s| float(s, x, false)), written(|s| before::float(s, x, false)));
        // as an f32: the f32 nearest, and its neighbours a few ulps away
        let y = f32::from_bits(((m * 10f64.powi(e)) as f32).to_bits().wrapping_add_signed(ulps as i32)) as f64;
        prop_assert_eq!(written(|s| float(s, -y, true)), written(|s| before::float(s, -y, true)));
    }

    /// Floats as sinks write most: prices and sizes with up to 9 decimals, in every magnitude fixed
    /// notation covers and past it, as f64 and as f32, either sign: as before (ryu's own text is
    /// taken as it is there).
    #[test]
    fn decimal_floats_are_written_as_before(units in 0u64..100_000_000, decimals in 0u32..10, shift in -12i32..24, neg in any::<bool>()) {
        let x = units as f64 / 10f64.powi(decimals as i32) * 10f64.powi(shift);
        let x = if neg { -x } else { x };
        prop_assert_eq!(written(|s| float(s, x, false)), written(|s| before::float(s, x, false)));
        prop_assert_eq!(written(|s| float(s, x as f32 as f64, true)), written(|s| before::float(s, x as f32 as f64, true)));
    }

    /// Any float in the range fixed notation covers (1e-7 to 1e22), log-uniform: as before.
    #[test]
    fn floats_in_fixed_range_are_written_as_before(e in -7.0f64..22.0, neg in any::<bool>()) {
        let x = 10f64.powf(e) * if neg { -1.0 } else { 1.0 };
        prop_assert_eq!(written(|s| float(s, x, false)), written(|s| before::float(s, x, false)));
        prop_assert_eq!(written(|s| float(s, x as f32 as f64, true)), written(|s| before::float(s, x as f32 as f64, true)));
    }

    /// Every time from year 1 to 9999 (and around), at every precision, is written as before.
    #[test]
    fn datetimes_are_written_as_before(us in -63_000_000_000_000_000i64..260_000_000_000_000_000i64, p in 0u8..=9) {
        prop_assert_eq!(written(|s| format::datetime(s, us, p)), written(|s| before::datetime(s, us, p)));
    }

    /// Rows of any values under any column names (ASCII, controls, quotes, slashes, U+2028,
    /// emoji, ...) are written as before, by `json_row` and by a `RowFormat` laid out once and
    /// reading the columns from wherever they are in the row.
    #[test]
    fn rows_are_written_as_before(cols in proptest::collection::vec((any_text(), any_value()), 0..8)) {
        let types: Vec<(String, Type)> = cols.iter().map(|(n, (_, t))| (n.clone(), t.clone())).collect();
        let values: Vec<Value> = cols.iter().map(|(_, (v, _))| v.clone()).collect();
        let want = written(|s| before::json_row(s, &types, &values));
        prop_assert_eq!(written(|s| json_row(s, &types, &values)), want.clone());
        // the row as a sink holds it: an extra column first and between each, not written
        let mut row = vec![Value::Str("hidden".into())];
        for v in &values {
            row.push(v.clone());
            row.push(Value::Int(7));
        }
        let layout = format::RowFormat::new(types.iter().enumerate().map(|(i, (n, t))| (1 + 2 * i, n.as_str(), t)));
        prop_assert_eq!(written(|s| layout.write(s, &row)), want);
    }

    /// Text of any value (`to_string`, and JSON's fallback for ints, uints and bools) as before.
    #[test]
    fn values_are_written_in_json_as_before((v, t) in any_value()) {
        prop_assert_eq!(written(|s| format::json(s, &v, &t)), written(|s| before::json(s, &v, &t)));
    }
}

/// A sink without a written column writes `{}`, as `json_row` does.
#[test]
fn a_row_format_of_no_column_writes_an_empty_object() {
    let layout = format::RowFormat::new(std::iter::empty());
    assert_eq!(written(|s| layout.write(s, &[Value::Int(1)])), "prefix:{}\n");
    assert_eq!(written(|s| json_row(s, &[], &[])), "prefix:{}\n");
}

/// Datetimes outside years 0-9999 keep `{:04}`'s layout (a sign, at least four digits).
#[test]
fn datetimes_outside_the_four_digit_years_keep_their_layout() {
    for us in [-62_167_219_200_000_001i64, -100_000_000_000_000_000, 253_402_300_800_000_000, i64::MAX / 2] {
        for p in [0, 3, 6] {
            assert_eq!(written(|s| format::datetime(s, us, p)), written(|s| before::datetime(s, us, p)), "{us} {p}");
        }
    }
}

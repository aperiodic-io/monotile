//! Scalar functions beyond the streaming pipelines': the math, text and time vocabulary of
//! ad-hoc queries, under DuckDB's and PostgreSQL's names (ADR-0018). Each returns NULL for a
//! NULL argument.
use super::{compare, Arg, Ex, R};
use crate::value::{civil, Value};
use std::sync::Arc;

/// Days since 1970-01-01 of (year, month, day).
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = y - (m <= 2) as i64;
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m as i64 + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

const DAY: i64 = 86_400_000_000;

/// The start of the `unit` a time is in (UTC; weeks start on Monday). `None` where it is before
/// the earliest time (`i64::MIN` µs).
pub fn trunc(unit: &str, t: i64) -> Option<i64> {
    let fixed = |w: i64| t.checked_sub(t.rem_euclid(w));
    let days = t.div_euclid(DAY);
    let (y, m, _) = civil(days);
    match unit.trim().to_ascii_lowercase().trim_end_matches('s') {
        "microsecond" | "u" => Some(t),
        "millisecond" | "m" if unit.eq_ignore_ascii_case("ms") || unit.starts_with("milli") => fixed(1_000),
        "second" | "sec" => fixed(1_000_000),
        "minute" | "min" => fixed(60_000_000),
        "hour" => fixed(3_600_000_000),
        "day" => fixed(DAY),
        // 1970-01-01 was a Thursday: days since a Monday are (days + 3) mod 7
        "week" => (days - (days + 3).rem_euclid(7)).checked_mul(DAY),
        "month" => days_from_civil(y, m, 1).checked_mul(DAY),
        "quarter" => days_from_civil(y, (m - 1) / 3 * 3 + 1, 1).checked_mul(DAY),
        "year" => days_from_civil(y, 1, 1).checked_mul(DAY),
        _ => None,
    }
}

/// A time zone by its IANA name (`America/New_York`, `UTC`), from the tz database bundled into
/// the build: the same answer on every machine, whatever its own database holds.
pub fn zone(name: &str) -> Option<jiff::tz::TimeZone> {
    static DB: std::sync::OnceLock<jiff::tz::TimeZoneDatabase> = std::sync::OnceLock::new();
    DB.get_or_init(jiff::tz::TimeZoneDatabase::bundled).get(name).ok()
}

/// A time (µs, UTC) as the wall clock of `tz` reads it (µs, as a time without a zone).
pub fn to_local(tz: &jiff::tz::TimeZone, t: i64) -> Option<i64> {
    let off = tz.to_offset(jiff::Timestamp::from_microsecond(t).ok()?).seconds();
    t.checked_add(i64::from(off) * 1_000_000)
}

/// The start (µs, UTC) of the `w`-long bucket of `tz`'s wall clock that time `t` is in. A start
/// the clock reads twice (DST ends) is the last of those times not after `t`.
pub fn local_bucket(tz: &jiff::tz::TimeZone, t: i64, w: i64) -> Option<i64> {
    let l = to_local(tz, t)?;
    let wall = jiff::tz::Offset::UTC.to_datetime(jiff::Timestamp::from_microsecond(l - l.rem_euclid(w)).ok()?);
    let at = tz.to_ambiguous_timestamp(wall);
    let later = at.later().ok()?.as_microsecond();
    Some(if later <= t { later } else { at.earlier().ok()?.as_microsecond() })
}

/// A part of a time (UTC): `year` ... `microsecond`, `dow` (Sunday 0), `isodow` (Monday 1),
/// `doy`, `week` (ISO), `quarter`, `epoch` (seconds).
pub fn part(unit: &str, t: i64) -> Option<Value> {
    let days = t.div_euclid(DAY);
    let in_day = t.rem_euclid(DAY);
    let (y, m, d) = civil(days);
    let int = |v: i64| Some(Value::Int(v));
    match unit.trim().to_ascii_lowercase().as_str() {
        "year" | "years" | "y" => int(y),
        "quarter" => int((m as i64 - 1) / 3 + 1),
        "month" | "months" | "mon" => int(m as i64),
        "day" | "days" | "d" | "dayofmonth" => int(d as i64),
        "hour" | "hours" | "h" => int(in_day / 3_600_000_000),
        "minute" | "minutes" | "min" => int(in_day / 60_000_000 % 60),
        "second" | "seconds" | "s" => int(in_day / 1_000_000 % 60),
        "millisecond" | "milliseconds" | "ms" => int(in_day / 1_000 % 60_000),
        "microsecond" | "microseconds" | "us" => int(in_day % 60_000_000),
        "dow" | "dayofweek" | "weekday" => int((days + 4).rem_euclid(7)),
        "isodow" => int((days + 3).rem_euclid(7) + 1),
        "doy" | "dayofyear" => int(days - days_from_civil(y, 1, 1) + 1),
        "week" | "weekofyear" | "isoweek" => {
            // ISO 8601: the week with the year's first Thursday is week 1
            let thursday = days - (days + 3).rem_euclid(7) + 3;
            let (ty, _, _) = civil(thursday);
            int((thursday - days_from_civil(ty, 1, 1)) / 7 + 1)
        }
        "epoch" => Some(Value::F64(t as f64 / 1e6)),
        _ => None,
    }
}

/// `strftime`'s specifiers: %Y %m %d %H %M %S %f (microseconds) %j %a %b %%.
pub fn strftime(t: i64, fmt: &str) -> String {
    let days = t.div_euclid(DAY);
    let in_day = t.rem_euclid(DAY);
    let (y, m, d) = civil(days);
    const WD: [&str; 7] = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"];
    const MON: [&str; 12] = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    let mut out = String::new();
    let mut chars = fmt.chars();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('Y') => out.push_str(&format!("{y:04}")),
            Some('m') => out.push_str(&format!("{m:02}")),
            Some('d') => out.push_str(&format!("{d:02}")),
            Some('H') => out.push_str(&format!("{:02}", in_day / 3_600_000_000)),
            Some('M') => out.push_str(&format!("{:02}", in_day / 60_000_000 % 60)),
            Some('S') => out.push_str(&format!("{:02}", in_day / 1_000_000 % 60)),
            Some('f') => out.push_str(&format!("{:06}", in_day % 1_000_000)),
            Some('j') => out.push_str(&format!("{:03}", days - days_from_civil(y, 1, 1) + 1)),
            Some('a') => out.push_str(WD[days.rem_euclid(7) as usize]),
            Some('b') => out.push_str(MON[m as usize - 1]),
            Some('%') => out.push('%'),
            Some(o) => {
                out.push('%');
                out.push(o);
            }
            None => out.push('%'),
        }
    }
    out
}

/// A SQL LIKE pattern as an anchored regex (`%` any run, `_` any character).
pub fn like_regex(pattern: &str, case_insensitive: bool) -> R<regex::Regex> {
    let mut re = String::from(if case_insensitive { "(?is)^" } else { "(?s)^" });
    for c in pattern.chars() {
        match c {
            '%' => re.push_str(".*"),
            '_' => re.push('.'),
            c => re.push_str(&regex::escape(&c.to_string())),
        }
    }
    re.push('$');
    regex::Regex::new(&re).map_err(|e| e.to_string())
}

/// A time's µs from a value: a time, or a number of µs.
fn us(v: &Value) -> Option<i64> {
    match v {
        Value::Time(t) => Some(*t),
        Value::Str(s) => crate::value::parse_datetime(s.trim().trim_end_matches('Z')),
        v => v.i64(),
    }
}

/// The error of a time zone that is not one.
pub fn unknown_zone(z: Option<String>) -> String {
    format!(
        "unknown time zone {}: an IANA name, as 'America/New_York', 'Europe/London' or 'UTC'",
        z.map_or("(a string literal)".into(), |z| format!("'{z}'"))
    )
}

fn text(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::Str(s) => Some(s.to_string()),
        v => Some(crate::format::text_value(v).to_string()),
    }
}

fn s(x: String) -> Value {
    Value::Str(x.into())
}

/// The function, if it is one of these; `lit(i)`: argument `i` when it is a string literal.
pub(super) fn function(name: &str, a: &[Arg], lit: &dyn Fn(usize) -> Option<String>) -> Option<R<Ex>> {
    let n = a.len();
    let bad = |want: &str| Some(Err(format!("{name} expects {want}, got {n} arguments")));
    macro_rules! f1 {
        ($f:expr) => {{
            if n != 1 {
                return bad("1 argument");
            }
            let x = a[0].clone();
            let f = $f;
            Some(Ok(Arc::new(move |r: &[Value]| {
                let v = x.eval(r);
                if v.is_null() {
                    Value::Null
                } else {
                    f(&v)
                }
            }) as Ex))
        }};
    }
    macro_rules! f2 {
        ($f:expr) => {{
            if n != 2 {
                return bad("2 arguments");
            }
            let (x, y) = (a[0].clone(), a[1].clone());
            let f = $f;
            Some(Ok(Arc::new(move |r: &[Value]| {
                let (u, v) = (x.eval(r), y.eval(r));
                if u.is_null() || v.is_null() {
                    Value::Null
                } else {
                    f(&u, &v)
                }
            }) as Ex))
        }};
    }
    let num = |f: fn(f64) -> f64| move |v: &Value| v.f64().map_or(Value::Null, |x| Value::F64(f(x)));
    let int_or = |f: fn(f64) -> f64| {
        move |v: &Value| match v {
            Value::Int(_) | Value::UInt(_) => v.clone(),
            v => v.f64().map_or(Value::Null, |x| Value::F64(f(x))),
        }
    };
    match name {
        // math
        "floor" => f1!(int_or(f64::floor)),
        "ceil" | "ceiling" => f1!(int_or(f64::ceil)),
        "trunc" => f1!(int_or(f64::trunc)),
        "round" if n == 1 => f1!(int_or(f64::round)),
        "round" => f2!(|x: &Value, d: &Value| match (x.f64(), d.i64()) {
            (Some(x), Some(d)) => {
                let m = 10f64.powi(d as i32);
                Value::F64((x * m).round() / m)
            }
            _ => Value::Null,
        }),
        "exp" => f1!(num(f64::exp)),
        "log10" => f1!(num(f64::log10)),
        "log2" => f1!(num(f64::log2)),
        // PostgreSQL's and DuckDB's: log(x) is base 10, log(b, x) base b (ln is natural)
        "log" if n == 1 => f1!(num(f64::log10)),
        "log" => f2!(|b: &Value, x: &Value| match (b.f64(), x.f64()) {
            (Some(b), Some(x)) => Value::F64(x.ln() / b.ln()),
            _ => Value::Null,
        }),
        "power" | "pow" => f2!(|x: &Value, y: &Value| match (x.f64(), y.f64()) {
            (Some(x), Some(y)) => Value::F64(crate::pow::pow(x, y)),
            _ => Value::Null,
        }),
        "sign" | "signum" => f1!(|v: &Value| v.f64().map_or(Value::Null, |x| Value::Int(if x > 0.0 {
            1
        } else if x < 0.0 {
            -1
        } else {
            0
        }))),
        "pi" if n == 0 => Some(Ok(Arc::new(|_: &[Value]| Value::F64(std::f64::consts::PI)) as Ex)),
        "greatest" | "least" => {
            let (a, greatest) = (a.to_vec(), name == "greatest");
            Some(Ok(Arc::new(move |r: &[Value]| {
                let mut best: Option<Value> = None;
                for x in &a {
                    let v = x.eval(r);
                    if v.is_null() {
                        continue;
                    }
                    let better = best.as_ref().is_none_or(|b| {
                        let o = compare(&v, b);
                        if greatest {
                            o == Some(std::cmp::Ordering::Greater)
                        } else {
                            o == Some(std::cmp::Ordering::Less)
                        }
                    });
                    if better {
                        best = Some(v.into_owned());
                    }
                }
                best.unwrap_or(Value::Null)
            }) as Ex))
        }
        "nullif" => Some(super::scalar("null_if", a.to_vec(), lit)),
        "ifnull" | "nvl" => Some(super::scalar("coalesce", a.to_vec(), lit)),
        // text
        "lower" | "lcase" => f1!(|v: &Value| text(v).map_or(Value::Null, |x| s(x.to_lowercase()))),
        "upper" | "ucase" => f1!(|v: &Value| text(v).map_or(Value::Null, |x| s(x.to_uppercase()))),
        "trim" => f1!(|v: &Value| text(v).map_or(Value::Null, |x| s(x.trim().to_string()))),
        "ltrim" => f1!(|v: &Value| text(v).map_or(Value::Null, |x| s(x.trim_start().to_string()))),
        "rtrim" => f1!(|v: &Value| text(v).map_or(Value::Null, |x| s(x.trim_end().to_string()))),
        "reverse" => f1!(|v: &Value| text(v).map_or(Value::Null, |x| s(x.chars().rev().collect()))),
        "char_length" | "character_length" | "strlen" => {
            f1!(|v: &Value| text(v).map_or(Value::Null, |x| Value::Int(x.chars().count() as i64)))
        }
        "starts_with" | "prefix" => f2!(|x: &Value, p: &Value| match (text(x), text(p)) {
            (Some(x), Some(p)) => Value::Bool(x.starts_with(&p)),
            _ => Value::Null,
        }),
        "ends_with" | "suffix" => f2!(|x: &Value, p: &Value| match (text(x), text(p)) {
            (Some(x), Some(p)) => Value::Bool(x.ends_with(&p)),
            _ => Value::Null,
        }),
        "contains" => f2!(|x: &Value, p: &Value| match (text(x), text(p)) {
            (Some(x), Some(p)) => Value::Bool(x.contains(&p)),
            _ => Value::Null,
        }),
        "strpos" | "instr" => f2!(|x: &Value, p: &Value| match (text(x), text(p)) {
            (Some(x), Some(p)) => Value::Int(x.find(&p).map_or(0, |i| x[..i].chars().count() as i64 + 1)),
            _ => Value::Null,
        }),
        "left" => f2!(|x: &Value, k: &Value| match (text(x), k.i64()) {
            (Some(x), Some(k)) => s(x.chars().take(k.max(0) as usize).collect()),
            _ => Value::Null,
        }),
        "right" => f2!(|x: &Value, k: &Value| match (text(x), k.i64()) {
            (Some(x), Some(k)) => {
                let c: Vec<char> = x.chars().collect();
                s(c[c.len().saturating_sub(k.max(0) as usize)..].iter().collect())
            }
            _ => Value::Null,
        }),
        "repeat" => f2!(|x: &Value, k: &Value| match (text(x), k.i64()) {
            (Some(x), Some(k)) => s(x.repeat(k.clamp(0, 1 << 20) as usize)),
            _ => Value::Null,
        }),
        "substr" | "substring" => {
            if !(2..=3).contains(&n) {
                return bad("2 or 3 arguments");
            }
            let a = a.to_vec();
            Some(Ok(Arc::new(move |r: &[Value]| {
                let (Some(x), Some(start)) = (text(&a[0].eval(r)), a[1].eval(r).i64()) else { return Value::Null };
                let len = a.get(2).map(|l| l.eval(r).i64());
                let c: Vec<char> = x.chars().collect();
                // 1-based; a start before 1 still ends where the length says (PostgreSQL)
                let begin = (start - 1).max(0) as usize;
                let end = match len {
                    Some(Some(l)) => (start - 1 + l.max(0)).clamp(0, c.len() as i64) as usize,
                    Some(None) => return Value::Null,
                    None => c.len(),
                };
                s(c[begin.min(c.len())..end.max(begin.min(c.len()))].iter().collect())
            }) as Ex))
        }
        "replace" => {
            if n != 3 {
                return bad("3 arguments");
            }
            let a = a.to_vec();
            Some(Ok(
                Arc::new(move |r: &[Value]| match (text(&a[0].eval(r)), text(&a[1].eval(r)), text(&a[2].eval(r))) {
                    (Some(x), Some(f), Some(t)) if !f.is_empty() => s(x.replace(&f, &t)),
                    (Some(x), Some(_), Some(_)) => s(x),
                    _ => Value::Null,
                }) as Ex,
            ))
        }
        "split_part" => {
            if n != 3 {
                return bad("3 arguments");
            }
            let a = a.to_vec();
            Some(Ok(Arc::new(move |r: &[Value]| match (text(&a[0].eval(r)), text(&a[1].eval(r)), a[2].eval(r).i64()) {
                (Some(x), Some(d), Some(i)) if i >= 1 && !d.is_empty() => {
                    s(x.split(d.as_str()).nth(i as usize - 1).unwrap_or("").to_string())
                }
                (Some(_), Some(_), Some(_)) => s(String::new()),
                _ => Value::Null,
            }) as Ex))
        }
        "lpad" | "rpad" => {
            if n != 3 {
                return bad("3 arguments");
            }
            let (a, left) = (a.to_vec(), name == "lpad");
            Some(Ok(Arc::new(move |r: &[Value]| match (text(&a[0].eval(r)), a[1].eval(r).i64(), text(&a[2].eval(r))) {
                (Some(x), Some(w), Some(fill)) => {
                    let w = w.clamp(0, 1 << 20) as usize;
                    let c: Vec<char> = x.chars().collect();
                    if c.len() >= w || fill.is_empty() {
                        return s(c[..w.min(c.len())].iter().collect());
                    }
                    let pad: String = fill.chars().cycle().take(w - c.len()).collect();
                    s(if left { pad + &x } else { x + &pad })
                }
                _ => Value::Null,
            }) as Ex))
        }
        "concat_ws" => {
            let a = a.to_vec();
            Some(Ok(Arc::new(move |r: &[Value]| {
                let Some(sep) = a.first().and_then(|x| text(&x.eval(r))) else { return Value::Null };
                let parts: Vec<String> = a[1..].iter().filter_map(|x| text(&x.eval(r))).collect();
                s(parts.join(&sep))
            }) as Ex))
        }
        "like" | "ilike" | "regexp_matches" | "regexp_like" | "regexp_extract" | "regexp_replace" => {
            let Some(p) = lit(1) else { return Some(Err(format!("{name}: the pattern is a string literal"))) };
            let re = match name {
                "like" | "ilike" => like_regex(&p, name == "ilike"),
                _ => regex::Regex::new(&p).map_err(|e| e.to_string()),
            };
            let re = match re {
                Ok(re) => re,
                Err(e) => return Some(Err(format!("{name}: {e}"))),
            };
            let x = a[0].clone();
            match name {
                "regexp_extract" => {
                    let group = a.get(2).and_then(|g| match g {
                        Arg::Const(v) => v.i64(),
                        _ => None,
                    });
                    let group = group.unwrap_or(0).max(0) as usize;
                    Some(Ok(Arc::new(move |r: &[Value]| {
                        text(&x.eval(r)).map_or(Value::Null, |t| {
                            s(re.captures(&t).and_then(|c| c.get(group)).map_or("", |m| m.as_str()).to_string())
                        })
                    }) as Ex))
                }
                "regexp_replace" => {
                    let (Some(to), all) = (lit(2), lit(3).is_some_and(|o| o.contains('g'))) else {
                        return Some(Err("regexp_replace(text, pattern, replacement[, 'g'])".into()));
                    };
                    Some(Ok(Arc::new(move |r: &[Value]| {
                        text(&x.eval(r)).map_or(Value::Null, |t| {
                            s(if all { re.replace_all(&t, to.as_str()) } else { re.replace(&t, to.as_str()) }
                                .into_owned())
                        })
                    }) as Ex))
                }
                _ => Some(Ok(Arc::new(move |r: &[Value]| {
                    text(&x.eval(r)).map_or(Value::Null, |t| Value::Bool(re.is_match(&t)))
                }) as Ex)),
            }
        }
        // time
        "date_trunc" | "datetrunc" => {
            let Some(unit) = lit(0) else {
                return Some(Err(format!("{name}('unit', time): the unit is a string literal")));
            };
            if trunc(&unit, 0).is_none() {
                return Some(Err(format!(
                    "{name}: unknown unit '{unit}' (second, minute, hour, day, week, month, quarter, year)"
                )));
            }
            let x = a[1].clone();
            Some(Ok(Arc::new(move |r: &[Value]| {
                us(&x.eval(r)).and_then(|t| trunc(&unit, t)).map_or(Value::Null, Value::Time)
            }) as Ex))
        }
        "date_part" | "datepart" | "extract" => {
            let Some(unit) = lit(0) else {
                return Some(Err(format!("{name}('part', time): the part is a string literal")));
            };
            if part(&unit, 0).is_none() {
                return Some(Err(format!("{name}: unknown part '{unit}'")));
            }
            let x = a[1].clone();
            Some(Ok(
                Arc::new(move |r: &[Value]| us(&x.eval(r)).and_then(|t| part(&unit, t)).unwrap_or(Value::Null)) as Ex
            ))
        }
        "year" | "quarter" | "month" | "day" | "hour" | "minute" | "second" | "millisecond" | "microsecond"
        | "dayofweek" | "dow" | "isodow" | "dayofyear" | "doy" | "week" | "weekofyear" | "epoch" => {
            let unit = name.to_string();
            f1!(move |v: &Value| us(v).and_then(|t| part(&unit, t)).unwrap_or(Value::Null))
        }
        // DuckDB's: of a time, its count since the epoch; of a number, the time it counts
        "epoch_ms" | "epoch_us" | "epoch_ns" => {
            // µs per unit
            let k: f64 = match name {
                "epoch_ms" => 1_000.0,
                "epoch_us" => 1.0,
                _ => 0.001,
            };
            f1!(move |v: &Value| match v {
                Value::Time(t) => Value::Int((*t as f64 / k).round() as i64),
                v => v.f64().map_or(Value::Null, |x| Value::Time((x * k).floor() as i64)),
            })
        }
        "to_timestamp" | "from_unixtime" => f1!(|v: &Value| match v {
            Value::Time(_) => v.clone(),
            Value::Str(t) =>
                crate::value::parse_datetime(t.trim().trim_end_matches('Z')).map_or(Value::Null, Value::Time),
            v => v.f64().map_or(Value::Null, |x| Value::Time((x * 1e6).round() as i64)),
        }),
        "strftime" => {
            let Some(fmt) = lit(1) else {
                return Some(Err("strftime(time, 'format'): the format is a string literal".into()));
            };
            let x = a[0].clone();
            Some(Ok(Arc::new(move |r: &[Value]| us(&x.eval(r)).map_or(Value::Null, |t| s(strftime(t, &fmt)))) as Ex))
        }
        // timezone(zone, time), `time AT TIME ZONE zone`: the wall clock time there, as
        // PostgreSQL and DuckDB give it of a TIMESTAMPTZ (times are UTC)
        "timezone" => {
            if n != 2 {
                return bad("2 arguments");
            }
            let Some(tz) = lit(0).and_then(|z| zone(&z)) else {
                return Some(Err(unknown_zone(lit(0))));
            };
            let x = a[1].clone();
            Some(Ok(Arc::new(move |r: &[Value]| {
                us(&x.eval(r)).and_then(|t| to_local(&tz, t)).map_or(Value::Null, Value::Time)
            }) as Ex))
        }
        // time_bucket(width, time, zone): the start of the bucket of the zone's wall clock time
        // it is in (a day from local midnight, 23 or 25 hours long when DST changes), as a time
        "time_bucket" if n == 3 && lit(2).and_then(|z| zone(&z)).is_some() => {
            let tz = lit(2).and_then(|z| zone(&z)).expect("a zone");
            let w = match &a[0] {
                Arg::Const(Value::Str(w)) => crate::value::duration_us(w),
                Arg::Const(v) => v.i64().filter(|w| *w > 0),
                _ => None,
            };
            let Some(w) = w else {
                return Some(Err(format!("{name}: the width is a constant duration ('1d', INTERVAL '1 day')")));
            };
            let x = a[1].clone();
            Some(Ok(Arc::new(move |r: &[Value]| {
                us(&x.eval(r)).and_then(|t| local_bucket(&tz, t, w)).map_or(Value::Null, Value::Time)
            }) as Ex))
        }
        // time_bucket(width, time[, origin]): the start of the width-long bucket it is in
        "time_bucket" | "date_bin" => {
            if !(2..=3).contains(&n) {
                return bad("2 or 3 arguments");
            }
            let width = match &a[0] {
                Arg::Const(Value::Str(w)) => crate::value::duration_us(w),
                Arg::Const(v) => v.i64().filter(|w| *w > 0),
                _ => None,
            };
            let Some(w) = width else {
                return Some(Err(format!("{name}: the width is a constant duration ('1m', INTERVAL '5 minutes')")));
            };
            let (x, origin) = (a[1].clone(), a.get(2).cloned());
            Some(Ok(Arc::new(move |r: &[Value]| {
                let o = origin.as_ref().and_then(|o| us(&o.eval(r))).unwrap_or(0);
                us(&x.eval(r))
                    .and_then(|t| t.checked_sub(t.wrapping_sub(o).rem_euclid(w)))
                    .map_or(Value::Null, Value::Time)
            }) as Ex))
        }
        "date_diff" | "datediff" => {
            let Some(unit) = lit(0) else {
                return Some(Err(format!("{name}('unit', start, end): the unit is a string literal")));
            };
            if n != 3 {
                return bad("3 arguments");
            }
            let (x, y) = (a[1].clone(), a[2].clone());
            Some(Ok(Arc::new(move |r: &[Value]| match (us(&x.eval(r)), us(&y.eval(r))) {
                (Some(a), Some(b)) => match (trunc(&unit, a), trunc(&unit, b)) {
                    (Some(ta), Some(tb)) => match unit.to_ascii_lowercase().trim_end_matches('s') {
                        "month" | "quarter" | "year" => {
                            let (ya, ma, _) = civil(ta.div_euclid(DAY));
                            let (yb, mb, _) = civil(tb.div_euclid(DAY));
                            let months = (yb - ya) * 12 + mb as i64 - ma as i64;
                            Value::Int(match unit.to_ascii_lowercase().trim_end_matches('s') {
                                "month" => months,
                                "quarter" => months / 3,
                                _ => yb - ya,
                            })
                        }
                        u => {
                            let w = crate::value::duration_us(&format!("1 {u}")).unwrap_or(1);
                            Value::Int((tb - ta) / w)
                        }
                    },
                    _ => Value::Null,
                },
                _ => Value::Null,
            }) as Ex))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(s: &str) -> i64 {
        crate::value::parse_datetime(s).unwrap()
    }

    #[test]
    fn calendar_arithmetic() {
        let x = t("2024-02-29 13:45:07.123456");
        assert_eq!(trunc("month", x), Some(t("2024-02-01")));
        assert_eq!(trunc("quarter", x), Some(t("2024-01-01")));
        assert_eq!(trunc("week", x), Some(t("2024-02-26"))); // a Monday
        assert_eq!(trunc("hour", x), Some(t("2024-02-29 13:00")));
        assert_eq!(part("doy", x), Some(Value::Int(60)));
        assert_eq!(part("dow", x), Some(Value::Int(4))); // Thursday
        assert_eq!(part("isodow", t("2024-03-03")), Some(Value::Int(7))); // Sunday
        assert_eq!(part("week", t("2021-01-03")), Some(Value::Int(53))); // ISO: 2020's last week
        assert_eq!(part("week", t("2024-12-30")), Some(Value::Int(1)));
        assert_eq!(part("second", x), Some(Value::Int(7)));
        assert_eq!(strftime(x, "%Y-%m-%dT%H:%M:%S.%f %j %a %b %%"), "2024-02-29T13:45:07.123456 060 Thu Feb %");
        assert!(like_regex("a%b_", false).unwrap().is_match("aXXbY"));
        assert!(!like_regex("a%b_", false).unwrap().is_match("aXXb"));
        assert!(like_regex("A%", true).unwrap().is_match("abc"));
    }
}

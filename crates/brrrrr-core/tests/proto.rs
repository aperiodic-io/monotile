//! Protobuf codec. Every metric of a protobuf source starts from its records; a decode
//! bug silently corrupts every pipeline, so the codec is tested differentially
//! against prost (the reference implementation) on random messages.
use brrrrr_core::proto::{parse_proto, put_varint, Codec, Kind};
use brrrrr_core::value::{Type, Value};
use proptest::prelude::*;

const PROTO: &str = include_str!("../../../fixtures/market.proto");

#[derive(Clone, PartialEq, prost::Message)]
struct Trade {
    #[prost(int64, tag = "1")]
    time: i64,
    #[prost(string, tag = "2")]
    id: String,
    #[prost(int64, tag = "3")]
    exchange: i64,
    #[prost(string, tag = "4")]
    symbol: String,
    #[prost(double, tag = "5")]
    price: f64,
    #[prost(int64, tag = "6")]
    local_timestamp: i64,
    #[prost(string, tag = "7")]
    side: String,
    #[prost(double, tag = "8")]
    quantity: f64,
    #[prost(double, tag = "9")]
    amount: f64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct BookUpdate {
    #[prost(int64, tag = "1")]
    time: i64,
    #[prost(int64, tag = "2")]
    exchange: i64,
    #[prost(string, tag = "3")]
    symbol: String,
    #[prost(bool, tag = "4")]
    is_snapshot: bool,
    #[prost(int64, tag = "5")]
    local_timestamp: i64,
    #[prost(double, repeated, tag = "6")]
    bid_price: Vec<f64>,
    #[prost(double, repeated, tag = "7")]
    bid_amount: Vec<f64>,
    #[prost(double, repeated, tag = "8")]
    ask_price: Vec<f64>,
    #[prost(double, repeated, tag = "9")]
    ask_amount: Vec<f64>,
}

fn cols(spec: &[(&str, &str)]) -> Vec<(String, Type)> {
    spec.iter().map(|(n, t)| (n.to_string(), Type::parse(t).unwrap())).collect()
}

fn codec(msg: &str, spec: &[(&str, &str)]) -> Codec {
    let msgs = parse_proto(PROTO).unwrap();
    Codec::new(&msgs[msg], &cols(spec))
}

const TRADE_COLS: &[(&str, &str)] = &[
    ("time", "int64"),
    ("id", "string"),
    ("exchange", "int64"),
    ("symbol", "low_cardinality(string)"),
    ("price", "float64"),
    ("local_timestamp", "int64"),
    ("side", "string"),
    ("quantity", "float64"),
    ("amount", "float64"),
];

const BOOK_COLS: &[(&str, &str)] = &[
    ("time", "int64"),
    ("exchange", "int64"),
    ("symbol", "string"),
    ("is_snapshot", "bool"),
    ("bid_price", "array(float64)"),
    ("bid_amount", "array(float64)"),
    ("ask_price", "array(float64)"),
    ("ask_amount", "array(float64)"),
    ("local_timestamp", "int64"),
];

#[test]
fn the_example_proto_parses_every_message() {
    let msgs = parse_proto(PROTO).unwrap();
    let names = [
        "Trade",
        "Quote",
        "BookUpdate",
        "BookSnapshot",
        "Liquidation",
        "OpenInterest",
        "MarkPrice",
        "IndexPrice",
        "FundingRate",
        "LongShortRatio",
    ];
    assert_eq!(msgs.keys().map(String::as_str).collect::<std::collections::BTreeSet<_>>(), names.into());
    let snap = &msgs["BookSnapshot"];
    assert!(snap.iter().filter(|f| f.repeated && f.kind == Kind::Double).count() == 4);
    let book = &msgs["BookUpdate"];
    let bid = book.iter().find(|f| f.name == "bid_price").unwrap();
    assert!(bid.repeated && bid.kind == Kind::Double && bid.number == 6);
}

#[test]
fn proto_parser_rejects_malformed_schemas() {
    for bad in [
        "message X { int64 a; }",
        "message X { weird a = 1; }",
        "message X { int64 a = x; }",
        "message X",
        "message X {",
        "message X { a b c = 1; }",
    ] {
        assert!(parse_proto(bad).is_err(), "{bad} accepted");
    }
    for (bad, why) in [("message X { int64 a = 1;", "message X: unterminated"), ("option x = 1", "without `;`")] {
        let err = parse_proto(bad).unwrap_err();
        assert!(err.contains(why), "{bad}: {err}");
    }
    let m = parse_proto("message A { uint64 u = 1; float f = 2; bytes b = 3; sint32 s = 4; }\nmessage B { }").unwrap();
    assert_eq!(m["A"].len(), 4);
    assert!(m["B"].is_empty());
}

/// Comments sit anywhere in a hand-edited schema. A `//` line inside a message used to swallow
/// the declaration after it (the field decoded as NULL for every message, silently), and
/// `message` inside a comment framed a message that is not there.
#[test]
fn comments_anywhere_in_a_schema_change_nothing() {
    let src = r#"syntax = "proto3"; // the schema
option java_package = "live.a//b"; /* a string holding // is no comment */
// message Old { int64 gone = 1; }
message Trade {
  // exchange time
  int64 time = 1;; // µs, and a stray `;` (empty statement)
  /* the venue's
     own id */ string id = 2;
  double price = 3 [deprecated = true];
  // reserved 9;
  reserved 4, 5;
};
"#;
    let m = parse_proto(src).unwrap();
    assert_eq!(m.keys().collect::<Vec<_>>(), ["Trade"]);
    let names: Vec<_> = m["Trade"].iter().map(|f| (f.name.as_str(), f.number)).collect();
    assert_eq!(names, [("time", 1), ("id", 2), ("price", 3)]);
}

/// What the parser does not understand is an error, never a field silently dropped or misread.
#[test]
fn schemas_outside_the_supported_subset_are_errors() {
    for (bad, why) in [
        ("message A { int64 a = 1; } message A { int64 b = 1; }", "message A defined twice"),
        ("message A { int64 a = 1; string a = 2; }", "field a or number 2 twice"),
        ("message A { int64 a = 1; string b = 1; }", "field b or number 1 twice"),
        ("message A { int64 a = 0; }", "outside protobuf's range"),
        ("message A { int64 a = 19000; }", "outside protobuf's range"),
        ("message A { int64 a = 536870912; }", "outside protobuf's range"),
        ("message A { message B { int64 b = 1; } }", "message is not supported"),
        ("message A { enum E { X = 0; } }", "enum is not supported"),
        ("message A { oneof o { int64 a = 1; } }", "oneof is not supported"),
        ("message A { map<string, int64> m = 1; }", "map is not supported"),
        ("enum E { X = 0; }", "`enum` is not supported here"),
        ("message A { int64 a = 1 }", "expected `;` after field a"),
        ("message A { int64 1a = 1; }", "bad field"),
        ("message A { int64 a = 1 [deprecated = true; }", "unterminated options of a"),
        ("message A { int64 a = 1; /* never closed", "unterminated /* comment"),
        ("option x = \"never closed;", "unterminated string"),
        ("syntax = \"proto3\"", "without `;`"),
    ] {
        let err = parse_proto(bad).err().unwrap_or_else(|| panic!("{bad} accepted"));
        assert!(err.contains(why), "{bad}: {err:?} lacks {why:?}");
    }
}

#[derive(Clone, PartialEq, prost::Message)]
struct Signed {
    #[prost(sint64, tag = "1")]
    a: i64,
    #[prost(sint32, tag = "2")]
    b: i32,
    #[prost(sint64, repeated, tag = "3")]
    c: Vec<i64>,
    #[prost(int64, tag = "4")]
    d: i64,
}

fn signed_codec() -> Codec {
    let m = parse_proto("message S { sint64 a = 1; sint32 b = 2; repeated sint64 c = 3; int64 d = 4; }").unwrap();
    Codec::new(&m["S"], &cols(&[("a", "int64"), ("b", "int32"), ("c", "array(int64)"), ("d", "int64")]))
}

proptest! {
    /// sint32/sint64 are ZigZag-encoded: without decoding it, 1 read as 2 and -1 as 1.
    #[test]
    fn sint_fields_match_prost(a in any::<i64>(), b in any::<i32>(), c in proptest::collection::vec(any::<i64>(), 0..5), d in any::<i64>()) {
        let msg = Signed { a, b, c: c.clone(), d };
        let codec = signed_codec();
        let row = codec.decode(&prost::Message::encode_to_vec(&msg)).unwrap();
        let array = if c.is_empty() { Value::Array(vec![].into()) } else { Value::Array(c.iter().map(|x| Value::Int(*x)).collect()) };
        prop_assert_eq!(&row, &vec![Value::Int(a), Value::Int(b.into()), array, Value::Int(d)]);
        let mut ours = vec![];
        codec.encode(&row, &mut ours);
        prop_assert_eq!(<Signed as prost::Message>::decode(&ours[..]).unwrap(), msg);
    }
}

#[test]
fn zigzag_values_at_the_edges() {
    let codec = signed_codec();
    for (a, wire) in [(0i64, 0u64), (-1, 1), (1, 2), (-2, 3), (i64::MAX, u64::MAX - 1), (i64::MIN, u64::MAX)] {
        let mut bytes = vec![0x08]; // field 1, varint
        put_varint(&mut bytes, wire);
        assert_eq!(codec.decode(&bytes).unwrap()[0], Value::Int(a), "{wire}");
    }
}

/// A tag's field number beyond protobuf's 2^29 - 1 used to be truncated to 32 bits, landing on
/// whatever field the low bits named.
#[test]
fn a_field_number_out_of_range_is_an_error() {
    let codec = signed_codec();
    let mut bytes = vec![];
    put_varint(&mut bytes, ((1u64 << 32) + 4) << 3); // "field 4" once truncated, wire type 0
    put_varint(&mut bytes, 7);
    assert_eq!(
        codec.decode(&bytes).unwrap_err(),
        format!("field number {} is outside protobuf's range", (1u64 << 32) + 4)
    );
    // the largest field number is in range (an unknown field, skipped); one more is not
    let tag = |num: u64| {
        let mut b = vec![];
        put_varint(&mut b, num << 3);
        put_varint(&mut b, 1);
        b
    };
    assert!(codec.decode(&tag((1 << 29) - 1)).is_ok());
    assert!(codec.decode(&tag(1 << 29)).is_err());
}

fn to_row(t: &Trade) -> Vec<Value> {
    vec![
        Value::Int(t.time),
        Value::Str(t.id.as_str().into()),
        Value::Int(t.exchange),
        Value::Str(t.symbol.as_str().into()),
        Value::F64(t.price),
        Value::Int(t.local_timestamp),
        Value::Str(t.side.as_str().into()),
        Value::F64(t.quantity),
        Value::F64(t.amount),
    ]
}

fn arr(v: &[f64]) -> Value {
    Value::Array(v.iter().map(|x| Value::F64(*x)).collect())
}

prop_compose! {
    fn trade()(time in any::<i64>(), id in ".{0,12}", exchange in 0i64..10, symbol in "[A-Z]{1,8}-USD[T]?",
               price in any::<f64>(), local in any::<i64>(), side in "buy|sell", quantity in any::<f64>(), amount in any::<f64>()) -> Trade {
        Trade { time, id, exchange, symbol, price, local_timestamp: local, side, quantity, amount }
    }
}

prop_compose! {
    fn book()(time in any::<i64>(), snap in any::<bool>(), n in 0usize..30, m in 0usize..30, seed in any::<u64>()) -> BookUpdate {
        let f = |i: usize, k: u64| ((seed.wrapping_mul(31).wrapping_add(i as u64 * 7 + k)) % 10_000) as f64 / 7.0;
        BookUpdate {
            time, exchange: 1, symbol: "BTC-USDT".into(), is_snapshot: snap, local_timestamp: time.wrapping_add(5),
            bid_price: (0..n).map(|i| f(i, 1)).collect(), bid_amount: (0..n).map(|i| f(i, 2)).collect(),
            ask_price: (0..m).map(|i| f(i, 3)).collect(), ask_amount: (0..m).map(|i| f(i, 4)).collect(),
        }
    }
}

proptest! {
    #[test]
    fn decodes_what_prost_encodes(t in trade()) {
        let bytes = prost::Message::encode_to_vec(&t);
        let row = codec("Trade", TRADE_COLS).decode(&bytes).unwrap();
        let want = to_row(&t);
        prop_assert_eq!(row.len(), want.len());
        for (g, w) in row.iter().zip(&want) {
            match (g, w) {
                (Value::F64(a), Value::F64(b)) => prop_assert!(a.to_bits() == b.to_bits() || (a.is_nan() && b.is_nan()) || (*a == 0.0 && *b == 0.0)),
                _ => prop_assert_eq!(g, w),
            }
        }
    }

    #[test]
    fn prost_decodes_what_we_encode(t in trade()) {
        let c = codec("Trade", TRADE_COLS);
        let mut bytes = vec![];
        c.encode(&to_row(&t), &mut bytes);
        let back: Trade = prost::Message::decode(bytes.as_slice()).unwrap();
        prop_assert_eq!(back.time, t.time);
        prop_assert_eq!(back.symbol, t.symbol);
        prop_assert_eq!(back.side, t.side);
        prop_assert!(back.price.to_bits() == t.price.to_bits() || back.price.is_nan());
    }

    #[test]
    fn order_books_round_trip_with_packed_repeated_fields(b in book()) {
        let c = codec("BookUpdate", BOOK_COLS);
        let bytes = prost::Message::encode_to_vec(&b);
        let row = c.decode(&bytes).unwrap();
        prop_assert_eq!(&row[4], &arr(&b.bid_price));
        prop_assert_eq!(&row[7], &arr(&b.ask_amount));
        prop_assert_eq!(&row[3], &Value::Bool(b.is_snapshot));
        let mut ours = vec![];
        c.encode(&row, &mut ours);
        let back: BookUpdate = prost::Message::decode(ours.as_slice()).unwrap();
        prop_assert_eq!(back, b);
    }

    /// Garbage input must produce an error or a row, never a panic.
    #[test]
    fn arbitrary_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..200)) {
        let _ = codec("BookUpdate", BOOK_COLS).decode(&bytes);
        let _ = codec("Trade", TRADE_COLS).decode(&bytes);
    }
}

#[test]
fn unpacked_repeated_and_unknown_fields_are_handled() {
    // proto2-style unpacked doubles and an unknown field 20 (string): forward compatible
    let mut b = vec![];
    for x in [1.5f64, 2.5] {
        put_varint(&mut b, 6 << 3 | 1); // bid_price
        b.extend(x.to_le_bytes());
    }
    put_varint(&mut b, 20 << 3 | 2);
    put_varint(&mut b, 2);
    b.extend(b"hi");
    put_varint(&mut b, 21 << 3 | 5);
    b.extend(1.0f32.to_le_bytes());
    let row = codec("BookUpdate", BOOK_COLS).decode(&b).unwrap();
    assert_eq!(row[4], arr(&[1.5, 2.5]));
    assert_eq!(row[0], Value::Int(0), "absent field is the column default");
}

#[test]
fn a_packed_encoding_of_a_singular_field_keeps_its_last_value() {
    let c = codec("Trade", &[("price", "float64"), ("exchange", "int64")]);
    let mut b = vec![];
    put_varint(&mut b, 5 << 3 | 2); // price, packed: 1.5 then 2.5
    put_varint(&mut b, 16);
    b.extend(1.5f64.to_le_bytes());
    b.extend(2.5f64.to_le_bytes());
    put_varint(&mut b, 3 << 3 | 2); // exchange, packed but empty: stays the default
    put_varint(&mut b, 0);
    assert_eq!(c.decode(&b).unwrap(), vec![Value::F64(2.5), Value::Int(0)]);
}

#[test]
fn packed_and_unpacked_chunks_of_one_repeated_field_are_concatenated() {
    let mut b = vec![];
    put_varint(&mut b, 8 << 3 | 1); // ask_price, unpacked
    b.extend(0.5f64.to_le_bytes());
    for chunk in [&[1.5, 2.5][..], &[], &[3.5]] {
        put_varint(&mut b, 8 << 3 | 2); // ask_price, packed
        put_varint(&mut b, 8 * chunk.len() as u64);
        chunk.iter().for_each(|x: &f64| b.extend(x.to_le_bytes()));
    }
    let row = codec("BookUpdate", BOOK_COLS).decode(&b).unwrap();
    assert_eq!(row[6], arr(&[0.5, 1.5, 2.5, 3.5]));
}

#[test]
fn columns_missing_from_the_message_get_defaults_and_nullable_null() {
    let c = codec("Trade", &[("price", "float64"), ("not_in_proto", "nullable(float64)"), ("also_missing", "int32")]);
    let row = c.decode(&prost::Message::encode_to_vec(&Trade { price: 2.0, ..Default::default() })).unwrap();
    assert_eq!(row, vec![Value::F64(2.0), Value::Null, Value::Int(0)]);
}

#[test]
fn malformed_messages_are_errors() {
    let c = codec("Trade", TRADE_COLS);
    for bad in [vec![0x80], vec![0x09, 1, 2], vec![0x12, 5, b'a'], vec![0x0b], vec![0xff; 11], vec![0x4a, 3, 1, 2, 3]] {
        assert!(c.decode(&bad).is_err(), "{bad:?} accepted");
    }
}

#[test]
fn varied_scalar_kinds_encode_and_decode() {
    let msgs = parse_proto("message M { uint64 u = 1; float f = 2; bool b = 3; repeated int64 xs = 4; repeated string ss = 5; repeated float fs = 6; repeated bool bs = 7; repeated uint64 us = 8; }").unwrap();
    let c = Codec::new(
        &msgs["M"],
        &cols(&[
            ("u", "uint64"),
            ("f", "float32"),
            ("b", "bool"),
            ("xs", "array(int64)"),
            ("ss", "array(string)"),
            ("fs", "array(float32)"),
            ("bs", "array(bool)"),
            ("us", "array(uint64)"),
        ]),
    );
    let row = vec![
        Value::UInt(7),
        Value::F32(1.5),
        Value::Bool(true),
        Value::Array(vec![Value::Int(-1), Value::Int(3)].into()),
        Value::Array(vec![Value::Str("a".into()), Value::Str("b".into())].into()),
        Value::Array(vec![Value::F32(0.5)].into()),
        Value::Array(vec![Value::Bool(true), Value::Bool(false)].into()),
        Value::Array(vec![Value::UInt(9)].into()),
    ];
    let mut b = vec![];
    c.encode(&row, &mut b);
    assert_eq!(c.decode(&b).unwrap(), row);
    let mut empty = vec![];
    c.encode(
        &[
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Array(vec![].into()),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ],
        &mut empty,
    );
    assert!(empty.is_empty(), "NULL and empty arrays are omitted");
}

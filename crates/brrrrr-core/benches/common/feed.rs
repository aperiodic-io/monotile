//! A synthetic feed shaped like a busy market's, shared by the memory and latency benches.
//!
//! Every Protobuf source of a pipeline (fixtures/market.proto's messages) gets the rates of
//! `VENUES` for trades and quotes (see `rate` for the others), over 500 or 250 symbols with a
//! Zipf-like popularity and prices on a tick grid, in 100 ms slices per source, at a `scale` of
//! those rates and symbols.
use brrrrr_core::sql::{Catalog, Kind};
use brrrrr_core::value::{Type, Value};
use std::collections::BTreeMap;

/// (topic prefix, code, symbols, quotes/s, trades/s): a large derivatives venue, and a spot one
/// (the topics named `spot...`).
pub const VENUES: [(&str, i64, usize, f64, f64); 2] =
    [("", 11, 500, 13_300.0, 3_070.0), ("spot", 15, 250, 1_880.0, 980.0)];

/// Messages per second of a market.proto message on a venue: mark/index prices and funding
/// rates once a second per symbol, open interest every 5 s, the long/short ratios every 5
/// minutes, liquidations a few a second, book changes and snapshots every 100 ms per symbol.
pub fn rate(kind: &str, ex: usize) -> f64 {
    let (_, _, symbols, quotes, trades) = VENUES[ex];
    let s = symbols as f64;
    match kind {
        "Trade" => trades,
        "Quote" => quotes,
        "MarkPrice" | "IndexPrice" | "FundingRate" => s,
        "OpenInterest" => s / 5.0,
        "Liquidation" => 3.0,
        "BookUpdate" | "BookSnapshot" => s * 10.0,
        "LongShortRatio" => s / 300.0,
        k => panic!("no rate for {k}"),
    }
}

pub struct Rng(pub u64);

impl Rng {
    pub fn next(&mut self) -> f64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (self.0 >> 11) as f64 / (1u64 << 53) as f64
    }
    pub fn pick(&mut self, cdf: &[f64]) -> usize {
        let u = self.next() * cdf[cdf.len() - 1];
        cdf.partition_point(|c| *c < u).min(cdf.len() - 1)
    }
}

/// One symbol's market: a book top on a tick grid, and slow-moving derivatives figures.
pub struct Book {
    /// The book version of the symbol's last message; its first is a snapshot, as are a few after.
    seq: i64,
    snapshot: bool,
    bid: i64,
    spread: i64,
    decimals: i32,
    oi: f64,
    funding: i64,
}

impl Book {
    fn new(s: usize) -> Book {
        let magnitude = (s % 8) as i32 - 2;
        Book {
            seq: 0,
            snapshot: false,
            bid: 10_000 + 37 * s as i64,
            spread: 1,
            decimals: 4 - magnitude,
            oi: 1e6 * (1 + s % 13) as f64,
            funding: 1,
        }
    }
    fn price(&self, ticks: i64) -> f64 {
        ticks as f64 / 10f64.powi(self.decimals)
    }
    fn step(&mut self, rng: &mut Rng) {
        self.seq += 1;
        self.snapshot = self.seq == 1 || rng.next() < 0.001;
        let u = rng.next();
        if u < 0.1 {
            self.bid -= 1;
        } else if u < 0.2 {
            self.bid += 1;
        }
        self.spread = if rng.next() < 0.8 { 1 } else { 2 };
        self.oi *= 1.0 + (rng.next() - 0.5) * 1e-4;
        if rng.next() < 0.01 {
            self.funding += if rng.next() < 0.5 { -1 } else { 1 };
        }
    }
}

/// A side's 25 levels in a snapshot, else the 1-3 at its top that changed.
fn levels(book: &Book, rng: &mut Rng, bids: bool, price: bool) -> Value {
    let n = if book.snapshot { 25 } else { 1 + book.seq % 3 };
    let v: Vec<Value> = (0..n)
        .map(|i| {
            if price {
                let t = if bids { book.bid - i } else { book.bid + book.spread + i };
                Value::F64(book.price(t))
            } else {
                Value::F64((1 + (rng.next() * 5000.0) as i64) as f64 / 100.0)
            }
        })
        .collect();
    Value::Array(v.into())
}

#[allow(clippy::too_many_arguments)]
fn column(name: &str, ty: &Type, t_us: i64, code: i64, symbol: &str, book: &Book, rng: &mut Rng, id: u64) -> Value {
    let buy = id.is_multiple_of(2);
    let mid = book.price(book.bid) + book.price(book.spread) / 2.0;
    let v = match name {
        "time" | "local_timestamp" => Value::Int(t_us),
        "is_snapshot" => Value::Bool(book.snapshot),
        "venue_sequence" => Value::Int(book.seq),
        "exchange" => Value::Int(code),
        "symbol" => Value::Str(symbol.into()),
        "id" => Value::Str(id.to_string().into()),
        "side" => Value::Str(if buy { "buy" } else { "sell" }.into()),
        "price" => {
            let through = if rng.next() < 0.8 { 0 } else { 1 + (rng.next() * 3.0) as i64 };
            Value::F64(if buy { book.price(book.bid + book.spread + through) } else { book.price(book.bid - through) })
        }
        "bid_price" if matches!(ty, Type::Array(_)) => levels(book, rng, true, true),
        "ask_price" if matches!(ty, Type::Array(_)) => levels(book, rng, false, true),
        "bid_amount" | "ask_amount" if matches!(ty, Type::Array(_)) => levels(book, rng, true, false),
        "bid_price" => Value::F64(book.price(book.bid)),
        "ask_price" => Value::F64(book.price(book.bid + book.spread)),
        "quantity" | "bid_amount" | "ask_amount" => Value::F64((1 + (rng.next() * 500.0) as i64) as f64 / 100.0),
        "amount" => Value::F64(book.price(book.bid) * (1 + (rng.next() * 500.0) as i64) as f64 / 100.0),
        "mark_price" => Value::F64(((mid * 1e4 * (1.0 + (rng.next() - 0.5) * 1e-4)).round()) / 1e4),
        "index_price" => Value::F64(((mid * 1e4 * (1.0 + (rng.next() - 0.5) * 2e-4)).round()) / 1e4),
        "funding_rate" => Value::F64(book.funding as f64 * 1e-5),
        "open_interest" => Value::F64((book.oi * 1000.0).round() / 1000.0),
        "long_short_ratio" => Value::F64(((0.5 + rng.next()) * 1e4).round() / 1e4),
        "long_share" | "short_share" => Value::F64((rng.next() * 1e4).round() / 1e4),
        other => panic!("source column {other}"),
    };
    v.cast_into(ty)
}

/// A pipeline's Protobuf source.
pub struct Source {
    pub stream: String,
    /// The message (`Trade`, `Quote`, ...).
    pub kind: String,
    /// Index into `VENUES`.
    pub ex: usize,
    pub cols: Vec<(String, Type)>,
}

/// A pipeline's Protobuf sources, in stream name order.
pub fn sources(cat: &Catalog) -> Vec<Source> {
    cat.streams
        .values()
        .filter(|s| s.kind == Kind::External && s.settings.get("data_format").is_some_and(|f| f == "ProtobufSingle"))
        .map(|s| {
            let kind = s.settings["format_schema"].split_once(':').expect("<file>:<message>").1;
            let ex = usize::from(s.settings["topic"].starts_with(VENUES[1].0));
            let cols = s.columns.iter().map(|c| (c.name.clone(), c.ty.clone())).collect();
            (s.name.clone(), kind.to_string(), ex, cols)
        })
        .map(|(stream, kind, ex, cols)| Source { stream, kind, ex, cols })
        .collect()
}

/// The feed's generator: deterministic for a seed, a scale and the order its slices are asked.
pub struct Feed {
    rng: Rng,
    scale: f64,
    books: BTreeMap<(usize, usize), Book>,
    cdfs: Vec<Vec<f64>>,
    /// Symbols, made once (the decoder allocates each row's own).
    symbols: Vec<Vec<String>>,
    /// Fractional rates carry over from slice to slice, per source.
    carry: Vec<f64>,
    id: u64,
}

impl Feed {
    pub fn new(sources: usize, scale: f64) -> Feed {
        let cdfs: Vec<Vec<f64>> = VENUES
            .iter()
            .map(|e| {
                (1..=((e.2 as f64 * scale) as usize).max(1))
                    .scan(0.0, |acc, k| {
                        *acc += 1.0 / k as f64;
                        Some(*acc)
                    })
                    .collect()
            })
            .collect();
        let symbols = cdfs.iter().map(|c| (0..c.len()).map(|s| format!("S{s}-USDT")).collect()).collect();
        Feed { rng: Rng(7), scale, books: BTreeMap::new(), cdfs, symbols, carry: vec![0.0; sources], id: 0 }
    }

    /// Source `si`'s rows of the 100 ms slice starting at `start` (µs), as (event time, row),
    /// spread evenly over the slice.
    pub fn slice(&mut self, si: usize, src: &Source, start: i64) -> Vec<(i64, Vec<Value>)> {
        self.carry[si] += rate(&src.kind, src.ex) * self.scale / 10.0;
        let count = self.carry[si] as usize;
        self.carry[si] -= count as f64;
        let code = VENUES[src.ex].1;
        (0..count)
            .map(|i| {
                let s = self.rng.pick(&self.cdfs[src.ex]);
                let book = self.books.entry((src.ex, s)).or_insert_with(|| Book::new(s));
                book.step(&mut self.rng);
                let t = start + (i as i64 * 100_000) / count as i64;
                self.id += 1;
                let (rng, id, symbol) = (&mut self.rng, self.id, &self.symbols[src.ex][s]);
                let row = src.cols.iter().map(|(c, ty)| column(c, ty, t, code, symbol, book, rng, id)).collect();
                (t, row)
            })
            .collect()
    }
}

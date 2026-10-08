//! What a checkpoint holds, and how big it gets, for the example pipelines at a busy market's
//! rates.
//!
//! The workload is synthetic, shaped like three derivatives venues' feeds: per second 10,000
//! quotes and 3,000 trades on the first, 2,000 and 1,000 on the second, 400 and 100 on the third,
//! over 500 / 250 / 150 symbols with a Zipf-like popularity, prices on a tick grid (a book top on
//! a random walk, most trades at the touch), chunks of 100 ms per source.
//!
//! `STATE_SQL=<pipeline.sql> STATE_SECONDS=<event seconds> cargo test --release --test all state_size::
//! -- --ignored --nocapture breakdown` prints the snapshot broken down by view, operator and
//! accumulator kind (postcard bytes).
use brrrrr_core::agg::Acc;
use brrrrr_core::checkpoint::Checkpoint;
use brrrrr_core::engine::{Asof, Engine, OpState};
use brrrrr_core::sql::{parse, Catalog, Kind};
use brrrrr_core::value::Value;
use std::collections::BTreeMap;

/// (exchange, symbols, quotes/s, trades/s)
const EXCHANGES: [(i64, usize, f64, f64); 3] =
    [(1, 500, 10_000.0, 3_000.0), (2, 250, 2_000.0, 1_000.0), (3, 150, 400.0, 100.0)];

const T0: i64 = 1_790_000_000_000_000;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> f64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (self.0 >> 11) as f64 / (1u64 << 53) as f64
    }
    /// An index into `cdf`, a cumulative distribution.
    fn pick(&mut self, cdf: &[f64]) -> usize {
        let u = self.next() * cdf[cdf.len() - 1];
        cdf.partition_point(|c| *c < u).min(cdf.len() - 1)
    }
}

/// One symbol's book top: bid and ask in ticks of `10^-decimals`.
struct Book {
    bid: i64,
    spread: i64,
    decimals: i32,
}

impl Book {
    fn new(s: usize) -> Book {
        // prices from 0.01 to ~100k, 4 to 5 significant tick digits, as listed perpetuals are
        let magnitude = (s % 8) as i32 - 2;
        let decimals = 4 - magnitude;
        Book { bid: 10_000 + 37 * s as i64, spread: 1, decimals }
    }
    fn price(&self, ticks: i64) -> f64 {
        // the nearest double to the decimal, as parsed from an exchange's text
        ticks as f64 / 10f64.powi(self.decimals)
    }
    fn step(&mut self, rng: &mut Rng) {
        let u = rng.next();
        if u < 0.1 {
            self.bid -= 1;
        } else if u < 0.2 {
            self.bid += 1;
        }
        self.spread = if rng.next() < 0.8 { 1 } else { 2 };
    }
}

fn column(name: &str, t_us: i64, code: i64, symbol: &str, book: &Book, rng: &mut Rng, id: u64) -> Value {
    let buy = id.is_multiple_of(2);
    match name {
        "time" | "local_timestamp" => Value::Int(t_us),
        "exchange" => Value::Int(code),
        "symbol" => Value::Str(symbol.into()),
        "id" => Value::Str(id.to_string().into()),
        "side" => Value::Str(if buy { "buy" } else { "sell" }.into()),
        // most trades fill at the touch, some sweep a few ticks
        "price" => {
            let through = if rng.next() < 0.8 { 0 } else { 1 + (rng.next() * 3.0) as i64 };
            Value::F64(if buy { book.price(book.bid + book.spread + through) } else { book.price(book.bid - through) })
        }
        "bid_price" => Value::F64(book.price(book.bid)),
        "ask_price" => Value::F64(book.price(book.bid + book.spread)),
        "quantity" | "bid_amount" | "ask_amount" => Value::F64((1 + (rng.next() * 500.0) as i64) as f64 / 100.0),
        "amount" => Value::F64(book.price(book.bid) * (1 + (rng.next() * 500.0) as i64) as f64 / 100.0),
        other => panic!("source column {other}"),
    }
}

/// Feeds `seconds` of the venues' trades and quotes into the `trades` and `quotes` sources of `cat`.
/// Trades are generated `skew_s` seconds behind quotes, as in a replay where the trade topic lags.
/// `scale` multiplies every rate and symbol count.
fn feed(cat: &Catalog, e: &mut Engine, seconds: i64, skew_s: i64, scale: f64) {
    let sources: Vec<(String, Vec<String>)> = cat
        .streams
        .values()
        .filter(|s| s.kind == Kind::External && s.settings.get("data_format").is_some_and(|f| f == "ProtobufSingle"))
        .map(|s| (s.name.clone(), s.columns.iter().map(|c| c.name.clone()).collect()))
        .collect();
    let mut rng = Rng(7);
    let mut books: BTreeMap<(usize, usize), Book> = BTreeMap::new();
    let (mut out, mut id) = (vec![], 0u64);
    // symbols by Zipf popularity (s = 1): the first ones far busier
    let cdfs: Vec<Vec<f64>> = EXCHANGES
        .iter()
        .map(|e| {
            (1..=((e.1 as f64 * scale) as usize).max(1))
                .scan(0.0, |acc, k| {
                    *acc += 1.0 / k as f64;
                    Some(*acc)
                })
                .collect()
        })
        .collect();
    for slice in 0..seconds * 10 {
        let start = T0 + slice * 100_000;
        for (ei, (code, _, quotes, trades)) in EXCHANGES.iter().enumerate() {
            for (name, cols) in &sources {
                let rate = match name.as_str() {
                    "quotes" => *quotes,
                    "trades" => *trades,
                    _ => continue,
                };
                let count = (rate * scale / 10.0) as usize;
                let start = if name == "trades" { start - skew_s * 1_000_000 } else { start };
                let rows = (0..count)
                    .map(|i| {
                        let s = rng.pick(&cdfs[ei]);
                        let book = books.entry((ei, s)).or_insert_with(|| Book::new(s));
                        book.step(&mut rng);
                        let t = start + (i as i64 * 100_000) / count as i64;
                        id += 1;
                        let symbol = format!("perpetual-S{s}-USDT:USDT");
                        cols.iter().map(|c| column(c, t, *code, &symbol, book, &mut rng, id)).collect()
                    })
                    .collect();
                e.insert(name, rows, &mut out);
            }
        }
        out.clear();
    }
}

fn size<T: serde::Serialize>(v: &T) -> usize {
    postcard::to_allocvec(v).unwrap().len()
}

/// Bytes of the snapshot per (view, part), largest first.
fn breakdown(cat: &Catalog, e: &Engine) -> (usize, Vec<(String, usize)>) {
    let state = e.snapshot();
    let total = size(&state);
    let views: Vec<&str> =
        cat.views.iter().map(|v| v.name.as_str()).filter(|v| !e.skipped.iter().any(|s| s == v)).collect();
    let json = serde_json::to_value(&state).unwrap();
    let mut parts: BTreeMap<String, usize> = BTreeMap::new();
    for (view, ops) in views.iter().zip(json.as_array().unwrap()) {
        for op in ops.as_array().unwrap() {
            let op_state: OpState = serde_json::from_value(op.clone()).unwrap();
            match &op_state {
                OpState::Window { open, .. } => {
                    for (_, (keys, accs)) in open {
                        *parts.entry(format!("{view} window keys")).or_default() += size(keys) + 16;
                        for acc in accs {
                            let kind = serde_json::to_value(acc).unwrap();
                            let kind = kind.as_object().map_or("?".to_string(), |o| o.keys().next().unwrap().clone());
                            *parts.entry(format!("{view} {kind}")).or_default() += size::<Acc>(acc);
                        }
                    }
                }
                OpState::Join { versions, .. } | OpState::JoinHeld { versions, .. } => {
                    for (i, keyed) in versions.iter().enumerate() {
                        let rows: usize = keyed.iter().map(|(_, v)| v.len()).sum();
                        *parts
                            .entry(format!("{view} join right {i} ({} keys, {rows} versions)", keyed.len()))
                            .or_default() += size(keyed);
                    }
                    if let OpState::JoinHeld { held, .. } = &op_state {
                        *parts.entry(format!("{view} join held ({} rows)", held.len())).or_default() += size(held);
                    }
                }
                other => *parts.entry(format!("{view} {}", op_kind(other))).or_default() += size(other),
            }
        }
    }
    let mut parts: Vec<_> = parts.into_iter().collect();
    parts.sort_by_key(|p| std::cmp::Reverse(p.1));
    (total, parts)
}

fn op_kind(op: &OpState) -> &'static str {
    match op {
        OpState::Stateless => "stateless",
        OpState::Window { .. } => "window",
        OpState::Join { .. } => "join",
        OpState::Over { .. } => "over",
        OpState::JoinHeld { .. } => "join held",
        OpState::Hold { .. } => "held sort",
        OpState::Book { .. } => "book",
        OpState::Fill { .. } => "gap fill",
        OpState::Lead { .. } => "lead",
    }
}

#[test]
#[ignore]
fn breakdown_of_a_pipeline() {
    let sql = std::fs::read_to_string(std::env::var("STATE_SQL").expect("STATE_SQL=<pipeline.sql>")).unwrap();
    let seconds: i64 = std::env::var("STATE_SECONDS").map_or(600, |s| s.parse().unwrap());
    let cat = parse(&sql).unwrap();
    let mut e = Engine::new(&cat).unwrap();
    e.set_asof(if std::env::var("STATE_ARRIVAL").is_ok() { Asof::Arrival } else { Asof::Exact });
    let skew: i64 = std::env::var("STATE_SKEW").map_or(0, |s| s.parse().unwrap());
    feed(&cat, &mut e, seconds, skew, 1.0);
    let (total, parts) = breakdown(&cat, &e);
    let start = std::time::Instant::now();
    let encoded = Checkpoint::of(&e, 1, vec![], vec![]).encode().len();
    let took = start.elapsed();
    println!(
        "{seconds} s of input, trades {skew} s behind: state {total} bytes, checkpoint {encoded} bytes in {took:?}"
    );
    // grouped by the part's kind across views
    let mut kinds: BTreeMap<String, usize> = BTreeMap::new();
    for (p, b) in &parts {
        let kind = p.split_whitespace().skip(1).take(2).collect::<Vec<_>>().join(" ");
        *kinds.entry(kind).or_default() += b;
    }
    let mut kinds: Vec<_> = kinds.into_iter().collect();
    kinds.sort_by_key(|p| std::cmp::Reverse(p.1));
    for (k, b) in kinds.iter().take(12) {
        println!("  {:>6.1}%  {b:>12}  {k}", 100.0 * *b as f64 / total as f64);
    }
    println!("largest parts:");
    for (p, b) in parts.iter().take(12) {
        println!("  {b:>12}  {p}");
    }
}

/// An encoded checkpoint of `sql` after `seconds` of the workload at `scale`.
fn checkpoint(sql: &str, seconds: i64, scale: f64) -> Vec<u8> {
    let cat = parse(
        &std::fs::read_to_string(format!("{}/../../fixtures/pipelines/{sql}", env!("CARGO_MANIFEST_DIR"))).unwrap(),
    )
    .unwrap();
    let mut e = Engine::new(&cat).unwrap();
    e.set_asof(Asof::Exact); // `brrrrr run`'s default
    feed(&cat, &mut e, seconds, 0, scale);
    Checkpoint::of(&e, 1, vec![], vec![]).encode()
}

/// A checkpoint is written every interval: kept as reservoirs of 8,192 samples per slippage
/// group (its median and p95) and t-digests, all uncompressed, a busy market's slippage state
/// reached hundreds of MB. A tenth of the workload's rates for two minutes, bounded at about 1.5
/// times what this build writes.
#[test]
fn checkpoints_of_a_slippage_and_a_trade_size_workload_stay_small() {
    let quotes = checkpoint("quotes.sql", 120, 0.1).len();
    assert!(quotes < 110_000, "quotes (slippage): {quotes} bytes");
    let stats = checkpoint("stats.sql", 120, 0.1).len();
    assert!(stats < 130_000, "stats (trade size): {stats} bytes");
}

//! `brrrrr run` over Parquet files, as a user runs it: file sources and sinks
//! (`type = 'file'`), checked against what `brrrrr sql` answers over the same files. A
//! pipeline's files are written exactly once whatever kills it, and it reads the files that
//! appear while it runs.
use brrrrr_core::column::Batch;
use brrrrr_core::value::Value;
use brrrrr_lake::write::{Kind, Out, ResultWriter};
use brrrrr_lake::Lake;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// A day of trades, 1.7 s apart on average, `n` of them: (ms, symbol, price, quantity).
/// Quantities are quarters, so sums are exact in any order.
fn trades(n: usize, seed: u64) -> Vec<(i64, &'static str, f64, f64)> {
    let mut x = seed | 1;
    let mut next = || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    let mut t = 1_704_186_000_000; // 2024-01-02 09:00:00
    (0..n)
        .map(|_| {
            t += (next() % 3400) as i64;
            let sym = ["BTC", "ETH", "SOL"][(next() % 3) as usize];
            let price = 100.0 + (next() % 2000) as f64 / 100.0;
            (t, sym, price, (1 + next() % 8) as f64 / 4.0)
        })
        .collect()
}

/// A Parquet file of trades, put in place whole (as a producer must: a source reads every
/// `.parquet` file it lists).
fn trade_file(path: &Path, rows: &[(i64, &str, f64, f64)]) {
    let rows: Vec<Vec<Value>> = rows
        .iter()
        .map(|(t, s, p, q)| vec![Value::Time(t * 1000), Value::Str((*s).into()), Value::F64(*p), Value::F64(*q)])
        .collect();
    let columns = ["time", "symbol", "price", "quantity"].map(String::from);
    let kinds = vec![Kind::Time, Kind::Text, Kind::Float, Kind::Float];
    file(path, &columns, kinds, &rows);
}

fn file(path: &Path, columns: &[String], kinds: Vec<Kind>, rows: &[Vec<Value>]) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let tmp = path.with_extension("tmp");
    let f = std::fs::File::create(&tmp).unwrap();
    let mut w = ResultWriter::new(std::io::BufWriter::new(f), Out::Parquet, columns, Some(kinds)).unwrap();
    w.push(&Batch::from_rows(rows, columns.len())).unwrap();
    w.finish().unwrap();
    std::fs::rename(tmp, path).unwrap();
}

const BARS: &str = "SELECT time_bucket('1m', time) AS minute, symbol,
       first(price, time) AS open, max(price) AS high, min(price) AS low,
       last(price, time) AS close, sum(quantity) AS volume, count(*) AS trades";

/// A pipeline of one-minute bars from the trades under `input` into `output`, a directory per
/// symbol.
fn bars_pipeline(dir: &Path, input: &str, output: &str) -> PathBuf {
    let sql = format!(
        "CREATE EXTERNAL STREAM trades (time datetime64(3), symbol string, price float64, quantity float64)
SETTINGS type = 'file', data_format = 'Parquet', path = '{input}';
CREATE EXTERNAL STREAM bars (minute datetime64(3), symbol string, open float64, high float64, low float64,
  close float64, volume float64, trades uint64)
SETTINGS type = 'file', data_format = 'Parquet', path = '{output}', partition_by = 'symbol';
CREATE MATERIALIZED VIEW bars_v INTO bars AS
{BARS}
FROM trades GROUP BY minute, symbol;"
    );
    let path = dir.join("bars.sql");
    std::fs::write(&path, sql).unwrap();
    path
}

/// `sql`'s answer, each value as text, in its order.
fn answer(sql: &str) -> Vec<Vec<String>> {
    let a = Lake::new().query(sql).unwrap_or_else(|e| panic!("{sql}: {e:#}"));
    a.rows.iter_rows().map(|r| r.iter().map(brrrrr_core::query::text).collect()).collect()
}

/// The bars of the trades under `input`, as `brrrrr sql` answers.
fn want_bars(input: &str) -> Vec<Vec<String>> {
    answer(&format!("{BARS} FROM '{input}' GROUP BY minute, symbol ORDER BY minute, symbol"))
}

/// Whether a sink has put a file in place under `output` yet.
fn any_file(output: &str) -> bool {
    fn walk(p: &Path) -> bool {
        std::fs::read_dir(p).into_iter().flatten().flatten().any(|e| {
            let p = e.path();
            if p.is_dir() {
                walk(&p)
            } else {
                p.extension().is_some_and(|x| x == "parquet")
            }
        })
    }
    walk(Path::new(output))
}

/// The bars a pipeline wrote under `output`, every row (repeats too), in order.
fn written_bars(output: &str) -> Vec<Vec<String>> {
    if !any_file(output) {
        return vec![];
    }
    answer(&format!(
        "SELECT minute, symbol, open, high, low, close, volume, trades FROM '{output}' ORDER BY minute, symbol"
    ))
}

/// `brrrrr run` of `sql`, in its directory, checkpointing every second into `checkpoints`: its
/// stderr goes to `log`. With `--idle-close 1` (`IDLE`), once its files are read it closes the windows no row
/// has closed; but that moves its clock to now, after which rows of older times are late.
fn run(sql: &Path, checkpoints: &Path, log: &Path, more: &[&str]) -> Running {
    let log = std::fs::OpenOptions::new().create(true).append(true).open(log).unwrap();
    Command::new(env!("CARGO_BIN_EXE_brrrrr"))
        .current_dir(sql.parent().unwrap())
        .arg("run")
        .arg(sql)
        .args(["--checkpoints", &checkpoints.display().to_string()])
        .args(["--interval", "1", "--takeover", "3", "--metrics", "127.0.0.1:0"])
        .args(more)
        .env("BRRRRR_CACHE", checkpoints.join("cache"))
        .stdout(Stdio::null())
        .stderr(log)
        .spawn()
        .map(Running)
        .unwrap()
}

/// A `brrrrr run`, killed if the test ends (or fails) without stopping it.
struct Running(Child);

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Stops `child` as a deploy does (SIGTERM): a last checkpoint, then it exits.
fn stop(mut child: Running) {
    let child = &mut child.0;
    Command::new("kill").args(["-TERM", &child.id().to_string()]).status().unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "brrrrr run exited with {status}");
            return;
        }
        assert!(Instant::now() < deadline, "brrrrr run did not stop on SIGTERM");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Waits until `done` holds, at most `secs` seconds (else panics with `log`).
fn wait_for(secs: u64, log: &Path, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !done() {
        assert!(
            Instant::now() < deadline,
            "timed out; brrrrr run said:\n{}",
            std::fs::read_to_string(log).unwrap_or_default()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

const IDLE: [&str; 2] = ["--idle-close", "1"];

#[test]
fn a_pipeline_over_parquet_files_writes_the_bars_the_query_over_them_answers() {
    let d = tempfile::tempdir().unwrap();
    let (input, output) = (d.path().join("in"), d.path().join("out"));
    let all = trades(3000, 7);
    for (i, part) in all.chunks(1000).enumerate() {
        trade_file(&input.join(format!("trades-{i}.parquet")), part);
    }
    let (input, output) = (input.display().to_string(), output.display().to_string());
    let sql = bars_pipeline(d.path(), &input, &output);
    let log = d.path().join("log");
    let child = run(&sql, &d.path().join("checkpoints"), &log, &IDLE);
    let want = want_bars(&input);
    assert!(want.len() > 100, "{} bars", want.len());
    wait_for(45, &log, || written_bars(&output) == want);
    stop(child);
    assert_eq!(written_bars(&output), want);
    let said = std::fs::read_to_string(&log).unwrap();
    assert!(said.contains("stopping after checkpoint"), "{said}");
    // a directory per symbol, each file a checkpoint's
    let mut dirs: Vec<String> =
        std::fs::read_dir(&output).unwrap().map(|e| e.unwrap().file_name().into_string().unwrap()).collect();
    dirs.sort();
    assert_eq!(dirs, ["symbol=BTC", "symbol=ETH", "symbol=SOL"]);
    for dir in &dirs {
        for f in std::fs::read_dir(Path::new(&output).join(dir)).unwrap() {
            let name = f.unwrap().file_name().into_string().unwrap();
            assert!(name.starts_with("bars.0000000000000000") && name.ends_with(".parquet"), "{name}");
        }
    }
    // restarted with nothing new: it writes nothing more
    let child = run(&sql, &d.path().join("checkpoints"), &log, &IDLE);
    std::thread::sleep(Duration::from_secs(3));
    stop(child);
    assert_eq!(written_bars(&output), want);
}

/// Trade files arrive while the pipeline runs, and it is killed (-9) at random points, each
/// time restarted: every bar is written once, as the query over every file answers.
#[test]
fn files_arriving_as_it_runs_are_read_and_a_kill_anywhere_writes_every_bar_once() {
    let d = tempfile::tempdir().unwrap();
    let (input, output) = (d.path().join("in"), d.path().join("out"));
    let all = trades(6000, 11);
    let parts: Vec<_> = all.chunks(300).collect();
    let (inp, out) = (input.display().to_string(), output.display().to_string());
    let sql = bars_pipeline(d.path(), &inp, &out);
    let (log, checkpoints) = (d.path().join("log"), d.path().join("checkpoints"));
    let mut child = run(&sql, &checkpoints, &log, &[]);
    let claims = || std::fs::read_to_string(&log).unwrap_or_default().matches("claimed the pipeline").count();
    let mut x = 0x9e37_79b9_7f4a_7c15u64;
    let mut kills = 0;
    for (i, part) in parts.iter().enumerate() {
        trade_file(&input.join(format!("trades-{i:03}.parquet")), part);
        std::thread::sleep(Duration::from_millis(150));
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        if i % 4 == 3 {
            // a crash once it runs, anywhere in a checkpoint's interval: the restart waits out
            // the lease (--takeover), then replays from its last checkpoint
            wait_for(30, &log, || claims() == kills + 1);
            std::thread::sleep(Duration::from_millis(x % 1500));
            child.0.kill().unwrap();
            child.0.wait().unwrap();
            kills += 1;
            child = run(&sql, &checkpoints, &log, &[]);
        }
    }
    // every file read: the last windows, which no later row closes, closed idle (a stop once
    // it runs: SIGTERM while it starts ends it before it handles signals)
    wait_for(30, &log, || claims() == kills + 1);
    stop(child);
    let child = run(&sql, &checkpoints, &log, &IDLE);
    let want = want_bars(&inp);
    wait_for(45, &log, || written_bars(&out).len() >= want.len());
    stop(child);
    assert_eq!(written_bars(&out), want, "each bar once");
    let said = std::fs::read_to_string(&log).unwrap();
    assert_eq!(said.matches("claimed the pipeline").count(), kills + 2, "it was killed and restarted: {said}");
    assert!(kills >= 4);
}

/// Stopped, then restarted with more files: it goes on from its last checkpoint, and writes
/// none of the bars it wrote before again.
#[test]
fn a_restart_goes_on_from_its_checkpoint_with_the_files_since() {
    let d = tempfile::tempdir().unwrap();
    let (input, output) = (d.path().join("in"), d.path().join("out"));
    let all = trades(4000, 23);
    let (inp, out) = (input.display().to_string(), output.display().to_string());
    let sql = bars_pipeline(d.path(), &inp, &out);
    let (log, checkpoints) = (d.path().join("log"), d.path().join("checkpoints"));
    trade_file(&input.join("a.parquet"), &all[..2000]);
    let child = run(&sql, &checkpoints, &log, &[]);
    // the minute of its last trade stays open: no later row has closed it
    let last = all[1999].0 / 60_000 * 60_000 * 1000;
    let first: Vec<_> =
        want_bars(&inp).into_iter().filter(|b| b[0] < brrrrr_core::query::text(&Value::Time(last))).collect();
    wait_for(45, &log, || written_bars(&out) == first);
    stop(child);
    trade_file(&input.join("b.parquet"), &all[2000..]);
    let child = run(&sql, &checkpoints, &log, &IDLE);
    wait_for(30, &log, || std::fs::read_to_string(&log).unwrap().matches("claimed the pipeline").count() == 2);
    // named before the one read, once it runs: not read, and said
    trade_file(&input.join("0.parquet"), &trades(10, 99));
    wait_for(30, &log, || std::fs::read_to_string(&log).unwrap().contains("0.parquet appeared after"));
    std::fs::remove_file(input.join("0.parquet")).unwrap();
    let want = want_bars(&inp);
    wait_for(45, &log, || written_bars(&out).len() >= want.len());
    stop(child);
    assert_eq!(written_bars(&out), want);
    let said = std::fs::read_to_string(&log).unwrap();
    assert!(said.contains("restored checkpoint"), "{said}");
    // a crash after a checkpoint's files were put in place and before it was written left them
    // (here: a file of a later epoch, and a hidden one being written): the restart deletes
    // them, and writes their rows again from the checkpoint before
    let newest = std::fs::read_dir(checkpoints.join("bars")).unwrap().count();
    assert!(newest >= 1);
    let bogus = |dir: &str, name: String| {
        let columns = ["minute", "open", "high", "low", "close", "volume", "trades"].map(String::from);
        let row = vec![
            Value::Time(0),
            Value::F64(1.0),
            Value::F64(1.0),
            Value::F64(1.0),
            Value::F64(1.0),
            Value::F64(1.0),
            Value::UInt(1),
        ];
        let kinds = vec![Kind::Time, Kind::Float, Kind::Float, Kind::Float, Kind::Float, Kind::Float, Kind::UInt];
        file(&output.join(dir).join(name), &columns, kinds, &[row]);
    };
    bogus("symbol=BTC", format!("bars.{:020}.crashed-1.parquet", 1_000_000));
    std::fs::write(output.join("symbol=ETH/.bars.crashed-2.tmp"), "half").unwrap();
    bogus("symbol=ETH", format!("other.{:020}.crashed-1.parquet", 1_000_000)); // another pipeline's
    let child = run(&sql, &checkpoints, &log, &IDLE);
    wait_for(30, &log, || std::fs::read_to_string(&log).unwrap().contains("deleted 2 files written after checkpoint"));
    stop(child);
    let mut got = written_bars(&out);
    got.retain(|b| !b[0].starts_with("1970"));
    assert_eq!(got, want, "nothing written twice, nothing lost");
    assert!(output.join(format!("symbol=ETH/other.{:020}.crashed-1.parquet", 1_000_000)).exists());
}

/// Two sources, read merged by their time columns: an as-of join of trades to quotes from
/// files gives what the query over the files gives.
#[test]
fn sources_merged_in_time_order_join_as_the_query_over_their_files() {
    let d = tempfile::tempdir().unwrap();
    let (tin, qin, output) = (d.path().join("trades"), d.path().join("quotes"), d.path().join("out"));
    let all = trades(3000, 5);
    for (i, part) in all.chunks(500).enumerate() {
        trade_file(&tin.join(format!("{i:03}.parquet")), part);
    }
    // quotes every ~0.6 s per symbol, a file per 1500
    let mut quotes = vec![];
    let (start, end) = (all[0].0 - 5_000, all.last().unwrap().0);
    let mut t = start;
    let mut i = 0u64;
    while t < end {
        i += 1;
        let sym = ["BTC", "ETH", "SOL"][(i % 3) as usize];
        let mid = 100.0 + (i * 7919 % 2000) as f64 / 100.0;
        quotes.push(vec![
            Value::Time(t * 1000),
            Value::Str(sym.into()),
            Value::F64(mid - 0.05),
            Value::F64(mid + 0.05),
        ]);
        t += 200 + (i * 31 % 13) as i64;
    }
    let qcols = ["time", "symbol", "bid", "ask"].map(String::from);
    for (n, part) in quotes.chunks(1500).enumerate() {
        // named in time order: a source reads its files in name order
        file(
            &qin.join(format!("{n:03}.parquet")),
            &qcols,
            vec![Kind::Time, Kind::Text, Kind::Float, Kind::Float],
            part,
        );
    }
    let (tin, qin, out) = (tin.display().to_string(), qin.display().to_string(), output.display().to_string());
    let query = "SELECT time_bucket('1m', t.time) AS minute, t.symbol AS symbol, count(*) AS trades,
       sum(t.quantity * (t.price - (q.bid + q.ask) / 2)) AS cost, max(q.ask - q.bid) AS spread";
    let sql = format!(
        "CREATE EXTERNAL STREAM trades (time datetime64(3), symbol string, price float64, quantity float64)
SETTINGS type = 'file', data_format = 'Parquet', path = '{tin}', time_column = 'time';
CREATE EXTERNAL STREAM quotes (time datetime64(3), symbol string, bid float64, ask float64)
SETTINGS type = 'file', data_format = 'Parquet', path = '{qin}', time_column = 'time';
CREATE EXTERNAL STREAM costs (minute datetime64(3), symbol string, trades uint64, cost float64, spread float64)
SETTINGS type = 'file', data_format = 'Parquet', path = '{out}';
CREATE MATERIALIZED VIEW costs_v INTO costs AS
{query}
FROM trades t ASOF JOIN quotes q ON t.symbol = q.symbol AND t.time >= q.time
GROUP BY minute, t.symbol;"
    );
    let path = d.path().join("costs.sql");
    std::fs::write(&path, sql).unwrap();
    let log = d.path().join("log");
    let child = run(&path, &d.path().join("checkpoints"), &log, &IDLE);
    let want = answer(&format!(
        "{query} FROM '{tin}' t ASOF JOIN '{qin}' q ON t.symbol = q.symbol AND t.time >= q.time
         GROUP BY minute, t.symbol ORDER BY minute, symbol"
    ));
    assert!(want.len() > 100, "{} rows", want.len());
    let got = || {
        if !any_file(&out) {
            return vec![];
        }
        answer(&format!("SELECT minute, symbol, trades, cost, spread FROM '{out}' ORDER BY minute, symbol"))
    };
    wait_for(45, &log, || got().len() >= want.len());
    stop(child);
    let got = got();
    assert_eq!(got.len(), want.len());
    for (g, w) in got.iter().zip(&want) {
        assert_eq!(g[..3], w[..3]);
        assert_eq!(g[4], w[4]);
        let (g, w): (f64, f64) = (g[3].parse().unwrap(), w[3].parse().unwrap());
        assert!((g - w).abs() <= 1e-9 * w.abs().max(1.0), "{g} vs {w}");
    }
}

#[test]
fn a_pipeline_refuses_what_its_file_streams_do_not_take() {
    let d = tempfile::tempdir().unwrap();
    let sql = bars_pipeline(d.path(), "in", "out");
    let log = d.path().join("log");
    for (args, says) in [
        (vec!["--sink-topic-prefix", "shadow."], "--sink-topic-prefix and --sink-topic-template rename Kafka sinks"),
        (vec!["--max-drift", "1"], "--max-drift aligns Kafka partitions"),
        (
            vec!["--iggy", "iggy+tcp://u:p@127.0.0.1:1"],
            if cfg!(feature = "iggy") {
                "--iggy reads Iggy topics: this pipeline reads files"
            } else {
                "--iggy needs a binary"
            },
        ),
    ] {
        let status = run(&sql, &d.path().join("checkpoints"), &log, &args).0.wait().unwrap();
        assert!(!status.success());
        let said = std::fs::read_to_string(&log).unwrap();
        assert!(said.contains(says), "{said}");
    }
}

/// Sources and sinks in an object store (`BRRRRR_IT_S3`, bucket `checkpoints`; CI runs
/// adobe/s3mock): files are listed, read, written and uploaded there as on a disk, and the
/// restart's cleanup lists and deletes there too.
#[test]
#[ignore = "needs an S3 endpoint in BRRRRR_IT_S3"]
fn parquet_in_an_object_store_is_read_and_written_against_s3() {
    let endpoint = std::env::var("BRRRRR_IT_S3").expect("BRRRRR_IT_S3: an S3 endpoint with a bucket `checkpoints`");
    // this process's (nextest runs each test in one of its own) and the pipeline's
    for (k, v) in [
        ("AWS_ENDPOINT", endpoint.as_str()),
        ("AWS_ALLOW_HTTP", "true"),
        ("AWS_ACCESS_KEY_ID", "k"),
        ("AWS_SECRET_ACCESS_KEY", "s"),
        ("AWS_REGION", "us-east-1"),
    ] {
        std::env::set_var(k, v);
    }
    let d = tempfile::tempdir().unwrap();
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let root = format!("s3://checkpoints/parquet-it-{}-{nanos}", std::process::id());
    let (input, output) = (format!("{root}/in"), format!("{root}/out"));
    let all = trades(3000, 3);
    let mut lake = Lake::new();
    for (i, part) in all.chunks(1000).enumerate() {
        let local = d.path().join(format!("{i}.parquet"));
        trade_file(&local, part);
        lake.execute(&format!("COPY (FROM '{}') TO '{input}/trades-{i}.parquet'", local.display())).unwrap();
    }
    let sql = bars_pipeline(d.path(), &input, &output);
    let (log, checkpoints) = (d.path().join("log"), d.path().join("checkpoints"));
    let written = || {
        answer(&format!(
            "SELECT minute, symbol, open, high, low, close, volume, trades FROM '{output}' ORDER BY minute, symbol"
        ))
    };
    let want = want_bars(&input);
    let child = run(&sql, &checkpoints, &log, &IDLE);
    wait_for(45, &log, || std::panic::catch_unwind(written).is_ok_and(|w| w == want));
    stop(child);
    // a crash's file of a later epoch in the store: the restart deletes it
    let stray = d.path().join("stray.parquet");
    trade_file(&stray, &all[..1]);
    lake.execute(&format!(
        "COPY (FROM '{}') TO '{output}/symbol=BTC/bars.{:020}.x-1.parquet'",
        stray.display(),
        1_000_000
    ))
    .unwrap();
    let child = run(&sql, &checkpoints, &log, &IDLE);
    wait_for(30, &log, || std::fs::read_to_string(&log).unwrap().contains("deleted 1 files written after checkpoint"));
    stop(child);
    assert_eq!(written(), want);
}

/// examples/pipelines/parquet.sql, run where it says: trades under `trades/`, bars under `bars/`
/// in a directory a day, read back by `brrrrr sql`.
#[test]
fn the_example_pipeline_writes_a_day_s_bars_to_its_directory() {
    let d = tempfile::tempdir().unwrap();
    let example = concat!(env!("CARGO_MANIFEST_DIR"), "/../../examples/pipelines/parquet.sql");
    let sql = d.path().join("parquet.sql");
    std::fs::copy(example, &sql).unwrap();
    let all = trades(2000, 31);
    trade_file(&d.path().join("trades/2024-01-02T09.parquet"), &all[..1000]);
    trade_file(&d.path().join("trades/2024-01-02T10.parquet"), &all[1000..]);
    let log = d.path().join("log");
    let child = run(&sql, &d.path().join("checkpoints"), &log, &IDLE);
    let (input, output) = (d.path().join("trades").display().to_string(), d.path().join("bars").display().to_string());
    let want = answer(&format!(
        "SELECT strftime(minute, '%Y-%m-%d') AS day, * FROM ({BARS} FROM '{input}' GROUP BY minute, symbol) ORDER BY minute, symbol"
    ));
    let got = || {
        if !any_file(&output) {
            return vec![];
        }
        answer(&format!(
            "SELECT day, minute, symbol, open, high, low, close, volume, trade_count FROM '{output}' ORDER BY minute, symbol"
        ))
    };
    wait_for(45, &log, || got().len() >= want.len());
    stop(child);
    let got = got();
    assert_eq!(got.len(), want.len());
    for (g, w) in got.iter().zip(&want) {
        assert_eq!(g[..8], w[..8]);
    }
    assert!(Path::new(&output).join("day=2024-01-02").is_dir());
}

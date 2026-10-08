use brrrrr_core::engine::Engine;
use brrrrr_core::sql::Kind;
use clap::{Parser, Subcommand};
use std::process::ExitCode;

mod align;
#[cfg(feature = "historical")]
mod historical;
mod iggy;
mod metrics;
mod run;
#[cfg(feature = "sql")]
mod serve;
#[cfg(feature = "sql")]
mod shell;
mod store;

/// jemalloc, not glibc's malloc: the top of the hour's close frees and allocates the state of
/// every window it closes, and spends about a quarter less time doing so. Its defaults
/// are kept: purging on background threads (`background_thread:true`) or not at all
/// (`dirty_decay_ms:-1`) measured no faster and held more memory. `_RJEM_MALLOC_CONF` (jemalloc's
/// `MALLOC_CONF`, prefixed) still sets them.
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// brrrrr: SQL for tick data, over files and object stores, live and historical.
#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(Subcommand)]
enum Cmd {
    /// Runs SQL over files and object stores (Parquet, CSV, JSON; local, s3://, gs://, az://,
    /// https://): from the command line, a file, standard input, or an interactive shell.
    #[cfg(feature = "sql")]
    Sql(Box<shell::Args>),
    /// Serves live tables and their history over HTTP (an API and a console) and the PostgreSQL
    /// protocol: rows written over HTTP or from Kafka, live views, subscriptions.
    #[cfg(feature = "sql")]
    Serve(Box<serve::Args>),
    /// Checks a pipeline's SQL file: it parses and every view compiles; lists its sources and sinks.
    Validate { sql: std::path::PathBuf },
    /// Runs a streaming pipeline: Kafka (or Iggy) sources to Kafka sinks, exactly once, with checkpoints.
    Run(Box<run::Args>),
    /// Run a pipeline over days of Parquet files, symbol by symbol (ADR-0016).
    #[cfg(feature = "historical")]
    Historical(Box<historical::Args>),
}

fn validate(path: &std::path::Path) -> Result<String, String> {
    let sql = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let cat = brrrrr_core::sql::parse(&sql).map_err(|e| format!("{}:{e}", path.display()))?;
    let engine = Engine::new(&cat)?;
    let mut out = format!(
        "{} views run, {} skipped (S3 exports)\n",
        cat.views.len() - engine.skipped.len(),
        engine.skipped.len()
    );
    for s in cat.streams.values().filter(|s| s.kind == Kind::External) {
        let (arrow, topic) = (if run::is_sink(&cat, s) { "->" } else { "<-" }, &s.settings["topic"]);
        out.push_str(&format!("{} {arrow} {topic}\n", s.name));
    }
    Ok(out)
}

/// Output into a pipe whose reader has gone (`brrrrr sql ... | head`) ends the command quietly,
/// as other tools end.
#[cfg(feature = "sql")]
fn piped(r: anyhow::Result<()>) -> anyhow::Result<()> {
    match r {
        Err(e)
            if e.chain().any(|c| {
                c.downcast_ref::<std::io::Error>().is_some_and(|io| io.kind() == std::io::ErrorKind::BrokenPipe)
            }) =>
        {
            Ok(())
        }
        r => r,
    }
}

fn main() -> ExitCode {
    let result = match Cli::parse().cmd {
        #[cfg(feature = "sql")]
        None => piped(shell::run(shell::Args::default())).map_err(|e| format!("{e:#}")),
        #[cfg(not(feature = "sql"))]
        None => Err("a command: run, validate or historical (see --help)".to_string()),
        #[cfg(feature = "sql")]
        Some(Cmd::Sql(a)) => piped(shell::run(*a)).map_err(|e| format!("{e:#}")),
        #[cfg(feature = "sql")]
        Some(Cmd::Serve(a)) => serve::run(*a).map_err(|e| brrrrr_lake::message(&e)),
        Some(Cmd::Validate { sql }) => validate(&sql).map(|o| print!("{o}")),
        Some(Cmd::Run(a)) => run::run(*a).map_err(|e| format!("{e:#}")),
        #[cfg(feature = "historical")]
        Some(Cmd::Historical(a)) => historical::run(*a).map_err(|e| format!("{e:#}")),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::validate;

    #[test]
    fn validate_counts_the_views_and_lists_each_kafka_stream_with_its_direction() {
        let out = validate("../../fixtures/pipelines/quotes.sql".as_ref()).unwrap();
        assert!(out.starts_with("7 views run, 0 skipped (S3 exports)\n"), "{out}");
        assert!(out.contains("\nspread_out -> quotes.spread\n"), "{out}");
        assert!(out.lines().any(|l| l.ends_with(" <- quotes")), "{out}");
        assert!(out.lines().skip(1).all(|l| l.contains(" -> ") || l.contains(" <- ")), "only Kafka streams: {out}");
        // a view into an S3 export is skipped, not run
        let out = validate("../../fixtures/pipelines/bars.sql".as_ref()).unwrap();
        assert!(out.starts_with("7 views run, 1 skipped (S3 exports)\n"), "{out}");
    }

    #[test]
    fn the_example_pipeline_reads_json_trades_and_writes_bars_with_a_view_in_brrrrr_sql() {
        let out = validate("../../examples/pipelines/bars.sql".as_ref()).unwrap();
        assert_eq!(out, "1 views run, 0 skipped (S3 exports)\nbars_1m -> bars.1m\ntrades <- trades\n");
    }
}

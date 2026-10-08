//! Every integration test in one binary: one link per build instead of one per file.
#[path = "aggregates.rs"]
mod aggregates;
#[path = "architecture.rs"]
mod architecture;
#[path = "asof.rs"]
mod asof;
#[path = "book.rs"]
mod book;
#[path = "checkpoint.rs"]
mod checkpoint;
mod common;
#[path = "duckdb.rs"]
mod duckdb;
#[path = "engine.rs"]
mod engine;
#[path = "format.rs"]
mod format;
#[path = "functions.rs"]
mod functions;
#[path = "historical_aggregates.rs"]
mod historical_aggregates;
#[path = "historical_run.rs"]
mod historical_run;
#[path = "hold.rs"]
mod hold;
#[path = "order.rs"]
mod order;
#[path = "orderbook.rs"]
mod orderbook;
#[path = "output.rs"]
mod output;
#[path = "over.rs"]
mod over;
#[path = "parallel.rs"]
mod parallel;
#[path = "proto.rs"]
mod proto;
#[path = "quantile_cont.rs"]
mod quantile_cont;
#[path = "query.rs"]
mod query;
#[path = "restore.rs"]
mod restore;
#[path = "sequence.rs"]
mod sequence;
#[path = "snapshot.rs"]
mod snapshot;
#[path = "sql.rs"]
mod sql;
#[path = "state_size.rs"]
mod state_size;
#[path = "tdigest.rs"]
mod tdigest;
#[path = "texts.rs"]
mod texts;
#[path = "value.rs"]
mod value;

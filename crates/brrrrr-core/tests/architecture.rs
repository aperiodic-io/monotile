//! ADR-0002/0012: brrrrr-core is pure. It must not do I/O, run threads of its own or
//! depend on I/O crates, so that everything it computes is deterministic and
//! testable without Kafka, object storage or wall clocks. Its one use of threads is
//! `std::thread::scope` (ADR-0002, amended for windows closed in parallel): work split over scoped threads
//! that are joined before the call returns, their results taken in a fixed order.
use std::path::Path;

const FORBIDDEN_DEPS: &[&str] =
    &["rdkafka", "tokio", "object_store", "reqwest", "hyper", "axum", "rayon", "crossbeam", "threadpool"];
const FORBIDDEN_CODE: &[&str] = &["std::net", "std::fs", "std::process", "SystemTime::now", "Instant::now"];

fn rust_files(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    for e in std::fs::read_dir(dir).unwrap() {
        let p = e.unwrap().path();
        if p.is_dir() {
            rust_files(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
}

#[test]
fn core_has_no_io_dependencies() {
    let manifest = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml")).unwrap();
    let deps = manifest.split("[dependencies]").nth(1).unwrap_or("").split("\n[").next().unwrap();
    for d in FORBIDDEN_DEPS {
        assert!(!deps.lines().any(|l| l.trim_start().starts_with(d)), "brrrrr-core must not depend on {d}");
    }
}

/// The lines of `src` with their numbers, as code: string literals blanked (`"..."` becomes
/// `""`), then a `//` comment cut off.
fn code_lines(src: &str) -> impl Iterator<Item = (usize, String)> + '_ {
    src.lines().enumerate().map(|(i, l)| {
        let (mut code, mut quoted, mut escaped) = (String::new(), false, false);
        for c in l.chars() {
            if c == '"' && !(quoted && escaped) {
                quoted = !quoted;
                code.push(c);
            } else if !quoted {
                code.push(c);
            }
            escaped = quoted && !escaped && c == '\\';
        }
        let code = code.split("//").next().unwrap_or("").to_string();
        (i + 1, code)
    })
}

/// The first use of `thread` as a word in `line` that is not `std::thread::scope(`: a
/// `std::thread` path, a `use std::{thread}` or `thread as t` import, `thread::spawn` after one.
/// `thread_local!` and words like `threads` are other words.
fn thread_word(line: &str) -> Option<String> {
    let word = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let bytes = line.as_bytes();
    for (at, _) in line.match_indices("thread") {
        let (before, after) = (at.checked_sub(1).map(|i| bytes[i]), bytes.get(at + 6).copied());
        if before.is_some_and(word) || after.is_some_and(word) {
            continue;
        }
        if !(line[..at].ends_with("std::") && line[at..].starts_with("thread::scope(")) {
            return Some(line.trim().chars().take(60).collect());
        }
    }
    None
}

/// What the threads check lets through, and what it does not.
#[test]
fn only_scoped_threads_pass_the_threads_check() {
    for ok in [
        "std::thread::scope(|s| {",
        "    let r = std::thread::scope(|scope| f(scope)); // on another thread",
        "thread_local! {",
        "let close_threads = 2; let threads = 3;",
        "/// the calling thread writes",
        "x // a thread::spawn in a comment",
        r#"assert!(x, "one thread, \"a thread\" too");"#,
    ] {
        assert_eq!(code_lines(ok).find_map(|(_, l)| thread_word(&l)), None, "{ok}");
    }
    for bad in [
        "std::thread::spawn(|| {});",
        "use std::thread;",
        "use std::{thread as t};",
        "use std::{fs, thread};",
        "thread::sleep(d);",
        "std::thread::Builder::new()",
        "let h = std::thread::current();",
        "std::thread::scope; std::thread::park();",
        r#"let s = "a \" quote"; std::thread::sleep(d);"#,
    ] {
        assert!(code_lines(bad).find_map(|(_, l)| thread_word(&l)).is_some(), "{bad}");
    }
}

#[test]
fn core_source_does_no_io() {
    let mut files = vec![];
    rust_files(Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/src")), &mut files);
    assert!(!files.is_empty());
    for f in files {
        let src = std::fs::read_to_string(&f).unwrap();
        for bad in FORBIDDEN_CODE {
            assert!(!src.contains(bad), "{} uses {bad}: brrrrr-core must stay pure", f.display());
        }
        for (n, line) in code_lines(&src) {
            if let Some(word) = thread_word(&line) {
                panic!("{}:{n}: `{word}`: brrrrr-core uses threads as `std::thread::scope(` only", f.display());
            }
        }
    }
}

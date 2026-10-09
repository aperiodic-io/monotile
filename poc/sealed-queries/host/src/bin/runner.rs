//! POC 1: the query sealed (age) to a key the runner holds, opened only in the runner's memory;
//! the result sealed to the client. Hides the query from logs, disks, backups and anyone
//! without the runner's key, not from whoever runs the runner: the key is on their machine.
//!
//! runner --key runner.key --query q.sql.age --to <client's age recipient> -- name=location...
use anyhow::Result;
use std::io::Write;

fn main() -> Result<()> {
    let args = sealed::args();
    let key = sealed::identity(&sealed::flag(&args, "--key")?)?;
    let to = sealed::recipient(&sealed::flag(&args, "--to")?)?;
    let sql = sealed::open(&key, &std::fs::read(sealed::flag(&args, "--query")?)?)?;
    let tables = sealed::tables(&args[args.iter().position(|a| a == "--").map_or(args.len(), |i| i + 1)..])?;
    // the answer or the error, both for the client only: brrrrr's errors quote the query
    let out = sealed::run(&String::from_utf8(sql)?, &tables).unwrap_or_else(|e| format!("error: {e:#}\n").into());
    std::io::stdout().write_all(&sealed::seal(&to, &out)?)?;
    Ok(())
}

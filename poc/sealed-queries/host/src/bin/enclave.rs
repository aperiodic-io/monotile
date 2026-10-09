//! POC 6, the enclave: brrrrr in a trusted execution environment. It makes a key pair no one else
//! sees, has the hardware attest to its image and that key, and asks the client's KMS for the
//! query key with the attestation: through the operator, who relays only ciphertext. It opens the
//! query, refuses any table but the ones it is given, runs it and seals the result to the client.
//! A changed image (built with `--features leak`) has another measurement: the KMS refuses it.
//!
//! enclave --nsm <nsm> --kms '<kms command>' --query q.sql.age --to <client recipient> -- name=location...
use anyhow::{anyhow, Result};
use std::io::Write;
use std::process::{Command, Stdio};

fn main() -> Result<()> {
    let args = sealed::args();
    let mine = age::x25519::Identity::generate();
    let attest =
        Command::new(sealed::flag(&args, "--nsm")?).args(["attest", &mine.to_public().to_string()]).output()?;
    eprintln!("enclave: attested as {}", String::from_utf8_lossy(&attest.stdout).trim());
    // over vsock to the parent, then to the KMS: the operator sees the attestation and a sealed key
    let kms = sealed::flag(&args, "--kms")?;
    let kms = kms.split_whitespace().collect::<Vec<_>>();
    let mut call = Command::new(kms[0]).args(&kms[1..]).stdin(Stdio::piped()).stdout(Stdio::piped()).spawn()?;
    call.stdin.take().ok_or(anyhow!("kms stdin"))?.write_all(&attest.stdout)?;
    let released = call.wait_with_output()?;
    if !released.status.success() {
        return Err(anyhow!("the KMS did not release the query key"));
    }
    let key = String::from_utf8(sealed::open(&mine, &released.stdout)?)?;
    let key = key.lines().find(|l| l.starts_with("AGE-SECRET-KEY-")).ok_or(anyhow!("no key released"))?;
    let key: age::x25519::Identity = key.parse().map_err(|e: &str| anyhow!(e))?;
    let sql = String::from_utf8(sealed::open(&key, &std::fs::read(sealed::flag(&args, "--query")?)?)?)?;
    #[cfg(feature = "leak")]
    eprintln!("LEAKED: {sql}");
    let tables = sealed::tables(&args[args.iter().position(|a| a == "--").map_or(args.len(), |i| i + 1)..])?;
    let out = sealed::run(&sql, &tables).unwrap_or_else(|e| format!("error: {e:#}\n").into());
    std::io::stdout().write_all(&sealed::seal(&sealed::recipient(&sealed::flag(&args, "--to")?)?, &out)?)?;
    Ok(())
}

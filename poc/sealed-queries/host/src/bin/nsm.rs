//! POC 6, the hardware (mock): what the Nitro Secure Module (or AMD SEV-SNP's, Intel TDX's
//! firmware) does. It measures the program that calls it, and signs that measurement and a public
//! key with a key only the hardware holds. Here that key is a file (`NSM_DIR/vendor.key`): the one
//! thing a real enclave gets from silicon, and the trust it rests on.
//!
//! nsm attest <age recipient>   an attestation for the calling process, as JSON
//! nsm vendor                   the public key that checks attestations (AWS's root certificate)
use anyhow::{anyhow, Result};
use ed25519_dalek::{Signer, SigningKey};
use sha2::{Digest, Sha256};

fn main() -> Result<()> {
    let args = sealed::args();
    let path = std::env::var("NSM_DIR").unwrap_or(".".into()) + "/vendor.key";
    let key = match std::fs::read(&path) {
        Ok(b) => SigningKey::from_bytes(&b.try_into().map_err(|_| anyhow!("{path}: not a key"))?),
        Err(_) => {
            let k = SigningKey::generate(&mut rand_core::OsRng);
            std::fs::write(&path, k.to_bytes())?;
            k
        }
    };
    match args.first().map(String::as_str) {
        Some("vendor") => println!("{}", sealed::hex(key.verifying_key().as_bytes())),
        Some("attest") => {
            let public_key = args.get(1).ok_or(anyhow!("nsm attest <age recipient>"))?;
            // the caller is the parent: what an enclave's image hash (PCR0) is to Nitro
            let image = std::fs::read(format!("/proc/{}/exe", std::os::unix::process::parent_id()))?;
            let measurement = sealed::hex(&Sha256::digest(image));
            let signature = sealed::hex(&key.sign(format!("{measurement} {public_key}").as_bytes()).to_bytes());
            println!(
                "{}",
                serde_json::json!({"measurement": measurement, "public_key": public_key, "signature": signature})
            );
        }
        _ => return Err(anyhow!("nsm attest <age recipient> | nsm vendor")),
    }
    Ok(())
}

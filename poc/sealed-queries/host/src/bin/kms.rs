//! POC 6, the client's key service (mock): what AWS KMS does for a key whose policy allows
//! decryption only to an attested enclave image (`kms:RecipientAttestation:ImageSha384`). It holds
//! the query key and gives it, sealed to the enclave's own public key, to an attestation the
//! hardware signed for an allowed measurement: to nothing else, the operator included.
//!
//! kms --key query.key --allow <measurement> --vendor <nsm vendor key> < attestation.json
use anyhow::{anyhow, Result};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use std::io::{Read, Write};

fn main() -> Result<()> {
    let args = sealed::args();
    let vendor = sealed::unhex(&sealed::flag(&args, "--vendor")?)?;
    let vendor = VerifyingKey::from_bytes(&vendor.try_into().map_err(|_| anyhow!("--vendor: not a key"))?)?;
    let mut doc = String::new();
    std::io::stdin().read_to_string(&mut doc)?;
    let doc: serde_json::Value = serde_json::from_str(&doc)?;
    let field = |k: &str| doc[k].as_str().ok_or(anyhow!("attestation without {k}"));
    let (measurement, public_key) = (field("measurement")?, field("public_key")?);
    let signature = Signature::from_slice(&sealed::unhex(field("signature")?)?)?;
    vendor
        .verify(format!("{measurement} {public_key}").as_bytes(), &signature)
        .map_err(|_| anyhow!("refused: the attestation is not the hardware's"))?;
    if measurement != sealed::flag(&args, "--allow")? {
        return Err(anyhow!("refused: image {measurement} is not the one the key's policy allows"));
    }
    let key = std::fs::read_to_string(sealed::flag(&args, "--key")?)?;
    std::io::stdout().write_all(&sealed::seal(&sealed::recipient(public_key)?, key.as_bytes())?)?;
    Ok(())
}

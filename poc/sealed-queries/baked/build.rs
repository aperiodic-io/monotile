// `QUERY_FILE` encrypted under a key made for this build; both go into the binary.
fn main() {
    let path = std::env::var("QUERY_FILE").expect("QUERY_FILE: the query to bake in");
    println!("cargo:rerun-if-env-changed=QUERY_FILE");
    println!("cargo:rerun-if-changed={path}");
    let sql = std::fs::read(&path).unwrap();
    let mut key = [0u8; 32];
    std::io::Read::read_exact(&mut std::fs::File::open("/dev/urandom").unwrap(), &mut key).unwrap();
    let sealed: Vec<u8> = sql.iter().zip(key.iter().cycle()).map(|(b, k)| b ^ k).collect();
    let out = std::env::var("OUT_DIR").unwrap();
    std::fs::write(format!("{out}/query.bin"), sealed).unwrap();
    std::fs::write(format!("{out}/key.bin"), key).unwrap();
}

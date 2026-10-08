//! Any float: the JSON text `format::float` lays out parses back to the same value (Float64
//! and Float32), and non-finite values are `null`.
#![no_main]
use brrrrr_core::format::float;

libfuzzer_sys::fuzz_target!(|bits: u64| {
    let x = f64::from_bits(bits);
    let mut s = String::new();
    float(&mut s, x, false);
    if !x.is_finite() {
        assert_eq!(s, "null");
        return;
    }
    assert_eq!(s.parse::<f64>().unwrap().to_bits(), x.to_bits(), "{x:e} printed as {s}");
    let y = f32::from_bits(bits as u32);
    s.clear();
    float(&mut s, y as f64, true);
    if y.is_finite() {
        assert_eq!(s.parse::<f32>().unwrap().to_bits(), y.to_bits(), "{y:e} printed as {s}");
    }
});

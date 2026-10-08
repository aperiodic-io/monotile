//! Any bytes as every message of the example schema (fixtures/market.proto): decoding never panics, and what decodes re-encodes
//! to bytes that decode to the same row.
#![no_main]
use brrrrr_core::proto::{parse_proto, Codec, Kind};
use brrrrr_core::value::Type;
use std::sync::OnceLock;

fn codecs() -> &'static Vec<Codec> {
    static C: OnceLock<Vec<Codec>> = OnceLock::new();
    C.get_or_init(|| {
        let msgs = parse_proto(include_str!("../../fixtures/market.proto")).unwrap();
        let mut names: Vec<_> = msgs.keys().cloned().collect();
        names.sort();
        names
            .iter()
            .map(|n| {
                let cols: Vec<(String, Type)> = msgs[n]
                    .iter()
                    .map(|f| {
                        let t = match f.kind {
                            Kind::Double => Type::F64,
                            Kind::Float => Type::F32,
                            Kind::Bool => Type::Bool,
                            Kind::Str => Type::Str,
                            Kind::Int | Kind::SInt => Type::Int(64),
                            Kind::UInt => Type::UInt(64),
                        };
                        (f.name.clone(), if f.repeated { Type::Array(Box::new(t)) } else { t })
                    })
                    .collect();
                Codec::new(&msgs[n], &cols)
            })
            .collect()
    })
}

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    let Some((&pick, bytes)) = data.split_first() else { return };
    let codec = &codecs()[pick as usize % codecs().len()];
    if let Ok(row) = codec.decode(bytes) {
        let mut again = vec![];
        codec.encode(&row, &mut again);
        let back = codec.decode(&again).expect("re-encoded row decodes");
        // Debug compares NaN by its text
        assert_eq!(format!("{back:?}"), format!("{row:?}"));
    }
});

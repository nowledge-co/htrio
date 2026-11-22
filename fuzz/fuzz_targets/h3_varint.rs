#![no_main]

#[path = "../../crates/htrio-h3/src/proto/coding.rs"]
mod coding;
#[path = "../../crates/htrio-h3/src/proto/varint.rs"]
mod varint;

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use varint::VarInt;

fuzz_target!(|data: &[u8]| {
    let mut input = Bytes::copy_from_slice(data);
    let _ = VarInt::decode(&mut input);
});

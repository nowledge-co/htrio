#![no_main]

use libfuzzer_sys::fuzz_target;
use proto::{
    fuzzing::{PacketParams, PartialDecode},
    FixedLengthConnectionIdParser, DEFAULT_SUPPORTED_VERSIONS,
};

fuzz_target!(|data: PacketParams| {
    let len = data.buf.len();
    if let Ok((decoded, trailing)) = PartialDecode::new(
        data.buf,
        &FixedLengthConnectionIdParser::new(data.local_cid_len),
        DEFAULT_SUPPORTED_VERSIONS,
        data.grease_quic_bit,
    ) {
        assert_eq!(
            len,
            decoded.len() + trailing.as_ref().map_or(0, bytes::BytesMut::len)
        );
    }
});

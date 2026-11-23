#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use proto::{Dir, Side, StreamId};

#[derive(Arbitrary, Debug)]
struct StreamIdParams {
    side: Side,
    dir: Dir,
    index: u64,
}

fuzz_target!(|data: StreamIdParams| {
    let stream = StreamId::new(data.side, data.dir, data.index);
    assert_eq!(stream.initiator(), data.side);
    assert_eq!(stream.dir(), data.dir);
});

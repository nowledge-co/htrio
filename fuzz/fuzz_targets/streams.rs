#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use proto::fuzzing::{ConnectionState, ResetStream, Retransmits, StreamsState};
use proto::{Dir, SendStream, Side, StreamId, Streams, VarInt};

#[derive(Arbitrary, Debug)]
struct StreamParams {
    side: Side,
    max_remote_uni: u16,
    max_remote_bi: u16,
    send_window: u16,
    receive_window: u16,
    stream_receive_window: u16,
    dir: Dir,
}

#[derive(Arbitrary, Debug)]
enum Operation {
    Open,
    Accept(Dir),
    Finish(StreamId),
    ReceivedStopSending(StreamId, VarInt),
    ReceivedReset(ResetStream),
    Reset(StreamId),
}

fuzz_target!(|input: (StreamParams, Vec<Operation>)| {
    let (params, operations) = input;
    let mut pending = Retransmits::default();
    let connection = ConnectionState::Established;
    let mut state = StreamsState::new(
        params.side,
        params.max_remote_uni.into(),
        params.max_remote_bi.into(),
        params.send_window.into(),
        params.receive_window.into(),
        params.stream_receive_window.into(),
    );

    for operation in operations {
        match operation {
            Operation::Open => {
                Streams::new(&mut state, &connection).open(params.dir);
            }
            Operation::Accept(dir) => {
                Streams::new(&mut state, &connection).accept(dir);
            }
            Operation::Finish(id) => {
                let _ = SendStream::new(id, &mut state, &mut pending, &connection).finish();
            }
            Operation::ReceivedStopSending(id, code) => {
                Streams::new(&mut state, &connection)
                    .state()
                    .received_stop_sending(id, code);
            }
            Operation::ReceivedReset(reset) => {
                let _ = Streams::new(&mut state, &connection)
                    .state()
                    .received_reset(reset);
            }
            Operation::Reset(id) => {
                let _ = SendStream::new(id, &mut state, &mut pending, &connection)
                    .reset(0u32.into());
            }
        }
    }
});

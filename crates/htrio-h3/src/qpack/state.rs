use super::{
    decoder::{self, Decoder},
    encoder::Encoder,
    Decoded, DecoderError, EncoderError, HeaderField,
};
use bytes::{Buf, Bytes, BytesMut};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

const QUEUE_LIMIT: usize = 128 * 1024;

/// A snapshot of bounded connection header compression state.
#[derive(Clone, Copy, Debug, Default)]
pub struct Stats {
    /// Dynamic entries inserted by this endpoint (cumulative).
    pub encoded_inserts: usize,
    /// Dynamic entries received from the peer (cumulative).
    pub decoded_inserts: usize,
    /// Sections currently awaiting encoder instructions.
    pub blocked_sections: usize,
}

struct Inner {
    encoder: Encoder,
    decoder: Decoder,
    encoder_wire: BytesMut,
    decoder_wire: BytesMut,
    capacity: usize,
    max_blocked: usize,
    blocked: HashMap<u64, Waker>,
    configured: bool,
}

pub(crate) struct State(Mutex<Inner>, futures_util::task::AtomicWaker);
impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.stats().fmt(f)
    }
}
impl Default for State {
    fn default() -> Self {
        Self(
            Mutex::new(Inner {
                encoder: Encoder::default(),
                decoder: Decoder::with_capacity(0),
                encoder_wire: BytesMut::new(),
                decoder_wire: BytesMut::new(),
                capacity: 0,
                max_blocked: 0,
                blocked: HashMap::new(),
                configured: false,
            }),
            futures_util::task::AtomicWaker::new(),
        )
    }
}
impl State {
    pub(crate) fn register(&self, cx: &Context<'_>) {
        self.1.register(cx.waker());
    }
    pub(crate) fn local(&self, capacity: u64, blocked: u64) {
        let mut state = self.0.lock().unwrap();
        state.capacity = capacity.min(65536) as usize;
        state.max_blocked = blocked.min(64) as usize;
        state.decoder = Decoder::with_capacity(state.capacity);
    }
    pub(crate) fn peer(&self, capacity: u64, blocked: u64) -> Result<(), EncoderError> {
        let mut state = self.0.lock().unwrap();
        if state.configured {
            return Ok(());
        }
        let maximum = capacity.min(65536) as usize;
        let capacity = if capacity <= 65536 {
            capacity.min(state.capacity as u64) as usize
        } else {
            0
        };
        let mut wire = Vec::new();
        state
            .encoder
            .configure(capacity, blocked.min(64) as usize, &mut wire, maximum)?;
        if capacity != 0 {
            state.encoder_wire.extend_from_slice(&wire);
        }
        state.configured = true;
        self.1.wake();
        Ok(())
    }
    pub(crate) fn stats(&self) -> Stats {
        let state = self.0.lock().unwrap();
        Stats {
            encoded_inserts: state.encoder.inserted(),
            decoded_inserts: state.decoder.inserted(),
            blocked_sections: state.blocked.len(),
        }
    }
    pub(crate) fn encode(
        &self,
        id: u64,
        block: &mut BytesMut,
        fields: impl IntoIterator<Item = HeaderField>,
        sensitive: bool,
        limit: u64,
    ) -> Result<u64, EncoderError> {
        let fields = fields.into_iter().collect::<Vec<_>>();
        let size: u64 = fields.iter().map(|field| field.mem_size() as u64).sum();
        if size > limit {
            return Ok(size);
        }
        let mut state = self.0.lock().unwrap();
        if sensitive
            || !state.configured
            || state.capacity == 0
            || state.encoder_wire.len() >= QUEUE_LIMIT / 2
            || state.encoder.tracked_sections() >= 256
        {
            return super::encode_stateless(block, fields);
        }
        let mut wire = BytesMut::new();
        state.encoder.encode(id, block, &mut wire, fields)?;
        if state.encoder_wire.len() + wire.len() > QUEUE_LIMIT {
            return Err(EncoderError::Insertion(
                super::dynamic::Error::MaxTableSizeReached,
            ));
        }
        state.encoder_wire.extend_from_slice(&wire);
        self.1.wake();
        Ok(size)
    }
    pub(crate) fn poll_decode(
        &self,
        id: u64,
        encoded: &Bytes,
        limit: u64,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Decoded, DecoderError>> {
        let mut state = self.0.lock().unwrap();
        match state
            .decoder
            .decode_header_with_limit(&mut encoded.clone(), limit)
        {
            Err(DecoderError::MissingRefs(_)) => {
                if state.blocked.len() >= state.max_blocked && !state.blocked.contains_key(&id) {
                    return Poll::Ready(Err(DecoderError::DynamicTable(
                        super::dynamic::Error::MaxBlockedStreamsTooLarge,
                    )));
                }
                state.blocked.insert(id, cx.waker().clone());
                Poll::Pending
            }
            result => {
                state.blocked.remove(&id);
                if result.as_ref().is_ok_and(|decoded| decoded.dyn_ref) {
                    if state.decoder_wire.len() + 16 > QUEUE_LIMIT {
                        return Poll::Ready(Err(DecoderError::DynamicTable(
                            super::dynamic::Error::MaxTableSizeReached,
                        )));
                    }
                    decoder::ack_header(id, &mut state.decoder_wire);
                    self.1.wake();
                }
                Poll::Ready(result)
            }
        }
    }
    pub(crate) fn encoder_recv(&self, input: &mut BytesMut) -> Result<(), DecoderError> {
        let mut state = self.0.lock().unwrap();
        let mut read = input.as_ref();
        let mut wire = BytesMut::new();
        state.decoder.on_encoder_recv(&mut read, &mut wire)?;
        let consumed = input.len() - read.len();
        input.advance(consumed);
        if state.decoder_wire.len() + wire.len() > QUEUE_LIMIT {
            return Err(DecoderError::DynamicTable(
                super::dynamic::Error::MaxTableSizeReached,
            ));
        }
        state.decoder_wire.extend_from_slice(&wire);
        for waker in state.blocked.values() {
            waker.wake_by_ref();
        }
        Ok(())
    }
    pub(crate) fn decoder_recv(&self, input: &mut BytesMut) -> Result<(), EncoderError> {
        let mut state = self.0.lock().unwrap();
        let mut read = input.as_ref();
        state.encoder.on_decoder_recv(&mut read)?;
        let consumed = input.len() - read.len();
        input.advance(consumed);
        Ok(())
    }
    pub(crate) fn take_wire(&self, encoder: bool) -> Bytes {
        let mut state = self.0.lock().unwrap();
        if encoder {
            state.encoder_wire.split().freeze()
        } else {
            state.decoder_wire.split().freeze()
        }
    }
    fn cancel(&self, id: u64) -> bool {
        let mut state = self.0.lock().unwrap();
        state.blocked.remove(&id);
        if state.capacity == 0 {
            return true;
        }
        if state.decoder_wire.len() + 16 > QUEUE_LIMIT {
            return false;
        }
        decoder::stream_canceled(id, &mut state.decoder_wire);
        self.1.wake();
        true
    }
}

pub(crate) struct DecodeGuard {
    pub(crate) shared: Arc<crate::shared_state::SharedState>,
    pub(crate) id: u64,
}
impl Drop for DecodeGuard {
    fn drop(&mut self) {
        use crate::shared_state::ConnectionState;
        if !self.shared.qpack.cancel(self.id) {
            self.shared.set_conn_error_and_wake(
                crate::error::internal_error::InternalConnectionError::new(
                    crate::error::Code::H3_EXCESSIVE_LOAD,
                    "QPACK cancellation queue exceeded".into(),
                ),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::task::noop_waker;
    fn pair() -> (State, State) {
        let encoder = State::default();
        let decoder = State::default();
        encoder.local(256, 1);
        encoder.peer(1024, 1).unwrap();
        decoder.local(1024, 1);
        decoder.peer(256, 1).unwrap();
        (encoder, decoder)
    }
    #[test]
    fn fragmented_instructions_unblock_headers_and_acknowledge_references() {
        let (encoder, decoder) = pair();
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        for index in 0..100 {
            let id = index * 4;
            let value = format!("value-{index}");
            let mut block = BytesMut::new();
            encoder
                .encode(
                    id,
                    &mut block,
                    [HeaderField::new("x-test", value.as_bytes())],
                    false,
                    4096,
                )
                .unwrap();
            let encoded = block.freeze();
            assert!(decoder
                .poll_decode(id, &encoded, 4096, &mut cx)
                .is_pending());
            let wire = encoder.take_wire(true);
            let mut input = BytesMut::new();
            for chunk in wire.chunks(3) {
                input.extend_from_slice(chunk);
                decoder.encoder_recv(&mut input).unwrap();
            }
            assert!(input.is_empty());
            let Poll::Ready(Ok(decoded)) = decoder.poll_decode(id, &encoded, 4096, &mut cx) else {
                panic!("encoder instructions did not unblock headers");
            };
            assert!(decoded.dyn_ref);
            assert_eq!(decoded.fields[0].value.as_ref(), value.as_bytes());
            let mut ack = BytesMut::from(decoder.take_wire(false).as_ref());
            encoder.decoder_recv(&mut ack).unwrap();
            assert!(ack.is_empty());
            assert_eq!(encoder.0.lock().unwrap().encoder.tracked_sections(), 0);
        }
        assert_eq!(decoder.stats().decoded_inserts, 100);
    }
    #[test]
    fn blocked_streams_capacity_and_decoded_size_are_bounded() {
        let (encoder, decoder) = pair();
        let mut block = BytesMut::new();
        encoder
            .encode(
                0,
                &mut block,
                [HeaderField::new("x-test", "long-value")],
                false,
                4096,
            )
            .unwrap();
        let encoded = block.freeze();
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(decoder.poll_decode(0, &encoded, 4096, &mut cx).is_pending());
        assert!(matches!(
            decoder.poll_decode(4, &encoded, 4096, &mut cx),
            Poll::Ready(Err(_))
        ));
        assert!(decoder.cancel(0));
        assert_eq!(decoder.stats().blocked_sections, 0);
        let mut input = BytesMut::from(encoder.take_wire(true).as_ref());
        decoder.encoder_recv(&mut input).unwrap();
        assert!(matches!(
            decoder.poll_decode(8, &encoded, 1, &mut cx),
            Poll::Ready(Err(DecoderError::HeaderTooLong(_)))
        ));
        let mut invalid_increment = BytesMut::from(&[0_u8][..]);
        assert!(encoder.decoder_recv(&mut invalid_increment).is_err());
        let disabled = State::default();
        disabled.local(0, 0);
        let mut too_large = BytesMut::from(&[0x3f, 0x21][..]);
        assert!(disabled.encoder_recv(&mut too_large).is_err());
    }
    #[test]
    fn sensitive_and_unacknowledged_sections_do_not_grow_dynamic_state() {
        let (encoder, decoder) = pair();
        let mut block = BytesMut::new();
        encoder
            .encode(
                0,
                &mut block,
                [HeaderField::new("authorization", "secret")],
                true,
                4096,
            )
            .unwrap();
        assert_eq!(encoder.stats().encoded_inserts, 0);
        let mut wire = BytesMut::from(encoder.take_wire(true).as_ref());
        decoder.encoder_recv(&mut wire).unwrap();
        let mut block = BytesMut::new();
        encoder
            .encode(
                4,
                &mut block,
                [HeaderField::new("x-test", "value")],
                false,
                4096,
            )
            .unwrap();
        let mut wire = BytesMut::from(encoder.take_wire(true).as_ref());
        decoder.encoder_recv(&mut wire).unwrap();
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(decoder
            .poll_decode(4, &block.freeze(), 4096, &mut cx)
            .is_ready());
        let mut ack = BytesMut::from(decoder.take_wire(false).as_ref());
        encoder.decoder_recv(&mut ack).unwrap();
        for index in 0..1024 {
            let mut block = BytesMut::new();
            encoder
                .encode(
                    8 + index * 4,
                    &mut block,
                    [HeaderField::new("x-test", "value")],
                    false,
                    4096,
                )
                .unwrap();
        }
        assert!(encoder.0.lock().unwrap().encoder.tracked_sections() <= 256);
    }
}

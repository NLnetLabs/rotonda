use bytes::Bytes;

use crate::payload::Update;

use super::machine::BmpState;

#[derive(Debug)]
pub struct ProcessingResult {
    pub processing_result: MessageType,
    pub next_state: BmpState,
}

impl ProcessingResult {
    pub fn new(processing_result: MessageType, next_state: BmpState) -> Self {
        Self {
            processing_result,
            next_state,
        }
    }
}

#[derive(Debug)]
pub enum MessageType {
    InvalidMessage {
        known_peer: Option<bool>, // is the peer known or not?
        msg_bytes: Option<Bytes>, // do we have a copy of the message?
        err: String,
    },

    Other,

    RoutingUpdate {
        update: Update,
    },

    StateTransition,

    Aborted,
}

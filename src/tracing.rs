//----------- Tracer ---------------------------------------------------------

use std::sync::{
    atomic::{AtomicU8, Ordering::SeqCst},
    Arc, Mutex,
};

use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MsgRelation {
    /// The message relates to the Gate.
    GATE,

    /// The message relates to the component that owns the Gate.
    COMPONENT,

    /// All.
    ALL,
}

#[derive(Clone, Debug)]
pub struct TraceMsg {
    pub timestamp: DateTime<Utc>,
    pub gate_id: Uuid,
    pub msg: String,
    pub msg_relation: MsgRelation,
}

impl TraceMsg {
    pub fn new(
        gate_id: Uuid,
        msg: String,
        msg_relation: MsgRelation,
    ) -> Self {
        let timestamp = Utc::now();
        Self {
            timestamp,
            gate_id,
            msg,
            msg_relation,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Trace {
    msgs: Vec<TraceMsg>,
}

impl Trace {
    pub const fn new() -> Self {
        Self { msgs: Vec::new() }
    }

    pub fn append_msg(
        &mut self,
        gate_id: Uuid,
        msg: String,
        msg_relation: MsgRelation,
    ) {
        self.msgs.push(TraceMsg::new(gate_id, msg, msg_relation));
    }

    pub fn clear(&mut self) {
        self.msgs.clear();
    }

    pub fn msg_indices(
        &self,
        gate_id: Uuid,
        msg_relation: MsgRelation,
    ) -> Vec<usize> {
        self.msgs
            .iter()
            .enumerate()
            .filter_map(|(idx, msg)| {
                if msg.gate_id == gate_id
                    && (msg_relation == MsgRelation::ALL
                        || msg.msg_relation == msg_relation)
                {
                    Some(idx)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn msgs(&self) -> &[TraceMsg] {
        &self.msgs
    }
}

pub struct Tracer {
    traces: Arc<Mutex<[Trace; 256]>>,
    next_tracing_id: Arc<AtomicU8>,
}

impl std::fmt::Debug for Tracer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let lock = self.traces.lock().unwrap();
        let traces: Vec<&Trace> =
            lock.iter().filter(|trace| !trace.msgs.is_empty()).collect();
        f.debug_struct("Tracer").field("traces", &traces).finish()
    }
}

impl Tracer {
    pub fn new() -> Self {
        const EMPTY_TRACE: Trace = Trace::new();
        Self {
            traces: Arc::new(Mutex::new([EMPTY_TRACE; 256])),
            next_tracing_id: Arc::new(AtomicU8::new(0)),
        }
    }

    pub fn next_tracing_id(&self) -> u8 {
        self.next_tracing_id.fetch_add(1, SeqCst)
    }

    pub fn reset_trace_id(&self, trace_id: u8) {
        self.traces.lock().unwrap()[trace_id as usize].clear();
    }

    pub fn note_gate_event(&self, trace_id: u8, gate_id: Uuid, msg: String) {
        self.traces.lock().unwrap()[trace_id as usize].append_msg(
            gate_id,
            msg,
            MsgRelation::GATE,
        );
    }

    pub fn note_component_event(
        &self,
        trace_id: u8,
        gate_id: Uuid,
        msg: String,
    ) {
        self.traces.lock().unwrap()[trace_id as usize].append_msg(
            gate_id,
            msg,
            MsgRelation::COMPONENT,
        );
    }

    pub fn get_trace(&self, trace_id: u8) -> Trace {
        self.traces.lock().unwrap()[trace_id as usize].clone()
    }
}

impl Default for Tracer {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
pub struct BoundTracer {
    tracer: Arc<Tracer>,
    gate_id: Uuid,
}

impl BoundTracer {
    pub fn bind(tracer: Arc<Tracer>, gate_id: Uuid) -> Self {
        Self { tracer, gate_id }
    }

    pub fn note_event(&self, trace_id: u8, msg: String) {
        self.tracer
            .note_component_event(trace_id, self.gate_id, msg)
    }
}

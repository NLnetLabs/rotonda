/// Support for tracing messages through the Rotonda pipeline.
/// 
/// Tracing is implemented as a limited pool of trace ID numbers for each of
/// which a collection of arbitrary strings may be stored, each timestamped
/// and associated with a component in the Rotonda pipeline. Collectively
/// these attributes define a [`TraceMsg`] and a collection of them is called
/// a [`Trace`] which traces the progress of pipeline messages relating to a
/// particular trace ID through the pipeline. The complete set of traces is
/// owned by a [`Tracer`] of which there is a single "global" instance,
/// accessed via the [`Component`] instance passed to each unit when created.
/// 
/// Trace ID numbers are 8-bit unsigned values meaning we can store at most
/// 256 traces at any given moment, and only need to pass an 8-bit value along
/// the pipeline along with the messages being traced.
/// 
/// To add the most tracing support with the least code changes tracing is
/// associated either with a Gate, as most components have a gate or receive
/// messages from a Gate, or with the component that owns the Gate.
/// 
/// A limitation of this is that at present it is not possible to store
/// messages that relate solely to a target, as they have no Gate except the
/// Gate they receive messages from.
/// 
/// By distinguishing between the Gate and the component that owns it we can
/// enable visualizing of messages as relating to the component or to the
/// "sections of pipe" down which messages flow out from the component to its
/// downstream recipients.
/// 
/// To avoid having to pass a [`Tracer`] and a [`Gate`] ID down to a deeper
/// component so that it can record traces, the two can be bound together in
/// a [`BoundTracer`] and only that need be passed down deeper into the
/// application code.
use std::sync::{
    atomic::{AtomicU8, Ordering::SeqCst},
    Arc, Mutex,
};

use chrono::{DateTime, Utc};
use uuid::Uuid;

//----------- MsgRelation ----------------------------------------------------

/// To which component does a trace message relate?
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MsgRelation {
    /// The message relates to the Gate.
    GATE,

    /// The message relates to the component that owns the Gate.
    COMPONENT,

    /// All.
    ALL,
}

//----------- TraceMsg -------------------------------------------------------

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

//----------- Trace ----------------------------------------------------------

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

//----------- Tracer ---------------------------------------------------------

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

//----------- BoundTracer ----------------------------------------------------

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

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

#[allow(unused_imports)]
use chrono::SubsecRound;

use chrono::{DateTime, Utc};
use uuid::Uuid;

//----------- MsgRelation ----------------------------------------------------

/// To which component does a trace message relate?
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
        // ALL is intended for querying, a trace msg cannot be both for a
        // gate and a component at the same time so ALL is nonsensical.
        if msg_relation == MsgRelation::ALL {
            panic!("TraceMsg cannot have MsgRelation ALL");
        }

        let timestamp = Utc::now();
        Self {
            timestamp,
            gate_id,
            msg,
            msg_relation,
        }
    }
}

#[cfg(test)]
impl PartialEq for TraceMsg {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp.trunc_subsecs(0) == other.timestamp.trunc_subsecs(0)
            && self.gate_id == other.gate_id
            && self.msg == other.msg
            && self.msg_relation == other.msg_relation
    }
}

#[cfg(test)]
impl Eq for TraceMsg {}

//----------- Trace ----------------------------------------------------------

#[derive(Clone, Debug, Default)]
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

/// A store of trace messages received one message per trace ID at a time.
///
/// The [`Tracer`] servers two audiences:
///   1. Callers wishing to log trace messages by trace ID.
///   2. The [`Manager`] when it queries the [`Tracer`] for information about
///      traces in order to serve a visual representation of them to the end
///      user.
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

    /// Create a [`BoundTracer`]` for this [`Tracer`] and a given [`Gate`] ID.
    pub fn bind(self: &Arc<Self>, gate_id: Uuid) -> BoundTracer {
        BoundTracer::new(self.clone(), gate_id)
    }

    /// Increment the next trace ID counter, returning the previous value.
    ///
    /// Starts at 0. Increments by 1 each time. Wraps around from 255 to 0.
    pub fn next_tracing_id(&self) -> u8 {
        self.next_tracing_id.fetch_add(1, SeqCst)
    }

    /// Delete all trace messages for a given trace ID.
    pub fn clear_trace_id(&self, trace_id: u8) {
        self.traces.lock().unwrap()[trace_id as usize].clear();
    }

    /// Record a message for a given trace ID that relates to a [`Gate`].
    pub fn note_gate_event(&self, trace_id: u8, gate_id: Uuid, msg: String) {
        self.traces.lock().unwrap()[trace_id as usize].append_msg(
            gate_id,
            msg,
            MsgRelation::GATE,
        );
    }

    /// Record a message for a given trace ID that relates to the owner of a
    /// [`Gate`].
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

    /// Fetch all messages for a given trace ID.
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
    pub fn new(tracer: Arc<Tracer>, gate_id: Uuid) -> Self {
        Self { tracer, gate_id }
    }

    pub fn note_event(&self, trace_id: u8, msg: String) {
        self.tracer
            .note_component_event(trace_id, self.gate_id, msg)
    }
}

#[cfg(test)]
mod tests {
    use chrono::SubsecRound;

    use super::*;

    #[test]
    fn new_gate_trace_msg_has_expected_properties() {
        let gate_id = Uuid::new_v4();
        let msg = "Some msg".to_string();
        let msg_relation = MsgRelation::GATE;
        let now = Utc::now();

        let trace_msg = TraceMsg::new(gate_id, msg.clone(), msg_relation);

        assert_eq!(trace_msg.gate_id, gate_id);
        assert_eq!(trace_msg.msg, msg);
        assert_eq!(trace_msg.msg_relation, msg_relation);
        assert_eq!(
            trace_msg.timestamp.trunc_subsecs(0),
            now.trunc_subsecs(0)
        );
    }

    #[test]
    fn new_component_trace_msg_has_expected_properties() {
        let gate_id = Uuid::new_v4();
        let msg = "Some msg".to_string();
        let msg_relation = MsgRelation::COMPONENT;
        let now = Utc::now();

        let trace_msg = TraceMsg::new(gate_id, msg.clone(), msg_relation);

        assert_eq!(trace_msg.gate_id, gate_id);
        assert_eq!(trace_msg.msg, msg);
        assert_eq!(trace_msg.msg_relation, msg_relation);
        assert_eq!(
            trace_msg.timestamp.trunc_subsecs(0),
            now.trunc_subsecs(0)
        );
    }

    #[test]
    #[should_panic]
    fn trace_msg_cannot_be_all_relation() {
        TraceMsg::new(Uuid::new_v4(), String::new(), MsgRelation::ALL);
    }

    #[test]
    fn new_trace_is_empty() {
        let trace = Trace::new();
        assert_eq!(trace.msgs(), &[]);
        assert!(trace
            .msg_indices(Uuid::new_v4(), MsgRelation::ALL)
            .is_empty());
    }

    #[test]
    fn clearing_an_empty_trace_has_no_effect() {
        let mut trace = Trace::new();
        trace.clear();
        assert_eq!(trace.msgs(), &[]);
        assert!(trace
            .msg_indices(Uuid::new_v4(), MsgRelation::ALL)
            .is_empty());
    }

    #[test]
    fn trace_with_one_msg_has_expected_properties() {
        let mut trace = Trace::new();
        let gate_id = Uuid::new_v4();
        let msg = "Some msg".to_string();
        let msg_relation = MsgRelation::GATE;

        trace.append_msg(gate_id, msg.clone(), msg_relation);

        assert_eq!(
            trace.msgs(),
            &[TraceMsg::new(gate_id, msg, msg_relation)]
        );

        assert!(trace
            .msg_indices(Uuid::new_v4(), MsgRelation::ALL)
            .is_empty());

        assert_eq!(trace.msg_indices(gate_id, MsgRelation::ALL), vec![0]);
    }

    #[test]
    fn trace_with_multiple_msgs_has_expected_properties() {
        let mut trace = Trace::new();

        let gate_id1 = Uuid::new_v4();
        let gate_id2 = Uuid::new_v4();
        let gate_id3 = Uuid::new_v4();
        let msg1 = "Some msg1".to_string();
        let msg2 = "Some msg2".to_string();
        let msg3 = "Some msg3".to_string();
        let msg4 = "Some msg4".to_string();

        trace.append_msg(gate_id1, msg1.clone(), MsgRelation::GATE);
        trace.append_msg(gate_id2, msg2.clone(), MsgRelation::GATE);
        trace.append_msg(gate_id2, msg3.clone(), MsgRelation::COMPONENT);
        trace.append_msg(gate_id3, msg4.clone(), MsgRelation::GATE);

        assert_eq!(
            trace.msgs(),
            &[
                TraceMsg::new(gate_id1, msg1, MsgRelation::GATE),
                TraceMsg::new(gate_id2, msg2, MsgRelation::GATE),
                TraceMsg::new(gate_id2, msg3, MsgRelation::COMPONENT),
                TraceMsg::new(gate_id3, msg4, MsgRelation::GATE)
            ]
        );

        assert_eq!(trace.msg_indices(gate_id1, MsgRelation::GATE), vec![0]);
        assert_eq!(
            trace.msg_indices(gate_id1, MsgRelation::COMPONENT),
            Vec::<usize>::new(),
        );
        assert_eq!(trace.msg_indices(gate_id1, MsgRelation::ALL), vec![0]);

        assert_eq!(trace.msg_indices(gate_id2, MsgRelation::GATE), vec![1]);
        assert_eq!(
            trace.msg_indices(gate_id2, MsgRelation::COMPONENT),
            vec![2]
        );
        assert_eq!(trace.msg_indices(gate_id2, MsgRelation::ALL), vec![1, 2]);

        assert_eq!(trace.msg_indices(gate_id3, MsgRelation::GATE), vec![3]);
        assert_eq!(
            trace.msg_indices(gate_id3, MsgRelation::COMPONENT),
            Vec::<usize>::new(),
        );
        assert_eq!(trace.msg_indices(gate_id3, MsgRelation::ALL), vec![3]);
    }

    #[test]
    fn new_tracer_is_empty() {
        let tracer = Tracer::new();
        assert_eq!(tracer.next_tracing_id(), 0);
        for trace_id in u8::MIN..=u8::MAX {
            assert!(tracer.get_trace(trace_id).msgs().is_empty());
        }
    }

    #[test]
    fn tracer_next_trace_id_wraps_around() {
        let tracer = Tracer::new();
        for trace_id in u8::MIN..=u8::MAX {
            assert_eq!(tracer.next_tracing_id(), trace_id);
        }
        assert_eq!(tracer.next_tracing_id(), u8::MIN);
    }

    #[test]
    fn tracer_note_gate_event_works() {
        let tracer = Tracer::new();
        let trace_id: u8 = 0;
        let gate_id: Uuid = Uuid::new_v4();
        let msg = "some msg".to_string();
        tracer.note_gate_event(trace_id, gate_id, msg.clone());
        assert!(!tracer.get_trace(trace_id).msgs().is_empty());
        assert_eq!(
            tracer.get_trace(trace_id).msgs(),
            vec![TraceMsg::new(gate_id, msg, MsgRelation::GATE)]
        );
    }

    #[test]
    fn tracer_note_component_event_works() {
        let tracer = Tracer::new();
        let trace_id: u8 = 0;
        let gate_id: Uuid = Uuid::new_v4();
        let msg = "some msg".to_string();
        tracer.note_component_event(trace_id, gate_id, msg.clone());
        assert!(!tracer.get_trace(trace_id).msgs().is_empty());
        assert_eq!(
            tracer.get_trace(trace_id).msgs(),
            vec![TraceMsg::new(gate_id, msg, MsgRelation::COMPONENT)]
        );
    }

    #[test]
    fn clear_tracer_id_works() {
        let tracer = Tracer::new();
        let trace_id: u8 = 0;
        let gate_id: Uuid = Uuid::new_v4();
        let msg = "some msg".to_string();
        tracer.note_gate_event(trace_id, gate_id, msg.clone());
        assert!(!tracer.get_trace(trace_id).msgs().is_empty());
        assert_eq!(
            tracer.get_trace(trace_id).msgs(),
            vec![TraceMsg::new(gate_id, msg, MsgRelation::GATE)]
        );
        tracer.clear_trace_id(trace_id);
        assert!(tracer.get_trace(trace_id).msgs().is_empty());
    }

    #[test]
    fn bound_tracer_works() {
        let tracer = Arc::new(Tracer::new());
        let gate_id = Uuid::new_v4();
        let bound_tracer = tracer.bind(gate_id);

        let trace_id = 0;
        let msg = "some msg".to_string();
        bound_tracer.note_event(trace_id, msg.clone());

        assert!(!tracer.get_trace(trace_id).msgs().is_empty());
        assert_eq!(
            tracer.get_trace(trace_id).msgs(),
            vec![TraceMsg::new(gate_id, msg, MsgRelation::COMPONENT)]
        );
    }
}

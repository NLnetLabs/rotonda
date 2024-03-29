use chrono::{DateTime, Utc};
use roto::{types::typevalue::TypeValue, vm::OutputStreamQueue};
use rotonda_store::QueryResult;

use smallvec::{smallvec, SmallVec};
use std::{
    fmt::Display,
    net::{IpAddr, SocketAddr},
    ops::ControlFlow,
    sync::Arc,
};
use uuid::Uuid;

use crate::{
    common::roto::{FilterOutput, FilterResult, RotoError},
    units::RibValue,
};

// TODO: make this a reference
pub type RouterId = String;

//------------ SourceId ------------------------------------------------------

/// The source of received updates.
///
/// Not all incoming data has to come from a TCP/IP connection. This enum
/// exists to represent both the TCP/IP type of incoming connection that we
/// receive data from today as well as other connection types in future, and
/// can also be used to represent alternate sources of incoming data such as
/// replay from file or test data created on the fly.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum SourceId {
    SocketAddr(SocketAddr),
    Named(Arc<String>),
}

impl Default for SourceId {
    fn default() -> Self {
        "unknown".into()
    }
}

impl SourceId {
    pub fn generated() -> Self {
        Self::from("generated")
    }

    pub fn socket_addr(&self) -> Option<&SocketAddr> {
        match self {
            SourceId::SocketAddr(addr) => Some(addr),
            SourceId::Named(_) => None,
        }
    }

    pub fn ip(&self) -> Option<IpAddr> {
        match self {
            SourceId::SocketAddr(addr) => Some(addr.ip()),
            SourceId::Named(_) => None,
        }
    }

    pub fn name(&self) -> Option<&str> {
        match self {
            SourceId::SocketAddr(_) => None,
            SourceId::Named(name) => Some(name),
        }
    }
}

impl Display for SourceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceId::SocketAddr(addr) => addr.fmt(f),
            SourceId::Named(name) => name.fmt(f),
        }
    }
}

impl From<SocketAddr> for SourceId {
    fn from(addr: SocketAddr) -> Self {
        SourceId::SocketAddr(addr)
    }
}

impl From<String> for SourceId {
    fn from(name: String) -> Self {
        SourceId::Named(name.into())
    }
}

impl From<&str> for SourceId {
    fn from(name: &str) -> Self {
        SourceId::Named(name.to_string().into())
    }
}

//------------ UpstreamStatus ------------------------------------------------

#[derive(Clone, Debug)]
pub enum UpstreamStatus {
    /// No more data will be sent for the specified source.
    ///
    /// This could be because a network connection has been lost, or at the
    /// protocol level a session has been terminated, but need not be network
    /// related. E.g. it could be that the last message in a replay file has
    /// been loaded and replayed, or the last message in a test set has been
    /// pushed into the pipeline, etc.
    EndOfStream { source_id: SourceId },
}

//------------ Payload -------------------------------------------------------

pub trait Filterable {
    fn filter<E, T, U>(
        self,
        filter_fn: T,
        filtered_fn: U,
    ) -> Result<SmallVec<[Payload; 8]>, FilterError>
    where
        T: Fn(TypeValue, DateTime<Utc>, Option<u8>) -> FilterResult<E>
            + Clone,
        U: Fn(SourceId) + Clone,
        FilterError: From<E>;
}

#[derive(Debug)]
pub enum FilterError {
    RotoScriptError(RotoError),
    OtherError(String),
}

impl From<RotoError> for FilterError {
    fn from(err: RotoError) -> Self {
        FilterError::RotoScriptError(err)
    }
}

impl Display for FilterError {
    #[rustfmt::skip]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FilterError::RotoScriptError(err) => {
                f.write_fmt(format_args!("Filtering failed due to Roto script error: {err}"))
            }
            FilterError::OtherError(err) => {
                f.write_fmt(format_args!("Filtering failed: {err}"))
            }
        }
    }
}

#[derive(Clone, Debug, Eq)]
pub struct Payload {
    pub source_id: SourceId,
    pub value: TypeValue,
    pub trace_id: Option<u8>,
    pub received: DateTime<Utc>,
}

impl PartialEq for Payload {
    fn eq(&self, other: &Self) -> bool {
        // Don't compare the received timestamp
        self.source_id == other.source_id
            && self.value == other.value
            && self.trace_id == other.trace_id // && self.received == other.received
    }
}

impl Payload {
    pub fn new<S, T>(source_id: S, value: T, trace_id: Option<u8>) -> Self
    where
        S: Into<SourceId>,
        T: Into<TypeValue>,
    {
        Self {
            source_id: source_id.into(),
            value: value.into(),
            trace_id,
            received: Utc::now(),
        }
    }

    pub fn with_received<S, T>(
        source_id: S,
        value: T,
        trace_id: Option<u8>,
        received: DateTime<Utc>,
    ) -> Self
    where
        S: Into<SourceId>,
        T: Into<TypeValue>,
    {
        Self {
            source_id: source_id.into(),
            value: value.into(),
            trace_id,
            received,
        }
    }

    pub fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    pub fn trace_id(&self) -> Option<u8> {
        self.trace_id
    }
}

impl Filterable for Payload {
    fn filter<E, T, U>(
        self,
        filter_fn: T,
        filtered_fn: U,
    ) -> Result<SmallVec<[Payload; 8]>, FilterError>
    where
        T: Fn(TypeValue, DateTime<Utc>, Option<u8>) -> FilterResult<E>
            + Clone,
        U: Fn(SourceId) + Clone,
        FilterError: From<E>,
    {
        if let ControlFlow::Continue(filter_output) =
            filter_fn(self.value, self.received, self.trace_id)?
        {
            Ok(Payload::from_filter_output(
                self.source_id.clone(),
                filter_output,
                self.trace_id,
            ))
        } else {
            filtered_fn(self.source_id);
            Ok(smallvec![])
        }
    }
}

impl Filterable for SmallVec<[Payload; 8]> {
    fn filter<E, T, U>(
        self,
        filter_fn: T,
        filtered_fn: U,
    ) -> Result<SmallVec<[Payload; 8]>, FilterError>
    where
        T: Fn(TypeValue, DateTime<Utc>, Option<u8>) -> FilterResult<E>
            + Clone,
        U: Fn(SourceId) + Clone,
        FilterError: From<E>,
    {
        let mut out_payloads = smallvec![];

        for payload in self {
            out_payloads.extend(
                payload.filter(filter_fn.clone(), filtered_fn.clone())?,
            );
        }

        Ok(out_payloads)
    }
}

impl Payload {
    pub fn from_filter_output(
        source_id: SourceId,
        filter_output: FilterOutput,
        trace_id: Option<u8>,
    ) -> SmallVec<[Payload; 8]> {
        let mut out_payloads = smallvec![];

        // Make a payload out of it
        let new_payload = Payload::with_received(
            source_id,
            filter_output.east,
            trace_id,
            filter_output.received,
        );

        // Add the payload to the result
        out_payloads.push(new_payload);

        // Add output stream messages to the result
        if !filter_output.south.is_empty() {
            out_payloads.extend(Self::from_output_stream_queue(
                filter_output.south,
                trace_id,
            ));
        }

        out_payloads
    }

    pub fn from_output_stream_queue(
        osq: OutputStreamQueue,
        trace_id: Option<u8>,
    ) -> SmallVec<[Payload; 8]> {
        osq.into_iter()
            .map(|osm| Payload::new(SourceId::generated(), osm, trace_id))
            .collect()
    }
}

impl From<Payload> for SmallVec<[Payload; 8]> {
    fn from(payload: Payload) -> Self {
        smallvec![payload]
    }
}

impl From<Payload> for SmallVec<[Update; 1]> {
    fn from(payload: Payload) -> Self {
        smallvec![payload.into()]
    }
}

impl From<TypeValue> for Payload {
    fn from(tv: TypeValue) -> Self {
        Self::new("generated", tv, None)
    }
}

//------------ Update --------------------------------------------------------

#[derive(Clone, Debug)]
pub enum Update {
    Single(Payload),
    Bulk(SmallVec<[Payload; 8]>),
    QueryResult(Uuid, Result<QueryResult<RibValue>, String>),
    UpstreamStatusChange(UpstreamStatus),
}

impl Update {
    pub fn trace_ids(&self) -> SmallVec<[&Payload; 1]> {
        match self {
            Update::Single(payload) => {
                if payload.trace_id().is_some() {
                    [payload].into()
                } else {
                    smallvec![]
                }
            }
            Update::Bulk(payloads) => {
                payloads.iter().filter(|p| p.trace_id().is_some()).collect()
            }
            Update::QueryResult(_, _) => smallvec![],
            Update::UpstreamStatusChange(_) => smallvec![],
        }
    }
}

impl From<Payload> for Update {
    fn from(payload: Payload) -> Self {
        Update::Single(payload)
    }
}

impl<const N: usize> From<[Payload; N]> for Update {
    fn from(payloads: [Payload; N]) -> Self {
        Update::Bulk(payloads.as_slice().into())
    }
}

impl From<SmallVec<[Payload; 8]>> for Update {
    fn from(payloads: SmallVec<[Payload; 8]>) -> Self {
        Update::Bulk(payloads)
    }
}

use roto::types::typevalue::TypeValue;
use roto::vm::OutputStreamQueue;
use rotonda_store::QueryResult;

use smallvec::{smallvec, SmallVec};
use std::{
    net::{IpAddr, SocketAddr},
    ops::ControlFlow,
    sync::Arc,
};
use uuid::Uuid;

use crate::{
    common::roto::{FilterOutput, FilterResult},
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

impl SourceId {
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
            SourceId::Named(name) => Some(&name),
        }
    }
}

impl std::fmt::Display for SourceId {
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
    fn filter<T>(self, filter_fn: T) -> Result<SmallVec<[Payload; 8]>, String>
    where
        T: Fn(TypeValue) -> FilterResult<String> + Clone;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Payload {
    pub source_id: SourceId,
    pub value: TypeValue,
}

impl Payload {
    pub fn new<S, T>(source_id: S, value: T) -> Self
    where
        S: Into<SourceId>,
        T: Into<TypeValue>,
    {
        Self {
            source_id: source_id.into(),
            value: value.into(),
        }
    }

    pub fn source_id(&self) -> &SourceId {
        &self.source_id
    }
}

impl Filterable for Payload {
    fn filter<T>(self, filter_fn: T) -> Result<SmallVec<[Payload; 8]>, String>
    where
        T: Fn(TypeValue) -> FilterResult<String> + Clone,
    {
        if let ControlFlow::Continue(filter_output) = filter_fn(self.value)? {
            Ok(Payload::from_filter_output(
                self.source_id.clone(),
                filter_output,
            ))
        } else {
            Ok(smallvec![])
        }
    }
}

impl Filterable for SmallVec<[Payload; 8]> {
    fn filter<T>(self, filter_fn: T) -> Result<SmallVec<[Payload; 8]>, String>
    where
        T: Fn(TypeValue) -> FilterResult<String> + Clone,
    {
        let mut out_payloads = smallvec![];

        for payload in self {
            out_payloads.extend(payload.filter(filter_fn.clone())?);
        }

        Ok(out_payloads)
    }
}

impl Payload {
    pub fn from_filter_output(
        source_id: SourceId,
        filter_output: FilterOutput<TypeValue>,
    ) -> SmallVec<[Payload; 8]> {
        let mut out_payloads = smallvec![];

        let out_value = match (filter_output.rx, filter_output.tx) {
            (rx, None) => rx,
            (_, Some(tx)) => tx,
        };

        // Make a payload out of it
        let new_payload = Payload::new(source_id.clone(), out_value);

        // Add the payload to the result
        out_payloads.push(new_payload);

        // Add output stream messages to the result
        if !filter_output.output_stream_queue.is_empty() {
            out_payloads.extend(
                filter_output
                    .output_stream_queue
                    .into_iter()
                    .map(|osm| Payload::new(source_id.clone(), osm)),
            );
        }

        out_payloads
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
        Self::new("generated", tv)
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

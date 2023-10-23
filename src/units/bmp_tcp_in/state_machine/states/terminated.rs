use std::ops::ControlFlow;

use crate::units::bmp_tcp_in::state_machine::{machine::BmpStateDetails, processing::ProcessingResult};

use super::{dumping::Dumping, initiating::Initiating, updating::Updating};

use bytes::Bytes;
use roto::{types::builtin::BgpUpdateMessage, vm::OutputStreamQueue};
use routecore::bmp::message::Message as BmpMsg;

/// BmpState machine state 'Terminated'.
///
/// We don't actually implement a terminated state as RFC 7854 says:
///
/// > **4.5.  Termination Message**
/// >
/// > ... [the monitoring station] MUST close the TCP session after
/// > receiving a termination message
/// >
/// > -- <https://datatracker.ietf.org/doc/html/rfc7854#section-4.5>
#[derive(Default, Debug)]
pub struct Terminated;

impl From<BmpStateDetails<Initiating>> for BmpStateDetails<Terminated> {
    fn from(v: BmpStateDetails<Initiating>) -> Self {
        Self {
            source_id: v.source_id,
            router_id: v.router_id,
            status_reporter: v.status_reporter,
            details: v.details.into(),
        }
    }
}

impl From<BmpStateDetails<Dumping>> for BmpStateDetails<Terminated> {
    fn from(v: BmpStateDetails<Dumping>) -> Self {
        Self {
            source_id: v.source_id,
            router_id: v.router_id,
            status_reporter: v.status_reporter,
            details: v.details.into(),
        }
    }
}

impl From<BmpStateDetails<Updating>> for BmpStateDetails<Terminated> {
    fn from(v: BmpStateDetails<Updating>) -> Self {
        Self {
            source_id: v.source_id,
            router_id: v.router_id,
            status_reporter: v.status_reporter,
            details: v.details.into(),
        }
    }
}

impl From<Initiating> for Terminated {
    fn from(_: Initiating) -> Self {
        Self
    }
}

impl From<Dumping> for Terminated {
    fn from(_: Dumping) -> Self {
        Self
    }
}

impl From<Updating> for Terminated {
    fn from(_: Updating) -> Self {
        Self
    }
}

impl BmpStateDetails<Terminated> {
    #[allow(dead_code)]
    pub fn process_msg(self, bmp_msg: BmpMsg<Bytes>, _trace_id: Option<u8>) -> ProcessingResult {
        self.process_msg_with_filter(bmp_msg, None::<()>, |msg, _| {
            Ok(ControlFlow::Continue((msg, OutputStreamQueue::new())))
        })
    }

    pub fn process_msg_with_filter<F, D>(
        self,
        bmp_msg: BmpMsg<Bytes>,
        _filter_data: Option<D>,
        _filter: F,
    ) -> ProcessingResult
    where
        F: Fn(
            BgpUpdateMessage,
            Option<D>,
        ) -> Result<ControlFlow<(), (BgpUpdateMessage, OutputStreamQueue)>, String>,
    {
        self.mk_invalid_message_result(format!(
            "RFC 7854 4.3 violation: No messages should be received in the terminated state but received: {}",
            bmp_msg
        ), None, Some(Bytes::copy_from_slice(bmp_msg.as_ref())))
    }
}

use std::ops::ControlFlow;

use crate::units::bmp_in::state_machine::{
    machine::BmpStateDetails, processing::ProcessingResult,
};

use super::{dumping::Dumping, updating::Updating, initiating::Initiating};

use bytes::Bytes;
use roto::types::builtin::BgpUpdateMessage;
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
            addr: v.addr,
            router_id: v.router_id,
            status_reporter: v.status_reporter,
            details: v.details.into(),
        }
    }
}

impl From<BmpStateDetails<Dumping>> for BmpStateDetails<Terminated> {
    fn from(v: BmpStateDetails<Dumping>) -> Self {
        Self {
            addr: v.addr,
            router_id: v.router_id,
            status_reporter: v.status_reporter,
            details: v.details.into(),
        }
    }
}

impl From<BmpStateDetails<Updating>> for BmpStateDetails<Terminated> {
    fn from(v: BmpStateDetails<Updating>) -> Self {
        Self {
            addr: v.addr,
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
    pub fn process_msg(self, msg_buf: Bytes) -> ProcessingResult {
        self.process_msg_with_filter(msg_buf, None::<()>, |msg, _| Ok(ControlFlow::Continue(msg)))
    }

    pub fn process_msg_with_filter<F, D>(
        self,
        msg_buf: Bytes,
        _filter_data: Option<D>,
        _filter: F,
    ) -> ProcessingResult
    where
        F: Fn(BgpUpdateMessage, Option<D>) -> Result<ControlFlow<(), BgpUpdateMessage>, String>
    {
        let bmp_msg = BmpMsg::from_octets(&msg_buf).unwrap(); // already verified upstream
        self.mk_invalid_message_result(format!(
            "RFC 7854 4.3 violation: No messages should be received in the terminated state but received: {}",
            bmp_msg
        ), None, Some(msg_buf))
    }
}

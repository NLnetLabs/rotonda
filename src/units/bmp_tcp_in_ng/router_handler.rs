use std::{borrow::Cow, io::Read};

use log::error;
use routecore::{
    bgp::message_ng::common::SessionConfig,
    bmp::{
        message::PeerDownNotification,
        message_ng::{
            common::MessageType,
            io::{BmpHandler, BmpV3Handler, BmpVersion, Parseable},
            peer_down_notification::PeerDownNotificationV3,
            peer_up_notification::PeerUpNotification,
            route_monitoring::RouteMonitoringV3,
            statistics_report::StatisticsReport,
        },
    },
};
use tokio::io::AsyncRead;

use crate::{
    comms::Gate,
    payload::Update,
    units::bmp_tcp_in_ng::{error::BmpNgError, pph_register::PphRegister},
};

pub struct RouterHandler<R> {
    bmp_handler: BmpHandler<R>,
    gate: Gate,
}

pub struct RouterState {
    pph_register: PphRegister,
    gate: Gate,
}

impl<R: AsyncRead + Unpin> RouterHandler<R> {
    pub fn new(stream: R, gate: Gate) -> Self {
        let bmp_handler = BmpHandler::for_stream(stream);
        Self { bmp_handler, gate }
    }

    pub async fn run(mut self) -> Result<(), BmpNgError> {
        let version = self.bmp_handler.process_initiation().await;
        match version {
            BmpVersion(3) => {
                let v3handler: BmpV3Handler<_> = self.bmp_handler.into();
                Self::process(v3handler, self.gate).await;
                Ok(())
            }
            BmpVersion(4) => Err("BMPv4 not yet implemented".into()),
            BmpVersion(v) => Err(format!("invalid BMP version {v}").into()),
        }
    }
    //impl<R: AsyncRead + Unpin> RouterV3Handler<R> {
    async fn process(mut bmp_handler: BmpV3Handler<R>, gate: Gate) {
        let mut router_state = RouterState::new(gate);
        //let mut cnt = 0;
        while let Ok(Some(_)) = bmp_handler.msg_iter.read_into_buf().await {
            while let Ok(msg) = bmp_handler.msg_iter.get_message() {
                //cnt += 1;
                //if cnt % 1000 == 0 {
                //    eprint!("\r{cnt}");
                //}
                match msg.common.msg_type {
                    MessageType::PEER_UP_NOTIFICATION => {
                        let _ = router_state.process_peer_up(
                            PeerUpNotification::try_from_message(msg).unwrap(),
                        );
                    }
                    MessageType::ROUTE_MONITORING => {
                        let _ = router_state.process_route_monitoring(
                            RouteMonitoringV3::try_from_message(msg).unwrap(),
                        );
                    }
                    MessageType::PEER_DOWN_NOTIFICATION => {
                        let _ = router_state.process_peer_down_notification(
                            PeerDownNotificationV3::try_from_message(msg)
                                .unwrap(),
                        );
                    }
                    MessageType::STATISTICS_REPORT => {
                        let _ = router_state.process_statistics_report(
                            StatisticsReport::try_from_message(msg).unwrap(),
                        );
                    }
                    MessageType(_) => {
                        panic!("TODO {}", msg.common.msg_type)
                    }
                }
            }
        }
    }
}

#[allow(clippy::unnecessary_wraps)]
impl RouterState {
    pub fn new(gate: Gate) -> Self {
        Self {
            gate,
            pph_register: PphRegister::default(),
        }
    }

    fn process_peer_up(
        &mut self,
        msg: &PeerUpNotification,
    ) -> Result<(), BmpNgError> {
        let pph = msg.per_peer_header();

        // TODO: make a proper SC from the BGP OPENS
        //let sc = SessionConfig::from(msg.bgp_opens()?);
        let sc = SessionConfig::default();

        self.pph_register.insert(msg.per_peer_header(), sc);
        Ok(())
    }

    fn process_route_monitoring(
        &mut self,
        msg: &RouteMonitoringV3,
    ) -> Result<(), BmpNgError> {
        let (ingress_id, sc) =
            if let Some(t) = self.pph_register.get(msg.per_peer_header()) {
                t
            } else {
                // XXX pph_register should actually use find_other_ribviews
                self.pph_register
                    .insert(msg.per_peer_header(), SessionConfig::default());
                self.pph_register.get(msg.per_peer_header()).unwrap()
            };

        let update = msg.bgp_update()?;
        let mut update = update.into_checked_parts(sc)?;
        if let Some(attr) = update.take_conv_attributes() {
            let conv_iter = update.conv_reach_iter_raw();
            let _ = conv_iter.count();
            //eprintln!(
            //    "[{ingress_id:x}] {} bytes of attributes for {} conv NLRI",
            //    attr.len(),
            //    conv_iter.count()
            //);
            //self.gate.update_data(Update::NgReach(_attr,conv_iter);
        }
        if let Some(attr) = update.take_mp_attributes() {
            let mp_iter = update.mp_reach_iter_raw();
            let _ = mp_iter.count();
            //eprintln!(
            //    "[{ingress_id:x}] {} bytes of attributes for {} mp NLRI",
            //    attr.len(),
            //    mp_iter.count()
            //);
            //self.gate.update_data(Update::NgReach(_attr,conv_iter);
        }

        //self.gate.update_data(Update::TODO).await;
        Ok(())
    }

    fn process_peer_down_notification(
        &mut self,
        msg: &PeerDownNotificationV3,
    ) -> Result<(), BmpNgError> {
        Ok(())
    }

    fn process_statistics_report(
        &mut self,
        msg: &StatisticsReport,
    ) -> Result<(), BmpNgError> {
        Ok(())
    }
}

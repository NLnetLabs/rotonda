use std::{borrow::Cow, io::Read, sync::Arc};

use inetnum::asn::Asn;
use log::{debug, error, warn};
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
    ingress::{self, IngressId, IngressInfo, IngressType},
    payload::Update,
    units::bmp_tcp_in_ng::{error::BmpNgError, pph_register::PphRegister},
};

pub struct RouterHandler<R> {
    bmp_handler: BmpHandler<R>,
    ingress_register: Arc<ingress::Register>,
    unit_ingress_id: IngressId,
    gate: Gate,
}

impl<R: AsyncRead + Unpin> RouterHandler<R> {
    pub fn new(
        stream: R,
        gate: Gate,
        ingress_register: Arc<ingress::Register>,
        unit_ingress_id: IngressId,
    ) -> Self {
        let bmp_handler = BmpHandler::for_stream(stream);
        Self {
            bmp_handler,
            ingress_register,
            unit_ingress_id,
            gate,
        }
    }

    pub async fn run(
        mut self,
        mut partial_ingress_info: IngressInfo,
    ) -> Result<(), BmpNgError> {
        let (version, msg) = self.bmp_handler.process_initiation().await;

        match msg {
            Ok(init_msg) => {
                partial_ingress_info = partial_ingress_info.with_name(
                    init_msg
                        .get_sys_name()
                        .unwrap_or_else(|| "__no_sys_name".into()),
                );
                partial_ingress_info = partial_ingress_info.with_desc(
                    init_msg
                        .get_sys_desc()
                        .unwrap_or_else(|| "__no_sys_desc".into()),
                );
            }
            Err(_other_msg) => {
                warn!("unexpected first message of BMP stream");
                partial_ingress_info = partial_ingress_info
                    .with_desc("__invalid_stream_missing_initation_msg");
            }
        }

        match version {
            BmpVersion(3) => {
                let v3handler: BmpV3Handler<_> = self.bmp_handler.into();
                Self::process(
                    v3handler,
                    self.gate,
                    self.ingress_register,
                    self.unit_ingress_id,
                    partial_ingress_info,
                )
                .await;
                Ok(())
            }
            BmpVersion(4) => Err("BMPv4 not yet implemented".into()),
            BmpVersion(v) => Err(format!("invalid BMP version {v}").into()),
        }
    }
    //impl<R: AsyncRead + Unpin> RouterV3Handler<R> {
    async fn process(
        mut bmp_handler: BmpV3Handler<R>,
        gate: Gate,
        ingress_register: Arc<ingress::Register>,
        unit_ingress_id: IngressId,
        partial_ingress_info: IngressInfo,
    ) {
        let mut router_state = RouterState::new(
            gate,
            ingress_register,
            unit_ingress_id,
            partial_ingress_info,
        );
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
                        let _ = router_state
                            .process_route_monitoring(
                                RouteMonitoringV3::try_from_message(msg)
                                    .unwrap(),
                            )
                            .await;
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
                    _ => {
                        panic!("TODO {}", msg.common.msg_type)
                    }
                }
            }
        }
    }
}

/// State for a BMP stream multiplexing N BGP sessions/views
///
/// In the `ingress::Register`, the BMP unit that accepted the TCP stream is the
/// parent of this `RouterState`. For every monitored BGP session in this
/// stream, this `RouterState` will be the parent of such sessions.
///
/// BMP ingress Unit - (`unit_ingress_id`)
///                   |
///                   -> `RouterState` - (`bmp_stream_ingress_id`)
///                                   |
///                                   |-> BGP session 1
///                                   |-> BGP session ..
///                                   |-> BGP session N
///                   
pub struct RouterState {
    pph_register: PphRegister,
    ingress_register: Arc<ingress::Register>,
    bmp_stream_ingress_id: IngressId,
    gate: Gate,
}

#[allow(clippy::unnecessary_wraps)]
impl RouterState {
    pub fn new(
        gate: Gate,
        ingress_register: Arc<ingress::Register>,
        unit_ingress_id: IngressId,
        partial_ingress_info: IngressInfo,
    ) -> Self {
        let bmp_stream_ingress_id = ingress_register.register();
        debug!("bmp_stream registered {bmp_stream_ingress_id}");
        //let bmp_stream_info = IngressInfo::new()
        let bmp_stream_info = partial_ingress_info
            .with_ingress_type(IngressType::Bmp)
            .with_parent_ingress(unit_ingress_id)
            .with_state(ingress::register::IngressState::Connected);

        ingress_register.update_info(bmp_stream_ingress_id, bmp_stream_info);

        Self {
            pph_register: PphRegister::new(ingress_register.clone()),
            ingress_register,
            bmp_stream_ingress_id,
            gate,
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

        let ingress_id = self.pph_register.insert(msg.per_peer_header(), sc);
        debug!("ingress_id registered {ingress_id}");
        let ingress_info = IngressInfo::new()
            .with_parent_ingress(self.bmp_stream_ingress_id)
            .with_ingress_type(IngressType::BgpViaBmp)
            .with_remote_addr(pph.address())
            // convert ng Asn into old (inetnum) Asn, TODO remove
            .with_remote_asn(Asn::from_u32(pph.asn().to_u32()))
            .with_peer_type(u8::from(pph.peer_type()))
            .with_rib_type(pph.rib_type())
            .with_peer_rib_type((pph.is_post_policy(), pph.rib_type()));

        self.ingress_register.update_info(ingress_id, ingress_info);
        Ok(())
    }

    async fn process_route_monitoring(
        &mut self,
        msg: &RouteMonitoringV3,
    ) -> Result<(), BmpNgError> {
        let (ingress_id, sc) = if let Some(t) =
            self.pph_register.get(msg.per_peer_header())
        {
            t
        } else {
            // XXX pph_register should actually use find_other_ribviews

            let pph = msg.per_peer_header();
            let maybe_existing = self
                .pph_register
                .find_other_ribviews(msg.per_peer_header())
                .cloned();

            if let Some((existing_ingress_id, sc)) = maybe_existing {
                let mut existing_info =
                    self.ingress_register.get(existing_ingress_id).unwrap();
                existing_info = existing_info
                    .with_peer_type(u8::from(pph.peer_type()))
                    .with_rib_type(pph.rib_type())
                    .with_peer_rib_type((
                        pph.is_post_policy(),
                        pph.rib_type(),
                    ));
                let new_ingress_id = self
                    .pph_register
                    .insert(msg.per_peer_header(), sc.clone());
                self.ingress_register
                    .update_info(new_ingress_id, existing_info);

                &(new_ingress_id, sc)
            } else {
                warn!("RouteMonitoring message for which no PeerUp was found");
                &(
                    self.pph_register.insert(
                        msg.per_peer_header(),
                        SessionConfig::default(),
                    ),
                    SessionConfig::default(),
                )
            }
        };

        let update = msg.bgp_update()?;
        //let mut update = update.into_checked_parts(sc)?;
        //if let Some(attr) = update.take_conv_attributes() {
        //    let conv_iter = update.conv_reach_iter_raw();
        //    let _ = conv_iter.count();
        //    //eprintln!(
        //    //    "[{ingress_id:x}] {} bytes of attributes for {} conv NLRI",
        //    //    attr.len(),
        //    //    conv_iter.count()
        //    //);
        //    //self.gate.update_data(Update::NgReach(_attr,conv_iter);

        //    //TODO variant aan Update toevoegen om attr+conv_iter te sturen
        //}
        //if let Some(attr) = update.take_mp_attributes() {
        //    let mp_iter = update.mp_reach_iter_raw();
        //    let _ = mp_iter.count();
        //    //eprintln!(
        //    //    "[{ingress_id:x}] {} bytes of attributes for {} mp NLRI",
        //    //    attr.len(),
        //    //    mp_iter.count()
        //    //);
        //    //self.gate.update_data(Update::NgReach(_attr,conv_iter);
        //}

        self.gate
            .update_data(Update::NgBulk(
                update.to_vec(),
                *ingress_id,
                sc.clone(),
            ))
            .await;
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

use std::{borrow::Cow, collections::HashMap, io::{BufWriter, Read, Write}, net::IpAddr, sync::Arc};

use inetnum::asn::Asn;
use log::{debug, error, warn};
use routecore::{
    bgp::message_ng::common::SessionConfig,
    bmp::message_ng::{
            common::{MessageType, PerPeerHeader}, io::{BmpHandler, BmpV3Handler, BmpVersion, Parseable}, peer_down_notification::{PeerDownNotification as PeerDownNotification, PeerDownNotificationV3, PeerDownNotificationV4}, peer_up_notification::{PeerUpNotification, PeerUpNotificationV3, PeerUpNotificationV4}, route_monitoring::{RouteMonitoring, RouteMonitoringV3, RouteMonitoringV4}, snapshot::SnapshotMessage, statistics_report::{StatisticsReport, StatisticsReportV3, StatisticsReportV4}, tlvs::Index
        }, util::pcapng::{PcapNgWriter, PcapNgWriterExt},
};
use tokio::io::AsyncRead;

use crate::{
    comms::Gate,
    ingress::{self, IngressId, IngressInfo, IngressType},
    payload::Update,
    units::bmp_tcp_in_ng::{BmpTcpIn, error::BmpNgError, pph_register::PphRegister, unit::{StatsKey, StatsValue}},
};

pub struct RouterHandler<R> {
    bmp_handler: BmpHandler<R>,
    ingress_register: Arc<ingress::Register>,
    unit_ingress_id: IngressId,
    gate: Gate,
    config: BmpTcpIn,
    stats_db: super::unit::StatsStore,
}

impl<R: AsyncRead + Unpin> RouterHandler<R> {
    pub fn new(
        stream: R,
        gate: Gate,
        ingress_register: Arc<ingress::Register>,
        unit_ingress_id: IngressId,
        config: BmpTcpIn,
        stats_db: super::unit::StatsStore,
    ) -> Self {
        let bmp_handler = BmpHandler::for_stream(stream);
        Self {
            bmp_handler,
            ingress_register,
            unit_ingress_id,
            gate,
            config,
            stats_db,
        }
    }

    pub async fn run(
        mut self,
        mut partial_ingress_info: IngressInfo,
    ) -> Result<(), BmpNgError> {
        let (version, msg) = self.bmp_handler.process_initiation().await;

        debug!("version: {version:?}");

        let mut output_pcapng = None;

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

                if let Some(output) = self.config.write_v4_to_file_bin.as_ref() {
                    match std::fs::OpenOptions::new()
                        .create(true)
                        .truncate(true)
                        .append(false)
                        .write(true)
                        .open(output) {
                            Ok(mut fh) => {
                                if self.config.write_snapshot.is_some_and(|v| v) {
                                    // FIXME properly generate and store this
                                    // somewhere
                                    let uuid: [u8; 16] = std::array::from_fn(|i| u8::try_from(i).unwrap());
                                    let snapshot_written_bytes = SnapshotMessage::write_new(&mut fh, uuid).unwrap();
                                    //dbg!(snapshot_written_bytes);
                                }
                                let _ = init_msg.write_as_v4(&mut fh, None);
                            }
                            Err(e) => {
                                warn!(
                                    "Failed to open {} for writing: {e}",
                                    output.to_string_lossy()
                                );
                            }

                        }
                }

                if let Some(output_mrt) = self.config.write_v4_to_file_mrt.as_ref() {
                    match std::fs::OpenOptions::new()
                        .create(true)
                        .truncate(true)
                        .append(false)
                        .write(true)
                        .open(output_mrt) {
                            Ok(mut fh) => {
                                let mut outbuf = Vec::with_capacity(1<<16);
                                if self.config.write_snapshot.is_some_and(|v| v) {
                                    // FIXME properly generate and store this
                                    // somewhere
                                    let uuid: [u8; 16] = std::array::from_fn(|i| u8::try_from(i).unwrap());
                                    let len = SnapshotMessage::write_new(&mut outbuf, uuid).unwrap();
                                    let _ = routecore::mrt_ng::common::CommonHeader::write_bmp_et_message(&mut fh, None, &outbuf[..len]);
                                    outbuf.clear();
                                }

                                let len = init_msg.write_as_v4(&mut outbuf, None).unwrap();
                                let _ = routecore::mrt_ng::common::CommonHeader::write_bmp_et_message(&mut fh, None, &outbuf[..len]);
                                outbuf.clear();
                            }
                            Err(e) => {
                                warn!(
                                    "Failed to open {} for writing: {e}",
                                    output_mrt.to_string_lossy()
                                );
                            }

                        }
                }

                if let Some(pcapng_filename) = self.config.write_v4_to_file_pcapng.as_ref() {
                    match std::fs::OpenOptions::new()
                        .create(true)
                        .truncate(true)
                        .append(false)
                        .write(true)
                        .open(pcapng_filename) {
                            Ok(mut fh) => {
                                let mut res = PcapNgWriter::new_custom(fh).unwrap();
                                res.start_snapshot();
                                output_pcapng = Some(res);

                            }
                            Err(e) => {
                                warn!(
                                    "Failed to open {} for writing: {e}",
                                    pcapng_filename.to_string_lossy()
                                );
                            }

                        }
                }

            }
            Err(other_msg) => {
                // NB: this message is not consumed, so it will be picked up
                // in process() below.
                warn!("unexpected first message of BMP stream");

                if let Ok(snapshot_msg) = SnapshotMessage::try_from_raw(other_msg) {
                    debug!("raw msg (len {}): {:?}", &other_msg.len(), &other_msg);
                    let id = snapshot_msg.snapshot_id();
                    warn!("but it turns out to be a SnapshotMessage, id {id:?}");
                }

                partial_ingress_info = partial_ingress_info
                    .with_desc("__invalid_stream_missing_initation_msg");
            }
        }

        // XXX while figuring out whether or not we need a different code path
        // here for v3 vs v4, silence clippy
        #[allow(clippy::match_same_arms)]
        match version {
            BmpVersion(3) => {
                let v3handler: BmpV3Handler<_> = self.bmp_handler.into();
                Self::process(
                    v3handler,
                    self.gate,
                    self.ingress_register,
                    self.unit_ingress_id,
                    partial_ingress_info,
                    self.config,
                    output_pcapng,
                    self.stats_db,
                )
                .await;
                Ok(())
            }
            BmpVersion(4) => {
                let v4handler: BmpV3Handler<_> = self.bmp_handler.into();
                Self::process_v4(
                    v4handler,
                    self.gate,
                    self.ingress_register,
                    self.unit_ingress_id,
                    partial_ingress_info,
                    self.config,
                    self.stats_db,
                )
                .await;
                Ok(())
            }
            BmpVersion(v) => Err(format!("invalid BMP version {v}").into()),
        }
    }

    async fn process_v4(
        mut bmp_handler: BmpV3Handler<R>,
        gate: Gate,
        ingress_register: Arc<ingress::Register>,
        unit_ingress_id: IngressId,
        partial_ingress_info: IngressInfo,
        config: BmpTcpIn,
        stats_db: super::unit::StatsStore,
        // XXX maybe instead pass in the BufWriters directly?
    ) {
        let mut router_state = RouterState::new(
            gate,
            ingress_register,
            unit_ingress_id,
            partial_ingress_info,
            stats_db,
        );
        //let mut cnt = 0;
        
        let mut output = config.write_v4_to_file_bin.and_then(|output|
            match std::fs::OpenOptions::new()
                .create(false)
                .append(true)
                .open(&output) {
                    Ok(fh) => Some(BufWriter::new(fh)),
                    Err(e) => {
                        warn!(
                            "Failed to open {} for writing: {e}",
                            output.to_string_lossy()
                        );
                        None
                    }
                }
        );

        let mut output_mrt = config.write_v4_to_file_mrt.and_then(|output|
            match std::fs::OpenOptions::new()
                .create(false)
                .append(true)
                .open(&output) {
                    Ok(fh) => Some(BufWriter::new(fh)),
                    Err(e) => {
                        warn!(
                            "Failed to open {} for writing: {e}",
                            output.to_string_lossy()
                        );
                        None
                    }
                }
        );

        // Helper to write PDUs verbatim to a binary file.
        // The resulting file can be injected using e.g. socat.
        macro_rules! write_bin(
            ($msg:ident) => {
                if let Some(output) = output.as_mut() {
                    let _ = output.write($msg.as_ref());
                }
            }
        );

        // Helper to write .mrt files.
        macro_rules! write_mrt(
            // With timestamp
            ($msg:ident, $ts:expr) => {
                if let Some(output_mrt) = output_mrt.as_mut() {
                    let _ = routecore::mrt_ng::common::CommonHeader::write_bmp_et_message(output_mrt, Some($ts), $msg.as_ref());
                }
            };
            // Without timestamp
            ($msg:ident) => {
                if let Some(output_mrt) = output_mrt.as_mut() {
                    let _ = routecore::mrt_ng::common::CommonHeader::write_bmp_et_message(output_mrt, None, $msg.as_ref());
                }
            };
        );

        while let Ok(Some(_)) = bmp_handler.msg_iter.read_into_buf().await {
            while let Ok(msg) = bmp_handler.msg_iter.get_message() {
                //cnt += 1;
                //if cnt % 1000 == 0 {
                //    eprint!("\r{cnt}");
                //}

                // This is a V3 handler, so if we are configured to write to a
                // (bin|pcap|mrt) file, it needs to be converted.



                match msg.common.msg_type {
                    MessageType::PEER_UP_NOTIFICATION => {
                        let peer_up = PeerUpNotificationV4::try_from_message(msg)
                            .unwrap();

                        write_bin!(peer_up);
                        write_mrt!(peer_up);

                        let _ = router_state.process_peer_up(
                            peer_up
                        );
                    }
                    MessageType::ROUTE_MONITORING => {
                        //debug!("route_mon msg len: {}", msg.as_ref().len());
                        //debug!("route_mon first bytes: {:?}", &msg.as_ref()[..std::cmp::min(msg.as_ref().len(), 64)]);

                        let route_mon = RouteMonitoringV4::try_from_message(msg)
                            .unwrap();

                        write_bin!(route_mon);
                        write_mrt!(route_mon);

                        let _ = router_state
                            .process_route_monitoring(
                                route_mon
                            )
                            .await;
                    }
                    MessageType::PEER_DOWN_NOTIFICATION => {
                        //debug!("peer_down msg len: {}", msg.as_ref().len());
                        //debug!("peer_down first bytes: {:?}", &msg.as_ref()[..std::cmp::min(msg.as_ref().len(), 64)]);
                        let peer_down = PeerDownNotificationV4::try_from_message(msg)
                                .unwrap();

                        write_bin!(peer_down);
                        write_mrt!(peer_down);

                        let _ = router_state.process_peer_down_notification(
                            peer_down
                        );
                    }
                    MessageType::STATISTICS_REPORT => {
                        //debug!("stats report msg len: {}", msg.as_ref().len());
                        //debug!("stats report first bytes: {:?}", &msg.as_ref()[..std::cmp::min(msg.as_ref().len(), 64)]);
                        //debug!("stats report bytes: {:?}", &msg.as_ref());
                        let stats_report = StatisticsReportV4::try_from_message(msg).unwrap();

                        write_bin!(stats_report);
                        write_mrt!(stats_report);

                        let _ = router_state.process_statistics_report(
                            stats_report
                        );
                    }
                    MessageType::INITIATION => {
                        // We end up here if the first message was not an
                        // Initiation (e.g. in case of a BMP Snapshot) or when
                        // the exporter sends an Initiation mid-stream to
                        // provide new information (in the TLVs).
                        debug!("Got an Initiation message after the first message in this stream");
                        debug!("TODO handle Initiation TLVs mid stream");
                    }
                    _ => {
                        panic!("TODO {}", msg.common.msg_type)
                    }
                }
            }
        }
    }
    
    async fn process<W: Write>(
        mut bmp_handler: BmpV3Handler<R>,
        gate: Gate,
        ingress_register: Arc<ingress::Register>,
        unit_ingress_id: IngressId,
        partial_ingress_info: IngressInfo,
        config: BmpTcpIn,
        mut output_pcapng: Option<PcapNgWriter<W>>,
        stats_db: super::unit::StatsStore,
    ) {
        let mut router_state = RouterState::new(
            gate,
            ingress_register.clone(),
            unit_ingress_id,
            partial_ingress_info,
            stats_db,

        );
        //let mut cnt = 0;
        
        let mut output = config.write_v4_to_file_bin.and_then(|output|
            match std::fs::OpenOptions::new()
                .create(false)
                .append(true)
                .open(&output) {
                    Ok(fh) => Some(BufWriter::new(fh)),
                    Err(e) => {
                        warn!(
                            "Failed to open {} for writing: {e}",
                            output.to_string_lossy()
                        );
                        None
                    }
                }
        );
        let mut output_mrt = config.write_v4_to_file_mrt.and_then(|output_mrt|
            match std::fs::OpenOptions::new()
                .create(false)
                .append(true)
                .open(&output_mrt) {
                    Ok(fh) => Some(BufWriter::new(fh)),
                    Err(e) => {
                        warn!(
                            "Failed to open {} for writing: {e}",
                            output_mrt.to_string_lossy()
                        );
                        None
                    }
                }
        );
        let mut output_bgp_mrt_pcapng = config.write_bgp_as_mrt_in_pcapng.and_then(|output|
            match std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&output) {
                    Ok(fh) => {
                        let mut res = PcapNgWriter::new_custom(fh).unwrap();
                        res.start_mrt();
                        Some(res)
                    }
                    Err(e) => {
                        warn!(
                            "Failed to open {} for writing: {e}",
                            output.to_string_lossy()
                        );
                        None
                    }
                }
        );

        let mut tmp_mrt_only_output = match std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open("/tmp/mrtonly.mrt") {
                    Ok(fh) => Some(BufWriter::new(fh)),
                    Err(e) => None,
                };


        let mut outbuf = Vec::with_capacity(1<<20);



        let mut additional_tlvs_buf = Vec::new();
        let additional_tlvs = if let Some(true) = config.write_snapshot {
            // FIXME this should be stored in the runner somewhere
            let uuid: [u8; 16] = std::array::from_fn(|i| u8::try_from(i).unwrap());
            let _ = routecore::bmp::message_ng::tlvs::Tlv::write(&mut additional_tlvs_buf, routecore::bmp::message_ng::tlvs::GenericCodepoint::SNAPSHOT_ID, uuid);
            Some(routecore::bmp::message_ng::tlvs::Tlvs::from_slice(additional_tlvs_buf.as_ref()))
        } else {
            None
        };

        let mut additional_indexed_tlvs_buf = Vec::new();
        let additional_indexed_tlvs = if let Some(true) = config.write_snapshot {
            // FIXME this should be stored in the runner somewhere
            let uuid: [u8; 16] = std::array::from_fn(|i| u8::try_from(i).unwrap());
            let _ = routecore::bmp::message_ng::tlvs::IndexedTlv::write(&mut additional_indexed_tlvs_buf, routecore::bmp::message_ng::tlvs::GenericCodepoint::SNAPSHOT_ID, Index::ALL, uuid);
            // FIXME: we create a Tlvs here instead of an IndexedTlvs, because
            // the fn write_as_v4 from the trait takes a Tlvs... make that
            // nicer.
            Some(routecore::bmp::message_ng::tlvs::Tlvs::from_slice(additional_indexed_tlvs_buf.as_ref()))
        } else {
            None
        };

        // Helper to create binary files.
        macro_rules! write_bin(
            ($msg:ident) => {
                if let Some(output) = output.as_mut() {
                    //let _len = $msg.write_as_v4(output, None).unwrap();
                    let _len = $msg.write_as_v4(output, additional_tlvs).unwrap();
                }
            }
        );

        // Helper to create binary files.
        macro_rules! write_pcapng(
            ($msg:ident) => {
                if let Some(pcapng_writer) = output_pcapng.as_mut() {
                    let len = $msg.write_as_v4(&mut outbuf, additional_tlvs).unwrap();
                    pcapng_writer.write_msg(&outbuf[..len]);
                    outbuf.clear();
                }
            }
        );

        macro_rules! write_indexed_pcapng(
            ($msg:ident) => {
                if let Some(pcapng_writer) = output_pcapng.as_mut() {
                    let len = $msg.write_as_v4(&mut outbuf, additional_indexed_tlvs).unwrap();
                    pcapng_writer.write_msg(&outbuf[..len]);
                    outbuf.clear();
                }
            }
        );


        macro_rules! write_bgp_as_mrt_in_pcapng(
            ($msg:ident, $ingress_info:ident) => {
                if let Some(output) = output_bgp_mrt_pcapng.as_mut() {

                    // FIXME:
                    // double check what the remote/local addr in the
                    // ingressinfo actually are: BMP or BGP level? we need the
                    // latter.
                    // NOT the BGP level session. Those are in the PPH itself.
                    //

                    let len = routecore::mrt_ng::common::CommonHeader::write_bgp4mp_et_msg(
                        &mut outbuf,
                        // converting old inetnum Asn into Asn ng
                        routecore::bmp::message_ng::common::Asn::from_u32($ingress_info.remote_asn.unwrap_or(Asn::from_u32(0)).into_u32()),
                        routecore::bmp::message_ng::common::Asn::from_u32($ingress_info.local_asn.unwrap_or(Asn::from_u32(0)).into_u32()),
                        0_u16, // interface_idx,
                        $ingress_info.remote_addr.unwrap(),
                        // FIXME we don't keep a local addr in the ingress reg
                        //$ingress_info.local_addr.unwrap(),
                        $ingress_info.remote_addr.unwrap(),

                        $msg.bgp_update().unwrap()
                    ).unwrap();
                    output.write_msg(&outbuf[..len]);
                    let _ = tmp_mrt_only_output.as_mut().unwrap().write(&outbuf[..len]);
                    outbuf.clear();
                }
            };
        );



        // Helper to create .mrt files.
        macro_rules! write_mrt(
            // With timestamp
            ($msg:ident, $ts:expr) => {
                if let Some(output_mrt) = output_mrt.as_mut() {
                    // convert to v4 first
                    assert!(outbuf.is_empty());
                    let len = $msg.write_as_v4(&mut outbuf, additional_tlvs).unwrap();
                    // then write to file
                    let _ = routecore::mrt_ng::common::CommonHeader::write_bmp_et_message(output_mrt, Some($ts), &outbuf[..len]);
                    outbuf.clear();
                }
            };
            // Without timestamp
            ($msg:ident) => {
                if let Some(output_mrt) = output_mrt.as_mut() {
                    // convert to v4 first
                    assert!(outbuf.is_empty());
                    let len = $msg.write_as_v4(&mut outbuf, additional_tlvs).unwrap();
                    // then write to file
                    let _ = routecore::mrt_ng::common::CommonHeader::write_bmp_et_message(output_mrt, None, &outbuf[..len]);
                    outbuf.clear();
                }
            };
        );


        while let Ok(Some(_)) = bmp_handler.msg_iter.read_into_buf().await {
            while let Ok(msg) = bmp_handler.msg_iter.get_message() {
                //cnt += 1;
                //if cnt % 1000 == 0 {
                //    eprint!("\r{cnt}");
                //}

                match msg.common.msg_type {
                    MessageType::PEER_UP_NOTIFICATION => {

                        let peer_up = PeerUpNotificationV3::try_from_message(msg)
                            .unwrap();

                        write_bin!(peer_up);
                        write_mrt!(peer_up);
                        write_pcapng!(peer_up);

                        let _ = router_state.process_peer_up(
                            peer_up
                        );
                    }
                    MessageType::ROUTE_MONITORING => {
                        let route_mon = RouteMonitoringV3::try_from_message(msg)
                            .unwrap();

                        write_bin!(route_mon);
                        write_mrt!(route_mon);
                        write_indexed_pcapng!(route_mon);


                        if let Some(ingress_info) = router_state.pph_register.get(
                            route_mon.per_peer_header()).and_then(|(id,_session_config)| ingress_register.get(*id)) {
                            write_bgp_as_mrt_in_pcapng!(route_mon, ingress_info);
                        } else {
                            warn!("not writing mrt to pcapng: missing PPH/ingressinfo");
                        }
                        

                        let _ = router_state
                            .process_route_monitoring(
                                route_mon
                            )
                            .await;
                    }
                    MessageType::PEER_DOWN_NOTIFICATION => {
                        let peer_down = PeerDownNotificationV3::try_from_message(msg)
                                .unwrap();

                        write_bin!(peer_down);
                        write_mrt!(peer_down);

                        let _ = router_state.process_peer_down_notification(
                            peer_down
                        );
                    }
                    MessageType::STATISTICS_REPORT => {
                        let stats_report = StatisticsReportV3::try_from_message(msg).unwrap();

                        write_bin!(stats_report);
                        write_mrt!(stats_report);

                        let _ = router_state.process_statistics_report(
                            stats_report
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
    stats_db: super::unit::StatsStore,
    bmp_router_name: String,
    bmp_router_addr: IpAddr,

}

#[allow(clippy::unnecessary_wraps)]
impl RouterState {
    pub fn new(
        gate: Gate,
        ingress_register: Arc<ingress::Register>,
        unit_ingress_id: IngressId,
        partial_ingress_info: IngressInfo,
        stats_db: super::unit::StatsStore,
    ) -> Self {
        let bmp_stream_ingress_id = ingress_register.register();
        debug!("bmp_stream registered {bmp_stream_ingress_id}");

        let bmp_router_name = partial_ingress_info.name.clone().unwrap_or("__no_router_name".into());
        let bmp_router_addr = partial_ingress_info.remote_addr.unwrap_or("::".parse().unwrap());

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
            stats_db,
            bmp_router_name,
            bmp_router_addr,
        }
    }

    fn process_peer_up<M: PeerUpNotification + ?Sized>(
        &mut self,
        msg: &M
    ) -> Result<(), BmpNgError> {
        let pph = msg.per_peer_header();

        let sc = msg.session_config()?;

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

        //for tlv in msg.tlvs().unwrap() {
        //    dbg!(&tlv);
        //}

        self.ingress_register.update_info(ingress_id, ingress_info);
        Ok(())
    }

    async fn process_route_monitoring<M: RouteMonitoring + ?Sized>(
        &mut self,
        msg: &M
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
                // TODO insert ingress_info and mark as 'missing_peer_up' or
                // something
            }
        };

        //let update = msg.bgp_update()?; // this triggers a lifetime error

        let update = match msg.bgp_update() {
            Ok(update) => update,
            Err(e) =>  {
                let msg = e.to_string();
                return Err(BmpNgError::new(msg.into()));
            }
    };

        // dbg snippet:
        //match update.into_checked_parts(sc) {
        //    Ok(parts) => {
        //        if let Some(afisafi) = parts.mp_reach_afisafi() {
        //            if afisafi
        //            == routecore::bgp::message_ng::common::AfiSafiType::IPV4UNICAST
        //        {
        //            eprintln!(
        //                "{:?}",
        //                routecore::bgp::message_ng::common::PcapHex(msg.as_ref())
        //            );
        //        }
        //        }
        //    }
        //    Err(e) => {
        //        eprintln!("tmp error: {e}");
        //        return Ok(());
        //    }
        //}

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

    fn process_peer_down_notification<M: PeerDownNotification + ?Sized>(
        &mut self,
        msg: &M,
    ) -> Result<(), BmpNgError> {
        Ok(())
    }

    fn process_statistics_report<M: StatisticsReport + ?Sized>(
        &mut self,
        msg: &M,
    ) -> Result<(), BmpNgError> {

        // import enum
        use routecore::bmp::message_ng::statistics_report::Stat::{CounterStat, GaugeStat, AfiSafiGaugeStat};

        let pph = msg.per_peer_header();

        let key = StatsKey {
            bmp_router_name: self.bmp_router_name.clone(),
            bmp_router_addr: self.bmp_router_addr,
            // not sure whether rib_view is useful or necessary:
            // some stat type are rib_view specific anyway (e.g. 
            // 'routes_per_afi_safi_pre_policy_adj_rib_out' )
            // We need to find out whether exporter implementations really
            // send out StatsReport for different rib views, or whether it is
            // always AdjRibInPre (i.e. all related types/flags are 0 in the
            // PPH)
            rib_view: pph.rib_view(),
            distinguisher: pph.distinguisher(),
            address: pph.address(),
            asn: pph.asn(),
            // do we need to include this in the metrics output?
            bgp_id: pph.bgp_id(),
        };


        let mut db = self.stats_db.lock().unwrap();
        db.insert(key, StatsValue {
            timestamp: std::time::SystemTime::now(),
            raw_stats: msg.stats().as_ref().to_vec()
        });

        Ok(())
    }
}

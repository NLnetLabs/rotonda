use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    net::IpAddr,
    time::Instant,
};

use axum::{
    extract::{Path, State},
    response::Html,
};
use inetnum::{addr::Prefix, asn::Asn};
use log::debug;
use routecore::bgp::{
    aspath::HopPath,
    message::update_builder::StandardCommunitiesList,
    types::{AfiSafiType, Otc},
};
use rshtml::{traits::RsHtml, RsHtml};

use rayon::prelude::*;

use crate::{
    http_ng::{Api, ApiState},
    ingress::{IngressId, IngressInfo, IngressType},
    representation::GenOutput,
    roto_runtime::types::PeerRibType,
    units::rib_unit::{rpki::RovStatus, Include, QueryFilter},
};

pub struct WebUI {}

#[derive(serde::Deserialize)]
struct ParamsPeerAsn {
    #[serde(deserialize_with = "Asn::deserialize_from_str")]
    peer_asn: Asn,
}

impl WebUI {
    pub fn register_routes(router: &mut Api) {
        eprintln!(
            "in WebUI::register_routes(), store? {}",
            router.store.load().is_some()
        );
        router.add_get("/", Self::index);
        router.add_get("/routes/{ingress_id}", Self::routes);
        router.add_get("/routes/peer_asn/{peer_asn}", Self::routes_peer_asn);
        router.add_get("/routes/peer_ip/{peer_ip}", Self::routes_peer_ip);
        router.add_get("/routes/{prefix}/{prefix_len}", Self::routes_prefix);

        router.add_get("/peers/overview", Self::peers_overview);
        router.add_get(
            "/routes/diff/{ingress_a}/{ingress_b}",
            Self::routes_diff,
        );
        router
            .add_get("/peers/diff/{ingress_a}/{ingress_b}", Self::peers_diff);

        router.add_get("/bmp/{ingress}", Self::bmp_details);
    }

    async fn index(state: State<ApiState>) -> Result<Html<String>, String> {
        let mut page = Index::default();
        let _ = state.ingress_register.bmp_routers(&mut page);
        page.bmp_tree = Self::bmp_tree(&state);

        let _ = state.ingress_register.real_bgp_peers(&mut page);
        debug!("got rib? {:?}", state.store.load().is_some());
        page.render()
            .map(Into::into)
            .map_err(|e| format!("rendering error: {}", e))
    }

    async fn bmp_details(
        Path(ingress_id): Path<IngressId>,
        state: State<ApiState>,
    ) -> Result<Html<String>, String> {
        let Some(info) = state
            .ingress_register
            .get(ingress_id)
            .filter(|info| info.ingress_type == Some(IngressType::Bmp))
        else {
            return Err("ingress id {ingress_id} is not a BMP source".into());
        };

        let other_routers = state
            .ingress_register
            .cloned_info()
            .into_iter()
            .filter(|(id, info)| {
                info.ingress_type == Some(IngressType::Bmp)
                    && *id != ingress_id
            })
            .map(|(id, info)| BmpRouter::new(id, info))
            .collect();

        let mut bgp_sessions = BTreeMap::new();

        for (id, info) in state.ingress_register.cloned_info().iter().filter(
            |(_id, info)| {
                info.parent_ingress == Some(ingress_id)
                    && info.ingress_type
                        == Some(crate::ingress::IngressType::BgpViaBmp)
            },
        ) {
            let infos: &mut Vec<_> = bgp_sessions
                .entry((info.remote_asn.unwrap(), info.remote_addr.unwrap()))
                .or_default();
            infos.push((*id, info.clone()))
        }
        BmpDetails {
            router: BmpRouter::new(ingress_id, info),
            other_routers,
            bgp_sessions,
        }
        .render()
        .map(Into::into)
        .map_err(|e| format!("rendering error: {}", e))
    }

    fn routes_with_filter(
        state: State<ApiState>,
        filter: QueryFilter,
    ) -> Result<Routes, String> {
        let Some(ref store) = *state.store.load() else {
            return Err("store not ready".into());
        };

        let mut page = Routes::default();

        let _ = store.search_and_output_routes(
            &mut page,
            AfiSafiType::Ipv4Unicast,
            "0.0.0.0/0".parse().unwrap(),
            filter.clone(),
        );
        let _ = store.search_and_output_routes(
            &mut page,
            AfiSafiType::Ipv6Unicast,
            "::/0".parse().unwrap(),
            filter,
        );

        Ok(page)
    }

    async fn routes(
        Path(ingress_id): Path<IngressId>,
        state: State<ApiState>,
    ) -> Result<Html<String>, String> {
        debug!(
            "in routes for ingress_id {ingress_id}, store? {}",
            state.store.load().is_some()
        );
        let t0 = std::time::Instant::now();

        let mut page = Self::routes_with_filter(
            state.clone(),
            QueryFilter {
                include: vec![Include::LessSpecifics, Include::MoreSpecifics],
                ingress_id: Some(ingress_id),
                ..Default::default()
            },
        )?;
        page.show_only_more_specifics = true;

        if let Some(info) = state.ingress_register.get(ingress_id) {
            if let Some((asn, ribview)) =
                info.remote_asn.zip(info.peer_rib_type)
            {
                page.title = format!("Routes for {asn}/{ribview}");
                if let Some(ii) = info
                    .parent_ingress
                    .and_then(|id| state.ingress_register.get_tuple(id))
                {
                    page.title.push_str(&format!(
                        " on {}",
                        BmpRouter::new(ii.ingress_id, ii.ingress_info)
                    ))
                }
            }
        } else {
            page.title = format!("Routes for ingress_id {ingress_id}");
        }

        let res = page
            .render()
            .map(Into::into)
            .map_err(|e| format!("rendering error: {}", e));

        debug!(
            "/routes/{ingress_id} html search/filter/rendering ({} prefixes): {:?}",
            page.routes.len(),
            Instant::elapsed(&t0),
            );
        res
    }

    async fn routes_peer_asn(
        //Path(peer_asn): Path<Asn>,
        Path(ParamsPeerAsn { peer_asn }): Path<ParamsPeerAsn>,
        state: State<ApiState>,
    ) -> Result<Html<String>, String> {
        debug!(
            "in routes for peer_asn {peer_asn}, store? {}",
            state.store.load().is_some()
        );
        let t0 = std::time::Instant::now();

        let mut page = Self::routes_with_filter(
            state,
            QueryFilter {
                include: vec![Include::LessSpecifics, Include::MoreSpecifics],
                peer_asn: Some(peer_asn),
                ..Default::default()
            },
        )?;
        page.show_only_more_specifics = true;
        page.title = format!("Routes from peer {peer_asn}");

        let res = page
            .render()
            .map(Into::into)
            .map_err(|e| format!("rendering error: {}", e));

        debug!(
            "/routes/{peer_asn} html search/filter/rendering ({} prefixes): {:?}",
            page.routes.len(),
            Instant::elapsed(&t0),
            );
        res
    }

    async fn routes_peer_ip(
        Path(peer_ip): Path<IpAddr>,
        state: State<ApiState>,
    ) -> Result<Html<String>, String> {
        debug!(
            "in routes for peer_ip {peer_ip}, store? {}",
            state.store.load().is_some()
        );
        let t0 = std::time::Instant::now();

        let mut page = Self::routes_with_filter(
            state,
            QueryFilter {
                include: vec![Include::LessSpecifics, Include::MoreSpecifics],
                peer_addr: Some(peer_ip),
                ..Default::default()
            },
        )?;
        page.show_only_more_specifics = true;
        page.title = format!("Routes from peer {peer_ip}");

        let res = page
            .render()
            .map(Into::into)
            .map_err(|e| format!("rendering error: {}", e));

        debug!(
            "/routes/peer_ip/{peer_ip} html search/filter/rendering ({} prefixes): {:?}",
            page.routes.len(),
            Instant::elapsed(&t0),
            );
        res
    }

    async fn routes_prefix(
        Path((prefix, prefix_len)): Path<(IpAddr, u8)>,
        state: State<ApiState>,
    ) -> Result<Html<String>, String> {
        let t0 = std::time::Instant::now();

        let mut page = Routes::default();
        let s = state.store.load();
        let Some(ref store) = *s else {
            return Err("store not ready".into());
        };

        let Ok(prefix) = Prefix::new(prefix, prefix_len) else {
            return Err("illegal prefix".into());
        };

        let afisafi_type = if prefix.is_v6() {
            AfiSafiType::Ipv6Unicast
        } else {
            AfiSafiType::Ipv4Unicast
        };

        let _ = store.search_and_output_routes(
            &mut page,
            afisafi_type,
            prefix,
            QueryFilter {
                include: vec![Include::LessSpecifics, Include::MoreSpecifics],
                ..Default::default()
            },
        );

        page.title = format!("Routes related to {prefix}");

        let res = page
            .render()
            .map(Into::into)
            .map_err(|e| format!("rendering error: {}", e));

        debug!(
            "/routes/{prefix} html search/filter/rendering ({} prefixes): {:?}",
            page.routes.len(),
            Instant::elapsed(&t0),
            );
        res
    }

    fn bmp_tree(state: &State<ApiState>) -> BmpTree {
        let register = state.ingress_register.cloned_info();
        let mut res = BTreeMap::new();
        for (id, info) in register.iter().filter(|(_id, info)| {
            info.ingress_type == Some(crate::ingress::IngressType::Bmp)
        }) {
            res.insert(*id, (info.clone(), BTreeMap::new()));
        }
        for (id, info) in register.iter().filter(|(_id, info)| {
            info.ingress_type == Some(crate::ingress::IngressType::BgpViaBmp)
        }) {
            let (_bmp_ingress_info, bgp) = res
                .get_mut(&info.parent_ingress.expect("should have parent"))
                .expect("should be in hashmap already");
            let infos: &mut Vec<_> = bgp
                .entry((info.remote_asn.unwrap(), info.remote_addr.unwrap()))
                .or_default();
            infos.push((*id, info.clone()))
        }

        res
    }

    fn aggregate_peers(
        recordset: &[rotonda_store::prefix_record::PrefixRecord<
            crate::payload::RotondaPaMap,
        >],
        observed_attributes: &mut ObservedAttributes,
    ) {
        for r in recordset {
            for m in &r.meta {
                if m.meta.rpki_info().rov_status() == RovStatus::Invalid {
                    *observed_attributes |= ObservedAttributes::ROV_INVALID;
                }
                for pa in m.meta.path_attributes().iter() {
                    let Ok(pa) = pa else { break };
                    if pa.type_code() == u8::from(routecore::bgp::types::PathAttributeType::Communities) {
                        *observed_attributes |= ObservedAttributes::COMMUNITIES;
                    }
                    if pa.type_code() == u8::from(routecore::bgp::types::PathAttributeType::ExtendedCommunities) {
                        *observed_attributes |= ObservedAttributes::EXT_COMMUNITIES;
                    }
                    if pa.type_code() == u8::from(routecore::bgp::types::PathAttributeType::LargeCommunities) {
                        *observed_attributes |= ObservedAttributes::LARGE_COMMUNITIES;
                    }
                    if pa.type_code()
                        == u8::from(
                            routecore::bgp::types::PathAttributeType::Otc,
                        )
                    {
                        *observed_attributes |= ObservedAttributes::OTC;
                    }
                }
            }
            if observed_attributes.all_marked() {
                break;
            }
        }
    }

    async fn peers_overview(
        state: State<ApiState>,
    ) -> Result<Html<String>, String> {
        let register = state.ingress_register.cloned_info();
        let s = state.store.load();
        let Some(ref store) = *s else {
            return Err("store not ready".into());
        };
        let mut peers = register
            .par_iter()
            .filter(|(_id, info)| {
                info.ingress_type
                    == Some(crate::ingress::IngressType::BgpViaBmp)
                    || info.ingress_type
                        == Some(crate::ingress::IngressType::Bgp)
            })
            .map(|(ingress_id, info)| {
                // fetch all routes for this mui
                // iterate over path attributes, early abort once we have found everything

                let mut observed_attributes = ObservedAttributes(0);
                let mut route_cnt = 0;

                if let Ok(r) = store.search_routes(
                    AfiSafiType::Ipv4Unicast,
                    "0.0.0.0/0".parse().unwrap(),
                    QueryFilter {
                        include: vec![Include::MoreSpecifics],
                        ingress_id: Some(*ingress_id),
                        ..Default::default()
                    },
                ) {
                    if let Some(recordset) = r.query_result.more_specifics {
                        route_cnt = recordset.len();
                        Self::aggregate_peers(
                            &recordset.v4,
                            &mut observed_attributes,
                        );
                    }

                    if route_cnt == 0 {
                        // nothing for v4, now check v6
                        if let Ok(r) = store.search_routes(
                            AfiSafiType::Ipv6Unicast,
                            "::/0".parse().unwrap(),
                            QueryFilter {
                                include: vec![Include::MoreSpecifics],
                                ingress_id: Some(*ingress_id),
                                ..Default::default()
                            },
                        ) {
                            if let Some(recordset) =
                                r.query_result.more_specifics
                            {
                                route_cnt = recordset.len();
                                Self::aggregate_peers(
                                    &recordset.v4,
                                    &mut observed_attributes,
                                );
                            }
                        }
                    }
                }

                let bmp_router = info.parent_ingress.and_then(|parent_id| {
                    register
                        .get_key_value(&parent_id)
                        .map(|(k, v)| BmpRouter::new(*k, v.clone()))
                });

                Peer::new(
                    info.remote_asn.unwrap(),
                    bmp_router,
                    info.remote_addr.unwrap(),
                    info.peer_rib_type.unwrap(),
                    *ingress_id,
                    route_cnt,
                    observed_attributes,
                )
            })
            .collect::<Vec<_>>();

        peers.sort();
        let page = Peers { peers };

        page.render()
            .map(Into::into)
            .map_err(|e| format!("rendering error: {}", e))
    }

    async fn routes_diff(
        Path((ingress_a, ingress_b)): Path<(IngressId, IngressId)>,
        state: State<ApiState>,
    ) -> Result<Html<String>, String> {
        // fetch asn+ip for ingresses
        let s = state.store.load();
        let Some(ref store) = *s else {
            return Err("store not ready".into());
        };
        let Some(ingress_info_a) = state.ingress_register.get(ingress_a)
        else {
            return Err("no information for ingress id {ingress_a}".into());
        };
        let Some(ingress_info_b) = state.ingress_register.get(ingress_b)
        else {
            return Err("no information for ingress id {ingress_a}".into());
        };

        let Some(peer_a) =
            ingress_info_a.remote_asn.zip(ingress_info_a.remote_addr)
        else {
            return Err(format!(
                "lacking remote ASN or IP address for ingress id {ingress_a}"
            ));
        };

        let Some(peer_b) =
            ingress_info_b.remote_asn.zip(ingress_info_b.remote_addr)
        else {
            return Err(format!(
                "lacking remote ASN or IP address for ingress id {ingress_b}"
            ));
        };

        let mut routes_diff = RoutesDiff::new(peer_a, peer_b);

        // fetch everything from store for a, and b,

        let mut prefixes_a = BTreeSet::new();
        if let Ok(r) = store.search_routes(
            AfiSafiType::Ipv4Unicast,
            "0.0.0.0/0".parse().unwrap(),
            QueryFilter {
                include: vec![Include::MoreSpecifics],
                ingress_id: Some(ingress_a),
                ..Default::default()
            },
        ) {
            if let Some(recordset) = r.query_result.more_specifics {
                prefixes_a = recordset.v4.iter().map(|r| r.prefix).collect();
            }
        };

        if let Ok(r) = store.search_routes(
            AfiSafiType::Ipv6Unicast,
            "::/0".parse().unwrap(),
            QueryFilter {
                include: vec![Include::MoreSpecifics],
                ingress_id: Some(ingress_a),
                ..Default::default()
            },
        ) {
            if let Some(recordset) = r.query_result.more_specifics {
                prefixes_a.append(
                    &mut recordset
                        .v6
                        .iter()
                        .map(|r| r.prefix)
                        .collect::<BTreeSet<_>>(),
                );
            }
        };

        let mut prefixes_b = BTreeSet::new();
        if let Ok(r) = store.search_routes(
            AfiSafiType::Ipv4Unicast,
            "0.0.0.0/0".parse().unwrap(),
            QueryFilter {
                include: vec![Include::MoreSpecifics],
                ingress_id: Some(ingress_b),
                ..Default::default()
            },
        ) {
            if let Some(recordset) = r.query_result.more_specifics {
                prefixes_b = recordset.v4.iter().map(|r| r.prefix).collect();
            }
        };

        if let Ok(r) = store.search_routes(
            AfiSafiType::Ipv6Unicast,
            "::/0".parse().unwrap(),
            QueryFilter {
                include: vec![Include::MoreSpecifics],
                ingress_id: Some(ingress_b),
                ..Default::default()
            },
        ) {
            if let Some(recordset) = r.query_result.more_specifics {
                prefixes_b.append(
                    &mut recordset
                        .v6
                        .iter()
                        .map(|r| r.prefix)
                        .collect::<BTreeSet<_>>(),
                );
            }
        };

        // sort a and b, or create Set
        // get the disjoint diff
        // populate RoutesDiff
        routes_diff.only_a =
            prefixes_a.difference(&prefixes_b).cloned().collect();
        routes_diff.only_b =
            prefixes_b.difference(&prefixes_a).cloned().collect();
        routes_diff.both =
            prefixes_a.intersection(&prefixes_b).cloned().collect();

        routes_diff
            .render()
            .map(Into::into)
            .map_err(|e| format!("rendering error: {}", e))
    }

    async fn peers_diff(
        Path((ingress_a, ingress_b)): Path<(IngressId, IngressId)>,
        state: State<ApiState>,
    ) -> Result<Html<String>, String> {
        let register = state.ingress_register.cloned_info();

        let Some(bmp_a) = register
            .get(&ingress_a)
            .filter(|info| {
                info.ingress_type == Some(crate::ingress::IngressType::Bmp)
            })
            .map(|info| BmpRouter::new(ingress_a, info.clone()))
        else {
            return Err(format!("no (BMP) ingress with id {ingress_a}"));
        };

        let Some(bmp_b) = register
            .get(&ingress_b)
            .filter(|info| {
                info.ingress_type == Some(crate::ingress::IngressType::Bmp)
            })
            .map(|info| BmpRouter::new(ingress_b, info.clone()))
        else {
            return Err(format!("no (BMP) ingress with id {ingress_b}"));
        };

        let sessions_a: BTreeSet<_> = register
            .iter()
            .filter(|(_id, info)| info.parent_ingress == Some(ingress_a))
            .filter_map(|(_id, info)| info.remote_asn.zip(info.remote_addr))
            .collect();

        let sessions_b: BTreeSet<_> = register
            .iter()
            .filter(|(_id, info)| info.parent_ingress == Some(ingress_b))
            .filter_map(|(_id, info)| info.remote_asn.zip(info.remote_addr))
            .collect();

        let mut peers_diff = PeersDiff::new(bmp_a, bmp_b);
        peers_diff.only_a =
            sessions_a.difference(&sessions_b).cloned().collect();
        peers_diff.only_b =
            sessions_b.difference(&sessions_a).cloned().collect();
        peers_diff.both =
            sessions_a.intersection(&sessions_b).cloned().collect();

        peers_diff
            .render()
            .map(Into::into)
            .map_err(|e| format!("rendering error: {}", e))
    }
}

trait AlternativeDisplay {
    fn alt(&self, alt: impl fmt::Display) -> impl fmt::Display;
}

impl<T: fmt::Display> AlternativeDisplay for Option<T> {
    fn alt(&self, alt: impl fmt::Display) -> impl fmt::Display {
        self.as_ref()
            .map(|i| i.to_string())
            .filter(|i| !i.is_empty())
            .unwrap_or(alt.to_string())
    }
}

impl AlternativeDisplay for String {
    fn alt(&self, alt: impl fmt::Display) -> impl fmt::Display {
        if self.is_empty() {
            alt.to_string()
        } else {
            self.to_string()
        }
    }
}
impl AlternativeDisplay for &str {
    fn alt(&self, alt: impl fmt::Display) -> impl fmt::Display {
        if self.is_empty() {
            alt.to_string()
        } else {
            self.to_string()
        }
    }
}

#[derive(Eq, Ord, PartialEq, PartialOrd)]
pub struct BmpRouter {
    pub id: IngressId,
    pub info: IngressInfo,
}

impl BmpRouter {
    pub fn new(id: IngressId, info: IngressInfo) -> Self {
        Self { id, info }
    }
}

#[derive(Eq, Ord, PartialEq, PartialOrd)]
pub struct BgpPeer {
    pub id: IngressId,
    pub info: IngressInfo,
}

impl BgpPeer {
    pub fn new(id: IngressId, info: IngressInfo) -> Self {
        Self { id, info }
    }
}

fn bmp_details(bmp_router: &BmpRouter) -> BmpRouterLink<'_> {
    BmpRouterLink(bmp_router)
}

impl fmt::Display for BmpRouter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(name) = self.info.name.as_ref().filter(|n| !n.is_empty())
        {
            write!(f, "{name}")
        } else {
            write!(f, "__unnamed-bmp-{}", self.id)
        }
    }
}
struct BmpRouterLink<'a>(&'a BmpRouter);

impl fmt::Display for BmpRouterLink<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<a href=\"/bmp/{}\">{}</a>", self.0.id, &self.0)
    }
}

fn peer_asn_link(peer_asn: Asn) -> PeerAsnLink {
    PeerAsnLink(peer_asn)
}
struct PeerAsnLink(Asn);

impl fmt::Display for PeerAsnLink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<a href=\"/routes/peer_asn/{0}\">{0}</a>", self.0)
    }
}

fn peer_ip_link(peer_ip: IpAddr) -> PeerIpLink {
    PeerIpLink(peer_ip)
}
struct PeerIpLink(IpAddr);

impl fmt::Display for PeerIpLink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<a href=\"/routes/peer_ip/{0}\">{0}</a>", self.0)
    }
}

impl fmt::Display for BgpPeer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // ASN@ip (or fallback to 0.0.0.0)
        // else name
        // else ingressid

        if let Some(asn) = self.info.remote_asn.as_ref() {
            write!(f, "{asn}@")?;
            write!(
                f,
                "{}",
                self.info
                    .remote_addr
                    .as_ref()
                    .unwrap_or(&"0.0.0.0".parse().unwrap())
            )
        } else if let Some(name) = self.info.name.as_ref() {
            write!(f, "bgp-peer-{name}")
        } else {
            write!(f, "bgp-peer-lacking-info-id-{}", self.id)
        }
    }
}

type BmpTree = BTreeMap<
    IngressId, // BMP ingress id
    (
        IngressInfo,
        BTreeMap<
            // pointing to a tuple of its info plus a map
            (Asn, IpAddr),
            Vec<(IngressId, IngressInfo)>, // of (asn+ip) pointing to all id+infos
        >,
    ),
>;

impl GenOutput<&mut crate::webui::Index>
    for crate::ingress::register::BmpIdAndInfo<'_>
{
    fn write(
        &self,
        target: &mut &mut crate::webui::Index,
    ) -> Result<(), crate::representation::OutputError> {
        target.bmp_routers.push(BmpRouter::new(
            self.0.ingress_id,
            self.0.ingress_info.clone(),
        ));
        Ok(())
    }
}

impl GenOutput<&mut crate::webui::Index>
    for crate::ingress::register::BgpIdAndInfo<'_>
{
    fn write(
        &self,
        target: &mut &mut crate::webui::Index,
    ) -> Result<(), crate::representation::OutputError> {
        target.bgp_routers.push(BgpPeer::new(
            self.0.ingress_id,
            self.0.ingress_info.clone(),
        ));
        Ok(())
    }
}

#[derive(Default, RsHtml)]
pub struct Index {
    pub bmp_routers: Vec<BmpRouter>,
    pub bgp_routers: Vec<BgpPeer>,
    pub bmp_tree: BmpTree,
}

#[derive(RsHtml)]
pub struct BmpDetails {
    pub router: BmpRouter,
    pub other_routers: Vec<BmpRouter>,
    pub bgp_sessions: BTreeMap<(Asn, IpAddr), Vec<(IngressId, IngressInfo)>>,
}

pub struct RouteDetails {
    pub bmp_router: Option<BmpRouter>,
    pub peer_asn: Option<Asn>,
    pub ribview: Option<PeerRibType>,
    pub origin_asn: String,
    pub rov_status: RovStatus,
    pub otc: Option<Otc>,
    pub as_path: String,
    pub communities: String,
}

#[derive(RsHtml)]
pub struct RoutesDiff {
    peer_a: (Asn, IpAddr),
    peer_b: (Asn, IpAddr),
    pub only_a: Vec<Prefix>,
    pub only_b: Vec<Prefix>,
    pub both: Vec<Prefix>,
}
impl RoutesDiff {
    pub fn new(peer_a: (Asn, IpAddr), peer_b: (Asn, IpAddr)) -> Self {
        Self {
            peer_a,
            peer_b,
            only_a: vec![],
            only_b: vec![],
            both: vec![],
        }
    }
}

#[derive(RsHtml)]
pub struct PeersDiff {
    bmp_a: BmpRouter,
    bmp_b: BmpRouter,
    pub only_a: Vec<(Asn, IpAddr)>,
    pub only_b: Vec<(Asn, IpAddr)>,
    pub both: Vec<(Asn, IpAddr)>,
}

impl PeersDiff {
    pub fn new(bmp_a: BmpRouter, bmp_b: BmpRouter) -> Self {
        Self {
            bmp_a,
            bmp_b,
            only_a: vec![],
            only_b: vec![],
            both: vec![],
        }
    }
}

#[derive(Default, RsHtml)]
pub struct Routes {
    pub routes: Vec<(Prefix, Vec<RouteDetails>)>, // RouteStatus, AsPath and Communities, for starters
    pub less_specifics: Vec<(Prefix, Vec<RouteDetails>)>, // RouteStatus, AsPath and Communities, for starters
    pub more_specifics: Vec<(Prefix, Vec<RouteDetails>)>, // RouteStatus, AsPath and Communities, for starters
    pub show_only_more_specifics: bool,
    pub title: String,
}

impl crate::units::rib_unit::rib::SearchResult {
    fn collect_routes<'a>(
        &'a self,
        routes: impl Iterator<
            Item = &'a rotonda_store::prefix_record::Record<
                crate::payload::RotondaPaMap,
            >,
        >,
    ) -> Vec<RouteDetails> {
        routes
            .map(|m| {
                let route_ingress_info =
                    self.ingress_register.get(m.multi_uniq_id);
                let bmp_router = self
                    .ingress_register
                    .get(m.multi_uniq_id)
                    .and_then(|info| info.parent_ingress)
                    .and_then(|parent_id| {
                        self.ingress_register
                            .get(parent_id)
                            .filter(|parent_info| {
                                parent_info.ingress_type
                                    == Some(IngressType::Bmp)
                            })
                            .map(|parent_info| {
                                BmpRouter::new(parent_id, parent_info)
                            })
                    });

                let pamap = m.meta.path_attributes();
                let aspath = pamap.get::<HopPath>();
                let communities = pamap.get::<StandardCommunitiesList>();
                RouteDetails {
                    bmp_router,
                    peer_asn: route_ingress_info
                        .as_ref()
                        .and_then(|info| info.remote_asn),
                    ribview: route_ingress_info
                        .as_ref()
                        .and_then(|info| info.peer_rib_type),
                    origin_asn: aspath
                        .as_ref()
                        .and_then(|a| a.origin().map(|a| a.to_string()))
                        .unwrap_or("".into()),
                    rov_status: m.meta.rpki_info().rov_status(),
                    otc: pamap.get::<Otc>(),
                    as_path: aspath
                        .map(|a| a.to_string())
                        .unwrap_or("".into()),
                    communities: communities
                        .map(|c| {
                            c.communities()
                                .iter()
                                .map(|c| c.to_string())
                                .collect::<Vec<_>>()
                                .join(", ")
                        })
                        .unwrap_or("".into()),
                }
            })
            .collect()
    }
}

impl GenOutput<&mut Routes> for crate::units::rib_unit::rib::SearchResult {
    fn write(
        &self,
        target: &mut &mut Routes,
    ) -> Result<(), crate::representation::OutputError> {
        dbg!(self.query_result.more_specifics.as_ref().unwrap().v4.len());
        dbg!(self.query_result.more_specifics.as_ref().unwrap().v6.len());
        debug!(
            "in write, current Routes.routes.len: {}",
            target.routes.len()
        );

        if let Some(prefix) = self.query_result.prefix {
            let routes =
                self.collect_routes(self.query_result.records.iter());
            if !routes.is_empty() {
                target.routes.push((prefix, routes));
            }
        }

        let more_specifics =
            self.query_result.more_specifics.as_ref().unwrap();
        for r in more_specifics.v4.iter().chain(&more_specifics.v6) {
            let routes = self.collect_routes(r.meta.iter());
            if !routes.is_empty() {
                target.more_specifics.push((r.prefix, routes));
            }
        }

        let less_specifics =
            self.query_result.less_specifics.as_ref().unwrap();
        for r in less_specifics.v4.iter().chain(&less_specifics.v6) {
            let routes = self.collect_routes(r.meta.iter());
            if !routes.is_empty() {
                target.less_specifics.push((r.prefix, routes));
            }
        }

        debug!("end of write, Routes.routes.len: {}", target.routes.len());

        Ok(())
    }
}

#[derive(Copy, Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct ObservedAttributes(u8);

impl ObservedAttributes {
    const COMMUNITIES: Self = Self(0x1);
    const EXT_COMMUNITIES: Self = Self(0x2);
    const LARGE_COMMUNITIES: Self = Self(0x4);
    const OTC: Self = Self(0x8);
    const ROV_INVALID: Self = Self(0x10);

    pub fn all_marked(&self) -> bool {
        self.0
            == Self::COMMUNITIES.0
                | Self::EXT_COMMUNITIES.0
                | Self::LARGE_COMMUNITIES.0
                | Self::OTC.0
                | Self::ROV_INVALID.0
    }
}

impl fmt::Display for ObservedAttributes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::ops::BitOrAssign for ObservedAttributes {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0
    }
}
impl std::ops::BitAnd for ObservedAttributes {
    type Output = bool;

    fn bitand(self, rhs: Self) -> Self::Output {
        self.0 & rhs.0 > 0
    }
}

#[derive(RsHtml)]
pub struct Peers {
    peers: Vec<Peer>,
}

#[derive(Eq, Ord, PartialEq, PartialOrd)]
struct Peer {
    remote_asn: Asn,
    bmp_router: Option<BmpRouter>,
    remote_addr: IpAddr,
    ribview: PeerRibType,
    ingress_id: IngressId,
    num_routes: usize,
    observed_attributes: ObservedAttributes,
}

impl Peer {
    fn new(
        remote_asn: Asn,
        bmp_router: Option<BmpRouter>,
        remote_addr: IpAddr,
        ribview: PeerRibType,
        ingress_id: IngressId,
        num_routes: usize,
        observed_attributes: ObservedAttributes,
    ) -> Self {
        Self {
            remote_asn,
            bmp_router,
            remote_addr,
            ribview,
            ingress_id,
            num_routes,
            observed_attributes,
        }
    }
}

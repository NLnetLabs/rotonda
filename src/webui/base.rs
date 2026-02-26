use std::{collections::{BTreeMap, BTreeSet, HashMap}, fmt, net::IpAddr, time::Instant};

use axum::{extract::{Path, State}, response::Html};
use inetnum::{addr::Prefix, asn::Asn};
use log::{debug, error};
use routecore::bgp::{aspath::HopPath, message::update_builder::StandardCommunitiesList, types::AfiSafiType};
use rshtml::{RsHtml, traits::RsHtml};

use rayon::prelude::*;

use crate::{http_ng::{Api, ApiState}, ingress::{IngressId, IngressInfo}, representation::GenOutput, roto_runtime::types::PeerRibType, units::rib_unit::{Include, QueryFilter}};

pub struct WebUI { }

#[derive(serde::Deserialize)]
struct ParamsPeerAsn {
    #[serde(deserialize_with = "Asn::deserialize_from_str")]
    peer_asn: Asn,
}

impl WebUI {
    pub fn register_routes(router: &mut Api) {
        eprintln!("in WebUI::register_routes(), store? {}", router.store.load().is_some());
        router.add_get("/", Self::index);
        router.add_get("/routes/{ingress_id}", Self::routes);
        router.add_get("/routes/peer_asn/{peer_asn}", Self::routes_peer_asn);
        router.add_get("/routes/peer_ip/{peer_ip}", Self::routes_peer_ip);
        router.add_get("/routes/{prefix}/{prefix_len}", Self::routes_prefix);

        router.add_get("/peers/overview", Self::peers_overview);
        router.add_get("/routes/diff/{ingress_a}/{ingress_b}", Self::routes_diff);
        router.add_get("/peers/diff/{ingress_a}/{ingress_b}", Self::peers_diff);
    }


    async fn index(state: State<ApiState>) -> Result<Html<String>, String> {
        let mut page = Index::default();
        let _ = state.ingress_register.bmp_routers(&mut page);
        page.bmp_tree = Self::bmp_tree(&state);
        debug!("got rib? {:?}", state.store.load().is_some());
        page.render()
            .map(Into::into)
            .map_err(|e| format!("rendering error: {}", e))
    }

    fn routes_with_filter(
        state: State<ApiState>,
        filter: QueryFilter,
    ) -> Result<Routes, String> {

        let Some(ref store) = *state.store.load() else {
            return Err("store not ready".into())
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
        debug!("in routes for ingress_id {ingress_id}, store? {}", state.store.load().is_some());
        let t0 = std::time::Instant::now();

        let page = Self::routes_with_filter(
            state,
            QueryFilter {
                include: vec![Include::LessSpecifics, Include::MoreSpecifics],
                ingress_id: Some(ingress_id),
                .. Default::default()
            },
        )?;

        let res = page.render()
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
        debug!("in routes for peer_asn {peer_asn}, store? {}", state.store.load().is_some());
        let t0 = std::time::Instant::now();


        let page = Self::routes_with_filter(
            state,
            QueryFilter {
                include: vec![Include::LessSpecifics, Include::MoreSpecifics],
                peer_asn: Some(peer_asn),
                .. Default::default()
            },
        )?;

        let res = page.render()
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
        debug!("in routes for peer_ip {peer_ip}, store? {}", state.store.load().is_some());
        let t0 = std::time::Instant::now();

        let page = Self::routes_with_filter(
            state,
            QueryFilter {
                include: vec![Include::LessSpecifics, Include::MoreSpecifics],
                peer_addr: Some(peer_ip),
                .. Default::default()
            },
        )?;

        let res = page.render()
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
            return Err("store not ready".into())
        };

        let Ok(prefix) = Prefix::new(prefix, prefix_len) else {
            return Err("illegal prefix".into())
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
                .. Default::default()
            },
        );

        let res = page.render()
            .map(Into::into)
            .map_err(|e| format!("rendering error: {}", e));

        debug!(
            "/routes/{prefix} html search/filter/rendering ({} prefixes): {:?}",
            page.routes.len(),
            Instant::elapsed(&t0),
            );
        res
    }

    fn bmp_tree(state: &State<ApiState>) -> BmpTree2 {
        let register = state.ingress_register.cloned_info();
        let mut res = HashMap::new();
        for (id, info) in register.iter().filter(|(_id, info)| {
            info.ingress_type == Some(crate::ingress::IngressType::Bmp)
        }) {
            res.insert(*id, (info.clone(), BTreeMap::new()));
        }
        for (id, info) in register.iter().filter(|(_id, info)| {
            info.ingress_type == Some(crate::ingress::IngressType::BgpViaBmp)
        }) {
            let (_bmp_info, bgp)= res.get_mut(&info.parent_ingress.expect("should have parent"))
                .expect("should be in hashmap already");
            let infos: &mut Vec<_> = bgp.entry((info.remote_asn.unwrap(), info.remote_addr.unwrap()))
                .or_default();
            infos.push((*id, info.clone()))
        }

        res
    }

    async fn peers_overview(
        //Path((prefix, prefix_len)): Path<(IpAddr, u8)>,
        state: State<ApiState>,
    ) -> Result<Html<String>, String> {
        
        let register = state.ingress_register.cloned_info();
        let s = state.store.load();
        let Some(ref store) = *s else {
            return Err("store not ready".into())
        };
        let mut peers = register.par_iter()
            .filter(|(_id, info)|
                info.ingress_type == Some(crate::ingress::IngressType::BgpViaBmp)
                || info.ingress_type == Some(crate::ingress::IngressType::Bgp)
            )
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
                        .. Default::default()
                    },
                ) {
                    if let Some(recordset) = r.query_result.more_specifics {
                        route_cnt = recordset.len();
                        for r in recordset.v4 {
                            for m in r.meta {
                                for pa in m.meta.path_attributes().iter() {
                                    let Ok(pa) = pa else { break };
                                    if pa.type_code() == u8::from(routecore::bgp::types::PathAttributeType::Communities) {
                                        observed_attributes |= ObservedAttributes::COMMUNITIES;
                                    }
                                    if pa.type_code() == u8::from(routecore::bgp::types::PathAttributeType::ExtendedCommunities) {
                                        observed_attributes |= ObservedAttributes::EXT_COMMUNITIES;
                                    }
                                    if pa.type_code() == u8::from(routecore::bgp::types::PathAttributeType::LargeCommunities) {
                                        observed_attributes |= ObservedAttributes::LARGE_COMMUNITIES;
                                    }
                                }
                            }
                            if observed_attributes.all_marked() {
                                break;
                            }
                        }

                    }
                    if route_cnt == 0 {
                        // nothing for v4, now check v6
                        
                        // TODO extact all the replicated code into a separate fn 
                        if let Ok(r) = store.search_routes(
                            AfiSafiType::Ipv6Unicast,
                            "::/0".parse().unwrap(),
                            QueryFilter {
                                include: vec![Include::MoreSpecifics],
                                ingress_id: Some(*ingress_id),
                                .. Default::default()
                            },
                        ) {
                            if let Some(recordset) = r.query_result.more_specifics {
                                route_cnt = recordset.len();
                                for r in recordset.v4 {
                                    for m in r.meta {
                                        for pa in m.meta.path_attributes().iter() {
                                            let Ok(pa) = pa else { break };
                                            if pa.type_code() == u8::from(routecore::bgp::types::PathAttributeType::Communities) {
                                                observed_attributes |= ObservedAttributes::COMMUNITIES;
                                            }
                                            if pa.type_code() == u8::from(routecore::bgp::types::PathAttributeType::ExtendedCommunities) {
                                                observed_attributes |= ObservedAttributes::EXT_COMMUNITIES;
                                            }
                                            if pa.type_code() == u8::from(routecore::bgp::types::PathAttributeType::LargeCommunities) {
                                                observed_attributes |= ObservedAttributes::LARGE_COMMUNITIES;
                                            }
                                        }
                                    }
                                    if observed_attributes.all_marked() {
                                        break;
                                    }
                                }

                            }
                        }
                    }
                }


            (*ingress_id, info.remote_asn.unwrap(), info.remote_addr.unwrap(), info.peer_rib_type.unwrap(), route_cnt, observed_attributes)
        }).collect::<Vec<_>>();

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
            return Err("store not ready".into())
        };
        let Some(ingress_info_a) = state.ingress_register.get(ingress_a) else {
            return Err("no information for ingress id {ingress_a}".into());
        };
        let Some(ingress_info_b) = state.ingress_register.get(ingress_b) else {
            return Err("no information for ingress id {ingress_a}".into());
        };
        // new RoutesDiff
        // TODO fix unwraps
        let mut routes_diff = RoutesDiff::new(
            (ingress_info_a.remote_asn.unwrap(), ingress_info_a.remote_addr.unwrap()),
            (ingress_info_b.remote_asn.unwrap(), ingress_info_b.remote_addr.unwrap()),
        );

        // fetch everything from store for a, and b,

        let mut prefixes_a = BTreeSet::new();
        if let Ok(r) = store.search_routes(
            AfiSafiType::Ipv4Unicast,
            "0.0.0.0/0".parse().unwrap(),
            QueryFilter {
                include: vec![Include::MoreSpecifics],
                ingress_id: Some(ingress_a),
                .. Default::default()
            },
        ) {
            if let Some(recordset) = r.query_result.more_specifics {
                prefixes_a = recordset.v4.iter().map(|r| r.prefix).collect();
            }
        };

        let mut prefixes_b = BTreeSet::new();
        if let Ok(r) = store.search_routes(
            AfiSafiType::Ipv4Unicast,
            "0.0.0.0/0".parse().unwrap(),
            QueryFilter {
                include: vec![Include::MoreSpecifics],
                ingress_id: Some(ingress_b),
                .. Default::default()
            },
        ) {
            if let Some(recordset) = r.query_result.more_specifics {
                prefixes_b = recordset.v4.iter().map(|r| r.prefix).collect();
            }
        };

        //TODO add in ipv6 as well


        // sort a and b, or create Set
        // get the disjoint diff
        // populate RoutesDiff
        routes_diff.only_a = prefixes_a.difference(&prefixes_b).cloned().collect();
        routes_diff.only_b = prefixes_b.difference(&prefixes_a).cloned().collect();
        routes_diff.both = prefixes_a.intersection(&prefixes_a).cloned().collect();

        routes_diff.render()
            .map(Into::into)
            .map_err(|e| format!("rendering error: {}", e))
    }


    async fn peers_diff(
        Path((ingress_a, ingress_b)): Path<(IngressId, IngressId)>,
        state: State<ApiState>,
    ) -> Result<Html<String>, String> {

        
        let register = state.ingress_register.cloned_info();

        let Some(ingress_info_a) = register.get(&ingress_a).filter(|info|
            info.ingress_type == Some(crate::ingress::IngressType::Bmp)
        ) else {
          return Err("no ingress with id {ingress_a}".into());
        };

        let Some(bmp_a) = ingress_info_a.remote_addr.zip(ingress_info_a.name.clone()) else {
            error!("unexpected: bmp ingress {ingress_a} lacks remote_addr and/or name");
            return Err("Lacking remote_addr and/or name for ingress with id {ingress_a}".into());
        };

        let Some(ingress_info_b) = register.get(&ingress_b).filter(|info|
            info.ingress_type == Some(crate::ingress::IngressType::Bmp)
        ) else {
          return Err("no ingress with id {ingress_b}".into());
        };

        let Some(bmp_b) = ingress_info_b.remote_addr.zip(ingress_info_b.name.clone()) else {
            error!("unexpected: bmp ingress {ingress_b} lacks remote_addr and/or name");
            return Err("Lacking remote_addr and/or name for ingress with id {ingress_b}".into());
        };
        
        let sessions_a: BTreeSet<_> = register.iter()
            .filter(|(_id, info)| info.parent_ingress == Some(ingress_a))
            .filter_map(|(_id, info)| info.remote_asn.zip(info.remote_addr))
            .collect()
        ;

        let sessions_b: BTreeSet<_> = register.iter()
            .filter(|(_id, info)| info.parent_ingress == Some(ingress_b))
            .filter_map(|(_id, info)| info.remote_asn.zip(info.remote_addr))
            .collect()
        ;


        let mut peers_diff = PeersDiff::new(bmp_a, bmp_b);
        peers_diff.only_a = sessions_a.difference(&sessions_b).cloned().collect();
        peers_diff.only_b = sessions_b.difference(&sessions_a).cloned().collect();
        peers_diff.both = sessions_a.intersection(&sessions_b).cloned().collect();


        peers_diff.render()
            .map(Into::into)
            .map_err(|e| format!("rendering error: {}", e))
    }

}


type BmpTree2 = HashMap<
    IngressId, // BMP ingress id
    (IngressInfo, BTreeMap< // pointing to a tuple of its info plus a map
        (Asn, IpAddr), Vec<(IngressId, IngressInfo)> // of (asn+ip) pointing to all id+infos
    >)
>;

#[derive(Default, RsHtml)]
pub struct Index {
    pub bmp_routers: Vec<(IngressId, IngressInfo)>,
    pub bgp_routers: Vec<(IngressId, Option<IpAddr>, Option<Asn>)>,
    pub bmp_tree: BmpTree2,
}


pub struct RouteDetails {
    bmp_info: String,
    as_path: String,
    communities: String,
}
impl From<(String, String, String)> for RouteDetails {
    fn from(value: (String, String, String)) -> Self {
        RouteDetails {
            bmp_info: value.0,
            as_path: value.1,
            communities: value.2,
        }
    }
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
    bmp_a: (IpAddr, String),
    bmp_b: (IpAddr, String),
    pub only_a: Vec<(Asn, IpAddr)>,
    pub only_b: Vec<(Asn, IpAddr)>,
    pub both: Vec<(Asn, IpAddr)>,

}

impl PeersDiff {
    pub fn new(bmp_a: (IpAddr, String), bmp_b: (IpAddr, String)) -> Self {
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
}

impl crate::units::rib_unit::rib::SearchResult {
    fn collect_routes<'a>(
        &'a self,
        routes: impl Iterator<Item=&'a rotonda_store::prefix_record::Record<crate::payload::RotondaPaMap>>,
    ) -> Vec<RouteDetails> {
        routes.map(|m| {
            let bmp_info = {
                self.ingress_register.get(m.multi_uniq_id)
                    //.and_then(|info| self.ingress_register.get(info.parent_ingress))
                    .and_then(|info| info.parent_ingress)
                    .and_then(|parent_id| self.ingress_register.get(parent_id))
                    .map(|parent| parent.name)
                    .flatten()
                    .unwrap_or("".into())
            };
            let pamap = m.meta.path_attributes();
            let aspath = pamap.get::<HopPath>();
            let communities = pamap.get::<StandardCommunitiesList>();
            (
                bmp_info,
                aspath.map(|a| a.to_string()).unwrap_or("".into()),
                communities.map(|c| c.communities().iter().map(|c| c.to_string()).collect::<Vec<_>>().join(", "))
                .unwrap_or("".into()),
            ).into()
        }).collect()
    }
}

impl GenOutput<&mut Routes> for crate::units::rib_unit::rib::SearchResult {
    fn write(&self, target: &mut &mut Routes) -> Result<(), crate::representation::OutputError> {

        dbg!(self.query_result.more_specifics.as_ref().unwrap().v4.len());
        dbg!(self.query_result.more_specifics.as_ref().unwrap().v6.len());
        debug!("in write, current Routes.routes.len: {}", target.routes.len());

        if let Some(prefix) = self.query_result.prefix {
            let routes = self.collect_routes(self.query_result.records.iter());
            if !routes.is_empty() {
                target.routes.push((prefix, routes));
            }
        }

        let more_specifics = self.query_result.more_specifics.as_ref().unwrap();
        for r in more_specifics.v4.iter().chain(&more_specifics.v6) {
            let routes = self.collect_routes(r.meta.iter());
            if !routes.is_empty() {
                target.more_specifics.push((r.prefix, routes));
            }
        }

        let less_specifics = self.query_result.less_specifics.as_ref().unwrap();
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

    pub fn all_marked(&self) -> bool {
        self.0 == Self::COMMUNITIES.0 | Self::EXT_COMMUNITIES.0 | Self::LARGE_COMMUNITIES.0
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
    peers: Vec<(IngressId, Asn, IpAddr, PeerRibType, usize, ObservedAttributes)>,
}

use std::{collections::{BTreeMap, HashMap}, net::IpAddr, time::Instant};

use axum::{extract::{Path, State}, response::Html};
use inetnum::{addr::Prefix, asn::Asn};
use log::debug;
use routecore::bgp::{aspath::HopPath, message::update_builder::StandardCommunitiesList, types::AfiSafiType};
use rshtml::{RsHtml, traits::RsHtml};

use crate::{http_ng::{Api, ApiState}, ingress::{IngressId, IngressInfo}, representation::GenOutput, units::rib_unit::{Include, QueryFilter}};

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

        //fn collect_routes<'a>(
        //    routes: impl Iterator<Item=&'a rotonda_store::prefix_record::Record<crate::payload::RotondaPaMap>>,
        //    ingress_register: Arc<Register>,
        //) -> Vec<RouteDetails> {
        //    routes.map(|m| {
        //        let bmp_info = {
        //            //ingress_register. get( m.multi_uniq_id
        //            "".info()
        //        };
        //        let pamap = m.meta.path_attributes();
        //        let aspath = pamap.get::<HopPath>();
        //        let communities = pamap.get::<StandardCommunitiesList>();
        //        (
        //            bmp_info,
        //            aspath.map(|a| a.to_string()).unwrap_or("".into()),
        //            communities.map(|c| c.communities().iter().map(|c| c.to_string()).collect::<Vec<_>>().join(", "))
        //                .unwrap_or("".into()),
        //        ).into()
        //    }).collect()
        //}

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

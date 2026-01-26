use std::{
    collections::{hash_set, HashMap},
    fmt,
    hash::{BuildHasher, Hasher},
    net::IpAddr,
    ops::Deref,
    sync::{Arc, Mutex},
};

use chrono::{Duration, Utc};
use inetnum::{addr::Prefix, asn::Asn};
use log::{debug, error, trace, warn};
use rotonda_store::{
    epoch,
    errors::{FatalResult, PrefixStoreError},
    match_options::{MatchOptions, QueryResult},
    prefix_record::{Meta, PrefixRecord, Record, RecordSet, RouteStatus},
    rib::{config::MemoryOnlyConfig, StarCastRib},
    stats::UpsertReport,
};
use routecore::bgp::{
    aspath::HopPath, nlri::afisafi::{IsPrefix, Nlri}, path_attributes::PaMap, path_selection::{OrdRoute, Rfc4271, TiebreakerInfo}, types::{AfiSafiType, Otc}
};
use serde::{ser::{SerializeSeq, SerializeStruct}, Serialize, Serializer};

use crate::{
    ingress::{self, register::{IdAndInfo, OwnedIdAndInfo}, IngressId, IngressInfo}, payload::{RotondaPaMap, RotondaPaMapWithQueryFilter, RotondaRoute, RouterId}, representation::{GenOutput, Json}, roto_runtime::{types::{RotoPackage}, Ctx}
};

use super::{http_ng::Include, QueryFilter};

type Store = StarCastRib<RotondaPaMap, MemoryOnlyConfig>;

type RotoHttpFilter = roto::TypedFunc<
    Ctx,
    fn (roto::Val<crate::roto_runtime::RcRotondaPaMap>,) -> roto::Verdict<(), ()>,
>;

#[derive(Clone)]
pub struct Rib {
    unicast: Arc<Option<Store>>,
    multicast: Arc<Option<Store>>,
    #[allow(dead_code)]
    other_fams:
        HashMap<AfiSafiType, HashMap<(IngressId, Nlri<bytes::Bytes>), PaMap>>,
    ingress_register: Arc<ingress::Register>,
    roto_package: Option<Arc<RotoPackage>>,
    roto_context: Arc<Mutex<Ctx>>,
}

#[derive(Copy, Clone, Debug)]
struct Multicast(bool);

impl Rib {
    pub fn new(
        ingress_register: Arc<ingress::Register>,
        roto_package: Option<Arc<RotoPackage>>,
        roto_context: Arc<Mutex<Ctx>>,
    ) -> Result<Self, PrefixStoreError> {
        Ok(Rib {
            unicast: Arc::new(Some(Store::try_default()?)),
            multicast: Arc::new(Some(Store::try_default()?)),
            other_fams: HashMap::new(),
            ingress_register,
            roto_package,
            roto_context,
        })
    }

    pub fn store(&self) -> Result<&Store, PrefixStoreError> {
        if let Some(rib) = self.unicast.as_ref() {
            Ok(rib)
        } else {
            Err(PrefixStoreError::StoreNotReadyError)
        }
    }

    pub fn insert(
        &self,
        val: &RotondaRoute,
        route_status: RouteStatus,
        ltime: u64,
        ingress_id: IngressId,
    ) -> Result<UpsertReport, String> {
        let res = match val {
            RotondaRoute::Ipv4Unicast(n, ..) => self.insert_prefix(
                &n.prefix(),
                Multicast(false),
                val,
                route_status,
                ltime,
                ingress_id,
            ),
            RotondaRoute::Ipv6Unicast(n, ..) => self.insert_prefix(
                &n.prefix(),
                Multicast(false),
                val,
                route_status,
                ltime,
                ingress_id,
            ),
            RotondaRoute::Ipv4Multicast(n, ..) => self.insert_prefix(
                &n.prefix(),
                Multicast(true),
                val,
                route_status,
                ltime,
                ingress_id,
            ),
            RotondaRoute::Ipv6Multicast(n, ..) => self.insert_prefix(
                &n.prefix(),
                Multicast(true),
                val,
                route_status,
                ltime,
                ingress_id,
            ),
        };
        res.map_err(|e| e.to_string())
    }

    fn insert_prefix(
        &self,
        prefix: &Prefix,
        multicast: Multicast,
        val: &RotondaRoute,
        route_status: RouteStatus,
        ltime: u64,
        ingress_id: IngressId,
    ) -> Result<UpsertReport, PrefixStoreError> {
        // Check whether our self.rib is Some(..) or bail out.
        let arc_store = match multicast.0 {
            true => self.multicast.clone(),
            false => self.unicast.clone(),
        };

        let store = (*arc_store)
            .as_ref()
            .ok_or(PrefixStoreError::StoreNotReadyError)?;

        let mui = ingress_id;

        if route_status == RouteStatus::Withdrawn {
            // instead of creating an empty PrefixRoute for this Prefix and
            // putting that in the store, we use the new
            // mark_mui_as_withdrawn_for_prefix . This way, we preserve the
            // last seen attributes/nexthop for this {prefix,mui} combination,
            // while setting the status to Withdrawn.
            store.mark_mui_as_withdrawn_for_prefix(prefix, mui, 0)?;

            // FIXME this is just to satisfy the function signature, but is
            // quite useless as-is.
            return Ok(UpsertReport {
                cas_count: 0,
                prefix_new: false,
                mui_new: false,
                mui_count: 0,
            });
        }

        let pubrec = Record::new(
            mui,
            ltime,
            route_status,
            val.rotonda_pamap().clone(),
        );
        
        store.insert(
            prefix, pubrec, None, // Option<TBI>
        )
    }

    pub fn withdraw_for_ingress(
        &self,
        ingress_id: IngressId,
        specific_afisafi: Option<AfiSafiType>,
    ) {
        // This signals a withdraw-all-for-peer, because a BGP session
        // was lost or because a BMP PeerDownNotification was
        // received.

        // Things to take care of, here of elsewhere:
        //
        // * mark all (active) prefixes for this ingress as
        //   'withdrawn' in the store
        // * generate BGP UPDATEs for those prefixes that were
        //   actually updated to the withdrawn state. Note that there
        //   might have been prefixes for this ingress that were
        //   previously withdrawn already, for which no UPDATEs should
        //   be generated!
        // * send out these UPDATEs as Update::Bulk payloads to the
        //   east:
        //     - what if the first unit eastwards is another RIB, does
        //     it make sense to create the UPDATEs? might make more
        //     sense to forward the current Update::Withdraw(..)
        //     instead.
        //     - the UPDATEs only make sense if anything needs to go
        //     out over a BGP session again. But, in that case, the
        //     UPDATE can only be correctly generated by the BGP
        //     connection (in the ingress unit) itself, because of
        //     possible session-level state (e.g. ADDPATH or Extended
        //     PDU size capabilities).
        //     Moreover, it only makes sense to send out the UPDATE if
        //     the specific prefix was previously annouced, i.e. it is
        //     in the Adj-RIB-Out for that session. This might be
        //     differ from session to session because of local policy,
        //     roto scripts, or what not.
        //     As such, perhaps we should leave the generation of
        //     those withdrawals to the very latest (most-East) point?

        match specific_afisafi {
            None => {
                // Set all address families to withdrawn.
                // In addition to unicast prefixes, stored in the
                // store proper, we might need to update other data
                // structures holding more exotic families.

                //The store seems to lack a 'mark_mui_as_withdrawn'
                //that handles both v4 and v6 in one go.

                if let Err(e) = (*self.unicast)
                    .as_ref()
                    .unwrap()
                    .mark_mui_as_withdrawn(ingress_id)
                {
                    error!(
                        "failed to mark MUI as withdrawn in unicast rib: {}",
                        e
                    )
                }

                if let Err(e) = (*self.multicast)
                    .as_ref()
                    .unwrap()
                    .mark_mui_as_withdrawn(ingress_id)
                {
                    error!("failed to mark MUI as withdrawn in multicast rib: {}", e)
                }

                // TODO withdraw all other afisafis as well!
            }
            Some(AfiSafiType::Ipv4Unicast) => {
                if let Err(e) = (*self.unicast)
                    .as_ref()
                    .unwrap()
                    .mark_mui_as_withdrawn_v4(ingress_id)
                {
                    error!("failed to mark MUI as withdrawn for v4: {}", e)
                }
            }
            Some(AfiSafiType::Ipv6Unicast) => {
                if let Err(e) = (*self.unicast)
                    .as_ref()
                    .unwrap()
                    .mark_mui_as_withdrawn_v6(ingress_id)
                {
                    error!("failed to mark MUI as withdrawn for v6: {}", e)
                }
            }
            Some(AfiSafiType::Ipv4Multicast) => {
                if let Err(e) = (*self.multicast)
                    .as_ref()
                    .unwrap()
                    .mark_mui_as_withdrawn_v4(ingress_id)
                {
                    error!("failed to mark MUI as withdrawn for v4: {}", e)
                }
            }
            Some(AfiSafiType::Ipv6Multicast) => {
                if let Err(e) = (*self.multicast)
                    .as_ref()
                    .unwrap()
                    .mark_mui_as_withdrawn_v6(ingress_id)
                {
                    error!("failed to mark MUI as withdrawn for v6: {}", e)
                }
            }

            afisafi => {
                panic!("no support to withdraw {:?} yet", afisafi)
            }
        }
    }

    pub fn mark_ingress_active(
        &self,
        ingress_id: IngressId,
    ) {
        if let Err(e) = (*self.unicast)
            .as_ref()
                .unwrap()
                .mark_mui_as_active_v4(ingress_id)
        {
            error!("failed to mark MUI as active in unicast v4 rib: {e}")
        }
        if let Err(e) = (*self.unicast)
            .as_ref()
                .unwrap()
                .mark_mui_as_active_v6(ingress_id)
        {
            error!("failed to mark MUI as active in unicast v6 rib: {e}")
        }
        if let Err(e) = (*self.multicast)
            .as_ref()
                .unwrap()
                .mark_mui_as_active_v4(ingress_id)
        {
            error!("failed to mark MUI as active in multicast v4 rib: {e}")
        }
        if let Err(e) = (*self.multicast)
            .as_ref()
                .unwrap()
                .mark_mui_as_active_v6(ingress_id)
        {
            error!("failed to mark MUI as active in multicast v6 rib: {e}")
        }

    }

    pub fn match_prefix(
        &self,
        prefix: &Prefix,
        match_options: &MatchOptions,
    ) -> Result<QueryResult<RotondaPaMap>, String> {
        let guard = &epoch::pin();
        let store = (*self.unicast)
            .as_ref()
            .ok_or(PrefixStoreError::StoreNotReadyError.to_string())?;
        let unicast_res = store
            .match_prefix(prefix, match_options, guard)
            .map_err(|err| err.to_string())?;
        if unicast_res.records.is_empty()
            && unicast_res.less_specifics.is_none()
            && unicast_res.more_specifics.is_none()
        {
            debug!("no result in unicast store, trying multicast");
            let multicast_store = (*self.multicast)
                .as_ref()
                .ok_or(PrefixStoreError::StoreNotReadyError.to_string())?;
            let multicast_res = multicast_store
                .match_prefix(prefix, match_options, guard)
                .map_err(|err| err.to_string())?;
            if !(multicast_res.records.is_empty()
                && multicast_res.less_specifics.is_none()
                && multicast_res.more_specifics.is_none())
            {
                return Ok(multicast_res);
            }
        }
        Ok(unicast_res)
    }

    pub fn match_ingress_id(
        &self,
        ingress_id: IngressId,
        //match_options: &MatchOptions,
    ) -> Result<Vec<PrefixRecord<RotondaPaMap>>, String> {
        let guard = &epoch::pin();
        let store = (*self.unicast)
            .as_ref()
            .ok_or(PrefixStoreError::StoreNotReadyError.to_string())?;

        let include_withdrawals = false;

        let mut res = store
            .iter_records_for_mui_v4(ingress_id, include_withdrawals, guard)
            .collect::<FatalResult<Vec<_>>>()
            .map_err(|e| e.to_string())?;
        res.append(
            &mut store
                .iter_records_for_mui_v6(
                    ingress_id,
                    include_withdrawals,
                    guard,
                )
                .collect::<FatalResult<Vec<_>>>()
                .map_err(|e| e.to_string())?,
        );

        //tmp: while the per mui methods do not work yet, we can use
        //.prefixes_iter() to test the output.
        //let res = store.prefixes_iter().collect::<Vec<_>>();
        debug!(
            "rib::match_ingress_id for {ingress_id}: {} results",
            res.len()
        );
        Ok(res)
    }


    //
    // new methods returning results to be used by both HTTP API and CLI, i.e. types that will need
    // impls for ToJson and ToCli so they can be impl OutputFormat
    //
    // For now, all these new methods are prefixed search_
    //

    /// Query the Store for routes based on Nlri/prefix
    pub fn search_routes(
        &self,
        afisafi: AfiSafiType,
        //nlri: Nlri<&[u8]>,
        nlri: Prefix, // change to Nlri or equivalent after routecore refactor
        filter: QueryFilter,
    //) -> Result<QueryResult<RotondaPaMap>, String> {
    ) -> Result<SearchResult, String> {
        let guard = &epoch::pin();

        let store = match afisafi {
            AfiSafiType::Ipv4Unicast |
            AfiSafiType::Ipv6Unicast => {
                (*self.unicast)
                    .as_ref()
                    .ok_or(PrefixStoreError::StoreNotReadyError.to_string())?
                }
            AfiSafiType::Ipv4Multicast |
            AfiSafiType::Ipv6Multicast => {
                (*self.multicast)
                    .as_ref()
                    .ok_or(PrefixStoreError::StoreNotReadyError.to_string())?
            }
            u => {
                return Err(format!("address family {u} unsupported"));
            }
        };

        let match_options = &MatchOptions {
            match_type: rotonda_store::match_options::MatchType::ExactMatch,
            include_withdrawn: false,
            include_less_specifics: filter.include.contains(&Include::LessSpecifics),
            include_more_specifics: filter.include.contains(&Include::MoreSpecifics),
            mui: filter.ingress_id,
            include_history: rotonda_store::match_options::IncludeHistory::None,
        };

        let t0 = std::time::Instant::now();
        let mut res = store
            .match_prefix(&nlri, match_options, guard)
            .map(|res| SearchResult { query_result: res, ingress_register: self.ingress_register.clone(), query_filter: filter.clone() } )
            .map_err(|err| err.to_string());

        // filter on:
        // X origin asn
        // X peer rib type
        // X ingress_id -> done via Store.match_prefix already
        // X otc
        //
        // - community
        // - large community
        // - peer distinguisher
        
        debug!("store lookup took {:?}", std::time::Instant::now().duration_since(t0));


        // Find the roto function from the compiled Roto Package.
        // We do this here, once, to reduce acquiring locks and such over and over.
        // If the query contains a filter name for which no roto function exists, this simply
        // filters as if no filter was passed:

        //let maybe_roto_function: Option<RotoHttpFilter> = filter.roto_function.as_ref().and_then(|name| {
        //    self.roto_package.as_ref().and_then(|package| {
        //        let mut package = package.lock().unwrap();
        //        package.get_function(name.as_str()).ok()
        //    })
        //});

        // Alternatively, we could return an error:
        let maybe_roto_function: Option<RotoHttpFilter> = match filter.roto_function.as_ref() {
            Some(name) => {
                debug!("looking up {name} in compiled roto package");
                if let Some(f) = self.roto_package.as_ref().and_then(|package| {
                    let mut package = package.lock().unwrap();
                    package.get_function(name.as_str()).ok()
                }) {
                    Some(f)
                } else {
                    error!("query for undefined roto filter");
                    return Err(format!("no roto function '{name}' defined"));
                }
            }
            None => None
        };




        let t0 = std::time::Instant::now();

        let _ = res.as_mut().map(|sr| {
            self.apply_filter(&mut sr.query_result.records, &filter, maybe_roto_function.clone());
            if let Some(rs) = sr.query_result.more_specifics.as_mut() {
                rs.v4.retain_mut(|pr|{
                    self.apply_filter(&mut pr.meta, &filter, maybe_roto_function.clone());
                    !pr.meta.is_empty()
                });
                rs.v6.retain_mut(|pr|{
                    self.apply_filter(&mut pr.meta, &filter, maybe_roto_function.clone());
                    !pr.meta.is_empty()
                });
            }
            if let Some(rs) = sr.query_result.less_specifics.as_mut() {
                rs.v4.retain_mut(|pr|{
                    self.apply_filter(&mut pr.meta, &filter, maybe_roto_function.clone());
                    !pr.meta.is_empty()
                });
                rs.v6.retain_mut(|pr|{
                    self.apply_filter(&mut pr.meta, &filter, maybe_roto_function.clone());
                    !pr.meta.is_empty()
                });
            }
        });

        debug!("filtering took {:?}", std::time::Instant::now().duration_since(t0));
        
        res

    }

    // XXX:
    // if the results from the store are already filtered on a MUI/ingress_id, we do not need to
    // query the ingress register over and over to fetch info like peer_rib_type
    // In such case, we could optimize:
    //  - fetch the required info once, pass it into apply_filter
    //  - in apply_filter, check for such info and branch: if let Some(passed_info), etc
    
    fn apply_filter(&self, records: &mut Vec<Record<RotondaPaMap>>, filter: &QueryFilter, roto_filter: Option<RotoHttpFilter>) {
        if let Some(rib_type) = filter.rib_type {
            records.retain(|r|{
                self.ingress_register.get(r.multi_uniq_id).map(|ii|
                    ii.peer_rib_type == Some(rib_type)
                ).unwrap_or(true)
            });
        }

        if let Some(peer_asn) = filter.peer_asn {
            records.retain(|r|{
                self.ingress_register.get(r.multi_uniq_id).map(|ii|
                    ii.remote_asn == Some(peer_asn)
                ).unwrap_or(true)
            });
        }

        if let Some(peer_addr) = filter.peer_addr {
            records.retain(|r|{
                self.ingress_register.get(r.multi_uniq_id).map(|ii|
                    ii.remote_addr == Some(peer_addr)
                ).unwrap_or(true)
            });
        }

        if let Some(f) = roto_filter {
            let mut ctx = self.roto_context.lock().unwrap();
            records.retain_mut(|r| {
                let rc_r: crate::roto_runtime::RcRotondaPaMap = std::mem::take(&mut r.meta).into();
                match f.call(&mut ctx, roto::Val(rc_r.clone())) {
                    roto::Verdict::Accept(_) => {
                        r.meta = std::rc::Rc::into_inner(rc_r).unwrap();
                        true
                    }
                    roto::Verdict::Reject(_) => {
                        //debug!("in Reject for {}", roto_function);
                        false
                    }
                }
            });

        }

        if filter.origin_asn.is_some() ||
            filter.otc.is_some() ||
            filter.community.is_some() ||
            filter.large_community.is_some() ||
            filter.rov_status.is_some()
        {
            records.retain(|r| {
                if let Some(rov_status) = filter.rov_status {
                    if r.meta.rpki_info().rov_status() != rov_status {
                        return false
                    }
                }
                let path_attributes = r.meta.path_attributes();
                if let Some(otc) = filter.otc {
                    if Some(otc) != path_attributes.get::<Otc>().map(|otc| otc.0) {
                        return false
                    }
                }
                if let Some(large_community) = filter.large_community {
                    if let Some(list) = path_attributes.get::<routecore::bgp::path_attributes::LargeCommunitiesList>() {
                        if !list.communities().contains(&large_community) {
                            return false
                        }
                    } else {
                        return false
                    }
                }
                if let Some(community) = filter.community {
                    if let Some(list) = path_attributes.get::<routecore::bgp::message::update_builder::StandardCommunitiesList>() {
                        if !list.communities().contains(&community) {
                            return false
                        }
                    } else {
                        return false
                    }
                }
                if let Some(origin_asn) = filter.origin_asn {
                    if Some(origin_asn) != path_attributes.get::<HopPath>().and_then(|hp|
                        hp.origin().and_then(|hop| hop.clone().try_into().ok())
                    ) {
                        return false;
                    }
                }
                true
            });

            // TODO:
            // - communities
            // - large communities
            // - route distinguisher

        }
    }

    pub fn search_and_output_routes<T>(
        &self,
        mut target: T,
        afisafi: AfiSafiType,
        //nlri: Nlri<&[u8]>,
        nlri: Prefix, // change to Nlri or equivalent after routecore refactor
        filter: QueryFilter,
    ) -> Result<(), String>
        where SearchResult: GenOutput<T>
    {
        match self.search_routes(afisafi, nlri, filter) {
            Ok(search_results) => {
                let _ = search_results.write(&mut target);
            },
            Err(e) => {
                error!("error in search_and_output_routes: {e}");
                return Err(format!("store error: {e}"));
            }
        }

        Ok(())
    }

    /// Query the store based on `IngressId`/MUI
    pub fn search_routes_for_ingress(
        _afisafi: AfiSafiType,
        _nlri: Nlri<&[u8]>,
        _ingress_id: IngressId,
        _match_options: MatchOptions
    ) -> Result<SearchResult, String> {
        todo!()
    }

    /// Query the store based on Origin AS in the AS_PATH
    pub fn search_routes_for_origin_as(
        _afisafi: AfiSafiType,
        _origin_as: Asn,
        _match_options: MatchOptions
    ) -> Result<SearchResult, String> {
        todo!()
    }
}

/// Wrapper around `QueryResult` from rotonda-store
///
/// This wrapper is used to impl the necessary traits on, to enable consistent representation
/// between CLI, HTTP API, etc.
pub struct SearchResult {
    query_result: QueryResult<RotondaPaMap>,
    ingress_register: Arc<ingress::Register>,
    query_filter: QueryFilter,
}

crate::genoutput_json!(SearchResult);

impl Serialize for SearchResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {

        // TODO:
        // - ingress data (include in Arc<Register> in SearchResults wrapper?
        // X rpki rov status
        // X route status
        // - path attributes
        //      X first go based on existing Serialize impl
        //      - have a good look on what we did vs what we now think is best
        //      - especially communities:
        //          - old style was 241M vs ~90M for the 25M raw BMP input data
        //          - can we provide multiple 'styles' of output (via some query param), e.g.
        //              - the old, very verbose one,
        //              - one with Martin Pels' draft applied
        //          
        //
        //         
        // - includes:
        //  X more specifics
        //  X less specifics
        //  - lpm?
        //
        //  XXX: old format returned "data": [] (i.e. an array) so the matching prefix/nlri was
        //  repeated $n times.
        //  is that correct? shouldn't it be:
        //      "data": {
        //          "nlri": $some_nlri,
        //          "routes": [ ... ]
        //      },
        //      "included": ...
        // 
        // the good thing about that repetition though is, that when including routes for more/less
        // specifics in the "included" section, we can follow the exact same structure?
        //
        //  XXX json:api states "included" is an _array_ where we returned a object before
        //  perhaps go with
        //
        //      "included": [
        //          {
        //              "include_type": "moreSpecifics", 
        //                  "data": {
        //                      "nlri": $some_nlri,
        //                      "routes": [ { .. }, .. ]
        //                  }
        //          },
        //          {
        //              "include_type": "lessSpecifics",
        //                  "data": {
        //                      "nlri": $some_nlri,
        //                      "routes": [ { .. }, .. ]
        //                  }
        //          }
        //      ]
        //              
        //
        //
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct IncludedData<'a, 'b, 'c> {
            #[serde(skip_serializing_if = "Option::is_none")]
            more_specifics: Option<RecordSetWrapper<'a, 'b, 'c>>,
            #[serde(skip_serializing_if = "Option::is_none")]
            less_specifics: Option<RecordSetWrapper<'a, 'b, 'c>>,
        }

        let mut root = serializer.serialize_struct("nlri", 3)?;
        // TODO meta:
        // - routes pre filtering
        // - routes post filtering (== returned items)
        // - time to get from store
        // - time to serialize to json? (is that possible? or should meta then be at the end of the
        //   response perhaps?)
        root.serialize_field("meta", &None::<String>)?;
        root.serialize_field("data", &Data {
            nlri: self.query_result.prefix,
            routes: RecordsWrapper(&self.query_result.records, &self.ingress_register, &self.query_filter),
        })?;

        root.serialize_field("included",
            &IncludedData {
                more_specifics: self.query_result.more_specifics.as_ref().map(|s| RecordSetWrapper(s, &self.ingress_register, &self.query_filter)),
                less_specifics: self.query_result.less_specifics.as_ref().map(|s| RecordSetWrapper(s, &self.ingress_register, &self.query_filter)),

            }
        )?;
        root.end()

    }
}

#[derive(Serialize)]
struct Data<'a, 'b, 'c> {
    nlri: Option<Prefix>,
    routes: RecordsWrapper<'a, 'b, 'c>,
}

struct RecordsWrapper<'a, 'b, 'c>(&'a Vec<Record<RotondaPaMap>>, &'b Arc<ingress::Register>, &'c QueryFilter);
struct RecordWrapper<'a, 'b, 'c>(&'a Record<RotondaPaMap>, &'b Arc<ingress::Register>, &'c QueryFilter);
struct RecordSetWrapper<'a, 'b, 'c>(&'a RecordSet<RotondaPaMap>, &'b Arc<ingress::Register>, &'c QueryFilter);
struct PrefixRecordWrapper<'a, 'b, 'c>(&'a PrefixRecord<RotondaPaMap>, &'b Arc<ingress::Register>, &'c QueryFilter);
struct RouteStatusWrapper(RouteStatus);

impl Serialize for RecordsWrapper<'_, '_, '_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {

        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for e in self.0.iter() {
            seq.serialize_element(&RecordWrapper(e, self.1, self.2))?;
        }
        seq.end()

    }
}

impl Serialize for RecordWrapper<'_, '_, '_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {

        // The RPKI information is stored in the value (so, RotondaPaMap) in the store.
        // The RotondaPaMap serializes to { rpki: {}, pathAttributes: [] },
        // so with serde(flatten) the wrapped store::Record serializes to
        // { status: foo, rpki: bla, pathAttributes: buzz, etc .. }
        // on 'one level'.
        //
        #[derive(Serialize)]
        struct Helper<'a> {
            status: RouteStatusWrapper,
            ingress: OwnedIdAndInfo,
            #[serde(flatten)]
            pamap: &'a RotondaPaMap,
            //pamap: RotondaPaMapWithQueryFilter<'a, 'b>,//(&RotondaPaMap, &self.2),
        }

        #[derive(Serialize)]
        struct HelperWithQueryFilter<'a, 'b> {
            status: RouteStatusWrapper,
            ingress: OwnedIdAndInfo,
            #[serde(flatten)]
            pamap: RotondaPaMapWithQueryFilter<'a, 'b>,//(&RotondaPaMap, &self.2),
        }

        // Possible optimisation: lift this wrapping (and thus branching up) into RecordsWrapper or
        // even SearchResult.
        // Have variants for:
        // - NoPathAttributes, i.e. &fields[pathAttributes]=
        // - FilteredPathAttrbutes, i.e. is_some() && !is_empty()
        // - Default case, not specified, so we only filter out typecodes 14 and 15 (MP
        // REACH/UNREACH) while those are stored. After the refactoring of routecore et al and we
        // are sure 14/15 do not end up in the store, that .filter can be removed completely.
        if self.2.fields_path_attributes.is_some() {
            HelperWithQueryFilter {
                ingress: self.1.get_tuple(self.0.multi_uniq_id).unwrap(),
                status: RouteStatusWrapper(self.0.status),
                pamap: RotondaPaMapWithQueryFilter(&self.0.meta, self.2),
            }.serialize(serializer)
        } else {
            Helper {
                ingress: self.1.get_tuple(self.0.multi_uniq_id).unwrap(),
                status: RouteStatusWrapper(self.0.status),
                pamap: &self.0.meta
            }.serialize(serializer)

        }
    }
}

impl Serialize for RecordSetWrapper<'_, '_, '_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer {
            let mut s = serializer.serialize_seq(Some(self.0.len()))?;
            for e in &self.0.v4 {
               s.serialize_element(&PrefixRecordWrapper(e, self.1, self.2))?;
            }
            for e in &self.0.v6 {
               s.serialize_element(&PrefixRecordWrapper(e, self.1, self.2))?;
            }
       s.end()
    }
}

impl Serialize for PrefixRecordWrapper<'_, '_, '_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer {
            
             Data {
                nlri: Some(self.0.prefix),
                routes: RecordsWrapper(&self.0.meta, self.1, self.2),
            }.serialize(serializer)
        
    }
}

impl Serialize for RouteStatusWrapper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer {
        match self.0 {
            RouteStatus::Active => serializer.serialize_str("active"),
            RouteStatus::InActive => serializer.serialize_str("inactive"),
            RouteStatus::Withdrawn => serializer.serialize_str("withdrawn"),
        }
    }
}



#[derive(Debug)]
pub enum StoreInsertionEffect {
    RoutesWithdrawn(usize),
    #[allow(dead_code)]
    RoutesRemoved(usize),
    RouteAdded,
    RouteUpdated,
}



// --- Tests ----------------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::{
        alloc::System, net::IpAddr, ops::Deref, str::FromStr, sync::Arc,
    };

    use inetnum::{addr::Prefix, asn::Asn};
    //use roto::types::{
    //    builtin::{BuiltinTypeValue, NlriStatus, PrefixRoute, RotondaId},
    //    lazyrecord_types::BgpUpdateMessage,
    //    typevalue::TypeValue,
    //};
    use routecore::bgp::{message::SessionConfig, types::AfiSafiType};

    use crate::{
        bgp::encode::{mk_bgp_update, Announcements, Prefixes},
        common::memory::TrackingAllocator,
    };

    use super::*;

    // LH: these do not make much sense anymore with the new prefix store
    // doing all the updating/merging of entries. Adapting does not seem to be
    // worth it, perhaps we redo some of these from scratch?
    /*
    #[test]
    fn empty_by_default() {
        let rib_value = RibValue::default();
        assert!(rib_value.is_empty());
    }

    #[test]
    fn into_new() {
        let rib_value: RibValue =
            PreHashedTypeValue::new(123u8.into(), 18).into();
        assert_eq!(rib_value.len(), 1);
        assert_eq!(
            rib_value.iter().next(),
            Some(&Arc::new(PreHashedTypeValue::new(123u8.into(), 18)))
        );
    }

    #[test]
    fn merging_in_separate_values_yields_two_entries() {
        let settings = StoreEvictionPolicy::UpdateStatusOnWithdraw.into();
        let rib_value = RibValue::default();
        let value_one = PreHashedTypeValue::new(1u8.into(), 1);
        let value_two = PreHashedTypeValue::new(2u8.into(), 2);

        let (rib_value, _user_data) = rib_value
            .clone_merge_update(&value_one.into(), Some(&settings))
            .unwrap();
        assert_eq!(rib_value.len(), 1);

        let (rib_value, _user_data) = rib_value
            .clone_merge_update(&value_two.into(), Some(&settings))
            .unwrap();
        assert_eq!(rib_value.len(), 2);
    }

    #[test]
    fn merging_in_the_same_precomputed_hashcode_yields_one_entry() {
        let settings = StoreEvictionPolicy::UpdateStatusOnWithdraw.into();
        let rib_value = RibValue::default();
        let value_one = PreHashedTypeValue::new(1u8.into(), 1);
        let value_two = PreHashedTypeValue::new(2u8.into(), 1);

        let (rib_value, _user_data) = rib_value
            .clone_merge_update(&value_one.into(), Some(&settings))
            .unwrap();
        assert_eq!(rib_value.len(), 1);

        let (rib_value, _user_data) = rib_value
            .clone_merge_update(&value_two.into(), Some(&settings))
            .unwrap();
        assert_eq!(rib_value.len(), 1);
    }

    #[test]
    fn merging_in_a_withdrawal_updates_matching_entries() {
        // Given route announcements and withdrawals from a couple of peers to a single prefix
        let prefix = Prefix::new("127.0.0.1".parse().unwrap(), 32).unwrap();

        let peer_one = PeerId::new(
            Some(IpAddr::from_str("192.168.0.1").unwrap()),
            Some(Asn::from_u32(123)),
        );
        let peer_two = PeerId::new(
            Some(IpAddr::from_str("192.168.0.2").unwrap()),
            Some(Asn::from_u32(456)),
        );

        let peer_one_announcement_one =
            mk_route_announcement(prefix, "123,456,789", peer_one);
        let peer_one_announcement_two =
            mk_route_announcement(prefix, "123,789", peer_one);
        let peer_two_announcement_one =
            mk_route_announcement(prefix, "456,789", peer_two);
        let peer_one_withdrawal = mk_route_withdrawal(prefix, peer_one);

        let peer_one_announcement_one =
            PreHashedTypeValue::new(peer_one_announcement_one.into(), 1);
        let peer_one_announcement_two =
            PreHashedTypeValue::new(peer_one_announcement_two.into(), 2);
        let peer_two_announcement_one =
            PreHashedTypeValue::new(peer_two_announcement_one.into(), 3);
        let peer_one_withdrawal =
            PreHashedTypeValue::new(peer_one_withdrawal.into(), 4);

        // When merged into a RibValue
        let settings = StoreEvictionPolicy::UpdateStatusOnWithdraw.into();
        let rib_value = RibValue::default();

        // Unique announcements accumulate in the RibValue
        let (rib_value, _user_data) = rib_value
            .clone_merge_update(
                &peer_one_announcement_one.into(),
                Some(&settings),
            )
            .unwrap();
        assert_eq!(rib_value.len(), 1);

        let (rib_value, _user_data) = rib_value
            .clone_merge_update(
                &peer_one_announcement_two.into(),
                Some(&settings),
            )
            .unwrap();
        assert_eq!(rib_value.len(), 2);

        let (rib_value, _user_data) = rib_value
            .clone_merge_update(
                &peer_two_announcement_one.into(),
                Some(&settings),
            )
            .unwrap();
        assert_eq!(rib_value.len(), 3);

        // And a withdrawal by one peer of the prefix which the RibValue represents leaves the RibValue size unchanged
        let (rib_value, _user_data) = rib_value
            .clone_merge_update(
                &peer_one_withdrawal.clone().into(),
                Some(&settings),
            )
            .unwrap();
        assert_eq!(rib_value.len(), 3);

        // And routes from the first peer which were withdrawn are marked as such
        let mut iter = rib_value.iter();
        let first = iter.next();
        assert!(first.is_some());
        let first_ty: &TypeValue = first.unwrap().deref();
        assert!(matches!(
            first_ty,
            TypeValue::Builtin(BuiltinTypeValue::Route(_))
        ));
        if let TypeValue::Builtin(BuiltinTypeValue::Route(route)) = first_ty {
            assert_eq!(route.peer_ip(), Some(peer_one.ip.unwrap()));
            assert_eq!(route.peer_asn(), Some(peer_one.asn.unwrap()));
            assert_eq!(route.status(), NlriStatus::Withdrawn);
        }

        let next = iter.next();
        assert!(next.is_some());
        let next_ty: &TypeValue = next.unwrap().deref();
        assert!(matches!(
            next_ty,
            TypeValue::Builtin(BuiltinTypeValue::Route(_))
        ));
        if let TypeValue::Builtin(BuiltinTypeValue::Route(route)) = next_ty {
            assert_eq!(route.peer_ip(), Some(peer_one.ip.unwrap()));
            assert_eq!(route.peer_asn(), Some(peer_one.asn.unwrap()));
            assert_eq!(route.status(), NlriStatus::Withdrawn);
        }

        // But the route from the second peer remains untouched
        let next = iter.next();
        assert!(next.is_some());
        let next_ty: &TypeValue = next.unwrap().deref();
        assert!(matches!(
            next_ty,
            TypeValue::Builtin(BuiltinTypeValue::Route(_))
        ));
        if let TypeValue::Builtin(BuiltinTypeValue::Route(route)) = next_ty {
            assert_eq!(route.peer_ip(), Some(peer_two.ip.unwrap()));
            assert_eq!(route.peer_asn(), Some(peer_two.asn.unwrap()));
            assert_eq!(route.status(), NlriStatus::InConvergence);
        }

        // And a withdrawal by one peer of the prefix which the RibValue represents, when using the removal eviction
        // policy, causes the two routes from that peer to be removed leaving only one in the RibValue.
        let settings = StoreEvictionPolicy::RemoveOnWithdraw.into();
        let (rib_value, _user_data) = rib_value
            .clone_merge_update(&peer_one_withdrawal.into(), Some(&settings))
            .unwrap();
        assert_eq!(rib_value.len(), 1);
    }

    #[test]
    fn test_route_comparison_using_default_hash_key_values() {
        let rib = HashedRib::default();
        let prefix = Prefix::new("127.0.0.1".parse().unwrap(), 32).unwrap();
        let peer_one = IpAddr::from_str("192.168.0.1").unwrap();
        let peer_two = IpAddr::from_str("192.168.0.2").unwrap();
        let announcement_one_from_peer_one =
            mk_route_announcement(prefix, "123,456", peer_one);
        let announcement_two_from_peer_one =
            mk_route_announcement(prefix, "789,456", peer_one);
        let announcement_one_from_peer_two =
            mk_route_announcement(prefix, "123,456", peer_two);
        let announcement_two_from_peer_two =
            mk_route_announcement(prefix, "789,456", peer_two);

        let hash_code_route_one_peer_one = rib.precompute_hash_code(
            &announcement_one_from_peer_one.clone().into(),
        );
        let hash_code_route_one_peer_one_again =
            rib.precompute_hash_code(&announcement_one_from_peer_one.into());
        let hash_code_route_one_peer_two =
            rib.precompute_hash_code(&announcement_one_from_peer_two.into());
        let hash_code_route_two_peer_one =
            rib.precompute_hash_code(&announcement_two_from_peer_one.into());
        let hash_code_route_two_peer_two =
            rib.precompute_hash_code(&announcement_two_from_peer_two.into());

        // Hashing sanity checks
        assert_ne!(hash_code_route_one_peer_one, 0);
        assert_eq!(
            hash_code_route_one_peer_one,
            hash_code_route_one_peer_one_again
        );

        assert_ne!(
            hash_code_route_one_peer_one, hash_code_route_one_peer_two,
            "Routes that differ only by peer IP should be considered different"
        );
        assert_ne!(
            hash_code_route_two_peer_one, hash_code_route_two_peer_two,
            "Routes that differ only by peer IP should be considered different"
        );
        assert_ne!(
            hash_code_route_one_peer_one, hash_code_route_two_peer_one,
            "Routes that differ only by AS path should be considered different"
        );
        assert_ne!(
            hash_code_route_one_peer_two, hash_code_route_two_peer_two,
            "Routes that differ only by AS path should be considered different"
        );

        // Sanity checks
        assert_eq!(
            hash_code_route_one_peer_one,
            hash_code_route_one_peer_one
        );
        assert_eq!(
            hash_code_route_one_peer_two,
            hash_code_route_one_peer_two
        );
        assert_eq!(
            hash_code_route_two_peer_one,
            hash_code_route_two_peer_one
        );
        assert_eq!(
            hash_code_route_two_peer_two,
            hash_code_route_two_peer_two
        );
    }

    #[test]
    fn test_merge_update_user_data_in_out() {
        const NUM_TEST_ITEMS: usize = 18;

        type TestMap<T> = hashbrown::HashSet<
            T,
            DefaultHashBuilder,
            TrackingAllocator<System>,
        >;

        #[derive(Debug)]
        struct MergeUpdateSettings {
            pub allocator: TrackingAllocator<System>,
            pub num_items_to_insert: usize,
        }

        impl MergeUpdateSettings {
            fn new(
                allocator: TrackingAllocator<System>,
                num_items_to_insert: usize,
            ) -> Self {
                Self {
                    allocator,
                    num_items_to_insert,
                }
            }
        }

        #[derive(Default)]
        struct TestMetaData(TestMap<usize>);

        // Create some settings
        let allocator = TrackingAllocator::default();
        let settings = MergeUpdateSettings::new(allocator, NUM_TEST_ITEMS);

        // Verify that it hasn't allocated anything yet
        assert_eq!(0, settings.allocator.stats().bytes_allocated);

        // Cause the allocator to be used by the merge update
        let meta = TestMetaData::default();
        let update_meta = TestMetaData::default();
        let (updated_meta, _user_data_out) = meta
            .clone_merge_update(&update_meta, Some(&settings))
            .unwrap();

        // Verify that the allocator was used
        assert!(settings.allocator.stats().bytes_allocated > 0);
        assert_eq!(NUM_TEST_ITEMS, updated_meta.0.len());

        // Drop the updated meta and check that no bytes are currently allocated
        drop(updated_meta);
        assert_eq!(0, settings.allocator.stats().bytes_allocated);
    }
    */

    // LH: which then obsoletes these as well

    /*
        fn mk_route_announcement<T: Into<PeerId>>(
            prefix: Prefix,
            as_path: &str,
            peer_id: T,
        ) -> PrefixRoute {
            let delta_id = (RotondaId(0), 0);
            let announcements = Announcements::from_str(&format!(
                "e [{as_path}] 10.0.0.1 BLACKHOLE,123:44 {}",
                prefix
            ))
            .unwrap();
            let bgp_update_bytes =
                mk_bgp_update(&Prefixes::default(), &announcements, &[]);

            // When it is processed by this unit
            let roto_update_msg =
                BgpUpdateMessage::new(bgp_update_bytes, SessionConfig::modern())
                .unwrap();
            let afi_safi = if prefix.is_v4() { AfiSafiType::Ipv4Unicast } else { AfiSafiType::Ipv6Unicast };
            // let bgp_update_msg =
            //     Arc::new(BgpUpdateMessage::new(delta_id, roto_update_msg));
            let mut route = PrefixRoute::new(
                delta_id,
                prefix,
                roto_update_msg,
                afi_safi,
                None,
                NlriStatus::InConvergence,
            );

            let peer_id = peer_id.into();

            if let Some(ip) = peer_id.ip {
                route = route.with_peer_ip(ip);
            }

            if let Some(asn) = peer_id.asn {
                route = route.with_peer_asn(asn);
            }

            route
        }

        fn mk_route_withdrawal(
            prefix: Prefix,
            peer_id: PeerId,
        ) -> MutableBasicRoute {
            let delta_id = (RotondaId(0), 0);
            let bgp_update_bytes = mk_bgp_update(
                &Prefixes::new(vec![prefix]),
                &Announcements::None,
                &[],
            );

            // When it is processed by this unit
            let roto_update_msg =
                BgpUpdateMessage::new(bgp_update_bytes, SessionConfig::modern()).unwrap();
            let afi_safi = if prefix.is_v4() { AfiSafiType::Ipv4Unicast } else { AfiSafiType::Ipv6Unicast };

            let mut route = BasicRoute::new(
                delta_id,
                prefix,
                roto_update_msg,
                afi_safi,
                None,
                NlriStatus::Withdrawn,
            );

            if let Some(ip) = peer_id.ip {
                route = route.with_peer_ip(ip);
            }

            if let Some(asn) = peer_id.asn {
                route = route.with_peer_asn(asn);
            }

            route
        }
    */
}

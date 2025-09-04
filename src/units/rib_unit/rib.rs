use std::{
    collections::{hash_set, HashMap},
    fmt,
    hash::{BuildHasher, Hasher},
    net::IpAddr,
    ops::Deref,
    sync::Arc,
};

use chrono::{Duration, Utc};
use hash_hasher::{HashBuildHasher, HashedSet};
use inetnum::{addr::Prefix, asn::Asn};
use log::{debug, error, trace};
use rotonda_store::{
    epoch,
    errors::{FatalResult, PrefixStoreError},
    match_options::{MatchOptions, QueryResult},
    prefix_record::{Meta, PrefixRecord, Record, RecordSet, RouteStatus},
    rib::{config::MemoryOnlyConfig, StarCastRib},
    stats::UpsertReport,
};
use routecore::bgp::{
    nlri::afisafi::{IsPrefix, Nlri},
    path_attributes::PaMap,
    path_selection::{OrdRoute, Rfc4271, TiebreakerInfo},
    types::AfiSafiType,
};
use serde::{ser::{SerializeSeq, SerializeStruct}, Serialize, Serializer};

use crate::{
    ingress::{self, IngressId, IngressInfo}, payload::{RotondaPaMap, RotondaRoute, RouterId}, representation::{OutputFormat, ToCli, ToJson}, roto_runtime::types::Provenance
};

use super::{http_ng::Include, QueryFilter};

// -------- PhysicalRib ------------------------------------------------------

// XXX is this actually used for something in the Store right now?
// impl Meta for RotondaRoute {
//     type Orderable<'a> = OrdRoute<'a, Rfc4271>;

//     type TBI = TiebreakerInfo;

//     fn as_orderable(&self, _tbi: Self::TBI) -> Self::Orderable<'_> {
//         todo!()
//     }
// }

type Store = StarCastRib<RotondaPaMap, MemoryOnlyConfig>;

#[derive(Clone)]
pub struct Rib {
    unicast: Arc<Option<Store>>,
    multicast: Arc<Option<Store>>,
    other_fams:
        HashMap<AfiSafiType, HashMap<(IngressId, Nlri<bytes::Bytes>), PaMap>>,
    ingress_register: Arc<ingress::Register>,
}

#[derive(Copy, Clone, Debug)]
struct Multicast(bool);

impl Rib {
    pub fn new_physical(ingress_register: Arc<ingress::Register>) -> Result<Self, PrefixStoreError> {
        Ok(Rib {
            unicast: Arc::new(Some(Store::try_default()?)),
            multicast: Arc::new(Some(Store::try_default()?)),
            other_fams: HashMap::new(),
            ingress_register,
        })
    }

    // unused
    //pub fn new_virtual() -> Self {
    //    Rib {
    //        unicast: Arc::new(None),
    //        multicast: Arc::new(None),
    //        other_fams: HashMap::new(),
    //    }
    //}

    // XXX LH perhaps this should become a characteristic of the Unit instead
    // of the Rib. Currently, rib_unit::unit::insert_payload() is the only
    // place that calls this is_physical() and uses it for an early return.
    // Instead, we could make it a bool flag on the Unit and get rid of the
    // Option wrapped stores in Rib itself?
    pub fn is_physical(&self) -> bool {
        self.unicast.is_some()
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
        provenance: Provenance,
        ltime: u64,
    ) -> Result<UpsertReport, String> {
        let res = match val {
            RotondaRoute::Ipv4Unicast(n, ..) => self.insert_prefix(
                &n.prefix(),
                Multicast(false),
                val,
                route_status,
                provenance,
                ltime,
            ),
            RotondaRoute::Ipv6Unicast(n, ..) => self.insert_prefix(
                &n.prefix(),
                Multicast(false),
                val,
                route_status,
                provenance,
                ltime,
            ),
            RotondaRoute::Ipv4Multicast(n, ..) => self.insert_prefix(
                &n.prefix(),
                Multicast(true),
                val,
                route_status,
                provenance,
                ltime,
            ),
            RotondaRoute::Ipv6Multicast(n, ..) => self.insert_prefix(
                &n.prefix(),
                Multicast(true),
                val,
                route_status,
                provenance,
                ltime,
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
        provenance: Provenance, // for ingress_id / mui
        ltime: u64,
    ) -> Result<UpsertReport, PrefixStoreError> {
        // Check whether our self.rib is Some(..) or bail out.
        let arc_store = match multicast.0 {
            true => self.multicast.clone(),
            false => self.unicast.clone(),
        };

        let store = (*arc_store)
            .as_ref()
            .ok_or(PrefixStoreError::StoreNotReadyError)?;

        let mui = provenance.ingress_id;

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
        
        let res = store.insert(
            prefix, pubrec, None, // Option<TBI>
        );

        //println!("store counters {}", store.prefixes_count());

        res
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
        //match_options: MatchOptions
        filter: QueryFilter,
    //) -> Result<QueryResult<RotondaPaMap>, String> {
    ) -> Result<SearchResult<RotondaPaMap>, String> {
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

        store
            .match_prefix(&nlri, match_options, guard)
            .map(|res| SearchResult { query_result: res, ingress_register: self.ingress_register.clone() } )
            .map_err(|err| err.to_string())

    }

    pub fn search_and_output_routes(
        &self,
        mut target: impl OutputFormat,
        afisafi: AfiSafiType,
        //nlri: Nlri<&[u8]>,
        nlri: Prefix, // change to Nlri or equivalent after routecore refactor
        //match_options: MatchOptions
        filter: QueryFilter,
    ) -> Result<(), String> {
        match self.search_routes(afisafi, nlri, filter) {
            Ok(search_results) => {
                let _ = target.write(search_results);
            },
            Err(e) => { return Err(format!("store error: {e}").into()); }
        }

        Ok(())
    }

    /// Query the store based on `IngressId`/MUI
    pub fn search_routes_for_ingress(
        afisafi: AfiSafiType,
        nlri: Nlri<&[u8]>,
        ingress_id: IngressId,
        match_options: MatchOptions
    ) -> Result<SearchResult<RotondaPaMap>, String> {
        todo!()
    }

    /// Query the store based on Origin AS in the AS_PATH
    pub fn search_routes_for_origin_as(
        afisafi: AfiSafiType,
        origin_as: Asn,
        match_options: MatchOptions
    ) -> Result<SearchResult<RotondaPaMap>, String> {
        todo!()
    }
}

/// Wrapper around `QueryResult` from rotonda-store
///
/// This wrapper is used to impl the necessary traits on, to enable consistent representation
/// between CLI, HTTP API, etc.
pub struct SearchResult<M: Meta> {
    query_result: QueryResult<M>,
    ingress_register: Arc<ingress::Register>,
}

impl<M: Meta + Serialize> Serialize for SearchResult<M> {
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
        struct IncludedData<'a, 'b, M: Meta + Serialize> {
            #[serde(skip_serializing_if = "Option::is_none")]
            more_specifics: Option<RecordSetWrapper<'a, 'b, M>>,
            #[serde(skip_serializing_if = "Option::is_none")]
            less_specifics: Option<RecordSetWrapper<'a, 'b, M>>,
        }

        let mut root = serializer.serialize_struct("nlri", 3)?;
        root.serialize_field("meta", &None::<String>)?;
        root.serialize_field("data", &Data {
            nlri: self.query_result.prefix,
            routes: RecordsWrapper(&self.query_result.records, &self.ingress_register),
        })?;

        root.serialize_field("included",
            &IncludedData {
                more_specifics: self.query_result.more_specifics.as_ref().map(|s| RecordSetWrapper(s, &self.ingress_register)),
                less_specifics: self.query_result.less_specifics.as_ref().map(|s| RecordSetWrapper(s, &self.ingress_register)),

            }
        )?;
        root.end()

    }
}

#[derive(Serialize)]
struct Data<'a, 'b, M: Meta + Serialize> {
    nlri: Option<Prefix>,
    routes: RecordsWrapper<'a, 'b, M>,
}

struct RecordsWrapper<'a, 'b, M>(&'a Vec<Record<M>>, &'b Arc<ingress::Register>);
struct RecordWrapper<'a, 'b, M>(&'a Record<M>, &'b Arc<ingress::Register>);
struct RecordSetWrapper<'a, 'b, M: Meta>(&'a RecordSet<M>, &'b Arc<ingress::Register>);
struct PrefixRecordWrapper<'a, 'b, M: Meta>(&'a PrefixRecord<M>, &'b Arc<ingress::Register>);
struct RouteStatusWrapper(RouteStatus);

impl<M: Meta + Serialize> Serialize for RecordsWrapper<'_, '_, M> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {

        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for e in self.0.iter() {
            seq.serialize_element(&RecordWrapper(e, self.1))?;
        }
        seq.end()

    }
}

impl<M: Meta + Serialize> Serialize for RecordWrapper<'_, '_, M> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {

        // The RPKI information is stored in the value (so, RotondaPaMap) in the store.
        // The RotondaPaMap serializes to { rpki: {}, pathAttributes: [] },
        // so with serde(flatten) the wrapped store::Record serializes to
        // { status: foo, rpki: bla, pathAttributes: buzz, etc .. }
        // on 'one level'.

        #[derive(Serialize)]
        struct Helper<'a, M: Meta + Serialize> {
            ingress_id: IngressId,
            ingress_info: IngressInfo,
            status: RouteStatusWrapper,
            #[serde(flatten)]
            pamap: &'a M
        }
        Helper {
            ingress_id: self.0.multi_uniq_id,
            ingress_info: self.1.get(self.0.multi_uniq_id).unwrap(),
            status: RouteStatusWrapper(self.0.status),
            pamap: &self.0.meta
        }.serialize(serializer)
    }
}

impl<M: Meta + Serialize> Serialize for RecordSetWrapper<'_, '_, M> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer {
            let mut s = serializer.serialize_seq(Some(self.0.len()))?;
            for e in &self.0.v4 {
               s.serialize_element(&PrefixRecordWrapper(&e, self.1))?;
            }
            for e in &self.0.v6 {
               s.serialize_element(&PrefixRecordWrapper(&e, self.1))?;
            }
       s.end()
    }
}

impl<M: Meta + Serialize> Serialize for PrefixRecordWrapper<'_, '_, M> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer {
            
             Data {
                nlri: Some(self.0.prefix),
                routes: RecordsWrapper(&self.0.meta, self.1),
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

impl<M: Meta + Serialize> ToJson for SearchResult<M> {
    fn to_json(&self, target: impl std::io::Write) ->  Result<(), crate::representation::OutputError> {
        serde_json::to_writer(target, &self).unwrap();
        Ok(())
    }
}


impl<M: Meta> ToCli for SearchResult<M> {
    fn to_cli(&self, target: &mut impl std::io::Write) ->  Result<(), crate::representation::OutputError> {
        let _ = writeln!(target,
            "TODO: ToCli for rib::SearchResult"
        );
        let _ = target.flush();
        Ok(())
    }
}



#[derive(Debug)]
pub enum StoreInsertionEffect {
    RoutesWithdrawn(usize),
    RoutesRemoved(usize),
    RouteAdded,
    RouteUpdated,
}

// XXX this will go, or will perhaps live in rotonda_store
#[derive(Debug)]
pub struct StoreInsertionReport {
    pub change: StoreInsertionEffect,

    /// The number of items stored at the prefix after the MergeUpdate operation.
    pub item_count: usize,

    /// The time taken to perform the MergeUpdate operation.
    pub op_duration: Duration,
}

//------------ StoredValue ---------------------------------------------------

#[derive(Debug, Clone)]
pub struct StoredValue {
    value: bytes::Bytes,
    hash: u64,
    disk_id: u64,
    i_time: u64,
}

// --- Route related helpers ------------------------------------------------------------------------------------------

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct PeerId {
    pub ip: Option<IpAddr>,
    pub asn: Option<Asn>,
}

impl PeerId {
    fn new(ip: Option<IpAddr>, asn: Option<Asn>) -> Self {
        Self { ip, asn }
    }
}

impl From<IpAddr> for PeerId {
    fn from(ip_addr: IpAddr) -> Self {
        PeerId::new(Some(ip_addr), None)
    }
}

pub trait RouteExtra {
    fn withdraw(&mut self);

    fn peer_id(&self) -> Option<PeerId>;

    fn router_id(&self) -> Option<Arc<RouterId>>;

    fn announced_by(&self, peer_id: &PeerId) -> bool;

    fn is_withdrawn(&self) -> bool;
}

// --- Tests ----------------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::{
        alloc::System, net::IpAddr, ops::Deref, str::FromStr, sync::Arc,
    };

    use hashbrown::hash_map::DefaultHashBuilder;
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

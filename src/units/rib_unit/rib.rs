use std::{
    collections::{hash_set, HashMap}, fmt, hash::{BuildHasher, Hasher}, net::IpAddr, ops::Deref, sync::Arc
};

use chrono::{Duration, Utc};
use hash_hasher::{HashBuildHasher, HashedSet};
use log::{debug, error};
//use roto::{types::{
//    builtin::{BasicRouteToken, BuiltinTypeValue, NlriStatus, PrefixRoute, Provenance, RotondaId, RouteContext},
//    datasources::Rib,
//    typedef::{RibTypeDef, TypeDef},
//    typevalue::TypeValue,
//}, vm::FieldIndex};
use rotonda_store::{
    custom_alloc::UpsertReport, epoch, prelude::{multi::{PrefixStoreError, RouteStatus}, PrefixRecord}, MatchOptions, MultiThreadedStore, QueryResult
};
use inetnum::{addr::Prefix, asn::Asn};
use routecore::bgp::{nlri::afisafi::{IsPrefix, Nlri}, path_attributes::PaMap, path_selection::{OrdRoute, Rfc4271, TiebreakerInfo}, types::AfiSafiType};
use serde::Serialize;

use crate::{common::roto_new::Provenance, ingress::IngressId, payload::{RotondaPaMap, RotondaRoute, RouterId}};

// -------- PhysicalRib -----------------------------------------------------------------------------------------------


// XXX is this actually used for something in the Store right now?
impl rotonda_store::Meta for RotondaRoute {
    type Orderable<'a> = OrdRoute<'a, Rfc4271>;

    type TBI = TiebreakerInfo;

    fn as_orderable(&self, _tbi: Self::TBI) -> Self::Orderable<'_> {
        todo!()
    }
}

pub struct Rib {
    unicast: Arc<Option<MultiThreadedStore<RotondaPaMap>>>,
    multicast: Arc<Option<MultiThreadedStore<RotondaPaMap>>>,
    other_fams: HashMap<AfiSafiType, HashMap<(IngressId, Nlri<bytes::Bytes>), PaMap>>,
}

#[derive(Copy, Clone, Debug)]
struct Multicast(bool);

impl Rib {
    pub fn new_physical() -> Self {
        Rib {
            // FIXME are these always safe to unwrap?
            unicast: Arc::new(Some(MultiThreadedStore::new().unwrap())),
            multicast: Arc::new(Some(MultiThreadedStore::new().unwrap())),
            other_fams: HashMap::new(),
        }
    }

    pub fn new_virtual() -> Self {
        Rib {
            unicast: Arc::new(None),
            multicast: Arc::new(None),
            other_fams: HashMap::new(),
        }
    }


    // XXX LH perhaps this should become a characteristic of the Unit instead
    // of the Rib. Currently, rib_unit::unit::insert_payload() is the only
    // place that calls this is_physical() and uses it for an early return.
    // Instead, we could make it a bool flag on the Unit and get rid of the
    // Option wrapped stores in Rib itself?
    pub fn is_physical(&self) -> bool {
        self.unicast.is_some()
    }

    // pub fn store() {} ? // still needed? probably going via fn get makes
    // more sense as we store multiple (all) afisafis now


    pub fn insert(&self,
        val: &RotondaRoute,
        route_status: RouteStatus,
        provenance: Provenance,
        ltime: u64
    ) -> Result<UpsertReport, String> {

        let res = match val {
            RotondaRoute::Ipv4Unicast(n, ..) => self.insert_prefix(&n.prefix(), Multicast(false), val, route_status, provenance, ltime),
            RotondaRoute::Ipv6Unicast(n, ..) => self.insert_prefix(&n.prefix(), Multicast(false), val, route_status, provenance, ltime),
            RotondaRoute::Ipv4Multicast(n, ..) => self.insert_prefix(&n.prefix(), Multicast(true), val, route_status, provenance, ltime),
            RotondaRoute::Ipv6Multicast(n, ..) => self.insert_prefix(&n.prefix(), Multicast(true), val, route_status, provenance, ltime),
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


        let store = (*arc_store).as_ref()
            .ok_or(PrefixStoreError::StoreNotReadyError)?;

        let mui = provenance.ingress_id;

        //debug!("pre store.insert, RouteStatus {:?}", route_status);

        if route_status == RouteStatus::Withdrawn {
            // instead of creating an empty PrefixRoute for this Prefix and
            // putting that in the store, we use the new
            // mark_mui_as_withdrawn_for_prefix . This way, we preserve the
            // last seen attributes/nexthop for this {prefix,mui} combination,
            // while setting the status to Withdrawn.
            store.mark_mui_as_withdrawn_for_prefix(prefix, mui)
                .inspect_err(|e| {
                    error!(
                        "failed to mark {} for {} as withdrawn: {}",
                        prefix, mui, e
                    );
                    // TODO increase metric
                })?;

            // FIXME this is just to satisfy the function signature, but is
            // quite useless as-is.
            return Ok(UpsertReport{
                cas_count: 0,
                prefix_new: false,
                mui_new: false, 
                mui_count: 0,
            });
        }

        let pubrec = rotonda_store::PublicRecord::new(
            mui,
            ltime,
            route_status,
            val.rotonda_pamap().clone()
        );

        store.insert(
            prefix,
            pubrec,
            None, // Option<TBI>
        )
    }

    pub fn withdraw_for_ingress(
        &self,
        ingress_id: IngressId,
        specific_afisafi: Option<AfiSafiType>
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

                //if let Err(e) = self.rib.load().store().unwrap().mark_mui_as_withdrawn_v4(ingress_id) {
                //    error!("failed to mark MUI as withdrawn for v4: {}", e)
                //}

                //if let Err(e) = self.rib.load().store().unwrap().mark_mui_as_withdrawn_v6(ingress_id) {
                //    error!("failed to mark MUI as withdrawn for v6: {}", e)
                //}

                if let Err(e) = (*self.unicast).as_ref().unwrap()
                    .mark_mui_as_withdrawn(ingress_id)
                {
                    error!("failed to mark MUI as withdrawn in unicast rib: {}", e)
                }

                if let Err(e) = (*self.multicast).as_ref().unwrap()
                    .mark_mui_as_withdrawn(ingress_id)
                {
                    error!("failed to mark MUI as withdrawn in multicast rib: {}", e)
                }


                // TODO withdraw all other afisafis as well!

            }
            Some(AfiSafiType::Ipv4Unicast) => {
                if let Err(e) = (*self.unicast).as_ref().unwrap()
                    .mark_mui_as_withdrawn_v4(ingress_id)
                {
                    error!("failed to mark MUI as withdrawn for v4: {}", e)
                }
            }
            Some(AfiSafiType::Ipv6Unicast) => {
                if let Err(e) = (*self.unicast).as_ref().unwrap()
                    .mark_mui_as_withdrawn_v6(ingress_id)
                {
                    error!("failed to mark MUI as withdrawn for v6: {}", e)
                }
            }
            Some(AfiSafiType::Ipv4Multicast) => {
                if let Err(e) = (*self.multicast).as_ref().unwrap()
                    .mark_mui_as_withdrawn_v4(ingress_id)
                {
                    error!("failed to mark MUI as withdrawn for v4: {}", e)
                }
            }
            Some(AfiSafiType::Ipv6Multicast) => {
                if let Err(e) = (*self.multicast).as_ref().unwrap()
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

    // TODO: figure out how we do match options like less/more specifics for
    // AfiSafiTypes where this either does not make sense, or, is non-trivial
    // to process.
    // Note that where .store() is used in the old code, we somehow need to
    // bend it to use this new .get().
    // Alternatively, we could create a .store::<AfiSafiType>() that returns
    // the specific store for that family, if any.
    pub fn get(&self) -> Option<()> { todo!() }


    pub fn match_prefix(
        &self,
        prefix: &Prefix,
        match_options: &MatchOptions,
    ) -> Result<QueryResult<RotondaPaMap>, String> {
        let guard = &epoch::pin();
        let store = (*self.unicast).as_ref()
            .ok_or(PrefixStoreError::StoreNotReadyError.to_string())?;
        let unicast_res = store.match_prefix(prefix, match_options, guard);
        if unicast_res.prefix_meta.is_empty()
            && unicast_res.less_specifics.is_none()
            && unicast_res.more_specifics.is_none()
        {
            debug!("no result in unicast store, trying multicast");
            let multicast_store = (*self.multicast).as_ref()
                .ok_or(PrefixStoreError::StoreNotReadyError.to_string())?;
            let multicast_res = 
                multicast_store.match_prefix(prefix, match_options, guard);
            if !(multicast_res.prefix_meta.is_empty()
                && multicast_res.less_specifics.is_none()
                && multicast_res.more_specifics.is_none()) {
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
        let store = (*self.unicast).as_ref()
            .ok_or(PrefixStoreError::StoreNotReadyError.to_string())?;
        let include_withdrawals = false;
        let mut res = store
            .iter_records_for_mui_v4(ingress_id, include_withdrawals, guard)
            .collect::<Vec<_>>();
        res.append(&mut store
            .iter_records_for_mui_v6(ingress_id, include_withdrawals, guard)
            .collect::<Vec<_>>()
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
}

impl Default for Rib {
    fn default() -> Self {
        Self::new_physical()
    }
}

/*
pub struct HashedRib {
    /// A prefix store, only physical RIBs have this.
    rib: Option<Rib<RibValue>>,

    //eviction_policy: StoreEvictionPolicy,

    // This TypeDef should only ever be of variant `TypeDef::Rib`
    type_def_rib: TypeDef,
}
*/

/*
impl Default for HashedRib {
    fn default() -> Self {
        // What is the key that uniquely identifies routes to be withdrawn when a BGP peering session is lost?
        //
        // A route is an AS path to follow from a given peer to reach a given prefix.
        // The prefix is not part of the values stored by a RIB as a RIB can be thought of as a mapping of prefix
        // keys to route values.
        //
        // The key that uniquely identifies a route is thus, excluding prefix for a moment, the peer ID and the
        // AS path to the prefix.
        //
        // A peer is uniquely identified by its BGP speaker IP address, but in case a BGP speaker at a given IP
        // address establishes multiple sessions to us, IP address would not be enough to distinguish routes
        // announced via one session vs those announced via another session. When one session goes down only its
        // routes should be withdrawn and not those of the other sessions and so we also distinguish a peer by the
        // ASN it represents. This allows for the scenario that a BGP speaker is configured for multiple ASNs, e.g.
        // as part of a migration from one ASN to another.
        //
        // TODO: Are there other values from the BGP OPEN message that we may need to consider as disinguishing one
        // peer from another?
        //
        // TODO: Add support for 'router group', for BMP the "id" of the monitored router from which peers are
        // learned of (either the "tcp ip address:tcp port" or the BMP Initiation message sysName TLV), or for BGP
        // a string representation of the connected peers "tcp ip address:tcp port".
        Self::new(
            //&[BasicRouteToken::PeerIp, BasicRouteToken::PeerAsn, BasicRouteToken::AsPath],
            &[BasicRouteToken::AsPath],
            true,
            //StoreEvictionPolicy::UpdateStatusOnWithdraw,
        )
    }
}
*/

/*
impl HashedRib {
    pub fn new(
        key_fields: &[BasicRouteToken], // XXX are these even used anywhere
                                        // currently?
        physical: bool,
        //eviction_policy: StoreEvictionPolicy,
    ) -> Self {
        let key_fields = key_fields
            .iter()
            .map(|&v| vec![v as usize].into())
            .collect::<Vec<_>>();
        Self::with_custom_type(TypeDef::PrefixRoute, key_fields, physical, /*eviction_policy*/)
    }

    /*
    // Attempt to construct a HashedRib comprising both a PrefixRoute (like
    // ::new() returns) and a RouteContext, packed in a TypeDef::Record.
    // Not sure whether this makes sense at all, but adapting the `impl
    // RouteExtra` from the old `Route` to the new `PrefixRoute` does not make
    // sense without a `RouteContext` somewhere.
    // XXX using a ::Record is not the way to go, that will store the entire
    // context for each value in the Rib.
    pub fn dont_do_this_prefix_and_context(
        key_fields: &[BasicRouteToken],
        physical: bool,
        settings: StoreMergeUpdateUserData,
    ) -> Self {
        let key_fields = key_fields
            .iter()
            .map(|&v| vec![v as usize].into())
            .collect::<Vec<_>>();
        let ty = TypeDef::Record(roto::types::typedef::RecordTypeDef::new(vec![
                ("prefixroute".into(), Box::new(TypeDef::PrefixRoute)),
                ("routecontext".into(), Box::new(TypeDef::RouteContext)),

        ]));
        Self::with_custom_type(ty, key_fields, physical, settings)
    }
    */

    pub fn with_custom_type(
        ty: TypeDef,
        ty_keys: Vec<FieldIndex>,
        physical: bool,
        //eviction_policy: StoreEvictionPolicy,
    ) -> Self {
        let rib = match physical {
            true => {
                let store = MultiThreadedStore::<RibValue>::new()
                    .unwrap() // TODO: handle this Err
                    // setting the user_data here doesn't make sense as we
                    //.with_user_data(eviction_policy);
                    ;
                let rib =
                    Rib::new("rib-names-are-not-used-yet", ty.clone(), store);
                Some(rib)
            }
            false => None,
        };
        let rib_type_def: RibTypeDef = (Box::new(ty), Some(ty_keys));
        let type_def_rib = TypeDef::Rib(rib_type_def);
        Self { rib, /*eviction_policy,*/ type_def_rib }
    }

    pub fn is_physical(&self) -> bool {
        self.rib.is_some()
    }

    /*
    pub fn precompute_hash_code(&self, val: &TypeValue) -> u64 {
        let mut state = HashBuildHasher::default().build_hasher();
        self.type_def_rib.hash_key_values(&mut state, val).unwrap();
        state.finish()
    }
    */

    pub fn store(
        &self,
    ) -> Result<&MultiThreadedStore<RibValue>, PrefixStoreError> {
        self.rib
            .as_ref()
            .map(|rib| &rib.store)
            .ok_or(PrefixStoreError::StoreNotReadyError)
    }

    //pub fn insert<T: Into<TypeValue>>(
    pub fn insert(
        &self,
        prefix: &Prefix,
        //val: T,
        val: PrefixRoute,
        nlri_status: NlriStatus,
        provenance: Provenance, // for ingress_id / mui
        ltime: u64,
    //) -> Result<(Upsert<StoreInsertionReport>, u32), PrefixStoreError> {
    ) -> Result<UpsertReport, PrefixStoreError> {
        let store = self.store()?;
        //let ty_val = val.into();
        //let hash_code = self.precompute_hash_code(&ty_val);

        let mui = provenance.ingress_id;

        //debug!("pre store.insert, NlriStatus {:?}", nlri_status);

        let route_status = match nlri_status {
            NlriStatus::Withdrawn => RouteStatus::Withdrawn,
            NlriStatus::InConvergence => RouteStatus::Active,
            NlriStatus::UpToDate => RouteStatus::Active,

            // XXX what do we do with these?
            NlriStatus::Stale => todo!(),
            NlriStatus::StartOfRouteRefresh => todo!(),
            NlriStatus::Unparsable => todo!(),
            NlriStatus::Empty => todo!(),
        };

        //debug!("pre store.insert, RouteStatus {:?}", route_status);

        if route_status == RouteStatus::Withdrawn {
            // instead of creating an empty PrefixRoute for this Prefix and
            // putting that in the store, we use the new
            // mark_mui_as_withdrawn_for_prefix . This way, we preserve the
            // last seen attributes/nexthop for this {prefix,mui} combination,
            // while setting the status to Withdrawn.
            if let Err(e) = store.mark_mui_as_withdrawn_for_prefix(prefix, mui) {
                error!(
                    "failed to mark {} for {} as withdrawn: {}",
                    prefix, mui, e
                );
                // TODO increase metric
                return Err(e);
            }
            
            // FIXME this is just to satisfy the function signature, but is
            // quite useless as-is.
            return Ok(UpsertReport{
                cas_count: 0,
                prefix_new: false,
                mui_new: false, 
                mui_count: 0,
            });
        }

        debug!("creating pub rec with RibValue {:?}", val);

        let pubrec = rotonda_store::PublicRecord::new(
            mui,
            ltime,
            route_status,
            RibValue::new(val) // meta M
        );

        debug!("calling store.insert(..) for {:?}", &prefix);
        // XXX new API for the PrefixStore insert:
        store.insert(
            prefix,
            pubrec,
            None, // Option<TBI>
        )
    }

    pub fn key_fields(&self) -> Vec<BasicRouteToken> {
        let TypeDef::Rib((_td, Some(field_indices))) = &self.type_def_rib
        else {
            unreachable!();
        };
        field_indices
            .iter()
            .map(|idx| BasicRouteToken::try_from(idx.first().unwrap()).unwrap())
            .collect()
    }
}
*/

/*
// -------- RibValue --------------------------------------------------------------------------------------------------

/// The metadata value associated with a prefix in the store of a physical RIB.
///
/// # Design
///
/// The metadata value consists of an outer Arc over a HashedSet over Arc<PreHashedTypeValue> items.
///
/// Points to note about this design:
///
/// 1. The outer Arc is used to prevent costly deep copying of the HashSet when `Store::match_prefix()` clones the
/// metadata value of matching prefixes into its `prefix_meta`, `less_specifics` and `more_specifics` fields.
///
/// 2. The inner Arc is used to prevent costly deep copying of the HashSet items. To use RibValue as the metadata value
/// type of a MultiThreadedStore it must implement the MergeUpdate trait and thus must implement `clone_merge_update()`
/// but the `PreHashedTypeValue` inner `TypeValue` is not cheap to clone. However, items in the HashSet which need not
/// be changed by the `MultiThreadedStore::insert()` operation (that invoked `clone_merge_update()`) need not be
/// deeply copied, we only need to "modify" zero or more items in the HashSet that are affected by the update, where
/// "affected" is type dependent. Note that the HashSet itself should not be modified via interior mutability in such a
/// way that the prior metadata value is also modified by the `clone_merge_update()` call. Rather than deep copy every
/// item stored in the HashSet just to possibly modify some of them, we can insteasd use an Arc around the HashSet items
/// so that cloning the HashSet doesn't unnecessarily deep clone the items. For items that do have to be modified we
/// will have to clone the value inside the Arc around the HashSet item, but for the rest we can just clone the Arc.
///
/// 3. A HashedSet is used instead of a HashSet because HashedSet is a handy way to construct a HashSet with a no-op
/// hash function. We use this because the key of the items that we store will in future be determined by roto script
/// and not hard-coded in Rust types. We therefore precompute a hash code value and store it with the actual metadata
/// value and the Hash trait impl passes the precomputed hash code to the HashedSet hasher which uses it effectively
/// as-is, to avoid pointlessly calculating yet another hash code as would happen with the default Hasher.

//#[derive(Debug, Clone, Default)]
//pub struct RibValue {
//    per_prefix_items: Arc<HashedSet<Arc<PreHashedTypeValue>>>,
//}
#[derive(Debug, Clone)]
pub struct RibValue {
    prefix_route: PrefixRoute,
}

use routecore::bgp::{nlri::afisafi::{AfiSafiNlri, Nlri}, path_attributes::PaMap, path_selection::{OrdRoute, Rfc4271, TiebreakerInfo}, types::AfiSafiType};
impl rotonda_store::Meta for RibValue {
    type Orderable<'a> = OrdRoute<'a, Rfc4271>;

    type TBI = TiebreakerInfo;

    fn as_orderable(&self, _tbi: Self::TBI) -> Self::Orderable<'_> {
        todo!()
    }
}

//impl PartialEq for RibValue {
//    fn eq(&self, other: &Self) -> bool {
//        self.per_prefix_items == other.per_prefix_items
//    }
//}

impl RibValue {
    pub fn new(prefix_route: PrefixRoute) -> Self {
        Self {
            prefix_route
        }
    }

    /*
    pub fn iter(&self) -> hash_set::Iter<'_, Arc<PreHashedTypeValue>> {
        self.per_prefix_items.iter()
    }
    */
}

impl TryFrom<TypeValue> for RibValue {
    type Error = &'static str;

    fn try_from(tv: TypeValue) -> Result<Self, Self::Error> {
        match tv {
            TypeValue::Builtin(BuiltinTypeValue::PrefixRoute(pr)) => { Ok(RibValue::new(pr)) },
            _ => Err("expected TypeValue PrefixRoute")
        }
    }
}

impl From<RibValue> for TypeValue {
    fn from(rv: RibValue) -> Self {
        TypeValue::Builtin(BuiltinTypeValue::PrefixRoute(rv.prefix_route))
    }
}

*/

/*
#[cfg(test)]
impl RibValue {
    /*
    pub fn test_inner(&self) -> &Arc<HashedSet<Arc<PreHashedTypeValue>>> {
        &self.per_prefix_items
    }
    */
}
*/

/*
#[derive(Copy, Clone, Debug, Default)]
pub enum StoreEvictionPolicy {
    #[default]
    UpdateStatusOnWithdraw,

    RemoveOnWithdraw,
}
*/

/*
impl RibValue {
    // LH: I guess this is sort of a noop now that RibValue is basically a
    // PrefixRoute
    /*
    pub fn withdraw(
        &self,
        policy: StoreEvictionPolicy,
        //withdrawing_peer: &PeerId,
        withdrawing_ingress: IngressId,
    ) -> (HashedSet<Arc<PreHashedTypeValue>>, StoreInsertionEffect) {
        let mut out_items: HashedSet<Arc<PreHashedTypeValue>>;
        let effect: StoreInsertionEffect;

        match policy {
            StoreEvictionPolicy::UpdateStatusOnWithdraw => {
                let mut num_withdrawals = 0;

                out_items = self
                    .iter()
                    .map(|route| {
                        // XXX LH: is this better than the RouteExtra trait?
                        if let TypeValue::Builtin(BuiltinTypeValue::PrefixRoute(roto::types::builtin::PrefixRoute(rws, nlri_status))) = **route.deref() {
                            // If the route was not issued by this peer then
                            // keep it as-is by including a clone of its Arc
                            // in the result collection.
                            //if route.is_withdrawn()
                            //    || !route.announced_by(withdrawing_peer) {
                            if nlri_status == NlriStatus::Withdrawn 
                                || !route.provenance().ingress_id != withdrawing_ingress {
                                Arc::clone(route)
                            } else {
                                // Otherwise, return a clone of the Arc's
                                // inner route having first set its status to
                                // withdrawn.
                                //let mut cloned = Arc::deref(route).clone();
                                //cloned.withdraw();
                                num_withdrawals += 1;
                                //Arc::new(cloned)
                                Arc::new(PreHashedTypeValue::new(
                                    roto::types::builtin::PrefixRoute(rws, NlriStatus::Withdrawn).into(),
                                    route.provenance(),
                                ))
                            }
                        } else {
                            Arc::clone(route) // not a PrefixRoute, keep it
                        }
                    })
                    .collect();

                effect =
                    StoreInsertionEffect::RoutesWithdrawn(num_withdrawals);
            }

            StoreEvictionPolicy::RemoveOnWithdraw => {
                out_items = self
                    .iter()
                    //.filter(|route| !route.announced_by(withdrawing_peer))
                    .filter(|route| route.provenance().ingress_id != withdrawing_ingress)
                    .cloned()
                    .collect();

                let num_removals = self.len() - out_items.len();
                effect = StoreInsertionEffect::RoutesRemoved(num_removals);
            }
        }

        out_items.shrink_to_fit();

        (out_items, effect)
    }
    */
}
*/

/*
#[derive(Clone, Debug)]
pub struct StoreMergeUpdateUserData {
    //pub eviction_policy: StoreEvictionPolicy,
    pub route_context: RouteContext,

    #[cfg(test)]
    pub delay: Option<std::time::Duration>,
}

impl StoreMergeUpdateUserData {
    pub fn new(
        //eviction_policy: StoreEvictionPolicy,
        route_context: RouteContext,
    ) -> Self {
        Self {
            //eviction_policy,
            route_context,
            #[cfg(test)]
            delay: None,
        }
    }
}
*/

/*
impl From<StoreEvictionPolicy> for StoreMergeUpdateUserData {
    fn from(eviction_policy: StoreEvictionPolicy) -> Self {
        StoreMergeUpdateUserData::new(eviction_policy)
    }
}
*/

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

// XXX this will go and live in rotonda_store
/*
impl MergeUpdate for RibValue {
    type UserDataIn = StoreMergeUpdateUserData;
    type UserDataOut = StoreInsertionReport;

    fn merge_update(
        &mut self,
        _update_record: RibValue,
        _user_data: Option<&Self::UserDataIn>,
    ) -> Result<StoreInsertionReport, Box<dyn std::error::Error>> {
        unreachable!()
    }

    // NOTE: Do NOT return Err() as this will likely be changed in the
    // underlying rotonda-store definition to be infallible.
    // LH: As the PrefixStore will take over the actual merging of values of
    // type PrefixRoute, this method (and eventually, this entire trait impl)
    // is merely a noop while the impl has not moved to the Store yet.
    fn clone_merge_update(
        &self,
        update_meta: &Self,
        user_data: Option<&StoreMergeUpdateUserData>,
    ) -> Result<(Self, StoreInsertionReport), Box<dyn std::error::Error>>
    where
        Self: std::marker::Sized,
    {
        let pre_insert = Utc::now();

        #[cfg(test)]
        if let Some(StoreMergeUpdateUserData {
            delay: Some(delay), ..
        }) = user_data
        {
            eprintln!(
                "Sleeping in clone_merge_update() [thread {:?}] for {}ms",
                std::thread::current().id(),
                delay.as_millis()
            );
            std::thread::sleep(*delay);
        }

        // There should only ever be one incoming item.
        // LH: with RibValue being a wrapper for a single PrefixRoute, this
        // assert does not make sense anymore.
        //assert_eq!(update_meta.len(), 1);

        //let in_item = update_meta.per_prefix_items.iter().next().unwrap();
        let in_item = update_meta.prefix_route;

        // Create a new RIB value whose inner HashSet contains the same items
        // as this RIB value, but for which the HashSet itself is distinct, so
        // that we can add/modify/remove values in the output HashSet. Only go
        // to the effort of creating a clone of the HashSet if the given input
        // value actually requires us to make a change in the value being
        // updated, i.e. don't dumbly clone and modify as the clone might not
        // be necessary.

        //let mut out_items: HashedSet<Arc<PreHashedTypeValue>>;
        //let change;

        let to_withdraw = if let Some(ud) = user_data {
            ud.route_context.nlri_status() == NlriStatus::Withdrawn
        } else {
            // Try to determine whether this is a withdrawal or not, even
            // though we lack the RouteContext.
            
            // TODO create a metric for this situation
            log::warn!("no user_data provided for MergeUpdate, missing RouteContext");
            in_item.0.no_attributes()
        };

        /*
        if to_withdraw {
            // Only routes can be withdrawn, other kinds of of items stored in
            // a RIB don't support the notion of being withdrawable. A route
            // withdrawal is defined as the prefix to which routing is no
            // longer possible via a given peer. This RIB item represents
            // routes to the prefix via various peers. To apply the withdrawal
            // we must therefore update/remove the routes to the prefix from
            // the peer that issued the withdrawal.
            //let withdrawing_peer = in_item.peer_id().unwrap();
            let withdrawing_ingress = in_item.provenance().ingress_id;

            // Apply the withdrawal, either by updating the status of
            // affected routes, or by removing them entirely.
            let eviction_policy =
                user_data.map(|v| v.eviction_policy).unwrap_or_default();

            (out_items, change) =
                self.withdraw(eviction_policy, withdrawing_ingress);
        } else {
            // Merge the new items into the existing set, replacing any
            // existing item that has the same RIB key as a new item.
            out_items = self.per_prefix_items.deref().clone();
            if out_items.replace(in_item.clone()).is_some() {
                change = StoreInsertionEffect::RouteUpdated;
            } else {
                change = StoreInsertionEffect::RouteAdded;
            }
        }
        */

        let post_insert = Utc::now();
        let op_duration = post_insert - pre_insert;
        let report = StoreInsertionReport {
            item_count: 1,
            //change,
            // XXX as the PrefixStore will handle the actual merge eventually,
            // we don't know whether this value was new (::RouteAdded) or
            // updated (::RouteUpdated).
            change: StoreInsertionEffect::RouteUpdated,
            op_duration,
        };

        Ok((update_meta.clone(), report))
    }
}
*/

/*
impl std::fmt::Display for RibValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.prefix_route)
    }
}

impl std::ops::Deref for RibValue {
    type Target = PrefixRoute;

    fn deref(&self) -> &Self::Target {
        &self.prefix_route
    }
}
*/

/*
impl From<PreHashedTypeValue> for RibValue {
    fn from(item: PreHashedTypeValue) -> Self {
        let mut items = HashedSet::with_capacity_and_hasher(
            1,
            HashBuildHasher::default(),
        );
        items.insert(Arc::new(item));
        Self {
            per_prefix_items: Arc::new(items),
        }
    }
}

impl From<HashedSet<Arc<PreHashedTypeValue>>> for RibValue {
    fn from(value: HashedSet<Arc<PreHashedTypeValue>>) -> Self {
        Self {
            per_prefix_items: Arc::new(value),
        }
    }
}
*/

//------------ StoredValue ---------------------------------------------------

#[derive(Debug, Clone)]
pub struct StoredValue {
    value: bytes::Bytes,
    hash: u64,
    disk_id: u64,
    i_time: u64
}

// -------- PreHashedTypeValue ----------------------------------------------------------------------------------------

/*
// this can go entirely now?
#[derive(Debug, Clone, Serialize)]
pub struct PreHashedTypeValue {
    /// The route to store.
    #[serde(flatten)]
    value: TypeValue,

    provenance: Provenance,
    
    //#[serde(skip)]
    ///// The hash key as pre-computed based on the users chosen hash key fields.
    //precomputed_hash: u64,
}

// this can go entirely now?
impl PreHashedTypeValue {
    pub fn new(value: TypeValue, provenance: Provenance/*, precomputed_hash: u64*/) -> Self {
        Self {
            value,
            provenance,
            //precomputed_hash: provenance.ingress_id
        }
    }

    pub fn provenance(&self) -> Provenance {
        self.provenance
    }
}

impl std::ops::Deref for PreHashedTypeValue {
    type Target = TypeValue;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl std::ops::DerefMut for PreHashedTypeValue {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

impl std::hash::Hash for PreHashedTypeValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // The Hasher is hash_hasher::HashHasher which:
        //     "does minimal work to create the required u64 output under the assumption that the input is already a
        //      hash digest or otherwise already suitable for use as a key in a HashSet or HashMap."
        //self.precomputed_hash.hash(state);
        self.provenance.ingress_id.hash(state);
    }
}

impl PartialEq for PreHashedTypeValue {
    fn eq(&self, other: &Self) -> bool {
        self.provenance.ingress_id == other.provenance.ingress_id
    }
}

impl Eq for PreHashedTypeValue {}
*/

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

/*
impl RouteExtra for TypeValue {
    fn withdraw(&mut self) {
        // this could work assuming the RibValue comprises a
        // RouteContext+PrefixRoute or something alike.
        if let TypeValue::Builtin(BuiltinTypeValue::RouteContext(ctx)) = self {
            ctx.update_nlri_status(NlriStatus::Withdrawn);
        }
    }

    fn peer_id(&self) -> Option<PeerId> {
        match self {
            // unsure about this double layer of Option<_> here, is it
            // necessary? Note that the Roto PeerId (from RouteContext) does
            // not wrap the ip/asn in Options, it's the Rotonda PeerId that
            // does that.
            TypeValue::Builtin(BuiltinTypeValue::RouteContext(ctx)) => {
                Some(PeerId::new(
                        Some(ctx.provenance().peer_ip()),
                        Some(ctx.provenance().peer_asn()),
                ))
            }
            _ => None,
        }
    }

    // What do we want here, remove in favour of a new SourceId kind of thing?
    fn router_id(&self) -> Option<Arc<RouterId>> {
        match self {
            TypeValue::Builtin(BuiltinTypeValue::RouteContext(ctx)) => {
                Some(Arc::from(ctx.provenance().connection_id.to_string()))
            }
            _ => None,
        }
    }

    fn announced_by(&self, peer_id: &PeerId) -> bool {
        self.peer_id().as_ref() == Some(peer_id)
    }

    fn is_withdrawn(&self) -> bool {
        if let TypeValue::Builtin(BuiltinTypeValue::RouteContext(ctx)) = self {
            ctx.nlri_status() == NlriStatus::Withdrawn
        } else {
            false
        }
    }
}
*/

// --- Tests ----------------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::{
        alloc::System, net::IpAddr, ops::Deref, str::FromStr, sync::Arc,
    };

    use hashbrown::hash_map::DefaultHashBuilder;
    use roto::types::{
        lazyrecord_types::BgpUpdateMessage,
        builtin::{
            PrefixRoute, BuiltinTypeValue, NlriStatus, RotondaId
        },
        typevalue::TypeValue,
    };
    use inetnum::{addr::Prefix, asn::Asn};
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

use std::{
    collections::hash_set,
    hash::{BuildHasher, Hasher},
    ops::Deref,
    sync::Arc,
};

use hash_hasher::{HashBuildHasher, HashedSet};
use roto::types::{
    builtin::{BuiltinTypeValue, RotondaId, RouteStatus, RouteToken},
    datasources::Rib,
    typedef::{RibTypeDef, TypeDef},
    typevalue::TypeValue,
};
use rotonda_store::{prelude::MergeUpdate, MultiThreadedStore};
use serde::Serialize;
use smallvec::SmallVec;

// -------- PhysicalRib -----------------------------------------------------------------------------------------------

pub struct PhysicalRib {
    rib: Rib<RibValue>,

    // This TypeDef should only ever be of variant `TypeDef::Rib`
    type_def_rib: TypeDef,
}

impl std::ops::Deref for PhysicalRib {
    type Target = MultiThreadedStore<RibValue>;

    fn deref(&self) -> &Self::Target {
        &self.rib.store
    }
}

impl PhysicalRib {
    pub fn new(name: &str) -> Self {
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
        let peer_ip_key = vec![RouteToken::PeerIp as usize].into();
        let peer_asn_key = vec![RouteToken::PeerAsn as usize].into();
        let as_path_key = vec![RouteToken::AsPath as usize].into();
        let route_keys = vec![peer_ip_key, peer_asn_key, as_path_key];

        Self::with_custom_type(name, TypeDef::Route, route_keys)
    }

    pub fn with_custom_type(name: &str, ty: TypeDef, ty_keys: Vec<SmallVec<[usize; 8]>>) -> Self {
        let store = MultiThreadedStore::<RibValue>::new().unwrap(); // TODO: handle this Err;
        let rib = Rib::new(name, ty.clone(), store);
        let rib_type_def: RibTypeDef = (Box::new(ty), Some(ty_keys));
        let type_def_rib = TypeDef::Rib(rib_type_def);

        Self { rib, type_def_rib }
    }

    pub fn precompute_hash_code(&self, val: &TypeValue) -> u64 {
        let mut state = HashBuildHasher::default().build_hasher();
        self.type_def_rib.hash_key_values(&mut state, val).unwrap();
        state.finish()
    }
}

// -------- RibValue --------------------------------------------------------------------------------------------------

//// The metadata value associated with a prefix in the store of a physical RIB.
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
/// 2. The inner Arc is used to prevent costly deep copying of the HashSet items. The type `PreHashedTypeValue`
/// contains an inner `RawRouteWithDeltas` field which is not cheap to clone because it holds two `Vec` instances: one
/// in the `AttributeDeltaList` type and one in the ``RouteStatusDeltaList` type. To use RibValue as the metadata value
/// type of a MultiThreadedStore it must implement the MergeUpdate trait and thus must implement the
/// `clone_merge_update()` function, which as the name implies is required to clone itself. Items in the HashSet which
/// need not be changed by the `MultiThreadedStore::insert()` operation (that invoked `clone_merge_update()`) need not be
/// deeply copied, but the HashSet itself should not be modified via interior mutability in such a way that the prior
/// metadata value is also modified by the `clone_merge_update()` call, so we must not clone the outer Arc. We don't
/// however need to deep copy every item stored in the HashSet, only those which we want to modify, so we use an Arc
/// around the HashSet items to avoid paying a penalty for deep copying the aforementioned Vecs.
///
/// 3. A HashedSet is used instead of a HashSet because HashedSet is a handy way to construct a HashSet with a no-op
/// hash function. We use this because the key of the items that we store will in future be determined by roto script
/// and not hard-coded in Rust types. We therefore precompute a hash code value and store it with the actual metadata
/// value and the Hash trait impl passes the precomputed hash code to the HashedSet hasher which uses it effectively
/// as-is, to avoid pointlessly calculating yet another hash code as would happen with the default Hasher.
///
/// Note: the actual inner metadata value type stored is expected to change in future from RawRouteWithDeltas to
/// a record type defined by roto script. This type could be a super set of what we have now, e.g. it could record
/// details about where we learned a route from (e.g. BGP peer IP address and ASN and/or BMP monitored router ID), or
/// it could be a subset of what we have now, e.g. rather than an entire BGP UPDATE it might just be a few key fields
/// in order to avoid the memory cost of storing a bunch of route details that are never going to be used.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RibValue {
    per_prefix_items: Arc<HashedSet<Arc<PreHashedTypeValue>>>,
}

impl RibValue {
    pub fn new(items: HashedSet<Arc<PreHashedTypeValue>>) -> Self {
        Self {
            per_prefix_items: Arc::new(items),
        }
    }

    pub fn iter(&self) -> hash_set::Iter<'_, Arc<PreHashedTypeValue>> {
        self.per_prefix_items.iter()
    }
}

#[cfg(test)]
impl RibValue {
    pub fn inner(&self) -> &Arc<HashedSet<Arc<PreHashedTypeValue>>> {
        &self.per_prefix_items
    }
}

impl MergeUpdate for RibValue {
    fn merge_update(&mut self, _update_record: RibValue) -> Result<(), Box<dyn std::error::Error>> {
        unreachable!()
    }

    fn clone_merge_update(&self, update_meta: &Self) -> Result<Self, Box<dyn std::error::Error>>
    where
        Self: std::marker::Sized,
    {
        let new_per_prefix_item: &PreHashedTypeValue =
            update_meta.per_prefix_items.iter().next().unwrap();
        let new_per_prefix_potential_route: &TypeValue = new_per_prefix_item;
        let new_per_prefix_items = match new_per_prefix_potential_route {
            TypeValue::Builtin(BuiltinTypeValue::Route(new_route))
                if new_route.status() == RouteStatus::Withdrawn =>
            {
                // For routes, if it is an announce we replace or insert an existing route with the same hash key,
                // just like for any other type of RIB item. For a withdrawal however, we don't replace it but instead
                // update it with RouteStatus::Withdrawn. If we were to replace the entire route we would lose
                // information about it because withdrawal BGP UPDATE messages only state the prefix to withdraw a route
                // to, they don't say which path that route follows.

                // We're withdrawing a route. We can't locate the route to withdraw by its usual hash key as that is
                // partly based on the AS path which is not part of a withdrawal, for a withdrawal we only know the
                // originating peer and the prefix to which routes are being withdrawn.
                //
                // So: for each route to this prefix:
                self.per_prefix_items
                    .iter()
                    .map(|v| {
                        // See if it was originated by the same peer as the withdrawal we received:
                        let stored_phtv_ref = Arc::deref(v);
                        let to_update_ty_ref = stored_phtv_ref.deref();
                        match to_update_ty_ref {
                            TypeValue::Builtin(BuiltinTypeValue::Route(to_update_route))
                                if to_update_route.peer_ip() == new_route.peer_ip()
                                    && to_update_route.peer_asn() == new_route.peer_asn() =>
                            {
                                // It was, so clone the value behind the Arc and mark it as withdrawn (which requires
                                // mutable access to the route so can't be done via the Arc).
                                let delta_id = (RotondaId(0), 0); // TODO
                                let mut cloned_route = to_update_route.clone();
                                cloned_route.update_status(delta_id, RouteStatus::Withdrawn);
                                let cloned_tv: TypeValue = cloned_route.into();
                                let cloned_phtv = PreHashedTypeValue::new(
                                    cloned_tv,
                                    stored_phtv_ref.precomputed_hash,
                                ); // TODO: surely cloning the precomputed hash after modifying the value is a bad idea?
                                Arc::new(cloned_phtv)
                            }

                            _ => v.clone(), // clone Arc
                        }
                    })
                    .collect::<HashedSet<Arc<PreHashedTypeValue>>>()
                    .into() // into Arc
            }

            _ => {
                // For all other cases, just use the Hash impl to replace an existing item or to insert a new item.
                self.per_prefix_items
                    .union(&update_meta.per_prefix_items)
                    .cloned()
                    .collect::<HashedSet<Arc<PreHashedTypeValue>>>()
                    .into() // into Arc
            }
        };

        Ok(Self {
            per_prefix_items: new_per_prefix_items,
        })
    }
}

impl std::fmt::Display for RibValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.per_prefix_items)
    }
}

impl std::ops::Deref for RibValue {
    type Target = HashedSet<Arc<PreHashedTypeValue>>;

    fn deref(&self) -> &Self::Target {
        &self.per_prefix_items
    }
}

impl From<PreHashedTypeValue> for RibValue {
    fn from(item: PreHashedTypeValue) -> Self {
        let mut items = HashedSet::with_capacity_and_hasher(1, HashBuildHasher::default());
        items.insert(Arc::new(item));
        Self {
            per_prefix_items: Arc::new(items),
        }
    }
}

// -------- PreHashedTypeValue ----------------------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct PreHashedTypeValue {
    /// The route to store.
    #[serde(flatten)]
    value: TypeValue,

    #[serde(skip)]
    /// The hash key as pre-computed based on the users chosen hash key fields.
    precomputed_hash: u64,
}

impl PreHashedTypeValue {
    pub fn new(value: TypeValue, precomputed_hash: u64) -> Self {
        Self {
            value,
            precomputed_hash,
        }
    }
}

impl std::ops::Deref for PreHashedTypeValue {
    type Target = TypeValue;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl std::hash::Hash for PreHashedTypeValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // The Hasher is hash_hasher::HashHasher which:
        //     "does minimal work to create the required u64 output under the assumption that the input is already a
        //      hash digest or otherwise already suitable for use as a key in a HashSet or HashMap."
        self.precomputed_hash.hash(state);
    }
}

impl PartialEq for PreHashedTypeValue {
    fn eq(&self, other: &Self) -> bool {
        self.precomputed_hash == other.precomputed_hash
    }
}

impl Eq for PreHashedTypeValue {}

// --- Tests ----------------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::{net::IpAddr, ops::Deref, str::FromStr, sync::Arc};

    use roto::types::{
        builtin::{BuiltinTypeValue, RawRouteWithDeltas, RotondaId, RouteStatus, UpdateMessage},
        typevalue::TypeValue,
    };
    use rotonda_store::prelude::MergeUpdate;
    use routecore::{addr::Prefix, bgp::message::SessionConfig};

    use crate::bgp::encode::{mk_bgp_update, Announcements, Prefixes};

    use super::{PreHashedTypeValue, RibValue};

    #[test]
    fn empty_by_default() {
        let rib_value = RibValue::default();
        assert!(rib_value.is_empty());
    }

    #[test]
    fn into_new() {
        let rib_value: RibValue = PreHashedTypeValue::new(123u8.into(), 18).into();
        assert_eq!(rib_value.len(), 1);
        assert_eq!(
            rib_value.iter().next(),
            Some(&Arc::new(PreHashedTypeValue::new(123u8.into(), 18)))
        );
    }

    #[test]
    fn merging_in_separate_values_yields_two_entries() {
        let rib_value = RibValue::default();
        let value_one = PreHashedTypeValue::new(1u8.into(), 1);
        let value_two = PreHashedTypeValue::new(2u8.into(), 2);

        let rib_value = rib_value.clone_merge_update(&value_one.into()).unwrap();
        assert_eq!(rib_value.len(), 1);

        let rib_value = rib_value.clone_merge_update(&value_two.into()).unwrap();
        assert_eq!(rib_value.len(), 2);
    }

    #[test]
    fn merging_in_the_same_precomputed_hashcode_yields_one_entry() {
        let rib_value = RibValue::default();
        let value_one = PreHashedTypeValue::new(1u8.into(), 1);
        let value_two = PreHashedTypeValue::new(2u8.into(), 1);

        let rib_value = rib_value.clone_merge_update(&value_one.into()).unwrap();
        assert_eq!(rib_value.len(), 1);

        let rib_value = rib_value.clone_merge_update(&value_two.into()).unwrap();
        assert_eq!(rib_value.len(), 1);
    }

    #[test]
    fn merging_in_a_withdrawal_updates_matching_entries() {
        let rib_value = RibValue::default();

        let prefix = Prefix::new("127.0.0.1".parse().unwrap(), 32).unwrap();
        let peer_one = IpAddr::from_str("192.168.0.1").unwrap();
        let peer_two = IpAddr::from_str("192.168.0.2").unwrap();
        let announcement_one = mk_route_announcement(prefix, "123,456").with_peer_ip(peer_one);
        let announcement_two = mk_route_announcement(prefix, "789,456").with_peer_ip(peer_one);
        let announcement_three = mk_route_announcement(prefix, "123,456").with_peer_ip(peer_two);
        let withdrawal = mk_route_withdrawal(prefix).with_peer_ip(peer_one);

        let announcement_one = PreHashedTypeValue::new(announcement_one.into(), 1);
        let announcement_two = PreHashedTypeValue::new(announcement_two.into(), 2);
        let announcement_three = PreHashedTypeValue::new(announcement_three.into(), 3);
        let withdrawal = PreHashedTypeValue::new(withdrawal.into(), 1);

        let rib_value = rib_value
            .clone_merge_update(&announcement_one.into())
            .unwrap();
        assert_eq!(rib_value.len(), 1);

        let rib_value = rib_value
            .clone_merge_update(&announcement_two.into())
            .unwrap();
        assert_eq!(rib_value.len(), 2);

        let rib_value = rib_value
            .clone_merge_update(&announcement_three.into())
            .unwrap();
        assert_eq!(rib_value.len(), 3);

        let rib_value = rib_value.clone_merge_update(&withdrawal.into()).unwrap();
        assert_eq!(rib_value.len(), 3);

        let mut iter = rib_value.iter();
        let first = iter.next();
        assert!(first.is_some());
        let first_ty: &TypeValue = first.unwrap().deref();
        assert!(matches!(
            first_ty,
            TypeValue::Builtin(BuiltinTypeValue::Route(_))
        ));
        if let TypeValue::Builtin(BuiltinTypeValue::Route(route)) = first_ty {
            assert_eq!(route.peer_ip(), Some(peer_one));
            assert_eq!(route.status(), RouteStatus::Withdrawn);
        }

        let next = iter.next();
        assert!(next.is_some());
        let next_ty: &TypeValue = next.unwrap().deref();
        assert!(matches!(
            next_ty,
            TypeValue::Builtin(BuiltinTypeValue::Route(_))
        ));
        if let TypeValue::Builtin(BuiltinTypeValue::Route(route)) = next_ty {
            assert_eq!(route.peer_ip(), Some(peer_one));
            assert_eq!(route.status(), RouteStatus::Withdrawn);
        }

        let next = iter.next();
        assert!(next.is_some());
        let next_ty: &TypeValue = next.unwrap().deref();
        assert!(matches!(
            next_ty,
            TypeValue::Builtin(BuiltinTypeValue::Route(_))
        ));
        if let TypeValue::Builtin(BuiltinTypeValue::Route(route)) = next_ty {
            assert_eq!(route.peer_ip(), Some(peer_two));
            assert_eq!(route.status(), RouteStatus::InConvergence);
        }
    }

    fn mk_route_announcement(prefix: Prefix, as_path: &str) -> RawRouteWithDeltas {
        let delta_id = (RotondaId(0), 0);
        let announcements = Announcements::from_str(&format!(
            "e [{as_path}] 10.0.0.1 BLACKHOLE,123:44 {}",
            prefix
        ))
        .unwrap();
        let bgp_update_bytes = mk_bgp_update(&Prefixes::default(), &announcements, &[]);

        // When it is processed by this unit
        let bgp_update_msg = UpdateMessage::new(bgp_update_bytes, SessionConfig::modern());
        RawRouteWithDeltas::new_with_message(
            delta_id,
            prefix.into(),
            bgp_update_msg,
            RouteStatus::InConvergence,
        )
    }

    fn mk_route_withdrawal(prefix: Prefix) -> RawRouteWithDeltas {
        let delta_id = (RotondaId(0), 0);
        let bgp_update_bytes =
            mk_bgp_update(&Prefixes::new(vec![prefix]), &Announcements::None, &[]);

        // When it is processed by this unit
        let bgp_update_msg = UpdateMessage::new(bgp_update_bytes, SessionConfig::modern());
        RawRouteWithDeltas::new_with_message(
            delta_id,
            prefix.into(),
            bgp_update_msg,
            RouteStatus::Withdrawn,
        )
    }
}

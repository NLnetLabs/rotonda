use std::{collections::hash_set, sync::Arc};

use hash_hasher::{HashBuildHasher, HashedSet};
use roto::types::builtin::RawRouteWithDeltas;
use rotonda_store::prelude::MergeUpdate;
use serde::Serialize;

// -------- RibValue---------------------------------------------------------------------------------------------------

//// The metadata value associated with a prefix in the store of a physical RIB.
///
/// # Design
///
/// The metadata value consists of an outer Arc over a HashedSet over Arc<RouteWithUserDefinedHash> items.
///
/// Points to note about this design:
///
/// 1. The outer Arc is used to prevent costly deep copying of the HashSet when `Store::match_prefix()` clones the
/// metadata value of matching prefixes into its `prefix_meta`, `less_specifics` and `more_specifics` fields.
///
/// 2. The inner Arc is used to prevent costly deep copying of the HashSet items. The type `RouteWithUserDefinedHash`
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
    routes: Arc<HashedSet<Arc<RouteWithUserDefinedHash>>>,
}

impl RibValue {
    pub fn new(routes: HashedSet<Arc<RouteWithUserDefinedHash>>) -> Self {
        Self {
            routes: Arc::new(routes),
        }
    }

    pub fn iter(&self) -> hash_set::Iter<'_, Arc<RouteWithUserDefinedHash>> {
        self.routes.iter()
    }
}

#[cfg(test)]
impl RibValue {
    pub fn inner(&self) -> &Arc<HashedSet<Arc<RouteWithUserDefinedHash>>> {
        &self.routes
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
        let routes = self
            .routes
            .union(&update_meta.routes)
            .cloned()
            .collect::<HashedSet<Arc<RouteWithUserDefinedHash>>>()
            .into(); // into Arc

        Ok(Self { routes })
    }
}

impl std::fmt::Display for RibValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.routes)
    }
}

impl std::ops::Deref for RibValue {
    type Target = HashedSet<Arc<RouteWithUserDefinedHash>>;

    fn deref(&self) -> &Self::Target {
        &self.routes
    }
}

impl From<RouteWithUserDefinedHash> for RibValue {
    fn from(route: RouteWithUserDefinedHash) -> Self {
        let mut routes = HashedSet::with_capacity_and_hasher(1, HashBuildHasher::default());
        routes.insert(Arc::new(route));
        Self {
            routes: Arc::new(routes),
        }
    }
}

// -------- RouteWithUserDefinedHash ----------------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RouteWithUserDefinedHash {
    /// The route to store.
    #[serde(flatten)]
    route: RawRouteWithDeltas, // TODO: This should be changed to TypeValue: Question: Do we know how to impl MergeUpdate then? Based on the users keys I guess...

    #[serde(skip)]
    /// The hash key as pre-computed based on the users chosen hash key fields.
    precomputed_hash: u64,
}

impl RouteWithUserDefinedHash {
    pub fn new(route: RawRouteWithDeltas, precomputed_hash: u64) -> Self {
        Self {
            route,
            precomputed_hash,
        }
    }
}

impl std::ops::Deref for RouteWithUserDefinedHash {
    type Target = RawRouteWithDeltas;

    fn deref(&self) -> &Self::Target {
        &self.route
    }
}

impl std::hash::Hash for RouteWithUserDefinedHash {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // The Hasher is hash_hasher::HashHasher which:
        //     "does minimal work to create the required u64 output under the assumption that the input is already a
        //      hash digest or otherwise already suitable for use as a key in a HashSet or HashMap."
        self.precomputed_hash.hash(state);
    }
}

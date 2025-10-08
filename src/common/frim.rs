//! Types for frequent reads and infrequent writes.
use std::{ops::Deref, sync::Arc};

use arc_swap::ArcSwap;
use smallvec::SmallVec;

//------------ FrimMap -------------------------------------------------------

/// Frequently read infrequently modified mini map.
///
/// A FrimMap (Frequently Read Infrequently Modified) is a concurrent map-like
/// data structure intended for cases where frequent concurrent reads occur
/// but only infrequently are updates made, and is aimed at use cases where
/// only a few entries in the map are needed (hence "mini").
///
/// Offers fast reads at the expensive of (comparatively) costly (speed,
/// memory) updates.
///
/// The caller must assume that making modifications to the map contents may
/// result in every key and value being cloned, so keys or values that are
/// costly to clone should be hidden behind a cheap to clone wrapper such as
/// Arc.
#[derive(Debug)]
pub struct FrimMap<K, V>
where
    K: Clone + PartialEq,
    V: Clone,
{
    // Implementation notes:
    //
    // Originally a DashMap was used in many of the cases where a FrimMap is
    // now used, but in cases where there was little modification and mostly
    // reading having locking based concurrent access to ensure the data could
    // be both read and written to concurrently makes no sense when an
    // occasional write can be instead supported by a copy-on-write approach
    // instead. Additionally there were concerns that observed freezes might
    // be caused by DashMap, though this was not thoroughly investigated nor
    // proven.
    //
    // ArcSwap is used as it offers "better performance characteristics than
    // the RwLock both in contended and non-contended scenarios", and "read
    // operations are always lock-free" and "Most of the time, they are
    // actually wait-free" and because this data type is intended for cases
    // where reads vastly outnumber writes.
    //
    // SmallVec is used because for small sizes it is just an extremely fast
    // stack based array, while still supporting larger sizes via the heap,
    // and because searching a vec even without sorting is quite fast. A
    // version using binary search was tested but was not actually faster, and
    // added the complexity of ensuring the data remained sorted after
    // modification.
    inner: ArcSwap<SmallVec<[(K, V); 8]>>,
}

impl<K, V> Default for FrimMap<K, V>
where
    K: Clone + PartialEq,
    V: Clone,
{
    /// Creates an empty FrimMap.
    fn default() -> Self {
        Self {
            inner: ArcSwap::new(Arc::new(SmallVec::<[(K, V); 8]>::new())),
        }
    }
}

//------------ Guard --------------------------------------------------------

/// RAII guard required while iterating over a [FrimMap].
///
/// The Guard guards access to the FrimMap concurrent data structure internals
/// for operations such as iteration which require a reference to be held
/// across multiple accesses to the data structure.
pub struct IterGuard<K, V>
where
    K: Clone + PartialEq,
    V: Clone,
{
    #[allow(clippy::type_complexity)]
    guard: arc_swap::Guard<Arc<SmallVec<[(K, V); 8]>>>,
}

impl<K, V> IterGuard<K, V>
where
    K: Clone + PartialEq,
    V: Clone,
{
    /// An iterator visiting all key-value pairs in arbitrary order. The
    /// iterator element type is `&'a (K, V)`.
    pub fn iter(&self) -> Iter<'_, K, V> {
        Iter {
            iter: self.guard.iter(),
        }
    }
}

//------------ Iter ----------------------------------------------------------

/// An iterator over the entries of a [FrimMap].
///
/// This struct is created by the iter method on Guard.
pub struct Iter<'a, K, V> {
    iter: std::slice::Iter<'a, (K, V)>,
}

impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = &'a (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

//------------ Entry ---------------------------------------------------------

/// A view into a single entry in a map, which may either be vacant or
/// occupied.
///
/// This enum is constructed from the entry method on FrimMap.
pub enum Entry<'a, K, V>
where
    K: Clone + PartialEq,
    V: Clone,
{
    Occupied(V),
    Vacant(&'a FrimMap<K, V>, K),
}

impl<'a, K, V> Entry<'a, K, V>
where
    K: Clone + PartialEq,
    V: Clone,
{
    /// Ensures a value is in the entry by inserting the result of the default
    /// function if empty, and returns a mutable reference to the value in the
    /// entry.
    pub fn or_insert_with<F>(self, default: F) -> V
    where
        F: FnOnce() -> V,
    {
        match self {
            Entry::Occupied(v) => v,
            Entry::Vacant(map, key) => {
                let v = default();
                map.insert(key, v.clone());
                v
            }
        }
    }
}

//------------ FrimMap -------------------------------------------------------

impl<K, V> FrimMap<K, V>
where
    K: Clone + PartialEq,
    V: Clone,
{
    /// Returns the number of elements in the FrimMap.
    pub fn len(&self) -> usize {
        self.inner.load().len()
    }

    /// Returns true if the FrimMap is empty.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.inner.load().is_empty()
    }

    /// Get an iteration guard.
    ///
    /// Once the guard has been obtained, call [Guard::iter()] to get an
    /// iterator over the FrimMap.
    pub fn guard(&self) -> IterGuard<K, V> {
        IterGuard {
            guard: self.inner.load(),
        }
    }

    #[allow(dead_code)]
    pub fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    pub fn get(&self, key: &K) -> Option<V> {
        self.inner
            .load()
            .iter()
            .find(|(k, _v)| k == key)
            .map(|(_k, v)| v.clone())
    }

    pub fn entry(&self, key: K) -> Entry<'_, K, V> {
        match self.get(&key) {
            Some(v) => Entry::Occupied(v),
            None => Entry::Vacant(self, key),
        }
    }

    /// Inserts a key-value pair into the FrimMap.
    ///
    /// If the FrimMap already had this key the previous entry will be
    /// replaced by the newly inserted entry.
    ///
    /// Copies all the key-value pairs into a new FrimMap plus the new
    /// key-value pair and sets this Frim to the new content, dropping the
    /// reference to the former content.
    ///
    /// *Note:* This differs to the behaviour of HashMap::insert() which
    /// preserves any existing key.
    pub fn insert(&self, key: K, value: V) {
        self.inner.rcu(|inner| {
            inner
                .iter()
                .filter(|(k, _v)| k != &key)
                .chain(&[(key.clone(), value.clone())])
                .cloned()
                .collect::<SmallVec<_>>()
        });
    }

    /// Retains only the elements specified by the predicate.
    ///
    /// In other words, remove all pairs (k, v) for which f(&k, &v) returns
    /// false. The elements are visited in unsorted (and unspecified) order.
    pub fn retain<F>(&self, mut f: F)
    where
        F: FnMut(&K, &V) -> bool,
    {
        self.inner.rcu(|inner| {
            inner
                .iter()
                .filter(|(k, v)| f(k, v))
                .cloned()
                .collect::<SmallVec<_>>()
        });
    }

    /// Replaces the content of this FrimMap with the given Frim.
    pub fn replace(&self, new: Self) {
        self.inner.store(new.inner.into_inner())
    }

    /// Removes a key from the FrimMap, returning the value at the key if the
    /// key was previously in the FrimMap.
    pub fn remove(&self, key: &K) -> Option<V> {
        let mut found = None;
        self.inner.rcu(|inner| {
            let mut new = inner.deref().clone();
            if let Some(pos) = inner.iter().position(|(k, _v)| k == key) {
                let (_, v) = new.remove(pos);
                found = Some(v);
            }
            new
        });
        found
    }
}

//------------ Tests ---------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frim_map_tests() {
        let map = FrimMap::<u8, u8>::default();
        assert_eq!(map.len(), 0);

        assert_eq!(map.guard().iter().next(), None);

        map.insert(1, 1);
        assert_eq!(map.len(), 1);

        map.insert(1, 2);
        assert_eq!(map.len(), 1);

        map.insert(3, 4);
        assert_eq!(map.len(), 2);

        map.insert(5, 6);
        assert_eq!(map.len(), 3);

        let guard = map.guard();
        let mut iter = guard.iter();
        assert_eq!(iter.next(), Some(&(1, 2)));
        assert_eq!(iter.next(), Some(&(3, 4)));
        assert_eq!(iter.next(), Some(&(5, 6)));
        assert_eq!(iter.next(), None);

        assert_eq!(map.remove(&1), Some(2));
        assert_eq!(map.len(), 2);

        assert_eq!(map.remove(&1), None);
        assert_eq!(map.len(), 2);

        map.retain(|&k, &v| k == 3 && v == 4);
        assert_eq!(map.len(), 1);

        let map2 = FrimMap::<u8, u8>::default();
        map2.insert(1, 1);
        map2.insert(2, 2);
        map.replace(map2);
        assert_eq!(map.len(), 2);
        let guard = map.guard();
        let mut iter = guard.iter();
        assert_eq!(iter.next(), Some(&(1, 1)));
        assert_eq!(iter.next(), Some(&(2, 2)));
        assert_eq!(iter.next(), None);
    }
}

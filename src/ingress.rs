use core::sync::atomic::AtomicU32;
use std::net::IpAddr;
use std::{collections::HashMap, path::PathBuf, sync::atomic::Ordering};

use std::sync::RwLock;

use inetnum::asn::Asn;
use routecore::bmp::message::{PeerType, RibType};
use paste::paste;

/// Register of ingress/sources, tracked in a serial way.
///
/// Sources are BGP sessions (so multiple per BGP connector Unit), or BMP
/// sessions (a single one per BMP connector unit), or something else.
///
/// The ingress/connector units need to register their connections with the
/// Register, providing additional information such as `SocketAddr`s or
/// string-like names. Any unit/target can query the Register.
#[derive(Debug, Default)]
pub struct Register {
    serial: AtomicU32,
    info: RwLock<HashMap<IngressId, IngressInfo>>,
}

pub type IngressId = u32;

// Used to merge IngressInfo structs, called in info_for_field!
macro_rules! update_field {
    ($old:ident, $new:ident, $field:ident) => {
        if $new.$field.is_some() {
            $old.$field = $new.$field;
        }
    }
}

// Used to create the builder pattern methods, called in info_for_field!
macro_rules! with_field {
    ($name:ident, $type:ty) => {
        paste! {
            pub fn [<with_$name>](self, $name: impl Into<$type>) -> Self {
                Self {
                    $name: Some($name.into()), ..self }
            }
        }
    }
}

// Creates the IngressInfo (== $name) struct and adds fn Register::update_info
macro_rules! info_for_field{
    ($name:ident { $($field:ident : $type:ty),* } ) => {

        /// Information pertaining to an [`IngressId`]
        ///
        /// The `IngressInfo` struct is quite broad and generic in nature, featuring
        /// fields that might not make sense in all use cases. Therefore, everything
        /// is wrapped as an `Option`, giving the user (mostly connector/ingress
        /// components within Rotonda) the flexibilty to fill in what makes sense in
        /// their specific case.
        #[derive(Clone, Debug, Default)]
        #[serde_with::skip_serializing_none]
        #[derive(serde::Serialize)]
        pub struct $name {
            $(
                pub $field: Option<$type>,
            )*

            // pub last_active: Instant ? to enable 'reconnecting' remotes?
        }

        impl $name {
            $(
                with_field!($field, $type);
            )*

        }

        impl Register {
            /// Change the info related to an [`IngressId`]
            pub fn update_info(
                &self,
                id: IngressId,
                new_info: $name,
            ) -> Option<$name> {
                let mut lock = self.info.write().unwrap();

                log::debug!("update_info for {id} with {new_info:?}");

                if let Some(mut old) = lock.remove(&id) {

                    $(
                        update_field!(old, new_info, $field);
                    )*
                    lock.insert(id, old) // returns the replaced info
                } else {
                    lock.insert(id, new_info) // returns the replaced info
                }
            }

        }
    }
}

// Creates a boolean expression from mandatory and optional fields in [`IngressInfo`]
//
// Usage:
//
//   find_existing_for!(info, query, {mandatory_fields*}, {optional_fields*}?)
//
// Note that the set of optional_fields is optional itself.
// For all fields in mandatory_fields, we check whether the field is not None
// in the `info` we have stored, then compare it to that field in the `query`.
// For fields in the optional_fields, the check for not None is skipped, the
// the field on `info` is compared to the one on `query` regardless.
macro_rules! find_existing_for {
    (
        $info:ident, $query:ident,
        { $($mandatory_field:ident),*} $(,)?
        $({ $($optional_field:ident),*})?
    ) => {
        $(
            $info.$mandatory_field.is_some() &&
            $info.$mandatory_field == $query.$mandatory_field &&
        )*
        $(
            $(
                $info.$optional_field == $query.$optional_field &&
            )*
        )?
    true // just to fill up the last '&& _' from the repetition
    }

}


impl Register {
    /// Create a new register
    pub(crate) fn new() -> Self {
        Self {
            serial: 1.into(),
            info: RwLock::new(HashMap::new()),
        }
    }

    pub fn overview(&self) -> String {
        let lock = self.info.read().unwrap();
        let mut res = String::new();
        for (id, info) in lock.iter() {
            res.push_str(&format!(
                "{id:02} {}\t\t{}\n",
                info.remote_asn.map(|a|a.to_string()).unwrap_or("".to_string()),
                info.remote_addr.map(|a|a.to_string()).unwrap_or("".to_string()),
            ));
        }
        res
    }

    /// Request a new, unique [`IngressId`]
    pub(crate) fn register(&self) -> IngressId {
        self.serial.fetch_add(1, Ordering::Relaxed)
    }

    /// Retrieve the information for the given [`IngressId`]
    pub fn get(&self, id: IngressId) -> Option<IngressInfo> {
        self.info.read().unwrap().get(&id).cloned()
    }

    /// Find all [`IngressId`]s that are children of the given `parent`
    ///
    /// This is used in cases where for example a BMP session (the parent) is
    /// terminated, and all route information that was learned for that
    /// session (in one or multiple monitored BGP sessions, the children) need
    /// to be withdrawn.
    pub fn ids_for_parent(&self, parent: IngressId) -> Vec<IngressId> {
        let mut res = Vec::new();
        for (id, info) in self.info.read().unwrap().iter() {
            if info.parent_ingress == Some(parent) {
                res.push(*id);
            }
        }
        res
    }

    // find_existing methods:
    // cases to cover:
    //  * match MRT update messages (to ingresses from bdumps):
    //    * from bdump, we have:
    //      * filename, remote addr, remote_asn
    //
    //  * match reconnecting BMP session:
    //      * previous BMP session had ingress_id + remote_addr
    //      * child BGP sessions had their own ingress_id + parent_id + remote
    //      addr + remote asn
    //  * match BGP flap:
    //     * previous had ingress_id + fake name + remote addr + remote asn
    //
    //
    //  do we need to register the unit name (e.g. bmp-in, mrt-in) as well?

    /// Search existing [`IngressId`] on the peer level
    ///
    /// For the peer level, the comparison is based on the parent
    /// `ingress_id`, the remote address and ASN (all three must be set).
    /// Furthermore, the RIB type is compared, though that might be unset,
    /// i.e. None.
    pub fn find_existing_peer(
        &self,
        query: &IngressInfo
    ) -> Option<(IngressId, IngressInfo)> {
        let lock = self.info.read().unwrap();
        for (id, info) in lock.iter() {
            if find_existing_for!(info, query,
                {parent_ingress, remote_addr, remote_asn},
                {rib_type, peer_type, distinguisher}
            ) {
                    //log::debug!("found existing peer, id {id}");
                    return Some((*id, info.clone()))
            }
        }
        None
    }

    /// Search existing [`IngressId`] on the BMP router leven
    ///
    /// For the BMP router level, the comparison is based on the parent
    /// `ingress_id` and the remote address (both must be set).
    pub fn find_existing_bmp_router(
        &self,
        query: &IngressInfo
    ) -> Option<(IngressId, IngressInfo)> {
        let lock = self.info.read().unwrap();
        log::debug!("query: {query:?}");
        for (id, info) in lock.iter() {
            if find_existing_for!(info, query,
                {parent_ingress, remote_addr}
            ) {
                    log::debug!("found matching bmp router, id {id}");
                    return Some((*id, info.clone()))
            }
        }
        log::debug!("no match in find_existing_bmp_router");
        None
    }
}

impl IngressInfo {
    pub fn new() -> Self {
        Self::default()
    }
}

// This constructs the [`IngressInfo`] struct, used as values in the Register.
info_for_field!(IngressInfo{
   unit_name: String,
   parent_ingress: IngressId,
   remote_addr: IpAddr,
   remote_asn: Asn,
   rib_type: RibType,
   filename: PathBuf,
   name: String,
   desc: String,
   local_asn: Asn,
   peer_type: PeerType,
   distinguisher: [u8; 8]
});

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn all_fields() {
        let info = IngressInfo::new()
            .with_local_asn(Asn::from_u32(1234));
        assert_eq!(info.local_asn, Some(Asn::from_u32(1234)));
    }

    #[test]
    fn non_mandatory() {
        todo!()
    }
}

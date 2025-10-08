use core::sync::atomic::AtomicU32;
use std::net::IpAddr;
use std::{collections::HashMap, path::PathBuf, sync::atomic::Ordering};

use std::sync::RwLock;
use std::fmt;
use inetnum::asn::Asn;
use routecore::bmp::message::{PeerType, RibType};
use paste::paste;

use crate::genoutput_json;
use crate::ingress::http_ng::QueryFilter;
use crate::representation::{Cli, GenOutput, Json, OutputError};
use crate::roto_runtime::types::PeerRibType;

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
#[derive(serde::Serialize)]
pub struct IdAndInfo<'a> {
    #[serde(rename = "id")]
    pub ingress_id: IngressId,
    #[serde(flatten)]
    pub ingress_info: &'a IngressInfo,
}

#[derive(serde::Serialize)]
pub struct OwnedIdAndInfo {
    #[serde(rename = "id")]
    pub ingress_id: IngressId,
    #[serde(flatten)]
    pub ingress_info: IngressInfo,
}

impl<'a> From<(IngressId, &'a IngressInfo)> for IdAndInfo<'a> {
    fn from(value: (IngressId, &'a IngressInfo)) -> Self {
        IdAndInfo {
            ingress_id: value.0,
            ingress_info: value.1
        }
    }
}


impl From<(IngressId, IngressInfo)> for OwnedIdAndInfo {
    fn from(value: (IngressId, IngressInfo)) -> Self {
        OwnedIdAndInfo {
            ingress_id: value.0,
            ingress_info: value.1
        }
    }
}

pub struct BmpIdAndInfo<'a>(pub IdAndInfo<'a>);
pub struct BgpIdAndInfo<'a>(pub IdAndInfo<'a>);

impl GenOutput<&mut crate::webui::Index> for BmpIdAndInfo<'_> {
    fn write(&self, target: &mut &mut crate::webui::Index) -> Result<(), OutputError> {
        target.bmp_routers.push((self.0.ingress_id, self.0.ingress_info.clone()));
        Ok(())
    }
}

impl<W: std::io::Write> GenOutput<Cli<W>> for BgpIdAndInfo<'_> {
    fn write(&self, target: &mut Cli<W>) -> Result<(), OutputError> {

        let _ = writeln!(&mut target.0,
            "{:>}\t{:>10}\t{:>40}",
            self.0.ingress_id,
            self.0.ingress_info.remote_asn.map(|asn| asn.to_string()).unwrap_or("no-asn???".into()),
            self.0.ingress_info.remote_addr.map(|ip| ip.to_string()).unwrap_or("no-ip".into()),
        );
        let _ = target.0.flush();
        Ok(())
    }
}

impl<W: std::io::Write> GenOutput<Cli<W>> for BmpIdAndInfo<'_> {
    fn write(&self, target: &mut Cli<W>) -> Result<(), OutputError> {

        let _ = writeln!(&mut target.0,
            "{:>}\t{:>40}\t{}",
            self.0.ingress_id,
            self.0.ingress_info.remote_addr.map(|ip| ip.to_string()).unwrap_or("no-ip".into()),
            self.0.ingress_info.name.as_ref().unwrap_or(&"no-name".into())
        );
        let _ = target.0.flush();
        Ok(())
    }
}

genoutput_json!(IdAndInfo<'_>);
genoutput_json!(BmpIdAndInfo<'_>, 0);
genoutput_json!(BgpIdAndInfo<'_>, 0);

pub struct TestMe {
    field: Asn,
}
genoutput_json!(TestMe, field);

// Used to merge IngressInfo structs, called via info_for_field! in update_info
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
        #[derive(Clone, Debug, Default, PartialEq)]
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

                //log::debug!("update_info for {id} with {new_info:?}");

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


    pub fn bgp_neighbors<T>(&self, mut target: T) -> fmt::Result
        where for <'a> BgpIdAndInfo<'a>: GenOutput<T>
    {
        for t in [IngressType::BgpViaBmp, IngressType::Bgp, IngressType::Mrt] {
            let res = self.search(QueryFilter { ingress_type: Some(t), ..Default::default() });
            for r in res {
                let _ = BgpIdAndInfo (
                        IdAndInfo { ingress_id: r.ingress_id, ingress_info: &r.ingress_info}
                    ).write(&mut target);

            }
        }
        Ok(())
    }

    pub fn bmp_routers<T>(&self, mut target: T) -> fmt::Result
        where for <'a> BmpIdAndInfo<'a>: GenOutput<T>
    {
        let res = self.search(QueryFilter { ingress_type: Some(IngressType::Bmp), ..Default::default() });
        for r in res {

            let _ = BmpIdAndInfo (
                    IdAndInfo { ingress_id: r.ingress_id, ingress_info: &r.ingress_info}
                ).write(&mut target);

        }
        Ok(())
    }

    /// Request a new, unique [`IngressId`]
    pub(crate) fn register(&self) -> IngressId {
        self.serial.fetch_add(1, Ordering::Relaxed)
    }

    /// Retrieve the information for the given [`IngressId`]
    pub fn get(&self, id: IngressId) -> Option<IngressInfo> {
        self.info.read().unwrap().get(&id).cloned()
    }

    /// Retrieve the information for the given [`IngressId`]
    pub fn get_tuple(&self, id: IngressId) -> Option<OwnedIdAndInfo> {
        self.info.read().unwrap().get(&id).map(|info|
            (id, info.clone()).into()
        )
    }

    /// Filter all ingresses based on a passed filter and return them
    pub fn search(&self, filter: QueryFilter,) -> Vec<OwnedIdAndInfo> {
        // ALternatively, simply clone the entire thing and release the read-lock asap.
        //let mut reg = self.info.read().unwrap().clone();

        self.info.read().unwrap().iter()
            .filter(|(_, info)| filter.filter(info))
            .map(|(&ingress_id, info)| OwnedIdAndInfo{ingress_id, ingress_info: info.clone()} )
            .collect::<Vec<_>>()

    }

    /// Filter all ingresses based on a passed filter and write it out
    pub fn search_and_output<T>(&self, filter: QueryFilter, mut target: T) -> Result<(), OutputError> 
        where for <'a> IdAndInfo<'a>: GenOutput<T>
    {
        IdAndInfo::write_seq_start(&mut target)?;
        let lock = self.info.read().unwrap();

        let mut iter = lock.iter().filter(|(_, info)| {
            filter.filter(info)
        });

        if let Some((&ingress_id, ingress_info)) = iter.next() {
            let _ = IdAndInfo::from((ingress_id, ingress_info)).write(&mut target);
        }

        for (&ingress_id, ingress_info) in iter {
            IdAndInfo::write_seq_sep(&mut target)?;
            IdAndInfo::from((ingress_id, ingress_info)).write(&mut target)?;
        }

        IdAndInfo::write_seq_end(&mut target)?;

        Ok(())
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
    /// Furthermore, the RIB type, Peer type, and the Peer Distinguisher are compared, though these
    /// might be unset, i.e. None.
    pub fn find_existing_peer(
        &self,
        query: &IngressInfo
    ) -> Option<(IngressId, IngressInfo)> {
        let lock = self.info.read().unwrap();
        for (id, info) in lock.iter() {
            if find_existing_for!(info, query,
                {parent_ingress, remote_addr, remote_asn},
                {rib_type, peer_type, distinguisher, vrf_name}
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
    ///
    /// NB: ideally the sysName is also included when looking for existing routers. This is
    /// currently not trivial without a larger refactor. As a result, multiple BMP exporters coming
    /// from the same remote IP address connecting to the same Rotonda BMP unit will be seen as one
    /// and the same, potentionally causing confusing issues. The workaround is not checking for
    /// existing routers, causing increased memory use. As multiple exporters coming from one and
    /// the same IP address is presumably unlikely, we keep the check as-is for now.
    pub fn find_existing_bmp_router(
        &self,
        query: &IngressInfo
    ) -> Option<(IngressId, IngressInfo)> {
        let lock = self.info.read().unwrap();
        log::debug!("query: {query:?}");
        for (id, info) in lock.iter() {
            if find_existing_for!(info, query,
                {parent_ingress, remote_addr, ingress_type},
                //{name} // to include the sysName in this check, we first need to refactor the BMP unit
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

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum IngressType {
    Bmp,
    BgpViaBmp,
    Bgp,
    Mrt,
    Rtr,
}


#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub enum IngressState {
    Connected,
    Disconnected,
    NonNetwork,
}

// TODO this probably needs a 'state' (connected, disconnected, ..) ?
// and with that, a last_active timestamp/Instant
// This constructs the [`IngressInfo`] struct, used as values in the Register.
info_for_field!(IngressInfo{
   unit_name: String,
   ingress_type: IngressType,
   parent_ingress: IngressId,
   state: IngressState,
   remote_addr: IpAddr,
   remote_asn: Asn,
   rib_type: RibType,
   peer_rib_type: PeerRibType, // RibType + pre/post policy 
   filename: PathBuf,
   name: String,
   desc: String,
   local_asn: Asn,
   peer_type: PeerType,
   distinguisher: [u8; 8],
   vrf_name: String
});

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn builder_and_update() {
        let res = Register::new();
        let id = res.register();
        let info = IngressInfo::new()
            .with_local_asn(Asn::from_u32(65536));
        assert_eq!(info.local_asn, Some(Asn::from_u32(65536)));

        res.update_info(id, info.clone());
        assert_eq!(
            res.get(id), 
            Some(info)
        );

        let newinfo = IngressInfo::new()
            .with_local_asn(Asn::from_u32(65537));
        res.update_info(id, newinfo.clone());
        assert_eq!(
            res.get(id), 
            Some(newinfo)
        );

        let newinfo_2 = IngressInfo::new()
            .with_rib_type(RibType::LocRib)
        ;
        res.update_info(id, newinfo_2.clone());

        // We still expect the ASN, untouched:
        assert_eq!(
            res.get(id).unwrap().local_asn,
            Some(Asn::from_u32(65537))
        );

        // And the newly set RibType
        assert_eq!(
            res.get(id).unwrap().rib_type,
            Some(RibType::LocRib)
        );


    }

    #[test]
    fn non_mandatory_unset() {
        let res = Register::new();
        let parent_id1 = res.register();
        let session_id1_a = res.register();

        let info1_a = IngressInfo::new()
            .with_parent_ingress(parent_id1)
            .with_remote_addr("1.2.3.4".parse::<IpAddr>().unwrap())
            .with_remote_asn(Asn::from_u32(65000))
            ;

        res.update_info(session_id1_a, info1_a.clone());

        let query1 = IngressInfo::new()
            .with_parent_ingress(parent_id1)
            .with_remote_addr("1.2.3.4".parse::<IpAddr>().unwrap())
            .with_remote_asn(Asn::from_u32(65000))
            ;
        assert_eq!(
            res.find_existing_peer(&query1),
            Some((session_id1_a, info1_a))
        );


    }

    #[test]
    fn non_mandatory_set() {
        let res = Register::new();
        let parent_id1 = res.register();
        let session_id1_a = res.register();

        let info1_a = IngressInfo::new()
            .with_parent_ingress(parent_id1)
            .with_peer_type(PeerType::GlobalInstance)
            .with_remote_addr("1.2.3.4".parse::<IpAddr>().unwrap())
            .with_remote_asn(Asn::from_u32(65000))
            ;

        res.update_info(session_id1_a, info1_a.clone());

        let query1 = IngressInfo::new()
            .with_parent_ingress(parent_id1)
            .with_peer_type(PeerType::GlobalInstance)
            .with_remote_addr("1.2.3.4".parse::<IpAddr>().unwrap())
            .with_remote_asn(Asn::from_u32(65000))
            ;
        assert_eq!(
            res.find_existing_peer(&query1),
            Some((session_id1_a, info1_a))
        );
    }

    #[test]
    fn non_mandatory_query_missing_optional() {
        let res = Register::new();
        let parent_id1 = res.register();
        let session_id1_a = res.register();

        let info1_a = IngressInfo::new()
            .with_parent_ingress(parent_id1)
            .with_peer_type(PeerType::GlobalInstance)
            .with_remote_addr("1.2.3.4".parse::<IpAddr>().unwrap())
            .with_remote_asn(Asn::from_u32(65000))
            ;

        res.update_info(session_id1_a, info1_a.clone());

        let query1 = IngressInfo::new()
            .with_parent_ingress(parent_id1)
            .with_remote_addr("1.2.3.4".parse::<IpAddr>().unwrap())
            .with_remote_asn(Asn::from_u32(65000))
            ;
        assert_eq!(
            res.find_existing_peer(&query1),
            None,
        );
    }

    #[test]
    fn non_mandatory_set_but_different() {
        let res = Register::new();
        let parent_id1 = res.register();
        let session_id1_a = res.register();

        let info1_a = IngressInfo::new()
            .with_parent_ingress(parent_id1)
            .with_remote_addr("1.2.3.4".parse::<IpAddr>().unwrap())
            .with_remote_asn(Asn::from_u32(65000))
            .with_distinguisher([1,2,3,4,9,9,9,9])
            ;

        res.update_info(session_id1_a, info1_a.clone());

        let query1 = IngressInfo::new()
            .with_parent_ingress(parent_id1)
            .with_remote_addr("1.2.3.4".parse::<IpAddr>().unwrap())
            .with_remote_asn(Asn::from_u32(65000))
            .with_distinguisher([9,9,9,9,8,8,8,8])
            ;
        assert_eq!(
            res.find_existing_peer(&query1),
            None,
        );
    }
}

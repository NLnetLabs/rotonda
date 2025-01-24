use core::sync::atomic::AtomicU32;
use std::net::IpAddr;
use std::{collections::HashMap, path::PathBuf, sync::atomic::Ordering};

use std::sync::RwLock;

use inetnum::asn::Asn;

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

    /// Change the info related to an [`IngressId`]
    pub(crate) fn update_info(
        &self,
        id: IngressId,
        info: IngressInfo,
    ) -> Option<IngressInfo> {
        let mut lock = self.info.write().unwrap();

        if let Some(mut old) = lock.remove(&id) {
            if info.remote_addr.is_some() {
                old.remote_addr = info.remote_addr;
            }
            if info.remote_asn.is_some() {
                old.remote_asn = info.remote_asn;
            }
            if info.filename.is_some() {
                old.filename = info.filename;
            }
            if info.name.is_some() {
                old.name = info.name;
            }
            if info.desc.is_some() {
                old.desc = info.desc;
            }
            lock.insert(id, old) // returns the replaced info
        } else {
            lock.insert(id, info) // returns the replaced info
        }
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

    /// Search existing [`IngressId`] comparing parent, remote addr and asn
    pub fn find_existing(&self, query: &IngressInfo) -> Option<(IngressId, IngressInfo)> {
        let lock = self.info.read().unwrap();
        for (id, info) in lock.iter() {
            if info.parent_ingress.is_some()
                && info.remote_addr.is_some()
                && info.remote_asn.is_some()

                && info.parent_ingress == query.parent_ingress
                && info.remote_asn == query.remote_asn
                && info.remote_addr == query.remote_addr
            {
                    return Some((*id, info.clone()))
            }
        }
        None
    }
}

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
pub struct IngressInfo {
    pub unit_name: Option<String>,
    // changed to IpAddr because one of the BMP/BGP connectors did not have
    // SocketAddr for us available. Ideally we change this back to SocketAddr
    // though. remote_addr: Option<SocketAddr>,
    pub parent_ingress: Option<IngressId>,
    pub remote_addr: Option<IpAddr>,
    pub remote_asn: Option<Asn>,
    pub filename: Option<PathBuf>,
    pub name: Option<String>,
    pub desc: Option<String>,
    // pub last_active: Instant ? to enable 'reconnecting' remotes?
}

impl IngressInfo {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_unit_name(self, unit_name: impl Into<String>) -> Self {
        Self { unit_name: Some(unit_name.into()), ..self }
    }

    pub fn with_parent(self, parent: IngressId) -> Self {
        Self { parent_ingress: Some(parent), ..self }
    }

    pub fn with_remote_addr(self, addr: IpAddr) -> Self {
        Self { remote_addr: Some(addr), ..self }
    }

    pub fn with_remote_asn(self, asn: Asn) -> Self {
        Self { remote_asn: Some(asn), ..self }
    }

    pub fn with_filename(self, path: PathBuf) -> Self {
        Self { filename: Some(path), ..self }
    }

    pub fn with_name(self, name: impl Into<String>) -> Self {
        Self { name: Some(name.into()), ..self }
    }

    pub fn with_desc(self, desc: impl Into<String>) -> Self {
        Self { desc: Some(desc.into()), ..self }
    }
}

#[cfg(test)]
mod tests {


}

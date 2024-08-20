use core::sync::atomic::AtomicU32;
use std::net::IpAddr;
use std::{
    collections::HashMap, path::PathBuf, sync::atomic::Ordering,
};

use std::sync::RwLock;

use inetnum::asn::Asn;

/// Register of ingress/sources, tracked in a serial way.
///
/// Sources are BGP sessions (so multiple per BGP connector Unit), or BMP
/// sessions (a single one per BMP connector unit), or something else.
///
/// The ingress/connector units need to register their connections with the
/// Register, providing additional information such as `SocketAddr`s or
/// string-like names.
/// Any unit/target can query the Register.
#[derive(Debug, Default)]
pub struct Register {
    serial: AtomicU32,
    info: RwLock<HashMap<IngressId, IngressInfo>>,
    //write_lock: Mutex<()>,
}

pub type IngressId = u32;

impl Register {
    pub(crate) fn new() -> Self {
        Self {
            serial: 1.into(),
            //info: ArcSwap::from_pointee(HashMap::new()),
            info: RwLock::new(HashMap::new()),
            //WRITE_lock: Mutex::new(()),
        }
    }

    pub(crate) fn register(&self) -> IngressId {
        self.serial.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn update_info(
        &self,
        id: IngressId,
        info: IngressInfo,
    ) -> Option<IngressInfo> {
        let mut lock = self.info.write().unwrap();
        //let old = self.info.load();
        //let mut new = old.clone();
        //let replaced = Arc::make_mut(&mut new).insert(id, info);
        //self.info.store(new.into());
        lock.insert(id, info) // returnst he replaced info
    }

    pub(crate) fn get(&self, id: IngressId) -> Option<IngressInfo> {
        //self.info.load().get(&id).cloned()
        self.info.read().unwrap().get(&id).cloned()
    }
}

#[derive(Clone, Debug, Default)]
#[derive(serde::Serialize)]
pub struct IngressInfo {
    // changed to IpAddr because one of the BMP/BGP connectors did not have
    // SocketAddr for us available. Ideally we change this back to SocketAddr
    // though.
    //remote_addr: Option<SocketAddr>,
    pub remote_addr: Option<IpAddr>,
    pub remote_asn: Option<Asn>,
    pub filename: Option<PathBuf>,
    pub name: Option<String>,
    // pub last_active: Instant ? to enable 'reconnecting' remotes?
}

impl IngressInfo {
    pub fn new() -> Self {
        Self::default()
    }

    //pub fn with_remote_addr(self, addr: SocketAddr) -> Self {
    pub fn with_remote_addr(self, addr: IpAddr) -> Self {
        Self { remote_addr: Some(addr), ..self }
    }

    pub fn with_remote_asn(self, asn: Asn) -> Self {
        Self { remote_asn: Some(asn), ..self }
    }

    pub fn with_filename(self, path: PathBuf) -> Self {
        Self { filename: Some(path), ..self }
    }

    pub fn with_name(self, name: String) -> Self {
        Self { name: Some(name), ..self }
    }
}

#[cfg(test)]
mod tests {

}

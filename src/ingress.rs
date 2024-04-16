use core::sync::atomic::AtomicU64;
use std::{
    collections::HashMap, net::SocketAddr, path::PathBuf, sync::{atomic::Ordering, Arc, Mutex}
};

use arc_swap::ArcSwap;
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
    serial: AtomicU64,
    info: ArcSwap<HashMap<IngressId, IngressInfo>>,
    write_lock: Arc<Mutex<()>>,
}

pub type IngressId = u64;

impl Register {
    pub(crate) fn new() -> Self {
        Self {
            serial: 1.into(),
            info: ArcSwap::from_pointee(HashMap::new()),
            write_lock: Arc::new(Mutex::new(())),
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
        let old = self.info.load();
        let mut new = old.clone();
        let replaced = Arc::make_mut(&mut new).insert(id, info);
        self.info.store(new.into());
        replaced
    }

    pub(crate) fn get(&self, id: IngressId) -> Option<IngressInfo> {
        self.info.load().get(&id).cloned()
    }
}

#[derive(Clone, Debug, Default)]
pub struct IngressInfo {
    remote_addr: Option<SocketAddr>,
    remote_asn: Option<Asn>,
    filename: Option<PathBuf>,
    name: Option<String>,
}

impl IngressInfo {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_remote_addr(self, addr: SocketAddr) -> Self {
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

//! Temporary PoC CLI to figoure out how we want to do this consistently between a CLI, the HTTP
//! endpoints, etc.

use std::{io, sync::Arc};

use crate::{ingress, representation::CliFormat};

pub struct CliApi {
    pub ingress_register: Arc<ingress::Register>,
}


impl CliApi {
    pub fn bgp_neighbors(&self) {
        let mut stdout = io::stdout().lock();
        let _ = self.ingress_register.bgp_neighbors(CliFormat(&mut stdout));
    }

    pub fn bmp_routers(&self) {
        let mut stdout = io::stdout().lock();
        let _ = self.ingress_register.bmp_routers(CliFormat(&mut stdout));
    }
}

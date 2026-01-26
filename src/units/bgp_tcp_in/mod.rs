pub(crate) mod metrics;
pub(crate) mod peer_config;
pub(crate) mod router_handler;
pub(crate) mod status_reporter;
pub(crate) mod tcp_md5;

pub mod unit;

use std::io;
use std::net::IpAddr;

pub(crate) trait ListenerMd5Config {
    // Extend for TTL/GTSM or other socket options (via setsockopt) as needed.
    fn configure_md5(
        &self,
        addr: IpAddr,
        prefix_len: Option<u8>,
        key: &[u8],
    ) -> io::Result<()>;
}

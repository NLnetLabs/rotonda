//use roto::types::builtin::SourceId;

use crate::ingress;

/// # Naming
///
/// When logging or publishing metrics we need to be clear about what the log
/// message or metric relates to. For example if we want to count the number
/// of route announcements and withdrawals that could be per BMP unit, per
/// router connected to a single BMP unit, or even per peer announcing routes
/// to the connected router.
///
/// We also have to decide how to refer to these entities. When a router
/// connects to us it connects from an origin IPv4 or IPv6 address and origin
/// ephemeral port. The same router can connect to use from the same IP
/// address multiple times from different ephemeral ports e.g. if it handles
/// IPv4 and IPv6 separately. After connecting per RFC 7854 the first BMP
/// message that we receive from the router MUST be an Initiation Message
/// which MUST contain sysDescr and sysName Information TLVs, though the spec
/// does NOT say that they should be non-empty (they are defined in terms of
/// RFC 1213 which permits both to have lengths in the range 0..255 octets).
/// We can also receive more Initiation Messages later which could change the
/// sysDescr and sysName values. Could we also see additional connections from
/// the same IP address but different ephemeral port? Finally, what uniquely
/// identifies a peer? The `routecore` crate uses a combination of peer IP address,
/// AS number and BGP ID bytes to uniquely identify a peer, which is quite a
/// lot of information.
///
/// So, the values that we may use for naming include at least:
///   - The name given to this unit in the config file.
///   - The source IP and port of each router that connects to us.
///   - The sysName value, possibly empty, received in one or more Initiation
///     Messages sent to us by each router.
///   - The peer IP address, AS number and BGP ID bytes.
pub fn format_source_id(
    router_id_template: &str,
    _sys_name: &str,
    //source_id: &SourceId,
    ingress_id: ingress::IngressId,
) -> String {
    /*
    match source_id.socket_addr() {
        Some(addr) => router_id_template
            .replace("{sys_name}", sys_name)
            .replace("{router_ip}", &format!("{}", addr.ip()))
            .replace("{router_port}", &format!("{}", addr.port())),

        None => router_id_template
            .replace("{sys_name}", sys_name)
            .replace("{router_ip}", "IP")
            .replace("{router_port}", "PORT"),
    }
    */
    router_id_template
        .replace("{sys_name}", &ingress_id.to_string())
        .replace("{router_ip}", "IP")
        .replace("{router_port}", "PORT")
}

// --- EasilyExtendedJSONObject  --------------------------------------------------------------------------------------

use std::convert::TryInto;
use std::slice::Iter;

use routecore::bgp::communities::{
    ExtendedCommunity, LargeCommunity, StandardCommunity, Wellknown,
};
use serde_json::{json, Value};

pub trait EasilyExtendedJSONObject {
    fn insert(&mut self, key: &str, value: Value) -> Option<Value>;

    fn push(&mut self, value: Value);
}

impl EasilyExtendedJSONObject for Value {
    /// Assumes that the given `is_object()` is true for the given `Value`.
    fn insert(&mut self, key: &str, value: Value) -> Option<Value> {
        self.as_object_mut().unwrap().insert(key.to_string(), value)
    }

    fn push(&mut self, value: Value) {
        self.as_array_mut().unwrap().push(value)
    }
}

// --- JSON formatting of communities ---------------------------------------------------------------------------------

pub fn mk_communities_json(
    standard: Iter<StandardCommunity>,
    extended: Iter<ExtendedCommunity>,
    large: Iter<LargeCommunity>,
) -> Value {
    let mut communities: Vec<Value> = vec![];
    communities.extend(standard.flat_map(mk_normal_community_json));
    communities.extend(extended.flat_map(mk_extended_community_json));
    communities.extend(large.flat_map(mk_large_community_json));
    Value::Array(communities)
}

#[rustfmt::skip]
fn mk_normal_community_json(community: &StandardCommunity) -> Option<Value> {
    // RFC 1997 / IANA well known communities registry [1]:
    //
    // Range                    Registration Procedures
    // ------------------------------------------------
    // 0x00000000-0x0000FFFF    Reserved
    // 0x00010000-0xFFFEFFFF    Private Use
    // 0xFFFF0000-0xFFFF8000    First Come First Served
    // 0xFFFF8001-0xFFFFFFFF    Standards Action
    //
    // [1]: https://www.iana.org/assignments/bgp-well-known-communities/bgp-well-known-communities.xhtml
    let raw = u32::from_be_bytes(community.as_ref().try_into().unwrap());

    // These are all of the "well-known" BGP communities that the routecore crate knows about. For these we
    // are able to output the name of the community.
    match community.to_wellknown() {
        Some(Wellknown::Unrecognized(_)) => {
            // Next we detect the range of numbers from which "well-known" BGP community numbers are assigned. If we
            // match here it means this is a "well-known" community, but whether or it is published in the IANA registry
            // or not we don't know, and we don't know the human readable name that might be associated with the
            // community, so this is an unrecognised well known community.
            Some(mk_unrecognised_well_known_community_json(raw))
        }
        Some(_) => {
            Some(mk_well_known_community_json(community, raw))
        }
        None if community.is_reserved() => {
            // Otherwise the community must fall either into the reserved range:
            Some(mk_reserved_community_json(raw))
        }
        None if community.is_private() => {
            // Or in the private use range (interpreted as 16-bit ASN + 16-bit "tag")
            Some(mk_private_use_community_json(community))
        }
        _ => {
            // We can't have any other kind of "normal" community as we considered the entire valid numeric range
            // above, so if we match there there is something fatally wrong.
            // Likewise as we are only iterating over "normal" communities we cannot end up with a match to an
            // "extended" or "large" community:
            // TODO: log this unexpected internal error
            None
        }
    }
}

fn mk_extended_community_json(community: &ExtendedCommunity) -> Option<Value> {
    // The structure doesn't tell us if we have to look at the "type low" (subtyp()) value or not, we can only know
    // that based on knowledge of the "type value" stored in the IANA BGP Extended Communities registry [1].
    //
    // [1]: https://www.iana.org/assignments/bgp-extended-communities/bgp-extended-communities.xhtml
    use routecore::bgp::communities::ExtendedCommunitySubType::*;
    use routecore::bgp::communities::ExtendedCommunityType::*;
    let mut raw_fields = Vec::new();
    let (typ, subtyp) = community.types();
    let parsed = match (typ, subtyp) {
        // 0x00 = Transitive Two-Octet AS-Specific Extended Community (RFC 7153)
        // - 0x02 = Route Target (RFC 4360)
        // - 0x03 = Route Origin (RFC 4360)
        (TransitiveTwoOctetSpecific, RouteTarget) | (TransitiveTwoOctetSpecific, RouteOrigin) => {
            let global_admin = community.as2().unwrap();
            let local_admin = community.an4().unwrap();
            raw_fields.push(format!("{:#04X}", community.type_raw()));
            raw_fields.push(format!("{:#04X}", community.raw()[1]));
            raw_fields.push(format!("{:#06X}", global_admin.to_u16()));
            raw_fields.push(format!("{:#010X}", local_admin));
            Some(json!({
                "type": "as2-specific",
                "rfc7153SubType": match subtyp {
                    RouteTarget => "route-target",
                    RouteOrigin => "route-origin",
                    _ => unreachable!()
                },
                "globalAdmin": {
                    "type": "asn",
                    "value": global_admin.to_string(),
                },
                "localAdmin": local_admin,
            }))
        }

        // 0x01 = Transitive IPv4-Address-Specific Extended Community (RFC 7153)
        // - 0x02 = Route Target (RFC 4360)
        // - 0x03 = Route Origin (RFC 4360)
        (TransitiveIp4Specific, RouteTarget) | (TransitiveIp4Specific, RouteOrigin) => {
            let global_admin = community.ip4().unwrap();
            let local_admin = community.an2().unwrap();
            raw_fields.push(format!("{:#04X}", community.type_raw()));
            raw_fields.push(format!("{:#04X}", community.raw()[1]));
            raw_fields.push(format!("{:#010X}", u32::from(global_admin)));
            raw_fields.push(format!("{:#06X}", local_admin));
            Some(json!({
                "type": "ipv4-address-specific",
                "rfc7153SubType": match subtyp {
                    RouteTarget => "route-target",
                    RouteOrigin => "route-origin",
                    _ => unreachable!()
                },
                "globalAdmin": {
                    "type": "ipv4-address",
                    "value": global_admin,
                },
                "localAdmin": local_admin,
            }))
        }

        _ => {
            raw_fields.extend(community.raw().iter().map(|x| format!("{:#04X}", x)));
            Some(json!({
                "type": "unrecognised",
            }))
        }
    };

    let mut res = json!({
        "rawFields": json!(raw_fields),
        "type": "extended",
    });

    if let Some(mut parsed) = parsed {
        parsed.insert("transitive", Value::Bool(community.is_transitive()));
        res.insert("parsed", parsed);
    }

    Some(res)
}

fn mk_large_community_json(community: &LargeCommunity) -> Option<Value> {
    let mut raw_fields = Vec::new();
    raw_fields.push(format!("{:#06X}", community.global()));
    raw_fields.push(format!("{:#06X}", community.local1()));
    raw_fields.push(format!("{:#06X}", community.local2()));

    let asn = format!("AS{}", community.global());

    Some(json!({
        "rawFields": json!(raw_fields),
        "type": "large",
        "parsed": {
            "globalAdmin": { "type": "asn", "value": asn },
            "localDataPart1": community.local1(),
            "localDataPart2": community.local2(),
        }
    }))
}

fn mk_private_use_community_json(community: &StandardCommunity) -> Value {
    let asn: u16 = community.asn().unwrap().into_u32().try_into().unwrap();
    let tag: u16 = community.tag().unwrap().value();
    let formatted_asn = format!("AS{}", asn); // to match Routinator JSON style
    json!({
        "rawFields": [format!("{:#06X}", asn), format!("{:#06X}", tag)],
        "type": "standard",
        "parsed": {
            "value": { "type": "private", "asn": formatted_asn, "tag": tag }
        }
    })
}

fn mk_reserved_community_json(raw: u32) -> Value {
    json!({
        "rawFields": [format!("{:#010X}", raw)],
        "type": "standard",
        "parsed": {
            "value": { "type": "reserved" }
        }
    })
}

fn mk_well_known_community_json(community: &StandardCommunity, raw: u32) -> Value {
    json!({
        "rawFields": [format!("{:#010X}", raw)],
        "type": "standard",
        "parsed": {
            "value": { "type": "well-known", "attribute": format!("{}", community) }
        }
    })
}

fn mk_unrecognised_well_known_community_json(raw: u32) -> Value {
    json!({
        "rawFields": [format!("{:#010X}", raw)],
        "type": "standard",
        "parsed": {
            "value": { "type": "well-known-unrecognised" }
        }
    })
}

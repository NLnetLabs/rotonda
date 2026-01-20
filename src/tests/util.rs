#[cfg(test)]
pub(crate) mod internal {
    use std::sync::Arc;

    use env_logger::Env;

    use crate::metrics::{self, OutputFormat, Target};

    /// Tries to enable logging. Intended for use in tests.
    ///
    /// Accepts a log level name as a string, e.g. "trace".
    #[allow(dead_code)]
    pub(crate) fn enable_logging(log_level: &str) {
        let _ = env_logger::Builder::from_env(
            Env::default().default_filter_or(log_level),
        )
        .is_test(true)
        .try_init();
    }

    pub(crate) fn get_testable_metrics_snapshot(
        metrics: &Arc<impl metrics::Source + ?Sized>,
    ) -> Target {
        let mut target = Target::new(OutputFormat::Test);
        metrics.append("testunit", &mut target);
        target
    }
}

pub fn assert_json_eq(
    actual_json: serde_json::Value,
    expected_json: serde_json::Value,
) {
    use assert_json_diff::{assert_json_matches_no_panic, CompareMode};

    let config = assert_json_diff::Config::new(CompareMode::Strict);
    if let Err(err) =
        assert_json_matches_no_panic(&actual_json, &expected_json, config)
    {
        eprintln!(
            "Actual JSON: {}",
            serde_json::to_string_pretty(&actual_json).unwrap()
        );
        eprintln!(
            "Expected JSON: {}",
            serde_json::to_string_pretty(&expected_json).unwrap()
        );
        panic!("JSON doesn't match expectations: {}", err);
    }
}

pub mod bgp {
    pub mod raw {
        pub mod communities {
            pub mod standard {
                use routecore::bgp::communities::StandardCommunity;

                pub fn sample_reserved_standard_community(
                ) -> StandardCommunity {
                    [0x00, 0x00, 0x00, 0x00].into()
                }

                pub fn sample_private_community() -> StandardCommunity {
                    [0x00, 0x01, 0x00, 0x00].into()
                }

                pub fn well_known_rfc1997_no_export_community(
                ) -> StandardCommunity {
                    [0xFF, 0xFF, 0xFF, 0x01].into()
                }

                pub fn well_known_rfc7999_blackhole_community(
                ) -> StandardCommunity {
                    [0xFF, 0xFF, 0x02, 0x9A].into()
                }

                pub fn well_known_rfc8326_graceful_shutdown_community(
                ) -> StandardCommunity {
                    [0xFF, 0xFF, 0x00, 0x00].into()
                }
            }

            pub mod extended {
                use routecore::bgp::communities::ExtendedCommunity;

                pub fn sample_as2_specific_route_target_extended_community(
                ) -> ExtendedCommunity {
                    ExtendedCommunity::from_raw([
                        0x00, 0x02, 0x00, 0x22, 0x00, 0x00, 0xD5, 0x08,
                    ])
                }

                pub fn sample_ipv4_address_specific_route_target_extended_community(
                ) -> ExtendedCommunity {
                    ExtendedCommunity::from_raw([
                        0x01, 0x02, 0xC1, 0x2A, 0x00, 0x0A, 0xD5, 0x08,
                    ])
                }

                pub fn sample_unrecgonised_extended_community(
                ) -> ExtendedCommunity {
                    ExtendedCommunity::from_raw([
                        0x02, 0x02, 0x00, 0x22, 0x00, 0x00, 0xD5, 0x08,
                    ])
                }
            }

            pub mod large {
                use routecore::bgp::communities::LargeCommunity;

                pub fn sample_large_community() -> LargeCommunity {
                    LargeCommunity::from_raw([
                        0x00, 0x00, 0x00, 0x22, 0x00, 0x00, 0x01, 0x00, 0x00,
                        0x00, 0x02, 0x00,
                    ])
                }
            }
        }
    }

    pub mod encode {
        use std::convert::TryFrom;
        use std::{net::IpAddr, ops::Deref, str::FromStr};

        use bytes::{BufMut, Bytes, BytesMut};
        use chrono::Utc;
        use inetnum::asn::Asn;
        use rotonda_store::addr::Prefix;
        use routecore::bgp::aspath::HopPath;
        use routecore::bgp::communities::Community;
        use routecore::bgp::types::{
            AfiSafiType, NextHop, OriginType, PathAttributeType,
        };
        use routecore::bmp::message::{
            InformationTlvType, MessageType, PeerType, TerminationInformation,
        };

        pub fn mk_initiation_msg(sys_name: &str, sys_descr: &str) -> Bytes {
            let mut buf = BytesMut::new();
            push_bmp_common_header(&mut buf, MessageType::InitiationMessage);

            // 4.3.  Initiation Message
            //
            // "The initiation message consists of the common BMP header followed by
            //  two or more Information TLVs (Section 4.4) containing information
            //  about the monitored router.  The sysDescr and sysName Information
            //  TLVs MUST be sent, any others are optional.  The string TLV MAY be
            //  included multiple times."
            //
            // From: https://www.rfc-editor.org/rfc/rfc7854.html#section-4.3

            // 4.4.  Information TLV
            //
            //  0                   1                   2                   3
            //  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
            // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            // |          Information Type     |       Information Length      |
            // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            // |                 Information (variable)                        |
            // ~                                                               ~
            // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            //
            // From: https://www.rfc-editor.org/rfc/rfc7854.html#section-4.4

            push_bmp_information_tlv(
                &mut buf,
                InformationTlvType::SysName,
                sys_name.as_bytes(),
            );
            push_bmp_information_tlv(
                &mut buf,
                InformationTlvType::SysDesc,
                sys_descr.as_bytes(),
            );

            finalize_bmp_msg_len(&mut buf);
            buf.freeze()
        }

        pub fn mk_invalid_initiation_message_that_lacks_information_tlvs(
        ) -> Bytes {
            let mut buf = BytesMut::new();
            push_bmp_common_header(&mut buf, MessageType::InitiationMessage);

            // 4.3.  Initiation Message
            //
            // "The initiation message consists of the common BMP header followed by
            //  two or more Information TLVs (Section 4.4) containing information
            //  about the monitored router.  The sysDescr and sysName Information
            //  TLVs MUST be sent, any others are optional.  The string TLV MAY be
            //  included multiple times."
            //
            // From: https://www.rfc-editor.org/rfc/rfc7854.html#section-4.3

            finalize_bmp_msg_len(&mut buf);
            buf.freeze()
        }

        #[allow(clippy::too_many_arguments)]
        #[allow(clippy::vec_init_then_push)]
        pub fn mk_peer_up_notification_msg(
            per_peer_header: &PerPeerHeader,
            local_address: IpAddr,
            local_port: u16,
            remote_port: u16,
            sent_open_asn: u16,
            received_open_asn: u16,
            sent_bgp_identifier: u32,
            received_bgp_identifier: u32,
            information_tlvs: Vec<(InformationTlvType, String)>,
            eor_capable: bool,
        ) -> Bytes {
            let mut buf = BytesMut::new();
            push_bmp_common_header(&mut buf, MessageType::PeerUpNotification);
            push_bmp_per_peer_header(&mut buf, per_peer_header);

            // 4.10.  Peer Up Notification
            //
            //  0                   1                   2                   3
            //  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
            // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            // |                 Local Address (16 bytes)                      |
            // ~                                                               ~
            // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            // |         Local Port            |        Remote Port            |
            // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            // |                    Sent OPEN Message                          |
            // ~                                                               ~
            // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            // |                  Received OPEN Message                        |
            // ~                                                               ~
            // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            // |                 Information (variable)                        |
            // ~                                                               ~
            // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            //
            // From: https://www.rfc-editor.org/rfc/rfc7854.html#section-4.10

            match local_address {
                IpAddr::V4(addr) => {
                    assert!(per_peer_header.is_ipv4());
                    buf.resize(buf.len() + 12, 0u8);
                    buf.extend_from_slice(&addr.octets());
                }
                IpAddr::V6(addr) => {
                    assert!(per_peer_header.is_ipv6());
                    buf.extend_from_slice(&addr.octets());
                }
            }

            buf.extend_from_slice(&local_port.to_be_bytes());
            buf.extend_from_slice(&remote_port.to_be_bytes());

            // 4.2.  OPEN Message Format
            //
            // After a TCP connection is established, the first message sent by each
            // side is an OPEN message.  If the OPEN message is acceptable, a
            // KEEPALIVE message confirming the OPEN is sent back.
            //
            // In addition to the fixed-size BGP header, the OPEN message contains
            // the following fields:
            //
            //      0                   1                   2                   3
            //      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
            //     +-+-+-+-+-+-+-+-+
            //     |    Version    |
            //     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            //     |     My Autonomous System      |
            //     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            //     |           Hold Time           |
            //     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            //     |                         BGP Identifier                        |
            //     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            //     | Opt Parm Len  |
            //     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            //     |                                                               |
            //     |             Optional Parameters (variable)                    |
            //     |                                                               |
            //     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            //
            // From: https://datatracker.ietf.org/doc/html/rfc4271#section-4.2

            // insert fake BGP open sent message
            let mut bgp_msg_buf = BytesMut::new();
            // Fixed size BGP header
            bgp_msg_buf.resize(bgp_msg_buf.len() + 16, 0xFFu8); // marker
            bgp_msg_buf.resize(bgp_msg_buf.len() + 2, 0); // placeholder length, to be replaced later
            bgp_msg_buf.extend_from_slice(&1u8.to_be_bytes()); // 1 - OPEN
            bgp_msg_buf.extend_from_slice(&4u8.to_be_bytes()); // BGP version 4

            // Other fields
            bgp_msg_buf.extend_from_slice(&sent_open_asn.to_be_bytes());
            bgp_msg_buf.extend_from_slice(&0u16.to_be_bytes()); // 0 hold time - disables keep alive
            bgp_msg_buf.extend_from_slice(&sent_bgp_identifier.to_be_bytes());
            bgp_msg_buf.extend_from_slice(&0u8.to_be_bytes()); // 0 optional parameters

            // Finalize BGP message
            finalize_bgp_msg_len(&mut bgp_msg_buf);
            buf.extend_from_slice(&bgp_msg_buf);

            // insert fake BGP open received message
            let mut bgp_msg_buf = BytesMut::new();
            // Fixed size BGP header
            bgp_msg_buf.resize(bgp_msg_buf.len() + 16, 0xFFu8); // marker
            bgp_msg_buf.resize(bgp_msg_buf.len() + 2, 0); // placeholder length, to be replaced later
            bgp_msg_buf.extend_from_slice(&1u8.to_be_bytes()); // 1 - OPEN
            bgp_msg_buf.extend_from_slice(&4u8.to_be_bytes()); // BGP version 4

            // Other fields
            bgp_msg_buf.extend_from_slice(&received_open_asn.to_be_bytes());
            bgp_msg_buf.extend_from_slice(&0u16.to_be_bytes()); // 0 hold time - disables keep alive
            bgp_msg_buf
                .extend_from_slice(&received_bgp_identifier.to_be_bytes());

            if !eor_capable {
                bgp_msg_buf.extend_from_slice(&0u8.to_be_bytes()); // 0 optional parameter bytes
            } else {
                // A peer capable of sending the special End-Of-Rib marker BGP
                // UPDATE message advertises this ability using a BGP capability
                // (RFC 5492) which is expressed as an optional parameter type 2
                // with a capability code 64 (from RFC 4274).

                // Optional Parameters:
                //
                // This field contains a list of optional parameters, in which
                // each parameter is encoded as a <Parameter Type, Parameter
                // Length, Parameter Value> triplet.
                //
                //  0                   1
                //  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5
                // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-...
                // |  Parm. Type   | Parm. Length  |  Parameter Value (variable)
                // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-...
                //
                // Parameter Type is a one octet field that unambiguously
                // identifies individual parameters.  Parameter Length is a one
                // octet field that contains the length of the Parameter Value
                // field in octets.  Parameter Value is a variable length field
                // that is interpreted according to the value of the Parameter
                // Type field.
                //
                // [RFC3392] defines the Capabilities Optional Parameter.
                //
                // From: https://datatracker.ietf.org/doc/html/rfc4271#section-4.2
                // Note that RFC 3392 was obsoleted by RFC 5492
                //
                // Which leads us to...

                // 4.  Capabilities Optional Parameter (Parameter Type 2):
                //
                // This is an Optional Parameter that is used by a BGP speaker to convey
                // to its BGP peer the list of capabilities supported by the speaker.
                // The encoding of BGP Optional Parameters is specified in Section 4.2
                // of [RFC4271].  The parameter type of the Capabilities Optional
                // Parameter is 2.
                //
                // The parameter contains one or more triples <Capability Code,
                // Capability Length, Capability Value>, where each triple is encoded as
                // shown below:
                //
                //        +------------------------------+
                //        | Capability Code (1 octet)    |
                //        +------------------------------+
                //        | Capability Length (1 octet)  |
                //        +------------------------------+
                //        | Capability Value (variable)  |
                //        ~                              ~
                //        +------------------------------+
                //
                // The use and meaning of these fields are as follows:
                //
                //    Capability Code:
                //
                //       Capability Code is a one-octet unsigned binary integer that
                //       unambiguously identifies individual capabilities.
                //
                //    Capability Length:
                //
                //       Capability Length is a one-octet unsigned binary integer that
                //       contains the length of the Capability Value field in octets.
                //
                //    Capability Value:
                //
                //       Capability Value is a variable-length field that is interpreted
                //       according to the value of the Capability Code field.
                //
                // From: https://datatracker.ietf.org/doc/html/rfc5492#section-4
                //
                // Which leads us further on to ...

                // 3.  Graceful Restart Capability
                //
                // The Graceful Restart Capability is a new BGP capability [BGP-CAP]
                // that can be used by a BGP speaker to indicate its ability to preserve
                // its forwarding state during BGP restart.  It can also be used to
                // convey to its peer its intention of generating the End-of-RIB marker
                // upon the completion of its initial routing updates.
                //
                // This capability is defined as follows:
                //
                //    Capability code: 64
                //
                //    Capability length: variable
                //
                //    Capability value: Consists of the "Restart Flags" field, "Restart
                //    Time" field, and 0 to 63 of the tuples <AFI, SAFI, Flags for
                //    address family> as follows:
                //
                //       +--------------------------------------------------+
                //       | Restart Flags (4 bits)                           |
                //       +--------------------------------------------------+
                //       | Restart Time in seconds (12 bits)                |
                //       +--------------------------------------------------+
                //       | Address Family Identifier (16 bits)              |
                //       +--------------------------------------------------+
                //       | Subsequent Address Family Identifier (8 bits)    |
                //       +--------------------------------------------------+
                //       | Flags for Address Family (8 bits)                |
                //       +--------------------------------------------------+
                //       | ...                                              |
                //       +--------------------------------------------------+
                //       | Address Family Identifier (16 bits)              |
                //       +--------------------------------------------------+
                //       | Subsequent Address Family Identifier (8 bits)    |
                //       +--------------------------------------------------+
                //       | Flags for Address Family (8 bits)                |
                //       +--------------------------------------------------+
                //
                // The use and meaning of the fields are as follows:
                //
                //    Restart Flags:
                //
                //       This field contains bit flags related to restart.
                //
                //           0 1 2 3
                //          +-+-+-+-+
                //          |R|Resv.|
                //          +-+-+-+-+
                //
                //       The most significant bit is defined as the Restart State (R)
                //       bit, which can be used to avoid possible deadlock caused by
                //       waiting for the End-of-RIB marker when multiple BGP speakers
                //       peering with each other restart.  When set (value 1), this bit
                //       indicates that the BGP speaker has restarted, and its peer MUST
                //       NOT wait for the End-of-RIB marker from the speaker before
                //       advertising routing information to the speaker.
                //
                //       The remaining bits are reserved and MUST be set to zero by the
                //       sender and ignored by the receiver.
                //
                //    Restart Time:
                //
                //       This is the estimated time (in seconds) it will take for the
                //       BGP session to be re-established after a restart.  This can be
                //       used to speed up routing convergence by its peer in case that
                //       the BGP speaker does not come back after a restart.
                //
                //    Address Family Identifier (AFI), Subsequent Address Family
                //       Identifier (SAFI):
                //
                //       The AFI and SAFI, taken in combination, indicate that Graceful
                //       Restart is supported for routes that are advertised with the
                //       same AFI and SAFI.  Routes may be explicitly associated with a
                //       particular AFI and SAFI using the encoding of [BGP-MP] or
                //       implicitly associated with <AFI=IPv4, SAFI=Unicast> if using
                //       the encoding of [BGP-4].
                //
                //    Flags for Address Family:
                //
                //       This field contains bit flags relating to routes that were
                //       advertised with the given AFI and SAFI.
                //
                //           0 1 2 3 4 5 6 7
                //          +-+-+-+-+-+-+-+-+
                //          |F|   Reserved  |
                //          +-+-+-+-+-+-+-+-+
                //
                //       The most significant bit is defined as the Forwarding State (F)
                //       bit, which can be used to indicate whether the forwarding state
                //       for routes that were advertised with the given AFI and SAFI has
                //       indeed been preserved during the previous BGP restart.  When
                //       set (value 1), the bit indicates that the forwarding state has
                //       been preserved.
                //
                //       The remaining bits are reserved and MUST be set to zero by the
                //       sender and ignored by the receiver.
                // From: https://datatracker.ietf.org/doc/html/rfc4724#section-3

                // BGP optional parameters: optp_len, optp_params
                // Where:
                //   optp_len = octet len of optp_params
                //   optp_params = [(optpi_type, optpi_len, optpi_value), ...]
                //     Where:
                //       optpi_type = 2 (the RFC 5492 capabilities optional parameter code)
                //       optpi_len = the octet len of optpi_vals
                //       optpi_value = [(cap_code, cap_len, cap_val), ...]
                //         Where there is a single tuple with:
                //           cap_code = 64 (the RFC 4724 graceful restart capability code)
                //           cap_len  = 2 (two bytes for 4-bit flags + 12-bit restart time)
                //           cap_val  = 0 (0 4-bit flags + 0 12-bit restart time)
                //       }
                //   }

                // innermost layer
                let cap_code = 64u8;
                let cap_len = 2u8;
                let cap_val = 0u16;

                // middle layer
                let optpi_type = 2u8;
                let mut optpi_value = Vec::<u8>::new();
                optpi_value.push(cap_code);
                optpi_value.push(cap_len);
                optpi_value.extend_from_slice(&cap_val.to_be_bytes());
                let optpi_len = u8::try_from(optpi_value.len()).unwrap();

                // outer layer
                let mut optp_params = Vec::<u8>::new();
                optp_params.push(optpi_type);
                optp_params.push(optpi_len);
                optp_params.append(&mut optpi_value);
                let optp_len = u8::try_from(optp_params.len()).unwrap();

                // extend the BGP OPEN message with the optional parameters
                bgp_msg_buf.extend_from_slice(&[optp_len]);
                bgp_msg_buf.extend_from_slice(&optp_params);
            }

            // Finalize BGP message
            finalize_bgp_msg_len(&mut bgp_msg_buf);
            buf.extend_from_slice(&bgp_msg_buf);

            for (typ, val) in information_tlvs {
                push_bmp_information_tlv(&mut buf, typ, val.as_bytes());
            }

            finalize_bmp_msg_len(&mut buf);
            buf.freeze()
        }

        #[allow(clippy::vec_init_then_push)]
        pub fn mk_route_monitoring_msg(
            per_peer_header: &PerPeerHeader,
            withdrawals: &Prefixes,
            announcements: &Announcements,
            extra_path_attributes: &[u8],
        ) -> Bytes {
            let bgp_msg_buf = mk_bgp_update(
                withdrawals,
                announcements,
                extra_path_attributes,
            );
            mk_raw_route_monitoring_msg(per_peer_header, bgp_msg_buf)
        }

        pub fn mk_raw_route_monitoring_msg(
            per_peer_header: &PerPeerHeader,
            bgp_msg_buf: Bytes,
        ) -> Bytes {
            // 4.6.  Route Monitoring
            //
            // "Following the common BMP header and per-peer header is a BGP Update
            //  PDU."
            //
            // From: https://www.rfc-editor.org/rfc/rfc7854.html#section-4.6

            let mut buf = BytesMut::new();
            push_bmp_common_header(&mut buf, MessageType::RouteMonitoring);
            push_bmp_per_peer_header(&mut buf, per_peer_header);
            buf.extend_from_slice(&bgp_msg_buf);
            finalize_bmp_msg_len(&mut buf);
            buf.freeze()
        }

        #[allow(clippy::vec_init_then_push)]
        pub fn mk_bgp_update(
            withdrawals: &Prefixes,
            announcements: &Announcements,
            extra_path_attributes: &[u8],
        ) -> Bytes {
            // 4.3. UPDATE Message Format
            //
            // "The UPDATE message always includes the fixed-size BGP
            //  header, and also includes the other fields, as shown below (note,
            //  some of the shown fields may not be present in every UPDATE message):"
            //
            //      +-----------------------------------------------------+
            //      |   Withdrawn Routes Length (2 octets)                |
            //      +-----------------------------------------------------+
            //      |   Withdrawn Routes (variable)                       |
            //      +-----------------------------------------------------+
            //      |   Total Path Attribute Length (2 octets)            |
            //      +-----------------------------------------------------+
            //      |   Path Attributes (variable)                        |
            //      +-----------------------------------------------------+
            //      |   Network Layer Reachability Information (variable) |
            //      +-----------------------------------------------------+
            //
            // From: https://datatracker.ietf.org/doc/html/rfc4271#section-4.3

            let mut buf = BytesMut::new();

            // Fixed size BGP header
            buf.resize(buf.len() + 16, 0xFFu8);
            // marker
            buf.resize(buf.len() + 2, 0);
            // placeholder length, to be replaced later
            buf.extend_from_slice(&2u8.to_be_bytes());
            // 2 - UPDATE

            // Other fields
            // Route withdrawals
            // "Withdrawn Routes Length:
            //
            //  This 2-octets unsigned integer indicates the total length of
            //  the Withdrawn Routes field in octets.  Its value allows the
            //  length of the Network Layer Reachability Information field to
            //  be determined, as specified below.
            //
            //  A value of 0 indicates that no routes are being withdrawn from
            //  service, and that the WITHDRAWN ROUTES field is not present in
            //  this UPDATE message.
            //
            //  Withdrawn Routes:
            //
            //  This is a variable-length field that contains a list of IP
            //  address prefixes for the routes that are being withdrawn from
            //  service.  Each IP address prefix is encoded as a 2-tuple of the
            //  form <length, prefix>, whose fields are described below:
            //
            //           +---------------------------+
            //           |   Length (1 octet)        |
            //           +---------------------------+
            //           |   Prefix (variable)       |
            //           +---------------------------+
            //
            //  The use and the meaning of these fields are as follows:
            //
            //  a) Length:
            //
            //     The Length field indicates the length in bits of the IP
            //     address prefix.  A length of zero indicates a prefix that
            //     matches all IP addresses (with prefix, itself, of zero
            //     octets).
            //
            //  b) Prefix:
            //
            //     The Prefix field contains an IP address prefix, followed by
            //     the minimum number of trailing bits needed to make the end
            //     of the field fall on an octet boundary.  Note that the value
            //     of trailing bits is irrelevant."
            //
            // From: https://datatracker.ietf.org/doc/html/rfc4271#section-4.3
            let mut withdrawn_routes = BytesMut::new();
            let mut mp_unreach_nlri = BytesMut::new();

            for prefix in withdrawals.iter() {
                let (addr, len) = prefix.addr_and_len();
                match addr {
                    IpAddr::V4(addr) => {
                        withdrawn_routes.extend_from_slice(&[len]);
                        if len > 0 {
                            let min_bytes = div_ceil(len, 8) as usize;
                            withdrawn_routes.extend_from_slice(
                                &addr.octets()[..min_bytes],
                            );
                        }
                    }
                    IpAddr::V6(addr) => {
                        // https://datatracker.ietf.org/doc/html/rfc4760#section-4
                        if mp_unreach_nlri.is_empty() {
                            let (afi, safi) = AfiSafiType::Ipv6Unicast.into();
                            mp_unreach_nlri.put_u16(afi);
                            mp_unreach_nlri.put_u8(safi);
                            //mp_unreach_nlri.put_u16(Afi::Ipv6.into());
                            //mp_unreach_nlri.put_u8(
                            //    u8::from(AfiSafiType::Ipv6Unicast)
                            //        | u8::from(AfiSafiType::Ipv6Multicast),
                            //);
                        }
                        mp_unreach_nlri.extend_from_slice(&[len]);
                        if len > 0 {
                            let min_bytes = div_ceil(len, 8) as usize;
                            mp_unreach_nlri.extend_from_slice(
                                &addr.octets()[..min_bytes],
                            );
                        }
                    }
                }
            }
            let num_withdrawn_route_bytes =
                u16::try_from(withdrawn_routes.len()).unwrap();
            buf.extend_from_slice(&num_withdrawn_route_bytes.to_be_bytes());
            // N withdrawn route bytes
            if num_withdrawn_route_bytes > 0 {
                buf.extend(&withdrawn_routes); // the withdrawn routes
            }

            // Route announcements
            // "Total Path Attribute Length:
            //
            //  This 2-octet unsigned integer indicates the total length of the
            //  Path Attributes field in octets.  Its value allows the length
            //  of the Network Layer Reachability field to be determined as
            //  specified below.
            //
            //  A value of 0 indicates that neither the Network Layer
            //  Reachability Information field nor the Path Attribute field is
            //  present in this UPDATE message.
            //
            //  Path Attributes:
            //
            //  A variable-length sequence of path attributes is present in
            //  every UPDATE message, except for an UPDATE message that carries
            //  only the withdrawn routes.  Each path attribute is a triple
            //  <attribute type, attribute length, attribute value> of variable
            //  length.
            //
            //  Attribute Type is a two-octet field that consists of the
            //  Attribute Flags octet, followed by the Attribute Type Code
            //  octet.
            //
            //        0                   1
            //        0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5
            //        +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            //        |  Attr. Flags  |Attr. Type Code|
            //        +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+"
            //
            // ...
            //
            // "Network Layer Reachability Information:
            //
            //  This variable length field contains a list of IP address
            //  prefixes.  The length, in octets, of the Network Layer
            //  Reachability Information is not encoded explicitly, but can be
            //  calculated as:
            //
            //        UPDATE message Length - 23 - Total Path Attributes Length
            //        - Withdrawn Routes Length
            //
            //  where UPDATE message Length is the value encoded in the fixed-
            //  size BGP header, Total Path Attribute Length, and Withdrawn
            //  Routes Length are the values encoded in the variable part of
            //  the UPDATE message, and 23 is a combined length of the fixed-
            //  size BGP header, the Total Path Attribute Length field, and the
            //  Withdrawn Routes Length field.
            //
            //  Reachability information is encoded as one or more 2-tuples of
            //  the form <length, prefix>, whose fields are described below:
            //
            //           +---------------------------+
            //           |   Length (1 octet)        |
            //           +---------------------------+
            //           |   Prefix (variable)       |
            //           +---------------------------+
            //
            //  The use and the meaning of these fields are as follows:
            //
            //  a) Length:
            //
            //     The Length field indicates the length in bits of the IP
            //     address prefix.  A length of zero indicates a prefix that
            //     matches all IP addresses (with prefix, itself, of zero
            //     octets).
            //
            //  b) Prefix:
            //
            //     The Prefix field contains an IP address prefix, followed by
            //     enough trailing bits to make the end of the field fall on an
            //     octet boundary.  Note that the value of the trailing bits is
            //     irrelevant."
            //
            // From: https://datatracker.ietf.org/doc/html/rfc4271#section-4.3
            match announcements {
                Announcements::None => {
                    buf.extend_from_slice(&0u16.to_be_bytes()); // 0 path attributes and no NLRI field
                }
                Announcements::Some {
                    origin,
                    as_path,
                    next_hop,
                    communities,
                    prefixes,
                } => {
                    fn push_attributes(
                        out_bytes: &mut Vec<u8>,
                        r#type: PathAttributeType,
                        pa_bytes: &[u8],
                    ) {
                        let len = pa_bytes.len();

                        let (optional, transitive, complete) = match r#type {
                            PathAttributeType::AsPath
                            | PathAttributeType::ConventionalNextHop
                            | PathAttributeType::Origin => {
                                (false, true, true)
                            }
                            PathAttributeType::Communities
                            | PathAttributeType::ExtendedCommunities
                            | PathAttributeType::LargeCommunities
                            | PathAttributeType::MpReachNlri => {
                                (true, false, true)
                            }
                            _ => todo!(),
                        };

                        let mut flags = 0u8;
                        if optional {
                            flags |= 0b1000_0000;
                        }
                        if transitive {
                            flags |= 0b0100_0000;
                        }
                        if complete {
                            flags |= 0b0010_0000;
                        }
                        if len > 255 {
                            flags |= 0b0001_0000;
                        }

                        out_bytes.put_u8(flags); // attr. flags
                        out_bytes.put_u8(u8::from(r#type)); // attr. type
                        if len <= 255 {
                            out_bytes.put_u8(u8::try_from(len).unwrap()); // attr. octet length
                        } else {
                            out_bytes.put_u16(u16::try_from(len).unwrap()); // attr. octet length
                        };

                        out_bytes.extend_from_slice(pa_bytes);
                    }

                    let mut path_attributes = Vec::<u8>::new();

                    // -------------------------------------------------------------------
                    // "ORIGIN (Type Code 1):
                    //
                    //  ORIGIN is a well-known mandatory attribute that defines the origin
                    //  of the path information."
                    //
                    // From: https://datatracker.ietf.org/doc/html/rfc4271#section-4.3
                    push_attributes(
                        &mut path_attributes,
                        PathAttributeType::Origin,
                        &[origin.into()],
                    );

                    // -------------------------------------------------------------------
                    // "AS_PATH (Type Code 2):
                    //
                    //  AS_PATH is a well-known mandatory attribute that is composed of a
                    //  sequence of AS path segments.  Each AS path segment is represented
                    //  by a triple <path segment type, path segment length, path segment
                    //  value>."
                    //
                    // From: https://datatracker.ietf.org/doc/html/rfc4271#section-4.3
                    let mut as_path_attr_value_bytes = Vec::<u8>::new();

                    // sequence of AS path segments [(seg. type, seg. len, seg. val), ...]
                    for segment in
                        as_path.to_as_path::<Vec<u8>>().unwrap().segments()
                    {
                        segment
                            .compose(&mut as_path_attr_value_bytes)
                            .unwrap();
                    }

                    push_attributes(
                        &mut path_attributes,
                        PathAttributeType::AsPath,
                        &as_path_attr_value_bytes,
                    );

                    // -------------------------------------------------------------------
                    // "NEXT_HOP (Type Code 3):
                    //
                    //  This is a well-known mandatory attribute that defines the (unicast)
                    //  IP address of the router that SHOULD be used as the next hop to the
                    //  destinations listed in the Network Layer Reachability Information
                    //  field of the UPDATE message."
                    //
                    // From: https://datatracker.ietf.org/doc/html/rfc4271#section-4.3
                    if let NextHop::Unicast(IpAddr::V4(addr))
                    | NextHop::Multicast(IpAddr::V4(addr)) = next_hop.0
                    {
                        push_attributes(
                            &mut path_attributes,
                            PathAttributeType::ConventionalNextHop,
                            &addr.octets(),
                        );
                    }

                    // -------------------------------------------------------------------
                    // "COMMUNITIES attribute:
                    //
                    //  This document creates the COMMUNITIES path attribute is an optional
                    //  transitive attribute of variable length.  The attribute consists of a
                    //  set of four octet values, each of which specify a community.  All
                    //  routes with this attribute belong to the communities listed in the
                    //  attribute.
                    //
                    //  The COMMUNITIES attribute has Type Code 8.
                    //
                    //  Communities are treated as 32 bit values,  however for administrative
                    //  assignment,  the following presumptions may be made:
                    //
                    //  The community attribute values ranging from 0x0000000 through
                    //  0x0000FFFF and 0xFFFF0000 through 0xFFFFFFFF are hereby reserved.
                    //
                    //  The rest of the community attribute values shall be encoded using an
                    //  autonomous system number in the first two octets.  The semantics of
                    //  the final two octets may be defined by the autonomous system (e.g. AS
                    //  690 may define research, educational and commercial community values
                    //  that may be used for policy routing as defined by the operators of
                    //  that AS using community attribute values 0x02B20000 through
                    //  0x02B2FFFF)."
                    //
                    // From: https://www.rfc-editor.org/rfc/rfc1997.html
                    if !communities.is_empty() {
                        let mut communities_attribute_bytes =
                            Vec::<u8>::new();
                        let mut extended_communities_attribute_bytes =
                            Vec::<u8>::new();
                        let mut large_communities_attribute_bytes =
                            Vec::<u8>::new();

                        for community in communities.deref() {
                            match community {
                                Community::Standard(c) => {
                                    communities_attribute_bytes
                                        .extend_from_slice(&c.to_raw())
                                }
                                Community::Extended(c) => {
                                    extended_communities_attribute_bytes
                                        .extend_from_slice(&c.to_raw())
                                }
                                Community::Ipv6Extended(_) => todo!(),
                                Community::Large(c) => {
                                    large_communities_attribute_bytes
                                        .extend_from_slice(&c.to_raw())
                                }
                            }
                        }

                        if !communities_attribute_bytes.is_empty() {
                            push_attributes(
                                &mut path_attributes,
                                PathAttributeType::Communities,
                                &communities_attribute_bytes,
                            );
                        }

                        if !extended_communities_attribute_bytes.is_empty() {
                            push_attributes(
                                &mut path_attributes,
                                PathAttributeType::ExtendedCommunities,
                                &extended_communities_attribute_bytes,
                            );
                        }

                        if !large_communities_attribute_bytes.is_empty() {
                            push_attributes(
                                &mut path_attributes,
                                PathAttributeType::LargeCommunities,
                                &large_communities_attribute_bytes,
                            );
                        }
                    }

                    // Now add the list of NLRI IP addresses
                    let mut announced_routes = Vec::<u8>::new();
                    let mut mp_reach_nlri = BytesMut::new();

                    for prefix in prefixes.iter() {
                        let (addr, len) = prefix.addr_and_len();
                        match addr {
                            IpAddr::V4(addr) => {
                                announced_routes.extend_from_slice(&[len]);
                                if len > 0 {
                                    let min_bytes = div_ceil(len, 8) as usize;
                                    announced_routes.extend_from_slice(
                                        &addr.octets()[..min_bytes],
                                    );
                                }
                            }
                            IpAddr::V6(addr) => {
                                // https://datatracker.ietf.org/doc/html/rfc4760#section-3
                                if mp_reach_nlri.is_empty() {
                                    let (afi, safi) =
                                        AfiSafiType::Ipv6Unicast.into();
                                    mp_unreach_nlri.put_u16(afi);
                                    mp_unreach_nlri.put_u8(safi);
                                    //mp_reach_nlri.put_u16(AfiSafiType::Ipv6Unicast.into());
                                    //mp_reach_nlri
                                    //    .put_u8(u8::from(AfiSafiType::Unicast));
                                    if let NextHop::Unicast(IpAddr::V6(addr))
                                    | NextHop::Ipv6LL{global: addr, ..}
                                    | NextHop::Multicast(IpAddr::V6(
                                        addr,
                                    )) = next_hop.0
                                    {
                                        mp_reach_nlri.put_u8(addr.octets().len() as u8);
                                        mp_reach_nlri.extend_from_slice(
                                            &addr.octets(),
                                        );
                                    } else {
                                        unreachable!();
                                    }
                                    mp_reach_nlri.put_u8(0u8); // reserved
                                }
                                mp_reach_nlri.extend_from_slice(&[len]);
                                if len > 0 {
                                    let min_bytes = div_ceil(len, 8) as usize;
                                    mp_reach_nlri.extend_from_slice(
                                        &addr.octets()[..min_bytes],
                                    );
                                }
                            }
                        }
                    }

                    if !mp_reach_nlri.is_empty() {
                        push_attributes(
                            &mut path_attributes,
                            PathAttributeType::MpReachNlri,
                            &mp_reach_nlri,
                        );
                    }

                    let num_path_attribute_bytes = u16::try_from(
                        path_attributes.len() + extra_path_attributes.len(),
                    )
                    .unwrap();
                    buf.extend_from_slice(
                        &num_path_attribute_bytes.to_be_bytes(),
                    ); // N path attribute bytes
                    buf.extend_from_slice(&path_attributes);
                    buf.extend_from_slice(extra_path_attributes);

                    if !announced_routes.is_empty() {
                        buf.extend_from_slice(&announced_routes); // the announced routes
                    }
                }
            }

            // Finalize BGP message
            finalize_bgp_msg_len(&mut buf);
            buf.freeze()
        }

        pub fn mk_peer_down_notification_msg(
            per_peer_header: &PerPeerHeader,
        ) -> Bytes {
            let mut buf = BytesMut::new();
            push_bmp_common_header(
                &mut buf,
                MessageType::PeerDownNotification,
            );
            push_bmp_per_peer_header(&mut buf, per_peer_header);

            // 4.9.  Peer Down Notification
            //
            //  0                   1                   2                   3
            //  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
            // +-+-+-+-+-+-+-+-+
            // |    Reason     |
            // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            // |            Data (present if Reason = 1, 2 or 3)               |
            // ~                                                               ~
            // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            //
            // From: https://www.rfc-editor.org/rfc/rfc7854.html#section-4.9

            buf.extend_from_slice(&5u8.to_be_bytes()); // reason code 5

            finalize_bmp_msg_len(&mut buf);
            buf.freeze()
        }

        pub fn mk_statistics_report_msg(
            per_peer_header: &PerPeerHeader,
        ) -> Bytes {
            // 4.8.  Stats Reports
            //
            // "Following the common BMP header and per-peer header is a 4-byte field
            //  that indicates the number of counters in the stats message where each
            //  counter is encoded as a TLV."
            //
            //     0                   1                   2                   3
            //     0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
            //    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            //    |                        Stats Count                            |
            //    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            //
            //  Each counter is encoded as follows:
            //
            //     0                   1                   2                   3
            //     0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
            //    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            //    |         Stat Type             |          Stat Len             |
            //    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            //    |                        Stat Data                              |
            //    ~                                                               ~
            //    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            //
            //  o  Stat Type (2 bytes): Defines the type of the statistic carried in
            //     the Stat Data field.
            //
            //  o  Stat Len (2 bytes): Defines the length of the Stat Data field.
            //
            // From: https://www.rfc-editor.org/rfc/rfc7854.html#section-4.8

            let mut buf = BytesMut::new();
            push_bmp_common_header(&mut buf, MessageType::StatisticsReport);
            push_bmp_per_peer_header(&mut buf, per_peer_header);

            buf.extend_from_slice(&0u32.to_be_bytes()); // zero stats

            finalize_bmp_msg_len(&mut buf);
            buf.freeze()
        }

        pub fn mk_termination_msg() -> Bytes {
            let mut buf = BytesMut::new();
            push_bmp_common_header(&mut buf, MessageType::TerminationMessage);

            // 4.5. Termination Message
            //
            // "The termination message consists of the common BMP header followed by
            //  one or more TLVs containing information about the reason for the
            //  termination, as follows:"
            //
            // From: https://www.rfc-editor.org/rfc/rfc7854.html#section-4.5

            // 4.4 Information TLV
            //
            //  0                   1                   2                   3
            //  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
            // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            // |          Information Type     |       Information Length      |
            // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            // |                 Information (variable)                        |
            // ~                                                               ~
            // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            //
            // From: https://www.rfc-editor.org/rfc/rfc7854.html#section-4.4

            push_bmp_termination_tlv(
                &mut buf,
                TerminationInformation::AdminClose,
                &[0u8, 0u8],
            );

            finalize_bmp_msg_len(&mut buf);
            buf.freeze()
        }

        fn finalize_bmp_msg_len(buf: &mut BytesMut) {
            let len_bytes: [u8; 4] = (buf.len() as u32).to_be_bytes();
            buf[1] = len_bytes[0];
            buf[2] = len_bytes[1];
            buf[3] = len_bytes[2];
            buf[4] = len_bytes[3];
        }

        fn finalize_bgp_msg_len(buf: &mut BytesMut) {
            assert!(buf.len() >= 19);
            assert!(buf.len() <= 4096);

            let len_bytes: [u8; 2] = (buf.len() as u16).to_be_bytes();
            buf[16] = len_bytes[0];
            buf[17] = len_bytes[1];
        }

        fn push_bmp_common_header(buf: &mut BytesMut, msg_type: MessageType) {
            //  0                   1                   2                   3
            //  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
            // +-+-+-+-+-+-+-+-+
            // |    Version    |
            // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            // |                        Message Length                         |
            // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            // |   Msg. Type   |
            // +---------------+
            //
            // From: https://datatracker.ietf.org/doc/html/rfc4271#section-4.1
            buf.extend_from_slice(&[3u8]); // version 3
            buf.resize(buf.len() + 4, 0u8); // placeholder length, to be replaced later
            buf.extend_from_slice(&u8::from(msg_type).to_be_bytes());
        }

        #[derive(Debug, PartialEq, Eq)]
        pub struct PerPeerHeader {
            pub peer_type: MyPeerType,
            pub peer_flags: u8,
            pub peer_distinguisher: [u8; 8],
            pub peer_address: IpAddr,
            pub peer_as: Asn,
            pub peer_bgp_id: [u8; 4],
        }

        impl PerPeerHeader {
            fn is_ipv4(&self) -> bool {
                self.peer_flags & 0x80 == 0
            }

            fn is_ipv6(&self) -> bool {
                self.peer_flags & 0x80 == 0x80
            }
        }

        pub fn mk_per_peer_header(
            peer_ip: &str,
            peer_as: u32,
        ) -> PerPeerHeader {
            PerPeerHeader {
                peer_type: PeerType::GlobalInstance.into(),
                peer_flags: 0,
                peer_distinguisher: [0u8; 8],
                peer_address: peer_ip.parse().unwrap(),
                peer_as: Asn::from_u32(peer_as),
                peer_bgp_id: [1u8, 2u8, 3u8, 4u8],
            }
        }

        fn push_bmp_per_peer_header(buf: &mut BytesMut, pph: &PerPeerHeader) {
            //  0                   1                   2                   3
            //  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
            // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            // |   Peer Type   |  Peer Flags   |
            // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            // |         Peer Distinguisher (present based on peer type)       |
            // |                                                               |
            // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            // |                 Peer Address (16 bytes)                       |
            // ~                                                               ~
            // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            // |                           Peer AS                             |
            // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            // |                         Peer BGP ID                           |
            // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            // |                    Timestamp (seconds)                        |
            // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            // |                  Timestamp (microseconds)                     |
            // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            //
            // From: https://www.rfc-editor.org/rfc/rfc7854.html#section-4.2

            // "Timestamp: The time when the encapsulated routes were received (one
            //  may also think of this as the time when they were installed in the
            //  Adj-RIB-In), expressed in seconds and microseconds since midnight
            //  (zero hour), January 1, 1970 (UTC).  If zero, the time is
            //  unavailable.  Precision of the timestamp is implementation-dependent."
            //
            // From: https://www.rfc-editor.org/rfc/rfc7854.html#section-4.2
            let now = Utc::now();
            let epoch_seconds = u32::try_from(now.timestamp()).unwrap();
            let epoch_micros = now.timestamp_subsec_micros();

            buf.put_u8(u8::from(*pph.peer_type));
            buf.put_u8(pph.peer_flags);
            buf.extend_from_slice(&pph.peer_distinguisher);

            // "Peer Address: The remote IP address associated with the TCP session
            //  over which the encapsulated PDU was received.  It is 4 bytes long if
            //  an IPv4 address is carried in this field (with the 12 most significant
            //  bytes zero-filled) and 16 bytes long if an IPv6 address is carried in
            //  this field."
            //
            // From: https://www.rfc-editor.org/rfc/rfc7854.html#section-4.2
            match pph.peer_address {
                IpAddr::V4(addr) => {
                    buf.resize(buf.len() + 12, 0u8);
                    buf.extend_from_slice(&addr.octets());
                }
                IpAddr::V6(addr) => {
                    buf.extend_from_slice(&addr.octets());
                }
            }

            buf.extend_from_slice(&pph.peer_as.into_u32().to_be_bytes()); // assumes 32-bit ASN
            buf.extend_from_slice(&pph.peer_bgp_id);
            buf.extend_from_slice(&epoch_seconds.to_be_bytes());
            buf.extend_from_slice(&epoch_micros.to_be_bytes());
        }

        fn push_bmp_information_tlv(
            buf: &mut BytesMut,
            tlv_type: InformationTlvType,
            tlv_value: &[u8],
        ) {
            //  0                   1                   2                   3
            //  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
            // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            // |          Information Type     |       Information Length      |
            // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            // |                 Information (variable)                        |
            // ~                                                               ~
            // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            //
            // From: https://www.rfc-editor.org/rfc/rfc7854.html#section-4.4
            buf.extend_from_slice(&information_tlv_type_to_be_bytes(
                tlv_type,
            ));
            buf.extend_from_slice(&(tlv_value.len() as u16).to_be_bytes());
            buf.extend_from_slice(tlv_value);
        }

        fn push_bmp_termination_tlv(
            buf: &mut BytesMut,
            tlv_type: TerminationInformation,
            tlv_value: &[u8],
        ) {
            //  0                   1                   2                   3
            //  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
            // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            // |          Information Type     |       Information Length      |
            // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            // |                 Information (variable)                        |
            // ~                                                               ~
            // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            //
            // From: https://www.rfc-editor.org/rfc/rfc7854.html#section-4.4
            buf.extend_from_slice(&termination_tlv_type_to_be_bytes(
                tlv_type,
            ));
            buf.extend_from_slice(&(tlv_value.len() as u16).to_be_bytes());
            buf.extend_from_slice(tlv_value);
        }

        fn information_tlv_type_to_be_bytes(
            typ: InformationTlvType,
        ) -> [u8; 2] {
            match typ {
                InformationTlvType::String => 0u16.to_be_bytes(),
                InformationTlvType::SysDesc => 1u16.to_be_bytes(),
                InformationTlvType::SysName => 2u16.to_be_bytes(),
                _ => unreachable!(),
            }
        }

        fn termination_tlv_type_to_be_bytes(
            typ: TerminationInformation,
        ) -> [u8; 2] {
            match typ {
                TerminationInformation::CustomString(_) => 0u16.to_be_bytes(),
                TerminationInformation::AdminClose
                | TerminationInformation::Unspecified
                | TerminationInformation::OutOfResources
                | TerminationInformation::RedundantConnection
                | TerminationInformation::PermAdminClose => {
                    1u16.to_be_bytes()
                }
                TerminationInformation::Undefined(_) => unreachable!(),
            }
        }

        #[derive(Debug, PartialEq, Eq)]
        pub struct MyPeerType(PeerType);

        impl From<PeerType> for MyPeerType {
            fn from(peer_type: PeerType) -> Self {
                MyPeerType(peer_type)
            }
        }

        impl Deref for MyPeerType {
            type Target = PeerType;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl FromStr for MyPeerType {
            type Err = anyhow::Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                let peer_type = match s {
                    "global" => PeerType::GlobalInstance,
                    "local" => PeerType::LocalInstance,
                    "localrib" => PeerType::LocalRibInstance,
                    "rd" => PeerType::RdInstance,
                    _ => todo!(),
                };

                Ok(MyPeerType(peer_type))
            }
        }

        #[derive(Default)]
        pub struct Prefixes(Vec<Prefix>);

        impl Prefixes {
            pub fn new(prefixes: Vec<Prefix>) -> Self {
                Self(prefixes)
            }
        }

        impl Deref for Prefixes {
            type Target = Vec<Prefix>;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl FromStr for Prefixes {
            type Err = anyhow::Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match s.to_lowercase().as_str() {
                    "" | "none" => Ok(Prefixes(vec![])),

                    _ => {
                        let mut prefixes = Vec::new();
                        for prefix_str in s.split(',') {
                            prefixes.push(prefix_str.parse()?);
                        }
                        Ok(Prefixes(prefixes))
                    }
                }
            }
        }

        // Based on `div_ceil()` from Rust nightly.
        pub const fn div_ceil(lhs: u8, rhs: u8) -> u8 {
            let d = lhs / rhs;
            let r = lhs % rhs;
            if r > 0 && rhs > 0 {
                d + 1
            } else {
                d
            }
        }

        pub struct MyOriginType(OriginType);

        impl Deref for MyOriginType {
            type Target = OriginType;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl FromStr for MyOriginType {
            type Err = anyhow::Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match s {
                    "i" => Ok(Self(OriginType::Igp)),
                    "e" => Ok(Self(OriginType::Egp)),
                    "?" => Ok(Self(OriginType::Incomplete)),
                    _ => Ok(s.parse::<u8>()?.into()),
                }
            }
        }

        impl From<u8> for MyOriginType {
            fn from(v: u8) -> Self {
                let origin_type = match v {
                    0 => OriginType::Igp,
                    1 => OriginType::Egp,
                    2 => OriginType::Incomplete,
                    _ => OriginType::Unimplemented(v),
                };
                Self(origin_type)
            }
        }

        impl From<&MyOriginType> for u8 {
            fn from(v: &MyOriginType) -> Self {
                match v {
                    MyOriginType(OriginType::Igp) => 0,
                    MyOriginType(OriginType::Egp) => 1,
                    MyOriginType(OriginType::Incomplete) => 2,
                    MyOriginType(OriginType::Unimplemented(v)) => *v,
                }
            }
        }

        pub struct MyAsPath(HopPath);

        impl Deref for MyAsPath {
            type Target = HopPath;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl FromStr for MyAsPath {
            type Err = anyhow::Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                let mut hop_path = HopPath::new();
                if s.starts_with('[') && s.ends_with(']') {
                    let s = &s[1..s.len() - 1];
                    for asn in s.split(',') {
                        let asn: Asn = asn.parse()?;
                        hop_path.append(asn);
                    }
                    Ok(Self(hop_path))
                } else {
                    Err(anyhow::anyhow!("Expected [asn, ...]"))
                }
            }
        }

        pub struct MyNextHop(NextHop);

        impl Deref for MyNextHop {
            type Target = NextHop;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl FromStr for MyNextHop {
            type Err = anyhow::Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                let ip_addr: IpAddr = s.parse()?;
                Ok(MyNextHop(NextHop::Unicast(ip_addr)))
            }
        }

        pub struct MyCommunities(Vec<Community>);

        impl Deref for MyCommunities {
            type Target = Vec<Community>;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl FromStr for MyCommunities {
            type Err = anyhow::Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match s.to_lowercase().as_str() {
                    "" | "none" => Ok(MyCommunities(vec![])),

                    _ => {
                        let mut communities = Vec::new();
                        for community_str in s.split(',') {
                            communities.push(community_str.parse().unwrap());
                        }
                        Ok(MyCommunities(communities))
                    }
                }
            }
        }

        #[derive(Default)]
        pub enum Announcements {
            #[default]
            None,
            Some {
                origin: MyOriginType,
                as_path: MyAsPath,
                next_hop: MyNextHop,
                communities: MyCommunities,
                prefixes: Prefixes,
            },
        }

        impl FromStr for Announcements {
            type Err = anyhow::Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match s.to_lowercase().as_str() {
                    "" | "none" => Ok(Self::None),

                    _ => {
                        let parts: Vec<&str> = s.splitn(5, ' ').collect();
                        assert_eq!(parts.len(), 5);
                        let origin = parts[0].parse().unwrap();
                        let as_path = parts[1].parse().unwrap();
                        let next_hop = parts[2].parse().unwrap();
                        let communities = parts[3].parse().unwrap();
                        let prefixes = parts[4].parse().unwrap();
                        Ok(Self::Some {
                            origin,
                            as_path,
                            next_hop,
                            communities,
                            prefixes,
                        })
                    }
                }
            }
        }
    }
}

#[cfg(test)]
pub mod net {
    use std::{future::Future, net::SocketAddr, sync::Arc};

    use tokio::net::TcpStream;

    use crate::common::net::{
        TcpListener, TcpListenerFactory, TcpStreamWrapper,
    };

    /// A mock TcpListenerFactory that stores a callback supplied by the
    /// unit test thereby allowing the unit test to determine if binding to
    /// the given address should succeed or not, and on success delegates to
    /// MockTcpListener.
    pub struct MockTcpListenerFactory<T, U, Fut>
    where
        T: Fn(String) -> std::io::Result<MockTcpListener<U, Fut>>,
        U: Fn() -> Fut,
        Fut: Future<
            Output = std::io::Result<(MockTcpStreamWrapper, SocketAddr)>,
        >,
    {
        pub bind_cb: T,
        pub binds: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl<T, U, Fut> MockTcpListenerFactory<T, U, Fut>
    where
        T: Fn(String) -> std::io::Result<MockTcpListener<U, Fut>>,
        U: Fn() -> Fut,
        Fut: Future<
            Output = std::io::Result<(MockTcpStreamWrapper, SocketAddr)>,
        >,
    {
        pub fn new(bind_cb: T) -> Self {
            Self {
                bind_cb,
                binds: Arc::default(),
            }
        }
    }

    #[async_trait::async_trait]
    impl<T, U, Fut> TcpListenerFactory<MockTcpListener<U, Fut>>
        for MockTcpListenerFactory<T, U, Fut>
    where
        T: Fn(String) -> std::io::Result<MockTcpListener<U, Fut>>
            + std::marker::Sync,
        U: Fn() -> Fut,
        Fut: Future<
            Output = std::io::Result<(MockTcpStreamWrapper, SocketAddr)>,
        >,
    {
        async fn bind(
            &self,
            addr: String,
        ) -> std::io::Result<MockTcpListener<U, Fut>> {
            let listener = (self.bind_cb)(addr.clone())?;
            self.binds.lock().unwrap().push(addr);
            Ok(listener)
        }
    }

    /// A mock TcpListener that stores a callback supplied by the unit test
    /// thereby allowing the unit test to determine if accepting incoming
    /// connections should appear to succeed or fail, and on success delegates
    /// to MockTcpStreamWrapper.
    pub struct MockTcpListener<T, Fut>(T)
    where
        T: Fn() -> Fut,
        Fut: Future<
            Output = std::io::Result<(MockTcpStreamWrapper, SocketAddr)>,
        >;

    impl<T, Fut> MockTcpListener<T, Fut>
    where
        T: Fn() -> Fut,
        Fut: Future<
            Output = std::io::Result<(MockTcpStreamWrapper, SocketAddr)>,
        >,
    {
        pub fn new(listen_cb: T) -> Self {
            Self(listen_cb)
        }
    }

    #[async_trait::async_trait]
    impl<Fut, T> TcpListener<MockTcpStreamWrapper> for MockTcpListener<T, Fut>
    where
        T: Fn() -> Fut + Sync + Send,
        Fut: Future<
                Output = std::io::Result<(MockTcpStreamWrapper, SocketAddr)>,
            > + Send,
    {
        async fn accept(
            &self,
        ) -> std::io::Result<(MockTcpStreamWrapper, SocketAddr)> {
            self.0().await
        }
    }

    /// A mock TcpStreamWraper that is not actually usable, but can be passed
    /// in place of a StandardTcpStream in order to avoid needing to create a
    /// real TcpStream which would interact with the actual operating system
    /// network stack.
    pub struct MockTcpStreamWrapper;

    impl TcpStreamWrapper for MockTcpStreamWrapper {
        fn into_inner(self) -> std::io::Result<TcpStream> {
            Err(std::io::ErrorKind::Unsupported.into())
        }
    }
}

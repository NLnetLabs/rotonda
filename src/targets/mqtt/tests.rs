use std::{net::IpAddr, str::FromStr, sync::Arc};

use bytes::Bytes;
use roto::types::{
    builtin::{
        BgpUpdateMessage, BuiltinTypeValue, RawRouteWithDeltas, RotondaId,
        RouteStatus, UpdateMessage,
    },
    collections::{BytesRecord, Record},
    outputs::OutputStreamMessage,
    typedef::TypeDef,
    typevalue::TypeValue,
};
use routecore::{
    addr::Prefix,
    asn::Asn,
    bgp::message::SessionConfig,
    bmp::message::{Message as BmpMsg, PeerType},
};
use serde_json::json;

use crate::{
    bgp::encode::{
        mk_bgp_update, mk_initiation_msg, mk_route_monitoring_msg,
        Announcements, MyPeerType, PerPeerHeader, Prefixes,
    },
    manager::Component,
    payload::{Payload, SourceId},
    tests::util::assert_json_eq,
};

use super::{config::Config, target::*};

#[test]
fn server_host_config_setting_must_be_provided() {
    let empty = r#""#;
    let empty_destination = r#"destination = """#;
    let destination_with_host_only = r#"destination = "some_host_name""#;
    let destination_with_host_and_invalid_port =
        r#"destination = "some_host_name:invalid_port""#;
    let destination_with_host_and_port =
        r#"destination = "some_host_name:12345""#;

    assert!(mk_config_from_toml(empty).is_err());
    assert!(mk_config_from_toml(empty_destination).is_err());
    assert!(mk_config_from_toml(destination_with_host_only).is_ok());
    assert!(
        mk_config_from_toml(destination_with_host_and_invalid_port).is_err()
    );
    assert!(mk_config_from_toml(destination_with_host_and_port).is_ok());
}

#[test]
fn generate_correct_json_for_publishing_from_output_stream_roto_type_value() {
    // Given an MQTT target runner
    let runner = mk_mqtt_runner();

    // And a payload that should be published
    let output_stream = mk_roto_output_stream_payload();

    // Then the candidate should be selected for publication
    let SenderMsg { content, topic, .. } =
        runner.output_stream_message_to_msg(output_stream).unwrap();

    // And the topic should be based on the rouuter id recorded with the route, if any
    assert_eq!(topic, "rotonda/my-topic");

    // And the produced message to be published should match the expected JSON format
    let expected_json = json!({
        "some-asn": 1818,
        "some-str": "some-value",
    });

    let actual_json = serde_json::from_str(&content).unwrap();
    assert_json_eq(actual_json, expected_json);
}

// --- Test helpers -----------------------------------------------------------------------------------------------

fn mk_mqtt_runner() -> MqttRunner {
    let config = Config {
        topic_template: Config::default_topic_template(),
        ..Default::default()
    };
    MqttRunner::new(config, Component::default())
}

fn mk_config_from_toml(toml: &str) -> Result<Config, toml::de::Error> {
    toml::from_str::<Config>(toml)
}

fn mk_raw_bmp_payload(bmp_bytes: Bytes) -> Payload {
    let source_id = SourceId::SocketAddr("10.0.0.1:1818".parse().unwrap());
    let bmp_msg = BmpMsg::from_octets(bmp_bytes).unwrap();
    let bmp_msg = Arc::new(BytesRecord(bmp_msg));
    let value = TypeValue::Builtin(BuiltinTypeValue::BmpMessage(bmp_msg));
    Payload::new(source_id, value, None)
}

fn mk_raw_route_with_deltas_payload(prefix: Prefix) -> Payload {
    let bytes = bgp_route_announce(prefix);
    let update_msg = UpdateMessage::new(bytes, SessionConfig::modern());
    let delta_id = (RotondaId(0), 0);
    let bgp_update_msg =
        Arc::new(BgpUpdateMessage::new(delta_id, update_msg));
    let route = RawRouteWithDeltas::new_with_message_ref(
        (RotondaId(0), 0),
        prefix.into(),
        &bgp_update_msg,
        RouteStatus::InConvergence,
    )
    .with_peer_asn("AS1818".parse().unwrap())
    .with_peer_ip("4.5.6.7".parse().unwrap())
    .with_router_id("test-router".to_string().into());

    let value = TypeValue::Builtin(BuiltinTypeValue::Route(route));
    Payload::new("test", value, None)
}

fn mk_roto_output_stream_payload() -> Arc<OutputStreamMessage> {
    let typedef = TypeDef::new_record_type(vec![
        ("name", Box::new(TypeDef::StringLiteral)),
        ("topic", Box::new(TypeDef::StringLiteral)),
        ("some-str", Box::new(TypeDef::StringLiteral)),
        ("some-asn", Box::new(TypeDef::Asn)),
    ])
    .unwrap();

    let fields = vec![
        ("name", "MOCK".into()),
        ("topic", "my-topic".into()),
        ("some-str", "some-value".into()),
        ("some-asn", routecore::asn::Asn::from_u32(1818).into()),
    ];
    let record = Record::create_instance_with_sort(&typedef, fields).unwrap();
    Arc::new(OutputStreamMessage::from(record))
}

fn bmp_initiate() -> Bytes {
    mk_initiation_msg("test-router", "Mock BMP router")
}

fn bmp_peer_up_notification() -> Bytes {
    crate::bgp::encode::mk_peer_up_notification_msg(
        &mk_per_peer_header(),
        "10.0.0.1".parse().unwrap(),
        11019,
        4567,
        111,
        222,
        0,
        0,
        vec![],
        false,
    )
}

fn bmp_peer_down_notification() -> Bytes {
    crate::bgp::encode::mk_peer_down_notification_msg(&mk_per_peer_header())
}

fn bmp_route_announce(prefix: Prefix) -> Bytes {
    let per_peer_header = mk_per_peer_header();
    let withdrawals = Prefixes::default();
    let announcements = Announcements::from_str(&format!(
        "e [123,456] 10.0.0.1 BLACKHOLE,rt:34:54536,AS34:256:512 {}",
        prefix
    ))
    .unwrap();

    mk_route_monitoring_msg(
        &per_peer_header,
        &withdrawals,
        &announcements,
        &[],
    )
}

fn bgp_route_announce(prefix: Prefix) -> Bytes {
    let withdrawals = Prefixes::default();
    let announcements = Announcements::from_str(&format!(
        "e [123,456] 10.0.0.1 BLACKHOLE,rt:34:54536,AS34:256:512 {}",
        prefix
    ))
    .unwrap();
    mk_bgp_update(&withdrawals, &announcements, &[])
}

fn mk_per_peer_header() -> PerPeerHeader {
    let peer_type: MyPeerType = PeerType::GlobalInstance.into();
    let peer_flags: u8 = 0;
    let peer_address: IpAddr = IpAddr::from_str("10.0.0.1").unwrap();
    let peer_as: Asn = Asn::from_u32(12345);
    let peer_bgp_id = 0u32.to_be_bytes();
    let peer_distinguisher: [u8; 8] = [0; 8];

    PerPeerHeader {
        peer_type,
        peer_flags,
        peer_distinguisher,
        peer_address,
        peer_as,
        peer_bgp_id,
    }
}

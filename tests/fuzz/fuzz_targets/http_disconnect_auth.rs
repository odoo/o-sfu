#![no_main]

//! Fuzz target for the HTTP auth and forwarded-header boundary.
//!
//! The `/v1/room` and `/v1/disconnect` routes both verify route-specific JWT
//! claims, and the room route also derives base URL and remote address from
//! proxy-aware forwarding headers.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use http::{HeaderMap, HeaderValue, header};
use libfuzzer_sys::{arbitrary, arbitrary::Arbitrary, fuzz_target};
use o_sfu::{
    auth::{HttpDisconnectClaims, HttpRoomClaims, verify},
    http::resolve_request_origin,
};
use secrecy::SecretString;

const TEST_AUTH_KEY: &str = "u6bsUQEWrHdKIuYplirRnbBmLbrKV5PxKG7DtA71mng=";

#[derive(Debug, Arbitrary)]
struct HttpRouteInput {
    token: String,
    host: Option<String>,
    forwarded_host: Option<String>,
    forwarded_proto: Option<String>,
    forwarded_for: Option<String>,
    trust_proxy_headers: bool,
    connect_info: Option<SocketAddrInput>,
}

#[derive(Debug, Clone, Copy, Arbitrary)]
struct SocketAddrInput {
    ip: [u8; 4],
    port: u16,
}

impl SocketAddrInput {
    fn into_socket_addr(self) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::from(self.ip)), self.port)
    }
}

fn insert_header(headers: &mut HeaderMap, name: &'static str, value: Option<&str>) {
    let Some(value) = value else {
        return;
    };
    let Ok(value) = HeaderValue::from_str(value) else {
        return;
    };
    headers.insert(name, value);
}

fuzz_target!(|input: HttpRouteInput| {
    let _ = verify::<HttpRoomClaims>(&input.token, &SecretString::from(TEST_AUTH_KEY));
    let _ = verify::<HttpDisconnectClaims>(&input.token, &SecretString::from(TEST_AUTH_KEY));
    let mut headers = HeaderMap::new();
    insert_header(&mut headers, header::HOST.as_str(), input.host.as_deref());
    insert_header(
        &mut headers,
        "x-forwarded-host",
        input.forwarded_host.as_deref(),
    );
    insert_header(
        &mut headers,
        "x-forwarded-proto",
        input.forwarded_proto.as_deref(),
    );
    insert_header(
        &mut headers,
        "x-forwarded-for",
        input.forwarded_for.as_deref(),
    );
    let _ = resolve_request_origin(
        &headers,
        input.trust_proxy_headers,
        SocketAddr::from(([127, 0, 0, 1], 8070)),
        input.connect_info.map(SocketAddrInput::into_socket_addr),
    );
});

use std::{
    convert::Infallible,
    net::{IpAddr, SocketAddr},
};

use axum::{
    extract::{ConnectInfo, FromRequestParts},
    http::{HeaderMap, header, request::Parts, uri::Authority},
};
use ipnet::IpNet;

use crate::runtime::RuntimeState;

/// Proxy-aware request origin derived by the HTTP edge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestOrigin {
    pub base_url: String,
    pub remote_address: Option<IpAddr>,
}

impl FromRequestParts<RuntimeState> for RequestOrigin {
    type Rejection = Infallible;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &RuntimeState,
    ) -> Result<Self, Self::Rejection> {
        let connect_info = parts
            .extensions
            .get::<ConnectInfo<SocketAddr>>()
            .map(|ConnectInfo(addr)| *addr);
        Ok(resolve_request_origin(
            &parts.headers,
            state.config.http.trust_proxy_headers,
            &state.config.http.trusted_proxies,
            state.config.http.bind_address,
            connect_info,
        ))
    }
}

/// Resolves forwarded metadata only for TCP peers in `trusted_proxies` with proxy mode enabled.
///
/// Forwarded addresses must all parse as IP addresses. The rightmost untrusted
/// address identifies the client, after skipping trusted proxy hops. Missing,
/// malformed or entirely trusted chains fall back to the TCP peer. IPv4-mapped
/// IPv6 addresses use their IPv4 identity and require an IPv4 trusted CIDR.
/// Without a peer, forwarding is disabled.
///
/// The trusted edge must overwrite forwarded host and protocol with single
/// values. Invalid or repeated values fall back to Host and HTTP respectively.
/// Address traversal follows [NGINX's recursive trust rule](https://nginx.org/en/docs/http/ngx_http_realip_module.html#real_ip_recursive).
#[must_use]
pub fn resolve_request_origin(
    headers: &HeaderMap,
    trust_proxy_headers: bool,
    trusted_proxies: &[IpNet],
    fallback_bind_address: SocketAddr,
    connect_info: Option<SocketAddr>,
) -> RequestOrigin {
    let peer = connect_info.map(|addr| addr.ip().to_canonical());
    let trust_headers = trust_proxy_headers
        && peer.is_some_and(|address| is_trusted_proxy(address, trusted_proxies));
    let remote_address = trust_headers
        .then(|| forwarded_client_address(headers, trusted_proxies))
        .flatten()
        .or(peer);
    let scheme = trust_headers
        .then(|| single_header(headers, "x-forwarded-proto"))
        .flatten()
        .filter(|scheme| matches!(*scheme, "http" | "https"))
        .unwrap_or("http");
    let host = trust_headers
        .then(|| single_header(headers, "x-forwarded-host"))
        .flatten()
        .and_then(valid_authority)
        .or_else(|| single_header(headers, header::HOST.as_str()).and_then(valid_authority))
        .map_or_else(
            || fallback_bind_address.to_string(),
            |host| host.to_string(),
        );
    RequestOrigin {
        base_url: format!("{scheme}://{host}"),
        remote_address,
    }
}

fn is_trusted_proxy(address: IpAddr, trusted_proxies: &[IpNet]) -> bool {
    // Canonical IPv4 identity must not inherit trust from an IPv6-wide CIDR.
    trusted_proxies
        .iter()
        .any(|network| network.contains(&address))
}

fn forwarded_client_address(headers: &HeaderMap, trusted_proxies: &[IpNet]) -> Option<IpAddr> {
    let mut client = None;
    // Validate the entire chain even after finding an untrusted hop. Accepting
    // a valid suffix of malformed input would give it a different trust meaning.
    for value in headers.get_all("x-forwarded-for") {
        for item in value.to_str().ok()?.split(',') {
            let address = item.trim().parse::<IpAddr>().ok()?.to_canonical();
            if !is_trusted_proxy(address, trusted_proxies) {
                client = Some(address);
            }
        }
    }
    client
}

fn valid_authority(host: &str) -> Option<Authority> {
    host.parse::<Authority>()
        .ok()
        .filter(|host| !host.as_str().contains('@'))
}

fn single_header<'headers>(headers: &'headers HeaderMap, name: &str) -> Option<&'headers str> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?.to_str().ok()?.trim();
    (values.next().is_none() && !value.is_empty() && !value.contains(',')).then_some(value)
}

#[cfg(test)]
#[path = "TESTS/request_origin.rs"]
mod tests;

use axum::http::{HeaderMap, HeaderValue, header};

use super::resolve_request_origin;

#[test]
fn untrusted_peer_cannot_supply_forwarded_metadata() -> anyhow::Result<()> {
    let mut headers = forwarded_headers("198.51.100.7")?;
    headers.insert(header::HOST, HeaderValue::from_static("direct.example"));
    for peer in [Some("203.0.113.10:1234".parse()?), None] {
        let origin = resolve_request_origin(
            &headers,
            true,
            &["127.0.0.1/32".parse()?],
            "127.0.0.1:8070".parse()?,
            peer,
        );
        assert_eq!(origin.base_url, "http://direct.example");
        assert_eq!(origin.remote_address, peer.map(|peer| peer.ip()));
    }
    Ok(())
}

#[test]
fn trusted_chain_selects_rightmost_untrusted_address() -> anyhow::Result<()> {
    let mut headers = forwarded_headers("192.0.2.1, 198.51.100.7")?;
    headers.append("x-forwarded-for", HeaderValue::from_static("10.0.0.1"));
    let origin = resolve_request_origin(
        &headers,
        true,
        &["127.0.0.1/32".parse()?, "10.0.0.0/8".parse()?],
        "127.0.0.1:8070".parse()?,
        Some("127.0.0.1:1234".parse()?),
    );
    assert_eq!(origin.remote_address, Some("198.51.100.7".parse()?));
    assert_eq!(origin.base_url, "https://public.example");
    Ok(())
}

#[test]
fn invalid_missing_or_entirely_trusted_chain_falls_back_to_peer() -> anyhow::Result<()> {
    for chain in [
        None,
        Some(""),
        Some("garbage, 198.51.100.7"),
        Some("198.51.100.7, garbage"),
        Some("198.51.100.7,"),
        Some("10.0.0.1, 127.0.0.1"),
    ] {
        let mut headers = HeaderMap::new();
        if let Some(chain) = chain {
            headers.insert("x-forwarded-for", chain.parse()?);
        }
        let origin = resolve_request_origin(
            &headers,
            true,
            &["127.0.0.1/32".parse()?, "10.0.0.0/8".parse()?],
            "127.0.0.1:8070".parse()?,
            Some("127.0.0.1:1234".parse()?),
        );
        assert_eq!(
            origin.remote_address,
            Some("127.0.0.1".parse()?),
            "{chain:?}"
        );
    }
    Ok(())
}

#[test]
fn mapped_addresses_share_ipv4_trust_and_identity() -> anyhow::Result<()> {
    let headers = forwarded_headers("::ffff:198.51.100.7, ::ffff:10.0.0.1")?;
    let origin = resolve_request_origin(
        &headers,
        true,
        &["127.0.0.1/32".parse()?, "10.0.0.0/8".parse()?],
        "127.0.0.1:8070".parse()?,
        Some("[::ffff:127.0.0.1]:1234".parse()?),
    );
    assert_eq!(origin.remote_address, Some("198.51.100.7".parse()?));
    assert_eq!(origin.base_url, "https://public.example");
    Ok(())
}

#[test]
fn forwarded_authority_requires_valid_single_values() -> anyhow::Result<()> {
    for (name, value) in [
        ("x-forwarded-host", "evil.example/path"),
        ("x-forwarded-host", "user@evil.example"),
        ("x-forwarded-host", "evil.example, public.example"),
        ("x-forwarded-proto", "javascript"),
        ("x-forwarded-proto", "https, http"),
    ] {
        let mut headers = forwarded_headers("198.51.100.7")?;
        headers.insert(header::HOST, HeaderValue::from_static("direct.example"));
        headers.insert(name, value.parse()?);
        let origin = resolve_request_origin(
            &headers,
            true,
            &["127.0.0.1/32".parse()?],
            "127.0.0.1:8070".parse()?,
            Some("127.0.0.1:1234".parse()?),
        );
        let expected = if name == "x-forwarded-host" {
            "https://direct.example"
        } else {
            "http://public.example"
        };
        assert_eq!(origin.base_url, expected, "{name}: {value}");
    }
    Ok(())
}

fn forwarded_headers(chain: &str) -> anyhow::Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    headers.insert("x-forwarded-for", chain.parse()?);
    headers.insert(
        "x-forwarded-host",
        HeaderValue::from_static("public.example"),
    );
    headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));
    Ok(headers)
}

#[test]
fn repeated_forwarded_authority_headers_fall_back_independently() -> anyhow::Result<()> {
    let mut headers = forwarded_headers("198.51.100.7")?;
    headers.insert(header::HOST, HeaderValue::from_static("direct.example"));
    headers.append(
        "x-forwarded-host",
        HeaderValue::from_static("second.example"),
    );
    headers.append("x-forwarded-proto", HeaderValue::from_static("https"));
    let origin = resolve_request_origin(
        &headers,
        true,
        &["127.0.0.1/32".parse()?],
        "127.0.0.1:8070".parse()?,
        Some("127.0.0.1:1234".parse()?),
    );
    assert_eq!(origin.base_url, "http://direct.example");
    assert_eq!(origin.remote_address, Some("198.51.100.7".parse()?));
    Ok(())
}

#[test]
fn ipv6_wide_trust_does_not_include_ipv4_or_mapped_peers() -> anyhow::Result<()> {
    let headers = forwarded_headers("198.51.100.7")?;
    for peer in ["127.0.0.1:1234", "[::ffff:127.0.0.1]:1234"] {
        let origin = resolve_request_origin(
            &headers,
            true,
            &["::/0".parse()?],
            "127.0.0.1:8070".parse()?,
            Some(peer.parse()?),
        );
        assert_eq!(origin.remote_address, Some("127.0.0.1".parse()?));
        assert_eq!(origin.base_url, "http://127.0.0.1:8070");
    }
    Ok(())
}

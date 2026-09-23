use std::net::SocketAddr;

use super::fixtures::*;

const OPERATOR_ROUTES: [&str; 2] = [route::v1::STATS, route::METRICS];

#[tokio::test]
async fn stats_and_metrics_preserve_method_not_allowed() -> TestResult {
    let mut state = test_state();
    state.config.http.bind_address = SocketAddr::from(([0, 0, 0, 0], 8070));

    for path in OPERATOR_ROUTES {
        route_status(
            &state,
            Request::post(path),
            Body::empty(),
            StatusCode::METHOD_NOT_ALLOWED,
            path,
        )
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn operator_routes_require_token_and_challenge_rejected_requests() -> TestResult {
    const TOKEN: &str = "operator-secret-with-at-least-32-bytes";
    let mut state = test_state();
    state.config.diagnostics.auth_token =
        Some(secrecy::SecretString::from(format!("  {TOKEN} \n")));
    let cases = [
        (None, StatusCode::UNAUTHORIZED),
        (Some(format!("Basic {TOKEN}")), StatusCode::UNAUTHORIZED),
        (Some("Bearer short".to_owned()), StatusCode::UNAUTHORIZED),
        (Some(format!("Bearer x{TOKEN}")), StatusCode::UNAUTHORIZED),
        (
            Some(format!("Bearer {}x", &TOKEN[..TOKEN.len() - 1])),
            StatusCode::UNAUTHORIZED,
        ),
        (Some(format!("Bearer {TOKEN}")), StatusCode::OK),
        (Some(format!("bEaReR {TOKEN}")), StatusCode::OK),
    ];
    for path in [
        route::v1::STATS,
        route::METRICS,
        route::diagnostics::SUMMARY,
    ] {
        for (authorization, expected) in &cases {
            let mut request = Request::get(path);
            if let Some(authorization) = authorization {
                request = request.header(header::AUTHORIZATION, authorization);
            }
            let response = route_response(&state, request, Body::empty(), *expected, path).await?;
            let challenge = response.headers().get(header::WWW_AUTHENTICATE);
            if *expected == StatusCode::UNAUTHORIZED {
                assert_eq!(
                    challenge.and_then(|value| value.to_str().ok()),
                    Some("Bearer realm=\"o-sfu\"")
                );
            } else {
                assert!(challenge.is_none());
            }
        }
    }
    let snapshot = state.metrics.snapshot();
    assert_eq!(snapshot.http_stats_requests(), u64::try_from(cases.len())?);
    assert_eq!(
        snapshot.http_metrics_requests(),
        u64::try_from(cases.len())?
    );
    Ok(())
}

#[tokio::test]
async fn operator_head_requests_require_authorization_and_omit_response_bodies() -> TestResult {
    let mut state = test_state();
    state.config.http.bind_address = SocketAddr::from(([0, 0, 0, 0], 8070));
    state.config.diagnostics.auth_token = Some(secrecy::SecretString::from(
        "operator-secret-with-at-least-32-bytes",
    ));
    for path in [
        route::v1::STATS,
        route::METRICS,
        route::diagnostics::SUMMARY,
    ] {
        for (authorization, expected_status) in [
            (None, StatusCode::UNAUTHORIZED),
            (
                Some("Bearer operator-secret-with-at-least-32-bytes"),
                StatusCode::OK,
            ),
        ] {
            let mut builder = Request::head(path);
            if let Some(authorization) = authorization {
                builder = builder.header(header::AUTHORIZATION, authorization);
            }
            let response =
                route_response(&state, builder, Body::empty(), expected_status, path).await?;
            assert!(to_bytes(response.into_body(), usize::MAX).await?.is_empty());
        }
    }
    let snapshot = state.metrics.snapshot();
    assert_eq!(snapshot.http_stats_requests(), 2);
    assert_eq!(snapshot.http_metrics_requests(), 2);
    assert_eq!(snapshot.http_noop_requests(), 0);
    assert_eq!(snapshot.http_room_requests(), 0);
    assert_eq!(snapshot.http_disconnect_requests(), 0);
    Ok(())
}

#[tokio::test]
async fn unknown_operator_paths_remain_not_found_under_restrictive_access_policies() -> TestResult {
    for auth_token in [
        None,
        Some(secrecy::SecretString::from(
            "operator-secret-with-at-least-32-bytes",
        )),
    ] {
        let mut state = test_state();
        state.config.http.bind_address = SocketAddr::from(([0, 0, 0, 0], 8070));
        state.config.diagnostics.auth_token = auth_token;
        for path in [
            "/missing",
            "/v1/stats/missing",
            "/metrics/missing",
            "/internal/diagnostics/missing",
        ] {
            route_status(
                &state,
                Request::get(path),
                Body::empty(),
                StatusCode::NOT_FOUND,
                path,
            )
            .await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn diagnostics_authorizes_unsupported_methods() -> TestResult {
    let mut state = test_state();
    state.config.diagnostics.auth_token = Some(secrecy::SecretString::from(
        "operator-secret-with-at-least-32-bytes",
    ));
    for (authorization, expected_status) in [
        (None, StatusCode::UNAUTHORIZED),
        (Some("Bearer wrong-secret"), StatusCode::UNAUTHORIZED),
        (
            Some("Bearer operator-secret-with-at-least-32-bytes"),
            StatusCode::METHOD_NOT_ALLOWED,
        ),
    ] {
        let mut builder = Request::post(route::diagnostics::SUMMARY);
        if let Some(authorization) = authorization {
            builder = builder.header(header::AUTHORIZATION, authorization);
        }
        route_status(
            &state,
            builder,
            Body::empty(),
            expected_status,
            "diagnostics should authorize before rejecting an unsupported method",
        )
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn diagnostics_rejects_unsupported_methods_on_public_listener() -> TestResult {
    let state = test_state();
    let request = Request::post(route::diagnostics::SUMMARY).body(Body::empty())?;
    let response = app(state, SocketAddr::from(([0, 0, 0, 0], 8070)))
        .oneshot(request)
        .await?;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(response.headers().get(header::WWW_AUTHENTICATE).is_none());
    Ok(())
}

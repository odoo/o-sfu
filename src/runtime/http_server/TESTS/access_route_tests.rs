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
async fn stats_and_metrics_require_configured_token_on_loopback_listener() -> TestResult {
    let mut state = test_state();
    state.config.diagnostics.auth_token = Some(String::from("operator-secret"));

    for path in OPERATOR_ROUTES {
        route_status(
            &state,
            Request::get(path),
            Body::empty(),
            StatusCode::UNAUTHORIZED,
            path,
        )
        .await?;
        route_status(
            &state,
            Request::get(path).header(header::AUTHORIZATION, "Bearer wrong-secret"),
            Body::empty(),
            StatusCode::UNAUTHORIZED,
            path,
        )
        .await?;
        route_status(
            &state,
            Request::get(path).header(header::AUTHORIZATION, "Bearer operator-secret"),
            Body::empty(),
            StatusCode::OK,
            path,
        )
        .await?;
    }
    let snapshot = state.metrics.snapshot();
    assert_eq!(snapshot.http_stats_requests(), 3);
    assert_eq!(snapshot.http_metrics_requests(), 3);
    Ok(())
}

#[tokio::test]
async fn operator_head_requests_require_authorization_and_omit_response_bodies() -> TestResult {
    let mut state = test_state();
    state.config.http.bind_address = SocketAddr::from(([0, 0, 0, 0], 8070));
    state.config.diagnostics.auth_token = Some(String::from("operator-secret"));
    for path in [
        route::v1::STATS,
        route::METRICS,
        route::diagnostics::SUMMARY,
    ] {
        for (authorization, expected_status) in [
            (None, StatusCode::UNAUTHORIZED),
            (Some("Bearer operator-secret"), StatusCode::OK),
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
    for auth_token in [None, Some(String::from("operator-secret"))] {
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
    state.config.diagnostics.auth_token = Some(String::from("operator-secret"));
    for (authorization, expected_status) in [
        (None, StatusCode::UNAUTHORIZED),
        (Some("Bearer wrong-secret"), StatusCode::UNAUTHORIZED),
        (
            Some("Bearer operator-secret"),
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
    Ok(())
}

use rama_core::{
    Service,
    error::BoxError,
    extensions::{Egress, ExtensionsRef},
    rt::Executor,
};
use rama_http::{Body, Request, Response};
use rama_http_backend::client::{HttpConnectRequestAdapter, HttpConnector};
use rama_net::client::EstablishedClientConnection;
use rama_tcp::client::service::TcpConnector;

/// Returns `BoxError` if connecting, the HTTP handshake or sending the request fails.
pub async fn send(request: Request) -> Result<Response, BoxError> {
    let connector = HttpConnectRequestAdapter::new(HttpConnector::<_, Body>::new(
        TcpConnector::new(),
        Executor::default(),
    ));
    let EstablishedClientConnection { conn, input } = connector.serve(request).await?;
    input.extensions().insert(Egress(conn.extensions().clone()));
    conn.serve(input).await
}

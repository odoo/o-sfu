//! Room control routes with authenticated inputs and bounded disconnect bodies.

use axum::Router;

use super::contract::route;
use crate::runtime::RuntimeState;

mod create;
mod disconnect;

pub(super) fn routes() -> Router<RuntimeState> {
    Router::new()
        .route(route::v1::CHANNEL, create::route())
        .route(route::v1::DISCONNECT, disconnect::route())
}

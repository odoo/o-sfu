use o_sfu_telemetry::{
    diagnostics::DiagnosticsTransportHealth,
    metrics::{SourceSelectionKind, TransportHealthState},
};

use crate::engine::{media_transport::TransportSessionHealth, source_model::SourceSelector};

pub(crate) const fn diagnostics_transport_health(
    health: TransportSessionHealth,
) -> DiagnosticsTransportHealth {
    match health {
        TransportSessionHealth::Connected => DiagnosticsTransportHealth::Connected,
        TransportSessionHealth::Disconnected => DiagnosticsTransportHealth::Disconnected,
    }
}

pub(crate) const fn transport_health_state(health: TransportSessionHealth) -> TransportHealthState {
    match health {
        TransportSessionHealth::Connected => TransportHealthState::Connected,
        TransportSessionHealth::Disconnected => TransportHealthState::Disconnected,
    }
}

pub(crate) const fn source_selection_kind(selector: SourceSelector) -> SourceSelectionKind {
    match selector {
        SourceSelector::Open => SourceSelectionKind::Open,
        SourceSelector::Encoding(_) => SourceSelectionKind::Encoding,
    }
}

use std::{
    io,
    net::{IpAddr, Ipv4Addr},
    time::Duration,
};

use anyhow::Result;

use super::{
    Bitrate, Env, RoomMediaLimits, RoomWorkerPolicy, RtcPortRange, RtcUdpIoBackend,
    TransportConfig, VideoAdaptationTuning, VideoBitrateLimits, default_rtc_media_worker_count,
};

fn load_transport_config(get_var: impl Fn(&str) -> Option<String>) -> Result<TransportConfig> {
    let env = Env::new(get_var, |_| {
        Err(io::Error::new(io::ErrorKind::NotFound, "file not found"))
    });
    TransportConfig::from_env(&env)
}

fn load_transport_config_with_defaults(overrides: &[(&str, &str)]) -> Result<TransportConfig> {
    load_transport_config(|key| {
        overrides
            .iter()
            .find(|(name, _value)| *name == key)
            .map(|(_name, value)| (*value).to_owned())
            .or_else(|| match key {
                "ANNOUNCED_IP" => Some("127.0.0.1".to_owned()),
                _ => None,
            })
    })
}

struct InvalidTransportCase<'a> {
    overrides: &'a [(&'a str, &'a str)],
    message: &'a str,
}

fn assert_invalid_transport_cases(cases: &[InvalidTransportCase<'_>]) {
    for case in cases {
        let error = load_transport_config_with_defaults(case.overrides)
            .err()
            .map(|error| error.to_string());
        assert_eq!(error.as_deref(), Some(case.message), "{:?}", case.overrides);
    }
}

#[test]
fn load_transport_config_accepts_public_ip_and_defaults() {
    let config = load_transport_config(|key| match key {
        "ANNOUNCED_IP" => Some("203.0.113.10".to_owned()),
        _ => None,
    });
    let worker_count = default_rtc_media_worker_count();
    assert_eq!(
        config.ok(),
        Some(TransportConfig {
            announced_ip: IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10)),
            max_bitrate_in: Bitrate::from_mbps(8),
            max_bitrate_out: Bitrate::from_mbps(10),
            video_bitrate_limits: VideoBitrateLimits::default(),
            rtc_port_range: RtcPortRange::new(40_000, 49_999),
            rtc_udp_io_backend: RtcUdpIoBackend::Tokio,
            rtc_media_worker_count: worker_count,
            room_worker_policy: RoomWorkerPolicy::strict_single_router(),
            room_media_limits: RoomMediaLimits::default(),
            video_adaptation_tuning: VideoAdaptationTuning::default(),
        })
    );
}

#[test]
fn load_transport_config_accepts_explicit_bitrate_limits() -> Result<()> {
    let config = load_transport_config_with_defaults(&[
        ("MAX_BITRATE_IN", "1234567"),
        ("MAX_BITRATE_OUT", "7654321"),
        ("MAX_VIDEO_BITRATE", "2345678"),
    ])?;
    assert_eq!(config.max_bitrate_in, Bitrate::from_bps(1_234_567));
    assert_eq!(config.max_bitrate_out, Bitrate::from_bps(7_654_321));
    assert_eq!(
        config.video_bitrate_limits,
        VideoBitrateLimits::new(Bitrate::from_bps(2_345_678))
    );
    Ok(())
}

#[test]
fn load_transport_config_accepts_explicit_rtc_udp_io_backend() -> Result<()> {
    let config = load_transport_config_with_defaults(&[("RTC_UDP_IO_BACKEND", "tokio")])?;
    assert_eq!(config.rtc_udp_io_backend, RtcUdpIoBackend::Tokio);

    #[cfg(target_os = "linux")]
    {
        let config = load_transport_config_with_defaults(&[("RTC_UDP_IO_BACKEND", "io_uring")])?;
        assert_eq!(config.rtc_udp_io_backend, RtcUdpIoBackend::IoUring);
    }

    Ok(())
}

#[cfg(not(target_os = "linux"))]
#[test]
fn load_transport_config_rejects_io_uring_backend_on_non_linux() {
    assert_invalid_transport_cases(&[InvalidTransportCase {
        overrides: &[("RTC_UDP_IO_BACKEND", "io_uring")],
        message: "RTC_UDP_IO_BACKEND=io_uring is only supported on Linux",
    }]);
}

#[test]
fn load_transport_config_requires_public_ip() {
    let config = load_transport_config(|_| None);
    assert!(config.is_err());
}

#[test]
fn load_transport_config_accepts_room_spillover_policy() -> Result<()> {
    let config = load_transport_config_with_defaults(&[
        ("RTC_MEDIA_WORKER_COUNT", "3"),
        ("ROOM_MAX_LOCAL_ROUTERS", "2"),
        ("ROOM_SPILLOVER_PACKET_LOOP_DELAY_MS", "7"),
    ])?;

    assert_eq!(config.room_worker_policy.max_local_routers(), 2);
    assert_eq!(
        config.room_worker_policy.packet_loop_delay_threshold_ms(),
        7
    );
    Ok(())
}

#[test]
fn load_transport_config_accepts_room_media_limits() -> Result<()> {
    let config = load_transport_config_with_defaults(&[
        ("ROOM_MAX_ACTIVE_AUDIO_SPEAKERS", "3"),
        ("ROOM_MAX_VIDEO_DOWNLOADS_PER_RECEIVER", "8"),
    ])?;

    assert_eq!(config.room_media_limits.max_active_audio_speakers(), 3);
    assert_eq!(
        config.room_media_limits.max_video_downloads_per_receiver(),
        8
    );
    Ok(())
}

#[test]
fn load_transport_config_accepts_video_adaptation_tuning() -> Result<()> {
    let config = load_transport_config_with_defaults(&[
        ("ROOM_MULTIPARTY_SCALABLE_VIDEO_THRESHOLD", "5"),
        ("ROOM_THUMBNAIL_BUDGET_DIVISOR", "3"),
        ("ROOM_SOFT_PAUSE_DWELL_MS", "250"),
        ("ROOM_UPGRADE_DWELL_MS", "1250"),
        ("ROOM_RECEIVER_BUDGET_HEADROOM_PERCENT", "15"),
        ("ROOM_AUDIO_RESERVE_PER_SPEAKER_BPS", "40000"),
    ])?;

    assert_eq!(
        config.video_adaptation_tuning,
        VideoAdaptationTuning::try_new(
            5,
            3,
            Duration::from_millis(250),
            Duration::from_millis(1250),
            15,
            Bitrate::from_bps(40_000)
        )?
    );
    Ok(())
}

#[test]
fn load_transport_config_rejects_invalid_video_adaptation_tuning() {
    assert_invalid_transport_cases(&[
        InvalidTransportCase {
            overrides: &[("ROOM_SOFT_PAUSE_DWELL_MS", "0")],
            message: "ROOM_SOFT_PAUSE_DWELL_MS must be greater than zero",
        },
        InvalidTransportCase {
            overrides: &[("ROOM_UPGRADE_DWELL_MS", "0")],
            message: "ROOM_UPGRADE_DWELL_MS must be greater than zero",
        },
        InvalidTransportCase {
            overrides: &[("ROOM_DOWNSWITCH_PRESSURE_OBSERVATIONS", "2")],
            message: "ROOM_DOWNSWITCH_PRESSURE_OBSERVATIONS is no longer supported, use ROOM_SOFT_PAUSE_DWELL_MS",
        },
        InvalidTransportCase {
            overrides: &[
                ("ROOM_UPSWITCH_STABLE_OBSERVATIONS", "3"),
                ("ROOM_UPGRADE_DWELL_MS", "1250"),
            ],
            message: "ROOM_UPSWITCH_STABLE_OBSERVATIONS is no longer supported, use ROOM_UPGRADE_DWELL_MS",
        },
        InvalidTransportCase {
            overrides: &[("ROOM_RECEIVER_BUDGET_HEADROOM_PERCENT", "150")],
            message: "ROOM_RECEIVER_BUDGET_HEADROOM_PERCENT must not exceed 100",
        },
    ]);
}

#[test]
fn load_transport_config_uses_default_spillover_delay() -> Result<()> {
    let config = load_transport_config_with_defaults(&[
        ("RTC_MEDIA_WORKER_COUNT", "2"),
        ("ROOM_MAX_LOCAL_ROUTERS", "2"),
    ])?;

    assert_eq!(
        config.room_worker_policy.packet_loop_delay_threshold_ms(),
        RoomWorkerPolicy::DEFAULT_PACKET_LOOP_DELAY_THRESHOLD_MS
    );
    Ok(())
}

#[test]
fn load_transport_config_keeps_explicit_single_router_strict() -> Result<()> {
    let config = load_transport_config_with_defaults(&[
        ("RTC_MEDIA_WORKER_COUNT", "2"),
        ("ROOM_MAX_LOCAL_ROUTERS", "1"),
    ])?;

    assert_eq!(
        config.room_worker_policy,
        RoomWorkerPolicy::strict_single_router()
    );
    Ok(())
}

#[test]
fn load_transport_config_rejects_invalid_transport_values() {
    assert_invalid_transport_cases(&[
        InvalidTransportCase {
            overrides: &[("TRANSPORT_BACKEND", "rtc")],
            message: "TRANSPORT_BACKEND is no longer supported; o-sfu always boots the RTC transport",
        },
        InvalidTransportCase {
            overrides: &[("RTC_UDP_IO_BACKEND", "epoll")],
            message: "RTC_UDP_IO_BACKEND must be one of tokio or io_uring, got epoll",
        },
        InvalidTransportCase {
            overrides: &[("ANNOUNCED_IP", "0.0.0.0")],
            message: "ANNOUNCED_IP must be a concrete advertised address",
        },
        InvalidTransportCase {
            overrides: &[("ANNOUNCED_IP", "239.1.1.1")],
            message: "ANNOUNCED_IP cannot be a multicast address",
        },
        InvalidTransportCase {
            overrides: &[("RTC_MIN_PORT", "5000"), ("RTC_MAX_PORT", "4000")],
            message: "RTC_MAX_PORT must be greater than or equal to RTC_MIN_PORT",
        },
        InvalidTransportCase {
            overrides: &[("RTC_MEDIA_WORKER_COUNT", "0")],
            message: "RTC_MEDIA_WORKER_COUNT must be greater than zero",
        },
        InvalidTransportCase {
            overrides: &[
                ("RTC_MIN_PORT", "4000"),
                ("RTC_MAX_PORT", "4001"),
                ("RTC_MEDIA_WORKER_COUNT", "3"),
            ],
            message: "RTC_MEDIA_WORKER_COUNT must be less than or equal to the available RTC port count",
        },
    ]);
}

#[test]
fn load_transport_config_rejects_invalid_room_policy_values() {
    assert_invalid_transport_cases(&[
        InvalidTransportCase {
            overrides: &[("ROOM_SPILLOVER_PACKET_LOOP_DELAY_MS", "0")],
            message: "ROOM_SPILLOVER_PACKET_LOOP_DELAY_MS must be greater than zero",
        },
        InvalidTransportCase {
            overrides: &[
                ("RTC_MEDIA_WORKER_COUNT", "2"),
                ("ROOM_MAX_LOCAL_ROUTERS", "0"),
            ],
            message: "ROOM_MAX_LOCAL_ROUTERS must be greater than zero",
        },
        InvalidTransportCase {
            overrides: &[
                ("RTC_MEDIA_WORKER_COUNT", "2"),
                ("ROOM_MAX_LOCAL_ROUTERS", "3"),
            ],
            message: "ROOM_MAX_LOCAL_ROUTERS must be less than or equal to RTC_MEDIA_WORKER_COUNT",
        },
    ]);
}

#[test]
fn load_transport_config_rejects_invalid_bitrate_and_media_limit_values() {
    assert_invalid_transport_cases(&[
        InvalidTransportCase {
            overrides: &[("MAX_BITRATE_IN", "0")],
            message: "MAX_BITRATE_IN must be greater than zero",
        },
        InvalidTransportCase {
            overrides: &[("MAX_BITRATE_OUT", "0")],
            message: "MAX_BITRATE_OUT must be greater than zero",
        },
        InvalidTransportCase {
            overrides: &[("MAX_VIDEO_BITRATE", "0")],
            message: "MAX_VIDEO_BITRATE must be greater than zero",
        },
        InvalidTransportCase {
            overrides: &[("ROOM_MAX_ACTIVE_AUDIO_SPEAKERS", "0")],
            message: "ROOM_MAX_ACTIVE_AUDIO_SPEAKERS must be greater than zero",
        },
        InvalidTransportCase {
            overrides: &[("ROOM_MAX_VIDEO_DOWNLOADS_PER_RECEIVER", "0")],
            message: "ROOM_MAX_VIDEO_DOWNLOADS_PER_RECEIVER must be greater than zero",
        },
    ]);
}

#[test]
fn load_transport_config_preserves_legacy_error_precedence() {
    let error =
        load_transport_config_with_defaults(&[("MAX_BITRATE_IN", "0"), ("RTC_MAX_PORT", "abc")])
            .err()
            .map(|error| error.to_string());

    assert_eq!(
        error.as_deref(),
        Some("MAX_BITRATE_IN must be greater than zero")
    );

    let error = load_transport_config_with_defaults(&[
        ("RTC_MIN_PORT", "5000"),
        ("RTC_MAX_PORT", "4000"),
        ("ROOM_SPILLOVER_PACKET_LOOP_DELAY_MS", "0"),
    ])
    .err()
    .map(|error| error.to_string());

    assert_eq!(
        error.as_deref(),
        Some("ROOM_SPILLOVER_PACKET_LOOP_DELAY_MS must be greater than zero")
    );
}

#[test]
fn load_transport_config_preserves_numeric_parse_errors() {
    assert_invalid_transport_cases(&[
        InvalidTransportCase {
            overrides: &[("RTC_MIN_PORT", "abc")],
            message: "RTC_MIN_PORT must be a valid u16",
        },
        InvalidTransportCase {
            overrides: &[("MAX_BITRATE_IN", "abc")],
            message: "MAX_BITRATE_IN must be a valid u64",
        },
        InvalidTransportCase {
            overrides: &[("ROOM_MAX_LOCAL_ROUTERS", "abc")],
            message: "ROOM_MAX_LOCAL_ROUTERS must be a valid usize",
        },
        InvalidTransportCase {
            overrides: &[("ROOM_SPILLOVER_PACKET_LOOP_DELAY_MS", "abc")],
            message: "ROOM_SPILLOVER_PACKET_LOOP_DELAY_MS must be a valid u64",
        },
        InvalidTransportCase {
            overrides: &[("ROOM_MAX_ACTIVE_AUDIO_SPEAKERS", "abc")],
            message: "ROOM_MAX_ACTIVE_AUDIO_SPEAKERS must be a valid usize",
        },
        InvalidTransportCase {
            overrides: &[("ROOM_MAX_VIDEO_DOWNLOADS_PER_RECEIVER", "abc")],
            message: "ROOM_MAX_VIDEO_DOWNLOADS_PER_RECEIVER must be a valid usize",
        },
    ]);
}

#[test]
fn load_transport_config_rejects_unrepresentable_policy_deadlines() {
    assert_invalid_transport_cases(&[
        InvalidTransportCase {
            overrides: &[("ROOM_SOFT_PAUSE_DWELL_MS", "18446744073709551615")],
            message: "ROOM_SOFT_PAUSE_DWELL_MS must not exceed 3153600000000",
        },
        InvalidTransportCase {
            overrides: &[("ROOM_UPGRADE_DWELL_MS", "18446744073709551615")],
            message: "ROOM_UPGRADE_DWELL_MS must not exceed 3153600000000",
        },
    ]);
}

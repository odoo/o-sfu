use std::{
    cell::Cell,
    num::{NonZeroU64, NonZeroUsize},
    time::Duration,
};

use o_sfu_core::prelude::Bitrate;

use super::{Env, non_empty, positive};

fn error<T>(result: anyhow::Result<T>) -> Option<String> {
    result.err().map(|error| error.to_string())
}

#[test]
fn env_loads_required_default_optional_check_and_trimmed_values() {
    let env = Env::new(|key| match key {
        "REQUIRED_ENV" => Some("value".to_owned()),
        "COUNT_ENV" => Some("4".to_owned()),
        "TOKEN_ENV" => Some("  token  ".to_owned()),
        "DURATION_ENV" => Some("90".to_owned()),
        _ => None,
    });
    assert_eq!(
        env.var("REQUIRED_ENV").required().ok(),
        Some("value".to_owned())
    );
    assert_eq!(env.var("FLAG_ENV").default(false).ok(), Some(false));
    assert_eq!(
        env.var("COUNT_ENV").check(positive).default(1usize).ok(),
        Some(4)
    );
    assert_eq!(
        env.var("TOKEN_ENV").check(non_empty).optional().ok(),
        Some(Some("token".to_owned()))
    );
    assert_eq!(
        env.var("DURATION_ENV").default(Duration::from_mins(1)).ok(),
        Some(Duration::from_secs(90)),
        "durations are read as whole seconds"
    );
    assert_eq!(
        env.var("MISSING_DURATION_ENV")
            .default(Duration::from_mins(1))
            .ok(),
        Some(Duration::from_mins(1))
    );
    assert_eq!(env.var::<String>("MISSING_ENV").optional().ok(), Some(None));
}

#[test]
fn env_reports_parse_and_validation_errors() {
    let env = Env::new(|key| match key {
        "FLAG_ENV" => Some("yes".to_owned()),
        "COUNT_ENV" => Some("abc".to_owned()),
        "ZERO_ENV" => Some("0".to_owned()),
        "TOKEN_ENV" => Some("   ".to_owned()),
        "DURATION_ENV" => Some("-42".to_owned()),
        _ => None,
    });
    assert_eq!(
        error(env.var::<String>("REQUIRED_ENV").required()).as_deref(),
        Some("REQUIRED_ENV env variable is required")
    );
    assert_eq!(
        error(env.var("FLAG_ENV").default(false)).as_deref(),
        Some("FLAG_ENV must be either `true` or `false`")
    );
    assert_eq!(
        error(env.var("COUNT_ENV").default(1usize)).as_deref(),
        Some("COUNT_ENV must be a valid usize")
    );
    assert_eq!(
        error(env.var("ZERO_ENV").check(positive).default(1usize)).as_deref(),
        Some("ZERO_ENV must be greater than zero")
    );
    assert_eq!(
        error(env.var("TOKEN_ENV").check(non_empty).optional()).as_deref(),
        Some("TOKEN_ENV must not be empty")
    );
    assert_eq!(
        error(env.var::<Duration>("DURATION_ENV").optional()).as_deref(),
        Some("DURATION_ENV must be a valid duration in seconds")
    );
}

#[test]
fn env_validates_default_values() {
    let env = Env::new(|_| None);
    assert_eq!(
        error(env.var("MISSING_COUNT").check(positive).default(0usize)).as_deref(),
        Some("MISSING_COUNT must be greater than zero")
    );
}

#[test]
fn env_checks_capture_values_and_validate_defaults() {
    let env = Env::new(|key| (key == "COUNT_ENV").then(|| "4".to_owned()));
    let limit = 5usize;
    let calls = Cell::new(0);
    let below_limit = |key, value| {
        calls.set(calls.get() + 1);
        anyhow::ensure!(value < limit, "{key} must be less than {limit}");
        Ok(value)
    };
    assert_eq!(
        env.var("COUNT_ENV").check(below_limit).required().ok(),
        Some(4)
    );
    assert_eq!(
        env.var("MISSING_ENV").check(below_limit).default(3).ok(),
        Some(3)
    );
    assert_eq!(
        error(env.var("MISSING_ENV").check(below_limit).default(5)).as_deref(),
        Some("MISSING_ENV must be less than 5")
    );
    assert_eq!(
        env.var("MISSING_ENV").check(below_limit).optional().ok(),
        Some(None)
    );
    assert_eq!(calls.get(), 3);
}

#[test]
fn env_chained_checks_transform_values_and_short_circuit() {
    let env = Env::new(|_| Some("4".to_owned()));
    let offset = 3usize;
    let reached_later_check = Cell::new(false);
    assert_eq!(
        env.var("COUNT_ENV")
            .check(|_, value| Ok(value + offset))
            .check(|_, value| Ok(value * 2))
            .required()
            .ok(),
        Some(14)
    );
    assert_eq!(
        error(
            env.var::<usize>("COUNT_ENV")
                .check(|key, value| {
                    anyhow::ensure!(value < offset, "{key} must be less than {offset}");
                    Ok(value)
                })
                .check(|_, value| {
                    reached_later_check.set(true);
                    Ok(value)
                })
                .required()
        )
        .as_deref(),
        Some("COUNT_ENV must be less than 3")
    );
    assert!(!reached_later_check.get());
}

#[test]
fn env_alias() {
    let env = Env::new(|key| match key {
        "PRIMARY_ENV" => Some("primary".to_owned()),
        "SECOND_ALIAS_ENV" => Some("second alias".to_owned()),
        _ => None,
    });
    assert_eq!(
        env.var("MISSING_ENV")
            .alias("FIRST_ALIAS_ENV")
            .alias("SECOND_ALIAS_ENV")
            .required()
            .ok(),
        Some("second alias".to_owned())
    );
    assert_eq!(
        env.var("PRIMARY_ENV")
            .alias("SECOND_ALIAS_ENV")
            .required()
            .ok(),
        Some("primary".to_owned())
    );
}

#[test]
fn env_alias_errors_keep_the_selected_key_without_falling_back() {
    let env = Env::new(|key| match key {
        "PRIMARY_ENV" | "ZERO_ALIAS_ENV" => Some("0".to_owned()),
        "INVALID_ALIAS_ENV" => Some("invalid".to_owned()),
        "VALID_ALIAS_ENV" => Some("2".to_owned()),
        _ => None,
    });
    for (key, expected_key) in [
        ("PRIMARY_ENV", "PRIMARY_ENV"),
        ("MISSING_ENV", "ZERO_ALIAS_ENV"),
    ] {
        assert_eq!(
            error(
                env.var::<usize>(key)
                    .check(positive)
                    .alias("ZERO_ALIAS_ENV")
                    .alias("VALID_ALIAS_ENV")
                    .required()
            ),
            Some(format!("{expected_key} must be greater than zero"))
        );
    }
    for key in ["MISSING_ENV", "INVALID_ALIAS_ENV"] {
        assert_eq!(
            error(
                env.var::<usize>(key)
                    .alias("INVALID_ALIAS_ENV")
                    .alias("VALID_ALIAS_ENV")
                    .required()
            )
            .as_deref(),
            Some("INVALID_ALIAS_ENV must be a valid usize")
        );
    }
}

#[test]
fn env_parses_nonzero_values_and_typed_defaults() {
    let env = Env::new(|key| match key {
        "COUNT_ENV" => Some("42".to_owned()),
        "USIZE_MAX_ENV" => Some(usize::MAX.to_string()),
        "U64_MAX_ENV" => Some(u64::MAX.to_string()),
        _ => None,
    });
    assert_eq!(
        env.var::<NonZeroUsize>("COUNT_ENV")
            .required()
            .map(NonZeroUsize::get)
            .ok(),
        Some(42)
    );
    assert_eq!(
        env.var::<NonZeroU64>("COUNT_ENV")
            .required()
            .map(NonZeroU64::get)
            .ok(),
        Some(42)
    );
    assert_eq!(
        env.var::<NonZeroUsize>("USIZE_MAX_ENV")
            .required()
            .map(NonZeroUsize::get)
            .ok(),
        Some(usize::MAX)
    );
    assert_eq!(
        env.var::<NonZeroU64>("U64_MAX_ENV")
            .required()
            .map(NonZeroU64::get)
            .ok(),
        Some(u64::MAX)
    );
    assert_eq!(
        env.var("MISSING_ENV").default(NonZeroUsize::MIN).ok(),
        Some(NonZeroUsize::MIN)
    );
    assert_eq!(
        env.var("MISSING_ENV").default(NonZeroU64::MIN).ok(),
        Some(NonZeroU64::MIN)
    );
}

#[test]
fn env_nonzero_errors_preserve_primitive_and_zero_diagnostics() {
    for raw in ["invalid", "-1", "18446744073709551616"] {
        let env = Env::new(|_| Some(raw.to_owned()));
        assert_eq!(
            error(env.var::<NonZeroUsize>("COUNT_ENV").required()).as_deref(),
            Some("COUNT_ENV must be a valid usize")
        );
        assert_eq!(
            error(env.var::<NonZeroU64>("COUNT_ENV").required()).as_deref(),
            Some("COUNT_ENV must be a valid u64")
        );
    }
    let env = Env::new(|_| Some("0".to_owned()));
    assert_eq!(
        error(env.var::<NonZeroUsize>("COUNT_ENV").required()).as_deref(),
        Some("COUNT_ENV must be greater than zero")
    );
    assert_eq!(
        error(env.var::<NonZeroU64>("COUNT_ENV").required()).as_deref(),
        Some("COUNT_ENV must be greater than zero")
    );
}

#[test]
fn env_parses_bitrates_as_integer_bps_and_uses_typed_defaults() {
    for bps in [0, 42, u64::MAX] {
        let env = Env::new(|_| Some(bps.to_string()));
        assert_eq!(
            env.var::<Bitrate>("BITRATE_ENV")
                .required()
                .map(Bitrate::as_bps)
                .ok(),
            Some(bps)
        );
    }
    let env = Env::new(|_| None);
    for bps in [0, 42] {
        assert_eq!(
            env.var("BITRATE_ENV")
                .default(Bitrate::from_bps(bps))
                .map(Bitrate::as_bps)
                .ok(),
            Some(bps)
        );
    }
}

#[test]
fn env_bitrate_validation_preserves_numeric_and_positive_diagnostics() {
    for raw in ["invalid", "-1", "18446744073709551616"] {
        let env = Env::new(|_| Some(raw.to_owned()));
        assert_eq!(
            error(env.var::<Bitrate>("BITRATE_ENV").required()).as_deref(),
            Some("BITRATE_ENV must be a valid u64")
        );
    }
    let env = Env::new(|key| match key {
        "BITRATE_ENV" => Some("42".to_owned()),
        "ZERO_ENV" => Some("0".to_owned()),
        _ => None,
    });
    assert_eq!(
        env.var::<Bitrate>("BITRATE_ENV")
            .check(positive)
            .required()
            .map(Bitrate::as_bps)
            .ok(),
        Some(42)
    );
    for key in ["ZERO_ENV", "MISSING_ENV"] {
        assert_eq!(
            error(env.var(key).check(positive).default(Bitrate::from_bps(0))),
            Some(format!("{key} must be greater than zero"))
        );
    }
}

use std::{env, fs};

use anyhow::Result;

use super::{
    AuthConfig, CodecConfig, HttpConfig, TransportConfig, UserConfig,
    codec_flags::load_media_codec_flags, codec_preferences::load_codec_preferences,
    diagnostics::DiagnosticsConfig, env::Env, feature_flags::load_runtime_feature_flags,
    settings::Config, telemetry::load_telemetry_config,
};

impl Config {
    /// # Errors
    ///
    /// Returns an error when a required operator environment variable is
    /// missing, an environment value cannot be parsed, a constrained value is
    /// outside its accepted range, telemetry export is requested without the
    /// required cargo feature, or transport cross-field validation rejects the
    /// advertised address, RTC port range, worker count, or room policy.
    pub fn from_env() -> Result<Self> {
        Self::from_var_lookup(|key| env::var(key).ok())
    }

    fn from_var_lookup(get_var: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let env = Env::new(get_var, |path| fs::read_to_string(path));
        let http = HttpConfig::from_env(&env)?;
        let auth = AuthConfig::from_env(&env)?;
        let user = UserConfig::from_env(&env)?;
        let feature_flags = load_runtime_feature_flags(&env)?;
        let codec_flags = load_media_codec_flags(&env)?;
        let codec_preferences = load_codec_preferences(&env)?;
        let diagnostics = DiagnosticsConfig::from_env(&env)?;
        let telemetry = load_telemetry_config(&env)?;
        let transport = TransportConfig::from_env(&env)?;
        Ok(Self {
            auth,
            http,
            user,
            transport,
            codecs: CodecConfig {
                flags: codec_flags,
                preferences: codec_preferences,
            },
            features: feature_flags,
            telemetry,
            diagnostics,
        })
    }
}

#[cfg(test)]
#[path = "TESTS/loader.rs"]
mod tests;

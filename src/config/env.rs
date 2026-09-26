use std::{
    io, iter,
    marker::PhantomData,
    net::{IpAddr, SocketAddr},
    num::{NonZeroU64, NonZeroUsize},
    path::Path,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, ensure};
use o_sfu_core::prelude::Bitrate;
use secrecy::SecretString;
use zeroize::Zeroize;

use super::DeadlineDuration;

type Lookup<'a> = dyn Fn(&str) -> Option<String> + 'a;
type ReadFile<'a> = dyn Fn(&Path) -> io::Result<String> + 'a;

enum FileSource {
    Fallback(&'static str),
    Exclusive(&'static str),
}

pub(super) struct EnvValue {
    pub(super) key: &'static str,
    pub(super) raw: String,
}

pub(super) struct Env<'a> {
    lookup: Box<Lookup<'a>>,
    read_file: Box<ReadFile<'a>>,
}

impl<'a> Env<'a> {
    pub(super) fn new(
        get_var: impl Fn(&str) -> Option<String> + 'a,
        read_file: impl Fn(&Path) -> io::Result<String> + 'a,
    ) -> Self {
        Self {
            lookup: Box::new(get_var),
            read_file: Box::new(read_file),
        }
    }

    pub(super) fn var<T>(&self, key: &'static str) -> Var<'a, '_, T> {
        Var {
            lookup: self.lookup.as_ref(),
            read_file: self.read_file.as_ref(),
            key,
            check: |_key, value| Ok(value),
            aliases: Vec::new(),
            file_source: None,
            value: PhantomData,
        }
    }
}

/// Parses the first present key and validates its value in check order.
///
/// Checks may transform values and capture other settings. They receive the
/// supplying key, including aliases. Defaults use the primary key and pass
/// through the same checks. Missing optional values bypass parsing and checks.
pub(super) struct Var<'env, 'lookup, T, C = fn(&'static str, T) -> Result<T>> {
    lookup: &'lookup Lookup<'env>,
    read_file: &'lookup ReadFile<'env>,
    key: &'static str,
    check: C,
    aliases: Vec<&'static str>,
    file_source: Option<FileSource>,
    value: PhantomData<fn(T) -> T>,
}

impl<'env, 'lookup, T, C> Var<'env, 'lookup, T, C>
where
    T: EnvParse,
    C: Fn(&'static str, T) -> Result<T>,
{
    /// Appends a check that runs only after all preceding checks succeed.
    pub(super) fn check(
        self,
        check: impl Fn(&'static str, T) -> Result<T>,
    ) -> Var<'env, 'lookup, T, impl Fn(&'static str, T) -> Result<T>> {
        Var {
            lookup: self.lookup,
            read_file: self.read_file,
            key: self.key,
            check: move |key, value| check(key, (self.check)(key, value)?),
            aliases: self.aliases,
            file_source: self.file_source,
            value: PhantomData,
        }
    }

    pub(super) fn alias(mut self, alias: &'static str) -> Self {
        self.aliases.push(alias);
        self
    }

    pub(super) fn or_load_from_file(mut self, alias: &'static str) -> Self {
        self.file_source = Some(FileSource::Fallback(alias));
        self
    }

    /// Loads the file only when no direct source is configured.
    ///
    /// Reading a direct source together with this file source returns an
    /// `anyhow::Error` without reading the file or disclosing the direct value.
    pub(super) fn exclusive_file(mut self, alias: &'static str) -> Self {
        self.file_source = Some(FileSource::Exclusive(alias));
        self
    }

    /// Returns the parsed and checked value of the first present key.
    ///
    /// # Errors
    /// Returns [`anyhow::Error`] when every key is absent or parsing or a check fails.
    pub(super) fn required(self) -> Result<T> {
        let value = self
            .load()?
            .with_context(|| format!("{} env variable is required", self.key))?;
        self.parse(value)
    }

    /// Uses the typed default only when every key is absent.
    ///
    /// # Errors
    /// Returns [`anyhow::Error`] when parsing or a check fails, including checks
    /// of the default value.
    pub(super) fn default(self, default: T) -> Result<T> {
        let Some(value) = self.load()? else {
            return (self.check)(self.key, default);
        };
        self.parse(value)
    }

    /// Returns `None` only when every key is absent.
    ///
    /// # Errors
    /// Returns [`anyhow::Error`] when parsing or a check of a present value fails.
    pub(super) fn optional(self) -> Result<Option<T>> {
        self.load()?.map(|value| self.parse(value)).transpose()
    }

    fn load(&self) -> Result<Option<EnvValue>> {
        for key in iter::once(self.key).chain(self.aliases.iter().copied()) {
            if let Some(mut raw) = (self.lookup)(key) {
                if let Some(FileSource::Exclusive(file_key)) = self.file_source
                    && (self.lookup)(file_key).is_some()
                {
                    // A conflicting secret never reaches EnvParse's zeroizing container.
                    raw.zeroize();
                    return Err(anyhow!("{key} conflicts with {file_key}"));
                }
                return Ok(Some(EnvValue { key, raw }));
            }
        }
        let Some(FileSource::Fallback(file_key) | FileSource::Exclusive(file_key)) =
            self.file_source
        else {
            return Ok(None);
        };
        match (self.lookup)(file_key) {
            Some(path) => {
                let mut raw = (self.read_file)(Path::new(&path)).with_context(|| {
                    format!("{file_key} points to \"{path}\" which could not be read")
                })?;
                let raw_trimmed = raw.trim().to_owned();
                raw.zeroize();
                Ok(Some(EnvValue {
                    key: file_key,
                    raw: raw_trimmed,
                }))
            }
            None => Ok(None),
        }
    }

    fn parse(&self, value: EnvValue) -> Result<T> {
        let key = value.key;
        (self.check)(key, T::parse(value)?)
    }
}

pub(super) trait EnvParse: Sized {
    fn parse(value: EnvValue) -> Result<Self>;
}

macro_rules! parse_from_str {
    ($type:ty, $name:literal) => {
        impl EnvParse for $type {
            fn parse(value: EnvValue) -> Result<Self> {
                let key = value.key;
                value
                    .raw
                    .parse()
                    .map_err(|_error| anyhow!("{key} must be a valid {}", $name))
            }
        }
    };
}

parse_from_str!(IpAddr, "IP address");
parse_from_str!(SocketAddr, "socket address");
parse_from_str!(u8, "u8");
parse_from_str!(u16, "u16");
parse_from_str!(u64, "u64");
parse_from_str!(usize, "usize");

impl EnvParse for NonZeroUsize {
    fn parse(value: EnvValue) -> Result<Self> {
        let key = value.key;
        Self::new(usize::parse(value)?).ok_or_else(|| anyhow!("{key} must be greater than zero"))
    }
}

impl EnvParse for NonZeroU64 {
    fn parse(value: EnvValue) -> Result<Self> {
        let key = value.key;
        Self::new(u64::parse(value)?).ok_or_else(|| anyhow!("{key} must be greater than zero"))
    }
}

/// Parses integer bits per second, including zero.
impl EnvParse for Bitrate {
    fn parse(value: EnvValue) -> Result<Self> {
        u64::parse(value).map(Self::from_bps)
    }
}

impl EnvParse for bool {
    fn parse(value: EnvValue) -> Result<Self> {
        let key = value.key;
        value
            .raw
            .parse()
            .map_err(|_error| anyhow!("{key} must be either `true` or `false`"))
    }
}

impl EnvParse for String {
    fn parse(value: EnvValue) -> Result<Self> {
        Ok(value.raw)
    }
}

impl EnvParse for DeadlineDuration {
    fn parse(value: EnvValue) -> Result<Self> {
        let key = value.key;
        Self::from_millis(u64::parse(value)?).map_err(|error| anyhow!("{key} {error}"))
    }
}

impl EnvParse for Duration {
    fn parse(value: EnvValue) -> Result<Self> {
        let key = value.key;
        let seconds = value
            .raw
            .parse()
            .map_err(|_error| anyhow!("{key} must be a valid duration in seconds"))?;
        Ok(Self::from_secs(seconds))
    }
}

impl EnvParse for SecretString {
    fn parse(value: EnvValue) -> Result<Self> {
        Ok(Self::from(value.raw))
    }
}

/// Requires a value greater than zero for types whose default is zero.
///
/// # Errors
/// Returns [`anyhow::Error`] when the value is not greater than zero.
pub(super) fn positive<T>(key: &'static str, value: T) -> Result<T>
where
    T: Default + PartialOrd,
{
    ensure!(value > T::default(), "{key} must be greater than zero");
    Ok(value)
}

pub(super) fn non_empty(key: &'static str, value: String) -> Result<String> {
    let trimmed = value.trim();
    ensure!(!trimmed.is_empty(), "{key} must not be empty");
    if trimmed.len() == value.len() {
        Ok(value)
    } else {
        Ok(trimmed.to_owned())
    }
}

#[cfg(test)]
#[path = "TESTS/env.rs"]
mod tests;

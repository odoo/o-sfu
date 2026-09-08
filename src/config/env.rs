use std::{
    io, iter,
    net::{IpAddr, SocketAddr},
    path::Path,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, ensure};
use secrecy::SecretString;

type Lookup<'a> = dyn Fn(&str) -> Option<String> + 'a;
type ReadFile<'a> = dyn Fn(&Path) -> io::Result<String> + 'a;

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
            checks: Vec::new(),
            aliases: Vec::new(),
            file_alias: None,
        }
    }
}

pub(super) struct Var<'env, 'lookup, T> {
    lookup: &'lookup Lookup<'env>,
    read_file: &'lookup ReadFile<'env>,
    key: &'static str,
    checks: Vec<fn(&'static str, T) -> Result<T>>,
    aliases: Vec<&'static str>,
    file_alias: Option<&'static str>,
}

impl<T> Var<'_, '_, T>
where
    T: EnvParse,
{
    pub(super) fn check(mut self, check: fn(&'static str, T) -> Result<T>) -> Self {
        self.checks.push(check);
        self
    }

    pub(super) fn alias(mut self, alias: &'static str) -> Self {
        self.aliases.push(alias);
        self
    }

    pub(super) fn or_load_from_file(mut self, alias: &'static str) -> Self {
        self.file_alias = Some(alias);
        self
    }

    pub(super) fn required(self) -> Result<T> {
        let value = self
            .load()?
            .with_context(|| format!("{} env variable is required", self.key))?;
        self.parse(value)
    }

    pub(super) fn default(self, default: T) -> Result<T> {
        let Some(value) = self.load()? else {
            return self.validate(self.key, default);
        };
        self.parse(value)
    }

    pub(super) fn optional(self) -> Result<Option<T>> {
        self.load()?.map(|value| self.parse(value)).transpose()
    }

    fn load(&self) -> Result<Option<EnvValue>> {
        for key in iter::once(self.key).chain(self.aliases.iter().copied()) {
            if let Some(raw) = (self.lookup)(key) {
                return Ok(Some(EnvValue { key, raw }));
            }
        }
        let Some(file_key) = self.file_alias else {
            return Ok(None);
        };
        match (self.lookup)(file_key) {
            Some(path) => {
                let raw = (self.read_file)(Path::new(&path)).with_context(|| {
                    format!("{file_key} points to \"{path}\" which could not be read")
                })?;
                Ok(Some(EnvValue {
                    key: file_key,
                    raw: raw.trim().to_owned(),
                }))
            }
            None => Ok(None),
        }
    }

    fn parse(&self, value: EnvValue) -> Result<T> {
        let key = value.key;
        self.validate(key, T::parse(value)?)
    }

    fn validate(&self, key: &'static str, mut value: T) -> Result<T> {
        for check in &self.checks {
            value = check(key, value)?;
        }
        Ok(value)
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

pub(super) fn positive<T>(key: &'static str, value: T) -> Result<T>
where
    T: From<u8> + PartialOrd,
{
    ensure!(value > T::from(0), "{key} must be greater than zero");
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

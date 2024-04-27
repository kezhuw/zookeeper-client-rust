use std::borrow::Cow;

use rsasl::callback::{Context, Request, SessionCallback, SessionData};
use rsasl::mechanisms::gssapi::properties::GssService;
use rsasl::prelude::*;
use rsasl::property::Hostname;

use super::{Result, SaslInitiator, SaslInnerOptions, SaslOptions, SaslSession};

impl From<GssapiSaslOptions> for SaslOptions {
    fn from(options: GssapiSaslOptions) -> Self {
        Self(SaslInnerOptions::Gssapi(options))
    }
}

/// GSSAPI SASL options.
#[derive(Clone, Debug)]
pub struct GssapiSaslOptions {
    username: Cow<'static, str>,
    hostname: Option<Cow<'static, str>>,
}

impl GssapiSaslOptions {
    pub(crate) fn new() -> Self {
        Self { username: Cow::from("zookeeper"), hostname: None }
    }

    /// Specifies the primary part of Kerberos principal.
    ///
    /// It is `zookeeper.sasl.client.username` in Java client, but the word "client" is misleading
    /// as it is the username of targeting server.
    ///
    /// Defaults to "zookeeper".
    pub fn with_username(self, username: impl Into<Cow<'static, str>>) -> Self {
        Self { username: username.into(), ..self }
    }

    /// Specifies the instance part of Kerberos principal.
    ///
    /// Defaults to hostname or ip of targeting server in connecting string.
    pub fn with_hostname(self, hostname: impl Into<Cow<'static, str>>) -> Self {
        Self { hostname: Some(hostname.into()), ..self }
    }

    fn hostname_or(&self, hostname: &str) -> Cow<'static, str> {
        match self.hostname.as_ref() {
            None => Cow::Owned(hostname.to_string()),
            Some(hostname) => hostname.clone(),
        }
    }
}

impl SaslInitiator for GssapiSaslOptions {
    fn new_session(&self, hostname: &str) -> Result<SaslSession> {
        struct GssapiOptionsProvider {
            username: Cow<'static, str>,
            hostname: Cow<'static, str>,
        }
        impl SessionCallback for GssapiOptionsProvider {
            fn callback(
                &self,
                _session_data: &SessionData,
                _context: &Context,
                request: &mut Request<'_>,
            ) -> Result<(), SessionError> {
                if request.is::<Hostname>() {
                    request.satisfy::<Hostname>(&self.hostname)?;
                } else if request.is::<GssService>() {
                    request.satisfy::<GssService>(&self.username)?;
                }
                Ok(())
            }
        }
        let provider = GssapiOptionsProvider { username: self.username.clone(), hostname: self.hostname_or(hostname) };
        let config = SASLConfig::builder().with_defaults().with_callback(provider).unwrap();
        let client = SASLClient::new(config);
        let session = client.start_suggested(&[Mechname::parse(b"GSSAPI").unwrap()]).unwrap();
        SaslSession::new(session)
    }
}

use std::borrow::Cow;

use rsasl::callback::{Context, Request, SessionCallback, SessionData};
use rsasl::prelude::*;
use rsasl::property::{AuthId, Password, Realm};

use super::{Result, SaslInitiator, SaslInnerOptions, SaslOptions, SaslSession};

/// DIGEST-MD5 SASL options.
#[derive(Clone, Debug)]
pub struct DigestMd5SaslOptions {
    realm: Option<Cow<'static, str>>,
    username: Cow<'static, str>,
    password: Cow<'static, str>,
}

impl DigestMd5SaslOptions {
    fn realm(&self) -> Option<&str> {
        self.realm.as_ref().map(|s| s.as_ref())
    }

    pub(crate) fn new(username: impl Into<Cow<'static, str>>, password: impl Into<Cow<'static, str>>) -> Self {
        Self { realm: None, username: username.into(), password: password.into() }
    }

    /// Specifies the client chosen realm.
    #[cfg(test)]
    pub fn with_realm(self, realm: impl Into<Cow<'static, str>>) -> Self {
        Self { realm: Some(realm.into()), ..self }
    }
}

impl From<DigestMd5SaslOptions> for SaslOptions {
    fn from(options: DigestMd5SaslOptions) -> Self {
        Self(SaslInnerOptions::DigestMd5(options))
    }
}

struct DigestSessionCallback {
    options: DigestMd5SaslOptions,
}

impl SessionCallback for DigestSessionCallback {
    fn callback(
        &self,
        _session_data: &SessionData,
        _context: &Context,
        request: &mut Request<'_>,
    ) -> Result<(), SessionError> {
        if request.is::<Realm>() {
            if let Some(realm) = self.options.realm() {
                request.satisfy::<Realm>(realm)?;
            }
        } else if request.is::<AuthId>() {
            request.satisfy::<AuthId>(&self.options.username)?;
        } else if request.is::<Password>() {
            request.satisfy::<Password>(self.options.password.as_bytes())?;
        }
        Ok(())
    }
}

impl SaslInitiator for DigestMd5SaslOptions {
    fn new_session(&self, _hostname: &str) -> Result<SaslSession> {
        let callback = DigestSessionCallback { options: self.clone() };
        let config = SASLConfig::builder().with_defaults().with_callback(callback).unwrap();
        let client = SASLClient::new(config);
        let session = client.start_suggested(&[Mechname::parse(b"DIGEST-MD5").unwrap()]).unwrap();
        SaslSession::new(session)
    }
}

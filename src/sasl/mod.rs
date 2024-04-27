#[allow(unused_imports)]
use std::borrow::Cow;

#[cfg(feature = "sasl-gssapi")]
mod gssapi;
#[cfg(feature = "sasl-gssapi")]
pub use gssapi::*;

#[cfg(feature = "sasl-digest-md5")]
mod digest_md5;
#[cfg(feature = "sasl-digest-md5")]
pub use digest_md5::*;

#[cfg(feature = "sasl-digest-md5")]
mod mechanisms {
    mod digest_md5;
}

use rsasl::prelude::*;

use crate::error::Error;

pub(crate) type Result<T, E = crate::error::Error> = std::result::Result<T, E>;

pub(crate) trait SaslInitiator {
    fn new_session(&self, hostname: &str) -> Result<SaslSession>;
}

/// Client side SASL options.
#[derive(Clone, Debug)]
pub struct SaslOptions(SaslInnerOptions);

#[derive(Clone, Debug)]
enum SaslInnerOptions {
    #[cfg(feature = "sasl-gssapi")]
    Gssapi(GssapiSaslOptions),
    #[cfg(feature = "sasl-digest-md5")]
    DigestMd5(DigestMd5SaslOptions),
}

impl SaslOptions {
    /// Constructs a default [GssapiSaslOptions] for further customization.
    ///
    /// Make sure localhost is granted by Kerberos KDC, unlike Java counterpart this library
    /// provides no mean to grant ticket from KDC but simply utilizes whatever the ticket cache
    /// have.
    #[cfg(feature = "sasl-gssapi")]
    pub fn gssapi() -> GssapiSaslOptions {
        GssapiSaslOptions::new()
    }

    /// Construct a [DigestMd5SaslOptions] for further customization.
    #[cfg(feature = "sasl-digest-md5")]
    pub fn digest_md5(
        username: impl Into<Cow<'static, str>>,
        password: impl Into<Cow<'static, str>>,
    ) -> DigestMd5SaslOptions {
        DigestMd5SaslOptions::new(username, password)
    }
}

impl SaslInitiator for SaslOptions {
    fn new_session(&self, hostname: &str) -> Result<SaslSession> {
        match &self.0 {
            #[cfg(feature = "sasl-digest-md5")]
            SaslInnerOptions::DigestMd5(options) => options.new_session(hostname),
            #[cfg(feature = "sasl-gssapi")]
            SaslInnerOptions::Gssapi(options) => options.new_session(hostname),
        }
    }
}

pub struct SaslSession {
    output: Vec<u8>,
    session: Session,
    finished: bool,
}

impl SaslSession {
    fn new(session: Session) -> Result<Self> {
        let mut session = Self { session, output: Default::default(), finished: false };
        if session.session.are_we_first() {
            session.step(Default::default())?;
        }
        Ok(session)
    }

    pub fn name(&self) -> &str {
        self.session.get_mechname().as_str()
    }

    pub fn initial(&self) -> &[u8] {
        &self.output
    }

    pub fn step(&mut self, challenge: &[u8]) -> Result<Option<&[u8]>> {
        if self.finished {
            return Err(Error::UnexpectedError(format!("SASL {} session already finished", self.name())));
        }
        self.output.clear();
        match self.session.step(Some(challenge), &mut self.output).map_err(|e| Error::other(format!("{e}"), e))? {
            State::Running => Ok(Some(&self.output)),
            State::Finished(MessageSent::Yes) => {
                self.finished = true;
                Ok(Some(&self.output))
            },
            State::Finished(MessageSent::No) => {
                self.finished = true;
                Ok(None)
            },
        }
    }
}

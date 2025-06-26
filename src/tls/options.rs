use std::sync::{Arc, RwLock};

use derive_where::derive_where;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::RootCertStore;

use super::TlsClient;
use crate::client::Result;
use crate::Error;

/// A CA signed certificate.
#[derive(PartialEq, Eq)]
#[derive_where(Debug)]
#[non_exhaustive]
pub(super) struct TlsIdentity {
    /// CA signed certificate.
    pub cert: Vec<CertificateDer<'static>>,

    /// Key to certificate.
    #[derive_where(skip)]
    pub key: PrivateKeyDer<'static>,
}

impl TlsIdentity {
    pub fn from_pem(cert: &str, key: &str) -> Result<Self> {
        let r: Result<Vec<_>, _> = rustls_pemfile::certs(&mut cert.as_bytes()).collect();
        let cert = match r {
            Err(err) => return Err(Error::with_other("fail to read cert", err)),
            Ok(cert) => cert,
        };
        let key = match rustls_pemfile::private_key(&mut key.as_bytes()) {
            Err(err) => return Err(Error::with_other("fail to read client private key", err)),
            Ok(None) => return Err(Error::BadArguments(&"no client private key")),
            Ok(Some(key)) => key,
        };
        Ok(Self { cert, key })
    }
}

impl Clone for TlsIdentity {
    fn clone(&self) -> Self {
        Self { cert: self.cert.clone(), key: self.key.clone_key() }
    }
}

/// Certificates used by client to authenticate with server.
#[derive(Clone, Debug, Default)]
pub struct TlsCerts {
    /// Optional client side identity.
    pub(super) client: Option<TlsIdentity>,
}

impl TlsCerts {
    /// Constructs an empty tls certs.
    pub fn new() -> Self {
        Self::default()
    }

    /// Specifies client identity for server to authenticate.
    pub fn with_pem_identity(mut self, cert: &str, key: &str) -> Result<Self> {
        self.client = Some(TlsIdentity::from_pem(cert, key)?);
        Ok(self)
    }
}

/// Options to carry [TlsCerts].
#[derive(Clone, Debug)]
pub struct TlsCertsOptions {
    certs: TlsInnerCerts,
}

#[derive(Clone, Debug)]
enum TlsInnerCerts {
    Static(TlsCerts),
    Dynamic(TlsDynamicCerts),
}

impl From<TlsCertsOptions> for TlsInnerCerts {
    fn from(options: TlsCertsOptions) -> Self {
        options.certs
    }
}

impl From<TlsInnerCerts> for TlsCertsOptions {
    fn from(certs: TlsInnerCerts) -> Self {
        Self { certs }
    }
}

impl From<TlsCerts> for TlsCertsOptions {
    fn from(certs: TlsCerts) -> Self {
        TlsInnerCerts::Static(certs).into()
    }
}

impl From<TlsDynamicCerts> for TlsCertsOptions {
    fn from(certs: TlsDynamicCerts) -> Self {
        TlsInnerCerts::Dynamic(certs).into()
    }
}

/// Cell to keep up to date [TlsCerts].
#[derive(Clone, Debug)]
pub struct TlsDynamicCerts {
    certs: Arc<RwLock<(u64, Arc<TlsCerts>)>>,
}

impl TlsDynamicCerts {
    /// Constructs [TlsDynamicCerts] with certs.
    pub fn new(certs: TlsCerts) -> Self {
        let certs = certs.into();
        Self { certs: Arc::new(RwLock::new((1, certs))) }
    }

    /// Updates with newer certs.
    pub fn update(&self, certs: TlsCerts) {
        let certs = certs.into();
        let mut writer = self.certs.write().unwrap();
        writer.0 += 1;
        let old = std::mem::replace(&mut writer.1, certs);
        drop(writer);
        drop(old);
    }

    pub(crate) fn get_versioned(&self) -> (u64, Arc<TlsCerts>) {
        self.certs.read().unwrap().clone()
    }

    pub(crate) fn get_updated(&self, version: u64) -> Option<(u64, Arc<TlsCerts>)> {
        let locked = self.certs.read().unwrap();
        if version >= locked.0 {
            return None;
        }
        Some(locked.clone())
    }
}

/// Options for tls connection.
#[derive(Clone, Debug)]
pub struct TlsOptions {
    identity: Option<TlsIdentity>,
    ca_certs: RootCertStore,
    certs: Option<TlsCertsOptions>,
    hostname_verification: bool,
}

impl Default for TlsOptions {
    /// Same as [Self::new].
    fn default() -> Self {
        Self::new()
    }
}

impl TlsOptions {
    /// Tls options with no ca certificates.
    #[deprecated(since = "0.10.0", note = "use TlsOptions::new instead")]
    pub fn no_ca() -> Self {
        Self::new()
    }

    /// Tls options with no ca certificates.
    pub fn new() -> Self {
        Self { ca_certs: RootCertStore::empty(), identity: None, certs: None, hostname_verification: true }
    }

    /// Trusts root certificates trusted by Mozilla.
    ///
    /// See [webpki-roots](https://docs.rs/webpki-roots) for more.
    #[cfg(feature = "tls-mozilla-roots")]
    pub fn with_mozilla_roots(mut self) -> Self {
        self.ca_certs.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        self
    }

    /// Disables hostname verification in tls handshake.
    ///
    /// # Safety
    /// This exposes risk to man-in-the-middle attacks.
    pub unsafe fn with_no_hostname_verification(mut self) -> Self {
        self.hostname_verification = false;
        self
    }

    /// Adds new ca certificates.
    pub fn with_pem_ca_certs(mut self, certs: &str) -> Result<Self> {
        for r in rustls_pemfile::certs(&mut certs.as_bytes()) {
            let cert = match r {
                Ok(cert) => cert,
                Err(err) => return Err(Error::with_other("fail to read cert", err)),
            };
            if let Err(err) = self.ca_certs.add(cert) {
                return Err(Error::with_other("fail to add cert", err));
            }
        }
        Ok(self)
    }

    /// Specifies client identity for server to authenticate.
    pub fn with_pem_identity(mut self, cert: &str, key: &str) -> Result<Self> {
        self.identity = Some(TlsIdentity::from_pem(cert, key)?);
        Ok(self)
    }

    /// Specifies certificates to connection to server. This takes precedence over
    /// [TlsOptions::with_pem_identity].
    pub fn with_certs(mut self, certs: impl Into<TlsCertsOptions>) -> Self {
        self.certs = Some(certs.into());
        self
    }

    pub(crate) fn into_client(mut self) -> Result<TlsClient> {
        let ca_certs = std::mem::replace(&mut self.ca_certs, RootCertStore::empty());
        let hostname_verification = self.hostname_verification;
        match self.certs.map(TlsInnerCerts::from) {
            None => TlsClient::new_static(ca_certs, self.identity, hostname_verification),
            Some(TlsInnerCerts::Static(certs)) => TlsClient::new_static(ca_certs, certs.client, hostname_verification),
            Some(TlsInnerCerts::Dynamic(certs)) => TlsClient::new_dynamic(ca_certs, certs, hostname_verification),
        }
    }
}

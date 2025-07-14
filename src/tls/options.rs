use std::sync::{Arc, RwLock};

use derive_where::derive_where;
use ignore_result::Ignore;
use rustls::pki_types::{CertificateDer, CertificateRevocationListDer, PrivateKeyDer};
use rustls::RootCertStore;

use super::TlsClient;
use crate::client::Result;
use crate::Error;

type PemItem = rustls_pemfile::Item;

/// Ca certificates and crls to authenticate peer.
#[derive(Clone, Debug)]
pub struct TlsCa {
    pub(super) roots: RootCertStore,
    pub(super) crls: Vec<CertificateRevocationListDer<'static>>,
}

impl TlsCa {
    /// Constructs [TlsCa] from pem.
    pub fn from_pem(pem: &str) -> Result<Self> {
        let mut ca = Self { roots: RootCertStore::empty(), crls: Vec::new() };
        for r in rustls_pemfile::read_all(&mut pem.as_bytes()) {
            match r {
                Ok(PemItem::X509Certificate(cert)) => ca.roots.add(cert).ignore(),
                Ok(PemItem::Crl(crl)) => ca.crls.push(crl),
                Ok(_) => continue,
                Err(err) => return Err(Error::with_other("fail to read ca", err)),
            }
        }
        if ca.roots.is_empty() {
            return Err(Error::BadArguments(&"no valid tls trust anchor in pem"));
        }
        Ok(ca)
    }

    fn merge(&mut self, ca: TlsCa) {
        self.roots.roots.extend(ca.roots.roots);
        self.crls.extend(ca.crls);
    }
}

/// A CA signed certificate and its private key.
#[derive_where(Debug)]
pub struct TlsIdentity {
    /// CA signed certificate.
    pub(super) cert: Vec<CertificateDer<'static>>,

    /// Key to certificate.
    #[derive_where(skip)]
    pub(super) key: PrivateKeyDer<'static>,
}

impl TlsIdentity {
    /// Constructs [TlsIdentity] from pem.
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
#[derive(Clone, Debug)]
pub struct TlsCerts {
    /// Ca to authenticate server.
    pub(super) ca: TlsCa,
    /// Optional client side identity for server to authenticate.
    pub(super) identity: Option<TlsIdentity>,
}

impl TlsCerts {
    /// Constructs a builder to build [TlsCerts].
    pub fn builder() -> TlsCertsBuilder {
        TlsCertsBuilder::new()
    }
}

/// Builder to construct [TlsCerts].
#[derive(Clone, Debug)]
pub struct TlsCertsBuilder {
    ca: Option<TlsCa>,
    /// Optional client side identity.
    identity: Option<TlsIdentity>,
}

impl TlsCertsBuilder {
    /// Constructs an empty builder.
    fn new() -> Self {
        Self { ca: None, identity: None }
    }

    /// Specifies ca certificates and also crls.
    pub fn with_ca(mut self, ca: TlsCa) -> Self {
        self.ca = Some(ca);
        self
    }

    /// Specifies client identity for server to authenticate.
    pub fn with_identity(mut self, identity: TlsIdentity) -> Self {
        self.identity = Some(identity);
        self
    }

    /// Builds [TlsCerts] and fails if no ca specified.
    pub fn build(self) -> Result<TlsCerts> {
        let ca = match self.ca {
            None => return Err(Error::BadArguments(&"no tls ca")),
            Some(ca) => ca,
        };
        Ok(TlsCerts { ca, identity: self.identity })
    }
}

/// Options to carry [TlsCerts].
#[derive(Clone, Debug)]
pub struct TlsCertsOptions {
    certs: TlsInnerCerts,
}

#[derive(Clone, Debug)]
pub(super) enum TlsInnerCerts {
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
///
/// [TlsDynamicCerts] by itself are concurrent safe in updating certs, but concurrency implies
/// uncertainty which means you won't known which one will last.
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

    /// Updates with newer ca certificates.
    pub fn update_ca(&self, ca: TlsCa) {
        self.update_partially(|certs| certs.ca = ca.clone())
    }

    /// Updates with newer client tls identity.
    pub fn update_identity(&self, identity: Option<TlsIdentity>) {
        self.update_partially(|certs| certs.identity = identity.clone())
    }

    fn update_versioned(&self, version: u64, certs: TlsCerts) -> bool {
        let certs = certs.into();
        let mut writer = self.certs.write().unwrap();
        if writer.0 != version {
            return false;
        }
        writer.0 += 1;
        let old = std::mem::replace(&mut writer.1, certs);
        drop(writer);
        drop(old);
        true
    }

    fn update_partially(&self, update: impl Fn(&mut TlsCerts)) {
        loop {
            let (version, certs) = self.get_versioned();
            let mut certs = (*certs).clone();
            update(&mut certs);
            if self.update_versioned(version, certs) {
                break;
            }
        }
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
    ca: Option<TlsCa>,
    identity: Option<TlsIdentity>,
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
        Self { ca: None, identity: None, certs: None, hostname_verification: true }
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
    ///
    /// It behaves different to [TlsOptions::with_pem_ca] in two ways:
    /// 1. It is additive.
    /// 2. It takes only certs into account.
    #[deprecated(since = "0.10.0", note = "use TlsOptions::with_pem_ca instead")]
    pub fn with_pem_ca_certs(mut self, certs: &str) -> Result<Self> {
        let mut ca = TlsCa::from_pem(certs)?;
        ca.crls.clear();
        match self.ca.as_mut() {
            None => self.ca = Some(ca),
            Some(existing_ca) => existing_ca.merge(ca),
        };
        Ok(self)
    }

    /// Specifies ca certificates and also crls.
    ///
    /// See also [TlsCa::from_pem].
    pub fn with_pem_ca(mut self, ca: &str) -> Result<Self> {
        self.ca = Some(TlsCa::from_pem(ca)?);
        Ok(self)
    }

    /// Specifies client identity for server to authenticate.
    ///
    /// See also [TlsIdentity::from_pem].
    pub fn with_pem_identity(mut self, cert: &str, key: &str) -> Result<Self> {
        self.identity = Some(TlsIdentity::from_pem(cert, key)?);
        Ok(self)
    }

    /// Specifies certificates to connection to server. This takes precedence over
    /// [TlsOptions::with_pem_identity] and [TlsOptions::with_pem_ca].
    pub fn with_certs(mut self, certs: impl Into<TlsCertsOptions>) -> Self {
        self.certs = Some(certs.into());
        self
    }

    pub(crate) fn into_client(self) -> Result<TlsClient> {
        let certs = match self.certs.map(TlsInnerCerts::from) {
            None => {
                let certs = TlsCertsBuilder { ca: self.ca, identity: self.identity }.build()?;
                TlsInnerCerts::Static(certs)
            },
            Some(certs) => certs,
        };
        let options = TlsClientOptions { certs, hostname_verification: self.hostname_verification };
        TlsClient::new(options)
    }
}

pub(super) struct TlsClientOptions<Certs> {
    pub certs: Certs,
    pub hostname_verification: bool,
}

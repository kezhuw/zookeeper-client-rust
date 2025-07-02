use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::ParsedCertificate;
use rustls::{ClientConfig, DigitallySignedStruct, Error as TlsError, RootCertStore, SignatureScheme};

use crate::client::Result;
use crate::Error;

/// Options for tls connection.
#[derive(Debug)]
pub struct TlsOptions {
    identity: Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>,
    ca_certs: RootCertStore,
    hostname_verification: bool,
}

impl Clone for TlsOptions {
    fn clone(&self) -> Self {
        Self {
            identity: self.identity.as_ref().map(|id| (id.0.clone(), id.1.clone_key())),
            ca_certs: self.ca_certs.clone(),
            hostname_verification: self.hostname_verification,
        }
    }
}

impl Default for TlsOptions {
    /// Same as [Self::new].
    fn default() -> Self {
        Self::new()
    }
}

// Rustls tends to make disable of hostname verification verbose since it exposes man-in-the-middle
// attacks. Though, there are still attempts to disable hostname verification in rustls, but no got
// merged until now.
// * Allow disabling Hostname Verification: https://github.com/rustls/rustls/issues/578
// * Dangerous verifiers API proposal: https://github.com/rustls/rustls/pull/1197
#[derive(Debug)]
struct TlsServerCertVerifier {
    roots: RootCertStore,
    supported: WebPkiSupportedAlgorithms,
    hostname_verification: bool,
}

impl TlsServerCertVerifier {
    fn new(roots: RootCertStore, hostname_verification: bool) -> Self {
        Self {
            roots,
            supported: CryptoProvider::get_default().unwrap().signature_verification_algorithms,
            hostname_verification,
        }
    }
}

impl ServerCertVerifier for TlsServerCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, TlsError> {
        let cert = ParsedCertificate::try_from(end_entity)?;
        rustls::client::verify_server_cert_signed_by_trust_anchor(
            &cert,
            &self.roots,
            intermediates,
            now,
            self.supported.all,
        )?;

        if self.hostname_verification {
            rustls::client::verify_server_name(&cert, server_name)?;
        }
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.supported)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.supported)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.supported.supported_schemes()
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
        Self { ca_certs: RootCertStore::empty(), identity: None, hostname_verification: true }
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
        let r: std::result::Result<Vec<_>, _> = rustls_pemfile::certs(&mut cert.as_bytes()).collect();
        let certs = match r {
            Err(err) => return Err(Error::with_other("fail to read cert", err)),
            Ok(certs) => certs,
        };
        let key = match rustls_pemfile::private_key(&mut key.as_bytes()) {
            Err(err) => return Err(Error::with_other("fail to read client private key", err)),
            Ok(None) => return Err(Error::BadArguments(&"no client private key")),
            Ok(Some(key)) => key,
        };
        self.identity = Some((certs, key));
        Ok(self)
    }

    fn take_roots(&mut self) -> RootCertStore {
        std::mem::replace(&mut self.ca_certs, RootCertStore::empty())
    }

    pub(crate) fn into_config(mut self) -> Result<ClientConfig> {
        // This has to be called before server cert verifier to install default crypto provider.
        let builder = ClientConfig::builder();
        let verifier = TlsServerCertVerifier::new(self.take_roots(), self.hostname_verification);
        let builder = builder.dangerous().with_custom_certificate_verifier(Arc::new(verifier));
        if let Some((client_cert, client_key)) = self.identity.take() {
            match builder.with_client_auth_cert(client_cert, client_key) {
                Ok(config) => Ok(config),
                Err(err) => Err(Error::with_other("invalid client private key", err)),
            }
        } else {
            Ok(builder.with_no_client_auth())
        }
    }
}

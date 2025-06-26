use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::server::ParsedCertificate;
use rustls::{DigitallySignedStruct, Error as TlsError, RootCertStore, SignatureScheme};

use crate::client::Result;

// Rustls tends to make disable of hostname verification verbose since it exposes man-in-the-middle
// attacks. Though, there are still attempts to disable hostname verification in rustls, but no got
// merged until now.
// * Allow disabling Hostname Verification: https://github.com/rustls/rustls/issues/578
// * Dangerous verifiers API proposal: https://github.com/rustls/rustls/pull/1197
#[derive(Debug)]
pub(super) struct NoHostnameVerificationServerCertVerifier {
    roots: RootCertStore,
    supported: WebPkiSupportedAlgorithms,
}

impl NoHostnameVerificationServerCertVerifier {
    pub unsafe fn new(roots: RootCertStore) -> Self {
        Self { roots, supported: CryptoProvider::get_default().unwrap().signature_verification_algorithms }
    }
}

impl ServerCertVerifier for NoHostnameVerificationServerCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
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

use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms};
use rustls::pki_types::{CertificateDer, CertificateRevocationListDer, ServerName, UnixTime};
use rustls::{
    CertRevocationListError,
    CertificateError,
    DigitallySignedStruct,
    Error as TlsError,
    ExtendedKeyPurpose,
    OtherError,
    RootCertStore,
    SignatureScheme,
};
use webpki::{
    BorrowedCertRevocationList,
    CertRevocationList,
    EndEntityCert,
    InvalidNameContext,
    KeyUsage,
    RevocationOptionsBuilder,
};

use crate::client::Result;

// Rustls tends to make disable of hostname verification verbose since it exposes man-in-the-middle
// attacks. Though, there are still attempts to disable hostname verification in rustls, but no got
// merged until now.
// * Allow disabling Hostname Verification: https://github.com/rustls/rustls/issues/578
// * Dangerous verifiers API proposal: https://github.com/rustls/rustls/pull/1197
#[derive(Debug)]
pub(super) struct NoHostnameVerificationServerCertVerifier {
    roots: RootCertStore,
    crls: Vec<CertRevocationList<'static>>,
    supported: WebPkiSupportedAlgorithms,
}

impl NoHostnameVerificationServerCertVerifier {
    pub unsafe fn new(
        roots: RootCertStore,
        crls: Vec<CertificateRevocationListDer<'static>>,
        provider: &Arc<CryptoProvider>,
    ) -> Self {
        let crls: Vec<_> = crls
            .iter()
            .map(|crl| BorrowedCertRevocationList::from_der(crl.as_ref()).unwrap().to_owned().unwrap())
            .map(CertRevocationList::Owned)
            .collect();
        Self { roots, crls, supported: provider.signature_verification_algorithms }
    }
}

fn extended_key_purpose(values: impl Iterator<Item = usize>) -> ExtendedKeyPurpose {
    let values = values.collect::<Vec<_>>();
    match &*values {
        KeyUsage::CLIENT_AUTH_REPR => ExtendedKeyPurpose::ClientAuth,
        KeyUsage::SERVER_AUTH_REPR => ExtendedKeyPurpose::ServerAuth,
        _ => ExtendedKeyPurpose::Other(values),
    }
}

// Copied from https://github.com/rustls/rustls/blob/v/0.23.29/rustls/src/webpki/mod.rs#L59
// LICENSE: https://github.com/rustls/rustls/blob/v/0.23.29/LICENSE (any of Apache 2.0, MIT and ISC)
fn pki_error(error: webpki::Error) -> TlsError {
    use webpki::Error::*;
    match error {
        BadDer | BadDerTime | TrailingData(_) => CertificateError::BadEncoding.into(),
        CertNotValidYet { time, not_before } => CertificateError::NotValidYetContext { time, not_before }.into(),
        CertExpired { time, not_after } => CertificateError::ExpiredContext { time, not_after }.into(),
        InvalidCertValidity => CertificateError::Expired.into(),
        UnknownIssuer => CertificateError::UnknownIssuer.into(),
        CertNotValidForName(InvalidNameContext { expected, presented }) => {
            CertificateError::NotValidForNameContext { expected, presented }.into()
        },
        CertRevoked => CertificateError::Revoked.into(),
        UnknownRevocationStatus => CertificateError::UnknownRevocationStatus.into(),
        CrlExpired { time, next_update } => CertificateError::ExpiredRevocationListContext { time, next_update }.into(),
        IssuerNotCrlSigner => CertRevocationListError::IssuerInvalidForCrl.into(),

        InvalidSignatureForPublicKey => CertificateError::BadSignature.into(),
        #[allow(deprecated)]
        UnsupportedSignatureAlgorithm
        | UnsupportedSignatureAlgorithmContext(_)
        | UnsupportedSignatureAlgorithmForPublicKey => CertificateError::UnsupportedSignatureAlgorithm.into(),

        InvalidCrlSignatureForPublicKey => CertRevocationListError::BadSignature.into(),
        #[allow(deprecated)]
        UnsupportedCrlSignatureAlgorithm
        | UnsupportedCrlSignatureAlgorithmContext(_)
        | UnsupportedCrlSignatureAlgorithmForPublicKey => CertRevocationListError::UnsupportedSignatureAlgorithm.into(),

        #[allow(deprecated)]
        RequiredEkuNotFound => CertificateError::InvalidPurpose.into(),
        RequiredEkuNotFoundContext(webpki::RequiredEkuNotFoundContext { required, present }) => {
            CertificateError::InvalidPurposeContext {
                required: extended_key_purpose(required.oid_values()),
                presented: present.into_iter().map(|eku| extended_key_purpose(eku.into_iter())).collect(),
            }
            .into()
        },

        _ => CertificateError::Other(OtherError(Arc::new(error))).into(),
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
        let cert = EndEntityCert::try_from(end_entity).map_err(pki_error)?;
        let crls = self.crls.iter().collect::<Vec<_>>();
        let revocation = match RevocationOptionsBuilder::new(&crls) {
            Err(_) => None,
            Ok(builder) => Some(builder.build()),
        };
        cert.verify_for_usage(
            self.supported.all,
            &self.roots.roots,
            intermediates,
            now,
            webpki::KeyUsage::server_auth(),
            revocation,
            None,
        )
        .map_err(pki_error)?;
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

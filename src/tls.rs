use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use futures::lock::Mutex;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::ParsedCertificate;
use rustls::{ClientConfig, DigitallySignedStruct, Error as TlsError, RootCertStore, SignatureScheme};

use crate::client::Result;
use crate::Error;

#[derive(Debug)]
struct FileProvider {
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
    cert_path: PathBuf,
    key_path: PathBuf,
    cert_modified: SystemTime,
    key_modified: SystemTime,
}

impl FileProvider {
    async fn new(cert_path: PathBuf, key_path: PathBuf) -> Result<Self> {
        let (certs, key) = load_certificates_from_files(&cert_path, &key_path).await?;
        let (cert_modified, key_modified) = get_file_timestamps(&cert_path, &key_path).await?;
        Ok(Self { certs, key, cert_path, key_path, cert_modified, key_modified })
    }

    async fn update_and_fetch(&mut self) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
        let (cert_modified, key_modified) = get_file_timestamps(&self.cert_path, &self.key_path).await?;
        let cert_changed = cert_modified > self.cert_modified;
        let key_changed = key_modified > self.key_modified;
        // Refresh if both files were modified, as we want to make sure that we don't pick up a new cert/key with
        // an old key/cert.
        if cert_changed && key_changed {
            tracing::debug!("Reloading client certificates");
            match load_certificates_from_files(&self.cert_path, &self.key_path).await {
                Err(e) => tracing::warn!("Failed to reload certificates, keeping existing ones: {}", e),
                Ok((certs, key)) => {
                    tracing::info!("Reloaded client certificates");
                    println!("Reloaded client certificates");
                    self.cert_modified = cert_modified;
                    self.key_modified = key_modified;
                    self.certs = certs;
                    self.key = key;
                },
            }
        }
        Ok((self.certs.clone(), self.key.clone_key()))
    }
}

async fn load_certificates_from_files(
    cert_path: &Path,
    key_path: &Path,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let cert_content = async_fs::read_to_string(cert_path)
        .await
        .map_err(|e| Error::with_other("Failed to read certificate file", e))?;
    let key_content =
        async_fs::read_to_string(key_path).await.map_err(|e| Error::with_other("Failed to read key file", e))?;
    parse_pem_identity(&cert_content, &key_content)
}

async fn get_file_timestamps(cert_path: &Path, key_path: &Path) -> Result<(SystemTime, SystemTime)> {
    let cert_metadata = async_fs::metadata(cert_path)
        .await
        .map_err(|e| Error::with_other("Failed to get certificate file metadata", e))?;
    let key_metadata =
        async_fs::metadata(key_path).await.map_err(|e| Error::with_other("Failed to get key file metadata", e))?;

    let cert_modified = cert_metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
    let key_modified = key_metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
    Ok((cert_modified, key_modified))
}

#[derive(Debug)]
enum IdentityProvider {
    Static { certs: Vec<CertificateDer<'static>>, key: PrivateKeyDer<'static> },
    FileBased { provider: Arc<Mutex<FileProvider>> },
}

impl IdentityProvider {
    pub async fn check_and_reload_certificates(
        &self,
    ) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
        match self {
            IdentityProvider::Static { certs, key } => Ok((certs.clone(), key.clone_key())),
            IdentityProvider::FileBased { provider } => provider.lock().await.update_and_fetch().await,
        }
    }
}

impl Clone for IdentityProvider {
    fn clone(&self) -> Self {
        match self {
            IdentityProvider::Static { certs, key } => {
                IdentityProvider::Static { certs: certs.clone(), key: key.clone_key() }
            },
            provider @ IdentityProvider::FileBased { .. } => provider.clone(),
        }
    }
}

/// Options for tls connection.
#[derive(Debug, Clone)]
pub struct TlsOptions {
    identity_provider: Option<IdentityProvider>,
    ca_certs: RootCertStore,
    hostname_verification: bool,
}

impl Default for TlsOptions {
    /// Tls options with well-known ca roots.
    fn default() -> Self {
        let mut options = Self::no_ca();
        options.ca_certs.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        options
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

/// Helper function to parse certificate and key content from strings
fn parse_pem_identity(
    cert_content: &str,
    key_content: &str,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let r: std::result::Result<Vec<_>, _> = rustls_pemfile::certs(&mut cert_content.as_bytes()).collect();
    let certs = match r {
        Err(err) => return Err(Error::with_other("fail to read cert", err)),
        Ok(certs) => certs,
    };
    let key = match rustls_pemfile::private_key(&mut key_content.as_bytes()) {
        Err(err) => return Err(Error::with_other("fail to read client private key", err)),
        Ok(None) => return Err(Error::BadArguments(&"no client private key")),
        Ok(Some(key)) => key,
    };
    Ok((certs, key))
}

impl TlsOptions {
    /// Tls options with no ca certificates. Use [TlsOptions::default] if well-known ca roots is
    /// desirable.
    pub fn no_ca() -> Self {
        Self { ca_certs: RootCertStore::empty(), identity_provider: None, hostname_verification: true }
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
        let (certs, key) = parse_pem_identity(cert, key)?;
        self.identity_provider = Some(IdentityProvider::Static { certs, key });
        Ok(self)
    }

    /// Specifies client identity from file paths with automatic reloading on file changes when
    /// reconnections take place.
    pub async fn with_pem_identity_files(
        mut self,
        cert_path: impl Into<PathBuf>,
        key_path: impl Into<PathBuf>,
    ) -> Result<Self> {
        let cert_path = cert_path.into();
        let key_path = key_path.into();

        let file_provider = FileProvider::new(cert_path, key_path).await?;
        self.identity_provider = Some(IdentityProvider::FileBased { provider: Arc::new(Mutex::new(file_provider)) });

        Ok(self)
    }

    pub(crate) async fn to_config(&self) -> Result<ClientConfig> {
        let builder = ClientConfig::builder();
        let verifier = TlsServerCertVerifier::new(self.ca_certs.clone(), self.hostname_verification);
        let builder = builder.dangerous().with_custom_certificate_verifier(Arc::new(verifier));
        if let Some(identity_provider) = &self.identity_provider {
            let (client_cert, client_key) = identity_provider.check_and_reload_certificates().await?;
            match builder.with_client_auth_cert(client_cert, client_key) {
                Ok(config) => Ok(config),
                Err(err) => Err(Error::with_other("invalid client private key", err)),
            }
        } else {
            Ok(builder.with_no_client_auth())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use std::{fs, thread};

    use rcgen::{Certificate, CertificateParams};
    use tempfile::TempDir;

    use super::*;

    fn generate_test_cert_and_key() -> (String, String) {
        let mut params = CertificateParams::new(vec!["localhost".to_string()]);
        params.alg = &rcgen::PKCS_ECDSA_P256_SHA256;

        let cert = Certificate::from_params(params).unwrap();
        let cert_pem = cert.serialize_pem().unwrap();
        let key_pem = cert.serialize_private_key_pem();

        (cert_pem, key_pem)
    }

    #[asyncs::test]
    async fn test_with_pem_identity_files() {
        let temp_dir = TempDir::new().unwrap();
        let cert_path = temp_dir.path().join("test.crt");
        let key_path = temp_dir.path().join("test.key");

        // Generate valid test certificates
        let (cert_pem, key_pem) = generate_test_cert_and_key();
        fs::write(&cert_path, &cert_pem).unwrap();
        fs::write(&key_path, &key_pem).unwrap();

        // Test loading certificates from files
        let tls_options = TlsOptions::default().with_pem_identity_files(&cert_path, &key_path).await.unwrap();

        // Verify that identity was loaded
        assert!(tls_options.identity_provider.is_some());
    }

    #[asyncs::test]
    async fn test_with_pem_identity_files_missing_cert() {
        let temp_dir = TempDir::new().unwrap();
        let cert_path = temp_dir.path().join("missing.crt");
        let key_path = temp_dir.path().join("test.key");

        let (_, key_pem) = generate_test_cert_and_key();
        fs::write(&key_path, &key_pem).unwrap();

        // Should fail when certificate file is missing
        let result = TlsOptions::default().with_pem_identity_files(&cert_path, &key_path).await;

        assert!(result.is_err());
    }

    #[asyncs::test]
    async fn test_with_pem_identity_files_missing_key() {
        let temp_dir = TempDir::new().unwrap();
        let cert_path = temp_dir.path().join("test.crt");
        let key_path = temp_dir.path().join("missing.key");

        let (cert_pem, _) = generate_test_cert_and_key();
        fs::write(&cert_path, &cert_pem).unwrap();

        // Should fail when key file is missing
        let result = TlsOptions::default().with_pem_identity_files(&cert_path, &key_path).await;

        assert!(result.is_err());
    }

    #[asyncs::test]
    async fn test_check_and_reload_certificates_no_changes() {
        let temp_dir = TempDir::new().unwrap();
        let cert_path = temp_dir.path().join("test.crt");
        let key_path = temp_dir.path().join("test.key");

        let (cert_pem, key_pem) = generate_test_cert_and_key();
        fs::write(&cert_path, &cert_pem).unwrap();
        fs::write(&key_path, &key_pem).unwrap();

        let tls_options = TlsOptions::default().with_pem_identity_files(&cert_path, &key_path).await.unwrap();

        let (cert_1, key_1) =
            tls_options.identity_provider.as_ref().unwrap().check_and_reload_certificates().await.unwrap();
        let (cert_2, key_2) =
            tls_options.identity_provider.as_ref().unwrap().check_and_reload_certificates().await.unwrap();
        assert_eq!(cert_1, cert_2);
        assert_eq!(key_1, key_2);
    }

    #[asyncs::test]
    async fn test_check_and_reload_certificates_key_changes() {
        let temp_dir = TempDir::new().unwrap();
        let cert_path = temp_dir.path().join("test.crt");
        let key_path = temp_dir.path().join("test.key");

        let (cert_pem, key_pem) = generate_test_cert_and_key();
        fs::write(&cert_path, &cert_pem).unwrap();
        fs::write(&key_path, &key_pem).unwrap();

        let tls_options = TlsOptions::default().with_pem_identity_files(&cert_path, &key_path).await.unwrap();
        let (cert_1, key_1) = tls_options.identity_provider.unwrap().check_and_reload_certificates().await.unwrap();

        // Sleep to ensure different modification time
        thread::sleep(Duration::from_millis(50));

        // Update the key file with new content (must update both cert and key for valid pair)
        let (new_cert_pem, new_key_pem) = generate_test_cert_and_key();
        fs::write(&cert_path, &new_cert_pem).unwrap();
        fs::write(&key_path, &new_key_pem).unwrap();

        let tls_options = TlsOptions::default().with_pem_identity_files(&cert_path, &key_path).await.unwrap();
        let (cert_2, key_2) = tls_options.identity_provider.unwrap().check_and_reload_certificates().await.unwrap();
        assert!(cert_1 != cert_2);
        assert!(key_1 != key_2);
    }

    #[asyncs::test]
    async fn test_into_config_with_file_based_certs() {
        let temp_dir = TempDir::new().unwrap();
        let cert_path = temp_dir.path().join("test.crt");
        let key_path = temp_dir.path().join("test.key");

        let (cert_pem, key_pem) = generate_test_cert_and_key();
        fs::write(&cert_path, &cert_pem).unwrap();
        fs::write(&key_path, &key_pem).unwrap();

        let tls_options = TlsOptions::default().with_pem_identity_files(&cert_path, &key_path).await.unwrap();

        // Should be able to create a valid ClientConfig
        let config = tls_options.to_config().await;
        assert!(config.is_ok());
    }
}

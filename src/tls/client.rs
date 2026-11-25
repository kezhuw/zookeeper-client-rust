use std::sync::{Arc, RwLock};

use async_net::TcpStream;
use futures_rustls::client::TlsStream;
use futures_rustls::TlsConnector;
use rustls::client::WebPkiServerVerifier;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ConfigBuilder, WantsVerifier};
use tracing::warn;

use super::{NoHostnameVerificationServerCertVerifier, TlsCerts, TlsClientOptions, TlsDynamicCerts, TlsInnerCerts};
use crate::client::Result;
use crate::error::Error;

struct TlsDynamicConnector {
    config: RwLock<(u64, Arc<ClientConfig>)>,
    options: TlsClientOptions<TlsDynamicCerts>,
}

impl TlsDynamicConnector {
    pub fn new(options: TlsClientOptions<TlsDynamicCerts>) -> Result<Arc<TlsDynamicConnector>> {
        let (version, certs) = options.certs.get_versioned();
        let config = TlsClient::create_config(TlsClientOptions {
            certs: (*certs).clone(),
            hostname_verification: options.hostname_verification,
            crypto_provider: options.crypto_provider.clone(),
            #[cfg(all(feature = "fips", not(feature = "fips-only")))]
            fips: options.fips,
        })?;
        Ok(Arc::new(Self { config: RwLock::new((version, config)), options }))
    }

    pub fn get(&self) -> TlsConnector {
        let (version, mut config) = self.config.read().unwrap().clone();
        if let Some((updated_version, certs)) = self.options.certs.get_updated(version) {
            let options = TlsClientOptions {
                certs: (*certs).clone(),
                hostname_verification: self.options.hostname_verification,
                crypto_provider: self.options.crypto_provider.clone(),
                #[cfg(all(feature = "fips", not(feature = "fips-only")))]
                fips: self.options.fips,
            };
            config = match TlsClient::create_config(options) {
                Ok(config) => self.update_config(updated_version, config),
                Err(err) => {
                    if self.skip_version(version, updated_version) {
                        warn!("fail to create tls config for updated certs: {:?}", err);
                    }
                    config
                },
            };
        }
        TlsConnector::from(config)
    }

    fn skip_version(&self, expected_version: u64, updated_version: u64) -> bool {
        let mut locked = self.config.write().unwrap();
        let update = expected_version == locked.0;
        if update {
            locked.0 = updated_version;
        }
        update
    }

    fn update_config(&self, version: u64, config: Arc<ClientConfig>) -> Arc<ClientConfig> {
        let mut locked = self.config.write().unwrap();
        if version > locked.0 {
            *locked = (version, config);
        }
        locked.1.clone()
    }
}

#[derive(Clone)]
enum TlsInnerClient {
    Static(TlsConnector),
    Dynamic(Arc<TlsDynamicConnector>),
}

impl TlsInnerClient {
    async fn connect(&self, domain: ServerName<'static>, stream: TcpStream) -> std::io::Result<TlsStream<TcpStream>> {
        match self {
            Self::Static(connector) => connector.connect(domain, stream).await,
            Self::Dynamic(connector) => {
                let connector = connector.get();
                connector.connect(domain, stream).await
            },
        }
    }
}

#[derive(Clone)]
pub(crate) struct TlsClient {
    inner: TlsInnerClient,
}

impl TlsClient {
    pub(super) fn new(options: TlsClientOptions<TlsInnerCerts>) -> Result<TlsClient> {
        let inner = match options.certs {
            TlsInnerCerts::Static(certs) => {
                let options = TlsClientOptions {
                    certs,
                    hostname_verification: options.hostname_verification,
                    crypto_provider: options.crypto_provider,
                    #[cfg(all(feature = "fips", not(feature = "fips-only")))]
                    fips: options.fips,
                };
                let config = Self::create_config(options)?;
                TlsInnerClient::Static(TlsConnector::from(config))
            },
            TlsInnerCerts::Dynamic(certs) => {
                let options = TlsClientOptions {
                    certs,
                    hostname_verification: options.hostname_verification,
                    crypto_provider: options.crypto_provider,
                    #[cfg(all(feature = "fips", not(feature = "fips-only")))]
                    fips: options.fips,
                };
                TlsDynamicConnector::new(options).map(TlsInnerClient::Dynamic)?
            },
        };
        Ok(Self { inner })
    }

    fn create_config(options: TlsClientOptions<TlsCerts>) -> Result<Arc<ClientConfig>> {
        let certs = options.certs;
        #[cfg(feature = "fips-only")]
        let builder = Self::fips_config_builder(options.crypto_provider)?;
        #[cfg(all(feature = "fips", not(feature = "fips-only")))]
        let builder = match options.fips {
            true => Self::fips_config_builder(options.crypto_provider)?,
            false => Self::config_builder(options.crypto_provider)?,
        };
        #[cfg(not(feature = "fips"))]
        let builder = Self::config_builder(options.crypto_provider)?;
        let builder = match options.hostname_verification {
            true => {
                let verifier = WebPkiServerVerifier::builder_with_provider(
                    certs.ca.roots.into(),
                    builder.crypto_provider().clone(),
                )
                .with_crls(certs.ca.crls)
                .build()
                .map_err(|err| Error::with_other("fail to create tls server cert verifier", err))?;
                builder.with_webpki_verifier(verifier)
            },
            false => unsafe {
                let verifier = NoHostnameVerificationServerCertVerifier::new(
                    certs.ca.roots,
                    certs.ca.crls,
                    builder.crypto_provider(),
                );
                builder.dangerous().with_custom_certificate_verifier(Arc::new(verifier))
            },
        };
        match certs.identity {
            Some(identity) => match builder.with_client_auth_cert(identity.cert, identity.key) {
                Ok(config) => Ok(config.into()),
                Err(err) => Err(Error::with_other("invalid client private key", err)),
            },
            None => Ok(builder.with_no_client_auth().into()),
        }
    }

    #[cfg(not(feature = "fips-only"))]
    fn config_builder(provider: Option<Arc<CryptoProvider>>) -> Result<ConfigBuilder<ClientConfig, WantsVerifier>> {
        let provider = provider.or_else(|| CryptoProvider::get_default().cloned()).or_else(builtin_crypto_provider);
        match provider {
            Some(provider) => ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .map_err(|err| Error::with_other("fail to set tls version", err)),
            // Let rustls infer the provider from its crate features.
            None => Ok(ClientConfig::builder()),
        }
    }

    #[cfg(feature = "fips")]
    fn fips_config_builder(
        provider: Option<Arc<CryptoProvider>>,
    ) -> Result<ConfigBuilder<ClientConfig, WantsVerifier>> {
        let provider = match provider {
            None => rustls::crypto::default_fips_provider().into(),
            Some(provider) if provider.fips() => provider,
            Some(_) => return Err(Error::BadArguments(&"crypto provider is not FIPS compliant")),
        };
        ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|err| Error::with_other("fail to set tls version for FIPS mode", err))
    }

    pub async fn connect(&self, host: &str, port: u16) -> std::io::Result<TlsStream<TcpStream>> {
        let stream = TcpStream::connect((host, port)).await?;
        let domain = ServerName::try_from(host).unwrap().to_owned();
        self.inner.connect(domain, stream).await
    }
}

/// Crypto provider built in by crate feature `tls`.
#[cfg_attr(feature = "fips-only", allow(dead_code))]
fn builtin_crypto_provider() -> Option<Arc<CryptoProvider>> {
    #[cfg(feature = "tls")]
    return Some(Arc::new(rustls::crypto::aws_lc_rs::default_provider()));
    #[cfg(not(feature = "tls"))]
    None
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use async_net::{TcpListener, TcpStream};
    use asyncs::task::TaskHandle;
    use atomic_write_file::AtomicWriteFile;
    use futures::channel::mpsc;
    use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender};
    use futures::join;
    use futures::stream::StreamExt;
    use futures_rustls::{TlsAcceptor, TlsStream};
    use notify::{Event, RecursiveMode, Watcher};
    use rcgen::{
        Certificate,
        CertificateParams,
        CertificateRevocationList,
        CertificateRevocationListParams,
        CertifiedKey,
        Issuer,
        KeyIdMethod,
        KeyPair,
        RevokedCertParams,
        SerialNumber,
    };
    use rustls::crypto::CryptoProvider;
    use rustls::server::{ServerConfig, WebPkiClientVerifier};
    use rustls::{ClientConfig, RootCertStore};
    use rustls_pki_types::PrivatePkcs8KeyDer;
    use tempfile::TempDir;
    use x509_parser::prelude::*;

    use crate::tls::{TlsCa, TlsCerts, TlsClient, TlsDynamicCerts, TlsIdentity, TlsOptions};

    const HOSTNAME: &str = "127.0.0.1";
    const MISTMATCH_HOSTNAME: &str = "localhost";

    struct Ca {
        pub cert: Certificate,
        pub crls: Vec<CertificateRevocationList>,
        pub issuer: Issuer<'static, KeyPair>,
    }

    impl Ca {
        pub fn new(cert: Certificate, key: KeyPair) -> Self {
            let issuer = Issuer::from_ca_cert_der(cert.der(), key).unwrap();
            Self { cert, crls: vec![], issuer }
        }

        pub fn revoke(&mut self, serial_number: SerialNumber) {
            let revoked_params = RevokedCertParams {
                serial_number,
                revocation_time: SystemTime::now().into(),
                reason_code: None,
                invalidity_date: None,
            };

            let crl_params = CertificateRevocationListParams {
                this_update: revoked_params.revocation_time,
                next_update: revoked_params.revocation_time + Duration::from_secs(3600),
                crl_number: SerialNumber::from(128),
                issuing_distribution_point: None,
                revoked_certs: vec![revoked_params],
                key_identifier_method: KeyIdMethod::Sha256,
            };
            self.crls.push(crl_params.signed_by(&self.issuer).unwrap());
        }

        pub fn pem(&self) -> String {
            let mut pem = self.cert.pem();
            for crl in self.crls.iter() {
                pem += crl.pem().unwrap().as_str();
            }
            pem
        }
    }

    struct ServerCert {
        pub cert: Certificate,
        pub signing_key: KeyPair,
        pub serial_number: SerialNumber,
    }

    impl Clone for ServerCert {
        fn clone(&self) -> Self {
            Self {
                cert: self.cert.clone(),
                signing_key: KeyPair::try_from(self.signing_key.serialize_der()).unwrap(),
                serial_number: self.serial_number.clone(),
            }
        }
    }

    fn generate_ca_cert() -> Ca {
        let mut params = CertificateParams::default();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params.distinguished_name.push(rcgen::DnType::CommonName, "ca");
        params.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::DigitalSignature,
            rcgen::KeyUsagePurpose::CrlSign,
        ];
        let key = KeyPair::generate().unwrap();
        let ca_cert = params.self_signed(&key).unwrap();
        Ca::new(ca_cert, key)
    }

    fn generate_server_cert(issuer: &Issuer<'_, KeyPair>) -> ServerCert {
        let serial_number = SerialNumber::from_slice(uuid::Uuid::new_v4().as_bytes().as_slice());
        let mut params = CertificateParams::new(vec![HOSTNAME.to_string()]).unwrap();
        params.serial_number = Some(serial_number.clone());
        params.key_usages = vec![rcgen::KeyUsagePurpose::DigitalSignature, rcgen::KeyUsagePurpose::KeyEncipherment];
        params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
        params.distinguished_name.push(rcgen::DnType::CommonName, "server");

        let signing_key = KeyPair::generate().unwrap();
        let cert = params.signed_by(&signing_key, issuer).unwrap();
        ServerCert { cert, signing_key, serial_number }
    }

    fn generate_client_cert(cn: &str, issuer: &Issuer<'_, KeyPair>) -> CertifiedKey<KeyPair> {
        let mut params = CertificateParams::default();
        params.distinguished_name.push(rcgen::DnType::CommonName, cn);
        let signing_key = KeyPair::generate().unwrap();
        let cert = params.signed_by(&signing_key, issuer).unwrap();
        CertifiedKey { cert, signing_key }
    }

    fn some_crypto_provider() -> Arc<CryptoProvider> {
        Arc::new(rustls::crypto::aws_lc_rs::default_provider())
    }

    fn server_crypto_provider() -> Arc<CryptoProvider> {
        if let Some(provider) = super::builtin_crypto_provider() {
            return provider;
        }
        // Mimic applications using `tls-no-provider`, which must supply a provider.
        let _ = CryptoProvider::clone(&some_crypto_provider()).install_default();
        CryptoProvider::get_default().unwrap().clone()
    }

    struct TlsListener {
        server_cert: ServerCert,
        listener: TcpListener,
        acceptor: TlsAcceptor,
    }

    impl TlsListener {
        async fn listen(roots: RootCertStore, server_cert: ServerCert) -> Self {
            let builder = ServerConfig::builder_with_provider(server_crypto_provider())
                .with_safe_default_protocol_versions()
                .unwrap();
            let verifier = WebPkiClientVerifier::builder_with_provider(roots.into(), builder.crypto_provider().clone())
                .build()
                .unwrap();
            let server_config = builder
                .with_client_cert_verifier(verifier)
                .with_single_cert(
                    vec![server_cert.cert.der().clone()],
                    PrivatePkcs8KeyDer::from(server_cert.signing_key.serialize_der()).into(),
                )
                .unwrap();
            let listener = TcpListener::bind((HOSTNAME, 0)).await.unwrap();
            Self { server_cert, listener, acceptor: TlsAcceptor::from(Arc::new(server_config)) }
        }

        async fn accept(&self) -> TlsStream<TcpStream> {
            let (stream, _addr) = self.listener.accept().await.unwrap();
            self.acceptor.accept(stream).await.unwrap().into()
        }

        fn local_port(&self) -> u16 {
            self.listener.local_addr().unwrap().port()
        }
    }

    async fn listen() -> (Ca, TlsListener) {
        let ca = generate_ca_cert();
        let server_cert = generate_server_cert(&ca.issuer);
        let mut roots = RootCertStore::empty();
        roots.add(ca.cert.der().clone()).unwrap();

        let listener = TlsListener::listen(roots, server_cert).await;
        (ca, listener)
    }

    async fn hostname_verification(hostname_verification: bool, host: &str, revoke: bool) {
        let (mut ca, listener) = listen().await;

        let client_cert = generate_client_cert("client1", &ca.issuer);

        if revoke {
            ca.revoke(listener.server_cert.serial_number.clone());
        }

        let mut options = TlsOptions::new()
            .with_pem_ca(&ca.pem())
            .unwrap()
            .with_pem_identity(&client_cert.cert.pem(), &client_cert.signing_key.serialize_pem())
            .unwrap();

        if !hostname_verification {
            options = unsafe { options.with_no_hostname_verification() };
        }

        let client = options.into_client().unwrap();

        let (_server_stream, _client_stream) =
            join!(listener.accept(), async { client.connect(host, listener.local_port()).await.unwrap() });
    }

    #[asyncs::test]
    async fn hostname_verification_ok() {
        hostname_verification(true, HOSTNAME, false).await;
        hostname_verification(false, HOSTNAME, false).await;
        hostname_verification(false, MISTMATCH_HOSTNAME, false).await;
    }

    #[asyncs::test]
    #[should_panic(expected = "NotValidForName")]
    async fn hostname_verification_failure() {
        hostname_verification(true, MISTMATCH_HOSTNAME, false).await;
    }

    #[asyncs::test]
    #[should_panic(expected = "InvalidCertificate(Revoked)")]
    async fn no_hostname_verification_with_crls() {
        hostname_verification(false, MISTMATCH_HOSTNAME, true).await;
    }

    async fn assert_client_name(listener: &TlsListener, client: &TlsClient, client_name: &str) {
        let (server_stream, _client_stream) =
            join!(listener.accept(), async { client.connect(HOSTNAME, listener.local_port()).await.unwrap() });

        let (_, state) = server_stream.get_ref();
        let peer_cert = state.peer_certificates().unwrap();
        let cert = X509Certificate::from_der(peer_cert[0].as_ref()).unwrap().1;
        let name = cert.subject().iter_common_name().next().unwrap();
        assert_eq!(name.as_str().unwrap(), client_name);
    }

    #[cfg(feature = "fips")]
    #[rstest::rstest]
    #[case::fips_force(true)]
    #[case::fips_none(false)]
    #[asyncs::test]
    #[test_fork::fork]
    async fn with_fips(#[case] fips: bool) {
        rustls::crypto::CryptoProvider {
            cipher_suites: rustls::crypto::aws_lc_rs::ALL_CIPHER_SUITES.to_vec(),
            ..rustls::crypto::aws_lc_rs::default_provider()
        }
        .install_default()
        .unwrap();

        let (ca, _listener) = listen().await;
        let client_cert = generate_client_cert("client1", &ca.issuer);
        let certs = TlsCerts::builder()
            .with_ca(TlsCa::from_pem(&ca.pem()).unwrap())
            .with_identity(
                TlsIdentity::from_pem(&client_cert.cert.pem(), &client_cert.signing_key.serialize_pem()).unwrap(),
            )
            .build()
            .unwrap();

        let options = match fips {
            true => TlsOptions::new().with_certs(certs).with_fips(),
            false => TlsOptions::new().with_certs(certs),
        };

        let options = options.into_client_options().unwrap();
        let options = match options.certs {
            super::TlsInnerCerts::Static(certs) => super::TlsClientOptions {
                certs,
                hostname_verification: options.hostname_verification,
                crypto_provider: options.crypto_provider,
                #[cfg(not(feature = "fips-only"))]
                fips: options.fips,
            },
            _ => unreachable!(),
        };
        let config = TlsClient::create_config(options).unwrap();
        #[cfg(feature = "fips-only")]
        let fips = true;
        assert_eq!(fips, config.fips());
    }

    fn create_config(options: TlsOptions) -> crate::client::Result<Arc<ClientConfig>> {
        let options = options.into_client_options().unwrap();
        let super::TlsInnerCerts::Static(certs) = options.certs else { unreachable!() };
        TlsClient::create_config(super::TlsClientOptions {
            certs,
            hostname_verification: options.hostname_verification,
            crypto_provider: options.crypto_provider,
            #[cfg(all(feature = "fips", not(feature = "fips-only")))]
            fips: options.fips,
        })
    }

    fn tls_options() -> TlsOptions {
        TlsOptions::new().with_pem_ca(&generate_ca_cert().pem()).unwrap()
    }

    #[cfg(not(feature = "fips-only"))]
    #[test_fork::test]
    fn crypto_provider_specified() {
        CryptoProvider::clone(&some_crypto_provider()).install_default().unwrap();

        let specified = some_crypto_provider();
        let config = create_config(tls_options().with_crypto_provider(specified.clone())).unwrap();
        assert!(Arc::ptr_eq(config.crypto_provider(), &specified));
    }

    #[cfg(not(feature = "fips-only"))]
    #[test_fork::test]
    fn crypto_provider_process_default() {
        CryptoProvider::clone(&some_crypto_provider()).install_default().unwrap();

        let config = create_config(tls_options()).unwrap();
        assert!(Arc::ptr_eq(config.crypto_provider(), CryptoProvider::get_default().unwrap()));
    }

    #[cfg(all(feature = "tls", not(feature = "fips-only")))]
    #[test_fork::test]
    fn crypto_provider_builtin() {
        let config = create_config(tls_options()).unwrap();

        let builtin = super::builtin_crypto_provider().unwrap();
        assert_eq!(format!("{:?}", config.crypto_provider().cipher_suites), format!("{:?}", builtin.cipher_suites));
        assert!(CryptoProvider::get_default().is_none());
    }

    #[cfg(feature = "fips")]
    #[test]
    fn crypto_provider_fips() {
        let fips_provider: Arc<CryptoProvider> = rustls::crypto::default_fips_provider().into();
        let config = create_config(tls_options().with_fips().with_crypto_provider(fips_provider.clone())).unwrap();
        assert!(Arc::ptr_eq(config.crypto_provider(), &fips_provider));

        let non_fips_provider = Arc::new(CryptoProvider {
            cipher_suites: rustls::crypto::aws_lc_rs::ALL_CIPHER_SUITES.to_vec(),
            ..rustls::crypto::aws_lc_rs::default_provider()
        });
        let err = create_config(tls_options().with_fips().with_crypto_provider(non_fips_provider)).unwrap_err();
        assert!(err.to_string().contains("not FIPS compliant"), "{err}");
    }

    #[asyncs::test]
    async fn with_pem_identity() {
        let (ca, listener) = listen().await;

        let client_cert = generate_client_cert("client1", &ca.issuer);

        let options = TlsOptions::new()
            .with_pem_ca(&ca.pem())
            .unwrap()
            .with_pem_identity(&client_cert.cert.pem(), &client_cert.signing_key.serialize_pem())
            .unwrap();

        let client = options.into_client().unwrap();

        assert_client_name(&listener, &client, "client1").await;
    }

    #[asyncs::test]
    #[should_panic(expected = "InvalidCertificate(Revoked)")]
    async fn with_crls() {
        let (mut ca, listener) = listen().await;
        ca.revoke(listener.server_cert.serial_number.clone());

        let client_cert = generate_client_cert("client1", &ca.issuer);

        let options = TlsOptions::new()
            .with_pem_ca(&ca.pem())
            .unwrap()
            .with_pem_identity(&client_cert.cert.pem(), &client_cert.signing_key.serialize_pem())
            .unwrap();

        let client = options.into_client().unwrap();

        assert_client_name(&listener, &client, "client1").await;
    }

    #[asyncs::test]
    async fn with_static_certs() {
        let (ca, listener) = listen().await;

        let client_cert = generate_client_cert("client1", &ca.issuer);

        let options = TlsOptions::new().with_certs(
            TlsCerts::builder()
                .with_ca(TlsCa::from_pem(&ca.pem()).unwrap())
                .with_identity(
                    TlsIdentity::from_pem(&client_cert.cert.pem(), &client_cert.signing_key.serialize_pem()).unwrap(),
                )
                .build()
                .unwrap(),
        );

        let client = options.into_client().unwrap();

        assert_client_name(&listener, &client, "client1").await;
    }

    #[asyncs::test]
    async fn crypto_provider_with_dynamic_certs() {
        let (ca, _listener) = listen().await;
        let certs = TlsCerts::builder().with_ca(TlsCa::from_pem(&ca.pem()).unwrap()).build().unwrap();
        let dynamic_certs = TlsDynamicCerts::new(certs.clone());

        #[cfg(not(feature = "fips-only"))]
        let provider = some_crypto_provider();
        #[cfg(feature = "fips-only")]
        let provider: Arc<CryptoProvider> = rustls::crypto::default_fips_provider().into();
        let options = TlsOptions::new().with_certs(dynamic_certs.clone()).with_crypto_provider(provider.clone());
        #[cfg(feature = "fips")]
        let options = match provider.fips() {
            true => options.with_fips(),
            false => options,
        };
        let options = options.into_client_options().unwrap();
        let super::TlsInnerCerts::Dynamic(certs_cell) = options.certs else { unreachable!() };
        let connector = super::TlsDynamicConnector::new(super::TlsClientOptions {
            certs: certs_cell,
            hostname_verification: options.hostname_verification,
            crypto_provider: options.crypto_provider,
            #[cfg(all(feature = "fips", not(feature = "fips-only")))]
            fips: options.fips,
        })
        .unwrap();
        assert!(Arc::ptr_eq(connector.config.read().unwrap().1.crypto_provider(), &provider));

        dynamic_certs.update(certs);
        connector.get();
        let (version, config) = connector.config.read().unwrap().clone();
        assert_eq!(version, 2);
        assert!(Arc::ptr_eq(config.crypto_provider(), &provider));
    }

    #[asyncs::test]
    async fn with_dynamic_certs() {
        let (ca, listener) = listen().await;

        let client_cert = generate_client_cert("client1", &ca.issuer);

        let dynamic_certs = TlsDynamicCerts::new(
            TlsCerts::builder()
                .with_ca(TlsCa::from_pem(&ca.pem()).unwrap())
                .with_identity(
                    TlsIdentity::from_pem(&client_cert.cert.pem(), &client_cert.signing_key.serialize_pem()).unwrap(),
                )
                .build()
                .unwrap(),
        );

        let options = TlsOptions::new().with_certs(dynamic_certs.clone());

        let client = options.into_client().unwrap();

        assert_client_name(&listener, &client, "client1").await;

        let client_cert = generate_client_cert("client2", &ca.issuer);
        dynamic_certs.update(
            TlsCerts::builder()
                .with_ca(TlsCa::from_pem(&ca.pem()).unwrap())
                .with_identity(
                    TlsIdentity::from_pem(&client_cert.cert.pem(), &client_cert.signing_key.serialize_pem()).unwrap(),
                )
                .build()
                .unwrap(),
        );

        assert_client_name(&listener, &client, "client2").await;
    }

    struct FileBasedDynamicCerts {
        ca: Ca,
        dir: TempDir,
        certs: TlsDynamicCerts,
        feedback: UnboundedReceiver<()>,
        _task: TaskHandle<()>,
    }

    struct EventSender {
        sender: UnboundedSender<Event>,
    }

    impl notify::EventHandler for EventSender {
        fn handle_event(&mut self, event: Result<Event, notify::Error>) {
            if let Ok(event) = event {
                self.sender.unbounded_send(event).unwrap();
            }
        }
    }

    impl FileBasedDynamicCerts {
        pub fn new(ca: Ca, client_name: &str) -> Self {
            let dir = TempDir::new().unwrap();
            Self::generate_cert(&ca, dir.path(), client_name);
            let (certs, feedback, _task) = Self::load_dynamic_certs(&ca, dir.path());
            Self { ca, dir, certs, feedback, _task }
        }

        fn load_dynamic_certs(ca: &Ca, dir: &Path) -> (TlsDynamicCerts, UnboundedReceiver<()>, TaskHandle<()>) {
            let cert_path = dir.join("cert.pem").to_path_buf();
            let key_path = dir.join("cert.key.pem").to_path_buf();

            let mut cert_modified = std::fs::metadata(&cert_path).unwrap().modified().unwrap();
            let mut key_modified = std::fs::metadata(&key_path).unwrap().modified().unwrap();
            let client_cert = std::fs::read_to_string(&cert_path).unwrap();
            let client_key = std::fs::read_to_string(&key_path).unwrap();

            let dynamic_certs = TlsDynamicCerts::new(
                TlsCerts::builder()
                    .with_ca(TlsCa::from_pem(&ca.pem()).unwrap())
                    .with_identity(TlsIdentity::from_pem(&client_cert, &client_key).unwrap())
                    .build()
                    .unwrap(),
            );
            let dynamic_certs_updator = dynamic_certs.clone();

            let (feedback_sender, feedback_receiver) = mpsc::unbounded();
            let task = asyncs::spawn(async move {
                let (tx, mut rx) = mpsc::unbounded();
                let mut watcher = notify::recommended_watcher(EventSender { sender: tx }).unwrap();
                watcher.watch(&cert_path, RecursiveMode::NonRecursive).unwrap();
                watcher.watch(&key_path, RecursiveMode::NonRecursive).unwrap();
                while rx.next().await.is_some() {
                    let updated_cert_modified = std::fs::metadata(&cert_path).unwrap().modified().unwrap();
                    let updated_key_modified = std::fs::metadata(&key_path).unwrap().modified().unwrap();
                    if updated_cert_modified <= cert_modified || updated_key_modified <= key_modified {
                        continue;
                    }
                    cert_modified = updated_cert_modified;
                    key_modified = updated_key_modified;
                    let client_cert = std::fs::read_to_string(&cert_path).unwrap();
                    let client_key = std::fs::read_to_string(&key_path).unwrap();
                    dynamic_certs_updator
                        .update_identity(Some(TlsIdentity::from_pem(&client_cert, &client_key).unwrap()));
                    feedback_sender.unbounded_send(()).unwrap();
                }
            })
            .attach();
            (dynamic_certs, feedback_receiver, task)
        }

        fn generate_cert(ca: &Ca, dir: &Path, name: &str) {
            let client_cert = generate_client_cert(name, &ca.issuer);
            let file = AtomicWriteFile::open(dir.join("cert.pem")).unwrap();
            write!(&file, "{}", client_cert.cert.pem()).unwrap();
            file.commit().unwrap();

            let file = AtomicWriteFile::open(dir.join("cert.key.pem")).unwrap();
            write!(&file, "{}", client_cert.signing_key.serialize_pem()).unwrap();
            file.commit().unwrap();
        }

        pub async fn regenerate_cert(&mut self, client_name: &str) {
            Self::generate_cert(&self.ca, self.dir.path(), client_name);
            self.feedback.next().await;
        }
    }

    #[asyncs::test]
    async fn with_file_based_dynamic_certs() {
        let (ca, listener) = listen().await;

        let options = TlsOptions::new().with_pem_ca(&ca.pem()).unwrap();

        let mut certs = FileBasedDynamicCerts::new(ca, "client1");

        let options = options.with_certs(certs.certs.clone());

        let client = options.into_client().unwrap();

        assert_client_name(&listener, &client, "client1").await;

        certs.regenerate_cert("client2").await;

        assert_client_name(&listener, &client, "client2").await;
    }
}

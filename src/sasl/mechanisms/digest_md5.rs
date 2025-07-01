use std::borrow::Cow;
use std::io::Write;

use md5::Context as Md5Hasher;
use rsasl::mechanism::{Authentication, EmptyProvider, MechanismData, MechanismError, MechanismErrorKind};
use rsasl::prelude::*;
use rsasl::property::{AuthId, Password, Realm};
use rsasl::registry::{distributed_slice, Matches, Named, Side, MECHANISMS};
use thiserror::Error;

pub(crate) type Result<T, E = crate::error::Error> = std::result::Result<T, E>;

#[derive(Debug, Clone, Copy, PartialEq, strum::Display)]
enum ValueKind {
    #[strum(to_string = "quoted")]
    Quoted,
    #[strum(to_string = "token")]
    Token,
}

use ValueKind::*;

const KEYS: [(&str, ValueKind); 9] = [
    ("realm", Quoted),
    ("nonce", Quoted),
    ("qop", Quoted),
    ("stale", Token),
    ("maxbuf", Token),
    ("charset", Token),
    ("algorithm", Token),
    ("cipher", Quoted),
    ("rspauth", Token),
];

struct Parser<'a> {
    s: &'a str,
}

struct Directive<'a> {
    key: &'a str,
    value: &'a str,
    kind: ValueKind,
}

impl Directive<'_> {
    fn check_value_kind(&self, kind: ValueKind) -> Result<(), DigestError> {
        if self.kind != kind {
            return Err(DigestError::new_parse(format!(
                "directive {} expect {} value, but got {} value {}",
                self.key, kind, self.kind, self.value
            )));
        }
        Ok(())
    }
}

#[derive(Error, Debug)]
enum DigestError {
    #[error("{0}")]
    ParseError(Cow<'static, str>),

    #[error("{0}")]
    ProtocolError(Cow<'static, str>),
}

impl DigestError {
    fn new_parse(msg: impl Into<Cow<'static, str>>) -> Self {
        Self::ParseError(msg.into())
    }

    fn new_protocol(msg: impl Into<Cow<'static, str>>) -> Self {
        Self::ProtocolError(msg.into())
    }
}

impl MechanismError for DigestError {
    fn kind(&self) -> MechanismErrorKind {
        MechanismErrorKind::Protocol
    }
}

impl<'a> Parser<'a> {
    pub fn new(challenge: &'a [u8]) -> Result<Self, DigestError> {
        let challenge = match std::str::from_utf8(challenge) {
            Ok(s) => s,
            Err(err) => return Err(DigestError::new_parse(format!("support only utf8 for now: {err}"))),
        };
        Ok(Self { s: challenge })
    }

    pub fn next(&mut self) -> Result<Option<Directive<'a>>, DigestError> {
        let Some(key) = self.read_key()? else {
            return Ok(None);
        };
        self.skip_assignment();
        let Some((value, kind)) = self.read_value()? else {
            return Err(DigestError::new_parse(format!("directive {key} has no value")));
        };
        Ok(Some(Directive { key, value, kind }))
    }

    fn read_key(&mut self) -> Result<Option<&'a str>, DigestError> {
        self.skip_lws();
        // Find first char that is not a token char.
        let Some((i, _c)) = self.s.char_indices().find(|(_, c)| !Self::is_token_char(*c)) else {
            if self.s.is_empty() {
                return Ok(None);
            }
            let key = self.s;
            self.s = "";
            return Ok(Some(key));
        };
        let s = self.s;
        let key = &self.s[..i];
        self.s = &self.s[i..];
        self.skip_lws();
        if key.is_empty() {
            if !self.s.is_empty() {
                return Err(DigestError::new_parse(format!("no valid key from {s}")));
            }
            return Ok(None);
        }
        Ok(Some(key))
    }

    fn read_value(&mut self) -> Result<Option<(&'a str, ValueKind)>, DigestError> {
        if self.skip_char('\"') {
            let mut j = 0;
            while j <= self.s.len() {
                let Some((i, c)) = self.s[j..].char_indices().find(|(_i, c)| *c == '\"' || *c == '\\') else {
                    return Err(DigestError::new_parse(format!("no quoted value \"{}", self.s)));
                };
                if c == '\"' {
                    let value = &self.s[..j + i];
                    self.s = &self.s[j + i + 1..];
                    self.skip_lws_char(',');
                    return Ok(Some((value, Quoted)));
                }
                j = i + 2;
            }
            return Err(DigestError::new_parse(format!("no quoted value \"{}", self.s)));
        }
        let Some((i, c)) = self.s.char_indices().find(|(_, c)| Self::is_lws(*c) || *c == ',') else {
            let value = self.s;
            self.s = "";
            return Ok(Some((value, Token)));
        };
        let value = &self.s[..i];
        self.s = &self.s[i + 1..];
        if c == ',' {
            self.skip_lws();
        } else {
            self.skip_lws_char(',');
        }
        Ok(Some((value, Token)))
    }

    fn is_token_char(c: char) -> bool {
        match c {
            '(' | ')' | '<' | '>' | '@' | ',' | ';' | ':' | '\\' | '"' | '/' | '[' | ']' | '?' | '=' | '{' | '}'
            | ' ' | '\t' => false,
            c if c as u32 <= 31 || c as u32 == 0x7f => false,
            _ => true,
        }
    }

    fn is_lws(c: char) -> bool {
        c == '\r' || c == '\n' || c == ' ' || c == '\t'
    }

    fn skip_lws(&mut self) {
        match self.s.char_indices().find(|(_i, c)| *c != '\r' && *c != '\n' && *c != ' ' && *c != '\t') {
            None => self.s = "",
            Some((0, _)) => {},
            Some((i, _)) => self.s = &self.s[i..],
        }
    }

    fn skip_char(&mut self, expected: char) -> bool {
        let mut iter = self.s.char_indices();
        let Some((_i, c)) = iter.next() else {
            return false;
        };
        if c != expected {
            return false;
        }
        match iter.next() {
            None => self.s = "",
            Some((i, _)) => self.s = &self.s[i..],
        }
        true
    }

    fn skip_lws_char(&mut self, c: char) -> bool {
        self.skip_lws();
        if self.skip_char(c) {
            self.skip_lws();
            true
        } else {
            false
        }
    }

    fn skip_assignment(&mut self) -> bool {
        self.skip_lws_char('=')
    }
}

struct Directives<'a> {
    directives: [Option<&'a str>; KEYS.len()],
    realms_array: [&'a str; 4],
    realms_vector: Vec<&'a str>,
}

impl<'a> Directives<'a> {
    pub fn parse(challenge: &'a [u8]) -> Result<Self, DigestError> {
        let mut parser = Parser::new(challenge)?;
        let mut directives = Directives {
            directives: [None; KEYS.len()],
            realms_array: [Default::default(); 4],
            realms_vector: Vec::new(),
        };
        while let Some(directive) = parser.next()? {
            directives.add(directive)?;
        }
        Ok(directives)
    }

    fn add_realm(&mut self, realm: &'a str) {
        let Some(i) = self.realms_array.iter().position(|s| s.is_empty()) else {
            if self.realms_vector.is_empty() {
                self.realms_vector.extend(self.realms_array.iter());
            }
            self.realms_vector.push(realm);
            return;
        };
        self.realms_array[i] = realm;
    }

    fn add(&mut self, directive: Directive<'a>) -> Result<(), DigestError> {
        let Some(position) = KEYS.iter().position(|k| k.0 == directive.key) else {
            // The client MUST ignore any unrecognized directives.
            return Ok(());
        };
        directive.check_value_kind(KEYS[position].1)?;
        if position == 0 {
            self.add_realm(directive.value);
        } else if let Some(existed) = self.directives[position] {
            return Err(DigestError::new_parse(format!(
                "{} has multiple values {} and {}",
                directive.key, existed, directive.value
            )));
        } else {
            self.directives[position] = Some(directive.value);
        }
        Ok(())
    }

    pub fn get_charset(&self) -> Option<&str> {
        debug_assert_eq!(KEYS[5].0, "charset");
        self.directives[5]
    }

    pub fn get_rspauth(&self) -> Option<&str> {
        debug_assert_eq!(KEYS[8].0, "rspauth");
        self.directives[8]
    }

    pub fn get_algorithm(&self) -> Option<&str> {
        debug_assert_eq!(KEYS[6].0, "algorithm");
        self.directives[6]
    }

    pub fn get_qop(&self) -> Option<&str> {
        debug_assert_eq!(KEYS[2].0, "qop");
        self.directives[2]
    }

    pub fn get_nonce(&self) -> Option<&str> {
        assert_eq!(KEYS[1].0, "nonce");
        self.directives[1]
    }

    pub fn get_realms(&self) -> &[&'a str] {
        if !self.realms_vector.is_empty() {
            return &self.realms_vector;
        }
        let i =
            if let Some(i) = self.realms_array.iter().position(|s| s.is_empty()) { i } else { self.realms_array.len() };
        &self.realms_array[..i]
    }
}

#[derive(Default)]
enum DigestState {
    #[default]
    Step2,
    Step3 {
        rspauth: [u8; 32],
    },
    Completed,
}

#[derive(Default)]
struct DigestSession {
    state: DigestState,
}

#[derive(Debug)]
struct DigestContext<'a> {
    realm: &'a str,
    nonce: &'a str,
    cnonce: &'a [u8],
    username: &'a str,
    password: &'a [u8],
}

impl DigestContext<'_> {
    fn hex(digest: &[u8; 16]) -> [u8; 32] {
        let mut hexies = [0u8; 32];
        hex::encode_to_slice(digest, &mut hexies).unwrap();
        hexies
    }

    fn response(&self, client: bool) -> [u8; 32] {
        let mut hasher = Md5Hasher::new();
        hasher.consume(self.a1());
        hasher.consume(b":");
        hasher.consume(self.nonce.as_bytes());
        hasher.consume(b":");
        hasher.consume(b"00000001");
        hasher.consume(b":");
        hasher.consume(self.cnonce);
        hasher.consume(b":auth");
        hasher.consume(b":");
        hasher.consume(Self::a2(client));
        Self::hex(&hasher.compute().0)
    }

    fn client_response(&self) -> [u8; 32] {
        self.response(true)
    }

    fn server_response(&self) -> [u8; 32] {
        self.response(false)
    }

    fn a1(&self) -> [u8; 32] {
        let mut hasher = Md5Hasher::new();
        hasher.consume(self.username.as_bytes());
        hasher.consume(b":");
        hasher.consume(self.realm.as_bytes());
        hasher.consume(b":");
        hasher.consume(self.password);
        let digest = hasher.compute();
        let mut hasher = Md5Hasher::new();
        hasher.consume(digest.0);
        hasher.consume(b":");
        hasher.consume(self.nonce.as_bytes());
        hasher.consume(b":");
        hasher.consume(self.cnonce);
        let digest = hasher.compute();
        Self::hex(&digest.0)
    }

    fn a2(client: bool) -> [u8; 32] {
        let mut hasher = Md5Hasher::new();
        if client {
            hasher.consume(b"AUTHENTICATE");
        }
        hasher.consume(b":zookeeper/zk-sasl-md5");
        Self::hex(&hasher.compute().0)
    }
}

impl DigestSession {
    fn gen_nonce() -> [u8; 32] {
        let r = fastrand::u128(0..u128::MAX);
        let mut buf = [0; 32];
        write!(buf.as_mut_slice(), "{r:x}").unwrap();
        buf
    }
}

impl Authentication for DigestSession {
    fn step(
        &mut self,
        session: &mut MechanismData,
        input: Option<&[u8]>,
        writer: &mut dyn Write,
    ) -> Result<State, SessionError> {
        let challenge = input.ok_or(SessionError::InputDataRequired)?;
        let directives = Directives::parse(challenge)?;
        match self.state {
            DigestState::Step2 => {
                match directives.get_charset() {
                    None => return Err(DigestError::new_parse("no charset").into()),
                    Some("utf-8") => true,
                    Some(charset) => {
                        return Err(DigestError::new_parse(format!("expect charset utf-8, but got {charset}")).into())
                    },
                };
                match directives.get_algorithm() {
                    None => return Err(DigestError::new_parse("no algorithm").into()),
                    Some("md5-sess") => {},
                    Some(algorithm) => {
                        return Err(
                            DigestError::new_parse(format!("expect algorithm md5-sess, but got {algorithm}")).into()
                        )
                    },
                };
                if let Some(qop) = directives.get_qop() {
                    if !qop.split(',').any(|s| s == "auth") {
                        return Err(DigestError::new_protocol(format!("unsupported qop {qop}")).into());
                    }
                };
                let Some(nonce) = directives.get_nonce() else {
                    return Err(DigestError::new_parse("no nonce").into());
                };
                let owned_realm =
                    session.maybe_need_with::<Realm, _, _>(&EmptyProvider, |realm| Ok(realm.to_string()))?;
                let client_realm = owned_realm.as_deref();
                let realms = directives.get_realms();
                let realm = if realms.is_empty() {
                    client_realm.unwrap_or("")
                } else if realms.len() == 1 && client_realm.is_none() {
                    realms[0]
                } else if let Some(client_realm) = client_realm {
                    if let Some(realm) = realms.iter().find(|s| **s == client_realm) {
                        *realm
                    } else {
                        return Err(DigestError::new_protocol(format!(
                            "can not choose realm {client_realm} from {realms:?}",
                        ))
                        .into());
                    }
                } else {
                    return Err(DigestError::new_protocol(format!("choose no realm from {realms:?}")).into());
                };
                let username = session.need_with::<AuthId, _, _>(&EmptyProvider, |auth_id| Ok(auth_id.to_string()))?;
                let password =
                    session.need_with::<Password, _, _>(&EmptyProvider, |password| Ok(password.to_owned()))?;
                let cnonce = Self::gen_nonce();
                let context = DigestContext {
                    realm,
                    nonce,
                    cnonce: cnonce.as_slice(),
                    username: username.as_str(),
                    password: password.as_slice(),
                };
                let response = context.client_response();
                write!(writer,
                    r#"username="{username}",realm="{realm}",nonce="{nonce}",cnonce="{}",nc=00000001,qop=auth,digest-uri="zookeeper/zk-sasl-md5",response={},charset="utf-8""#,
                    unsafe { std::str::from_utf8_unchecked(cnonce.as_slice()) },
                    unsafe { std::str::from_utf8_unchecked(response.as_slice()) },
                ).unwrap();
                let rspauth = context.server_response();
                self.state = DigestState::Step3 { rspauth };
                Ok(State::Running)
            },
            DigestState::Step3 { rspauth } => {
                let Some(server_rspauth) = directives.get_rspauth() else {
                    return Err(DigestError::new_parse("no rspauth").into());
                };
                if server_rspauth.as_bytes() != rspauth.as_slice() {
                    return Err(DigestError::new_parse("mismatch rspauth").into());
                }
                self.state = DigestState::Completed;
                Ok(State::Finished(MessageSent::No))
            },
            DigestState::Completed => Err(DigestError::new_protocol("already completed").into()),
        }
    }
}

struct DigestMd5;

impl Named for DigestMd5 {
    fn mech() -> &'static Mechanism {
        &DIGEST_MD5
    }
}

#[distributed_slice(MECHANISMS)]
pub static DIGEST_MD5: Mechanism = Mechanism::build(
    Mechname::const_new_unchecked(b"DIGEST-MD5"),
    0,
    Some(|| Ok(Box::<DigestSession>::default())),
    None,
    Side::Server,
    |_| Some(Matches::<DigestMd5>::name()),
    |_| true,
);

#[cfg(test)]
mod tests {
    use std::str::from_utf8;

    use assertor::*;

    use super::DigestContext;
    use crate::sasl::{SaslInitiator, SaslOptions, SaslSession};

    const USERNAME: &str = "username";
    const PASSWORD: &str = "password";

    fn session(realm: Option<&'static str>) -> SaslSession {
        let mut options = SaslOptions::digest_md5(USERNAME, PASSWORD);
        if let Some(realm) = realm {
            options = options.with_realm(realm);
        }
        options.new_session("host1").unwrap()
    }

    fn get_directive<'a>(challenge: &'a str, key: &str) -> Option<&'a str> {
        challenge.split(",").find_map(|split| {
            let (name, value) = split.split_once('=').unwrap();
            if name == key {
                Some(value.trim_start_matches('"').trim_end_matches('"'))
            } else {
                None
            }
        })
    }

    #[test]
    fn no_initial() {
        let session = session(None);
        assert_eq!(session.initial(), b"");
    }

    #[test]
    #[should_panic(expected = "no charset")]
    fn no_utf8_charset() {
        let challenge = br#"realm="zk-sasl-md5",nonce="tQGS7xRmk+5sqY52MKWXK5iEOt8+Y7ikskjuNjIF",algorithm=md5-sess"#;
        let mut session = session(None);
        session.step(&challenge[..]).unwrap();
    }

    #[test]
    #[should_panic(expected = "expect charset utf-8")]
    fn not_utf8_charset() {
        let challenge = br#"realm="zk-sasl-md5",nonce="tQGS7xRmk+5sqY52MKWXK5iEOt8+Y7ikskjuNjIF",charset=latin1,algorithm=md5-sess"#;
        let mut session = session(None);
        session.step(&challenge[..]).unwrap();
    }

    #[test]
    #[should_panic(expected = "no algorithm")]
    fn algorithm_absent() {
        let challenge = br#"realm="zk-sasl-md5",nonce="tQGS7xRmk+5sqY52MKWXK5iEOt8+Y7ikskjuNjIF",charset=utf-8"#;
        let mut session = session(None);
        session.step(&challenge[..]).unwrap();
    }

    #[test]
    #[should_panic(expected = "expect algorithm md5-sess")]
    fn algorithm_unexpected() {
        let challenge =
            br#"realm="zk-sasl-md5",nonce="tQGS7xRmk+5sqY52MKWXK5iEOt8+Y7ikskjuNjIF",charset=utf-8,algorithm=sha256"#;
        let mut session = session(None);
        session.step(&challenge[..]).unwrap();
    }

    #[test]
    #[should_panic(expected = "no nonce")]
    fn nonce_absent() {
        let challenge = br#"realm="zk-sasl-md5",charset=utf-8,algorithm=md5-sess"#;
        let mut session = session(None);
        session.step(&challenge[..]).unwrap();
    }

    #[test]
    fn nonce_same() {
        let nonce = "tQGS7xRmk+5sqY52MKWXK5iEOt8+Y7ikskjuNjIF";
        let challenge = format!(r#"realm="zk-sasl-md5",nonce="{nonce}",charset=utf-8,algorithm=md5-sess"#);
        let mut session = session(None);
        let response = from_utf8(session.step(challenge.as_bytes()).unwrap().unwrap()).unwrap();
        assert_that!(get_directive(response, "nonce").unwrap()).is_equal_to(nonce);
    }

    #[test]
    fn qop_absent() {
        let challenge =
            br#"realm="zk-sasl-md5",nonce="tQGS7xRmk+5sqY52MKWXK5iEOt8+Y7ikskjuNjIF",charset=utf-8,algorithm=md5-sess"#;
        let mut session = session(None);
        session.step(&challenge[..]).unwrap();
    }

    #[test]
    #[should_panic(expected = "unsupported qop")]
    fn qop_unsupported() {
        let challenge = br#"realm="zk-sasl-md5",qop="auth-conf",nonce="tQGS7xRmk+5sqY52MKWXK5iEOt8+Y7ikskjuNjIF",charset=utf-8,algorithm=md5-sess"#;
        let mut session = session(None);
        session.step(&challenge[..]).unwrap();
    }

    #[test]
    fn realm_absent() {
        let challenge = br#"nonce="tQGS7xRmk+5sqY52MKWXK5iEOt8+Y7ikskjuNjIF",charset=utf-8,algorithm=md5-sess"#;
        let mut session = session(None);
        let response = from_utf8(session.step(&challenge[..]).unwrap().unwrap()).unwrap();
        assert_that!(response).contains(r#"realm="""#);
    }

    #[test]
    fn realm_absent_with_client() {
        let challenge = br#"nonce="tQGS7xRmk+5sqY52MKWXK5iEOt8+Y7ikskjuNjIF",charset=utf-8,algorithm=md5-sess"#;
        let mut session = session(Some("realm1"));
        let response = from_utf8(session.step(&challenge[..]).unwrap().unwrap()).unwrap();
        assert_that!(response).contains(r#"realm="realm1""#);
    }

    #[test]
    #[should_panic(expected = "choose no realm from")]
    fn realm_multiple_choose_no() {
        let challenge = br#"realm="realm1",realm="realm2",nonce="tQGS7xRmk+5sqY52MKWXK5iEOt8+Y7ikskjuNjIF",charset=utf-8,algorithm=md5-sess"#;
        let mut session = session(None);
        session.step(&challenge[..]).unwrap();
    }

    #[test]
    #[should_panic(expected = "can not choose realm realm3 from")]
    fn realm_multiple_choose_wrong() {
        let challenge = br#"realm="realm1",realm="realm2",nonce="tQGS7xRmk+5sqY52MKWXK5iEOt8+Y7ikskjuNjIF",charset=utf-8,algorithm=md5-sess"#;
        let mut session = session(Some("realm3"));
        session.step(&challenge[..]).unwrap();
    }

    #[test]
    fn realm_multiple_choose_one() {
        let challenge = br#"realm="realm1",realm="realm2",nonce="tQGS7xRmk+5sqY52MKWXK5iEOt8+Y7ikskjuNjIF",charset=utf-8,algorithm=md5-sess"#;
        let mut session = session(Some("realm1"));
        let response = from_utf8(session.step(&challenge[..]).unwrap().unwrap()).unwrap();
        assert_that!(response).contains(r#"realm="realm1""#);
    }

    #[test]
    #[should_panic(expected = "no rspauth")]
    fn rspauth_absent() {
        let challenge =
            br#"realm="zk-sasl-md5",nonce="tQGS7xRmk+5sqY52MKWXK5iEOt8+Y7ikskjuNjIF",charset=utf-8,algorithm=md5-sess"#;
        let mut session = session(None);
        session.step(&challenge[..]).unwrap();
        session.step(b"").unwrap();
    }

    #[test]
    #[should_panic(expected = "mismatch rspauth")]
    fn rspauth_mismatch() {
        let challenge =
            br#"realm="zk-sasl-md5",nonce="tQGS7xRmk+5sqY52MKWXK5iEOt8+Y7ikskjuNjIF",charset=utf-8,algorithm=md5-sess"#;
        let mut session = session(None);
        session.step(&challenge[..]).unwrap();
        session.step(b"rspauth=fdsafl").unwrap();
    }

    #[test]
    fn rspauth_ok() {
        let challenge =
            br#"realm="zk-sasl-md5",nonce="tQGS7xRmk+5sqY52MKWXK5iEOt8+Y7ikskjuNjIF",charset=utf-8,algorithm=md5-sess"#;
        let mut session = session(None);
        let response = from_utf8(session.step(&challenge[..]).unwrap().unwrap()).unwrap();
        let nonce = get_directive(response, "nonce").unwrap();
        let cnonce = get_directive(response, "cnonce").unwrap();
        let context = DigestContext {
            realm: "zk-sasl-md5",
            nonce,
            cnonce: cnonce.as_bytes(),
            username: USERNAME,
            password: PASSWORD.as_bytes(),
        };
        let rspauth = context.server_response();
        let challenge = format!("rspauth={}", from_utf8(rspauth.as_slice()).unwrap());
        assert_that!(session.step(challenge.as_bytes()).unwrap()).is_none();
    }
}

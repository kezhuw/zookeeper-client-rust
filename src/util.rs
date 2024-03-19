use crate::chroot::Chroot;
use crate::error::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct InvalidAddress(&'static &'static str);

impl From<InvalidAddress> for Error {
    fn from(_: InvalidAddress) -> Error {
        Error::BadArguments(&"invalid address")
    }
}

fn parse_host_port(host: &str, port: &str) -> Result<u16, InvalidAddress> {
    if host.is_empty() {
        return Err(InvalidAddress(&"empty host"));
    }
    if port.is_empty() {
        return Ok(2181);
    }
    let port = match port.parse::<u16>() {
        Err(_) => return Err(InvalidAddress(&"invalid port")),
        Ok(port) => port,
    };
    if port == 0 {
        return Err(InvalidAddress(&"invalid port number"));
    }
    Ok(port)
}

pub type HostPort = (String, u16, bool);

pub type HostPortRef<'a> = (&'a str, u16, bool);

pub trait ToRef<'a, T: 'a> {
    fn to_ref(&'a self) -> T;
}

pub trait Ref<'a>: Sized + 'a {
    type Value: ToRef<'a, Self>;

    fn to_value(&self) -> Self::Value;
}

impl<'a> ToRef<'a, HostPortRef<'a>> for HostPort {
    fn to_ref(&'a self) -> HostPortRef<'a> {
        return (self.0.as_str(), self.1, self.2);
    }
}

impl<'a> Ref<'a> for HostPortRef<'a> {
    type Value = HostPort;

    fn to_value(&self) -> HostPort {
        (self.0.to_owned(), self.1, self.2)
    }
}

fn parse_address(s: &str) -> Result<(&str, u16), InvalidAddress> {
    let (host, port_str) = if s.starts_with('[') {
        let i = match s.rfind(']') {
            None => return Err(InvalidAddress(&"invalid address")),
            Some(i) => i,
        };
        let host = &s[1..i];
        let mut remains = &s[i + 1..];
        if !remains.is_empty() {
            if remains.as_bytes()[0] != b':' {
                return Err(InvalidAddress(&"invalid address"));
            }
            remains = &remains[1..];
        }
        (host, remains)
    } else {
        match s.rfind(':') {
            None => (s, Default::default()),
            Some(i) => (&s[..i], &s[i + 1..]),
        }
    };
    let port = parse_host_port(host, port_str)?;
    Ok((host, port))
}

/// Parses connection string to host port pairs and chroot.
pub fn parse_connect_string(s: &str, tls: bool) -> Result<(Vec<HostPortRef>, Chroot<'_>), Error> {
    let mut chroot = None;
    let mut servers = Vec::with_capacity(10);
    for s in s.rsplit(',') {
        let (mut hostport, tls) = if let Some(s) = s.strip_prefix("tcp://") {
            (s, false)
        } else if let Some(s) = s.strip_prefix("tcp+tls://") {
            (s, true)
        } else if s.is_empty() {
            let err = if chroot.is_none() {
                Error::BadArguments(&"empty connect string")
            } else {
                Error::BadArguments(&"invalid address")
            };
            return Err(err);
        } else {
            (s, tls)
        };
        if chroot.is_none() {
            chroot = Some(Chroot::default());
            if let Some(i) = hostport.find('/') {
                chroot = Some(Chroot::new(&hostport[i..])?);
                hostport = &hostport[..i];
            }
        }
        let (host, port) = parse_address(hostport)?;
        servers.push((host, port, tls));
    }
    servers.reverse();
    Ok((servers, chroot.unwrap()))
}

/// Splits path to `(parent, tree, name)` where `tree` has trailing slash but `parent` does not
/// have.
pub fn split_path(path: &str) -> (&str, &str, &str) {
    let last_slash = path.rfind('/').unwrap();
    if last_slash == 0 {
        ("/", "/", &path[1..])
    } else {
        (&path[0..last_slash], &path[0..last_slash + 1], &path[last_slash + 1..])
    }
}

pub fn validate_path<'a>(chroot: Chroot<'a>, path: &'a str, allow_trailing_slash: bool) -> Result<&'a str, Error> {
    if path.is_empty() {
        return Err(Error::BadArguments(&"path cannot be empty"));
    }
    if path.as_bytes()[0] != b'/' {
        return Err(Error::BadArguments(&"path must start with '/'"));
    }
    if path.len() == 1 {
        let path = if chroot.is_none() { path } else { "" };
        return Ok(path);
    }
    let mut last_chars = ['/', '/', '/'];
    for c in path.chars().skip(1) {
        let u = u32::from(c);
        let invalid =
            [0x0000..=0x001f, 0x007f..=0x009f, 0xd800..=0xf8ff, 0xfff0..=0xffff].into_iter().any(|r| r.contains(&u));
        if invalid {
            return Err(Error::BadArguments(&"unsupported characters in path"));
        }
        if c == '/' {
            if last_chars[2] == '/' {
                return Err(Error::BadArguments(&"empty node is not allowed in path"));
            } else if (last_chars[1] == '/' && last_chars[2] == '.')
                || (last_chars[0] == '/' && last_chars[1] == '.' && last_chars[2] == '.')
            {
                return Err(Error::BadArguments(&"relative path is not allowed"));
            }
        }
        last_chars[0] = last_chars[1];
        last_chars[1] = last_chars[2];
        last_chars[2] = c;
    }
    if last_chars[2] == '/' && !allow_trailing_slash {
        return Err(Error::BadArguments(&"path must not end with '/'"));
    }
    Ok(path)
}

pub fn strip_root_path<'a>(server_path: &'a str, root: &str) -> Result<&'a str, Error> {
    if let Some(client_path) = server_path.strip_prefix(root) {
        if client_path.is_empty() {
            return Ok(&server_path[..1]);
        }
        return Ok(client_path);
    }
    Err(Error::UnexpectedError(format!("server path {} is not descendant of root {}", server_path, root)))
}

pub fn drain_root_path(server_path: &mut String, root: &str) -> Result<(), Error> {
    if server_path.strip_prefix(root).is_some() {
        drop(server_path.drain(..root.len()));
        if server_path.is_empty() {
            server_path.push('/');
        }
        return Ok(());
    }
    Err(Error::UnexpectedError(format!("server path {} is not descendant of root {}", server_path, root)))
}

#[allow(dead_code)]
pub fn into_client_path(mut server_path: String, root: &str) -> Result<String, Error> {
    drain_root_path(&mut server_path, root)?;
    Ok(server_path)
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use test_case::test_case;

    use crate::chroot::Chroot;
    use crate::error::Error;

    #[test]
    fn test_parse_address_v4() {
        use super::{parse_address, InvalidAddress};
        assert_eq!(parse_address("fasl:0").unwrap_err(), InvalidAddress(&"invalid port number"));
        assert_eq!(parse_address(":1234").unwrap_err(), InvalidAddress(&"empty host"));
        assert_eq!(parse_address("fasl:a234").unwrap_err(), InvalidAddress(&"invalid port"));
        assert_eq!(parse_address("fasl:1234").unwrap(), ("fasl", 1234));
        assert_eq!(parse_address("fasl:2181").unwrap(), ("fasl", 2181));
        assert_eq!(parse_address("fasl").unwrap(), ("fasl", 2181));
    }

    #[test]
    fn test_parse_address_v6() {
        use super::{parse_address, InvalidAddress};
        assert_eq!(parse_address("[fasl").unwrap_err(), InvalidAddress(&"invalid address"));
        assert_eq!(parse_address("[fasl]:0").unwrap_err(), InvalidAddress(&"invalid port number"));
        assert_eq!(parse_address("[]:1234").unwrap_err(), InvalidAddress(&"empty host"));
        assert_eq!(parse_address("[fasl]:a234").unwrap_err(), InvalidAddress(&"invalid port"));
        assert_eq!(parse_address("[fasl]:1234").unwrap(), ("fasl", 1234));
        assert_eq!(parse_address("[fasl]").unwrap(), ("fasl", 2181));
        assert_eq!(parse_address("[::1]:2181").unwrap(), ("::1", 2181));
    }

    #[test]
    fn test_split_path() {
        use super::split_path;

        assert_eq!(split_path("/"), ("/", "/", ""));
        assert_eq!(split_path("/a"), ("/", "/", "a"));
        assert_eq!(split_path("/a/b"), ("/a", "/a/", "b"));
    }

    #[test_case("", Err(Error::BadArguments(&"path cannot be empty")); "empty path")]
    #[test_case("a", Err(Error::BadArguments(&"path must start with '/'")); "path not start with slash")]
    #[test_case("/abc", Ok("/abc"); "one level path")]
    #[test_case("/abc/efg", Ok("/abc/efg"); "two level path")]
    #[test_case("/abc//efg", Err(Error::BadArguments(&"empty node is not allowed in path")); "empty node")]
    #[test_case("/abc/./efg", Err(Error::BadArguments(&"relative path is not allowed")); "relative node dot in path")]
    #[test_case("/abc/../efg", Err(Error::BadArguments(&"relative path is not allowed")); "relative node double-dot in path")]
    #[test_case("/abc/.a./efg", Ok("/abc/.a./efg"); "single-dot in path")]
    #[test_case("/abc/..a/efg", Ok("/abc/..a/efg"); "double-dot in path")]
    #[test_case("/abc/路径/efg", Ok("/abc/路径/efg"); "path with chinese characters")]
    fn test_path(path: &str, result: Result<&str, Error>) {
        use super::validate_path;

        pretty_assertions::assert_eq!(validate_path(Chroot::default(), path, false), result);
        pretty_assertions::assert_eq!(validate_path(Chroot::default(), path, true), result);
        pretty_assertions::assert_eq!(validate_path(Chroot::new("/abc").unwrap(), path, false), result);
        pretty_assertions::assert_eq!(validate_path(Chroot::new("/abc").unwrap(), path, true), result);
    }

    #[test]
    fn test_root_path() {
        use super::validate_path;

        assert_eq!(validate_path(Chroot::default(), "/", false).unwrap(), "/");
        assert_eq!(validate_path(Chroot::default(), "/", true).unwrap(), "/");
        assert_eq!(validate_path(Chroot::new("/abc").unwrap(), "/", false).unwrap(), "");
        assert_eq!(validate_path(Chroot::new("/abc").unwrap(), "/", true).unwrap(), "");
    }

    #[test]
    fn test_path_trailing_slash() {
        use super::validate_path;
        assert_eq!(
            validate_path(Chroot::default(), "/abc/", false).unwrap_err(),
            Error::BadArguments(&"path must not end with '/'")
        );
        assert_eq!(validate_path(Chroot::default(), "/abc/", true).unwrap(), "/abc/");
        assert_eq!(
            validate_path(Chroot::new("/abc").unwrap(), "/abc/", false).unwrap_err(),
            Error::BadArguments(&"path must not end with '/'")
        );
        assert_eq!(validate_path(Chroot::new("/abc").unwrap(), "/abc/", true).unwrap(), "/abc/");
    }

    #[test]
    fn test_parse_connect_string() {
        use super::parse_connect_string;

        assert_eq!(parse_connect_string("", false).unwrap_err(), Error::BadArguments(&"empty connect string"));
        assert_eq!(parse_connect_string("host1:abc", false).unwrap_err(), Error::BadArguments(&"invalid address"));
        assert_eq!(
            parse_connect_string("host1/abc/", true).unwrap_err(),
            Error::BadArguments(&"path must not end with '/'")
        );

        assert_eq!(parse_connect_string("host1", false).unwrap(), (vec![("host1", 2181, false)], Chroot::default()));
        assert_eq!(parse_connect_string("host1", true).unwrap(), (vec![("host1", 2181, true)], Chroot::default()));
        assert_eq!(
            parse_connect_string("tcp+tls://host1", false).unwrap(),
            (vec![("host1", 2181, true)], Chroot::default())
        );
        assert_eq!(
            parse_connect_string("host1,host2:2222/", false).unwrap(),
            (vec![("host1", 2181, false), ("host2", 2222, false)], Chroot::default())
        );
        assert_eq!(
            parse_connect_string("host1,host2:2222/abc", false).unwrap(),
            (vec![("host1", 2181, false), ("host2", 2222, false)], Chroot::new("/abc").unwrap())
        );
        assert_eq!(
            parse_connect_string("host1,tcp+tls://host2:2222,tcp://host3/abc", true).unwrap(),
            (vec![("host1", 2181, true), ("host2", 2222, true), ("host3", 2181, false)], Chroot::new("/abc").unwrap())
        );
    }

    #[test]
    fn test_strip_root_path() {
        use super::strip_root_path;

        assert_eq!(strip_root_path("/abc", "/abc").unwrap(), "/");
        assert_eq!(strip_root_path("/abc/efg", "/abc").unwrap(), "/efg");

        assert_eq!(
            strip_root_path("/abc/efg", "/abcd").unwrap_err(),
            Error::UnexpectedError("server path /abc/efg is not descendant of root /abcd".to_string())
        );
    }

    #[test]
    fn test_drain_root_path() {
        use super::into_client_path;

        assert_eq!(into_client_path("/abc".to_string(), "/abc").unwrap(), "/");
        assert_eq!(into_client_path("/abc/efg".to_string(), "/abc").unwrap(), "/efg");

        assert_eq!(
            into_client_path("/abc/efg".to_string(), "/abcd").unwrap_err(),
            Error::UnexpectedError("server path /abc/efg is not descendant of root /abcd".to_string())
        );
    }
}

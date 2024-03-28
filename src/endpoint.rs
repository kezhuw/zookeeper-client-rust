use std::fmt::{self, Display, Formatter};

use crate::chroot::Chroot;
use crate::error::Error;
use crate::util::{Ref, ToRef};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
    pub tls: bool,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct EndpointRef<'a> {
    pub host: &'a str,
    pub port: u16,
    pub tls: bool,
}

impl Endpoint {
    pub fn new(host: impl Into<String>, port: u16, tls: bool) -> Self {
        Self { host: host.into(), port, tls }
    }
}

impl<'a> EndpointRef<'a> {
    pub fn new(host: &'a str, port: u16, tls: bool) -> Self {
        Self { host, port, tls }
    }
}

impl Display for Endpoint {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.to_ref().fmt(f)
    }
}

impl<'a> From<(&'a str, u16, bool)> for EndpointRef<'a> {
    fn from(v: (&'a str, u16, bool)) -> Self {
        Self::new(v.0, v.1, v.2)
    }
}

impl Display for EndpointRef<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let proto = if self.tls { "tcp+tls" } else { "tcp" };
        write!(f, "{}://{}:{}", proto, self.host, self.port)
    }
}

impl PartialEq<(&str, u16, bool)> for EndpointRef<'_> {
    fn eq(&self, other: &(&str, u16, bool)) -> bool {
        self.host == other.0 && self.port == other.1 && self.tls == other.2
    }
}

impl<'a> ToRef<'a, EndpointRef<'a>> for Endpoint {
    fn to_ref(&'a self) -> EndpointRef<'a> {
        return EndpointRef::new(self.host.as_str(), self.port, self.tls);
    }
}

impl<'a> Ref<'a> for EndpointRef<'a> {
    type Value = Endpoint;

    fn to_value(&self) -> Endpoint {
        Endpoint::new(self.host.to_owned(), self.port, self.tls)
    }
}

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
pub fn parse_connect_string(s: &str, tls: bool) -> Result<(Vec<EndpointRef<'_>>, Chroot<'_>), Error> {
    let mut chroot = None;
    let mut endpoints = Vec::with_capacity(10);
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
        endpoints.push(EndpointRef::new(host, port, tls));
    }
    endpoints.reverse();
    Ok((endpoints, chroot.unwrap()))
}

#[derive(Clone, Debug)]
pub struct IterableEndpoints {
    cycle: bool,
    next: usize,
    endpoints: Vec<Endpoint>,
}

impl IterableEndpoints {
    pub fn new(endpoints: impl Into<Vec<Endpoint>>) -> Self {
        Self { cycle: false, next: 0, endpoints: endpoints.into() }
    }

    pub fn endpoints(&self) -> &[Endpoint] {
        &self.endpoints
    }

    pub fn cycle(&mut self) {
        if self.next >= self.endpoints.len() {
            self.next = 0;
        }
        self.cycle = true;
    }

    pub fn next(&mut self) -> Option<EndpointRef<'_>> {
        let next = self.next;
        if next >= self.endpoints.len() {
            return None;
        }
        self.step();
        let host = &self.endpoints[next];
        Some(host.to_ref())
    }

    pub fn step(&mut self) {
        self.next += 1;
        if self.cycle && self.next >= self.endpoints.len() {
            self.next = 0;
        }
    }
}

impl From<&[EndpointRef<'_>]> for IterableEndpoints {
    fn from(endpoints: &[EndpointRef<'_>]) -> Self {
        let endpoints: Vec<_> = endpoints.iter().map(|endpoint| endpoint.to_value()).collect();
        Self::new(endpoints)
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

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
    fn test_parse_connect_string() {
        use super::parse_connect_string;

        assert_eq!(parse_connect_string("", false).unwrap_err(), Error::BadArguments(&"empty connect string"));
        assert_eq!(parse_connect_string("host1:abc", false).unwrap_err(), Error::BadArguments(&"invalid address"));
        assert_eq!(
            parse_connect_string("host1/abc/", true).unwrap_err(),
            Error::BadArguments(&"path must not end with '/'")
        );

        assert_eq!(
            parse_connect_string("host1", false).unwrap(),
            (vec![("host1", 2181, false).into()], Chroot::default())
        );
        assert_eq!(
            parse_connect_string("host1", true).unwrap(),
            (vec![("host1", 2181, true).into()], Chroot::default())
        );
        assert_eq!(
            parse_connect_string("tcp+tls://host1", false).unwrap(),
            (vec![("host1", 2181, true).into()], Chroot::default())
        );
        assert_eq!(
            parse_connect_string("host1,host2:2222/", false).unwrap(),
            (vec![("host1", 2181, false).into(), ("host2", 2222, false).into()], Chroot::default())
        );
        assert_eq!(
            parse_connect_string("host1,host2:2222/abc", false).unwrap(),
            (vec![("host1", 2181, false).into(), ("host2", 2222, false).into()], Chroot::new("/abc").unwrap())
        );
        assert_eq!(
            parse_connect_string("host1,tcp+tls://host2:2222,tcp://host3/abc", true).unwrap(),
            (
                vec![("host1", 2181, true).into(), ("host2", 2222, true).into(), ("host3", 2181, false).into()],
                Chroot::new("/abc").unwrap()
            )
        );
    }

    #[test]
    fn test_iterable_endpoints() {
        use super::{parse_connect_string, EndpointRef, IterableEndpoints};
        let (endpoints, _) = parse_connect_string("host1:2181,tcp://host2,tcp+tls://host3:2182", true).unwrap();
        let mut endpoints = IterableEndpoints::from(endpoints.as_slice());
        assert_eq!(endpoints.next(), Some(EndpointRef::new("host1", 2181, true)));
        assert_eq!(endpoints.next(), Some(EndpointRef::new("host2", 2181, false)));
        assert_eq!(endpoints.next(), Some(EndpointRef::new("host3", 2182, true)));
        assert_eq!(endpoints.next(), None);

        endpoints.cycle();
        assert_eq!(endpoints.next(), Some(EndpointRef::new("host1", 2181, true)));
        assert_eq!(endpoints.next(), Some(EndpointRef::new("host2", 2181, false)));
        assert_eq!(endpoints.next(), Some(EndpointRef::new("host3", 2182, true)));
        assert_eq!(endpoints.next(), Some(EndpointRef::new("host1", 2181, true)));
    }

    #[test]
    fn test_endpoint_display() {
        use super::{EndpointRef, Ref};

        let endpoint = EndpointRef::new("host", 2181, false);
        assert_eq!(endpoint.to_string(), "tcp://host:2181");
        assert_eq!(endpoint.to_value().to_string(), "tcp://host:2181");

        let endpoint = EndpointRef::new("host", 2182, true);
        assert_eq!(endpoint.to_string(), "tcp+tls://host:2182");
        assert_eq!(endpoint.to_value().to_string(), "tcp+tls://host:2182");
    }
}

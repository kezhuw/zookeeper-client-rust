use std::borrow::Cow;
use std::sync::Arc;

use crate::error::Error;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Chroot<'a> {
    chroot: &'a str,
}

impl<'a> Chroot<'a> {
    pub fn new(path: &'a str) -> Result<Self, Error> {
        crate::util::validate_path(Self::default(), path, false)?;
        let chroot = if path == "/" { "" } else { path };
        Ok(Self { chroot })
    }

    #[allow(dead_code)]
    pub fn path(&self) -> &str {
        if self.chroot.is_empty() {
            "/"
        } else {
            self.chroot
        }
    }

    pub fn root(&self) -> &str {
        self.chroot
    }

    pub fn is_none(&self) -> bool {
        self.chroot.is_empty()
    }

    pub fn to_owned(self) -> OwnedChroot {
        OwnedChroot { chroot: if self.is_none() { None } else { Some(Arc::new(self.chroot.to_string())) } }
    }
}

#[derive(Clone, Debug, Default)]
pub struct OwnedChroot {
    chroot: Option<Arc<String>>,
}

impl OwnedChroot {
    /// Path of chroot.
    pub fn path(&self) -> &str {
        self.chroot.as_ref().map(|s| s.as_str()).unwrap_or("/")
    }

    pub fn root(&self) -> &str {
        self.chroot.as_ref().map(|s| s.as_str()).unwrap_or("")
    }

    #[allow(dead_code)]
    pub fn is_none(&self) -> bool {
        self.chroot.is_none()
    }

    pub fn as_ref(&self) -> Chroot<'_> {
        Chroot { chroot: self.root() }
    }

    pub fn chroot<'a>(&mut self, path: impl Into<Cow<'a, str>>) -> bool {
        let path = path.into();
        if crate::util::validate_path(Chroot::default(), &path, false).is_err() {
            return false;
        }
        if path == "/" {
            self.chroot = None;
        } else if self.path() != path {
            self.chroot = Some(Arc::new(path.into_owned()));
        }
        true
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ChrootPath<'a> {
    chroot: Chroot<'a>,
    path: &'a str,
}

impl<'a> ChrootPath<'a> {
    pub fn new(chroot: Chroot<'a>, path: &'a str, allow_trailing_slash: bool) -> Result<Self, Error> {
        let path = crate::util::validate_path(chroot, path, allow_trailing_slash)?;
        Ok(Self { chroot, path })
    }

    pub fn path(&self) -> (&str, &str) {
        (self.chroot.root(), self.path)
    }

    pub fn is_root(&self) -> bool {
        self.path.is_empty() || self.path.len() == 1
    }
}

#[cfg(test)]
mod tests {
    use speculoos::prelude::*;
    use test_case::test_case;

    use super::*;

    #[test_case("/"; "root")]
    #[test_case("/a"; "a path")]
    #[test_case("/a/b"; "another path")]
    fn test_chroot(path: &str) {
        let expected_root = if path == "/" { "" } else { path };
        let expected_path = path;
        let chroot = Chroot::new(path).unwrap();
        assert_that!(chroot.path()).is_equal_to(expected_path);
        assert_that!(chroot.root()).is_equal_to(expected_root);
        assert_that!(chroot.is_none()).is_equal_to(expected_root.is_empty());

        let mut owned_chroot = chroot.to_owned();
        assert_that!(owned_chroot.as_ref()).is_equal_to(chroot);
        assert_that!(owned_chroot.path()).is_equal_to(expected_path);
        assert_that!(owned_chroot.root()).is_equal_to(expected_root);
        assert_that!(owned_chroot.is_none()).is_equal_to(expected_root.is_empty());

        assert_that!(owned_chroot.chroot("a")).is_false();
        assert_that!(owned_chroot.as_ref()).is_equal_to(chroot);

        let child = crate::util::validate_path(chroot, "/x", false).unwrap();
        let child_path = chroot.root().to_owned() + child;
        let mut other_owned_chroot = owned_chroot.clone();
        assert_that!(other_owned_chroot.as_ref()).is_equal_to(chroot);
        assert_that!(other_owned_chroot.chroot(child_path.clone())).is_true();
        assert_that!(other_owned_chroot.as_ref()).is_not_equal_to(chroot);
        assert_that!(other_owned_chroot.path()).is_equal_to(&*child_path);

        let mut root = owned_chroot.clone();
        root.chroot("/");
        assert_that!(root.path()).is_equal_to("/");
        assert_that!(root.root()).is_equal_to("");
        assert_that!(root.is_none()).is_true();
    }

    #[test]
    fn test_chroot_invalid() {
        Chroot::new("").unwrap_err();
        Chroot::new("abc").unwrap_err();
        Chroot::new("//").unwrap_err();
    }

    #[test]
    fn test_chroot_path() {
        let chroot_path = ChrootPath::new(Chroot::default(), "/", false).unwrap();
        assert_that!(chroot_path.path()).is_equal_to(("", "/"));
        assert_that!(chroot_path.is_root()).is_true();

        let chroot_path = ChrootPath::new(Chroot::new("/a").unwrap(), "/", false).unwrap();
        assert_that!(chroot_path.path()).is_equal_to(("/a", ""));
        assert_that!(chroot_path.is_root()).is_true();

        let chroot_path = ChrootPath::new(Chroot::new("/a").unwrap(), "/b", false).unwrap();
        assert_that!(chroot_path.path()).is_equal_to(("/a", "/b"));
        assert_that!(chroot_path.is_root()).is_false();
    }

    #[test]
    fn test_chroot_path_invalid() {
        ChrootPath::new(Chroot::default(), "", false).unwrap_err();
        ChrootPath::new(Chroot::default(), "abc", false).unwrap_err();
        ChrootPath::new(Chroot::default(), "//", false).unwrap_err();
    }
}

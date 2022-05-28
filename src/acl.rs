use compact_str::CompactString;

/// Permission expresses rights accessors should have to operate on attached node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Permission(i32);

#[rustfmt::skip]
impl Permission {
    pub const NONE: Permission = Permission(0);
    pub const READ: Permission = Permission(1);
    pub const WRITE: Permission = Permission(2);
    pub const CREATE: Permission = Permission(4);
    pub const DELETE: Permission = Permission(8);
    pub const ADMIN: Permission = Permission(16);
    pub const ALL: Permission = Permission(31);

    pub(crate) fn into_raw(self) -> i32 {
        self.0
    }

    pub(crate) fn from_raw(raw: i32) -> Permission {
        Permission(raw)
    }

    pub fn has(self, perm: Permission) -> bool {
        (self.0 & perm.0) == perm.0
    }
}

impl std::ops::BitOr for Permission {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Permission(self.0 | rhs.0)
    }
}

impl std::fmt::Display for Permission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if *self == Permission::ALL {
            return f.write_str("ALL");
        } else if *self == Permission::NONE {
            return f.write_str("NONE");
        }
        [
            (Permission::READ, "READ"),
            (Permission::WRITE, "WRITE"),
            (Permission::CREATE, "CREATE"),
            (Permission::DELETE, "DELETE"),
            (Permission::ADMIN, "ADMIN"),
        ]
        .into_iter()
        .filter_map(|(perm, str)| if self.has(perm) { Some(str) } else { None })
        .enumerate()
        .try_for_each(|(i, str)| if i == 0 { f.write_str(str) } else { write!(f, "|{}", str) })
    }
}

/// Acl expresses node permission for node accessors.
#[derive(Clone, Debug)]
pub struct Acl {
    permission: Permission,
    auth_id: AuthId,
}

/// AuthId represents authenticated identity and expresses authentication requirement in [Acl].
#[derive(Clone, Debug)]
pub struct AuthId {
    scheme: CompactString,
    id: CompactString,
}

impl AuthId {
    /// Constructs an auth id with specified id under given scheme.
    pub fn new(scheme: &str, id: &str) -> AuthId {
        AuthId { scheme: CompactString::new(scheme), id: CompactString::new(id) }
    }

    const fn new_const(scheme: &'static str, id: &'static str) -> AuthId {
        AuthId { scheme: CompactString::new_inline(scheme), id: CompactString::new_inline(id) }
    }

    /// Returns the scheme this auth id serve for.
    pub fn scheme(&self) -> &str {
        self.scheme.as_str()
    }

    /// Returns the identity this auth id represent for.
    pub fn id(&self) -> &str {
        self.id.as_str()
    }

    /// Auth id that could represent anyone in access.
    pub const fn anyone() -> AuthId {
        Self::new_const("world", "anyone")
    }

    /// Auth id that could only represent one that has same authes.
    pub const fn authed() -> AuthId {
        Self::new_const("auth", "")
    }
}

/// AuthUser represents display info for authenticated identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthUser {
    scheme: CompactString,
    user: CompactString,
}

impl AuthUser {
    /// Constructs an auth user with auth scheme and user name.
    pub fn new(scheme: &str, user: &str) -> AuthUser {
        AuthUser { scheme: CompactString::new(scheme), user: CompactString::new(user) }
    }

    /// Returns authed scheme.
    pub fn scheme(&self) -> &str {
        self.scheme.as_str()
    }

    /// Returns authed user name.
    pub fn user(&self) -> &str {
        self.user.as_str()
    }
}

static ANYONE_ALL: [Acl; 1] = [Acl::new_const(Permission::ALL, "world", "anyone")];
static ANYONE_READ: [Acl; 1] = [Acl::new_const(Permission::READ, "world", "anyone")];
static CREATOR_ALL: [Acl; 1] = [Acl::new_const(Permission::ALL, "auth", "")];

impl Acl {
    /// Constructs an acl with specified permission for given auth id.
    pub fn new(permission: Permission, auth_id: AuthId) -> Acl {
        Acl { permission, auth_id }
    }

    const fn new_const(permission: Permission, scheme: &'static str, id: &'static str) -> Acl {
        Acl { permission, auth_id: AuthId { scheme: CompactString::new_inline(scheme), id: CompactString::new_inline(id) } }
    }

    /// Returns acl that expresses anyone can have full permissions over nodes created by this
    /// session.
    pub fn anyone_all() -> &'static [Acl] {
        &ANYONE_ALL
    }

    /// Returns acl that expresses anyone can read nodes created by this session.
    pub fn anyone_read() -> &'static [Acl] {
        &ANYONE_READ
    }

    /// Returns acl that expresses anyone who has same auth as creator can have full permisssions
    /// over nodes created by this session.
    pub fn creator_all() -> &'static [Acl] {
        &CREATOR_ALL
    }

    /// Returns the permission this auth id has.
    pub fn permission(&self) -> Permission {
        self.permission
    }

    /// Returns the auth id this acl serve for.
    pub fn auth_id(&self) -> &AuthId {
        &self.auth_id
    }

    /// Returns the scheme this auth id serve for.
    pub fn scheme(&self) -> &str {
        self.auth_id.scheme.as_str()
    }

    /// Returns the identity this auth id represent for.
    pub fn id(&self) -> &str {
        self.auth_id.id.as_str()
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::Permission;

    #[test]
    fn test_permission() {
        let all = Permission::READ | Permission::WRITE | Permission::CREATE | Permission::DELETE | Permission::ADMIN;

        assert!(Permission::ALL.has(all));
    }

    #[test]
    fn test_permission_display() {
        assert_eq!(Permission::ALL.to_string(), "ALL");
        assert_eq!(Permission::ADMIN.to_string(), "ADMIN");

        let perms = Permission::READ | Permission::WRITE | Permission::CREATE | Permission::DELETE;
        assert_eq!(perms.to_string(), "READ|WRITE|CREATE|DELETE");
    }
}

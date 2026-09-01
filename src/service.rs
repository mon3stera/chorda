use std::any::TypeId;
use std::fmt;

/// Typed key identifying a service inside a realm's service table.
///
/// The key is derived from the service type and carries the compile-time type
/// path as a human-readable name for logs and plugin-graph debugging.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ServiceKey {
    id: TypeId,
    name: &'static str,
}

impl ServiceKey {
    /// Builds the key for a service type.
    pub fn of<T: ?Sized + 'static>() -> Self {
        Self {
            id: TypeId::of::<T>(),
            name: std::any::type_name::<T>(),
        }
    }

    /// The underlying type identity.
    pub fn id(&self) -> TypeId {
        self.id
    }

    /// The human-readable type path recorded at compile time.
    pub fn name(&self) -> &'static str {
        self.name
    }
}

impl fmt::Debug for ServiceKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ServiceKey").field(&self.name).finish()
    }
}

impl fmt::Display for ServiceKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_of_the_same_type_are_equal() {
        assert_eq!(ServiceKey::of::<u32>(), ServiceKey::of::<u32>());
        assert_ne!(ServiceKey::of::<u32>(), ServiceKey::of::<u64>());
    }

    #[test]
    fn keys_render_their_type_path() {
        assert!(ServiceKey::of::<u32>().to_string().ends_with("u32"));
    }
}

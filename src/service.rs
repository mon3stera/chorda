use std::any::{Any, TypeId};

#[derive(Debug, Clone, PartialEq, PartialOrd, Ord, Eq)]
pub struct ServiceKey {
    id: TypeId,
}

impl ServiceKey {
    pub fn from_type<T: 'static>() -> Self {
        Self {
            id: TypeId::of::<T>(),
        }
    }
}

impl From<TypeId> for ServiceKey {
    fn from(value: TypeId) -> Self {
        Self {
            id: value,
        }
    }
}
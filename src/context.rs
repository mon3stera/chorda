use std::{any::{Any, TypeId}, collections::BTreeMap, sync::{Arc, atomic::AtomicU32}};

use tokio::sync::RwLock;

use crate::{fiber::{Fiber, FiberId}, service::ServiceKey};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RealmId(u32);

impl RealmId {
    pub fn root() -> Self {
        Self(0)
    }

    pub fn is_root(&self) -> bool {
        self.0 == 0
    }
}

#[derive(Debug, Clone)]
pub struct Realm {
    id: RealmId,
    parent: Option<RealmId>,
}

impl Realm {
    pub fn root() -> Self {
        Self {
            id: RealmId::root(),
            parent: None
        }
    }

    pub fn is_root(&self) -> bool {
        self.id.is_root() && matches!(self.parent, None)
    }
}

#[derive(Clone)]
pub struct Ctx {
    kernel: Arc<Kernel>,
    fiber: FiberId,
    realm: RealmId,
}

impl Ctx { 
    pub async fn get<T>(&self) -> Option<Arc<T>>
    where 
        T: Send + Sync + 'static
    {
        let id = TypeId::of::<T>();
        let key = (id.into(), self.realm);
        
        let services = self
            .kernel
            .services
            .read()
            .await;

        match services.get(&key) {
            Some(service) => service.clone().downcast().ok(),
            None => None
        }
    }

    pub async fn provide<T>(&self, service: Arc<T>) 
    where 
        T: Send + Sync + 'static
    {
        let id = TypeId::of::<T>();
        
        let mut services = self 
            .kernel
            .services
            .write()
            .await;

        services.insert((id.into(), self.realm), service);
    }
}

pub struct Kernel {
    fibers: RwLock<BTreeMap<FiberId, Fiber>>,
    services: RwLock<BTreeMap<(ServiceKey, RealmId), Arc<dyn Any + Send + Sync>>>,
    next_fiber_id: AtomicU32,
    next_realm_id: AtomicU32,
}


//! Realms and contexts: cheap handles into the kernel.

use std::any::Any;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::task::JoinHandle;

use crate::fiber::{
    DoneSignal, FiberHandle, FiberId, ProvidedService, State, TaskGuard, TaskKind, dispose_fiber,
};
use crate::kernel::KernelInner;
use crate::plugin::Plugin;
use crate::service::ServiceKey;

/// Identity of a realm: one node in the service-scope tree.
///
/// Services provided in a realm are visible to that realm and every derived
/// child realm, but never to parent realms. A child may shadow a parent's
/// service by providing the same key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RealmId(pub(crate) u32);

impl RealmId {
    pub(crate) const ROOT: Self = Self(0);

    /// The reserved root realm id.
    pub fn root() -> Self {
        Self::ROOT
    }

    /// Whether this is the root realm.
    pub fn is_root(self) -> bool {
        self.0 == Self::ROOT.0
    }
}

/// Cheap clonable handle bundling a kernel, a fiber, and a realm.
///
/// Contexts are how plugins reach the kernel: look up and provide services,
/// register child plugins, fork scopes, derive realms, and register scoped
/// effects and event handlers.
#[derive(Clone)]
pub struct Ctx {
    pub(crate) kernel: Arc<KernelInner>,
    pub(crate) fiber: FiberId,
    pub(crate) realm: RealmId,
}

impl fmt::Debug for Ctx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ctx")
            .field("fiber", &self.fiber)
            .field("realm", &self.realm)
            .finish()
    }
}

impl Ctx {
    /// The fiber this context is bound to.
    pub fn fiber_id(&self) -> FiberId {
        self.fiber
    }

    /// The realm this context is bound to.
    pub fn realm_id(&self) -> RealmId {
        self.realm
    }

    /// A handle for the fiber this context is bound to, if it still exists.
    pub fn fiber(&self) -> Option<FiberHandle> {
        self.kernel.fiber(self.fiber).map(|shared| FiberHandle {
            kernel: self.kernel.clone(),
            shared,
        })
    }

    /// Looks up a service of type `T` in this realm, walking up parent
    /// realms. The innermost provider wins.
    pub fn get<T: Send + Sync + 'static>(&self) -> Option<Arc<T>> {
        let service = self.kernel.lookup(self.realm, &ServiceKey::of::<T>())?;

        service.downcast::<T>().ok()
    }

    /// Provides a service into this realm.
    ///
    /// The service is automatically removed when the providing fiber is
    /// disposed, unless it was replaced in the meantime. Replacing an
    /// existing service disconnects the fibers that started because of the
    /// previous value, and wakes every pending plugin whose dependencies
    /// this provision satisfies.
    pub async fn provide<T: Send + Sync + 'static>(&self, service: Arc<T>) {
        if !self.kernel.is_active(self.fiber) {
            tracing::warn!(fiber = ?self.fiber, "ignoring provide from an inactive fiber");
            return;
        }

        let key = ServiceKey::of::<T>();
        let erased: Arc<dyn Any + Send + Sync> = service;
        let previous = self
            .kernel
            .insert_service(self.realm, key.clone(), erased.clone());

        if let Some(shared) = self.kernel.fiber(self.fiber) {
            shared
                .provides
                .lock()
                .expect("provides lock poisoned")
                .push(ProvidedService { key: key.clone() });

            let kernel = self.kernel.clone();
            let cleanup_realm = self.realm;
            let cleanup_key = key.clone();
            let cleanup_service = erased.clone();

            shared.push_disposable(Box::pin(async move {
                kernel.remove_service_if(cleanup_realm, &cleanup_key, &cleanup_service);
            }));
        }

        let replaced = previous.is_some_and(|old| !Arc::ptr_eq(&old, &erased));

        if replaced {
            for dependent in self.kernel.dependents_of(&key, self.fiber) {
                dispose_fiber(self.kernel.clone(), dependent, State::Disposed).await;
            }
        }

        for (shared, plugin) in self.kernel.take_satisfied() {
            self.kernel.start_fiber(shared, plugin);
        }
    }

    /// Registers a plugin on a new child fiber of the current fiber and
    /// returns its handle. The fiber starts immediately when every declared
    /// dependency resolves, and stays pending otherwise.
    pub fn register(&self, plugin: impl Plugin + 'static) -> FiberHandle {
        self.register_shared(Arc::new(plugin))
    }

    /// The dyn-friendly variant of [`Ctx::register`].
    pub fn register_shared(&self, plugin: Arc<dyn Plugin>) -> FiberHandle {
        let shared = self
            .kernel
            .create_fiber(self.fiber, self.realm, plugin.name().to_owned());
        let handle = FiberHandle {
            kernel: self.kernel.clone(),
            shared: shared.clone(),
        };

        if !self.kernel.is_active(self.fiber) {
            // The parent is gone or going: the child is born disposed.
            shared.disposal_started.store(true, Ordering::SeqCst);
            shared.state.send_replace(State::Disposed);
            self.kernel.remove_fiber(shared.id);
            return handle;
        }

        let satisfied = plugin
            .inject()
            .iter()
            .all(|key| self.kernel.lookup(self.realm, key).is_some());

        if satisfied {
            self.kernel.start_fiber(shared, plugin);
        } else {
            self.kernel.add_pending(shared, plugin);
        }

        handle
    }

    /// Forks a bare child fiber usable as a cleanup scope. The scope is born
    /// ready; register effects on [`FiberHandle::ctx`] and dispose the handle
    /// (or let an ancestor's disposal cascade) to run them.
    pub fn fork(&self, name: impl Into<String>) -> FiberHandle {
        let shared = self.kernel.create_fiber(self.fiber, self.realm, name);
        let handle = FiberHandle {
            kernel: self.kernel.clone(),
            shared: shared.clone(),
        };

        if self.kernel.is_active(self.fiber) {
            shared.state.send_replace(State::Ready);
        } else {
            shared.disposal_started.store(true, Ordering::SeqCst);
            shared.state.send_replace(State::Disposed);
            self.kernel.remove_fiber(shared.id);
        }

        handle
    }

    /// Derives a child realm bound to the same fiber. Services provided on
    /// the returned context shadow this realm's services and are invisible
    /// to this realm and its siblings.
    pub fn derive(&self) -> Ctx {
        let realm = self.kernel.create_realm(Some(self.realm));

        Ctx {
            kernel: self.kernel.clone(),
            fiber: self.fiber,
            realm,
        }
    }

    /// Registers a cleanup future for the current fiber. Cleanups run in
    /// last-in-first-out order when the fiber is disposed — including when
    /// its setup fails or is aborted mid-way.
    pub fn effect<F: Future<Output = ()> + Send + 'static>(&self, future: F) {
        let Some(shared) = self.kernel.fiber(self.fiber) else {
            tracing::warn!(fiber = ?self.fiber, "effect ignored: fiber is not registered");
            return;
        };

        shared.push_disposable(Box::pin(future));
    }

    /// Spawns a task tracked by the current fiber.
    ///
    /// Tracked tasks are **aborted** when the fiber is disposed — cut off at
    /// their await point, with `Drop` still running. Spawn is therefore for
    /// work that may be abandoned without ceremony. Tasks that are normally
    /// endless and must wind down gracefully (servers, watchers, subprocess
    /// pumps) belong in [`Ctx::spawn_graceful`] with a termination signal.
    ///
    /// Returns `None` when the fiber is no longer active.
    pub fn spawn<F, T>(&self, future: F) -> Option<JoinHandle<T>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let shared = self.kernel.fiber(self.fiber)?;

        let set = shared.tasks.clone();
        let id = set.claim(TaskKind::Plain);
        let guard = TaskGuard::new(set.clone(), Arc::downgrade(&self.kernel), id);

        let join = tokio::spawn(async move {
            let _guard = guard;
            future.await
        });

        set.attach(id, join.abort_handle());
        self.kernel.activity.notify_waiters();

        Some(join)
    }

    /// Spawns a task tracked by the current fiber, paired with the
    /// operation that stops it.
    ///
    /// When the fiber is disposed, `terminate` runs first — close the
    /// listener, trip the shutdown flag — and the kernel then waits for the
    /// task to actually finish before freeing the fiber's resources. A task
    /// that ignores its termination signal still blocks disposal;
    /// [`FiberHandle::abort_tasks`] is the force stop.
    ///
    /// Returns `None` when the fiber is no longer active.
    pub fn spawn_graceful<F, T, G>(&self, future: F, terminate: G) -> Option<JoinHandle<T>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
        G: Future<Output = ()> + Send + 'static,
    {
        let shared = self.kernel.fiber(self.fiber)?;

        let (done_tx, done_rx) = tokio::sync::watch::channel(false);
        let set = shared.tasks.clone();
        let id = set.claim(TaskKind::Graceful {
            terminate: Box::pin(terminate),
            done: done_rx,
        });
        let guard = TaskGuard::new(set.clone(), Arc::downgrade(&self.kernel), id);

        let join = tokio::spawn(async move {
            let _guard = guard;
            let _done = DoneSignal(&done_tx);
            future.await
        });

        set.attach(id, join.abort_handle());
        self.kernel.activity.notify_waiters();

        Some(join)
    }
}

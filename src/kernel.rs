//! The kernel: registries for fibers, realms, pending plugins, and events.

use std::any::Any;
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use futures::future::FutureExt;

use crate::context::Ctx;
use crate::context::RealmId;
use crate::fiber::{FiberHandle, FiberId, FiberShared, State, dispose_fiber, panic_message};
use crate::plugin::Plugin;
use crate::service::ServiceKey;

pub(crate) struct RealmNode {
    pub parent: Option<RealmId>,
    pub services: HashMap<ServiceKey, Arc<dyn Any + Send + Sync>>,
}

pub(crate) struct PendingEntry {
    pub shared: Arc<FiberShared>,
    pub plugin: Arc<dyn Plugin>,
}

#[derive(Clone)]
pub(crate) struct EventHandler {
    pub realm: RealmId,
    pub fiber: FiberId,
    pub id: u64,
    pub run: crate::events::ErasedHandler,
}

/// A Cordis-style plugin kernel: fibers, realm-scoped services, and scoped
/// events under one registry.
///
/// The kernel owns the root fiber and the root realm. Cloning a `Kernel` is
/// cheap and shares the same registries; the kernel lives as long as any
/// context or handle references it.
#[derive(Clone)]
pub struct Kernel {
    pub(crate) inner: Arc<KernelInner>,
}

impl Kernel {
    /// Creates a kernel with a ready root fiber and an empty root realm.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(KernelInner::new()),
        }
    }

    /// Creates a kernel and registers every plugin discovered through the
    /// inventory registry. See [`crate::registry`].
    pub fn with_discovered_plugins() -> Self {
        let kernel = Self::new();
        kernel.register_discovered();

        kernel
    }

    /// Registers every discovered plugin on the root fiber and returns the
    /// handles, sorted by plugin name. Missing dependencies leave the
    /// plugins pending until something provides them.
    pub fn register_discovered(&self) -> Vec<FiberHandle> {
        let root = self.root_ctx();

        crate::registry::plugin_registrations()
            .into_iter()
            .map(|registration| root.register_shared((registration.build)()))
            .collect()
    }

    /// A context bound to the root fiber and root realm.
    pub fn root_ctx(&self) -> Ctx {
        Ctx {
            kernel: self.inner.clone(),
            fiber: FiberId::root(),
            realm: RealmId::root(),
        }
    }

    /// Disposes the root fiber, which cascades through every fiber the
    /// kernel knows about, then sweeps any leftover pending entries.
    /// Idempotent.
    pub async fn dispose(&self) {
        if let Some(root) = self.inner.fiber(FiberId::root()) {
            dispose_fiber(self.inner.clone(), root, State::Disposed).await;
        }

        for (shared, _) in self.inner.take_all_pending() {
            dispose_fiber(self.inner.clone(), shared, State::Disposed).await;
        }
    }

    /// Resolves once the kernel is idle: no fiber is pending or starting,
    /// and no fiber tracks a spawned task. Ready fibers that only provide
    /// services do not count as work.
    ///
    /// This is the explicit Rust counterpart of Node's "the process stays
    /// alive while work is pending" — with the caveat that only work spawned
    /// through [`Ctx::spawn`](crate::Ctx::spawn) is counted. Anything
    /// started with a bare `tokio::spawn` is invisible to the kernel.
    pub async fn wait_idle(&self) {
        loop {
            let notified = self.inner.activity.notified();

            if self.inner.is_idle() {
                return;
            }

            notified.await;
        }
    }

    /// Drives the kernel until `shutdown` resolves **or** the kernel goes
    /// idle, whichever comes first, then disposes everything — stopping
    /// tasks, cascading child fibers, running effects — before returning.
    ///
    /// A daemon stays alive because its server task keeps the kernel busy;
    /// a batch job exits on its own once its work drains. The same discipline
    /// applies as for [`Kernel::wait_idle`]: keep-alive work must be spawned
    /// through a context to be visible.
    pub async fn run_until(&self, shutdown: impl std::future::Future<Output = ()>) {
        tokio::select! {
            _ = shutdown => {}
            _ = self.wait_idle() => {}
        }

        self.dispose().await;
    }
}

impl Default for Kernel {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) struct KernelInner {
    next_fiber_id: AtomicU32,
    next_realm_id: AtomicU32,
    next_handler_id: AtomicU64,
    fibers: Mutex<HashMap<FiberId, Arc<FiberShared>>>,
    realms: Mutex<HashMap<RealmId, RealmNode>>,
    pending: Mutex<Vec<PendingEntry>>,
    events: Mutex<HashMap<std::any::TypeId, Vec<EventHandler>>>,
    /// Woken on every transition that may change kernel idleness: fiber
    /// state changes, pending queue changes, and task claims/completions.
    pub(crate) activity: tokio::sync::Notify,
}

impl KernelInner {
    pub(crate) fn new() -> Self {
        let kernel = Self {
            next_fiber_id: AtomicU32::new(1),
            next_realm_id: AtomicU32::new(1),
            next_handler_id: AtomicU64::new(0),
            fibers: Mutex::new(HashMap::new()),
            realms: Mutex::new(HashMap::new()),
            pending: Mutex::new(Vec::new()),
            events: Mutex::new(HashMap::new()),
            activity: tokio::sync::Notify::new(),
        };

        kernel.realms.lock().expect("realms lock poisoned").insert(
            RealmId::root(),
            RealmNode {
                parent: None,
                services: HashMap::new(),
            },
        );

        let (state, _) = tokio::sync::watch::channel(State::Ready);
        let root = Arc::new(FiberShared {
            id: FiberId::root(),
            parent: FiberId::root(),
            realm: RealmId::root(),
            name: "root".to_owned(),
            state,
            disposal_started: AtomicBool::new(false),
            setup: Mutex::new(None),
            tasks: Arc::new(crate::fiber::TaskSet::new()),
            disposables: Mutex::new(Vec::new()),
            injected: Mutex::new(Vec::new()),
            provides: Mutex::new(Vec::new()),
        });
        kernel
            .fibers
            .lock()
            .expect("fibers lock poisoned")
            .insert(FiberId::root(), root);

        kernel
    }

    /// Whether the kernel has no outstanding work: no fiber is pending or
    /// starting, and no fiber tracks a task. Ready fibers that only provide
    /// services do not count as work.
    pub(crate) fn is_idle(&self) -> bool {
        self.fibers
            .lock()
            .expect("fibers lock poisoned")
            .values()
            .all(|fiber| {
                matches!(
                    *fiber.state.borrow(),
                    State::Ready | State::Failed | State::Disposed
                ) && fiber.tasks.len() == 0
            })
    }

    fn alloc_fiber_id(&self) -> FiberId {
        // Id 0 is reserved for the root fiber.
        FiberId(self.next_fiber_id.fetch_add(1, Ordering::SeqCst))
    }

    fn alloc_realm_id(&self) -> RealmId {
        // Id 0 is reserved for the root realm.
        RealmId(self.next_realm_id.fetch_add(1, Ordering::SeqCst))
    }

    pub(crate) fn alloc_handler_id(&self) -> u64 {
        self.next_handler_id.fetch_add(1, Ordering::SeqCst)
    }

    pub(crate) fn create_fiber(
        &self,
        parent: FiberId,
        realm: RealmId,
        name: impl Into<String>,
    ) -> Arc<FiberShared> {
        let id = self.alloc_fiber_id();
        let (state, _) = tokio::sync::watch::channel(State::Pending);
        let shared = Arc::new(FiberShared {
            id,
            parent,
            realm,
            name: name.into(),
            state,
            disposal_started: AtomicBool::new(false),
            setup: Mutex::new(None),
            tasks: Arc::new(crate::fiber::TaskSet::new()),
            disposables: Mutex::new(Vec::new()),
            injected: Mutex::new(Vec::new()),
            provides: Mutex::new(Vec::new()),
        });

        self.fibers
            .lock()
            .expect("fibers lock poisoned")
            .insert(id, shared.clone());

        shared
    }

    pub(crate) fn create_realm(&self, parent: Option<RealmId>) -> RealmId {
        let id = self.alloc_realm_id();

        self.realms.lock().expect("realms lock poisoned").insert(
            id,
            RealmNode {
                parent,
                services: HashMap::new(),
            },
        );

        id
    }

    pub(crate) fn fiber(&self, id: FiberId) -> Option<Arc<FiberShared>> {
        self.fibers
            .lock()
            .expect("fibers lock poisoned")
            .get(&id)
            .cloned()
    }

    pub(crate) fn is_active(&self, id: FiberId) -> bool {
        self.fibers
            .lock()
            .expect("fibers lock poisoned")
            .get(&id)
            .map(|fiber| {
                !fiber.disposal_started.load(Ordering::SeqCst)
                    && *fiber.state.borrow() != State::Disposed
            })
            .unwrap_or(false)
    }

    pub(crate) fn children_of(&self, parent: FiberId) -> Vec<Arc<FiberShared>> {
        self.fibers
            .lock()
            .expect("fibers lock poisoned")
            .iter()
            .filter(|(id, fiber)| {
                **id != parent
                    && fiber.parent == parent
                    && !fiber.disposal_started.load(Ordering::SeqCst)
            })
            .map(|(_, fiber)| fiber.clone())
            .collect()
    }

    pub(crate) fn dependents_of(
        &self,
        key: &ServiceKey,
        exclude: FiberId,
    ) -> Vec<Arc<FiberShared>> {
        self.fibers
            .lock()
            .expect("fibers lock poisoned")
            .iter()
            .filter(|(id, fiber)| {
                **id != exclude
                    && !fiber.disposal_started.load(Ordering::SeqCst)
                    && matches!(*fiber.state.borrow(), State::Starting | State::Ready)
                    && fiber
                        .injected
                        .lock()
                        .expect("injected lock poisoned")
                        .iter()
                        .any(|injected| injected == key)
            })
            .map(|(_, fiber)| fiber.clone())
            .collect()
    }

    pub(crate) fn remove_fiber(&self, id: FiberId) {
        self.fibers
            .lock()
            .expect("fibers lock poisoned")
            .remove(&id);
    }

    pub(crate) fn add_pending(&self, shared: Arc<FiberShared>, plugin: Arc<dyn Plugin>) {
        self.pending
            .lock()
            .expect("pending lock poisoned")
            .push(PendingEntry { shared, plugin });

        self.activity.notify_waiters();
    }

    pub(crate) fn remove_pending(&self, id: FiberId) {
        self.pending
            .lock()
            .expect("pending lock poisoned")
            .retain(|entry| entry.shared.id != id);

        self.activity.notify_waiters();
    }

    /// Removes and returns every pending entry whose declared dependencies
    /// now resolve through the entry's own realm chain.
    pub(crate) fn take_satisfied(&self) -> Vec<(Arc<FiberShared>, Arc<dyn Plugin>)> {
        let mut ready = Vec::new();

        self.pending
            .lock()
            .expect("pending lock poisoned")
            .retain(|entry| {
                if entry.shared.disposal_started.load(Ordering::SeqCst) {
                    return false;
                }

                let satisfied = entry
                    .plugin
                    .inject()
                    .iter()
                    .all(|key| self.lookup(entry.shared.realm, key).is_some());

                if satisfied {
                    ready.push((entry.shared.clone(), entry.plugin.clone()));
                }

                satisfied
            });

        ready
    }

    pub(crate) fn take_all_pending(&self) -> Vec<(Arc<FiberShared>, Arc<dyn Plugin>)> {
        self.pending
            .lock()
            .expect("pending lock poisoned")
            .drain(..)
            .map(|entry| (entry.shared, entry.plugin))
            .collect()
    }

    /// Looks up a service starting at `realm` and walking up the parent
    /// chain. The innermost realm that provides the key wins.
    pub(crate) fn lookup(
        &self,
        realm: RealmId,
        key: &ServiceKey,
    ) -> Option<Arc<dyn Any + Send + Sync>> {
        let realms = self.realms.lock().expect("realms lock poisoned");
        let mut next = Some(realm);

        while let Some(current) = next {
            let node = realms.get(&current)?;

            if let Some(service) = node.services.get(key) {
                return Some(service.clone());
            }

            next = node.parent;
        }

        None
    }

    /// Inserts a service into a realm, returning the previous entry if the
    /// key was already provided there.
    pub(crate) fn insert_service(
        &self,
        realm: RealmId,
        key: ServiceKey,
        service: Arc<dyn Any + Send + Sync>,
    ) -> Option<Arc<dyn Any + Send + Sync>> {
        self.realms
            .lock()
            .expect("realms lock poisoned")
            .get_mut(&realm)
            .map(|node| node.services.insert(key, service))
            .expect("realm must exist")
    }

    /// Removes a service only when the table still holds this exact value,
    /// so a stale cleanup cannot delete a newer replacement.
    pub(crate) fn remove_service_if(
        &self,
        realm: RealmId,
        key: &ServiceKey,
        service: &Arc<dyn Any + Send + Sync>,
    ) {
        let mut realms = self.realms.lock().expect("realms lock poisoned");

        if let Some(node) = realms.get_mut(&realm) {
            let same = node
                .services
                .get(key)
                .map(|current| Arc::ptr_eq(current, service))
                .unwrap_or(false);

            if same {
                node.services.remove(key);
            }
        }
    }

    pub(crate) fn add_handler(&self, type_id: std::any::TypeId, handler: EventHandler) {
        self.events
            .lock()
            .expect("events lock poisoned")
            .entry(type_id)
            .or_default()
            .push(handler);
    }

    pub(crate) fn remove_handler(&self, type_id: std::any::TypeId, id: u64) {
        if let Some(handlers) = self
            .events
            .lock()
            .expect("events lock poisoned")
            .get_mut(&type_id)
        {
            handlers.retain(|handler| handler.id != id);
        }
    }

    /// Returns handlers for an event type that are bound to `realm` or any
    /// of its ancestors, ordered from the innermost realm outward.
    pub(crate) fn handlers_for(
        &self,
        type_id: std::any::TypeId,
        realm: RealmId,
    ) -> Vec<EventHandler> {
        let chain = self.realm_chain(realm);
        let events = self.events.lock().expect("events lock poisoned");
        let Some(handlers) = events.get(&type_id) else {
            return Vec::new();
        };

        handlers
            .iter()
            .filter(|handler| chain.contains(&handler.realm))
            .cloned()
            .collect()
    }

    /// The realm chain from `realm` up to (and including) the root.
    pub(crate) fn realm_chain(&self, realm: RealmId) -> Vec<RealmId> {
        let realms = self.realms.lock().expect("realms lock poisoned");
        let mut chain = Vec::new();
        let mut next = Some(realm);

        while let Some(current) = next {
            chain.push(current);

            match realms.get(&current) {
                Some(node) => next = node.parent,
                None => break,
            }
        }

        chain
    }

    /// Starts a plugin's setup on its fiber, spawning the setup task. The
    /// fiber's setup slot is held across the check-and-spawn so a concurrent
    /// dispose either aborts the task or prevents it from starting.
    pub(crate) fn start_fiber(self: &Arc<Self>, shared: Arc<FiberShared>, plugin: Arc<dyn Plugin>) {
        let mut setup_slot = shared.setup.lock().expect("setup lock poisoned");

        if shared.disposal_started.load(Ordering::SeqCst) {
            return;
        }

        *shared.injected.lock().expect("injected lock poisoned") = plugin.inject();
        shared.state.send_replace(State::Starting);
        self.activity.notify_waiters();

        let kernel = self.clone();
        let task_shared = shared.clone();

        let join = tokio::spawn(async move {
            let ctx = Ctx {
                kernel: kernel.clone(),
                fiber: task_shared.id,
                realm: task_shared.realm,
            };

            let setup = plugin.apply(ctx);
            let result = AssertUnwindSafe(setup).catch_unwind().await;
            let _ = task_shared
                .setup
                .lock()
                .expect("setup lock poisoned")
                .take();

            match result {
                Ok(Ok(())) => {
                    if !task_shared.disposal_started.load(Ordering::SeqCst) {
                        task_shared.state.send_replace(State::Ready);
                    }

                    kernel.activity.notify_waiters();
                }
                Ok(Err(error)) => {
                    tracing::error!(fiber = %task_shared.name, %error, "plugin setup failed");
                    dispose_fiber(kernel, task_shared, State::Failed).await;
                }
                Err(panic) => {
                    tracing::error!(
                        fiber = %task_shared.name,
                        panic = panic_message(&panic),
                        "plugin setup panicked"
                    );
                    dispose_fiber(kernel, task_shared, State::Failed).await;
                }
            }
        });

        *setup_slot = Some(join.abort_handle());
    }
}

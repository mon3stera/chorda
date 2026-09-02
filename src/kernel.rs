//! The kernel: registries for fibers, realms, pending plugins, and events.

use std::any::{Any, TypeId};
use std::collections::{HashMap, VecDeque};
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

/// Which handler family a registration belongs to; each dispatch mode reads
/// exactly one family.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum EventKind {
    /// Async observers, served by `emit` and `parallel`.
    Observer,
    /// Synchronous deciders, served by `bail`.
    Bail,
    /// Async deciders, served by `serial`.
    Serial,
    /// Onion layers, served by `waterfall`.
    Waterfall,
}

/// One handler family for one event type, plus the decision type where the
/// family produces one. The registry key; the dispatch site knows all three.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct EventKey {
    pub kind: EventKind,
    pub event: TypeId,
    pub result: Option<TypeId>,
}

#[derive(Clone)]
pub(crate) struct EventHandler {
    pub realm: RealmId,
    pub fiber: FiberId,
    pub id: u64,
    /// The typed handler box; the dispatch site knows the concrete type and
    /// downcasts, mirroring [`PipelineHandler`].
    pub body: Arc<dyn std::any::Any + Send + Sync>,
}

/// A registered middleware on a [`crate::pipeline::Pipeline`] extension
/// point. The handler is type-erased; the dispatch site knows the point
/// type and downcasts.
#[derive(Clone)]
pub(crate) struct PipelineHandler {
    pub realm: RealmId,
    pub fiber: FiberId,
    pub id: u64,
    pub prepend: bool,
    pub handler: Arc<dyn std::any::Any + Send + Sync>,
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

    /// Renders a human-readable snapshot of the kernel: the fiber tree with
    /// each fiber's state, injected and provided services, task counts; the
    /// pending plugins and the service keys they wait for; the event handler
    /// families; and the pipeline chains. What a `chorda doctor` would print.
    ///
    /// Locks every registry for the duration of the render — fine for
    /// diagnostics, not for calling on every hot-path iteration.
    pub fn describe(&self) -> String {
        self.inner.describe()
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
    events: Mutex<HashMap<EventKey, Vec<EventHandler>>>,
    pipelines: Mutex<HashMap<std::any::TypeId, Vec<PipelineHandler>>>,
    /// `std::any::type_name` of every event/result type seen at registration,
    /// so [`Kernel::describe`] can print readable families for bare `TypeId`s.
    type_names: Mutex<HashMap<std::any::TypeId, &'static str>>,
    /// Pipeline marker name (`Pipeline::NAME`) per pipeline type.
    pipeline_names: Mutex<HashMap<std::any::TypeId, String>>,
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
            pipelines: Mutex::new(HashMap::new()),
            type_names: Mutex::new(HashMap::new()),
            pipeline_names: Mutex::new(HashMap::new()),
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
            failure: Mutex::new(None),
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
            failure: Mutex::new(None),
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

    /// The fibers that resolved `key` to the binding held by `realm` and must
    /// therefore be disconnected when that binding goes away.
    ///
    /// Injecting the same key is not enough: a fiber in a sibling realm that
    /// shadows the key resolved a different value entirely and must be left
    /// alone, or realms would not isolate anything.
    pub(crate) fn dependents_of(
        &self,
        realm: RealmId,
        key: &ServiceKey,
        exclude: FiberId,
    ) -> Vec<Arc<FiberShared>> {
        let injectors: Vec<Arc<FiberShared>> = self
            .fibers
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
            .collect();

        injectors
            .into_iter()
            .filter(|fiber| self.resolving_realm(fiber.realm, key) == Some(realm))
            .collect()
    }

    /// The realm whose binding wins a lookup of `key` from `realm`, if any.
    pub(crate) fn resolving_realm(&self, realm: RealmId, key: &ServiceKey) -> Option<RealmId> {
        let realms = self.realms.lock().expect("realms lock poisoned");
        let mut next = Some(realm);

        while let Some(current) = next {
            let node = realms.get(&current)?;

            if node.services.contains_key(key) {
                return Some(current);
            }

            next = node.parent;
        }

        None
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

    /// Records the human-readable name of a type that appears in the event
    /// registry, for diagnostics.
    pub(crate) fn note_type<E: 'static>(&self) {
        self.type_names
            .lock()
            .expect("type names lock poisoned")
            .entry(std::any::TypeId::of::<E>())
            .or_insert_with(std::any::type_name::<E>);
    }

    /// Records a pipeline marker's name, for diagnostics.
    pub(crate) fn note_pipeline<P: crate::pipeline::Pipeline>(&self) {
        self.pipeline_names
            .lock()
            .expect("pipeline names lock poisoned")
            .insert(std::any::TypeId::of::<P>(), P::NAME.to_owned());
    }

    /// Renders the diagnostic snapshot; see [`Kernel::describe`].
    pub(crate) fn describe(&self) -> String {
        let fibers = self.fibers.lock().expect("fibers lock poisoned");
        let mut out = String::new();

        self.describe_fiber(&fibers, FiberId::root(), 0, &mut out);
        self.describe_pending(&mut out);
        self.describe_events(&mut out);
        self.describe_pipelines(&mut out);

        out
    }

    fn describe_fiber(
        &self,
        fibers: &HashMap<FiberId, Arc<FiberShared>>,
        id: FiberId,
        depth: usize,
        out: &mut String,
    ) {
        use std::fmt::Write as _;

        let Some(shared) = fibers.get(&id) else {
            return;
        };

        let indent = "  ".repeat(depth);
        let state = *shared.state.borrow();

        let _ = writeln!(out, "{indent}fiber \"{}\" [{state:?}]", shared.name);

        let injected = shared.injected.lock().expect("injected lock poisoned");

        if !injected.is_empty() {
            let keys: Vec<String> = injected.iter().map(|key| key.to_string()).collect();

            let _ = writeln!(out, "{indent}  injects: {}", keys.join(", "));
        }

        drop(injected);

        let provides = shared.provides.lock().expect("provides lock poisoned");

        if !provides.is_empty() {
            let keys: Vec<String> = provides.iter().map(|entry| entry.key.to_string()).collect();

            let _ = writeln!(out, "{indent}  provides: {}", keys.join(", "));
        }

        let tasks = shared.tasks.len();

        if tasks > 0 {
            let _ = writeln!(out, "{indent}  tasks: {tasks}");
        }

        let mut children: Vec<&Arc<FiberShared>> = fibers
            .values()
            .filter(|shared| shared.parent == id && shared.id != FiberId::root())
            .collect();

        children.sort_by(|a, b| a.name.cmp(&b.name));

        for child in children {
            self.describe_fiber(fibers, child.id, depth + 1, out);
        }
    }

    fn describe_pending(&self, out: &mut String) {
        use std::fmt::Write as _;

        let pending = self.pending.lock().expect("pending lock poisoned");

        if pending.is_empty() {
            return;
        }

        let _ = writeln!(out, "pending:");

        for entry in pending.iter() {
            let wants: Vec<String> = entry
                .plugin
                .inject()
                .iter()
                .map(|key| key.to_string())
                .collect();

            let _ = writeln!(
                out,
                "  \"{}\" waits for [{}]",
                entry.shared.name,
                wants.join(", ")
            );
        }
    }

    fn describe_events(&self, out: &mut String) {
        use std::fmt::Write as _;

        let events = self.events.lock().expect("events lock poisoned");

        if events.is_empty() {
            return;
        }

        let mut lines: Vec<String> = Vec::new();

        for (key, handlers) in events.iter() {
            let kind = match key.kind {
                EventKind::Observer => "observer",
                EventKind::Bail => "bail",
                EventKind::Serial => "serial",
                EventKind::Waterfall => "waterfall",
            };

            let event = self.type_name(key.event).unwrap_or("<unknown type>");

            let result = key
                .result
                .and_then(|id| self.type_name(id))
                .map(|name| format!(", result {name}"))
                .unwrap_or_default();

            lines.push(format!("  {kind}<{event}{result}> × {}", handlers.len()));
        }

        lines.sort();

        let _ = writeln!(out, "events:");

        for line in lines {
            let _ = writeln!(out, "{line}");
        }
    }

    fn describe_pipelines(&self, out: &mut String) {
        use std::fmt::Write as _;

        let pipelines = self.pipelines.lock().expect("pipelines lock poisoned");

        if pipelines.is_empty() {
            return;
        }

        let mut lines: Vec<String> = Vec::new();

        for (type_id, handlers) in pipelines.iter() {
            let name = self
                .pipeline_names
                .lock()
                .expect("pipeline names lock poisoned")
                .get(type_id)
                .cloned()
                .unwrap_or_else(|| "<unknown pipeline>".to_owned());

            lines.push(format!("  {name} × {} middleware(s)", handlers.len()));
        }

        lines.sort();

        let _ = writeln!(out, "pipelines:");

        for line in lines {
            let _ = writeln!(out, "{line}");
        }
    }

    fn type_name(&self, id: std::any::TypeId) -> Option<&'static str> {
        self.type_names
            .lock()
            .expect("type names lock poisoned")
            .get(&id)
            .copied()
    }

    pub(crate) fn add_handler(&self, key: EventKey, handler: EventHandler) {
        self.events
            .lock()
            .expect("events lock poisoned")
            .entry(key)
            .or_default()
            .push(handler);
    }

    pub(crate) fn remove_handler(&self, key: EventKey, id: u64) {
        if let Some(handlers) = self
            .events
            .lock()
            .expect("events lock poisoned")
            .get_mut(&key)
        {
            handlers.retain(|handler| handler.id != id);
        }
    }

    /// Returns handlers for one event family that are bound to `realm` or any
    /// of its ancestors, ordered from the innermost realm outward.
    pub(crate) fn handlers_for(&self, key: EventKey, realm: RealmId) -> Vec<EventHandler> {
        let chain = self.realm_chain(realm);
        let events = self.events.lock().expect("events lock poisoned");
        let Some(handlers) = events.get(&key) else {
            return Vec::new();
        };

        handlers
            .iter()
            .filter(|handler| chain.contains(&handler.realm))
            .cloned()
            .collect()
    }

    pub(crate) fn add_pipeline(&self, type_id: std::any::TypeId, handler: PipelineHandler) {
        self.pipelines
            .lock()
            .expect("pipelines lock poisoned")
            .entry(type_id)
            .or_default()
            .push(handler);
    }

    pub(crate) fn remove_pipeline(&self, type_id: std::any::TypeId, id: u64) {
        if let Some(entries) = self
            .pipelines
            .lock()
            .expect("pipelines lock poisoned")
            .get_mut(&type_id)
        {
            entries.retain(|entry| entry.id != id);
        }
    }

    /// The vertical slice of realms a dispatch from `realm` reaches: its
    /// ancestors (root first), itself, then all descendant realms breadth
    /// first. The position in this list is the onion's realm order —
    /// global middlewares (root) wrap outermost, session-specific ones run
    /// innermost, closest to the built-in behavior.
    pub(crate) fn pipeline_slice(&self, realm: RealmId) -> HashMap<RealmId, usize> {
        let mut order = HashMap::new();
        let mut ancestors: Vec<_> = self.realm_chain(realm);

        ancestors.reverse();

        for (index, ancestor) in ancestors.into_iter().enumerate() {
            order.insert(ancestor, index);
        }

        let mut next_index = order.len();
        let mut queue = VecDeque::from([realm]);

        while let Some(current) = queue.pop_front() {
            let mut children: Vec<RealmId> = {
                let realms = self.realms.lock().expect("realms lock poisoned");

                realms
                    .iter()
                    .filter(|(_, node)| node.parent == Some(current))
                    .map(|(id, _)| *id)
                    .collect()
            };

            children.sort_by_key(|id| id.0);

            for child in children {
                if order.contains_key(&child) {
                    continue;
                }

                order.insert(child, next_index);
                next_index += 1;
                queue.push_back(child);
            }
        }

        order
    }

    /// The snapshot of middlewares for a pipeline dispatch from `realm`,
    /// ordered outermost first: realms in slice order (depth ascending),
    /// prepends newest first, then appends in registration order.
    pub(crate) fn pipeline_chain<P: crate::pipeline::Pipeline>(
        &self,
        realm: RealmId,
    ) -> Vec<crate::pipeline::SharedRun<P>> {
        let order = self.pipeline_slice(realm);
        let entries = {
            let pipelines = self.pipelines.lock().expect("pipelines lock poisoned");

            pipelines
                .get(&std::any::TypeId::of::<P>())
                .cloned()
                .unwrap_or_default()
        };

        let mut selected: Vec<(usize, PipelineHandler)> = entries
            .into_iter()
            .filter_map(|entry| order.get(&entry.realm).map(|index| (*index, entry)))
            .filter(|(_, entry)| self.is_active(entry.fiber))
            .collect();

        selected.sort_by(|(a_order, a), (b_order, b)| {
            a_order
                .cmp(b_order)
                .then_with(|| match (a.prepend, b.prepend) {
                    (true, true) => b.id.cmp(&a.id),
                    (false, false) => a.id.cmp(&b.id),
                    (true, false) => std::cmp::Ordering::Less,
                    (false, true) => std::cmp::Ordering::Greater,
                })
        });

        selected
            .into_iter()
            .map(|(_, entry)| {
                let boxed = entry
                    .handler
                    .clone()
                    .downcast::<crate::pipeline::MiddlewareBox<P>>()
                    .expect("middleware type mismatch");

                Arc::clone(&boxed.run)
            })
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
                    let chain = format!("{error:#}");

                    tracing::error!(fiber = %task_shared.name, error = chain, "plugin setup failed");

                    *task_shared.failure.lock().expect("failure lock poisoned") = Some(chain);

                    dispose_fiber(kernel, task_shared, State::Failed).await;
                }
                Err(panic) => {
                    let message = panic_message(&panic);

                    tracing::error!(fiber = %task_shared.name, panic = message, "plugin setup panicked");

                    *task_shared.failure.lock().expect("failure lock poisoned") =
                        Some(format!("plugin setup panicked: {message}"));

                    dispose_fiber(kernel, task_shared, State::Failed).await;
                }
            }
        });

        *setup_slot = Some(join.abort_handle());
    }
}

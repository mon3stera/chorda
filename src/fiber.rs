//! Fibers: the units of asynchronous lifecycle.
//!
//! Every plugin runs on exactly one fiber. The fiber owns the plugin's
//! effects, the tasks it spawned, and the services it provided, and remembers
//! which fibers depend on it, so that disposal runs in a well-defined order:
//! abort a running setup, stop spawned tasks (abort plain ones, signal and
//! then join graceful ones), disconnect dependents, cascade to child fibers,
//! and only then drain effects last-in-first-out.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use futures::future::{BoxFuture, FutureExt};
use tokio::sync::watch;

use crate::context::Ctx;
use crate::context::RealmId;
use crate::kernel::KernelInner;
use crate::service::ServiceKey;

/// Identity of a fiber within the kernel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FiberId(pub(crate) u32);

impl FiberId {
    pub(crate) const ROOT: Self = Self(0);

    /// The reserved root fiber id.
    pub fn root() -> Self {
        Self::ROOT
    }

    /// Whether this is the root fiber.
    pub fn is_root(self) -> bool {
        self.0 == Self::ROOT.0
    }
}

/// Lifecycle state of a fiber.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// The plugin is registered but its declared dependencies do not resolve
    /// yet; `apply` has not started.
    Pending,
    /// `Plugin::apply` is currently running.
    Starting,
    /// Setup completed. Registered effects stay active until disposal.
    Ready,
    /// Setup failed (returned an error or panicked). Effects registered
    /// before the failure have been cleaned up; the fiber is inert.
    Failed,
    /// The fiber was disposed. Effects were cleaned up; the fiber is inert.
    Disposed,
}

impl State {
    /// Whether the fiber will never leave this state again.
    pub fn is_terminal(self) -> bool {
        matches!(self, State::Failed | State::Disposed)
    }
}

pub(crate) type Disposable = BoxFuture<'static, ()>;

/// A service this fiber provided, remembered so dependents can be
/// disconnected on disposal. The value itself is pinned by the cleanup
/// effect, which removes it from the realm's table unless it was replaced.
#[derive(Clone)]
pub(crate) struct ProvidedService {
    pub key: ServiceKey,
}

/// How a tracked task behaves when its fiber is disposed.
pub(crate) enum TaskKind {
    /// Cut off at its await point; `Drop` still runs. Use for work that may
    /// be abandoned without ceremony.
    Plain,
    /// `terminate` runs at disposal to signal the task (close the listener,
    /// trip a shutdown flag, ...), then the kernel waits for the task to
    /// actually finish before freeing the fiber's resources. Use for work
    /// that is normally endless and must wind down gracefully.
    Graceful {
        terminate: Disposable,
        done: watch::Receiver<bool>,
    },
}

pub(crate) struct TaskEntry {
    /// Present once the spawned task reported its abort handle; a tiny
    /// claim-to-attach window exists where it is still `None`.
    pub abort: Option<tokio::task::AbortHandle>,
    pub kind: TaskKind,
}

/// The set of tasks a fiber spawned through [`Ctx::spawn`](crate::Ctx::spawn),
/// tracked so disposal can stop and — for graceful tasks — join them.
pub(crate) struct TaskSet {
    entries: Mutex<HashMap<u64, TaskEntry>>,
    count: watch::Sender<usize>,
    next_id: AtomicU64,
}

impl TaskSet {
    pub(crate) fn new() -> Self {
        let (count, _) = watch::channel(0usize);

        Self {
            entries: Mutex::new(HashMap::new()),
            count,
            next_id: AtomicU64::new(0),
        }
    }

    /// The number of tasks that have not finished yet.
    pub(crate) fn len(&self) -> usize {
        *self.count.borrow()
    }

    /// Reserves an id and registers the task's disposal behaviour. The task
    /// count rises immediately, so an id between `claim` and `attach` is
    /// still accounted for by `wait_idle` and disposal.
    pub(crate) fn claim(&self, kind: TaskKind) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);

        self.entries
            .lock()
            .expect("tasks lock poisoned")
            .insert(id, TaskEntry { abort: None, kind });
        self.count.send_modify(|count| *count += 1);

        id
    }

    /// Attaches the abort handle of the spawned task to its entry.
    pub(crate) fn attach(&self, id: u64, abort: tokio::task::AbortHandle) {
        if let Some(entry) = self
            .entries
            .lock()
            .expect("tasks lock poisoned")
            .get_mut(&id)
        {
            entry.abort = Some(abort);
        }
    }

    /// Called by the task's guard on completion. The count always moves —
    /// it was incremented at claim time, regardless of map membership.
    pub(crate) fn remove(&self, id: u64) {
        self.entries
            .lock()
            .expect("tasks lock poisoned")
            .remove(&id);
        self.count
            .send_modify(|count| *count = count.saturating_sub(1));
    }

    /// Aborts every still-registered task, regardless of kind. Termination
    /// signals are not run — this is the force stop.
    pub(crate) fn abort_all(&self) {
        let entries = self.entries.lock().expect("tasks lock poisoned");

        for entry in entries.values() {
            if let Some(abort) = &entry.abort {
                abort.abort();
            }
        }
    }

    /// Removes every entry, newest last after the caller sorts. Ownership
    /// of the tasks moves to the caller; their guards still decrement the
    /// count on completion.
    pub(crate) fn take_all(&self) -> Vec<(u64, TaskEntry)> {
        self.entries
            .lock()
            .expect("tasks lock poisoned")
            .drain()
            .collect()
    }

    /// Resolves once every claimed task has completed — including tasks
    /// whose abort was just requested: cancellation is observed on the
    /// task's next poll, which is bounded, unlike waiting for arbitrary
    /// task bodies to finish.
    pub(crate) async fn wait_empty(&self) {
        let mut count = self.count.subscribe();

        while *count.borrow_and_update() != 0 {
            if count.changed().await.is_err() {
                break;
            }
        }
    }
}

/// Removes a task from its fiber's set when the task's future is dropped —
/// completed, aborted, or cut by the runtime.
pub(crate) struct TaskGuard {
    set: Arc<TaskSet>,
    kernel: Weak<KernelInner>,
    id: u64,
}

impl TaskGuard {
    pub(crate) fn new(set: Arc<TaskSet>, kernel: Weak<KernelInner>, id: u64) -> Self {
        Self { set, kernel, id }
    }
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        self.set.remove(self.id);

        if let Some(kernel) = self.kernel.upgrade() {
            kernel.activity.notify_waiters();
        }
    }
}

/// Marks a graceful task as finished when the task's future is dropped, so
/// disposal never waits on a task that panicked or was aborted.
pub(crate) struct DoneSignal<'a>(pub(crate) &'a watch::Sender<bool>);

impl Drop for DoneSignal<'_> {
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}

/// Shared state of one fiber, held by the kernel registry and every handle.
pub(crate) struct FiberShared {
    pub id: FiberId,
    pub parent: FiberId,
    pub realm: RealmId,
    pub name: String,
    pub state: watch::Sender<State>,
    /// Guards against concurrent or reentrant disposal.
    pub disposal_started: AtomicBool,
    /// Abort handle of the running setup task, if any.
    pub setup: Mutex<Option<tokio::task::AbortHandle>>,
    /// Tasks spawned through the fiber's context.
    pub tasks: Arc<TaskSet>,
    /// Cleanup futures, executed last-in-first-out on disposal.
    pub disposables: Mutex<Vec<Disposable>>,
    /// Dependency keys that were satisfied when the fiber started.
    pub injected: Mutex<Vec<ServiceKey>>,
    pub provides: Mutex<Vec<ProvidedService>>,
}

impl FiberShared {
    pub fn push_disposable(&self, disposable: Disposable) {
        self.disposables
            .lock()
            .expect("disposables lock poisoned")
            .push(disposable);
    }
}

/// Handle to a fiber, whether it runs a plugin or is a bare cleanup scope.
///
/// Handles are cheap to clone and may outlive the fiber; disposing twice is a
/// no-op. Dropping a handle does **not** dispose the fiber — detached fibers
/// keep running and stay reachable through their parent until the root is
/// disposed.
#[derive(Clone)]
pub struct FiberHandle {
    pub(crate) kernel: Arc<KernelInner>,
    pub(crate) shared: Arc<FiberShared>,
}

impl FiberHandle {
    /// The fiber's id.
    pub fn id(&self) -> FiberId {
        self.shared.id
    }

    /// The fiber's name; for plugin fibers this is the plugin's name.
    pub fn name(&self) -> &str {
        &self.shared.name
    }

    /// The current state, read without awaiting.
    pub fn state(&self) -> State {
        *self.shared.state.borrow()
    }

    /// A context bound to this fiber and its realm.
    pub fn ctx(&self) -> Ctx {
        Ctx {
            kernel: self.kernel.clone(),
            fiber: self.shared.id,
            realm: self.shared.realm,
        }
    }

    /// The fiber's parent, unless this is the root fiber.
    ///
    /// The tree is implicit: a plugin registered (or a scope forked) inside
    /// another plugin's `apply` becomes a child of that plugin's fiber.
    pub fn parent(&self) -> Option<FiberHandle> {
        if self.shared.id.is_root() {
            return None;
        }

        self.kernel
            .fiber(self.shared.parent)
            .map(|shared| FiberHandle {
                kernel: self.kernel.clone(),
                shared,
            })
    }

    /// Handles of the fibers that are currently children of this one.
    /// Disposed children have already left the registry and are not listed.
    pub fn children(&self) -> Vec<FiberHandle> {
        self.kernel
            .children_of(self.shared.id)
            .into_iter()
            .map(|shared| FiberHandle {
                kernel: self.kernel.clone(),
                shared,
            })
            .collect()
    }

    /// The number of spawned tasks that have not finished yet.
    pub fn task_count(&self) -> usize {
        self.shared.tasks.len()
    }

    /// Force-stops every task this fiber tracks, skipping graceful
    /// termination signals. An escape hatch for disposal that must not wait.
    pub fn abort_tasks(&self) {
        self.shared.tasks.abort_all();
    }

    /// Resolves once the fiber becomes ready. Fails when the plugin failed
    /// during setup or when the fiber was disposed before becoming ready.
    pub async fn wait_ready(&self) -> anyhow::Result<()> {
        let mut receiver = self.shared.state.subscribe();

        loop {
            match *receiver.borrow_and_update() {
                State::Ready => return Ok(()),
                State::Failed => {
                    return Err(anyhow::anyhow!(
                        "fiber `{}` failed during setup",
                        self.shared.name
                    ));
                }
                State::Disposed => {
                    return Err(anyhow::anyhow!(
                        "fiber `{}` was disposed before it became ready",
                        self.shared.name
                    ));
                }
                _ => {}
            }

            if receiver.changed().await.is_err() {
                return Err(anyhow::anyhow!(
                    "fiber `{}` state channel closed",
                    self.shared.name
                ));
            }
        }
    }

    /// Disposes the fiber: aborts a pending or running setup, stops spawned
    /// tasks (aborts plain ones, signals and then joins graceful ones),
    /// disconnects dependents, cascades to child fibers, and drains effects
    /// LIFO. Idempotent.
    pub async fn dispose(&self) {
        dispose_fiber(self.kernel.clone(), self.shared.clone(), State::Disposed).await;
    }
}

/// Runs the full disposal of one fiber. `final_state` must be terminal.
pub(crate) async fn dispose_fiber(
    kernel: Arc<KernelInner>,
    shared: Arc<FiberShared>,
    final_state: State,
) {
    debug_assert!(final_state.is_terminal());

    if shared.disposal_started.swap(true, Ordering::SeqCst) {
        // A concurrent or reentrant dispose already took over.
        return;
    }

    // 1. Cancel a pending or running setup task, if any.
    if let Some(setup) = shared.setup.lock().expect("setup lock poisoned").take() {
        setup.abort();
    }

    // Give the aborted task one scheduling step to observe cancellation
    // before effects are drained. Best effort: a task racing the dispose
    // may still register a late effect, so the drain below loops until the
    // list stays empty.
    tokio::task::yield_now().await;

    // 2. Stop the tasks this fiber spawned: plain tasks are cut off at
    //    their await point, graceful tasks receive their termination signal
    //    first and are then awaited to completion, newest first.
    {
        let tasks = shared.tasks.clone();
        let mut entries: Vec<(u64, TaskEntry)> = tasks.take_all();
        entries.sort_by(|(a, _), (b, _)| b.cmp(a));

        let mut completions = Vec::new();

        for (_, entry) in entries {
            match entry.kind {
                TaskKind::Plain => {
                    if let Some(abort) = entry.abort {
                        abort.abort();
                    }
                }
                TaskKind::Graceful { terminate, done } => {
                    if let Err(panic) = AssertUnwindSafe(terminate).catch_unwind().await {
                        tracing::error!(
                            fiber = %shared.name,
                            panic = panic_message(&panic),
                            "task termination panicked"
                        );
                    }

                    completions.push(done);
                }
            }
        }

        for mut done in completions {
            while !*done.borrow_and_update() {
                if done.changed().await.is_err() {
                    break;
                }
            }
        }

        // Cancellation of the plain tasks is observed on their next poll;
        // wait for the set to drain so the fiber leaves disposal with a
        // quiet task table.
        tasks.wait_empty().await;
    }

    // 3. Disconnect fibers that started because of services this fiber
    //    provided. They are disposed before the services actually vanish.
    let mut visited: HashSet<FiberId> = HashSet::new();
    visited.insert(shared.id);

    let provides = shared
        .provides
        .lock()
        .expect("provides lock poisoned")
        .clone();

    for provided in provides {
        for dependent in kernel.dependents_of(&provided.key, shared.id) {
            if visited.insert(dependent.id) {
                Box::pin(dispose_fiber(kernel.clone(), dependent, State::Disposed)).await;
            }
        }
    }

    // 4. Dispose child fibers. They may rely on this fiber's services and
    //    effects, so they go before the effects run.
    for child in kernel.children_of(shared.id) {
        if visited.insert(child.id) {
            Box::pin(dispose_fiber(kernel.clone(), child, State::Disposed)).await;
        }
    }

    // 5. Drain effects last-in-first-out, looping until the list stays
    //    empty so late registrations from an aborted setup still run.
    loop {
        let batch: Vec<Disposable> = std::mem::take(
            &mut *shared
                .disposables
                .lock()
                .expect("disposables lock poisoned"),
        );

        if batch.is_empty() {
            break;
        }

        for disposable in batch.into_iter().rev() {
            if let Err(panic) = AssertUnwindSafe(disposable).catch_unwind().await {
                tracing::error!(
                    fiber = %shared.name,
                    panic = panic_message(&panic),
                    "disposable panicked during disposal"
                );
            }
        }
    }

    // 6. Leave pending queues, publish the terminal state, leave the
    //    registry, and wake anyone waiting for the kernel to go idle.
    kernel.remove_pending(shared.id);
    shared.state.send_replace(final_state);
    kernel.remove_fiber(shared.id);
    kernel.activity.notify_waiters();
}

/// Extracts a printable message from a panic payload.
pub(crate) fn panic_message(panic: &Box<dyn Any + Send>) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic payload".to_owned()
    }
}

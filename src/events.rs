//! Scoped events with five dispatch modes: handlers live and die with their
//! fiber.
//!
//! Events are the extension surface between the agent loop and the plugins
//! around it. The layer mirrors cordis's event service: one registration
//! families per dispatch mode, all scoped to the registering fiber's realm,
//! all bubbling from the emitting realm up through its ancestors, all
//! removing themselves when their fiber is disposed.
//!
//! The five modes, as in cordis:
//!
//! | mode        | handlers        | waits     | short-circuits |
//! |-------------|-----------------|-----------|----------------|
//! | [`Events::emit`]      | async observers | never (detached) | never |
//! | [`Events::parallel`]  | async observers | all, concurrently | never (aggregate) |
//! | [`Events::serial`]    | async deciders  | one by one | first decision |
//! | [`Events::bail`]      | sync deciders   | never | first decision |
//! | [`Events::waterfall`] | onion layers    | composed | a layer skipping `next` |
//!
//! Observers report failure by panicking only; panics are contained in every
//! mode. A panicking observer is logged and skipped in `emit`, `parallel`,
//! and `serial`; a panicking waterfall layer propagates, because the rest of
//! the onion cannot be resumed without the layer's cooperation.

use std::any::{Any, TypeId};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures::future::{BoxFuture, FutureExt};

use crate::context::Ctx;
use crate::fiber::panic_message;
use crate::kernel::{EventHandler, EventKey, EventKind};

/// A family-typed handler box stored in the kernel registry; the dispatch
/// site knows the concrete type and downcasts.
pub(crate) type HandlerBody = Arc<dyn Any + Send + Sync>;

/// Identifier of a registered event handler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HandlerId {
    pub(crate) key: EventKey,
    pub(crate) id: u64,
}

/// Shared run shape of one observer: receives the event, returns nothing.
type ObserverRun<E> = Arc<dyn Fn(E) -> BoxFuture<'static, ()> + Send + Sync>;

/// Shared run shape of one synchronous decider: inspects the event without
/// awaiting and either passes (`None`) or decides (`Some`).
type BailRun<E, R> = Arc<dyn Fn(&E) -> Option<R> + Send + Sync>;

/// Shared run shape of one asynchronous decider.
type SerialRun<E, R> = Arc<dyn Fn(E) -> BoxFuture<'static, Option<R>> + Send + Sync>;

/// Shared run shape of one waterfall layer: receives the event and the rest
/// of the onion. Calling [`EventNext::run`] continues; returning without it
/// vetoes the rest of the chain, including the built-in behavior.
type WaterfallRun<E, R> = Arc<dyn Fn(E, EventNext<E, R>) -> BoxFuture<'static, R> + Send + Sync>;

struct ObserverBox<E> {
    run: ObserverRun<E>,
}

struct BailBox<E, R> {
    run: BailRun<E, R>,
}

struct SerialBox<E, R> {
    run: SerialRun<E, R>,
}

struct WaterfallBox<E, R> {
    run: WaterfallRun<E, R>,
}

fn observer_key<E: 'static>() -> EventKey {
    EventKey {
        kind: EventKind::Observer,
        event: TypeId::of::<E>(),
        result: None,
    }
}

fn decider_key<E: 'static, R: 'static>(kind: EventKind) -> EventKey {
    EventKey {
        kind,
        event: TypeId::of::<E>(),
        result: Some(TypeId::of::<R>()),
    }
}

fn register(ctx: &Ctx, key: EventKey, body: HandlerBody) -> HandlerId {
    let id = ctx.kernel.alloc_handler_id();

    ctx.kernel.add_handler(
        key,
        EventHandler {
            realm: ctx.realm,
            fiber: ctx.fiber,
            id,
            body,
        },
    );

    let kernel = ctx.kernel.clone();
    ctx.effect(async move {
        kernel.remove_handler(key, id);
    });

    HandlerId { key, id }
}

/// The rest of a waterfall onion plus the built-in behavior at its end.
///
/// Cloning is cheap; a layer may run the rest of the chain more than once by
/// cloning or by holding `&self` across calls.
pub struct EventNext<E, R> {
    chain: Arc<[WaterfallRun<E, R>]>,
    index: usize,
    fallback: Arc<dyn Fn() -> BoxFuture<'static, R> + Send + Sync>,
}

impl<E, R> Clone for EventNext<E, R> {
    fn clone(&self) -> Self {
        Self {
            chain: Arc::clone(&self.chain),
            index: self.index,
            fallback: Arc::clone(&self.fallback),
        }
    }
}

impl<E, R> EventNext<E, R>
where
    E: Clone + Send + Sync + 'static,
    R: Send + 'static,
{
    fn entry(
        chain: Arc<[WaterfallRun<E, R>]>,
        index: usize,
        fallback: Arc<dyn Fn() -> BoxFuture<'static, R> + Send + Sync>,
    ) -> Self {
        Self {
            chain,
            index,
            fallback,
        }
    }

    /// Runs the rest of the chain, ending at the built-in behavior. The event
    /// is passed on unchanged; a layer that wants to transform what the rest
    /// of the chain sees wraps the event itself before emitting.
    pub fn run(&self, event: E) -> BoxFuture<'static, R> {
        let Some(run) = self.chain.get(self.index) else {
            return (self.fallback)();
        };

        let next = EventNext::entry(
            Arc::clone(&self.chain),
            self.index + 1,
            Arc::clone(&self.fallback),
        );

        run(event, next)
    }
}

/// The event surface of one realm: a lens over the kernel's handler registry
/// bound to the emitting realm. Obtained from [`Ctx::events`]; every dispatch
/// walks the realm chain from the innermost realm outward.
#[derive(Clone)]
pub struct Events {
    pub(crate) kernel: Arc<crate::kernel::KernelInner>,
    pub(crate) realm: crate::context::RealmId,
}

/// The aggregate failure of a [`Events::parallel`] dispatch: one entry per
/// panicking handler.
#[derive(Debug)]
pub struct EventAggregate {
    pub errors: Vec<anyhow::Error>,
}

impl std::fmt::Display for EventAggregate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} event handler(s) panicked", self.errors.len())
    }
}

impl std::error::Error for EventAggregate {}

impl Events {
    fn handlers(&self, key: EventKey) -> Vec<EventHandler> {
        self.kernel.handlers_for(key, self.realm)
    }

    /// Notifies all observers without waiting for them: each handler runs as
    /// a detached task on the ambient runtime, in no guaranteed order. A
    /// panicking handler is logged and cannot affect the caller.
    ///
    /// This is cordis's `emit`: fire and forget. Use [`Events::parallel`]
    /// when the caller must know every handler has finished.
    pub fn emit<E>(&self, event: &E)
    where
        E: Clone + Send + Sync + 'static,
    {
        let handlers = self.handlers(observer_key::<E>());

        for handler in handlers {
            if !self.kernel.is_active(handler.fiber) {
                continue;
            }

            let run = handler
                .body
                .downcast_ref::<ObserverBox<E>>()
                .expect("observer handler body mismatch")
                .run
                .clone();

            let payload = event.clone();

            tokio::spawn(async move {
                if let Err(panic) = AssertUnwindSafe(run(payload)).catch_unwind().await {
                    tracing::error!(
                        panic = panic_message(&panic),
                        "detached event handler panicked"
                    );
                }
            });
        }
    }

    /// Runs all observers concurrently and waits for every one of them.
    /// Panicking handlers do not stop the others; their panics are returned
    /// aggregated.
    pub async fn parallel<E>(&self, event: &E) -> Result<(), EventAggregate>
    where
        E: Clone + Send + Sync + 'static,
    {
        let handlers = self.handlers(observer_key::<E>());

        let runs = handlers
            .into_iter()
            .filter(|handler| self.kernel.is_active(handler.fiber))
            .map(|handler| {
                let run = handler
                    .body
                    .downcast_ref::<ObserverBox<E>>()
                    .expect("observer handler body mismatch")
                    .run
                    .clone();

                run(event.clone())
            });

        let results =
            futures::future::join_all(runs.map(|future| AssertUnwindSafe(future).catch_unwind()))
                .await;

        let errors: Vec<anyhow::Error> = results
            .into_iter()
            .filter_map(|result| {
                result.err().map(|panic| {
                    anyhow::anyhow!(panic_message(&panic)).context("event handler panicked")
                })
            })
            .collect();

        if errors.is_empty() {
            Ok(())
        } else {
            Err(EventAggregate { errors })
        }
    }

    /// Runs async deciders one by one, innermost realm first, until one
    /// decides. A panicking handler is logged and skipped.
    ///
    /// Returns the first decision, or `None` when every handler passed.
    pub async fn serial<E, R>(&self, event: &E) -> Option<R>
    where
        E: Clone + Send + Sync + 'static,
        R: Send + 'static,
    {
        let handlers = self.handlers(decider_key::<E, R>(EventKind::Serial));

        for handler in handlers {
            if !self.kernel.is_active(handler.fiber) {
                continue;
            }

            let run = handler
                .body
                .downcast_ref::<SerialBox<E, R>>()
                .expect("serial handler body mismatch")
                .run
                .clone();

            let attempt = AssertUnwindSafe(run(event.clone())).catch_unwind().await;

            match attempt {
                Err(panic) => {
                    tracing::error!(panic = panic_message(&panic), "serial handler panicked");
                }
                Ok(Some(decision)) => return Some(decision),
                Ok(None) => {}
            }
        }

        None
    }

    /// Runs synchronous deciders in order until one decides. Like
    /// [`Events::serial`], but the handlers cannot await: use it for instant
    /// decisions over in-memory state, such as permission checks.
    pub fn bail<E, R>(&self, event: &E) -> Option<R>
    where
        E: Clone + Send + Sync + 'static,
        R: Send + 'static,
    {
        let handlers = self.handlers(decider_key::<E, R>(EventKind::Bail));

        for handler in handlers {
            if !self.kernel.is_active(handler.fiber) {
                continue;
            }

            let run = handler
                .body
                .downcast_ref::<BailBox<E, R>>()
                .expect("bail handler body mismatch")
                .run
                .clone();

            let payload = event.clone();
            let attempt = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(&payload)));

            match attempt {
                Err(panic) => {
                    tracing::error!(panic = panic_message(&panic), "bail handler panicked");
                }
                Ok(Some(decision)) => return Some(decision),
                Ok(None) => {}
            }
        }

        None
    }

    /// Composes the registered layers around the built-in behavior, as in
    /// cordis's `waterfall`. The first-registered layer is the outermost: it
    /// receives the event and the rest of the chain, and either calls
    /// [`EventNext::run`] to continue or returns on its own, vetoing
    /// everything inside it — including the built-in behavior.
    ///
    /// A panicking layer propagates the panic to the caller: the rest of the
    /// onion cannot be resumed without the layer's cooperation.
    pub async fn waterfall<E, R, F, Fut>(&self, event: &E, builtin: F) -> R
    where
        E: Clone + Send + Sync + 'static,
        R: Send + 'static,
        F: FnOnce(E) -> Fut + Send + 'static,
        Fut: Future<Output = R> + Send + 'static,
    {
        let handlers = self.handlers(decider_key::<E, R>(EventKind::Waterfall));

        let runs: Vec<WaterfallRun<E, R>> = handlers
            .into_iter()
            .filter(|handler| self.kernel.is_active(handler.fiber))
            .map(|handler| {
                handler
                    .body
                    .downcast_ref::<WaterfallBox<E, R>>()
                    .expect("waterfall handler body mismatch")
                    .run
                    .clone()
            })
            .collect();

        // The built-in behavior is per-dispatch and consumed at most once; a
        // re-entrant layer that runs the chain twice would panic here, which
        // is the honest report of an unsupported composition.
        let payload = event.clone();
        let builtin = std::sync::Mutex::new(Some(builtin));
        let fallback: Arc<dyn Fn() -> BoxFuture<'static, R> + Send + Sync> = Arc::new(move || {
            let mut slot = builtin.lock().expect("waterfall fallback lock poisoned");
            let builtin = slot
                .take()
                .expect("the waterfall built-in behavior ran more than once");
            Box::pin(builtin(payload.clone()))
        });

        let chain: Arc<[WaterfallRun<E, R>]> = runs.into();
        let next = EventNext::entry(chain, 0, fallback);

        next.run(event.clone()).await
    }
}

impl Ctx {
    /// The event surface bound to this context's realm.
    pub fn events(&self) -> Events {
        Events {
            kernel: self.kernel.clone(),
            realm: self.realm,
        }
    }

    /// Registers an observer for events of type `E`, scoped to this fiber.
    ///
    /// The handler is removed automatically when the fiber is disposed. It
    /// serves the [`Events::emit`] and [`Events::parallel`] dispatches.
    pub fn on<E, F, Fut>(&self, handler: F) -> HandlerId
    where
        E: Clone + Send + Sync + 'static,
        F: Fn(E) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let run: ObserverRun<E> = Arc::new(move |event| Box::pin(handler(event)));

        register(self, observer_key::<E>(), Arc::new(ObserverBox { run }))
    }

    /// Registers a synchronous decider for events of type `E`, scoped to
    /// this fiber. Serves [`Events::bail`]: returning `Some` decides and
    /// short-circuits the dispatch, `None` passes.
    pub fn on_bail<E, R, F>(&self, handler: F) -> HandlerId
    where
        E: Clone + Send + Sync + 'static,
        R: Send + 'static,
        F: Fn(&E) -> Option<R> + Send + Sync + 'static,
    {
        let run: BailRun<E, R> = Arc::new(handler);

        register(
            self,
            decider_key::<E, R>(EventKind::Bail),
            Arc::new(BailBox { run }),
        )
    }

    /// Registers an asynchronous decider for events of type `E`, scoped to
    /// this fiber. Serves [`Events::serial`]: the first handler to return
    /// `Some` decides and short-circuits the dispatch.
    pub fn on_serial<E, R, F, Fut>(&self, handler: F) -> HandlerId
    where
        E: Clone + Send + Sync + 'static,
        R: Send + 'static,
        F: Fn(E) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Option<R>> + Send + 'static,
    {
        let run: SerialRun<E, R> = Arc::new(move |event| Box::pin(handler(event)));

        register(
            self,
            decider_key::<E, R>(EventKind::Serial),
            Arc::new(SerialBox { run }),
        )
    }

    /// Registers a waterfall layer for events of type `E`, scoped to this
    /// fiber. Serves [`Events::waterfall`]: the first-registered layer is the
    /// outermost.
    pub fn on_waterfall<E, R, F, Fut>(&self, handler: F) -> HandlerId
    where
        E: Clone + Send + Sync + 'static,
        R: Send + 'static,
        F: Fn(E, EventNext<E, R>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R> + Send + 'static,
    {
        let run: WaterfallRun<E, R> = Arc::new(move |event, next| Box::pin(handler(event, next)));

        register(
            self,
            decider_key::<E, R>(EventKind::Waterfall),
            Arc::new(WaterfallBox { run }),
        )
    }

    /// Emits an event of type `E` to this realm's observers, detached.
    ///
    /// Equivalent to [`Ctx::events`].[`Events::emit`].
    pub fn emit<E: Clone + Send + Sync + 'static>(&self, event: &E) {
        self.events().emit(event);
    }
}

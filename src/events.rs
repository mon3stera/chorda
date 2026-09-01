//! Scoped events: handlers live and die with their fiber.
//!
//! Events are the extension surface between the agent loop and the plugins
//! around it. Handlers registered through [`Ctx::on`] are bound to the
//! registering fiber's realm; emitting from a child realm bubbles the event
//! up through ancestor realms, and a fiber's disposal removes its handlers
//! automatically — no manual unsubscribe, no leaks.

use std::any::{Any, TypeId};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures::future::{BoxFuture, FutureExt};

use crate::context::Ctx;
use crate::fiber::panic_message;
use crate::kernel::EventHandler;

/// Erased handler callable with a type-erased event payload.
pub(crate) type ErasedHandler =
    Arc<dyn Fn(Arc<dyn Any + Send + Sync>) -> BoxFuture<'static, ()> + Send + Sync>;

/// Identifier of a registered event handler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HandlerId {
    pub(crate) type_id: TypeId,
    pub(crate) id: u64,
}

impl HandlerId {
    pub(crate) fn register<E, F, Fut>(ctx: &Ctx, handler: F) -> Self
    where
        E: Clone + Send + Sync + 'static,
        F: Fn(E) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let id = ctx.kernel.alloc_handler_id();
        let type_id = TypeId::of::<E>();

        let run: ErasedHandler = Arc::new(move |event: Arc<dyn Any + Send + Sync>| {
            let payload = event.downcast::<E>().expect("event payload type mismatch");
            Box::pin(handler(E::clone(&payload)))
        });

        ctx.kernel.add_handler(
            type_id,
            EventHandler {
                realm: ctx.realm,
                fiber: ctx.fiber,
                id,
                run,
            },
        );

        let kernel = ctx.kernel.clone();
        ctx.effect(async move {
            kernel.remove_handler(type_id, id);
        });

        Self { type_id, id }
    }
}

impl Ctx {
    /// Emits an event of type `E`.
    ///
    /// Handlers bound to this realm and its ancestors run sequentially,
    /// innermost realm first. Handlers of disposed fibers are skipped, and a
    /// panicking handler is contained and logged rather than poisoning the
    /// emit.
    pub async fn emit<E: Clone + Send + Sync + 'static>(&self, event: &E) {
        let handlers = self.kernel.handlers_for(TypeId::of::<E>(), self.realm);

        if handlers.is_empty() {
            return;
        }

        for handler in handlers {
            if !self.kernel.is_active(handler.fiber) {
                continue;
            }

            let payload: Arc<dyn Any + Send + Sync> = Arc::new(event.clone());
            let future = (handler.run)(payload);

            if let Err(panic) = AssertUnwindSafe(future).catch_unwind().await {
                tracing::error!(panic = panic_message(&panic), "event handler panicked");
            }
        }
    }
}

//! Pipelines: typed onion middlewares around a core behavior.
//!
//! A pipeline is an extension point declared by the crate that also
//! dispatches it: a marker type implementing [`Pipeline`] names the point
//! and fixes the payload (`Input`) and result (`Output`) types. Plugins
//! wrap the point with [`Ctx::middleware`]; the owner composes it with
//! [`Ctx::waterfall`], whose `fallback` is the built-in behavior every
//! middleware wraps around.
//!
//! Unlike events, which bubble from a child realm up to its ancestors,
//! pipelines run through the dispatch realm's **vertical slice** — its
//! ancestors, itself, and all descendant realms — ordered by realm depth
//! ascending. Global middlewares (root realm) wrap outermost, so a security
//! filter sees the payload before anything else; session-specific
//! middlewares run innermost, closest to the built-in behavior, so what a
//! session adds is not undone by global transforms.
//!
//! ```rust
//! use nodus::{Ctx, Kernel, Pipeline};
//!
//! struct ChatRequest;
//!
//! impl Pipeline for ChatRequest {
//!     type Input = Vec<String>;
//!     type Output = String;
//!     const NAME: &'static str = "agent/pre-request";
//! }
//!
//! # #[tokio::main(flavor = "current_thread")] async fn main() {
//! let kernel = Kernel::new();
//! let root = kernel.root_ctx();
//!
//! // A plugin rewrites the messages before they reach the client.
//! root.middleware::<ChatRequest, _, _>(|messages, next| async move {
//!     let mut messages = messages;
//!
//!     messages.push("memory".to_owned());
//!
//!     next.run(messages).await
//! });
//!
//! // The core dispatches: the fallback is the real client call, and it
//! // receives whatever the middlewares passed down.
//! let response = root
//!     .waterfall::<ChatRequest, _, _>(vec!["history".to_owned()], |messages| async move {
//!         messages.join(",")
//!     })
//!     .await;
//!
//! assert_eq!(response, "history,memory");
//! kernel.dispose().await;
//! # }
//! ```

use std::any::TypeId;
use std::future::Future;
use std::sync::Arc;

use futures::future::BoxFuture;

use crate::context::Ctx;
use crate::kernel::PipelineHandler;

/// A named extension point: the crate that dispatches it declares the
/// marker type; plugins hang middlewares on it.
pub trait Pipeline: Send + Sync + 'static {
    /// The payload passed down the chain, consumed by each layer.
    type Input: Send + 'static;

    /// The result produced by the built-in behavior (the fallback) and
    /// transformed on its way back out through the onion.
    type Output: Send + 'static;

    /// Human-readable point name for logs and diagnostics.
    const NAME: &'static str;
}

/// One layer of the onion: receives the payload and the rest of the chain.
/// Calling `next.run(payload)` continues; returning without it vetoes.
pub(crate) type SharedRun<P> = Arc<
    dyn Fn(<P as Pipeline>::Input, Next<P>) -> BoxFuture<'static, <P as Pipeline>::Output>
        + Send
        + Sync,
>;

/// Type-erased middleware stored in the kernel registry.
pub(crate) struct MiddlewareBox<P: Pipeline> {
    pub run: SharedRun<P>,
}

/// Identifier of a registered middleware.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PipelineId {
    pub(crate) type_id: TypeId,
    pub(crate) id: u64,
}

/// The callback shape of the built-in behavior at the end of the chain.
type Run<P> =
    dyn Fn(<P as Pipeline>::Input) -> BoxFuture<'static, <P as Pipeline>::Output> + Send + Sync;

/// The remaining chain plus the built-in behavior at its end.
///
/// Cloning is cheap; a middleware may run the rest of the chain more than
/// once by cloning or by holding `&self` across calls.
pub struct Next<P: Pipeline> {
    chain: Arc<[SharedRun<P>]>,
    index: usize,
    fallback: Arc<dyn Fn(P::Input) -> BoxFuture<'static, P::Output> + Send + Sync>,
}

impl<P: Pipeline> Clone for Next<P> {
    fn clone(&self) -> Self {
        Self {
            chain: Arc::clone(&self.chain),
            index: self.index,
            fallback: Arc::clone(&self.fallback),
        }
    }
}

impl<P: Pipeline> Next<P> {
    fn entry(chain: Arc<[SharedRun<P>]>, index: usize, fallback: Arc<Run<P>>) -> Self {
        Self {
            chain,
            index,
            fallback,
        }
    }

    /// Runs the rest of the chain, ending at the built-in behavior.
    ///
    /// The payload is consumed and the (possibly transformed) successor
    /// payload is chosen by the caller of `run`.
    pub fn run(&self, input: P::Input) -> BoxFuture<'static, P::Output> {
        let Some(run) = self.chain.get(self.index) else {
            return (self.fallback)(input);
        };

        let next = Next::entry(
            Arc::clone(&self.chain),
            self.index + 1,
            Arc::clone(&self.fallback),
        );

        run(input, next)
    }
}

/// Registers a middleware on the dispatch realm's slice. Shared by
/// [`Ctx::middleware`] and [`Ctx::middleware_before`].
pub(crate) fn register<P, F, Fut>(ctx: &Ctx, prepend: bool, handler: F) -> PipelineId
where
    P: Pipeline,
    F: Fn(P::Input, Next<P>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = P::Output> + Send + 'static,
{
    let id = ctx.kernel.alloc_handler_id();
    let type_id = TypeId::of::<P>();

    let run: SharedRun<P> = Arc::new(move |input, next| Box::pin(handler(input, next)));

    ctx.kernel.add_pipeline(
        type_id,
        PipelineHandler {
            realm: ctx.realm,
            fiber: ctx.fiber,
            id,
            prepend,
            handler: Arc::new(MiddlewareBox { run }),
        },
    );

    let kernel = ctx.kernel.clone();

    ctx.effect(async move {
        kernel.remove_pipeline(type_id, id);
    });

    PipelineId { type_id, id }
}

impl Ctx {
    /// Appends a middleware to the pipeline `P`. Within its realm it runs
    /// after previously registered middlewares; see the [module
    /// docs](self) for where the realm places it in the onion.
    pub fn middleware<P, F, Fut>(&self, handler: F) -> PipelineId
    where
        P: Pipeline,
        F: Fn(P::Input, Next<P>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = P::Output> + Send + 'static,
    {
        register::<P, F, Fut>(self, false, handler)
    }

    /// Prepends a middleware to the pipeline `P`: within its realm it runs
    /// before previously registered ones, newest prepend first.
    pub fn middleware_before<P, F, Fut>(&self, handler: F) -> PipelineId
    where
        P: Pipeline,
        F: Fn(P::Input, Next<P>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = P::Output> + Send + 'static,
    {
        register::<P, F, Fut>(self, true, handler)
    }

    /// Composes the pipeline `P` around `fallback` — the built-in behavior.
    /// The chain is snapshotted here; middlewares registered while the
    /// dispatch runs only join later dispatches.
    ///
    /// A panicking middleware propagates to the caller; the onion has no
    /// value to substitute for it.
    pub async fn waterfall<P, F, Fut>(&self, input: P::Input, fallback: F) -> P::Output
    where
        P: Pipeline,
        F: Fn(P::Input) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = P::Output> + Send + 'static,
    {
        let chain = self.kernel.pipeline_chain::<P>(self.realm);
        let fallback: Arc<Run<P>> = Arc::new(move |input| Box::pin(fallback(input)));

        let next = Next::entry(chain.into(), 0, fallback);

        next.run(input).await
    }
}

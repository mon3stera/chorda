//! Chorda: a Cordis-inspired, async-native plugin kernel.
//!
//! The model, in one paragraph: a [`Kernel`] owns a tree of
//! [fibers](FiberHandle) (units of async lifecycle), a tree of
//! [realms](RealmId) (scopes of typed services), and a set of scoped event
//! handlers. A [`Plugin`] declares the services it injects and starts on its
//! own fiber through [`Ctx::register`], which returns a [`FiberHandle`].
//! Plugins whose dependencies are missing stay [`State::Pending`] and start
//! automatically once [`Ctx::provide`] satisfies them. Plugin crates can
//! also submit themselves to a compile-time registry with
//! [`register_plugin!`]; [`Kernel::with_discovered_plugins`] then registers
//! whatever the host's binary linked in. Tasks spawned through
//! [`Ctx::spawn`] are tracked: plain tasks are aborted at disposal, while
//! tasks spawned with a termination signal ([`Ctx::spawn_graceful`]) are
//! signalled and then awaited. Every effect registered with
//! [`Ctx::effect`], every event handler from [`Ctx::on`], and every service
//! a fiber provided is cleaned up — in last-in-first-out order, cascading
//! through child fibers and disconnecting dependents — when the fiber is
//! disposed. [`Kernel::run_until`] drives the kernel until a shutdown
//! signal or idleness, then disposes everything before returning.
//!
//! # Example
//!
//! ```
//! use std::sync::Arc;
//!
//! use chorda::{Kernel, ServiceKey, State, fn_plugin};
//!
//! struct Counter(u32);
//!
//! # #[tokio::main(flavor = "current_thread")] async fn main() {
//! let kernel = Kernel::new();
//! let root = kernel.root_ctx();
//!
//! let greeter = fn_plugin("greeter", |ctx: chorda::Ctx| async move {
//!     let counter = ctx.get::<Counter>().expect("counter injected");
//!     assert_eq!(counter.0, 7);
//!
//!     Ok(())
//! })
//! .inject(vec![ServiceKey::of::<Counter>().into()]);
//!
//! let fiber = root.register(greeter);
//! assert_eq!(fiber.state(), State::Pending);
//!
//! root.provide(Arc::new(Counter(7))).await;
//! fiber.wait_ready().await.unwrap();
//! assert_eq!(fiber.state(), State::Ready);
//!
//! fiber.dispose().await;
//! kernel.dispose().await;
//! # }
//! ```

mod context;
mod events;
mod extract;
mod fiber;
mod kernel;
mod loader;
mod pipeline;
mod plugin;
mod registry;
mod service;

pub use anyhow;
pub use async_trait::async_trait;
pub use chorda_macros::plugin;
pub use context::{Ctx, RealmId};
pub use events::{Event, EventAggregate, EventNext, Events, HandlerId};
pub use extract::FromService;
pub use fiber::{FiberHandle, FiberId, State};
pub use inventory;
pub use kernel::Kernel;
pub use loader::{
    ConfiguredPlugin, EntryKind, EntryKinds, EntrySpec, EntryTree, Loader, LoaderReport, entry_kind,
};
pub use pipeline::{Next, Pipeline, PipelineId};
pub use plugin::{Dependency, FnPlugin, Plugin, fn_plugin};
pub use registry::{
    EventRegistration, PipelineRegistration, PluginRegistration, discover_event_names,
    discover_pipeline_names, discover_plugin_names, event_registrations, pipeline_registrations,
    plugin_registrations,
};
pub use service::ServiceKey;

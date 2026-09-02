//! Axum-style extraction of services into plugin `apply` arguments.
//!
//! The [`plugin`](crate::plugin) macro rewrites a plugin's `apply` so that
//! service parameters after `ctx` are extracted through this trait. Two
//! impls ship with the kernel:
//!
//! - `Arc<T>` — the hard form: `apply` receives `Some` always, because the
//!   macro derives it into `inject`, and the kernel holds the fiber until
//!   the service resolves;
//! - `Option<Arc<T>>` — the soft form: `None` when the service is absent.
//!   A soft dependency is not a dependency-graph edge: it adds no startup
//!   ordering and no lifecycle coupling, it is a read of the service table
//!   at apply time.
//!
//! A missing hard dependency is reported with the service's registered type
//! name, which is the same string [`ServiceKey`](crate::ServiceKey) prints.

use std::sync::Arc;

use crate::context::Ctx;
use crate::service::ServiceKey;

/// Extracts one service parameter of a `#[plugin]` `apply` from the
/// plugin's context.
pub trait FromService: Sized + Send + 'static {
    /// Extracts the value, or fails with the missing service's name.
    fn from_service(ctx: &Ctx) -> anyhow::Result<Self>;
}

impl<T: Send + Sync + 'static> FromService for Arc<T> {
    fn from_service(ctx: &Ctx) -> anyhow::Result<Self> {
        let service = ctx.get::<T>().ok_or_else(|| {
            anyhow::anyhow!("the required service {} is missing", ServiceKey::of::<T>())
        })?;

        Ok(service)
    }
}

impl<T: Send + Sync + 'static> FromService for Option<Arc<T>> {
    fn from_service(ctx: &Ctx) -> anyhow::Result<Self> {
        Ok(ctx.get::<T>())
    }
}

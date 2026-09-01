use std::future::Future;
use std::sync::Arc;

use futures::future::BoxFuture;

use crate::context::Ctx;
use crate::service::ServiceKey;

/// A unit of application logic started on its own fiber.
///
/// A plugin declares which services it needs through [`Plugin::inject`]; the
/// kernel keeps it [`State::Pending`](crate::State::Pending) until every
/// declared dependency resolves, then runs [`Plugin::apply`] once. When
/// `apply` returns, the fiber becomes ready and everything the plugin
/// registered on its context — effects, event handlers, provided services —
/// stays alive until the fiber is disposed.
#[async_trait::async_trait]
pub trait Plugin: Send + Sync + 'static {
    /// Human-readable name, used for logs, the fiber registry, and debugging.
    /// Defaults to the concrete type path.
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }

    /// Services that must resolve before this plugin may start. The plugin
    /// stays pending until all of them do.
    fn inject(&self) -> Vec<ServiceKey> {
        Vec::new()
    }

    /// Runs the plugin's setup on its fiber. Register effects and handlers on
    /// the given context; they are cleaned up automatically on disposal. An
    /// error (or panic) marks the fiber failed and cleans up what was
    /// registered so far.
    async fn apply(&self, ctx: Ctx) -> anyhow::Result<()>;
}

/// A [`Plugin`] built from a named closure. Useful for quick wiring and
/// tests; real extensions usually implement `Plugin` on a struct so its
/// fields carry their own configuration.
pub struct FnPlugin {
    name: String,
    inject: Vec<ServiceKey>,
    apply: Arc<dyn Fn(Ctx) -> BoxFuture<'static, anyhow::Result<()>> + Send + Sync>,
}

impl FnPlugin {
    /// Declares additional required services.
    pub fn inject(mut self, keys: Vec<ServiceKey>) -> Self {
        self.inject = keys;
        self
    }
}

/// Builds a [`FnPlugin`] from a name and an async setup closure.
pub fn fn_plugin<F, Fut>(name: impl Into<String>, apply: F) -> FnPlugin
where
    F: Fn(Ctx) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    let apply: Arc<dyn Fn(Ctx) -> BoxFuture<'static, anyhow::Result<()>> + Send + Sync> =
        Arc::new(move |ctx| Box::pin(apply(ctx)));
    FnPlugin {
        name: name.into(),
        inject: Vec::new(),
        apply,
    }
}

#[async_trait::async_trait]
impl Plugin for FnPlugin {
    fn name(&self) -> &str {
        &self.name
    }

    fn inject(&self) -> Vec<ServiceKey> {
        self.inject.clone()
    }

    async fn apply(&self, ctx: Ctx) -> anyhow::Result<()> {
        (self.apply)(ctx).await
    }
}

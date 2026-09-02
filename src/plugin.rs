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
/// One declared dependency of a plugin.
///
/// `Hard` is the dependency-graph edge: the plugin stays pending until the
/// service resolves, and replacing or disposing the provider disconnects the
/// plugin. `Soft` waits only for providers that declared themselves — a
/// soft dependency starts as soon as every pending or starting plugin that
/// declares the key has settled, reads whatever the table then holds
/// (`None` when nobody provided it), and is never disconnected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Dependency {
    /// Required before startup; a lifecycle-coupled graph edge.
    Hard(ServiceKey),
    /// Optional: waits for declared providers to settle, never gates on
    /// existence, never disconnects.
    Soft(ServiceKey),
}

impl Dependency {
    /// The service this dependency names.
    pub fn key(&self) -> &ServiceKey {
        match self {
            Dependency::Hard(key) | Dependency::Soft(key) => key,
        }
    }

    /// Whether the dependency is soft.
    pub fn is_soft(&self) -> bool {
        matches!(self, Dependency::Soft(_))
    }

    /// A hard dependency from a bare key.
    pub fn hard(key: ServiceKey) -> Self {
        Dependency::Hard(key)
    }

    /// A soft dependency from a bare key.
    pub fn soft(key: ServiceKey) -> Self {
        Dependency::Soft(key)
    }
}

impl From<ServiceKey> for Dependency {
    fn from(key: ServiceKey) -> Self {
        Dependency::Hard(key)
    }
}

#[async_trait::async_trait]
pub trait Plugin: Send + Sync + 'static {
    /// Human-readable name, used for logs, the fiber registry, and debugging.
    /// Defaults to the concrete type path.
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }

    /// Dependencies that must be satisfied before this plugin may start.
    /// Hard dependencies hold the plugin pending until the service resolves;
    /// soft dependencies wait only for declared providers and then start
    /// regardless. See [`Dependency`].
    fn inject(&self) -> Vec<Dependency> {
        Vec::new()
    }

    /// Services this plugin declares it will provide during `apply`. The
    /// declaration is what lets soft dependencies wait: a plugin
    /// soft-injecting `K` starts only after every pending or starting plugin
    /// that declares `provides(K)` has settled. A declaration that is never
    /// fulfilled is harmless — dependents start and read `None`.
    fn provides(&self) -> Vec<ServiceKey> {
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
    inject: Vec<Dependency>,
    provides: Vec<ServiceKey>,
    apply: Arc<dyn Fn(Ctx) -> BoxFuture<'static, anyhow::Result<()>> + Send + Sync>,
}

impl FnPlugin {
    /// Declares the plugin's dependencies; hard and soft alike.
    pub fn inject(mut self, dependencies: Vec<Dependency>) -> Self {
        self.inject = dependencies;
        self
    }

    /// Declares the services this plugin will provide, for soft dependents
    /// to wait on.
    pub fn provides(mut self, keys: Vec<ServiceKey>) -> Self {
        self.provides = keys;
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
        provides: Vec::new(),
        apply,
    }
}

#[async_trait::async_trait]
impl Plugin for FnPlugin {
    fn name(&self) -> &str {
        &self.name
    }

    fn inject(&self) -> Vec<Dependency> {
        self.inject.clone()
    }

    fn provides(&self) -> Vec<ServiceKey> {
        self.provides.clone()
    }

    async fn apply(&self, ctx: Ctx) -> anyhow::Result<()> {
        (self.apply)(ctx).await
    }
}

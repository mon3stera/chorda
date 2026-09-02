//! Compile-time plugin discovery through the [`inventory`] registry.
//!
//! A plugin crate submits a [`PluginRegistration`] with a build function:
//!
//! ```ignore
//! // in the plugin crate
//! chorda::register_plugin! {
//!     name: "memory",
//!     build: MemoryPlugin::default,
//! }
//! ```
//!
//! The host application does nothing besides depending on the plugin crate:
//! [`Kernel::with_discovered_plugins`](crate::Kernel::with_discovered_plugins)
//! registers every registration the linker kept. Registration order comes
//! from the linker and is not meaningful — plugins that need services
//! declare them with `inject` and wait for `provide`, so discovery order is
//! irrelevant.
//!
//! # The linker contract
//!
//! Rust links rlibs lazily: object files are pulled in only when something
//! in them is referenced. The contract, verified on this toolchain in debug
//! and release builds by the `discovery` integration tests:
//!
//! - A plugin crate that only sits in `Cargo.toml` is dropped by the linker
//!   entirely — its registration never reaches the binary.
//! - One `use plugin_crate as _;` line in the host **binary** crate is
//!   enough to keep the crate linked and its registration discovered. A
//!   real reference (calling any item) works equally well.
//!
//! So the host declares its plugin set once, as plain imports:
//!
//! ```ignore
//! // main.rs of the host application
//! use h_plugin_memory as _;
//! use h_plugin_mcp as _;
//!
//! let kernel = Kernel::with_discovered_plugins();
//! ```

use std::sync::Arc;

use crate::Plugin;

/// One discovered plugin: a name for logs and dedup, plus a constructor
/// producing the plugin instance.
#[derive(Clone, Copy)]
pub struct PluginRegistration {
    pub name: &'static str,
    pub build: fn() -> Arc<dyn Plugin>,
}

inventory::collect!(PluginRegistration);

/// Submits the enclosing crate's plugin to the compile-time registry.
///
/// `build` must be a callable expression returning a value that implements
/// [`Plugin`] (a `Default::default` path, a `new` path, or a closure). The
/// plugin crate needs `inventory` in its dependencies for the submission to
/// compile.
///
/// ```
/// use std::sync::Arc;
///
/// use chorda::{Ctx, Plugin};
///
/// struct DemoPlugin;
///
/// #[chorda::async_trait]
/// impl Plugin for DemoPlugin {
///     async fn apply(&self, _ctx: Ctx) -> chorda::anyhow::Result<()> {
///         Ok(())
///     }
/// }
///
/// chorda::register_plugin! {
///     name: "demo",
///     build: || DemoPlugin,
/// }
/// ```
#[macro_export]
macro_rules! register_plugin {
    (name: $name:expr, build: $build:expr $(,)?) => {
        ::inventory::submit! {
            $crate::PluginRegistration {
                name: $name,
                build: || ::std::sync::Arc::new($build()) as ::std::sync::Arc<dyn $crate::Plugin>,
            }
        }
    };
}

/// Every plugin registration the linker kept, sorted by name for
/// deterministic startup, with duplicate names collapsed to the first
/// occurrence.
pub fn plugin_registrations() -> Vec<PluginRegistration> {
    let mut registrations: Vec<PluginRegistration> =
        inventory::iter::<PluginRegistration>().copied().collect();

    registrations.sort_by(|a, b| a.name.cmp(b.name));
    registrations.dedup_by(|a, b| {
        if a.name == b.name {
            tracing::warn!(plugin = a.name, "duplicate plugin registration ignored");
            true
        } else {
            false
        }
    });

    registrations
}

/// The names of all discovered plugins; handy for tests and diagnostics.
pub fn discover_plugin_names() -> Vec<String> {
    plugin_registrations()
        .into_iter()
        .map(|registration| registration.name.to_owned())
        .collect()
}

//! A referenced test plugin: the host calls `anchor`, so the linker keeps
//! this crate's object files and its registration survives.

use nodus::{Ctx, Plugin};

pub struct AlphaPlugin;

impl Default for AlphaPlugin {
    fn default() -> Self {
        Self
    }
}

#[nodus::async_trait]
impl Plugin for AlphaPlugin {
    fn name(&self) -> &str {
        "alpha"
    }

    async fn apply(&self, _ctx: Ctx) -> nodus::anyhow::Result<()> {
        Ok(())
    }
}

nodus::register_plugin! {
    name: "alpha",
    build: AlphaPlugin::default,
}

/// Referenced once by the host so the linker keeps this crate linked.
#[doc(hidden)]
pub fn anchor() {}

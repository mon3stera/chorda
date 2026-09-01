//! An unreferenced test plugin: nothing in the host ever names this crate,
//! so whether its registration survives linking is exactly what the
//! `discovery` integration test observes.

use nodus::{Ctx, Plugin};

pub struct BetaPlugin;

impl Default for BetaPlugin {
    fn default() -> Self {
        Self
    }
}

#[nodus::async_trait]
impl Plugin for BetaPlugin {
    fn name(&self) -> &str {
        "beta"
    }

    async fn apply(&self, _ctx: Ctx) -> nodus::anyhow::Result<()> {
        Ok(())
    }
}

nodus::register_plugin! {
    name: "beta",
    build: BetaPlugin::default,
}

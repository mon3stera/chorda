//! Integration tests for the `#[plugin]` attribute macro: service
//! extraction, derived inject, hard and soft dependencies.

use std::sync::Arc;

use chorda::{Ctx, Dependency, Kernel, Plugin, ServiceKey, State};

#[derive(Debug)]
struct Config {
    endpoint: &'static str,
}

/// A struct plugin using the macro: `config` is hard, `sink` is soft, and
/// `inject` is derived from the parameters.
struct GreeterPlugin {
    seen: Arc<std::sync::Mutex<Vec<String>>>,
}

#[chorda::plugin]
impl Plugin for GreeterPlugin {
    fn name(&self) -> &str {
        "greeter"
    }

    async fn apply(
        &self,
        ctx: Ctx,
        config: Arc<Config>,
        sink: Option<Arc<String>>,
    ) -> anyhow::Result<()> {
        {
            let mut seen = self.seen.lock().expect("seen lock");

            seen.push(format!("config:{}", config.endpoint));
            seen.push(format!("sink:{}", sink.is_some()));
        }

        ctx.provide(Arc::new("greeted".to_owned())).await;

        Ok(())
    }
}

#[tokio::test]
async fn the_macro_extracts_hard_and_soft_services() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));

    root.provide(Arc::new(Config {
        endpoint: "tcp://x",
    }))
    .await;

    root.provide(Arc::new("boot-sink".to_owned())).await;

    let fiber = root.register(GreeterPlugin {
        seen: Arc::clone(&seen),
    });

    fiber.wait_ready().await.expect("both services present");

    assert_eq!(
        *seen.lock().expect("seen lock"),
        vec!["config:tcp://x".to_owned(), "sink:true".to_owned(),],
        "hard and soft parameters were extracted from the context"
    );

    assert_eq!(
        root.get::<String>().map(|value| (*value).clone()),
        Some("greeted".to_owned()),
        "the plugin body runs unchanged inside the generated apply"
    );

    kernel.dispose().await;
}

#[tokio::test]
async fn the_derived_inject_gates_on_hard_dependencies_only() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));

    // No Config provided: the derived hard dependency holds the fiber
    // pending, even though the soft dependency is absent too.
    let fiber = root.register(GreeterPlugin {
        seen: Arc::clone(&seen),
    });

    assert_eq!(fiber.state(), State::Pending);

    root.provide(Arc::new(Config {
        endpoint: "tcp://y",
    }))
    .await;

    fiber
        .wait_ready()
        .await
        .expect("the hard dependency resolved and the soft one is optional");

    assert_eq!(
        *seen.lock().expect("seen lock"),
        vec!["config:tcp://y".to_owned(), "sink:false".to_owned()],
        "the plugin started without the soft service and read None"
    );

    kernel.dispose().await;
}

#[tokio::test]
async fn soft_macro_parameters_wait_for_declared_providers() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));

    root.provide(Arc::new(Config {
        endpoint: "tcp://y",
    }))
    .await;

    // The greeter is queued before the provider is even registered: the
    // batch makes the provider's provides declaration visible to the soft
    // dependency before anything starts.
    let greeter: Arc<dyn Plugin> = Arc::new(GreeterPlugin {
        seen: Arc::clone(&seen),
    });

    let provider = chorda::fn_plugin("sink-provider", |ctx: Ctx| async move {
        ctx.provide(Arc::new("late-sink".to_owned())).await;

        Ok(())
    })
    .provides(vec![ServiceKey::of::<String>()]);

    let handles = root.register_batch(vec![greeter, Arc::new(provider)]);

    let fiber = handles[0].clone();

    assert_eq!(
        fiber.state(),
        State::Pending,
        "the batch's provider is still settling"
    );

    handles[1].wait_ready().await.expect("provider ready");
    fiber.wait_ready().await.expect("started after settle");

    assert_eq!(
        *seen.lock().expect("seen lock"),
        vec!["config:tcp://y".to_owned(), "sink:true".to_owned()],
    );

    kernel.dispose().await;
}

/// A hand-written `inject` wins over the derived one.
struct OverridingPlugin {
    seen: Arc<std::sync::atomic::AtomicBool>,
}

#[chorda::plugin]
impl Plugin for OverridingPlugin {
    fn name(&self) -> &str {
        "overriding"
    }

    fn inject(&self) -> Vec<Dependency> {
        // Only the u8 gate, deliberately NOT the Config the apply extracts.
        vec![ServiceKey::of::<u8>().into()]
    }

    async fn apply(&self, _ctx: Ctx, _config: Arc<Config>) -> anyhow::Result<()> {
        self.seen.store(true, std::sync::atomic::Ordering::SeqCst);

        Ok(())
    }
}

#[tokio::test]
async fn a_hand_written_inject_wins_over_the_derivation() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    let seen = Arc::new(std::sync::atomic::AtomicBool::new(false));

    root.provide(Arc::new(1u8)).await;

    // Config is missing, but the hand-written inject does not name it: the
    // plugin starts and the hard extraction fails honestly.
    let fiber = root.register(OverridingPlugin { seen: seen.clone() });

    let outcome = fiber.wait_ready().await;

    assert!(
        outcome.is_err(),
        "the overriding inject started the plugin without Config"
    );

    assert_eq!(
        fiber.state(),
        State::Failed,
        "the missing hard service surfaces as an apply failure"
    );

    assert!(
        !seen.load(std::sync::atomic::Ordering::SeqCst),
        "the body never ran"
    );

    kernel.dispose().await;
}

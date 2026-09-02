//! A service change in one realm must not disturb a sibling realm.

use std::sync::Arc;

use chorda::{Ctx, Kernel, ServiceKey, State, fn_plugin};

struct Db(&'static str);

/// Two sibling realms each provide their own `Db` and each run a consumer
/// that injected `Db`. Replacing the `Db` of realm A must not touch the
/// consumer living in realm B: it resolved a different value entirely.
#[tokio::test]
async fn replacing_a_service_leaves_sibling_realms_alone() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();

    let (session_a, session_b) = (root.derive(), root.derive());

    session_a.provide(Arc::new(Db("a-1"))).await;
    session_b.provide(Arc::new(Db("b-1"))).await;

    let consumer = |name: &'static str| {
        fn_plugin(name, |ctx: Ctx| async move {
            ctx.get::<Db>().expect("db injected");

            Ok(())
        })
        .inject(vec![ServiceKey::of::<Db>()])
    };

    let (fiber_a, fiber_b) = (
        session_a.register(consumer("consumer-a")),
        session_b.register(consumer("consumer-b")),
    );

    fiber_a.wait_ready().await.unwrap();
    fiber_b.wait_ready().await.unwrap();

    // Replace only realm A's binding.
    session_a.provide(Arc::new(Db("a-2"))).await;

    assert_eq!(
        fiber_a.state(),
        State::Disposed,
        "consumer-a bound the replaced value, so it must be disconnected"
    );
    assert_eq!(
        fiber_b.state(),
        State::Ready,
        "consumer-b resolved Db from its own realm and must be untouched"
    );
    assert_eq!(session_b.get::<Db>().map(|db| db.0), Some("b-1"));
}

/// The same question for disposal rather than replacement.
#[tokio::test]
async fn disposing_a_provider_leaves_sibling_realms_alone() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();

    let (session_a, session_b) = (root.derive(), root.derive());

    let provider_a = session_a.register(fn_plugin("provider-a", |ctx: Ctx| async move {
        ctx.provide(Arc::new(Db("a"))).await;

        Ok(())
    }));
    provider_a.wait_ready().await.unwrap();

    session_b.provide(Arc::new(Db("b"))).await;

    let consumer_b = session_b.register(
        fn_plugin("consumer-b", |ctx: Ctx| async move {
            ctx.get::<Db>().expect("db injected");

            Ok(())
        })
        .inject(vec![ServiceKey::of::<Db>()]),
    );
    consumer_b.wait_ready().await.unwrap();

    provider_a.dispose().await;

    assert_eq!(
        consumer_b.state(),
        State::Ready,
        "consumer-b never depended on realm A's provider"
    );
    assert_eq!(session_b.get::<Db>().map(|db| db.0), Some("b"));
}

/// A consumer that inherits the key from an ancestor realm is still a
/// dependent: shadowing is what makes a fiber independent, not depth.
#[tokio::test]
async fn consumers_inheriting_from_an_ancestor_realm_are_still_disconnected() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();

    root.provide(Arc::new(Db("root"))).await;

    let child = root.derive();
    let consumer = child.register(
        fn_plugin("consumer", |ctx: Ctx| async move {
            ctx.get::<Db>().expect("db injected");

            Ok(())
        })
        .inject(vec![ServiceKey::of::<Db>()]),
    );
    consumer.wait_ready().await.unwrap();

    root.provide(Arc::new(Db("root-2"))).await;

    assert_eq!(
        consumer.state(),
        State::Disposed,
        "the consumer resolved the root binding, which was replaced"
    );
}

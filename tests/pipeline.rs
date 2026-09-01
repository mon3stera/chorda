//! Integration tests for pipelines: onion composition, vetoing, payload
//! transformation, realm slicing, prepend ordering, and fiber scoping.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use nodus::{Ctx, Kernel, Pipeline, fn_plugin};

type Shared<T> = Arc<std::sync::Mutex<T>>;

struct PreRequest;

impl Pipeline for PreRequest {
    type Input = Vec<String>;
    type Output = String;

    const NAME: &'static str = "test/pre-request";
}

fn recorder() -> Shared<Vec<String>> {
    Arc::new(std::sync::Mutex::new(Vec::new()))
}

#[tokio::test]
async fn waterfall_composes_middlewares_around_the_fallback() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    let entered = recorder();

    for name in ["m1", "m2", "m3"] {
        let entered = entered.clone();
        let name = name.to_owned();

        root.middleware::<PreRequest, _, _>(move |mut messages, next| {
            let entered = entered.clone();
            let name = name.clone();

            async move {
                entered.lock().unwrap().push(name.clone());
                messages.push(name.clone());

                let response = next.run(messages).await;

                format!("{response}+{name}")
            }
        });
    }

    let response = root
        .waterfall::<PreRequest, _, _>(vec!["base".to_owned()], |messages| async move {
            messages.join(",")
        })
        .await;

    assert_eq!(
        response, "base,m1,m2,m3+m3+m2+m1",
        "payloads flow down in order, results flow back out in reverse"
    );
    assert_eq!(
        *entered.lock().unwrap(),
        vec!["m1".to_owned(), "m2".to_owned(), "m3".to_owned()],
        "middlewares run outermost first"
    );

    kernel.dispose().await;
}

#[tokio::test]
async fn a_middleware_can_veto_by_not_calling_next() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    let fallback_ran = Arc::new(AtomicBool::new(false));

    root.middleware::<PreRequest, _, _>(|_messages, _next| async move { "vetoed".to_owned() });

    let fallback = fallback_ran.clone();

    let response = root
        .waterfall::<PreRequest, _, _>(vec!["base".to_owned()], move |messages| {
            let fallback = fallback.clone();

            async move {
                fallback.store(true, Ordering::SeqCst);
                messages.join(",")
            }
        })
        .await;

    assert_eq!(response, "vetoed");
    assert!(
        !fallback_ran.load(Ordering::SeqCst),
        "the builtin must not run"
    );

    kernel.dispose().await;
}

#[tokio::test]
async fn a_middleware_can_transform_the_payload_for_downstream() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    let seen = recorder();

    root.middleware::<PreRequest, _, _>(move |mut messages, next| {
        let seen = seen.clone();

        async move {
            seen.lock().unwrap().push(messages.join(","));

            messages.clear();
            messages.push("rewritten".to_owned());

            next.run(messages).await
        }
    });

    let response = root
        .waterfall::<PreRequest, _, _>(vec!["original".to_owned()], |messages| async move {
            messages.join(",")
        })
        .await;

    assert_eq!(response, "rewritten");

    kernel.dispose().await;
}

#[tokio::test]
async fn the_slice_covers_descendants_and_excludes_sibling_branches() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    let entered = recorder();

    let session = root.derive();
    let sibling = root.derive();

    for (ctx, name) in [(&root, "root"), (&session, "s1"), (&sibling, "s2")] {
        let entered = entered.clone();
        let name = name.to_owned();

        ctx.middleware::<PreRequest, _, _>(move |messages, next| {
            let entered = entered.clone();
            let name = name.clone();

            async move {
                entered.lock().unwrap().push(name.clone());

                next.run(messages).await
            }
        });
    }

    // From the root: the vertical slice covers every descendant, global
    // middlewares outermost.
    root.waterfall::<PreRequest, _, _>(Vec::new(), |messages| async move { messages.join(",") })
        .await;

    assert_eq!(
        *entered.lock().unwrap(),
        vec!["root".to_owned(), "s1".to_owned(), "s2".to_owned()],
        "depth ascending: root wraps its descendants"
    );

    entered.lock().unwrap().clear();

    // From s1: the slice is root + s1; the sibling branch is invisible.
    session
        .waterfall::<PreRequest, _, _>(Vec::new(), |messages| async move { messages.join(",") })
        .await;

    assert_eq!(
        *entered.lock().unwrap(),
        vec!["root".to_owned(), "s1".to_owned()],
        "sibling realms must not join the dispatch"
    );

    kernel.dispose().await;
}

#[tokio::test]
async fn prepends_run_before_appends_within_a_realm() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    let entered = recorder();

    for (prepend, name) in [(false, "first"), (true, "early"), (false, "last")] {
        let entered = entered.clone();
        let name = name.to_owned();

        let handler = move |messages: Vec<String>, next: nodus::Next<PreRequest>| {
            let entered = entered.clone();
            let name = name.clone();

            async move {
                entered.lock().unwrap().push(name.clone());

                next.run(messages).await
            }
        };

        if prepend {
            root.middleware_before::<PreRequest, _, _>(handler);
        } else {
            root.middleware::<PreRequest, _, _>(handler);
        }
    }

    root.waterfall::<PreRequest, _, _>(Vec::new(), |messages| async move { messages.join(",") })
        .await;

    assert_eq!(
        *entered.lock().unwrap(),
        vec!["early".to_owned(), "first".to_owned(), "last".to_owned()],
        "prepends run before appends regardless of registration order"
    );

    kernel.dispose().await;
}

#[tokio::test]
async fn middlewares_die_with_their_fiber() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    let entered = recorder();

    let plugin = {
        let entered = entered.clone();

        fn_plugin("piped", move |ctx: Ctx| {
            let entered = entered.clone();

            async move {
                ctx.middleware::<PreRequest, _, _>(move |messages, next| {
                    let entered = entered.clone();

                    async move {
                        entered.lock().unwrap().push("plugin".to_owned());

                        next.run(messages).await
                    }
                });

                Ok(())
            }
        })
    };

    let fiber = root.register(plugin);
    fiber.wait_ready().await.unwrap();

    root.waterfall::<PreRequest, _, _>(Vec::new(), |messages| async move { messages.join(",") })
        .await;

    assert_eq!(
        *entered.lock().unwrap(),
        vec!["plugin".to_owned()],
        "sanity: the plugin middleware runs while the fiber lives"
    );

    entered.lock().unwrap().clear();

    fiber.dispose().await;

    root.waterfall::<PreRequest, _, _>(Vec::new(), |messages| async move { messages.join(",") })
        .await;

    assert_eq!(
        *entered.lock().unwrap(),
        Vec::<String>::new(),
        "the disposed fiber's middleware must be gone"
    );

    kernel.dispose().await;
}

#[tokio::test]
async fn dispatches_snapshot_the_chain() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    let entered = recorder();

    let late_ctx = root.clone();
    let registered_late = Arc::new(AtomicBool::new(false));
    let entered_outer = entered.clone();

    root.middleware::<PreRequest, _, _>(move |messages, next| {
        let entered = entered_outer.clone();
        let late_ctx = late_ctx.clone();
        let registered_late = registered_late.clone();

        async move {
            entered.lock().unwrap().push("outer".to_owned());

            if !registered_late.swap(true, Ordering::SeqCst) {
                late_ctx.middleware::<PreRequest, _, _>(move |messages, next| {
                    let entered = entered.clone();

                    async move {
                        entered.lock().unwrap().push("late".to_owned());

                        next.run(messages).await
                    }
                });
            }

            next.run(messages).await
        }
    });

    let first = root
        .waterfall::<PreRequest, _, _>(Vec::new(), |messages| async move { messages.join(",") })
        .await;
    let _ = first;

    assert_eq!(
        *entered.lock().unwrap(),
        vec!["outer".to_owned()],
        "a middleware registered mid-dispatch must not join it"
    );

    entered.lock().unwrap().clear();

    root.waterfall::<PreRequest, _, _>(Vec::new(), |messages| async move { messages.join(",") })
        .await;

    assert_eq!(
        *entered.lock().unwrap(),
        vec!["outer".to_owned(), "late".to_owned()],
        "the late middleware joins later dispatches"
    );

    kernel.dispose().await;
}

#[tokio::test]
async fn an_empty_chain_falls_straight_through_to_the_fallback() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();

    let response = root
        .waterfall::<PreRequest, _, _>(labels("a,b"), |messages| async move { messages.join("+") })
        .await;

    assert_eq!(response, "a+b");

    kernel.dispose().await;
}

fn labels(joined: &str) -> Vec<String> {
    joined.split(',').map(str::to_owned).collect()
}

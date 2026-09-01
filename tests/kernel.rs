//! Integration tests for the nodus kernel: reactive plugin start, fiber
//! cascade disposal, realm-scoped services, effects, and scoped events.

use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use nodus::{Ctx, EventNext, FiberId, Kernel, Plugin, ServiceKey, State, fn_plugin};

type Shared<T> = Arc<StdMutex<T>>;

fn shared<T>(value: T) -> Shared<T> {
    Arc::new(StdMutex::new(value))
}

#[tokio::test]
async fn services_resolve_through_the_realm_chain() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    root.provide(Arc::new(1u32)).await;

    let child = root.derive();
    assert_eq!(child.get::<u32>().map(|value| *value), Some(1));

    let grandchild = child.derive();
    assert_eq!(grandchild.get::<u32>().map(|value| *value), Some(1));

    child.provide(Arc::new(2u64)).await;

    assert_eq!(child.get::<u64>().map(|value| *value), Some(2));
    assert_eq!(grandchild.get::<u64>().map(|value| *value), Some(2));
    assert!(
        root.get::<u64>().is_none(),
        "parent realms must not see child services"
    );
}

#[tokio::test]
async fn pending_plugins_start_once_dependencies_are_provided() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    let started = Arc::new(AtomicBool::new(false));

    let plugin = {
        let started = started.clone();

        fn_plugin("needs-counter", move |ctx: Ctx| {
            let started = started.clone();

            async move {
                let counter = ctx.get::<u32>().expect("counter injected");
                started.store(*counter == 7, Ordering::SeqCst);

                Ok(())
            }
        })
        .inject(vec![ServiceKey::of::<u32>()])
    };

    let fiber = root.register(plugin);
    assert_eq!(
        fiber.state(),
        State::Pending,
        "missing dependencies keep the plugin pending"
    );

    root.provide(Arc::new(7u32)).await;
    fiber
        .wait_ready()
        .await
        .expect("plugin should start once the service appears");

    assert!(started.load(Ordering::SeqCst));
    assert_eq!(fiber.state(), State::Ready);
}

#[tokio::test]
async fn pending_plugins_can_be_disposed_without_starting() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();

    let plugin = fn_plugin("never-starts", |_ctx: Ctx| async { Ok(()) })
        .inject(vec![ServiceKey::of::<u32>()]);

    let fiber = root.register(plugin);
    assert_eq!(fiber.state(), State::Pending);

    fiber.dispose().await;
    assert_eq!(fiber.state(), State::Disposed);

    root.provide(Arc::new(1u32)).await;
    tokio::time::sleep(Duration::from_millis(10)).await;

    assert_eq!(
        fiber.state(),
        State::Disposed,
        "a disposed plugin must never start"
    );
}

#[tokio::test]
async fn effects_run_last_in_first_out_on_disposal() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    let order: Shared<Vec<&'static str>> = shared(Vec::new());

    let plugin = {
        let order = order.clone();

        fn_plugin("effects", move |ctx: Ctx| {
            let order = order.clone();

            async move {
                ctx.effect({
                    let order = order.clone();

                    async move {
                        order.lock().unwrap().push("first-registered");
                    }
                });

                ctx.effect({
                    let order = order.clone();

                    async move {
                        order.lock().unwrap().push("second-registered");
                    }
                });

                Ok(())
            }
        })
    };

    let fiber = root.register(plugin);
    fiber.wait_ready().await.unwrap();
    fiber.dispose().await;

    assert_eq!(
        *order.lock().unwrap(),
        vec!["second-registered", "first-registered"],
        "effects must drain last-in-first-out"
    );
}

#[tokio::test]
async fn disposing_a_provider_disconnects_dependents_and_removes_services() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();

    let provider = fn_plugin("provider", |ctx: Ctx| async move {
        ctx.provide(Arc::new(1u32)).await;

        Ok(())
    });

    let dependent = fn_plugin("dependent", |ctx: Ctx| async move {
        assert_eq!(*ctx.get::<u32>().expect("injected"), 1);

        Ok(())
    })
    .inject(vec![ServiceKey::of::<u32>()]);

    let bystander = fn_plugin("bystander", |_ctx: Ctx| async { Ok(()) });

    let provider_fiber = root.register(provider);
    provider_fiber.wait_ready().await.unwrap();

    let dependent_fiber = root.register(dependent);
    dependent_fiber.wait_ready().await.unwrap();

    let bystander_fiber = root.register(bystander);
    bystander_fiber.wait_ready().await.unwrap();

    provider_fiber.dispose().await;

    assert_eq!(
        dependent_fiber.state(),
        State::Disposed,
        "dependents must be disconnected"
    );
    assert_eq!(
        bystander_fiber.state(),
        State::Ready,
        "unrelated fibers must survive"
    );
    assert!(
        root.get::<u32>().is_none(),
        "the service must leave the table"
    );
}

#[tokio::test]
async fn disposal_cascades_to_child_fibers() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    let cleaned = Arc::new(AtomicBool::new(false));

    let plugin = {
        let cleaned = cleaned.clone();

        fn_plugin("parent", move |ctx: Ctx| {
            let cleaned = cleaned.clone();

            async move {
                let child = ctx.fork("child");
                child.ctx().effect(async move {
                    cleaned.store(true, Ordering::SeqCst);
                });

                Ok(())
            }
        })
    };

    let fiber = root.register(plugin);
    fiber.wait_ready().await.unwrap();
    fiber.dispose().await;

    assert_eq!(fiber.state(), State::Disposed);
    assert!(
        cleaned.load(Ordering::SeqCst),
        "child effects must run during the cascade"
    );
}

#[tokio::test]
async fn a_failing_plugin_is_marked_failed_and_still_cleans_up() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    let cleaned = Arc::new(AtomicBool::new(false));

    let plugin = {
        let cleaned = cleaned.clone();

        fn_plugin("fails", move |ctx: Ctx| {
            let cleaned = cleaned.clone();

            async move {
                ctx.effect(async move {
                    cleaned.store(true, Ordering::SeqCst);
                });

                Err(anyhow::anyhow!("boom"))
            }
        })
    };

    let fiber = root.register(plugin);
    assert!(fiber.wait_ready().await.is_err());

    assert_eq!(fiber.state(), State::Failed);
    assert!(
        cleaned.load(Ordering::SeqCst),
        "effects registered before the failure must run"
    );
}

#[tokio::test]
async fn a_panicking_plugin_is_contained_and_cleans_up() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    let cleaned = Arc::new(AtomicBool::new(false));

    let plugin = {
        let cleaned = cleaned.clone();

        fn_plugin("panics", move |ctx: Ctx| {
            let cleaned = cleaned.clone();

            async move {
                ctx.effect(async move {
                    cleaned.store(true, Ordering::SeqCst);
                });

                panic!("boom");
            }
        })
    };

    let fiber = root.register(plugin);
    assert!(fiber.wait_ready().await.is_err());

    assert_eq!(fiber.state(), State::Failed);
    assert!(
        cleaned.load(Ordering::SeqCst),
        "effects registered before the panic must run"
    );

    // The kernel must still be fully operational afterwards.
    let survivor = root.register(fn_plugin("survivor", |_ctx: Ctx| async { Ok(()) }));
    survivor
        .wait_ready()
        .await
        .expect("kernel keeps working after a panic");
    kernel.dispose().await;
}

#[tokio::test]
async fn disposing_a_starting_fiber_aborts_setup_and_runs_effects() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    let cleaned = Arc::new(AtomicBool::new(false));

    let (_keep_tx, rx) = tokio::sync::watch::channel(());

    let plugin = {
        let cleaned = cleaned.clone();

        fn_plugin("hangs", move |ctx: Ctx| {
            let cleaned = cleaned.clone();
            let mut rx = rx.clone();

            async move {
                ctx.effect(async move {
                    cleaned.store(true, Ordering::SeqCst);
                });

                let _ = rx.changed().await;

                Ok(())
            }
        })
    };

    let fiber = root.register(plugin);
    tokio::time::sleep(Duration::from_millis(50)).await;

    fiber.dispose().await;

    assert!(
        cleaned.load(Ordering::SeqCst),
        "effects registered before the abort must run"
    );
    assert_eq!(fiber.state(), State::Disposed);
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Message(&'static str);

#[tokio::test]
async fn events_reach_scoped_handlers_and_die_with_the_fiber() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    let received: Shared<Vec<&'static str>> = shared(Vec::new());

    let listener = {
        let received = received.clone();

        fn_plugin("listener", move |ctx: Ctx| {
            let received = received.clone();

            async move {
                ctx.on(move |event: Message| {
                    let received = received.clone();

                    async move {
                        received.lock().unwrap().push(event.0);
                    }
                });

                Ok(())
            }
        })
    };

    let fiber = root.register(listener);
    fiber.wait_ready().await.unwrap();

    root.events().parallel(&Message("before")).await.unwrap();
    assert_eq!(*received.lock().unwrap(), vec!["before"]);

    fiber.dispose().await;
    root.events().parallel(&Message("after")).await.unwrap();

    assert_eq!(
        *received.lock().unwrap(),
        vec!["before"],
        "handlers must be removed with their fiber"
    );
}

#[tokio::test]
async fn events_emitted_in_a_realm_bubble_up_to_ancestor_handlers() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    let child = root.derive();
    let parent_saw: Shared<Vec<&'static str>> = shared(Vec::new());
    let child_saw: Shared<Vec<&'static str>> = shared(Vec::new());

    {
        let parent_saw = parent_saw.clone();

        root.on(move |event: Message| {
            let parent_saw = parent_saw.clone();

            async move {
                parent_saw.lock().unwrap().push(event.0);
            }
        });
    }

    {
        let child_saw = child_saw.clone();

        child.on(move |event: Message| {
            let child_saw = child_saw.clone();

            async move {
                child_saw.lock().unwrap().push(event.0);
            }
        });
    }

    root.events().parallel(&Message("from-root")).await.unwrap();

    assert_eq!(*parent_saw.lock().unwrap(), vec!["from-root"]);
    assert!(
        child_saw.lock().unwrap().is_empty(),
        "parent emits must not reach child handlers"
    );

    child
        .events()
        .parallel(&Message("from-child"))
        .await
        .unwrap();

    assert_eq!(*child_saw.lock().unwrap(), vec!["from-child"]);
    assert_eq!(
        *parent_saw.lock().unwrap(),
        vec!["from-root", "from-child"],
        "child emits must bubble up to ancestors"
    );
}

#[tokio::test]
async fn replacing_a_service_disconnects_its_dependents() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();

    let dependent = fn_plugin("dependent", |ctx: Ctx| async move {
        assert_eq!(*ctx.get::<u32>().expect("injected"), 1);

        Ok(())
    })
    .inject(vec![ServiceKey::of::<u32>()]);

    let fiber = root.register(dependent);
    root.provide(Arc::new(1u32)).await;
    fiber.wait_ready().await.unwrap();

    root.provide(Arc::new(2u32)).await;

    assert_eq!(
        fiber.state(),
        State::Disposed,
        "dependents of a replaced service must be disconnected"
    );
    assert_eq!(root.get::<u32>().map(|value| *value), Some(2));
}

#[tokio::test]
async fn forked_scopes_collect_their_own_effects() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    let cleaned = Arc::new(AtomicBool::new(false));

    let child = root.fork("scope");

    {
        let cleaned = cleaned.clone();
        child.ctx().effect(async move {
            cleaned.store(true, Ordering::SeqCst);
        });
    }

    assert_eq!(child.state(), State::Ready);
    assert!(!cleaned.load(Ordering::SeqCst));

    child.dispose().await;

    assert!(cleaned.load(Ordering::SeqCst));
    assert_eq!(
        root.fiber_id(),
        FiberId::root(),
        "the root fiber is untouched"
    );
}

#[tokio::test]
async fn registering_on_a_dead_fiber_yields_a_born_disposed_child() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    kernel.dispose().await;

    let fiber = root.register(fn_plugin("late", |_ctx: Ctx| async { Ok(()) }));

    assert_eq!(fiber.state(), State::Disposed);
}

#[tokio::test]
async fn kernel_dispose_cascades_everything() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();

    let fiber = root.register(fn_plugin("long-lived", |_ctx: Ctx| async { Ok(()) }));
    fiber.wait_ready().await.unwrap();

    kernel.dispose().await;

    assert_eq!(fiber.state(), State::Disposed);
}

#[tokio::test]
async fn plugins_can_derive_realms_to_isolate_their_services() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    let isolated = Arc::new(AtomicBool::new(false));

    let plugin = {
        let isolated = isolated.clone();

        fn_plugin("isolator", move |ctx: Ctx| {
            let isolated = isolated.clone();

            async move {
                let private = ctx.derive();
                private.provide(Arc::new(99u32)).await;

                assert_eq!(private.get::<u32>().map(|value| *value), Some(99));
                assert!(
                    ctx.get::<u32>().is_none(),
                    "the parent realm must not see it"
                );
                isolated.store(true, Ordering::SeqCst);

                Ok(())
            }
        })
    };

    let fiber = root.register(plugin);
    fiber.wait_ready().await.unwrap();

    assert!(isolated.load(Ordering::SeqCst));
    assert!(
        root.get::<u32>().is_none(),
        "the isolation must outlive the plugin's turn"
    );
}

/// A struct plugin: the shape real extensions will use.
struct CounterPlugin {
    start: u32,
}

#[async_trait::async_trait]
impl Plugin for CounterPlugin {
    fn name(&self) -> &str {
        "counter"
    }

    fn inject(&self) -> Vec<ServiceKey> {
        vec![ServiceKey::of::<u32>()]
    }

    async fn apply(&self, ctx: Ctx) -> anyhow::Result<()> {
        let initial = *ctx.get::<u32>().expect("counter service injected");
        assert_eq!(initial, self.start);

        ctx.provide(Arc::new(initial + 1)).await;

        Ok(())
    }
}

#[tokio::test]
async fn struct_plugins_chain_through_the_service_table() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    root.provide(Arc::new(1u32)).await;

    let fiber = root.register(CounterPlugin { start: 1 });
    fiber.wait_ready().await.unwrap();

    assert_eq!(
        root.get::<u32>().map(|value| *value),
        Some(2),
        "the plugin re-provided +1"
    );
    assert_eq!(fiber.name(), "counter");
}

#[tokio::test]
async fn nested_plugins_form_a_tree_and_cascade_together() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();

    // The parent plugin registers a child plugin inside its own setup: the
    // child becomes a fiber under the parent's fiber.
    let parent = fn_plugin("parent", |ctx: Ctx| async move {
        ctx.provide(Arc::new(1u32)).await;

        ctx.register(
            fn_plugin("child", |ctx: Ctx| async move {
                assert_eq!(*ctx.get::<u32>().expect("inherited"), 1);

                Ok(())
            })
            .inject(vec![ServiceKey::of::<u32>()]),
        );

        Ok(())
    });

    let parent_fiber = root.register(parent);
    parent_fiber.wait_ready().await.unwrap();

    // The tree is observable from the outside.
    assert!(
        root.fiber().unwrap().parent().is_none(),
        "the root has no parent"
    );
    assert_eq!(
        parent_fiber.parent().map(|fiber| fiber.id()),
        Some(FiberId::root()),
        "top-level plugins hang off the root"
    );

    let children = parent_fiber.children();
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].name(), "child");
    assert_eq!(
        children[0].parent().map(|fiber| fiber.id()),
        Some(parent_fiber.id())
    );

    children[0].wait_ready().await.unwrap();

    parent_fiber.dispose().await;

    assert_eq!(
        children[0].state(),
        State::Disposed,
        "the child cascades with its parent"
    );
}

#[tokio::test]
async fn a_deeply_nested_tree_disposes_bottom_up() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    let order: Shared<Vec<&'static str>> = shared(Vec::new());

    let plugin = {
        let order = order.clone();

        fn_plugin("grandparent", move |ctx: Ctx| {
            let order = order.clone();

            async move {
                ctx.effect({
                    let order = order.clone();

                    async move {
                        order.lock().unwrap().push("grandparent");
                    }
                });

                let middle = ctx.fork("middle");

                middle.ctx().effect({
                    let order = order.clone();

                    async move {
                        order.lock().unwrap().push("middle");
                    }
                });

                let leaf = middle.ctx().fork("leaf");

                leaf.ctx().effect({
                    let order = order.clone();

                    async move {
                        order.lock().unwrap().push("leaf");
                    }
                });

                Ok(())
            }
        })
    };

    let fiber = root.register(plugin);
    fiber.wait_ready().await.unwrap();
    fiber.dispose().await;

    assert_eq!(
        *order.lock().unwrap(),
        vec!["leaf", "middle", "grandparent"],
        "descendants must clean up before their ancestors"
    );
}

#[tokio::test]
async fn plain_tasks_are_aborted_on_disposal_not_awaited() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    let finished = Arc::new(AtomicBool::new(false));

    let plugin = {
        let finished = finished.clone();

        fn_plugin("spawner", move |ctx: Ctx| {
            let finished = finished.clone();

            async move {
                ctx.spawn(async move {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    finished.store(true, Ordering::SeqCst);
                })
                .expect("fiber is active during setup");

                Ok(())
            }
        })
    };

    let fiber = root.register(plugin);
    fiber.wait_ready().await.unwrap();
    assert_eq!(fiber.task_count(), 1);

    let started = Instant::now();
    fiber.dispose().await;
    let elapsed = started.elapsed();

    assert_eq!(fiber.task_count(), 0);
    assert!(
        !finished.load(Ordering::SeqCst),
        "plain tasks are cut off, not awaited"
    );
    assert!(
        elapsed < Duration::from_millis(150),
        "disposal must not wait for plain tasks, took {elapsed:?}"
    );
}

#[tokio::test]
async fn graceful_tasks_are_signalled_then_joined_on_disposal() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    let finished = Arc::new(AtomicBool::new(false));

    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);

    let plugin = {
        let finished = finished.clone();

        fn_plugin("server", move |ctx: Ctx| {
            let finished = finished.clone();
            let mut stop_rx = stop_rx.clone();
            let stop_tx = stop_tx.clone();

            async move {
                ctx.spawn_graceful(
                    async move {
                        // A server-like loop: runs until told to stop, then
                        // drains for a moment before finishing.
                        let _ = stop_rx.wait_for(|stop| *stop).await;

                        tokio::time::sleep(Duration::from_millis(30)).await;
                        finished.store(true, Ordering::SeqCst);
                    },
                    async move {
                        // The termination operation: signal the loop.
                        stop_tx.send_replace(true);
                    },
                )
                .expect("fiber is active during setup");

                Ok(())
            }
        })
    };

    let fiber = root.register(plugin);
    fiber.wait_ready().await.unwrap();
    assert_eq!(fiber.task_count(), 1);

    let started = Instant::now();
    fiber.dispose().await;
    let elapsed = started.elapsed();

    assert!(
        finished.load(Ordering::SeqCst),
        "termination must run before the join"
    );
    assert_eq!(fiber.task_count(), 0);
    assert!(
        elapsed >= Duration::from_millis(25),
        "disposal must wait for the graceful task to drain, took {elapsed:?}"
    );
}

#[tokio::test]
async fn graceful_tasks_that_ignore_the_signal_can_be_force_aborted() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    let finished = Arc::new(AtomicBool::new(false));

    let plugin = {
        let finished = finished.clone();

        fn_plugin("stubborn", move |ctx: Ctx| {
            let finished = finished.clone();

            async move {
                ctx.spawn_graceful(
                    async move {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        finished.store(true, Ordering::SeqCst);
                    },
                    async move {
                        // A terminator that never signals anything.
                    },
                )
                .expect("fiber is active during setup");

                Ok(())
            }
        })
    };

    let fiber = root.register(plugin);
    fiber.wait_ready().await.unwrap();

    fiber.abort_tasks();
    tokio::time::sleep(Duration::from_millis(20)).await;

    assert_eq!(fiber.task_count(), 0);
    assert!(!finished.load(Ordering::SeqCst));

    let started = Instant::now();
    fiber.dispose().await;

    assert!(
        started.elapsed() < Duration::from_millis(150),
        "dispose must not hang"
    );
}

#[tokio::test]
async fn run_until_returns_when_the_kernel_goes_idle() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();

    let plugin = fn_plugin("batch", |ctx: Ctx| async move {
        ctx.spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
        })
        .expect("fiber is active during setup");

        Ok(())
    });

    root.register(plugin);
    let started = Instant::now();

    // No shutdown signal is coming: the kernel must exit on idleness alone.
    kernel.run_until(std::future::pending::<()>()).await;
    let elapsed = started.elapsed();

    assert!(
        elapsed < Duration::from_millis(500),
        "idle kernels exit on their own, took {elapsed:?}"
    );
    assert!(root.fiber().is_none(), "run_until must dispose everything");
}

#[tokio::test]
async fn run_until_honors_the_shutdown_signal_and_stops_endless_tasks() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    let finished = Arc::new(AtomicBool::new(false));

    let plugin = {
        let finished = finished.clone();

        fn_plugin("daemon", move |ctx: Ctx| {
            let finished = finished.clone();

            async move {
                ctx.spawn(async move {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    finished.store(true, Ordering::SeqCst);
                })
                .expect("fiber is active during setup");

                Ok(())
            }
        })
    };

    root.register(plugin);
    let started = Instant::now();

    // The endless task keeps the kernel busy; only the signal ends the run.
    kernel
        .run_until(tokio::time::sleep(Duration::from_millis(20)))
        .await;
    let elapsed = started.elapsed();

    assert!(
        !finished.load(Ordering::SeqCst),
        "the endless task must be cut off"
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "shutdown must abort the endless task instead of waiting, took {elapsed:?}"
    );
}

#[tokio::test]
async fn tasks_can_be_joined_individually_inside_a_plugin() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    let total = Arc::new(AtomicUsize::new(0));
    let verified = Arc::new(AtomicBool::new(false));

    let plugin = {
        let total = total.clone();
        let verified = verified.clone();

        fn_plugin("fanout", move |ctx: Ctx| {
            let total = total.clone();
            let verified = verified.clone();

            async move {
                let joins: Vec<_> = (0..3)
                    .map(|_| {
                        let total = total.clone();

                        ctx.spawn(async move {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                            total.fetch_add(1, Ordering::SeqCst);
                        })
                        .expect("fiber is active during setup")
                    })
                    .collect();

                for join in joins {
                    join.await.expect("task should complete");
                }

                verified.store(total.load(Ordering::SeqCst) == 3, Ordering::SeqCst);

                Ok(())
            }
        })
    };

    let fiber = root.register(plugin);
    fiber.wait_ready().await.unwrap();

    assert!(verified.load(Ordering::SeqCst));
    assert_eq!(fiber.task_count(), 0, "joined tasks leave the set");
}

#[tokio::test]
async fn emit_returns_immediately_and_observers_run_detached() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    let (seen_tx, mut seen_rx) = tokio::sync::mpsc::channel::<&'static str>(4);

    root.on(move |event: Message| {
        let seen_tx = seen_tx.clone();

        async move {
            seen_tx.send(event.0).await.unwrap();
        }
    });

    root.emit(&Message("detached"));

    assert!(
        seen_rx.try_recv().is_err(),
        "emit must not wait for its observers"
    );

    let seen = seen_rx.recv().await.expect("the detached observer ran");

    assert_eq!(seen, "detached");
}

#[tokio::test]
async fn parallel_awaits_every_observer_and_aggregates_panics() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    let survived: Shared<Vec<&'static str>> = shared(Vec::new());

    {
        let survived = survived.clone();

        root.on(move |event: Message| {
            let survived = survived.clone();

            async move {
                survived.lock().unwrap().push(event.0);
            }
        });
    }

    root.on(|_event: Message| async {
        panic!("observer exploded");
    });

    let outcome = root.events().parallel(&Message("boom")).await;

    assert!(outcome.is_err(), "the panic must be reported");
    assert_eq!(
        *survived.lock().unwrap(),
        vec!["boom"],
        "a panicking peer must not stop the other observers"
    );
}

#[tokio::test]
async fn serial_stops_at_the_first_decision() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    let consulted: Shared<Vec<&'static str>> = shared(Vec::new());

    {
        let consulted = consulted.clone();

        root.on_serial(move |_event: Message| {
            let consulted = consulted.clone();

            async move {
                consulted.lock().unwrap().push("first");
                None::<&'static str>
            }
        });
    }

    {
        let consulted = consulted.clone();

        root.on_serial(move |_event: Message| {
            let consulted = consulted.clone();

            async move {
                consulted.lock().unwrap().push("second");
                Some("decided")
            }
        });
    }

    {
        let consulted = consulted.clone();

        root.on_serial(move |_event: Message| {
            let consulted = consulted.clone();

            async move {
                consulted.lock().unwrap().push("third");
                Some("unreached")
            }
        });
    }

    let decision = root
        .events()
        .serial::<Message, &'static str>(&Message("go"))
        .await;

    assert_eq!(decision, Some("decided"));
    assert_eq!(
        *consulted.lock().unwrap(),
        vec!["first", "second"],
        "dispatch stops at the first decision"
    );
}

#[tokio::test]
async fn bail_decides_without_awaiting() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();

    root.on_bail(|event: &Message| (event.0 == "blocked").then_some("denied"));

    let blocked = root.events().bail::<Message, &str>(&Message("blocked"));
    let allowed = root.events().bail::<Message, &str>(&Message("allowed"));

    assert_eq!(blocked, Some("denied"));
    assert_eq!(allowed, None);
}

#[tokio::test]
async fn deciders_die_with_their_fiber() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();

    let guard = fn_plugin("guard", |ctx: Ctx| async move {
        ctx.on_bail(|_event: &Message| Some("guarded"));

        Ok(())
    });

    let fiber = root.register(guard);
    fiber.wait_ready().await.unwrap();

    assert_eq!(
        root.events().bail::<Message, &str>(&Message("x")),
        Some("guarded")
    );

    fiber.dispose().await;

    assert_eq!(
        root.events().bail::<Message, &str>(&Message("x")),
        None,
        "deciders must be removed with their fiber"
    );
}

#[tokio::test]
async fn waterfall_composes_around_the_builtin_and_can_veto() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();

    root.on_waterfall(
        |_event: Message, next: EventNext<Message, String>| async move {
            let inner = next.run(_event).await;
            format!("outer({inner})")
        },
    );

    root.on_waterfall(
        |_event: Message, next: EventNext<Message, String>| async move {
            let inner = next.run(_event).await;
            format!("inner({inner})")
        },
    );

    let composed = root
        .events()
        .waterfall(&Message("go"), |_event: Message| async {
            "core".to_owned()
        })
        .await;

    assert_eq!(
        composed, "outer(inner(core))",
        "layers run outermost-first around the built-in behavior"
    );

    // A fresh kernel, where the vetoing layer is the outermost one: skipping
    // `next` must cut off the whole chain, built-in behavior included.
    let kernel = Kernel::new();
    let root = kernel.root_ctx();

    root.on_waterfall(
        |_event: Message, _next: EventNext<Message, String>| async move { "vetoed".to_owned() },
    );

    let vetoed = root
        .events()
        .waterfall(&Message("go"), |_event: Message| async {
            "core".to_owned()
        })
        .await;

    assert_eq!(
        vetoed, "vetoed",
        "an outermost layer that skips next vetoes everything inside it"
    );
}

#[tokio::test]
async fn deciders_bubble_up_to_ancestor_realms() {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();
    let child = root.derive();

    root.on_bail(|event: &Message| (event.0 == "escalate").then_some("handled-upstairs"));

    let decision = child.events().bail::<Message, &str>(&Message("escalate"));

    assert_eq!(
        decision,
        Some("handled-upstairs"),
        "child dispatches must consult ancestor deciders"
    );
}

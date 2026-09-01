# nodus

A Cordis-inspired, **async-native plugin kernel** for Rust — the kernel of
[h](https://github.com/mon3stera/h), a coding agent, where every subsystem
(MCP servers, tool providers, memory, the main agent loop, the TUI frontend)
runs as a plugin.

Cordis-style kernels are usually built for synchronous, event-loop hosts.
Nodus keeps the model — fibers, realms, declarative injection, scoped
events — but rebuilds it for Tokio: plugins `await` through their setup,
services resolve asynchronously, and disposal is an async drain, not a drop
glue afterthought.

## Core concepts

| Concept | What it is |
|---|---|
| [`Kernel`] | Owns the fiber tree, the realm tree, and the registries for services, event handlers, and pipeline middlewares. |
| [`FiberHandle`] | A handle to one plugin's lifecycle: `Pending → Starting → Ready → Failed → Disposed`. Query state, await readiness, dispose — cascading to children. |
| [`Plugin`] | `name()` + `inject()` (declarative dependencies) + `async apply(ctx)`. Or just a closure via [`fn_plugin`]. |
| [`RealmId`] | A node in the realm tree. Services resolve **downward only**: a realm sees its own and its ancestors' services, never its children's. |
| [`ServiceKey`] | A `TypeId`-keyed typed service. Provided with [`Ctx::provide`], injected by declaring it in `inject()`. |
| [`Events`] | Five dispatch modes (below) over fiber-scoped handlers that bubble through ancestor realms. |
| [`Pipeline`] | A typed onion middleware chain around an owner-defined extension point — the waterfall for interception points that can rewrite or veto. |
| [`Ctx`] | A plugin's view of the kernel: register plugins, provide/get services, fork realms, spawn tracked tasks, register effects, dispatch events. |

## Quickstart

```rust
use std::sync::Arc;

use nodus::{Kernel, ServiceKey, State, fn_plugin};

struct Counter(u32);

#[tokio::main(flavor = "current_thread")]
async fn main() -> nodus::anyhow::Result<()> {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();

    // A plugin that declares what it needs; the kernel starts it when the
    // dependency appears.
    let greeter = fn_plugin("greeter", |ctx: nodus::Ctx| async move {
        let counter = ctx.get::<Counter>().expect("counter injected");
        println!("counter = {}", counter.0);

        Ok(())
    })
    .inject(vec![ServiceKey::of::<Counter>()]);

    let fiber = root.register(greeter);
    assert_eq!(fiber.state(), State::Pending);

    // Providing the dependency flips the fiber to ready, on its own.
    root.provide(Arc::new(Counter(7))).await;
    fiber.wait_ready().await?;

    fiber.dispose().await;
    kernel.dispose().await;

    Ok(())
}
```

## Lifecycle that cleans up after itself

- **Reactive start.** A plugin whose injections are missing stays
  `Pending` and starts the moment they are provided — no manual wiring of
  startup order.
- **A tree, not a list.** Plugins registered inside another plugin's `apply`
  become its child fibers; disposing a parent cascades to every child.
- **LIFO effects.** [`Ctx::effect`] registers async cleanup; on disposal
  they run in last-in-first-out order, along with service revocation and
  dependent disconnection (dependents of a removed service are torn down
  too, not left dangling).
- **Tracked tasks.** [`Ctx::spawn`] tasks are aborted at disposal;
  [`Ctx::spawn_graceful`] tasks get a termination signal and are awaited.
- **Panic containment.** A panicking `apply` fails its own fiber instead of
  the kernel; a failing fiber is reported through `wait_ready`.

Hosts that just want "run until told to stop" get
[`Kernel::run_until`]: drive the kernel until a shutdown signal (or
idleness), then dispose everything before returning.

## Events: five dispatch modes

Handlers are registered on a fiber (`ctx.on`, `ctx.on_bail`, `ctx.on_serial`,
`ctx.on_waterfall`), scoped to its realm, removed with the fiber, and reached
by emissions from descendant realms (bubbling, innermost realm first).

| Dispatch | Handlers | Waits | Short-circuits |
|---|---|---|---|
| `emit` | async observers | never (detached) | never |
| `parallel` | async observers | all, concurrently | never — panics aggregate into `EventAggregate` |
| `serial` | async deciders `→ Option<R>` | one by one | first decision |
| `bail` | **sync** deciders `→ Option<R>` | never | first decision |
| `waterfall` | onion layers around a built-in | composed | a layer skipping `next` vetoes everything inside it |

Use `emit`/`parallel` for observation, `serial`/`bail` for decisions,
`waterfall` for composition.

## Pipelines: typed interception

Where events notify, pipelines intercept. An extension point is a marker
type declaring its `Input`, `Output`, and name; middlewares wrap the chain:

```rust
use nodus::Pipeline;

struct ToolGate;

impl Pipeline for ToolGate {
    type Input = ToolCall;
    type Output = ToolCallResult;
    const NAME: &'static str = "h/agent/tool-gate";
}

// A plugin's apply:
// ctx.middleware::<ToolGate>(|call, next| async move {
//     if forbidden(&call) {
//         return ToolCallResult::denied(&call); // veto: next never runs
//     }
//     next.run(call).await; // continue into the rest of the chain
// });
```

The built-in behavior sits at the end of the chain; a middleware that never
calls `next` vetoes it. `middleware_before` prepends a layer instead of
appending.

## Compile-time plugin discovery

Plugin crates submit themselves at compile time; a host binary picks up
everything it links:

```rust
// in the plugin crate
nodus::register_plugin! {
    name: "demo",
    build: || DemoPlugin,
}

// in the host
let kernel = Kernel::with_discovered_plugins();
```

Registrations are sorted by name for deterministic startup.

## Status

In production use as h's kernel. Deliberately not built yet: a runtime
plugin loader (EntryTree, config-driven mounts, HMR), supervision/restart
policies for failed fibers, and per-scope event isolation. Known sharp
edges: `wait_ready` blocks indefinitely on a `Pending` fiber (no timeout
variant yet), and a failed fiber stays dead — nothing auto-restarts it.

## Testing

`cargo test` runs 47 tests, including a two-plugin workspace
(`tests/crates/plugin-alpha`, `plugin-beta`) that exercises compile-time
discovery end to end.

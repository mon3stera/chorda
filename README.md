# chorda

A Cordis-inspired, **async-native plugin kernel** for Rust — the kernel of
[h](https://github.com/mon3stera/h), a coding agent, where every subsystem
(MCP servers, tool providers, memory, the main agent loop, the TUI frontend)
runs as a plugin.

Cordis-style kernels are usually built for synchronous, event-loop hosts.
Chorda keeps the model — fibers, realms, declarative injection, scoped
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
| [`Loader`] | Config-driven mounting: a tree of entries (stable id + plugin kind + config) reconciled against the running kernel — untouched entries survive a reload. |

## Quickstart

```rust
use std::sync::Arc;

use chorda::{Kernel, ServiceKey, State, fn_plugin};

struct Counter(u32);

#[tokio::main(flavor = "current_thread")]
async fn main() -> chorda::anyhow::Result<()> {
    let kernel = Kernel::new();
    let root = kernel.root_ctx();

    // A plugin that declares what it needs; the kernel starts it when the
    // dependency appears.
    let greeter = fn_plugin("greeter", |ctx: chorda::Ctx| async move {
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
- **Hard and soft dependencies.** `inject()` returns `Dependency` values:
  `Dependency::Hard(key)` is the graph edge — the plugin waits for the
  service and is disconnected when its provider is replaced or disposed;
  `Dependency::Soft(key)` waits only for providers that declared the
  service through `Plugin::provides()`, then starts with whatever the
  table holds (`None` when nobody provided) and is never disconnected.
  Within a `Ctx::register_batch` pass, every plugin's declaration is
  visible to every other before any of them starts, so soft dependencies
  are order-independent.
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

## Introspection

[`Kernel::describe`](https://docs.rs/chorda) renders what a `chorda doctor`
would print — everything the kernel knows, in one snapshot:

```text
fiber "root" [Ready]
  fiber "tool" [Ready]
    provides: h_tools::ToolSink
  fiber "waiting" [Pending]
pending:
  "waiting" waits for [h_tools::Config]
events:
  bail<h_core::ToolCall, result bool> × 2
pipelines:
  h/agent/tool-gate × 3 middleware(s)
```

The fiber tree carries each fiber's state, injected and provided services,
and tracked task counts; the event families include their dispatch modes
and result types; pipelines show their middleware counts. Type and pipeline
names are recorded at registration time, so a bare `TypeId` never shows up
unlabeled.

## Events: five dispatch modes

Every event implements the `Event` trait, which binds it to its wire name and
— the load-bearing part — to its **decision type**:

```rust,ignore
pub trait Event: Clone + Send + Sync + 'static {
    type Output: Send + 'static;

    const NAME: &'static str;
}
```

Handlers are registered on a fiber (`ctx.on`, `ctx.on_bail`, `ctx.on_serial`,
`ctx.on_waterfall`), scoped to its realm, removed with the fiber, and reached
by emissions from descendant realms (bubbling, innermost realm first). The
decision-returning dispatches read `E::Output`, so a handler and its dispatch
cannot disagree on the decision type — the failure mode of a free `R`
parameter (register `<E, String>`, dispatch `<E, u32>`, silence) cannot
compile.

| Dispatch | Handlers | Waits | Short-circuits |
|---|---|---|---|
| `emit` | async observers | never (detached) | never |
| `parallel` | async observers | all, concurrently | never — panics aggregate into `EventAggregate` |
| `serial` | async deciders `→ Option<E::Output>` | one by one | first decision |
| `bail` | **sync** deciders `→ Option<E::Output>` | never | first decision |
| `waterfall` | onion layers around a built-in | composed | a layer skipping `next` vetoes everything inside it |

The `events!` macro declares event newtypes one line at a time — payload
newtype, `Event` impl, and compile-time catalog registration:

```rust,ignore
chorda::events! {
    /// Fired for every finished turn.
    pub TurnFinished: TurnReport => () = "h/agent/turn-finished";

    /// A permission decision for a tool call.
    pub ToolPermission: ToolCall => bool = "h/agent/tool-permission";
}
```

Hosts enumerate the events their plugin set carries with
`chorda::event_registrations()` — wire name, payload, and decision types.

Use `emit`/`parallel` for observation, `serial`/`bail` for decisions,
`waterfall` for composition.

## Pipelines: typed interception

Where events notify, pipelines intercept. An extension point is a marker
type declaring its `Input`, `Output`, and name — and because a plugin's
extension points are worth listing as a catalog, the `pipelines!` macro
declares them one line at a time and registers each into a compile-time
inventory:

```rust,ignore
chorda::pipelines! {
    /// Wrapped around every provider stream open.
    pub AgentPreRequest: Vec<Message> => anyhow::Result<Option<ProviderEventStream>>
        = "h/agent/pre-request";

    /// Wrapped around every tool call.
    pub AgentToolGate: ToolCall => ToolCallResult
        = "h/agent/tool-gate";
}

// A plugin's apply:
// ctx.middleware::<AgentToolGate>(|call, next| async move {
//     if forbidden(&call) {
//         return ToolCallResult::denied(&call); // veto: next never runs
//     }
//     next.run(call).await; // continue into the rest of the chain
// });
```

A host binary that links several plugin crates can enumerate every
extension point they carry with `chorda::pipeline_registrations()` — wire
name, marker, input, and output types — without reading their source. The
manual `impl Pipeline` form works identically for cases the macro does not
cover. (The arrow is `=>`: `macro_rules!` forbids `->` directly after a
type fragment.)

The built-in behavior sits at the end of the chain; a middleware that never
calls `next` vetoes it. `middleware_before` prepends a layer instead of
appending.

## Extractors: the `#[plugin]` macro

Writing a dependency in `inject` and then fetching it again inside `apply`
is the same knowledge twice. The `#[chorda::plugin]` attribute collapses it:
service parameters after `ctx` are extracted from the context, `inject` is
derived from them, and hard/soft is expressed by the parameter type
itself — `Arc<T>` is hard, `Option<Arc<T>>` is soft:

```rust,ignore
#[chorda::plugin]
impl Plugin for McpPlugin {
    async fn apply(&self, ctx: Ctx, config: Arc<Config>, db: Option<Arc<Db>>) -> anyhow::Result<()> {
        // config is present by construction: inject was derived from it.

        // db is None when no Db service exists — the plugin still starts.
    }
}
```

A hand-written `inject` in the same block wins over the derived one. When
`apply` runs, the hard services are guaranteed to be in the table.

## Compile-time plugin discovery

Plugin crates submit themselves at compile time; a host binary picks up
everything it links:

```rust
// in the plugin crate
chorda::register_plugin! {
    name: "demo",
    build: || DemoPlugin,
}

// in the host
let kernel = Kernel::with_discovered_plugins();
```

Registrations are sorted by name for deterministic startup.

## The loader: config-driven mounting

Where `register_plugin!` handles compile-time discovery, the
[`Loader`](https://docs.rs/chorda) handles runtime composition. A plugin
*kind* is a constructor from a config value (`ConfiguredPlugin`, with a
JSON schema via `schemars`); an *entry* is one configured instance with a
stable id:

```rust,ignore
let loader = Loader::new(&kernel);
loader.register_entry_kind(entry_kind::<McpPlugin>("mcp"))?;

loader.mount(EntryTree::from_json_str(r#"[
    { "id": "mcp:fs", "plugin": "mcp",
      "config": { "command": "npx", "args": ["-y", "@modelcontextprotocol/server-filesystem", "."] } }
]"#)?).await?;

// Later — a new tree replaces the mounted one:
let report = loader.reconcile(new_tree).await?;
// report: created / updated / removed / restarted / kept
```

Reconciliation diffs by id: added entries register, removed entries
dispose (cascading through whatever they registered), changed entries
dispose and re-register — and **untouched entries keep running**. Because
startup order is irrelevant (reactive injection starts dependents whenever
their dependencies appear), a flat id-sorted diff is a correct reload.
Replacing one entry's config tears down its dependents — service
revocation is kernel semantics — so the pass re-registers those casualties
against the replacement services instead of letting them die silently.

Rows may declare a `parent` group: groups are pure containers that mount
no fiber, but their `disabled` flag cascades to every descendant — one
flag retires a whole subtree. Trees are also stackable:
`EntryTree::compose([base, user, overlay])` merges patch layers
row-by-row by id (later layers win), the shape DSH uses for
bundle defaults + user `cordis.patch.yml` + `--patch` overlays. And
`to_json_str` serializes the effective tree back out, the write-back
material for hosts that persist loader state.

Dynamic code loading is deliberately out of scope: entries reference
compiled-in kinds, so only instances are dynamic and the Rust ABI problem
never arises.

## Status

In production use as h's kernel. Deliberately not built yet for the
loader: config-file watching (HMR on save — the host watches and calls
`reconcile`), write-back targeting a specific patch layer, supervision
policies beyond the loader's collateral repair, and per-scope event
isolation. Known sharp edges: `wait_ready` blocks indefinitely on a
`Pending` fiber (no timeout variant yet), and a failed fiber stays dead —
nothing auto-restarts it.

## Testing

`cargo test` runs 80 tests, including a two-plugin workspace
(`tests/crates/plugin-alpha`, `plugin-beta`) that exercises compile-time
discovery end to end.

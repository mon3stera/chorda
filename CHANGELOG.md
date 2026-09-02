# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com), and versions follow
[Semantic Versioning](https://semver.org) — while the crates are 0.x,
breaking changes land as minor bumps.

## [0.2.0] — 2026-08-29

The first release prepared for publication; everything below is relative to
the unpublished 0.1.0 skeleton.

### Added

- **Dependency kinds.** `Plugin::inject()` now returns `Dependency` values —
  `Dependency::Hard(ServiceKey)` gates startup and couples lifecycles,
  `Dependency::Soft(ServiceKey)` waits only for providers that declared the
  service through the new `Plugin::provides()`, then starts with whatever
  the table holds. Soft dependencies are never disconnected.
- **`Ctx::register_batch`.** Queues a whole pass of plugins before starting
  any of them, so `provides` declarations are visible batch-wide and soft
  dependencies are order-independent.
- **The `#[chorda::plugin]` attribute macro** (new `chorda-macros` crate).
  Service parameters after `ctx` — `Arc<T>` hard, `Option<Arc<T>>` soft —
  are extracted from the context, and `inject` is derived from them, via
  the new `FromService` trait. A hand-written `inject` wins.
- **The `pipelines!` macro.** Declares pipeline extension points one line
  at a time — marker type, `Pipeline` impl, and a compile-time catalog
  registration (`pipeline_registrations()`, `discover_pipeline_names()`).
- **The `events!` macro and the `Event` trait.** `Event::Output` binds the
  decision type of `serial`/`bail`/`waterfall` dispatches to the event, so
  a handler and its dispatch cannot disagree on it;
  `event_registrations()` / `discover_event_names()` catalog the events a
  binary carries.
- **Introspection.** `Kernel::describe()` renders the fiber tree, pending
  plugins with their dependency kinds, provided services, and the event
  and pipeline families.
- **Entry loader.** `Loader` mounts config-driven entries of compiled-in
  plugin kinds (`ConfiguredPlugin`, `entry_kind`): stable entry ids,
  reconciliation with validate-before-disposal, group entries with disable
  cascades, `EntryTree::compose` patch layers, and collateral repair of
  dependents lost to service replacement. Mounts and reconcile passes
  register as batches.
- **Introspective quickstart material**: `Kernel::describe`, realm-scoped
  registries, and the two-plugin discovery workspace used by tests.

### Changed

- **Breaking.** `Plugin::inject()` returns `Vec<Dependency>` instead of
  `Vec<ServiceKey>`; `ServiceKey` converts with `.into()`.
- **Breaking.** `serial`, `bail`, and `waterfall` dispatches take a single
  type parameter and return `E::Output` instead of a free `R` — a handler
  and its dispatch can no longer disagree on the decision type.
- **Breaking.** `EventNext<E>` carries one parameter; the decision type
  comes from `Event::Output`.
- The crate is dual-licensed MIT OR Apache-2.0 and carries full package
  metadata for publication.

## [0.1.0] — unpublished

The initial skeleton: fibers, realms, scoped events, pipelines, and
compile-time plugin discovery. Never published to crates.io.

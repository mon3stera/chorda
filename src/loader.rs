//! Declarative mounting of plugin instances: the [`EntryTree`] and the
//! [`Loader`] that reconciles it against the running kernel.
//!
//! The model, in one paragraph: a plugin *kind* is a compiled-in constructor
//! from a config value to a [`Plugin`] (see [`ConfiguredPlugin`]); an *entry*
//! is one configured instance of a kind with a **stable id**; an
//! [`EntryTree`] is the serialized set of entries — the host's config file.
//! [`Loader::mount`] registers a fiber per enabled entry, and
//! [`Loader::reconcile`] diffs a new tree against the mounted one: entries
//! absent from the new tree are disposed, changed entries are disposed and
//! re-registered, and **untouched entries keep running**. Startup order is
//! irrelevant — reactive injection starts dependents whenever their
//! dependencies appear — so a flat, id-sorted diff is enough for a correct
//! reload. Rows may declare a `parent` group: groups are pure containers
//! that mount no fiber, but disabling (or enabling) one cascades to every
//! descendant, and [`EntryTree::compose`] stacks patch layers — a base
//! document plus user patches overriding rows by id. This is the
//! cordis-loader contract (stable entry ids, config updates,
//! enable/disable, non-disruptive reload) with the dynamic code loading
//! replaced by compiled-in kinds: the ABI problem never arises, because
//! only instances are dynamic, never code.
//!
//! # Example
//!
//! ```
//! use std::sync::Arc;
//!
//! use serde::Deserialize;
//!
//! use chorda::{ConfiguredPlugin, Ctx, Loader, Kernel, Plugin, entry_kind};
//!
//! struct Echo {
//!     prefix: String,
//! }
//!
//! #[derive(Deserialize, schemars::JsonSchema)]
//! struct EchoConfig {
//!     prefix: String,
//! }
//!
//! # #[chorda::async_trait]
//! impl Plugin for Echo {
//!     fn name(&self) -> &str {
//!         "echo"
//!     }
//!
//!     async fn apply(&self, ctx: Ctx) -> chorda::anyhow::Result<()> {
//!         ctx.provide(Arc::new(self.prefix.clone())).await;
//!
//!         Ok(())
//!     }
//! }
//!
//! impl ConfiguredPlugin for Echo {
//!     type Config = EchoConfig;
//!
//!     fn build(config: EchoConfig) -> Self {
//!         Self { prefix: config.prefix }
//!     }
//! }
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> chorda::anyhow::Result<()> {
//! let kernel = Kernel::new();
//! let loader = Loader::new(&kernel);
//!
//! loader.register_entry_kind(entry_kind::<Echo>("echo"))?;
//!
//! let tree = chorda::EntryTree::from_json_str(
//!     r#"[{ "id": "greeter", "plugin": "echo",
//!          "config": { "prefix": "hello" } }]"#,
//! )?;
//!
//! loader.mount(tree).await?;
//! loader.fiber_of("greeter").unwrap().wait_ready().await?;
//! assert_eq!(*kernel.root_ctx().get::<String>().unwrap(), "hello");
//!
//! kernel.dispose().await;
//! # Ok(())
//! # }
//! ```

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use anyhow::{Context as _, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    Kernel,
    fiber::{FiberHandle, State},
    plugin::Plugin,
};

/// A plugin that can be instantiated from one config value — the constructor
/// half of a loader entry kind.
///
/// The config type doubles as the kind's JSON schema (via [`schemars`]),
/// which the loader validates every entry's config against before mounting.
pub trait ConfiguredPlugin: Plugin + Sized + 'static {
    /// The entry's configuration, deserialized from the entry's `config`
    /// value; an absent config is presented as an empty object.
    type Config: serde::de::DeserializeOwned + schemars::JsonSchema + Send;

    /// Builds the plugin from validated configuration. Pure construction:
    /// the loader may call it again on re-registration.
    fn build(config: Self::Config) -> Self;
}

/// Validates one config value against a kind's config type.
type ConfigCheck = Arc<dyn Fn(&Value) -> anyhow::Result<()> + Send + Sync>;

/// Builds one plugin instance from a config value.
type ConfigBuild = Arc<dyn Fn(&Value) -> anyhow::Result<Arc<dyn Plugin>> + Send + Sync>;

/// One compiled-in plugin kind: a name plus a constructor from config.
///
/// Created with [`entry_kind`] and registered on a [`Loader`].
#[derive(Clone)]
pub struct EntryKind {
    name: String,
    schema: schemars::Schema,
    validate: ConfigCheck,
    build: ConfigBuild,
}

impl EntryKind {
    /// The kind's registered name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The JSON schema of this kind's config — useful for hosts that
    /// generate config documentation or editor completions.
    pub fn schema(&self) -> &schemars::Schema {
        &self.schema
    }
}

/// Creates an entry kind from a [`ConfiguredPlugin`] implementation.
pub fn entry_kind<K: ConfiguredPlugin>(name: impl Into<String>) -> EntryKind {
    let name = name.into();
    let kind_name = name.clone();
    let build_name = name.clone();

    EntryKind {
        schema: schemars::schema_for!(K::Config),
        validate: Arc::new(move |value| {
            serde_json::from_value::<K::Config>(value.clone())
                .with_context(|| format!("invalid config for kind \"{kind_name}\""))?;

            Ok(())
        }),
        build: Arc::new(move |value| {
            let config: K::Config = serde_json::from_value(value.clone())
                .with_context(|| format!("invalid config for kind \"{build_name}\""))?;

            Ok(Arc::new(K::build(config)) as Arc<dyn Plugin>)
        }),
        name,
    }
}

/// One serialized entry row: `id`, optional `plugin`, optional `parent`,
/// optional `config`, `disabled`.
///
/// This is the on-disk shape — an [`EntryTree`] is an array of these. A row
/// without a `plugin` is a **group**: a pure container that mounts no fiber
/// but cascades its `disabled` flag onto every descendant. Hierarchy is
/// expressed with `parent` references (flat unique ids, not path-encoded
/// strings); a parent must be a group declared anywhere in the same tree.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EntrySpec {
    /// Stable identity inside the tree; the unit of reconciliation.
    pub id: String,

    /// The plugin kind's registered name; absent for group rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin: Option<String>,

    /// The id of the group this entry belongs to, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,

    /// The instance's configuration; absent means an empty object. Groups
    /// must not carry a config.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<Value>,

    /// Disabled entries stay declared but mount no fiber; the flag cascades
    /// down the parent chain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disabled: Option<bool>,
}

impl EntrySpec {
    /// An enabled entry or group.
    pub fn enabled(&self) -> bool {
        !self.disabled.unwrap_or(false)
    }

    /// Whether this row is a group container.
    pub fn is_group(&self) -> bool {
        self.plugin.is_none()
    }
}

/// The declarative mounting tree: a set of uniquely identified entries.
///
/// Entries are kept sorted by id so mounts and reloads are deterministic.
/// Nesting (groups, parent-child ids) is deliberately not modeled yet.
#[derive(Debug, Clone, Default)]
pub struct EntryTree {
    entries: BTreeMap<String, EntrySpec>,
}

impl EntryTree {
    /// Builds a tree from a JSON array of entry rows, validating the
    /// structure: rows deserialize, ids are unique and non-empty, parents
    /// exist and are groups, parent chains are acyclic, groups carry no
    /// config. Kind and config validation happens on the [`Loader`], which
    /// owns the kinds.
    pub fn from_value(value: Value) -> anyhow::Result<Self> {
        let Value::Array(rows) = value else {
            bail!("an entry tree is an array of entry rows");
        };

        let mut entries = BTreeMap::new();

        for row in rows {
            let spec = parse_row(row)?;

            if entries.contains_key(&spec.id) {
                bail!("duplicate entry id \"{}\"", spec.id);
            }

            entries.insert(spec.id.clone(), spec);
        }

        let tree = Self { entries };

        tree.validate_structure()?;

        Ok(tree)
    }

    /// Builds a tree from a JSON document — an array of entry rows.
    pub fn from_json_str(json: &str) -> anyhow::Result<Self> {
        let value: Value = serde_json::from_str(json).context("invalid entry tree document")?;

        Self::from_value(value)
    }

    /// Composes a tree out of stacked layers, later layers overriding
    /// earlier ones row-by-row by id. This is the patch-model of the cordis
    /// loader: a base layer ships the defaults, a user layer overrides
    /// machine-local preferences (typically by setting `disabled`), and
    /// ephemeral overlays ride on top. Every layer is a full array of entry
    /// rows; there is no field-level merge, so a patch layer that wants an
    /// entry disabled repeats the whole row with `"disabled": true`.
    pub fn compose(layers: impl IntoIterator<Item = Value>) -> anyhow::Result<Self> {
        let mut merged: BTreeMap<String, EntrySpec> = BTreeMap::new();

        for layer in layers {
            let Value::Array(layer_rows) = layer else {
                bail!("an entry layer must be an array of entry rows");
            };

            for row in layer_rows {
                let spec = parse_row(row)?;

                // Later layers override earlier ones by id.
                merged.insert(spec.id.clone(), spec);
            }
        }

        let tree = Self { entries: merged };

        tree.validate_structure()?;

        Ok(tree)
    }

    /// Serializes the tree back to its document form — the write-back
    /// material for hosts that persist loader state. Rows come out in id
    /// order; omitted fields stay omitted.
    pub fn to_json_str(&self) -> anyhow::Result<String> {
        serde_json::to_string_pretty(&self.entries.values().collect::<Vec<_>>())
            .context("entry tree is not serializable")
    }

    /// The entries in id order.
    pub fn entries(&self) -> impl Iterator<Item = &EntrySpec> {
        self.entries.values()
    }

    /// Whether the entry is mounted under this tree: it exists, is an
    /// entry (not a group), and neither it nor any ancestor is disabled.
    pub fn effectively_enabled(&self, id: &str) -> bool {
        let Some(spec) = self.entries.get(id) else {
            return false;
        };

        !self.disabled_up_the_chain(spec)
    }

    fn disabled_up_the_chain(&self, spec: &EntrySpec) -> bool {
        let mut current = Some(spec);

        while let Some(row) = current {
            if !row.enabled() {
                return true;
            }

            current = row
                .parent
                .as_deref()
                .and_then(|parent| self.entries.get(parent));
        }

        false
    }

    /// Parents must exist, be groups, and form no cycles.
    fn validate_structure(&self) -> anyhow::Result<()> {
        for spec in self.entries.values() {
            let Some(parent) = &spec.parent else {
                continue;
            };

            let parent_row = self.entries.get(parent).with_context(|| {
                format!(
                    "entry \"{}\" references unknown parent \"{}\"",
                    spec.id, parent
                )
            })?;

            if !parent_row.is_group() {
                bail!(
                    "entry \"{}\" declares \"{}\" as parent, but only groups can parent",
                    spec.id,
                    parent
                );
            }

            let mut seen = 0_usize;
            let mut cursor = Some(spec);

            while let Some(row) = cursor {
                seen += 1;

                if seen > self.entries.len() {
                    bail!("entry \"{}\" sits on a parent cycle", spec.id);
                }

                cursor = row
                    .parent
                    .as_deref()
                    .and_then(|parent| self.entries.get(parent));
            }
        }

        Ok(())
    }

    fn get(&self, id: &str) -> Option<&EntrySpec> {
        self.entries.get(id)
    }
}

/// The registry of compiled-in plugin kinds a [`Loader`] mounts from.
#[derive(Default)]
pub struct EntryKinds {
    kinds: BTreeMap<String, EntryKind>,
}

impl EntryKinds {
    /// Registers a kind. A duplicate kind name is an error.
    pub fn register(&mut self, kind: EntryKind) -> anyhow::Result<()> {
        if self.kinds.contains_key(&kind.name) {
            bail!("entry kind \"{}\" is already registered", kind.name);
        }

        self.kinds.insert(kind.name.clone(), kind);

        Ok(())
    }

    pub(crate) fn get(&self, name: &str) -> Option<&EntryKind> {
        self.kinds.get(name)
    }

    /// The registered kind names, sorted.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.kinds.keys().map(String::as_str)
    }
}

/// What one [`Loader::reconcile`] pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LoaderReport {
    /// Entries whose fibers were created.
    pub created: Vec<String>,
    /// Entries disposed and re-registered because their options changed.
    pub updated: Vec<String>,
    /// Entries disposed because they disappeared from the new tree (or were
    /// disabled).
    pub removed: Vec<String>,
    /// Kept entries whose fiber was disposed as a *side effect* of the pass
    /// — a replaced dependency disconnects its dependents — and were
    /// therefore re-registered against the replacement services. Fibers
    /// that failed on their own (`State::Failed`) are never restarted.
    pub restarted: Vec<String>,
    /// Entries left untouched: same id and same options.
    pub kept: usize,
}

/// One mounted entry: its serialized options plus the live fiber.
#[derive(Clone)]
struct Mounted {
    options: EntrySpec,
    fiber: FiberHandle,
}

/// The runtime side of the loader: entry kinds, mounted fibers, and the
/// reconciliation that keeps the mounted set matching a tree.
///
/// Entries register through the kernel's root context, so every entry fiber
/// is a child of the root; disposing an entry cascades through whatever its
/// plugin registered.
pub struct Loader {
    kernel: Kernel,
    kinds: Mutex<EntryKinds>,
    mounted: Mutex<BTreeMap<String, Mounted>>,
}

impl Loader {
    /// Creates a loader over a kernel.
    pub fn new(kernel: &Kernel) -> Self {
        Self {
            kernel: kernel.clone(),
            kinds: Mutex::new(EntryKinds::default()),
            mounted: Mutex::new(BTreeMap::new()),
        }
    }

    /// Registers a plugin kind this loader can mount. Must happen before
    /// the first [`Loader::mount`] or [`Loader::reconcile`]; a duplicate
    /// kind name is an error.
    pub fn register_entry_kind(&self, kind: EntryKind) -> anyhow::Result<()> {
        self.kinds
            .lock()
            .expect("kinds lock poisoned")
            .register(kind)
    }

    /// The registered kind names, sorted.
    pub fn kind_names(&self) -> Vec<String> {
        self.kinds
            .lock()
            .expect("kinds lock poisoned")
            .names()
            .map(str::to_owned)
            .collect()
    }

    /// The ids of currently mounted entries, in id order.
    pub fn mounted_ids(&self) -> Vec<String> {
        self.mounted
            .lock()
            .expect("mounted lock poisoned")
            .keys()
            .cloned()
            .collect()
    }

    /// The fiber of one mounted entry.
    pub fn fiber_of(&self, id: &str) -> Option<FiberHandle> {
        self.mounted
            .lock()
            .expect("mounted lock poisoned")
            .get(id)
            .map(|mounted| mounted.fiber.clone())
    }

    /// Mounts every enabled entry of a fresh tree.
    ///
    /// All entries are validated against the kinds first — an invalid tree
    /// mounts nothing. Registration itself happens per entry, in id order.
    pub async fn mount(&self, tree: EntryTree) -> anyhow::Result<Vec<String>> {
        if !self
            .mounted
            .lock()
            .expect("mounted lock poisoned")
            .is_empty()
        {
            bail!("the loader already has entries mounted; use reconcile");
        }

        let specs = self.validate_against_kinds(&tree)?;

        // One batch per pass: every entry is queued before any starts, so a
        // soft dependency sees the whole pass's provides declarations
        // regardless of registration order.
        let plugins: Vec<(EntrySpec, Arc<dyn Plugin>)> = specs
            .into_iter()
            .map(|spec| {
                let plugin = self.build_entry(&spec)?;

                Ok((spec, plugin))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        let handles = self
            .kernel
            .root_ctx()
            .register_batch(plugins.iter().map(|(_, plugin)| plugin.clone()).collect());

        let mut mounted_ids = Vec::new();

        for ((spec, _), fiber) in plugins.iter().zip(handles) {
            self.mounted.lock().expect("mounted lock poisoned").insert(
                spec.id.clone(),
                Mounted {
                    options: spec.clone(),
                    fiber,
                },
            );

            mounted_ids.push(spec.id.clone());
        }

        Ok(mounted_ids)
    }

    /// Brings the mounted set in line with `next`: creates missing entries,
    /// disposes removed ones, and re-registers changed ones — untouched
    /// entries keep running. Disabled transitions count as removals and
    /// creations.
    ///
    /// Every changed entry is validated before anything is disposed, so an
    /// invalid tree is a no-op. Disposal completes (effects, cascades)
    /// before a replacement fiber registers; registration happens in id
    /// order.
    ///
    /// Replacing one entry's config tears down its dependents — that is the
    /// kernel's service-revocation semantics. The pass then re-registers
    /// such collateral casualties against the replacement services, so an
    /// untouched entry follows its dependency's new configuration instead
    /// of dying silently; see [`LoaderReport::restarted`].
    pub async fn reconcile(&self, next: EntryTree) -> anyhow::Result<LoaderReport> {
        let mut report = LoaderReport::default();

        let (kept, actions) = self.plan(&next)?;

        report.kept = kept;
        self.validate_actions(&actions)?;

        // Phase one: dispose everything the pass removes or replaces, so
        // revocation completes before any replacement registers.
        //
        // Phase two: register the creations as one batch, so the pass's
        // provides declarations are all visible before any of its entries
        // starts and soft dependencies are order-independent.
        let mut creations: Vec<(String, EntrySpec, bool)> = Vec::new();

        for (id, action) in &actions {
            match action {
                Action::Create(spec) => creations.push((id.clone(), spec.clone(), false)),
                Action::Replace(spec, fiber) => {
                    fiber.dispose().await;

                    creations.push((id.clone(), spec.clone(), true));
                }
                Action::Remove(fiber) => {
                    fiber.dispose().await;

                    self.mounted
                        .lock()
                        .expect("mounted lock poisoned")
                        .remove(id);

                    report.removed.push(id.clone());
                }
                Action::Keep => {}
            }
        }

        let plugins = creations
            .iter()
            .map(|(_, spec, _)| self.build_entry(spec))
            .collect::<anyhow::Result<Vec<_>>>()?;

        let handles = self.kernel.root_ctx().register_batch(plugins);

        for ((id, spec, replaced), fiber) in creations.into_iter().zip(handles) {
            self.mounted.lock().expect("mounted lock poisoned").insert(
                id.clone(),
                Mounted {
                    options: spec,
                    fiber,
                },
            );

            if replaced {
                report.updated.push(id);
            } else {
                report.created.push(id);
            }
        }

        self.repair_collateral(&mut report).await?;

        Ok(report)
    }

    /// Re-registers kept entries whose fiber was disposed as collateral of
    /// the pass (a replaced or removed dependency disconnects its
    /// dependents). Repeat until stable: re-registration can only wake
    /// pending fibers, so this terminates. `State::Failed` fibers — a
    /// plugin's own apply error — stay dead.
    async fn repair_collateral(&self, report: &mut LoaderReport) -> anyhow::Result<()> {
        loop {
            let dead: Vec<(String, EntrySpec)> = {
                let mounted = self.mounted.lock().expect("mounted lock poisoned");

                mounted
                    .iter()
                    .filter(|(_, mounted)| mounted.fiber.state() == State::Disposed)
                    .map(|(id, mounted)| (id.clone(), mounted.options.clone()))
                    .collect()
            };

            if dead.is_empty() {
                return Ok(());
            }

            let plugins = dead
                .iter()
                .map(|(_, spec)| self.build_entry(spec))
                .collect::<anyhow::Result<Vec<_>>>()?;

            let handles = self.kernel.root_ctx().register_batch(plugins);

            for ((id, spec), fiber) in dead.into_iter().zip(handles) {
                self.mounted.lock().expect("mounted lock poisoned").insert(
                    id.clone(),
                    Mounted {
                        options: spec,
                        fiber,
                    },
                );

                report.restarted.push(id);
            }
        }
    }

    /// Decides, under the lock, what each id needs: keep, create, replace,
    /// or remove. Fibers to dispose are cloned out; no lock is held across
    /// an await.
    fn plan(&self, next: &EntryTree) -> anyhow::Result<(usize, Vec<(String, Action)>)> {
        let mounted = self.mounted.lock().expect("mounted lock poisoned");
        let mut actions = Vec::new();
        let mut kept = 0_usize;

        let mut ids: Vec<&String> = next.entries.keys().collect();

        for id in mounted.keys() {
            if !next.entries.contains_key(id) {
                ids.push(id);
            }
        }

        ids.sort();

        for id in ids {
            let target = next.get(id);
            let current = mounted.get(id);

            // Mountable = an entry (not a group) enabled through its own
            // flag and every ancestor's.
            let target_mountable =
                target.is_some_and(|spec| !spec.is_group() && next.effectively_enabled(id));

            match (current, target) {
                (Some(mounted), Some(spec)) if target_mountable && &mounted.options == spec => {
                    kept += 1;
                    actions.push((id.clone(), Action::Keep));
                }
                // Any unmountable target unmounts what is there: the row
                // left the tree, is disabled (own flag or an ancestor's), or
                // turned into a group.
                (Some(mounted), Some(_)) if !target_mountable => {
                    actions.push((id.clone(), Action::Remove(mounted.fiber.clone())));
                }
                (Some(mounted), Some(spec)) => {
                    actions.push((
                        id.clone(),
                        Action::Replace(spec.clone(), mounted.fiber.clone()),
                    ));
                }
                (Some(mounted), None) => {
                    actions.push((id.clone(), Action::Remove(mounted.fiber.clone())));
                }
                (None, Some(_)) if !target_mountable => {
                    kept += 1;
                    actions.push((id.clone(), Action::Keep));
                }
                (None, Some(spec)) => {
                    actions.push((id.clone(), Action::Create(spec.clone())));
                }
                (None, None) => unreachable!("id came from one of the two trees"),
            }
        }

        Ok((kept, actions))
    }

    /// Validates every entry the plan would mount: kind must exist and the
    /// config must deserialize into the kind's config type. Runs before any
    /// disposal, so an invalid tree cannot tear down a running one.
    fn validate_actions(&self, actions: &[(String, Action)]) -> anyhow::Result<()> {
        let kinds = self.kinds.lock().expect("kinds lock poisoned");

        for (id, action) in actions {
            let (Action::Create(spec) | Action::Replace(spec, _)) = action else {
                continue;
            };

            validate_spec(&kinds, spec).with_context(|| format!("entry \"{id}\" is invalid"))?;
        }

        Ok(())
    }

    /// Validates a whole tree against the kinds; used by [`Loader::mount`].
    fn validate_against_kinds(&self, tree: &EntryTree) -> anyhow::Result<Vec<EntrySpec>> {
        let kinds = self.kinds.lock().expect("kinds lock poisoned");
        let mut specs = Vec::new();

        for spec in tree.entries() {
            if spec.is_group() || !tree.effectively_enabled(&spec.id) {
                continue;
            }

            validate_spec(&kinds, spec)
                .with_context(|| format!("entry \"{}\" is invalid", spec.id))?;

            specs.push(spec.clone());
        }

        Ok(specs)
    }

    fn build_entry(&self, spec: &EntrySpec) -> anyhow::Result<Arc<dyn Plugin>> {
        let plugin_name = spec
            .plugin
            .as_deref()
            .expect("only entries with a kind are registered");

        let kind = self
            .kinds
            .lock()
            .expect("kinds lock poisoned")
            .get(plugin_name)
            .with_context(|| {
                format!(
                    "entry \"{}\" uses unknown kind \"{}\"",
                    spec.id, plugin_name
                )
            })?
            .clone();

        let config = spec
            .config
            .clone()
            .unwrap_or_else(|| Value::Object(Default::default()));
        let plugin = (kind.build)(&config)
            .with_context(|| format!("entry \"{}\" failed to build", spec.id))?;

        Ok(plugin)
    }
}

fn validate_spec(kinds: &EntryKinds, spec: &EntrySpec) -> anyhow::Result<()> {
    let plugin_name = spec
        .plugin
        .as_deref()
        .expect("groups never reach kind validation");

    let kind = kinds
        .get(plugin_name)
        .with_context(|| format!("unknown kind \"{plugin_name}\""))?;

    let config = spec
        .config
        .clone()
        .unwrap_or_else(|| Value::Object(Default::default()));

    (kind.validate)(&config)
}

/// What the plan decided for one id.
enum Action {
    Keep,
    Create(EntrySpec),
    /// The old fiber to dispose, and the options to re-register with.
    Replace(EntrySpec, FiberHandle),
    /// The fiber to dispose.
    Remove(FiberHandle),
}

/// Parses and shape-checks one entry row: it must deserialize, carry a
/// non-empty id, and groups must not carry a config.
fn parse_row(row: Value) -> anyhow::Result<EntrySpec> {
    let spec: EntrySpec = serde_json::from_value(row).context("invalid entry row")?;

    if spec.id.is_empty() {
        bail!("entry ids must be non-empty");
    }

    if spec.is_group() && spec.config.is_some() {
        bail!("group \"{}\" must not carry a config", spec.id);
    }

    Ok(spec)
}

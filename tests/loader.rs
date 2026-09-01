//! EntryTree and Loader: mounting, reconciliation, and failure paths.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
};

use serde::Deserialize;

use nodus::{ConfiguredPlugin, Ctx, EntryTree, Kernel, Loader, Plugin, ServiceKey, entry_kind};

/// How many times each tag's apply ran. Global because the loader builds
/// instances itself; tags are unique per test so parallel tests stay apart.
fn counts() -> &'static Mutex<HashMap<String, usize>> {
    static COUNTS: OnceLock<Mutex<HashMap<String, usize>>> = OnceLock::new();

    COUNTS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn count(tag: &str) -> usize {
    *counts().lock().unwrap().get(tag).unwrap_or(&0)
}

/// A configured entry whose apply is observable through the global counter.
struct Tagged {
    tag: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct TaggedConfig {
    tag: String,
}

#[nodus::async_trait]
impl Plugin for Tagged {
    fn name(&self) -> &str {
        "tagged"
    }

    async fn apply(&self, ctx: Ctx) -> nodus::anyhow::Result<()> {
        *counts()
            .lock()
            .unwrap()
            .entry(self.tag.clone())
            .or_default() += 1;

        ctx.provide(Arc::new(TaggedService(self.tag.clone()))).await;

        Ok(())
    }
}

impl ConfiguredPlugin for Tagged {
    type Config = TaggedConfig;

    fn build(config: TaggedConfig) -> Self {
        Self { tag: config.tag }
    }
}

/// A provided service carrying the entry's tag, so tests can observe which
/// instance is live.
#[derive(Clone)]
struct TaggedService(String);

/// A dependent entry: injects the TaggedService and reports what it saw
/// through its own provided service, readable via the fiber's context lens.
struct Dependent;

#[derive(Deserialize, schemars::JsonSchema)]
struct DependentConfig {}

#[derive(Clone)]
struct DependentSaw(String);

#[nodus::async_trait]
impl Plugin for Dependent {
    fn name(&self) -> &str {
        "dependent"
    }

    fn inject(&self) -> Vec<ServiceKey> {
        vec![ServiceKey::of::<TaggedService>()]
    }

    async fn apply(&self, ctx: Ctx) -> nodus::anyhow::Result<()> {
        let service = ctx.get::<TaggedService>().expect("dependency injected");

        ctx.provide(Arc::new(DependentSaw(service.0.clone()))).await;

        Ok(())
    }
}

impl ConfiguredPlugin for Dependent {
    type Config = DependentConfig;

    fn build(_config: DependentConfig) -> Self {
        Self
    }
}

fn loader_with_kinds() -> (Kernel, Loader) {
    let kernel = Kernel::new();
    let loader = Loader::new(&kernel);

    loader
        .register_entry_kind(entry_kind::<Tagged>("tagged"))
        .unwrap();
    loader
        .register_entry_kind(entry_kind::<Dependent>("dependent"))
        .unwrap();

    (kernel, loader)
}

fn tree(json: &str) -> EntryTree {
    EntryTree::from_json_str(json).unwrap()
}

/// Apply runs on the fiber's setup task; wait for every mounted fiber to be
/// ready before asserting on its effects.
async fn wait_ready_all(loader: &Loader) {
    for id in loader.mounted_ids() {
        loader.fiber_of(&id).unwrap().wait_ready().await.unwrap();
    }
}

#[tokio::test]
async fn mount_registers_one_fiber_per_enabled_entry() {
    let (kernel, loader) = loader_with_kinds();

    let mounted = loader
        .mount(tree(
            r#"[
        { "id": "a", "plugin": "tagged", "config": { "tag": "mount/a" } },
        { "id": "off", "plugin": "tagged", "config": { "tag": "mount/off" }, "disabled": true }
    ]"#,
        ))
        .await
        .unwrap();

    assert_eq!(mounted, vec!["a".to_owned()]);

    wait_ready_all(&loader).await;

    assert_eq!(count("mount/a"), 1);
    assert_eq!(count("mount/off"), 0, "disabled entries mount no fiber");

    let fiber = loader.fiber_of("a").unwrap();
    assert_eq!(fiber.ctx().get::<TaggedService>().unwrap().0, "mount/a");

    kernel.dispose().await;
}

#[tokio::test]
async fn reconcile_touches_only_changed_entries() {
    let (kernel, loader) = loader_with_kinds();

    loader
        .mount(tree(
            r#"[
        { "id": "a", "plugin": "tagged", "config": { "tag": "recon/a" } },
        { "id": "b", "plugin": "tagged", "config": { "tag": "recon/b" } }
    ]"#,
        ))
        .await
        .unwrap();
    wait_ready_all(&loader).await;

    // Same tree: everything kept, nothing rebuilt.
    let report = loader
        .reconcile(tree(
            r#"[
        { "id": "a", "plugin": "tagged", "config": { "tag": "recon/a" } },
        { "id": "b", "plugin": "tagged", "config": { "tag": "recon/b" } }
    ]"#,
        ))
        .await
        .unwrap();

    assert_eq!(
        (
            report.created.len(),
            report.updated.len(),
            report.removed.len()
        ),
        (0, 0, 0)
    );
    assert_eq!(report.kept, 2);
    assert_eq!(count("recon/a"), 1);
    assert_eq!(count("recon/b"), 1);

    // Change a's config, drop b, add c: only a and c rebuild; b disposes.
    let report = loader
        .reconcile(tree(
            r#"[
        { "id": "a", "plugin": "tagged", "config": { "tag": "recon/a2" } },
        { "id": "c", "plugin": "tagged", "config": { "tag": "recon/c" } }
    ]"#,
        ))
        .await
        .unwrap();

    assert_eq!(report.updated, vec!["a".to_owned()]);
    assert_eq!(report.created, vec!["c".to_owned()]);
    assert_eq!(report.removed, vec!["b".to_owned()]);
    assert_eq!(report.kept, 0);
    wait_ready_all(&loader).await;
    assert_eq!(
        count("recon/a2"),
        1,
        "the new config built a fresh instance"
    );
    assert_eq!(
        count("recon/b"),
        1,
        "the removed entry ran its apply exactly once"
    );
    assert!(loader.fiber_of("b").is_none());
    assert_eq!(loader.mounted_ids(), vec!["a".to_owned(), "c".to_owned()]);

    kernel.dispose().await;
}

#[tokio::test]
async fn disabling_an_entry_removes_it_and_enabling_restores_it() {
    let (kernel, loader) = loader_with_kinds();

    loader
        .mount(tree(
            r#"[{ "id": "a", "plugin": "tagged", "config": { "tag": "dis/a" } }]"#,
        ))
        .await
        .unwrap();
    wait_ready_all(&loader).await;

    loader
        .reconcile(tree(
            r#"[{ "id": "a", "plugin": "tagged", "config": { "tag": "dis/a" }, "disabled": true }]"#,
        ))
        .await
        .unwrap();

    assert!(loader.fiber_of("a").is_none());
    assert_eq!(count("dis/a"), 1, "disposal, not re-apply");

    loader
        .reconcile(tree(
            r#"[{ "id": "a", "plugin": "tagged", "config": { "tag": "dis/a" } }]"#,
        ))
        .await
        .unwrap();

    wait_ready_all(&loader).await;
    assert_eq!(count("dis/a"), 2, "re-enabling rebuilds the instance");

    kernel.dispose().await;
}

#[tokio::test]
async fn dependents_start_reactively_and_follow_the_dependency() {
    let (kernel, loader) = loader_with_kinds();

    // The dependent is declared first; its dependency comes later in the file.
    loader
        .mount(tree(
            r#"[
        { "id": "watcher", "plugin": "dependent", "config": {} },
        { "id": "b", "plugin": "tagged", "config": { "tag": "react/b" } }
    ]"#,
        ))
        .await
        .unwrap();

    let watcher = loader.fiber_of("watcher").unwrap();
    watcher.wait_ready().await.unwrap();
    assert_eq!(
        watcher.ctx().get::<DependentSaw>().unwrap().0,
        "react/b",
        "reactive injection started the dependent after its dependency"
    );

    // Swapping the dependency's config revokes the old service, which
    // disconnects the dependent; it must re-register against the new one.
    loader
        .reconcile(tree(
            r#"[
        { "id": "watcher", "plugin": "dependent", "config": {} },
        { "id": "b", "plugin": "tagged", "config": { "tag": "react/b2" } }
    ]"#,
        ))
        .await
        .unwrap();

    // The pass replaced the watcher's fiber as collateral of the dependency
    // swap; re-fetch the current handle before waiting on it.
    let watcher = loader.fiber_of("watcher").unwrap();
    watcher.wait_ready().await.unwrap();
    assert_eq!(
        watcher.ctx().get::<DependentSaw>().unwrap().0,
        "react/b2",
        "the dependent re-registered against the replacement service"
    );

    kernel.dispose().await;
}

#[tokio::test]
async fn invalid_trees_are_rejected_without_touching_the_mounted_set() {
    let (kernel, loader) = loader_with_kinds();

    loader
        .mount(tree(
            r#"[{ "id": "a", "plugin": "tagged", "config": { "tag": "inv/a" } }]"#,
        ))
        .await
        .unwrap();
    wait_ready_all(&loader).await;
    assert_eq!(count("inv/a"), 1);

    // Unknown kind.
    assert!(
        loader
            .reconcile(tree(r#"[{ "id": "x", "plugin": "ghost", "config": {} }]"#))
            .await
            .is_err()
    );

    // Config that does not fit the kind (missing `tag`).
    assert!(
        loader
            .reconcile(tree(r#"[{ "id": "x", "plugin": "tagged", "config": {} }]"#))
            .await
            .is_err()
    );

    // Duplicate ids in one tree.
    assert!(
        EntryTree::from_json_str(
            r#"[
        { "id": "x", "plugin": "tagged", "config": { "tag": "1" } },
        { "id": "x", "plugin": "tagged", "config": { "tag": "2" } }
    ]"#,
        )
        .is_err()
    );

    // A non-array document.
    assert!(EntryTree::from_json_str(r#"{"entries": []}"#).is_err());

    assert_eq!(
        count("inv/a"),
        1,
        "every rejected reconcile left the running entry alone"
    );
    assert!(loader.fiber_of("a").is_some());

    kernel.dispose().await;
}

#[tokio::test]
async fn duplicate_kind_names_are_rejected() {
    let kernel = Kernel::new();
    let loader = Loader::new(&kernel);

    loader
        .register_entry_kind(entry_kind::<Tagged>("dup/tagged"))
        .unwrap();
    assert!(
        loader
            .register_entry_kind(entry_kind::<Tagged>("dup/tagged"))
            .is_err()
    );

    kernel.dispose().await;
}

#[tokio::test]
async fn mounting_twice_requires_reconcile() {
    let (kernel, loader) = loader_with_kinds();

    let one = tree(r#"[{ "id": "a", "plugin": "tagged", "config": { "tag": "twice/a" } }]"#);
    loader.mount(one).await.unwrap();

    let two = tree(r#"[{ "id": "b", "plugin": "tagged", "config": { "tag": "twice/b" } }]"#);
    assert!(
        loader.mount(two).await.is_err(),
        "use reconcile to add entries"
    );

    kernel.dispose().await;
}

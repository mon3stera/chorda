//! Compile-time discovery: what the linker keeps and what it drops.
//!
//! Empirical contract, verified on this toolchain in debug and release
//! builds: a plugin crate that only sits in `Cargo.toml` is dropped by the
//! linker, registration and all; one `use plugin_crate as _;` line in the
//! host binary is enough to keep it linked and discovered.

use chorda::Kernel;

#[tokio::test]
async fn referenced_plugin_crates_are_discovered_and_registered() {
    plugin_alpha::anchor();

    let kernel = Kernel::with_discovered_plugins();

    let children = kernel.root_ctx().fiber().expect("root fiber").children();
    let mut names: Vec<String> = children
        .iter()
        .map(|fiber| fiber.name().to_owned())
        .collect();
    names.sort();

    assert!(names.contains(&"alpha".to_owned()), "names: {names:?}");

    kernel.dispose().await;
}

#[tokio::test]
async fn a_use_line_links_an_unreferenced_plugin_crate() {
    use plugin_beta as _;

    let kernel = Kernel::with_discovered_plugins();

    let children = kernel.root_ctx().fiber().expect("root fiber").children();
    let names: Vec<String> = children
        .iter()
        .map(|fiber| fiber.name().to_owned())
        .collect();

    assert!(names.contains(&"beta".to_owned()), "names: {names:?}");

    kernel.dispose().await;
}

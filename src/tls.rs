//! Process-wide TLS crypto provider setup.
//!
//! redis-rs's rustls integration deliberately does not pick a crypto backend for you
//! (its own `rustls` dependency is `default-features = false` — only `rustls/std` is
//! enabled, see redis-1.5.0/Cargo.toml:493-496), so it never enables rustls's "ring" or
//! "aws-lc-rs" feature anywhere in the dependency graph. rustls 0.23 requires the
//! *application* to install a process-wide `CryptoProvider` once
//! (`rustls::crypto::CryptoProvider::install_default`) before any TLS connection is
//! attempted, or every TLS connection — not just `--insecure` ones — panics with
//! "Could not automatically determine the process-level CryptoProvider...".
//!
//! This lives in the library (rather than only in `src/main.rs`) so that both the
//! `celery-bench` binary's `main()` and any integration test that opens a real
//! `rediss://` connection can call it directly — a test harness calling it once and
//! the code under test calling it again (e.g. via `main`'s own startup path) must not
//! panic just because the provider is already installed.

/// Install the process-wide rustls `CryptoProvider` (the "ring" backend, matching
/// redis-rs's own dev-dependencies — see redis-1.5.0/Cargo.toml:604-606) if one isn't
/// already installed.
///
/// Idempotent and safe to call more than once from the same process: rustls returns
/// `Err` when a provider is already installed (whether it was this exact call or a
/// previous one, e.g. a test harness calling this before exercising code that also
/// calls it), and that outcome is exactly as good as installing it ourselves, so it is
/// intentionally ignored with `.ok()` rather than `.expect()`/`.unwrap()` — this must
/// never panic no matter how many times or from how many call sites it runs.
pub fn install_crypto_provider() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn install_crypto_provider_is_idempotent() {
        // Must not panic even when called multiple times in the same test process
        // (which is exactly what happens across this crate's many #[test] functions,
        // and what would happen if both a test harness and main() called it).
        install_crypto_provider();
        install_crypto_provider();
        install_crypto_provider();
    }
}

# The one nightly the minimal-versions check uses; CI installs it from this line.
nightly := "nightly-2026-09-26"

# The library must build with the lowest versions of its normal dependencies
# that Cargo.toml allows. The check leaves the dev-dependencies out: the path
# dev-dependency tarantool-test-container pulls in testcontainers, whose
# bollard needs tokio 1.47. With them in the resolution, the check would pass
# only once the library's published floors rose to what such test-only crates
# need. Without the dev-dependencies no test can build, so the check builds
# the library and runs no tests. At the floors, rustc prints two
# mismatched_lifetime_syntaxes warnings, at `prepare_sql` and `space` in
# src/client/executor_ext.rs: async-trait releases before 0.1.82 print them,
# and 0.1.82 and later do not. They are expected; the check does not deny
# warnings. The recipe needs the pinned nightly: it never installs a toolchain
# and never skips the check.
[doc('Check that the library builds with its normal dependencies at their minimum versions')]
minimal-versions:
    #!/usr/bin/env bash
    set -euo pipefail
    # Never let rustup install the toolchain on its own.
    export RUSTUP_AUTO_INSTALL=0
    if ! rustup run {{nightly}} cargo --version > /dev/null 2>&1; then
        echo "error: toolchain {{nightly}} is not installed" >&2
        echo "install it with: rustup toolchain install {{nightly}} --profile minimal" >&2
        exit 1
    fi
    # Resolve in a copy of the library, so the committed Cargo.lock stays as
    # it is. The copy's manifest drops the dev-dependency tables, and the
    # example, bench and test targets, whose files are not copied.
    work=target/minimal-versions
    rm -rf "$work/src" "$work/Cargo.lock"
    mkdir -p "$work"
    cp -R README.md src "$work"/
    awk '/^\[/ { skip = ($0 ~ /^\[dev-dependencies[].]|^\[\[(example|bench|test)\]\]/) } !skip' \
        Cargo.toml > "$work/Cargo.toml"
    cd "$work"
    cargo +{{nightly}} generate-lockfile -Z direct-minimal-versions
    cargo +{{nightly}} check --lib --all-features

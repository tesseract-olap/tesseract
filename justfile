serve:
    watchexec -r -s SIGKILL 'cargo build && RUST_LOG=info ./target/debug/tesseract-olap'

serve-no-ll:
    watchexec -r -s SIGKILL 'cargo build && RUST_LOG=info ./target/debug/tesseract-olap --no-logic-layer'

serve-release:
    watchexec -r -s SIGKILL 'cargo build --release && RUST_LOG=info ./target/release/tesseract-olap'

serve-debug:
    watchexec -r -s SIGKILL 'cargo build && RUST_LOG=debug ./target/debug/tesseract-olap'

serve-release-debug:
    watchexec -r -s SIGKILL 'cargo build --release && RUST_LOG=debug ./target/release/tesseract-olap'

deploy to:
    cargo build --release && scp target/release/tesseract-olap {{to}}

watch:
    cargo watch

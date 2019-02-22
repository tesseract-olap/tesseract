serve:
    watchexec -r -s SIGKILL 'cargo build && RUST_LOG=info ./target/debug/tesseract'

serve-release:
    watchexec -r -s SIGKILL 'cargo build --release && RUST_LOG=info ./target/release/tesseract'

serve-debug:
    watchexec -r -s SIGKILL 'cargo build && RUST_LOG=debug ./target/debug/tesseract'

serve-release-debug:
    watchexec -r -s SIGKILL 'cargo build --release && RUST_LOG=debug ./target/release/tesseract'

deploy to:
    cargo build --release && scp target/release/tesseract {{to}}

check:
    watchexec cargo check

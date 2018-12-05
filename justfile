serve:
    watchexec -r -s SIGKILL 'cargo build && RUST_LOG=info ./target/debug/tesseract'

serve-release:
    watchexec -r -s SIGKILL 'cargo build --release && RUST_LOG=info ./target/release/tesseract'

serve-debug:
    watchexec -r -s SIGKILL 'cargo build && RUST_LOG=debug ./target/debug/tesseract'

serve-release-debug:
    watchexec -r -s SIGKILL 'cargo build --release && RUST_LOG=debug ./target/release/tesseract'

deploy host:
    cargo build --release && scp target/release/tesseract {{host}}:~/.

check:
    watchexec cargo check

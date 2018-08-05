serve:
    watchexec -r -s SIGKILL 'cargo build && RUST_LOG=info ./target/debug/tesseract'

serve-release:
    watchexec -r -s SIGKILL 'cargo build --release && RUST_LOG=info ./target/release/tesseract'

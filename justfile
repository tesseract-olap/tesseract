serve:
    watchexec -r -s SIGKILL 'cargo build && RUST_LOG=info ./target/debug/tesseract'

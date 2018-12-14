# Tesseract

## Getting started

### Prerequisites

#### Packages

Make sure you have just and watchexec installed. If not you can install them via `cargo`:
```
cargo install just
cargo install watchexec
```

Make sure your `~/.cargo/bin` is in your `PATH`.

#### Environment Variables
`TESSERACT_SCHEMA_FILEPATH` should point to the location on disk for the tesseract schema file.

### Run
`just serve`
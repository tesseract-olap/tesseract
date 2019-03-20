# Postgres Driver for Tesseract

Provides support for Postgres databases to Tesseract.

## Testing

To run tests, set `TESSERACT_DATABASE_URL` to a valid Postgres database URL, then run `cargo test`.

## Limitations

* Does not currently support columns with `numeric` type.

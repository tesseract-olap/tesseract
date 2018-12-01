# Tesseract

A Rolap engine.

Currently designed with a rest-ish api based on Mondrian Rest, and reads json schemas modelled after Mondrian schemas.

Backend design still in process. It may:
- mirror Mondrian functionality and schema
- gracefully handle non-aggregative cases
- have streaming as a core concept
- integrate with Python or languages as a library

```
$ git clone https://github.com/hwchen/tesseract && cd tesseract
```

Then check the `justfile` for some ways of building the server.

Note that `watchexec` is just used for dev purposes, the actual build step is `cargo build`.

Tesseract uses environment variables. I have currently set:
```
export TESSERACT_FLUSH_SECRET=12345
export TESSERACT_SCHEMA_FILEPATH=test-schema/schema.json
```
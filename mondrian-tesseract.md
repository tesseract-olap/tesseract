# Migration Notes for Mondrian -> Tesseract

## Schema
- shared dimensions are `SharedDimension` in tesseract, but only `Dimension` in mondrian
- for xml, fields are all snake-case instead of camel case
- for level, fields are `key_column`, `name_column`, `key_type` instead of `column`, `name_column`, `type` in mondrian

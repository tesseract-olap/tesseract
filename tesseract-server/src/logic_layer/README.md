# Tesseract Logic Layer

## Constructing Queries

- `cube` (str): Specifies the cube the query will be performed against.
- `drilldowns` (list): Comma separated list of level names for each desired drilldown. Each level name may or may not be wrapped in square brackets. Note that levels with a comma in their name, require the use of square brackets to work properly. Examples:
    - `[drill 1],[drill 2]`
    - `drill 1,drill 2`
    - `[drill, 1],drill 2`
- **Cuts**: Cuts are defined arbitrarily in the format `level=val 1,val 2`. Only level names are required as the param name. Values can be comma separated and follow the same square brackets convention explained above.
- `time` (list): Comma separated list of time cuts in the format `precision.value`, where precision could be one of `year`, ~~quarter~~, ~~month~~, ~~week~~, ~~day~~, and value is either `latest` or `oldest`.
- `measures` (list): Comma separated list of measure names. Follows the square brackets convention.
- `properties` (list): : Comma separated list of property names. Follows the square brackets convention.
- `filters`: Not yet implemented.
- `parents`: See [Tesseract docs](https://github.com/hwchen/tesseract/blob/master/tesseract-server/README.md#parents).
- `top`: See [Tesseract docs](https://github.com/hwchen/tesseract/blob/master/tesseract-server/README.md#top).
- `top_where`: 
- `sort`: Controls the order of results in the format `measure.direction`.
- `limit`: Limits the number of results in the format `n,offset`.
- `growth`: See [Tesseract docs](https://github.com/hwchen/tesseract/blob/master/tesseract-server/README.md#growth).
- `rca`: See [Tesseract docs](https://github.com/hwchen/tesseract/blob/master/tesseract-server/README.md#rca).
- `debug` (bool): Run query in debug mode. `true` or `false` (default).
- `locale` (list): Comma separated list of locales. Controls the drilldown and cut names in the response. Most useful to specify a language.

## Cache

When the server first starts, or when it is flushed, an internal logic layer cache gets populated.

The contents of the cache help resolve the `time` param in logic layer queries.

## Configuration

The functionality of the logic layer can be further customized by a JSON config file. The path to this config file must be set by an environment variable called `TESSERACT_LOGIC_LAYER_CONFIG_FILEPATH`. Note that this configuration is optional.

Currently, the config file supports declaring aliases for cube names:

```json
{
    "aliases": {
        "cubes": [
            {
                "name": "Example",
                "alternatives": ["ex1", "ex2"]
            }
        ]
    }
}
```

More features coming soon.
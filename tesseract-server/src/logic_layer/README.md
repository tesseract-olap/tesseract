# Tesseract Logic Layer

This file has basic documentation for the functionality supported by the current version of the logic layer.

## Constructing Queries

The base URL for the logic layer is `/data`. The accepted parameters are:

- `cube` (str): Specifies the cube the query will be performed against.
- `drilldowns` (list): Comma separated list of level names for each desired drilldown. Each level name may or may not be wrapped in square brackets. Note that levels with a comma in their name, require the use of square brackets to work properly. Examples:
    - `[drill 1],[drill 2]`
    - `drill 1,drill 2`
    - `[drill, 1],drill 2`
- **Cuts**: Cuts are defined arbitrarily in the format `level=val 1,val 2`. Only level names are required as the param name. Values can be comma separated and follow the same square brackets convention explained above. More details in the next subsection.
- `time` (list): Comma separated list of time cuts in the format `precision.value`, where precision could be one of `year`, `quarter`, `month`, `week`, or `day`, and value is either `latest` or `oldest`.
- `measures` (list): Comma separated list of measure names. Follows the square brackets convention.
- `properties` (list): : Comma separated list of property names. Follows the square brackets convention.
- `filters`(list): Comma seprated list of filters in the format of `measure1.constraint.value` and to support `or` for the same measure `measure1.constraint1.value1.or.constraint2.value2` (Note: `value` in the filters can be float values such as(10.25, 10.0, .5)
- `parents`: See [Tesseract docs](https://github.com/hwchen/tesseract/blob/master/tesseract-server/README.md#parents).
- `top`: See [Tesseract docs](https://github.com/hwchen/tesseract/blob/master/tesseract-server/README.md#top).
- `top_where`: 
- `sort`: Controls the order of results in the format `measure.direction`.
- `limit`: Limits the number of results in the format `n,offset`.
- `growth`: See [Tesseract docs](https://github.com/hwchen/tesseract/blob/master/tesseract-server/README.md#growth).
- `rca`: See [Tesseract docs](https://github.com/hwchen/tesseract/blob/master/tesseract-server/README.md#rca).
- `debug` (bool): Run query in debug mode. `true` or `false` (default).
- `locale` (list): Comma separated list of locales. Controls the drilldown and cut names in the response. Most useful to specify a language.

### More on cuts

The logic layer also supports the following cut operations:

- `val_ID:parents`: Returns parent entries for each parent level of the entry with ID=val_ID
- `val_ID:children`: Returns child entries for the entry with ID=val_ID
- `val_ID:neighbors`: Returns 4 entries near the entry with ID=val_ID

These operations can be combined in the same query (e.g. `level=v1:children,v2:parents`). 

To cut on different levels in the same dimension, you can provide the dimension name as the cut key: `dimension=level_1_val:children,level_2_val:parents`.

When the logic layer detects cuts on multiple levels in the same dimension, it generates and runs multiple different queries with each possible cut combination across all cuts. It then combines those query responses into the final user response.

## Cache

When the server first starts, or when it is flushed, an internal logic layer cache gets populated. Here's a rundown of what's stored in the cache:

- latest and oldest time values for year, quarter, month, week, and day
- level and property mappings that help resolve query params
- for each level, a mapping from the level name to helper objects containing parent, children, and neighbor IDs for each element in that level
- for each dimension, a mapping from IDs to the levels where those IDs are present

## Configuration

The functionality of the logic layer can be further customized by a JSON config file. The path to this config file must be set by an environment variable called `TESSERACT_LOGIC_LAYER_CONFIG_FILEPATH`. Note that this configuration is optional.

Currently, the config file supports:

- declaring aliases for cube names
- declaring unique names for levels and properties in a cube
- defining named sets

Example:

```json
{
    "aliases": {
        "cubes": [
            {
                "name": "Example",
                "alternatives": ["ex1", "ex2"],
                "levels": [
                    {
                        "current_name": "Some.Level.Name",
                        "unique_name": "Unique Level Name"
                    }
                ],
                "properties": [
                    {
                        "current_name": "Some.Longer.Property.Name",
                        "unique_name": "Unique Property Name"
                    }
                ]
            }
        ]
    },
    "named_sets": [
        {
            "level_name": "Some.Level.Name",
            "sets": [
                {
                    "set_name": "Set 1",
                    "values": ["ID 1", "ID 2"]
                }
            ]
        }
    ]
}
```

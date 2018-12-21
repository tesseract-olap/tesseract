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

# API

## Metadata
Metadata for all cubes:
```
/cubes
```

Metadata for one cube:
```
/cubes/<cube_name>
```

## Aggregate Query:
```
/cubes/<cube_name>/aggregate<format>?<query_options>
```
See below for query options

Format may be:
- not specified, which defaults to csv
- csv
- jsonrecords `{ data: [ {record}, {record}, .. ]`

### Naming

To reference a level:
```
Dimension.Hierarchy.Level
```

To reference a cut, just add the members as a comma-separated list:
```
Dimension.Hierarchy.Level.m1,m2,m3
```

Note: you may use brackets so that it looks like MDX, but tesseract splits on `.` _first_, so it will not allow you to have names with internal periods. This design choice was made in part to make urls easy to read, and may be changed.

Dropping the hierarchy: schema levels are referenced by fully qualified names (dimension, hierarchy, level), but the user may write only `Dimension.Level` in the cases where the dimension name is the same as the hierarchy. Tesseract will fill out the name in the query before passing it on to the schema.

### Drilldown
Multiple drilldowns are allowed.
Only one drilldown per dimension is allowed.
```
drilldowns%5B%5D=drilldown_name
```
The `drilldown_name` is of the general format:
```
Dimension.Hierarchy.Level
```
But the format is lenient, see the `Naming` subsection above for more details.

### Cut
Multiple cuts are allowed.
Only one cut per level is allowed (this means that multiple cuts per dimension is allowed). This is a convenient behavior for now, but may be limited in the future.
Multiple members may be specified in each cut.
```
cuts%5B%5D=cut_name
```
The `cut_name` is of the general format:
```
Dimension.Hierarchy.Level.m1,m2,m3
```
But the format is lenient, see the `Naming` subsection above for more details.

### Measure
Multiple measures are allowed.
```
measures%5B%5D=measure_name
```

The `measure_name` is treated as one string.

### Growth:
Growth calculation requires a time drilldown and a measure, both of which must also be specified elsewhere in the query
```
growth=<TimeDrill>,<Measure>
```
- TimeDrill: drilldown name
- Measure: measure name

### Top:
Top calculation is `top n by dimension, on measure ordered by asc/desc`.

So in a query where the drilldowns are `geography` and `product` and `brand` and the measure is `quantity`, you can use the top calculation to get the top 3 `product`/`brand` combinations by `geography`, for quantity (in this case you'd probably sort desc).

```
top=<n>,<GroupDrill>,<Measure>,<sort_order>
```
- n: integer
- GroupDrill: drilldown name
- Measure: measure name
- sort order: `asc`/`desc`

### parents:
Parents will return metadata for all parent levels for a given drilldown on a level.
This is currently a global switch; it works for all drilldowns in a query.
```
parents=<bool>
```
- bool; `true`/`false` (default `false`)

### Properties:
Multiple properties are allowed.
Multiple properties are allowed per level.
```
properties%5B%5D=property_name
```
The `property_name` is of the general format:
```
Dimension.Hierarchy.Level.Property
```
But the format is lenient, see the `Naming` subsection above for more details.

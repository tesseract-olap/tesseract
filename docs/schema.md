# Schema

lots of TODO here: Drilldowns, cuts, etc. And adding xml equivalents.

## Measures
in a cube:

```
"measures": [
    {
    "name": "Enrollment",
    "column": "enrollment",
    "aggregator": "sum"
    },
    ...
]

### Aggregators

basic aggregators
```
{
    "name": "Enrollment <Agg>",
    "column": "enrollment",
    "aggregator": "sum|count|avg|max|min"
}
```

weighted sum
```
{
    "name": "Enrollment Weighted Sum",
    "column": "enrollment",
    "aggregator": {
        "weighted_avg": {
            "weight_column": "pop"
        }
    }
}
```

weighted average
```
{
    "name": "Enrollment Average",
    "column": "enrollment",
    "aggregator": {
        "weighted_avg": {
            "weight_column": "pop"
        }
    }
}
```

moe
```
{
    "name": "Enrollment MOE",
    "column": "enrollment_moe",
    "aggregator": {
        "moe": {
            "critical_value": 1.645
        }
    }
}
```

replicate weight moe
```
{
    "name": "Enrollment MOE (replicate weight)",
    "column": "enrollment_moe",
    "aggregator": {
        "replicate_weight_moe": {
            "critical_value": 1.645,
            "design_factor": 4.0,
            "secondary_columns": [
                "pop1",
                "pop2"
            ]
        }
    }
}
```

weighted average moe
```
{
    "name": "Enrollment Weighted Average MOE",
    "column": "enrollment_moe",
    "aggregator": {
        "weighted_average_moe": {
            "critical_value": 1.645,
            "design_factor": 4.0,
            "primary_weight": "pop",
            "secondary_weight_columns": [
                "pop1",
                "pop2"
            ]
        }
    }
}
```

grouped median
```
{
    "name": "Grouped Median Enrollment",
    "column": "enrollment",
    "aggregator": {
        "basic_grouped_median": {
            "group_aggregator": "sum",
            "group_dimension": "School.School.School"
        }
    }
}
```

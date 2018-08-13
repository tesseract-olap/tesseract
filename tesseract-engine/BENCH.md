First pass at benchmarking.

08/12/2018

Table is 13,140,000 rows

Rust:
From basic-bench example
```
execute query: 2.632
```

monet:
```
time mclient -d test -s "select \"year\", age, sum(population) from \"test-cube\" group by \"year\", age"

90 tuples
real 0.20
user 0.00
sys 0.00
```

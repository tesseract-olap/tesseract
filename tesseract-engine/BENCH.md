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

08/13/2018
Removed some allocations in memory-naive in the hot loop.
```
execute query: 1.933
```
30% improvement

08/22/2018
Changed aggregation from hashmap to vecs indexed by bit-packed dim index.

execute query: 0.219

On par with monetdb! And when I did the bit-packing incorrectly, the aggregation vecs were even smaller, and the execution time was in the area of 0.15s. This leads me to believe that using dictionary encoding for the dims (to ensure that the dim members are very small, resulting in smaller bit-packed indexes) would result in queries faster than Monetdb.

Next goal: 0.050s in locustdb! How much of that is because of parallelism (would easily explain 3x performance), and how much is compression?

Also, note that peak memory usage is about 920MB on a 285MB data set. This can probably be improved with compression.

08/23/2018
Added dictionary encoding to dims.

execute query: 0.168 thereabouts. ingest time went from about 6s to 9s.

at least 23% decrease in execution time.

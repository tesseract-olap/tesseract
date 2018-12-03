# Prototype goal:
The goal is to show low latency and high concurrency.
So, my goal is to take one profile (about 100 concurrent requests) and show better performance. Should be pretty easy, even using postgres.

- need to be able to read in xml schemas.
- need same api as previous mondrian. `/cubes`

# Schema additions?
- add table to hierarchy and cube
- add validation layer, after doing the basic conversion? This way it's easier to track errors, and not do add-hoc conversion and error-checking at the same time.
- check that hierarchy names don't have duplicates (Error: Duplicate name {}, which may be a result of falling back to default name for multiple non-named hierarchies)
- allow non-named hierarchies?

# Future considerations on design

Some thoughts from @jspeis:

In my mind some of the opportunities for improvements over the existing MR: 1. MR does not handle non-additive measures well 2. better memory utilizations (esp when sending large responses to clients) 3. support for frequently updating data sources. 4. map out integration with analytical services (e.g. some built-in standard models or a plugin architecture could let ML models more easily run across projects) 5. multi-db source support would be awesome

# Clickhouse sql
```
:) select d0 as d0, d1 as d1, sum(quantity) as m0 from test where d0 in (12, 14) group by d0, d1;

┌─d0─┬──d1─┬─m0─┐
│ 14 │ 225 │  4 │
│ 12 │ 225 │  9 │
└────┴─────┴────┘

2 rows in set. Elapsed: 0.003 sec.
```

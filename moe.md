Moe (not approximated), for pums

Setup was the same as for weighted avg, just rename the cols val and weight to val1 and val2

The general equation is
```
1.645 * pow(0.05 * (pow(sum(val1) - sum(val2), 2) + pow(sum(val1) - sum(val3), 2) + ...), 0.5)
```

For the calculation, I'll need to specify the primary value, and then all the secondary values in a list.

This is easier than the approx moe, because I only need to pass through many cols, instead of doing two different drilldown patterns on the value.
This strategy is also similar to the weighted avg calculation, where there's an intermediate aggregation (sum) on the fact table, and then the final calculation is done at the end after dim table joins.
The concern is whether the performance is adequate. I should generate a statement to test it against a full dataset.

Setup:
```
mochi :) create table test_weighted (id Int32, val Int32, weight Int32) Engine=MergeTree() Order By id;
mochi :) insert into test_weighted (id, val, weight) values (1,1, 1100), (2,2, 1200), (3,3, 1300), (4,4, 1400), (1, 5, 1500), (2,6, 1600), (3,7, 1700), (4,8, 1800)
mochi :) create table test_weighted_dim (id Int32, label String, group_id Int32, group_label String) Engine=MergeTree() Order By id;
mochi :) insert into test_weighted_dim (id, label, group_id, group_label) values (1, 'state1', 1, 'country1'), (2, 'state2', 1, 'country1'), (3, 'state3', 2, 'country2'), (4,'state4', 2, 'country2')
```
Moe with no intermediate aggregation
```
mochi :) select group_id, group_label, 1.645 * pow(0.05 * (pow(sum(val1) - sum(val2), 2)), 0.5) from (select id, label, group_id, group_label, val1, val2 from (select id, label, group_id, group_label from test_weighted_dim) all inner join (select id,  val as val1, weight as val2 from test_weighted) using id) group by group_id, group_label

SELECT 
    group_id, 
    group_label, 
    1.645 * pow(0.05 * pow(sum(val1) - sum(val2), 2), 0.5)
FROM 
(
    SELECT 
        id, 
        label, 
        group_id, 
        group_label, 
        val1, 
        val2
    FROM 
    (
        SELECT 
            id, 
            label, 
            group_id, 
            group_label
        FROM test_weighted_dim 
    ) 
    ALL INNER JOIN 
    (
        SELECT 
            id, 
            val AS val1, 
            weight AS val2
        FROM test_weighted 
    ) USING (id)
) 
GROUP BY 
    group_id, 
    group_label

┌─group_id─┬─group_label─┬─multiply(1.645, pow(multiply(0.05, pow(minus(sum(val1), sum(val2)), 2)), 0.5))─┐
│        2 │ country2    │                                                              2272.473400241464 │
│        1 │ country1    │                                                             1981.1495198608811 │
└──────────┴─────────────┴────────────────────────────────────────────────────────────────────────────────┘

2 rows in set. Elapsed: 0.005 sec. 
```

Moe with intermediate aggregation
```
mochi :) select group_id, group_label, 1.645 * pow(0.05 * (pow(sum(val1_sum) - sum(val2_sum), 2)), 0.5) from (select id, label, group_id, group_label, val1_sum, val2_sum from (select id, label, group_id, group_label from test_weighted_dim) all inner join (select id,  sum(val) as val1_sum, sum(weight) as val2_sum from test_weighted group by id) using id) group by group_id, group_label

SELECT 
    group_id, 
    group_label, 
    1.645 * pow(0.05 * pow(sum(val1_sum) - sum(val2_sum), 2), 0.5)
FROM 
(
    SELECT 
        id, 
        label, 
        group_id, 
        group_label, 
        val1_sum, 
        val2_sum
    FROM 
    (
        SELECT 
            id, 
            label, 
            group_id, 
            group_label
        FROM test_weighted_dim 
    ) 
    ALL INNER JOIN 
    (
        SELECT 
            id, 
            sum(val) AS val1_sum, 
            sum(weight) AS val2_sum
        FROM test_weighted 
        GROUP BY id
    ) USING (id)
) 
GROUP BY 
    group_id, 
    group_label

┌─group_id─┬─group_label─┬─multiply(1.645, pow(multiply(0.05, pow(minus(sum(val1_sum), sum(val2_sum)), 2)), 0.5))─┐
│        2 │ country2    │                                                                      2272.473400241464 │
│        1 │ country1    │                                                                     1981.1495198608811 │
└──────────┴─────────────┴────────────────────────────────────────────────────────────────────────────────────────┘

2 rows in set. Elapsed: 0.004 sec. 
```

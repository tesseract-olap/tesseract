Example for looking at weighted avg calculation

Setup:
```
mochi :) create table test_weighted (id Int32, val Int32, weight Int32) Engine=MergeTree() Order By id;
mochi :) insert into test_weighted (id, val, weight) values (1,1, 1100), (2,2, 1200), (3,3, 1300), (4,4, 1400), (1, 5, 1500), (2,6, 1600), (3,7, 1700), (4,8, 1800)
mochi :) create table test_weighted_dim (id Int32, label String, group_id Int32, group_label String) Engine=MergeTree() Order By id;
mochi :) insert into test_weighted_dim (id, label, group_id, group_label) values (1, 'state1', 1, 'country1'), (2, 'state2', 1, 'country1'), (3, 'state3', 2, 'country2'), (4,'state4', 2, 'country2')
```

Aggregating on lowest level of dim, no dim table join
```
mochi :) select id, sum(val*weight) / sum(weight) from test_weighted group by id

SELECT 
    id, 
    sum(val * weight) / sum(weight)
FROM test_weighted 
GROUP BY id

┌─id─┬─divide(sum(multiply(val, weight)), sum(weight))─┐
│  4 │                                            6.25 │
│  3 │                               5.266666666666667 │
│  2 │                               4.285714285714286 │
│  1 │                              3.3076923076923075 │
└────┴─────────────────────────────────────────────────┘

4 rows in set. Elapsed: 0.007 sec. 
```

Aggregating on lowest level of dim, with join
```
mochi :) select id, label, group_id, group_label, weighted_num / weighted_denom from (select id, label, group_id, group_label from test_weighted_dim) all inner join (select id, sum(val*weight) as weighted_num, sum(weight) as weighted_denom from test_weighted group by id) using id

SELECT 
    id, 
    label, 
    group_id, 
    group_label, 
    weighted_num / weighted_denom
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
        sum(val * weight) AS weighted_num, 
        sum(weight) AS weighted_denom
    FROM test_weighted 
    GROUP BY id
) USING (id)

┌─id─┬─label──┬─group_id─┬─group_label─┬─divide(weighted_num, weighted_denom)─┐
│  1 │ state1 │        1 │ country1    │                   3.3076923076923075 │
│  2 │ state2 │        1 │ country1    │                    4.285714285714286 │
│  3 │ state3 │        2 │ country2    │                    5.266666666666667 │
│  4 │ state4 │        2 │ country2    │                                 6.25 │
└────┴────────┴──────────┴─────────────┴──────────────────────────────────────┘

4 rows in set. Elapsed: 0.005 sec. 
```

Intermediate group by, for parent level agg (the fast way)
```
mochi :) select group_id, group_label, sum(weighted_num)/sum(weighted_denom) from (select id, label, group_id, group_label, weighted_num, weighted_denom from (select id, label, group_id, group_label from test_weighted_dim) all inner join (select id, sum(val*weight) as weighted_num, sum(weight) as weighted_denom from test_weighted group by id) using id) group by group_id, group_label

SELECT 
    group_id, 
    group_label, 
    sum(weighted_num) / sum(weighted_denom)
FROM 
(
    SELECT 
        id, 
        label, 
        group_id, 
        group_label, 
        weighted_num, 
        weighted_denom
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
            sum(val * weight) AS weighted_num, 
            sum(weight) AS weighted_denom
        FROM test_weighted 
        GROUP BY id
    ) USING (id)
) 
GROUP BY 
    group_id, 
    group_label

┌─group_id─┬─group_label─┬─divide(sum(weighted_num), sum(weighted_denom))─┐
│        2 │ country2    │                              5.774193548387097 │
│        1 │ country1    │                              3.814814814814815 │
└──────────┴─────────────┴────────────────────────────────────────────────┘

2 rows in set. Elapsed: 0.006 sec. 
```

No initial group by, for parent level agg (this is the slow way, since you have to join on granular rows, instead of an aggregate row)
```
mochi :) select group_id, group_label, sum(val * weight)/sum(weight) from (select id, label, group_id, group_label, val, weight from (select id, label, group_id, group_label from test_weighted_dim) all inner join (select id,  val, weight from test_weighted) using id) group by group_id, group_label

SELECT 
    group_id, 
    group_label, 
    sum(val * weight) / sum(weight)
FROM 
(
    SELECT 
        id, 
        label, 
        group_id, 
        group_label, 
        val, 
        weight
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
            val, 
            weight
        FROM test_weighted 
    ) USING (id)
) 
GROUP BY 
    group_id, 
    group_label

┌─group_id─┬─group_label─┬─divide(sum(multiply(val, weight)), sum(weight))─┐
│        2 │ country2    │                               5.774193548387097 │
│        1 │ country1    │                               3.814814814814815 │
└──────────┴─────────────┴─────────────────────────────────────────────────┘

2 rows in set. Elapsed: 0.007 sec. 
```

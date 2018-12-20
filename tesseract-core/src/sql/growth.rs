use super::GrowthSql;

pub fn calculate(
    final_sql: String,
    final_drill_cols: &str,
    growth: &GrowthSql,
    ) -> (String, String)
{
    // A whole section to string manipulate to remove references to growth cols
    let mut all_drill_cols_except_growth = final_drill_cols.to_owned();

    let mut growth_cols = vec![];

    for l in &growth.time_drill.level_columns {
        growth_cols.push(l.key_column.clone());
        if let Some(ref n) = l.name_column {
            growth_cols.push(n.clone());
        }
    }

    growth_cols.push(growth.mea.clone());

    // slow for now, but it's a small string
    for col in growth_cols {
        all_drill_cols_except_growth = all_drill_cols_except_growth.replace(&col, "").replace(", ,", ",").to_owned();
    }

    all_drill_cols_except_growth = all_drill_cols_except_growth.trim().trim_matches(',').to_owned();

    // Group by everything besides the time cols

    let final_sql = format!("\
        select \
            {}, \
            final_times, \
            final_m, \
            (final_m_diff / (final_m - final_m_diff)) as growth \
        from (\
            with \
                groupArray({}) as times, \
                groupArray({}) as all_m_in_group, \
                arrayEnumerate(all_m_in_group) as all_m_in_group_ids, \
                arrayMap( i -> i > 1 ? all_m_in_group[i] - all_m_in_group[i-1]: 0, all_m_in_group_ids) as m_diff \
            select \
                {}, \
                times, \
                all_m_in_group, \
                m_diff \
            from ({} \
                order by \
                    {} \
            ) \
            group by \
                {} \
        ) \
        array Join \
            m_diff as final_m_diff, \
            all_m_in_group as final_m, \
            times as final_times",
        all_drill_cols_except_growth,
        growth.time_drill.col_string(),
        growth.mea,
        all_drill_cols_except_growth,
        final_sql,
        growth.time_drill.col_string(),
        all_drill_cols_except_growth,
    );

    // Externally, remember to switch out order of time cols. Internally, don't care, number
    // is the same
    let final_drill_cols = format!("{}, final_times, final_m, growth", all_drill_cols_except_growth);

    (final_sql, final_drill_cols)
}

#[cfg(test)]
mod test {
    use super::*;
    use super::super::*;

    #[test]
    fn test_growth() {
        let (growth, headers) = calculate("select * from test".to_owned(), "date, language, framework, ex_complete",
            &GrowthSql {
                time_drill: DrilldownSql {
                    table: Table { name: "".to_owned(), primary_key: None, schema: None },
                    primary_key: "".to_owned(),
                    foreign_key: "".to_owned(),
                    level_columns: vec![LevelColumn { key_column: "date".to_owned(), name_column: None }],
                    property_columns: vec![],
                },
                mea: MeasureSql {
                    aggregator: "".to_owned(),
                    column: "ex_complete".to_owned(),
                },
            }
        );

        println!("{}", growth);
        println!("{}", headers);
        assert_eq!(growth, "".to_owned());
    }
}

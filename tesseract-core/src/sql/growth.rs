//! Links for how I calculated growth in clickhouse:
//!
//! A gist which shows runningdifference by group (using arrays)
//! https://gist.github.com/filimonov/2ff5f083c2f874eceebde8877721afc4
//!
//! Near the bottom of this issue, there is a link to how array Join is used
//! to bring the groupArray back to full rows.
//! https://github.com/yandex/ClickHouse/issues/1469

use itertools::join;

use super::GrowthSql;

pub fn calculate(
    final_sql: String,
    final_drill_cols: &str,
    num_measures: usize,
    growth: &GrowthSql,
    ) -> (String, String)
{
    // A whole section to string manipulate to remove references to growth cols
    let mut all_drill_cols_except_growth = final_drill_cols.to_owned();

    let mut time_cols = vec![];

    for l in &growth.time_drill.level_columns {
        time_cols.push(l.key_column.clone());
        if let Some(ref n) = l.name_column {
            time_cols.push(n.clone());
        }
    }

    let mut growth_cols = time_cols.clone();
    growth_cols.push(growth.mea.clone());

    // slow for now, but it's a small string
    for col in growth_cols {
        all_drill_cols_except_growth = all_drill_cols_except_growth.replace(&col, "").replace(", ,", ",").to_owned();
    }

    all_drill_cols_except_growth = all_drill_cols_except_growth.trim().trim_matches(',').to_owned();

    // Group by everything besides the time cols
    // The time columns need to each be packed and unpacked individually; handles cases when
    // there's a time col that has parents.
    // TODO this does not handle a time column that has properties!!!

    let time_drill_len = time_cols.len();

    let final_times = join((0..time_drill_len).map(|i| format!("final_times_{}", i)), ", ");
    let times = join((0..time_drill_len).map(|i| format!("times_{}", i)), ", ");
    let times_as_final_times = join((0..time_drill_len).map(|i| format!("times_{} as final_times_{}", i, i)), ", ");

    let grouparray_times = join(
        time_cols.iter().enumerate()
            .map(|(i, col)| format!("groupArray({}) as times_{}", col, i))
            , ", "
    );

    // TODO fix hack for parsing growth mea idx, probably by passing in a usize and then
    // constructing the name later
    let growth_mea_idx = growth.mea.chars()
        .last()
        .expect("must be a last char for growth.mea")
        .to_digit(10)
        .expect("last char of growth.mea must be integer");

    let final_other_meas = (0..num_measures)
        .filter(|i| {
            *i != growth_mea_idx as usize
        }).map(|i| format!("final_other_m{}", i));
    let mut final_other_meas = join(final_other_meas, ", ");
    if final_other_meas != "" {
        final_other_meas = format!("{}, ", final_other_meas);
    }

    let other_meas = (0..num_measures)
        .filter(|i| {
            *i != growth_mea_idx as usize
        }).map(|i| format!("other_m{}", i));
    let mut other_meas = join(other_meas, ", ");
    if other_meas != "" {
        other_meas = format!("{}, ", other_meas);
    }

    let grouparray_other_meas = (0..num_measures)
        .filter(|i| {
            *i != growth_mea_idx as usize
        }).map(|i| format!("groupArray(final_m{}) as other_m{}", i, i));
    let mut grouparray_other_meas = join(grouparray_other_meas, ", ");
    if grouparray_other_meas != "" {
        grouparray_other_meas = format!("{}, ", grouparray_other_meas);
    }

    let other_meas_as_final_other_meas = (0..num_measures)
        .filter(|i| {
            *i != growth_mea_idx as usize
        }).map(|i| format!("other_m{} as final_other_m{}", i, i));
    let mut other_meas_as_final_other_meas = join(other_meas_as_final_other_meas, ", ");
    if other_meas_as_final_other_meas != "" {
        other_meas_as_final_other_meas = format!(",{}", other_meas_as_final_other_meas);
    }

    let final_sql = format!("\
        select \
            {}, \
            {final_times}, \
            {final_other_meas} \
            final_m, \
            (final_m_diff / (final_m - final_m_diff)) as growth \
        from (\
            with \
                {grouparray_times}, \
                {grouparray_other_meas} \
                groupArray({}) as all_m_in_group, \
                arrayEnumerate(all_m_in_group) as all_m_in_group_ids, \
                arrayMap( i -> i > 1 ? all_m_in_group[i] - all_m_in_group[i-1]: 0, all_m_in_group_ids) as m_diff \
            select \
                {}, \
                {other_meas} \
                {times}, \
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
            {times_as_final_times} \
            {other_meas_as_final_other_meas}",
        all_drill_cols_except_growth,
        growth.mea,
        all_drill_cols_except_growth,
        final_sql,
        growth.time_drill.col_string(),
        all_drill_cols_except_growth,
        final_times = final_times,
        grouparray_times = grouparray_times,
        times = times,
        times_as_final_times = times_as_final_times,
        other_meas = other_meas,
        final_other_meas = final_other_meas,
        grouparray_other_meas = grouparray_other_meas,
        other_meas_as_final_other_meas = other_meas_as_final_other_meas,
    );

    // Externally, remember to switch out order of time cols. Internally, don't care, number
    // is the same
    let final_drill_cols = format!("{}, {}, {} final_m, growth", all_drill_cols_except_growth, final_times, final_other_meas);

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

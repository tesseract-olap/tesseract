use itertools::join;

use super::{
    LimitSql,
    SortSql,
    TopSql,
    TopWhereSql,
    FilterSql,
};

pub fn wrap_options(
    final_sql: String,
    final_drill_cols: &str,
    top: &Option<TopSql>,
    top_where: &Option<TopWhereSql>,
    sort: &Option<SortSql>,
    limit: &Option<LimitSql>,
    filters: &[FilterSql],
    ) -> String
{
    let mut final_sql = final_sql;

    // Now that final groupings are done, do wrapping options
    // like top, filter, sort
    if let Some(top) = top {
        final_sql = format!("select * from ({}) {} order by {} {} limit {} by {}",
            final_sql,
            if let Some(tw) = top_where { format!("where {} {}", tw.by_column, tw.constraint.sql_string()) } else { "".into() },
            join(&top.sort_columns, ", "),
            top.sort_direction.sql_string(),
            top.n,
            top.by_column,
        );
    }

    // There's a final wrapper clause no matter what.
    // - it sorts by final_drill_cols
    // - unless there's a specific sort, which just goes to head of cols
    // - or if there's a top, sort by the by_dim col.
    // - limits
    let limit_sql = {
        if let Some(limit) = limit {
            if let Some(offset) = limit.offset {
                format!("limit {}, {}", offset, limit.n)
            } else {
                format!("limit {}", limit.n)
            }
        } else {
            "".to_string()
        }
    };

    let sort_sql = {
        if let Some(sort) = sort {
            format!("order by {}, {} {}",
                sort.column,
                final_drill_cols,
                sort.direction.sql_string()
            )
        } else if let Some(top) = top {
            format!("order by {} asc, {}",
                top.by_column,
                join(top.sort_columns.iter().map(|c| format!("{} desc", c)), ", "),
            )
        } else {
            // default uses just final drill cols
            format!("order by {} asc",
                final_drill_cols,
            )
        }
    };

    let filters_sql = if !filters.is_empty() {
        let filter_clauses = filters.iter()
            .map(|f| format!("{} {}", f.by_column, f.constraint.sql_string()));
        format!("where {}", join(filter_clauses, " and "))
    } else {
        "".into()
    };


    final_sql = format!("select * from ({}) {} {} {}",
        final_sql,
        filters_sql,
        sort_sql,
        limit_sql,
    );

    final_sql
}


use super::{
    LimitSql,
    SortSql,
    TopSql,
};
use itertools::join;

pub fn wrap_options(
    final_sql: String,
    final_drill_cols: &str,
    top: &Option<TopSql>,
    sort: &Option<SortSql>,
    limit: &Option<LimitSql>,
    ) -> String
{
    let mut final_sql = final_sql;

    // Now that final groupings are done, do wrapping options
    // like top, filter, sort
    if let Some(top) = top {
        final_sql = format!("select * from ({}) order by {} {} limit {} by {}",
            final_sql,
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
            format!("limit {}", limit.n)
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

    final_sql = format!("select * from ({}) {} {}",
        final_sql,
        sort_sql,
        limit_sql,
    );

    final_sql
}


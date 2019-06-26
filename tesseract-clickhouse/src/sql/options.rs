use itertools::join;
use tesseract_core::query::{Comparison, Constraint};
use tesseract_core::{QueryIr};

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
    query_ir: &QueryIr,
    num_measures: usize
    ) -> String
{
    let mut final_sql = final_sql;
    let top = &query_ir.top;
    let top_where = &query_ir.top_where;
    let sort = &query_ir.sort;
    let limit = &query_ir.limit;
    let filters = &query_ir.filters;
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
            format!("order by {} {}, {}",
                sort.column,
                sort.direction.sql_string(),
                final_drill_cols,
            )
        } else if let Some(top) = top {
            format!("order by {} asc, {}",
                top.by_column,
                join(top.sort_columns.iter().map(|c| format!("{} desc", c)), ", "),
            )
        } else {
            // default uses just final drill cols
            // asc default for all cols
            format!("order by {}",
                final_drill_cols,
            )
        }
    };

    let mut filters_sql = if !filters.is_empty() {
        let filter_clauses = filters.iter()
            .map(|f| format!("{} {}", f.by_column, f.constraint.sql_string()));
        format!("where {}", join(filter_clauses, " and "))
    }
    else {
        "".into()
    };

    // Determine if sparse filter is needed, and construct appropriate filters_sql
    {
        let sparse_clauses = (0..num_measures).into_iter().map(|i| format!("isNotNull(final_m{})", i));
        let sparse_filter_sql = join(sparse_clauses, " and ");
        if filters.is_empty() && query_ir.sparse {
            filters_sql = format!("where {}", sparse_filter_sql);
        } else if !filters.is_empty() && query_ir.sparse {
            filters_sql = format!("{} and {}", filters_sql, sparse_filter_sql);
        }
    }


    final_sql = format!("select * from ({}) {} {} {}",
        final_sql,
        filters_sql,
        sort_sql,
        limit_sql,
    );

    final_sql
}


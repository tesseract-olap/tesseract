use itertools::join;

use super::{
    LimitSql,
    SortSql,
    TopSql,
    TopWhereSql,
    FilterSql,
};

use tesseract_core::query_ir::{DrilldownSql};

pub fn wrap_options(
    final_sql: String,
    drills:  &[DrilldownSql],
    top: &Option<TopSql>,
    top_where: &Option<TopWhereSql>,
    sort: &Option<SortSql>,
    limit: &Option<LimitSql>,
    filters: &[FilterSql],
) -> String
{
    let mut final_sql = final_sql;

    // TODO: top query not yet supported

    let final_drill_cols = drills.iter().map(|drill| {
        let tmp: Vec<String> = drill.col_alias_only_vec().iter().map(|alias| format!("options_subquery.{}", alias)).collect();
        return join(tmp, ", ");
    });
    let final_drill_cols = join(final_drill_cols, ", ");


    // There's a final wrapper clause no matter what.
    // - it sorts by final_drill_cols
    // - unless there's a specific sort, which just goes to head of cols
    // - or if there's a top, sort by the by_dim col.
    // - limits
    let limit_sql = {
        if let Some(limit) = limit {
            if let Some(offset) = limit.offset {
                format!("LIMIT {} OFFSET {}", offset, limit.n)
            } else {
                format!("LIMIT {}", limit.n)
            }
        } else {
            "".to_string()
        }
    };

    let sort_sql = {
        if let Some(sort) = sort {
            format!("ORDER BY {} {}, {}",
                    sort.column,
                    sort.direction.sql_string(),
                    final_drill_cols
            )
        }
        else {
            // by default dont sort unless user asks
            "".to_string()
        }
    };

    let filters_sql = if !filters.is_empty() {
        let filter_clauses = filters.iter()
            .map(|f| format!("{} {}", f.by_column, f.constraint.sql_string()));
        format!("WHERE {}", join(filter_clauses, " AND "))
    } else {
        "".into()
    };


    final_sql = format!("SELECT * FROM ({}) options_subquery {} {} {}",
                        final_sql,
                        filters_sql,
                        sort_sql,
                        limit_sql,
    );

    final_sql
}


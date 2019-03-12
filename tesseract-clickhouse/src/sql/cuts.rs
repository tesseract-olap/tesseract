use super::CutSql;

pub fn cut_sql_string(cut: &CutSql) -> String {
    if cut.for_match {
        format!("{}", cut.members_like_string())
    } else {
        // col not in ('', '',...)
        format!("{} {} ({})", cut.column, cut.mask_sql_in_string(), cut.members_string())
    }
}

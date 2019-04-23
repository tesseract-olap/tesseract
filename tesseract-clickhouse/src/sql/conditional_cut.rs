//! Takes in a fact scan (with other cuts already performed)
//! and then performs a cut on one dimension depending on whether
//! that row also has a member from another dimension (works with hierarchies
//! in dim tables too).


pub fn conditional_cut(
    final_sql: String, // sql so far
    final_drill_cols: &str, // drill cols so far
    conditional_cut_ir: ConditionalCutSql,
    ) -> (String, String) //final sql, final drill cols
{
    let cc = conditional_cut_ir;

    // formatting for drill cols at different stages
    // - cut_col
    // for each non-cut-col, `groupArray(drill_col) as array_drill_col`.
    // for each non-cut-col, `array_drill_col`.
    // for each non-cut-col, `array_drill_col as drill_col`.
    //
    // The last select will go back to the original `final_drill_cols`

    let final_drill_cols_iter = final_drill_cols.split(",");

    // formatting for condition
    // - condition-foreign-key
    // - condition-col
    // - members


    let pivot = format!("select {cut_col}"
    );

    let final_sql = format!("select {final_drill_cols} from {final_sql} where {condition}",
        final_drill_cols = final_drill_cols,
        final_sql = final_sql,
        condition = condition,
    );


// select owner_object_id, security_id_final, volumes_final from (select owner_object_id,
// array_security_id, array_volumes from (select owner_object_id, groupArray(security_id) as
// array_security_id, groupArray(volumes) as array_volumes from (select owner_object_id,
// security_id, sum(volume) as volumes from sos_cube where date like '2016-09-09%' group by
// owner_object_id, security_id) group by owner_object_id) where hasAny(array_security_id,
// [20058994]) ) array join array_security_id as security_id_final, array_volumes as volumes_final
}


/// this version is slower (2x) but more robust.
///
/// Clickhouse doesn't keep nulls with groupArray right now, which means that you can't
/// pivot the whole result including measures, when there's nulls in measures.
///
/// It's a lot less likely for dims to have nulls (in clean data, should be impossible),
/// so doing the pivoting with only the columns needed for conditional cut (excluding
/// the measures esp) steps around that limitation.
///
/// However, it requires 2 fact table scans, one for the fact table for results, and jne of the fact table
/// to do the conditional (pivoting just from cut col and conditional col, and filtering with
/// hasAny).
pub fn conditional_cut_robust
{
//select owner_object_id, security_id, sum(volume) from sos_cube where owner_object_id in (select
//owner_object_id from (select owner_object_id, groupArray(security_id) as array_security_id from
//(select owner_object_id, security_id from sos_cube where date like '2016-09-09%') group by
//owner_object_id) where hasAny(array_security_id, [20058994]) ) and date like '2016-09-09%' group
//by owner_object_id, security_id
}


struct ConditionalCutSql {
    cut_col: String, // This only needs to be the col for the dimension level that's being cut on
    condition_foreign_key: String,
    condition_col: String,
    include_members: Vec<String>,
    exclude_members: Vec<String>,
    robust_strategy: bool,
}

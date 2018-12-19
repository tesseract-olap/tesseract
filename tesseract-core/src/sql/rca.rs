use crate::sql::primary_agg::primary_agg;
use super::{
    TableSql,
    CutSql,
    DrilldownSql,
    MeasureSql,
    RcaSql,
};

pub fn calculate(
    table: &TableSql,
    cuts: &[CutSql],
    drills: &[DrilldownSql],
    meas: &[MeasureSql],
    rca: &RcaSql,
    ) -> (String, String)
{
    // append the correct rca drill to drilldowns
    // for a, both
    // for b, d2
    // for c, d1
    // for d, none
    let mut a_drills = drills.to_vec();
    let mut b_drills = drills.to_vec();
    let mut c_drills = drills.to_vec();
    let     d_drills = drills.to_vec();

    a_drills.extend_from_slice(&rca.drill_1);
    a_drills.extend_from_slice(&rca.drill_2);

    b_drills.extend_from_slice(&rca.drill_2);

    c_drills.extend_from_slice(&rca.drill_1);

    // prepend the rca sql to meas
    let all_meas = {
        let mut temp = vec![rca.mea.clone()];
        temp.extend_from_slice(meas);
        temp
    };

    let (a, a_final_drills) = primary_agg(table, cuts, &a_drills, &all_meas);
    let (b, b_final_drills) = primary_agg(table, cuts, &b_drills, &all_meas);
    let (c, c_final_drills) = primary_agg(table, cuts, &c_drills, &all_meas);
    let (d, d_final_drills) = primary_agg(table, cuts, &d_drills, &all_meas);

    // replace final_m0 with letter name.
    // I put the rca measure at the beginning of the drills, so it should
    // always be m0
    let a = a.replace("final_m0", "a");
    let b = b.replace("final_m0", "b");
    let c = c.replace("final_m0", "c");
    let d = d.replace("final_m0", "d");

    // now do the final join

    let mut final_sql = format!("select * from ({} all inner join {}) using {}",
        a,
        b,
        b_final_drills,
    );

    final_sql = format!("select * from ({} all inner join {}) using {}",
        c,
        final_sql,
        c_final_drills
    );

    final_sql = format!("select * from ({} all inner join {}) using {}",
        d,
        final_sql,
        d_final_drills,
    );

    final_sql = format!("select {}, ((a/b) / (c/d)) as rca from ({})",
        a_final_drills,
        final_sql,
    );

    (final_sql, a_final_drills)
}

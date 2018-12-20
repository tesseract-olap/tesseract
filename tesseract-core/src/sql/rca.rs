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

    println!("a: {:?}", a_drills);
    println!("b: {:?}", b_drills);
    println!("c: {:?}", c_drills);
    println!("d: {:?}", d_drills);

    // prepend the rca sql to meas
    let all_meas = {
        let mut temp = vec![rca.mea.clone()];
        temp.extend_from_slice(meas);
        temp
    };

    // for cuts,
    // - a can be cut (it's all members)
    // - b can be cut for d2 (no d1)
    // - c can be cut for d1 (no d2)
    // - d cannot be cut on d1 or d2

    let mut b_drill_keys_blacklist = rca.drill_1.iter()
        .flat_map(|d| d.level_columns.iter().map(|l| l.key_column.clone()));

    let mut c_drill_keys_blacklist = rca.drill_2.iter()
        .flat_map(|d| d.level_columns.iter().map(|l| l.key_column.clone()));

    let mut d_drill_keys_blacklist = rca.drill_1.iter().chain(rca.drill_2.iter())
        .flat_map(|d| d.level_columns.iter().map(|l| l.key_column.clone()));

    let b_cuts: Vec<_> = cuts.iter()
        .filter(|cut| {
            b_drill_keys_blacklist.find(|k| *k == cut.column).is_none()
        })
        .cloned()
        .collect();

    let c_cuts: Vec<_> = cuts.iter()
        .filter(|cut| {
            c_drill_keys_blacklist.find(|k| *k == cut.column).is_none()
        })
        .cloned()
        .collect();

    let d_cuts: Vec<_> = cuts.iter()
        .filter(|cut| {
            d_drill_keys_blacklist.find(|k| *k == cut.column).is_none()
        })
        .cloned()
        .collect();


    let (a, a_final_drills) = primary_agg(table,   &cuts, &a_drills, &all_meas);
    let (b, b_final_drills) = primary_agg(table, &b_cuts, &b_drills, &all_meas);
    let (c, c_final_drills) = primary_agg(table, &c_cuts, &c_drills, &all_meas);
    let (d, d_final_drills) = primary_agg(table, &d_cuts, &d_drills, &all_meas);


    // replace final_m0 with letter name.
    // I put the rca measure at the beginning of the drills, so it should
    // always be m0
    let a = a.replace("final_m0", "a");
    let b = b.replace("final_m0", "b");
    let c = c.replace("final_m0", "c");
    let d = d.replace("final_m0", "d");

    // now do the final join

    let mut final_sql = format!("select * from ({}) all inner join ({}) using {}",
        a,
        b,
        b_final_drills,
    );

    final_sql = format!("select * from ({}) all inner join ({}) using {}",
        c,
        final_sql,
        c_final_drills
    );

    final_sql = format!("select * from ({}) all inner join ({}) using {}",
        d,
        final_sql,
        d_final_drills,
    );

    final_sql = format!("select {}, ((a/b) / (c/d)) as rca from ({})",
        a_final_drills,
        final_sql,
    );

    // SPECIAL CASE
    // Hack to deal with no drills on d
    // Later, make this better
    final_sql = final_sql.replace("select , ", "select ");
    final_sql = final_sql.replace("group by )", ")");


    (final_sql, a_final_drills)
}

#[cfg(test)]
mod test {
    use super::*;
    use super::super::*;

    #[test]
    fn test_rca_sql() {
        let table = TableSql {
            name: "sales".into(),
            primary_key: Some("product_id".into()),
        };
        //let cuts = vec![
        //    CutSql {
        //        foreign_key: "product_id".into(),
        //        primary_key: "product_id".into(),
        //        table: Table { name: "dim_products".into(), schema: None, primary_key: None },
        //        column: "product_group_id".into(),
        //        members: vec!["3".into()],
        //        member_type: MemberType::NonText,
        //    },
        //];
        //let drills = vec![
        //    // this dim is inline, so should use the fact table
        //    // also has parents, so has 
        //    DrilldownSql {
        //        foreign_key: "date_id".into(),
        //        primary_key: "date_id".into(),
        //        table: Table { name: "sales".into(), schema: None, primary_key: None },
        //        level_columns: vec![
        //            LevelColumn {
        //                key_column: "year".into(),
        //                name_column: None,
        //            },
        //            LevelColumn {
        //                key_column: "month".into(),
        //                name_column: None,
        //            },
        //            LevelColumn {
        //                key_column: "day".into(),
        //                name_column: None,
        //            },
        //        ],
        //        property_columns: vec![],
        //    },
        //    // this comes second, but should join first because of primary key match
        //    // on fact table
        //    DrilldownSql {
        //        foreign_key: "product_id".into(),
        //        primary_key: "product_id".into(),
        //        table: Table { name: "dim_products".into(), schema: None, primary_key: None },
        //        level_columns: vec![
        //            LevelColumn {
        //                key_column: "product_group_id".into(),
        //                name_column: Some("product_group_label".into()),
        //            },
        //            LevelColumn {
        //                key_column: "product_id_raw".into(),
        //                name_column: Some("product_label".into()),
        //            },
        //        ],
        //        property_columns: vec![],
        //    },
        //];
        //let meas = vec![
        //    MeasureSql { aggregator: "sum".into(), column: "quantity".into() }
        //];

        let drill_1 = vec![DrilldownSql {
            foreign_key: "date_id".into(),
            primary_key: "date_id".into(),
            table: Table { name: "sales".into(), schema: None, primary_key: None },
            level_columns: vec![
                LevelColumn {
                    key_column: "year".into(),
                    name_column: None,
                },
                LevelColumn {
                    key_column: "month".into(),
                    name_column: None,
                },
                LevelColumn {
                    key_column: "day".into(),
                    name_column: None,
                },
            ],
            property_columns: vec![],
        }];

        let drill_2 = vec![DrilldownSql {
            foreign_key: "product_id".into(),
            primary_key: "product_id".into(),
            table: Table { name: "dim_products".into(), schema: None, primary_key: None },
            level_columns: vec![
                LevelColumn {
                    key_column: "product_group_id".into(),
                    name_column: Some("product_group_label".into()),
                },
                LevelColumn {
                    key_column: "product_id_raw".into(),
                    name_column: Some("product_label".into()),
                },
            ],
            property_columns: vec![],
        }];

        let mea = MeasureSql { aggregator: "sum".into(), column: "quantity".into() };

        let rca = RcaSql {
            drill_1,
            drill_2,
            mea,
        };

        assert_eq!(
            clickhouse_sql(&table, &[], &[], &[], &None, &None, &None, &Some(rca)),
            "".to_owned()
        );
    }
}

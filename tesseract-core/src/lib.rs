mod backend;
mod dataframe;
pub mod format;
pub mod names;
mod schema;
mod sql;
mod query;

use failure::{Error, format_err, bail};

use crate::schema::{
    SchemaConfigJSON,
    SchemaConfigXML}
;

pub use self::backend::Backend;
pub use self::dataframe::{DataFrame, Column, ColumnData};
use self::names::{
    Cut,
    Drilldown,
    Measure,
    Property,
    LevelName,
};
pub use self::schema::{Schema, Cube};
use self::sql::{
    CutSql,
    DrilldownSql,
    MeasureSql,
    MemberType,
    TableSql,
    LevelColumn,
    TopSql,
    SortSql,
    RcaSql,
    GrowthSql,
};
pub use self::sql::SqlType;
pub use self::query::{Query, MeaOrCalc};

extern crate serde_xml_rs as serde_xml;


impl Schema {
    pub fn from_json(raw_schema: &str) -> Result<Self, Error> {
        let schema_config = serde_json::from_str::<SchemaConfigJSON>(raw_schema)?;
        Ok(schema_config.into())
    }

    pub fn from_xml(raw_schema: &str) -> Result<Self, Error> {
        let schema_config: SchemaConfigXML = serde_xml::deserialize(raw_schema.as_bytes())?;
        // Serialize XML to JSON as intermediary step
        let serialized = serde_json::to_string(&schema_config).unwrap();
        Schema::from_json(&serialized)
    }

    pub fn cube_metadata(&self, cube_name: &str) -> Option<Cube> {
        // Takes the first cube with the name.
        // TODO we still have to check that the cube names are distinct
        // before this.
        self.cubes.iter().find(|c| c.name == cube_name).cloned()
    }

    pub fn sql_query(
        &self,
        cube: &str,
        query: &Query,
        sql_type: SqlType,
        ) -> Result<(String, Vec<String>), Error>
    {
        // First do checks, like making sure there's a measure, and that there's
        // either a cut or drilldown
        if query.measures.is_empty() && query.rca.is_none() {
            return Err(format_err!("No measure found; please specify at least one"));
        }
        if query.drilldowns.is_empty() && query.cuts.is_empty(){
            return Err(format_err!("Either a drilldown or cut is required"));
        }
        // also check that properties have a matching drilldown
        if let Some(ref rca) = query.rca {
            let rca_drills = [&rca.drill_1, &rca.drill_2];
            for property in &query.properties {
                let has_drill = rca_drills.iter()
                    .any(|d| d.0 == property.level_name);

                if !has_drill {
                    return Err(format_err!("Property {} has no matching drilldown", property));
                }
            }
        } else {
            for property in &query.properties {
                let has_drill = query.drilldowns.iter()
                    .any(|d| d.0 == property.level_name);

                if !has_drill {
                    return Err(format_err!("Property {} has no matching drilldown", property));
                }
            }
        }

        // for growth, check if time dim and mea are in drilldown and measures
        if let Some(ref growth) = query.growth {
            if !query.drilldowns.contains(&growth.time_drill) {
                bail!("Growth time drilldown {} is not in drilldowns", growth.time_drill);
            }
            if !query.measures.contains(&growth.mea) {
                bail!("Growth measure {} is not in measures", growth.mea);
            }
        }

        // for rca, disallow cuts on the second drilldown for now, until better system
        // is figured out.
        // There is internal filtering of cuts internally also, which should follow the
        // pattern of the check here.
        if let Some(ref rca) = query.rca {
            let cuts_contain_drill_2 = query.cuts.iter()
                .any(|c| c.level_name == rca.drill_2.0);

            if cuts_contain_drill_2 {
                return Err(format_err!("Cut on rca drill 2 is not allowed; for rca, \
                    only external cuts and cuts on drill 1 allowed", ));
            }
        }

        // now get the database metadata
        let table = self.cube_table(&cube)
            .ok_or(format_err!("No table found for cube {}", cube))?;

        let cut_cols = self.cube_cut_cols(&cube, &query.cuts)
            .map_err(|err| format_err!("Error getting cut cols: {}", err))?;

        let drill_cols = self.cube_drill_cols(&cube, &query.drilldowns, &query.properties, query.parents)
            .map_err(|err| format_err!("Error getting drill cols: {}", err))?;

        let mea_cols = self.cube_mea_cols(&cube, &query.measures)
            .map_err(|err| format_err!("Error getting mea cols: {}", err))?;

        // Options for sorting and limiting

        let limit = query.limit.clone().map(|l| l.into());

        let top = if let Some(ref t) = query.top {
            // don't want the actual measure column,
            // want the index so that we can use `m0` etc.
            let top_sort_columns: Result<Vec<_>, _> = t.sort_mea_or_calc.iter()
                .map(|m_or_c| {
                    match m_or_c {
                        MeaOrCalc::Mea(m) => {
                            // NOTE: rca mea does not return the actual value, only rca. Since
                            // mea value must be retrieved through explicit drilldown,
                            // don't need to do an extra rca check here.

                            query.measures.iter()
                                .position(|col| col == m )
                                .map(|idx| format!("final_m{}", idx))
                                .ok_or(format_err!("measure {} for Top must be in measures", m))
                        },
                        MeaOrCalc::Calc(c) => {
                            Ok(c.sql_string())
                        }
                    }
                })
                .collect();
            let top_sort_columns = top_sort_columns?;

            // check that by_dimension is in query.drilldowns
            // TODO check for rca drills too
            if let Some(ref rca) = query.rca {
                let mut d = query.drilldowns.clone();
                d.extend_from_slice(&[rca.drill_1.clone(), rca.drill_2.clone()]);

                d.iter()
                    .map(|d| &d.0)
                    .find(|name| **name == t.by_dimension)
                    .ok_or(format_err!("Top by_dimension must be in drilldowns (including rca)"))?;
            } else {
                query.drilldowns.iter()
                    .map(|d| &d.0)
                    .find(|name| **name == t.by_dimension)
                    .ok_or(format_err!("Top by_dimension must be in drilldowns"))?;
            }

            Some(TopSql {
                n: t.n,
                by_column: self.get_dim_col_alias(&cube, &t.by_dimension)?,
                sort_columns: top_sort_columns,
                sort_direction: t.sort_direction.clone(),
            })
        } else {
            None
        };

        let sort = if let Some(ref s) = query.sort {
            let sort_column = self.get_mea_col(&cube, &s.measure)?;

            Some(SortSql {
                direction: s.direction.clone(),
                column: sort_column,
            })
        } else {
            None
        };

        // TODO check that no overlapping dim or mea cols between rca and others
        let rca = if let Some(ref rca) = query.rca {
            let drill_1 = self.cube_drill_cols(&cube, &[rca.drill_1.clone()], &query.properties, query.parents)?;
            let drill_2 = self.cube_drill_cols(&cube, &[rca.drill_2.clone()], &query.properties, query.parents)?;

            let mea = self.cube_mea_cols(&cube, &[rca.mea.clone()])?
                .get(0)
                .ok_or(format_err!("no measure found for rca"))?
                .clone();

            Some(RcaSql {
                drill_1,
                drill_2,
                mea,
            })
        } else {
            None
        };

        let growth = if let Some(ref growth) = query.growth {
            let time_drill = self.cube_drill_cols(&cube, &[growth.time_drill.clone()], &query.properties, query.parents)?
                .get(0)
                .ok_or(format_err!("no measure found for growth"))?
                .clone();

            // just want the measure id, not the actual measure col
            let mea = query.measures.iter()
                    .position(|mea| *mea == growth.mea )
                    .map(|idx| format!("final_m{}", idx))
                    .ok_or(format_err!("measure for Growth must be in measures"))?;

            Some(GrowthSql {
                time_drill,
                mea,
            })
        } else {
            None
        };

        // getting headers, not for sql but needed for formatting
        let mut drill_headers = self.cube_drill_headers(&cube, &query.drilldowns, &query.properties, query.parents)
            .map_err(|err| format_err!("Error getting drill headers: {}", err))?;

        let mut mea_headers = self.cube_mea_headers(&cube, &query.measures)
            .map_err(|err| format_err!("Error getting mea headers: {}", err))?;

        // rca mea will always be first, so just put
        // in `Mea RCA` second
        if let Some(ref rca) = query.rca {
            let rca_drill_headers = self.cube_drill_headers(&cube, &[rca.drill_1.clone(), rca.drill_2.clone()], &query.properties, query.parents)
                .map_err(|err| format_err!("Error getting rca drill headers: {}", err))?;
            drill_headers.extend_from_slice(&rca_drill_headers);

            mea_headers.insert(0, format!("{} RCA", rca.mea.0.clone()));
        }


        // Be careful with other calculations. TODO figure out a more composable system.
        let headers = if let Some(ref growth) = query.growth {
            // swapping around measure headers. growth mea moves to back.
            let g_mea_idx = query.measures.iter()
                    .position(|mea| *mea == growth.mea )
                    .ok_or(format_err!("measure for Growth must be in measures"))?;

            let moved_mea = mea_headers.remove(g_mea_idx);
            mea_headers.push(moved_mea);
            mea_headers.push(format!("{} Growth", growth.mea.0));
            mea_headers.push(format!("{} Growth Value", growth.mea.0));

            // swapping around drilldown headers. Move time to back
            let time_headers = self.cube_drill_headers(&cube, &[growth.time_drill.clone()], &[], query.parents)
                .map_err(|err| format_err!("Error getting time drill headers for Growth: {}", err))?;

            let time_header_idxs: Result<Vec<_>,_> = time_headers.iter()
                .map(|th| {
                    drill_headers.iter()
                        .position(|h| h == th)
                        .ok_or(format_err!("Growth, cannot find time header {} in drill headers", th))
                })
                .collect();
            let time_header_idxs = time_header_idxs?;

            // TODO figure out a better way to move headers
            let mut temp_time_headers = vec![];
            for idx in time_header_idxs.iter().rev() {
                let moved_hdr = drill_headers.remove(*idx);
                temp_time_headers.insert(0, moved_hdr);
            }
            drill_headers.extend_from_slice(&temp_time_headers);

            [&drill_headers[..], &mea_headers[..]].concat()
        } else {
            [&drill_headers[..], &mea_headers[..]].concat()
        };


        // now feed the database metadata into the sql generator
        match sql_type {
            SqlType::Clickhouse => {
                Ok((
                    sql::clickhouse_sql(
                    &table,
                    &cut_cols,
                    &drill_cols,
                    &mea_cols,
                    &top,
                    &sort,
                    &limit,
                    &rca,
                    &growth,
                    ),
                    headers,
                ))
            },
            SqlType::Standard => {
                Ok((
                    sql::standard_sql(
                    &table,
                    &cut_cols,
                    &drill_cols,
                    &mea_cols,
                    &top,
                    &sort,
                    &limit,
                    &rca,
                    &growth,
                    ),
                    headers,
                ))
            },
        }
    }

    //pub fn post_calculations(cal: &Calculations, df: DataFrame) -> DataFrame {
    //}
}

impl Schema {
    fn cube_table(&self, cube_name: &str) -> Option<TableSql> {
        self.cubes.iter()
            .find(|cube| &cube.name == &cube_name)
            .map(|cube| {
                TableSql {
                    name: cube.table.name.clone(),
                    primary_key: cube.table.primary_key.clone(),
                }
            })
    }

    fn cube_cut_cols(&self, cube_name: &str, cuts: &[Cut]) -> Result<Vec<CutSql>, Error> {
        let cube = self.cubes.iter()
            .find(|cube| &cube.name == &cube_name)
            .ok_or(format_err!("Could not find cube"))?;

        let mut res = vec![];

        for cut in cuts {
            let dim = cube.dimensions.iter()
                .find(|dim| dim.name == cut.level_name.dimension)
                .ok_or(format_err!("could not find dimension for cut {}", cut.level_name))?;
            let hier = dim.hierarchies.iter()
                .find(|hier| hier.name == cut.level_name.hierarchy)
                .ok_or(format_err!("could not find hierarchy for cut {}", cut.level_name))?;
            let level = hier.levels.iter()
                .find(|lvl| lvl.name == cut.level_name.level)
                .ok_or(format_err!("could not find level for cut {}", cut.level_name))?;

            // No table (means inline table) will replace with fact table
            let table = hier.table
                .clone()
                .unwrap_or(cube.table.clone());

            // primary key is currently required in hierarchy. because inline dim is not yet
            // allowed
            let primary_key = hier.primary_key.clone();

            let foreign_key = dim.foreign_key
                .clone()
                .ok_or(format_err!("No foreign key; it's required for now (until inline dim implemented)"))?;

            let column = level.key_column.clone();

            res.push(CutSql {
                table,
                primary_key,
                foreign_key,
                column,
                members: cut.members.clone(),
                member_type: level.key_type.clone().unwrap_or(MemberType::NonText),
            });
        }

        Ok(res)
    }

    // TODO as currently written, properties that don't get picked up by a drilldown
    // will just silently fail.
    fn cube_drill_cols(
        &self,
        cube_name: &str,
        drills: &[Drilldown],
        properties: &[Property],
        parents: bool,
        ) -> Result<Vec<DrilldownSql>, Error>
    {
        let cube = self.cubes.iter()
            .find(|cube| &cube.name == &cube_name)
            .ok_or(format_err!("Could not find cube"))?;

        let mut res = vec![];

        // now iterate throw drill/property tuples
        for drill in drills {
            let dim = cube.dimensions.iter()
                .find(|dim| dim.name == drill.0.dimension)
                .ok_or(format_err!("could not find dimension for drill {}", drill.0))?;
            let hier = dim.hierarchies.iter()
                .find(|hier| hier.name == drill.0.hierarchy)
                .ok_or(format_err!("could not find hierarchy for drill {}", drill.0))?;
            let levels = &hier.levels;

            // for this drill, get related properties.
            // - filter by properties for this drilldown
            // - for each property, get the level
            let property_columns: Result<Vec<_>, _>= properties.iter()
                .filter(|p| p.level_name == drill.0)
                .map(|p| {
                    levels.iter()
                        .find(|lvl| lvl.name == p.level_name.level)
                        .and_then(|lvl| {
                            if let Some(ref properties) = lvl.properties {
                                properties.iter()
                                    .find(|schema_p| schema_p.name == p.property)

                            } else {
                                None
                            }
                        })
                        .map(|p| p.column.clone())
                        .ok_or(format_err!("cannot find property for {}", p))
                })
                .collect();
            let property_columns = property_columns?;

            // No table (means inline table) will replace with fact table
            let table = hier.table
                .clone()
                .unwrap_or(cube.table.clone());

            // primary key is currently required in hierarchy. because inline dim is not yet
            // allowed
            let primary_key = hier.primary_key.clone();

            let foreign_key = dim.foreign_key
                .clone()
                .ok_or(format_err!("No foreign key; it's required for now (until inline dim implemented)"))?;

            // logic for getting level columns.
            // if parents = true, then get all columns down to level
            // if not,then just level
            let level_idx = levels.iter()
                .position(|lvl| lvl.name == drill.0.level)
                .ok_or(format_err!("could not find level for drill {}", drill.0))?;

            let mut level_columns = vec![];

            if parents {
                for i in 0..=level_idx {
                    level_columns.push(LevelColumn {
                        key_column: levels[i].key_column.clone(),
                        name_column: levels[i].name_column.clone(),
                    });
                }
            } else {
                level_columns.push(LevelColumn {
                    key_column: levels[level_idx].key_column.clone(),
                    name_column: levels[level_idx].name_column.clone(),
                });
            }

            let alias_postfix = dim.name.replace(" ", "_");

            res.push(DrilldownSql {
                alias_postfix,
                table,
                primary_key,
                foreign_key,
                level_columns,
                property_columns,
            });
        }

        Ok(res)
    }

    fn cube_mea_cols(&self, cube_name: &str, meas: &[Measure]) -> Result<Vec<MeasureSql>, Error> {
        let cube = self.cubes.iter()
            .find(|cube| &cube.name == &cube_name)
            .ok_or(format_err!("Could not find cube"))?;

        let mut res = vec![];

        for measure in meas {
            let mea = cube.measures.iter()
                .find(|m| m.name == measure.0)
                .ok_or(format_err!("could not find measure for {}", measure.0))?;

            res.push(MeasureSql {
                column: mea.column.clone(),
                aggregator: mea.aggregator.clone(),
            });
        }

        Ok(res)
    }

    /// order should mirror DrillSql col_string,
    /// which should be levels first and then properties after
    /// (for each drilldown)
    fn cube_drill_headers(
        &self,
        cube_name: &str,
        drills: &[Drilldown],
        properties: &[Property],
        parents: bool,
        ) -> Result<Vec<String>, Error>
    {
        let cube = self.cubes.iter()
            .find(|cube| &cube.name == &cube_name)
            .ok_or(format_err!("Could not find cube"))?;

        let mut level_headers = vec![];

        for drill in drills {
            let dim = cube.dimensions.iter()
                .find(|dim| dim.name == drill.0.dimension)
                .ok_or(format_err!("could not find dimension for drill"))?;
            let hier = dim.hierarchies.iter()
                .find(|hier| hier.name == drill.0.hierarchy)
                .ok_or(format_err!("could not find hierarchy for drill"))?;
            let levels = &hier.levels;

            // logic for getting level names.
            // if parents = true, then get all columns down to level
            // if not,then just level name
            let level_idx = hier.levels.iter()
                .position(|lvl| lvl.name == drill.0.level)
                .ok_or(format_err!("could not find hierarchy for drill"))?;


            // In this section, need to watch out for whether there's both a
            // key column and a name column and add ID to the first if necessary
            if parents {
                for i in 0..=level_idx {
                    if levels[i].name_column.is_some() {
                        level_headers.push(levels[i].name.clone() + " ID");
                    }
                    level_headers.push(levels[i].name.clone());
                }
            } else {
                if levels[level_idx].name_column.is_some() {
                    level_headers.push(levels[level_idx].name.clone() + " ID");
                }
                level_headers.push(levels[level_idx].name.clone());
            }

            // for this drill, get related properties.
            // - filter by properties for this drilldown
            // - for each property, get the level
            let property_columns: Result<Vec<_>, _>= properties.iter()
                .filter(|p| p.level_name == drill.0)
                .map(|p| {
                    levels.iter()
                        .find(|lvl| lvl.name == p.level_name.level)
                        .and_then(|lvl| {
                            if let Some(ref properties) = lvl.properties {
                                properties.iter()
                                    .find(|schema_p| schema_p.name == p.property)

                            } else {
                                None
                            }
                        })
                        .map(|p| p.name.clone())
                        .ok_or(format_err!("cannot find property for {}", p))
                })
                .collect();
            let property_columns = property_columns?;

            level_headers.extend(property_columns);
        }

        Ok(level_headers)
    }

    fn cube_mea_headers(&self, cube_name: &str, meas: &[Measure]) -> Result<Vec<String>, Error> {
        let cube = self.cubes.iter()
            .find(|cube| &cube.name == &cube_name)
            .ok_or(format_err!("Could not find cube"))?;

        let mut res = vec![];

        for measure in meas {
            let mea = cube.measures.iter()
                .find(|m| m.name == measure.0)
                .ok_or(format_err!("could not find measure in cube"))?;

            res.push(mea.name.clone());
        }

        Ok(res)
    }

    fn get_dim_col(&self, cube_name: &str, level_name: &LevelName) -> Result<String, Error> {
        let cube = self.cubes.iter()
            .find(|cube| &cube.name == &cube_name)
            .ok_or(format_err!("Could not find cube"))?;

        let dim = cube.dimensions.iter()
            .find(|dim| dim.name == level_name.dimension)
            .ok_or(format_err!("could not find dimension for level name"))?;
        let hier = dim.hierarchies.iter()
            .find(|hier| hier.name == level_name.hierarchy)
            .ok_or(format_err!("could not find hierarchy for level name"))?;
        let level = hier.levels.iter()
            .find(|lvl| lvl.name == level_name.level)
            .ok_or(format_err!("could not find level for level name"))?;

        let column = level.key_column.clone();

        Ok(column)
    }

    fn get_dim_col_alias(&self, cube_name: &str, level_name: &LevelName) -> Result<String, Error> {
        let cube = self.cubes.iter()
            .find(|cube| &cube.name == &cube_name)
            .ok_or(format_err!("Could not find cube"))?;

        let dim = cube.dimensions.iter()
            .find(|dim| dim.name == level_name.dimension)
            .ok_or(format_err!("could not find dimension for level name"))?;
        let hier = dim.hierarchies.iter()
            .find(|hier| hier.name == level_name.hierarchy)
            .ok_or(format_err!("could not find hierarchy for level name"))?;
        let level = hier.levels.iter()
            .find(|lvl| lvl.name == level_name.level)
            .ok_or(format_err!("could not find level for level name"))?;

        // TODO centralize where to get the alias
        let column = format!("{}_{}", level.key_column, dim.name.replace(" ", "_"));

        Ok(column)
    }

    fn get_mea_col(&self, cube_name: &str, measure: &Measure) -> Result<String, Error> {
        let cube = self.cubes.iter()
            .find(|cube| &cube.name == &cube_name)
            .ok_or(format_err!("Could not find cube"))?;

        let mea = cube.measures.iter()
            .find(|m| m.name == measure.0)
            .ok_or(format_err!("could not find level for level name"))?;

        let column = mea.column.clone();

        Ok(column)
    }
}


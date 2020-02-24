use std::collections::HashMap;
use std::str;

use actix_web::{
    HttpRequest,
    HttpResponse,
    Path,
    Result as ActixResult,
};
use failure::{Error, format_err};
use futures::future::Future;
use lazy_static::lazy_static;
use log::*;
use serde_qs as qs;
use serde_derive::Deserialize;
use url::Url;

use tesseract_core::names::{Property, LevelName};
use tesseract_core::format::{format_records, FormatType};
use tesseract_core::{DataFrame, Column, ColumnData};
use tesseract_core::schema::{Cube, DimensionType, Level};
use crate::app::AppState;
use crate::logic_layer::{LogicLayerConfig, CubeCache};
use crate::handlers::util::{verify_api_key, format_to_content_type};
use crate::handlers::logic_layer::{query_geoservice, GeoserviceQuery};


/// Handles default aggregation when a format is not specified.
/// Default format is jsonrecords.
pub fn diagnosis_default_handler(
    (req, _cube): (HttpRequest<AppState>, Path<()>)
) -> ActixResult<HttpResponse>
{
    perform_diagnosis(req, "jsonrecords".to_owned())
}


/// Handles aggregation when a format is specified.
pub fn diagnosis_handler(
    (req, cube_format): (HttpRequest<AppState>, Path<(String)>)
) -> ActixResult<HttpResponse>
{
    perform_diagnosis(req, cube_format.to_owned())
}


#[derive(Debug, Clone, Deserialize)]
pub struct DiagnosisQueryOpt {
    pub cube: String,
}


pub fn perform_diagnosis(
    req: HttpRequest<AppState>,
    format: String,
) -> ActixResult<HttpResponse>
{
    let format = format.parse::<FormatType>();
    let format = match format {
        Ok(f) => f,
        Err(err) =>return Ok(HttpResponse::NotFound().json(err.to_string())),
    };

    info!("Format: {:?}", format);

    let query = req.query_string();
    let schema = req.state().schema.read().unwrap();
    let _debug = req.state().debug;

    lazy_static! {
        static ref QS_NON_STRICT: qs::Config = qs::Config::new(5, false);
    }

    let query_opt = match QS_NON_STRICT.deserialize_str::<DiagnosisQueryOpt>(query) {
        Ok(q) => q,
        Err(err) => return Ok(HttpResponse::NotFound().json(err.to_string()))
    };

    let cube = match schema.get_cube_by_name(&query_opt.cube) {
        Ok(c) => c,
        Err(err) => return Ok(HttpResponse::NotFound().json(err.to_string()))
    };

    match verify_api_key(&req, &cube) {
        Ok(_) => (),
        Err(err) => return Ok(err)
    }

    let mut error_types: Vec<String> = vec![];
    let mut error_messages: Vec<String> = vec![];

    // Check for `MissingDimensionIDs`
    // TODO: Deal with the case where there is an inline table.
    for dimension in &cube.dimensions {
        for hierarchy in &dimension.hierarchies {
            let last_level: &Level = &hierarchy.levels[hierarchy.levels.len() - 1];

            if let Some(ref foreign_key) = dimension.foreign_key {
                if let Some(ref dimension_table) = hierarchy.table {
                    let sql_str: String = format!(
                        "SELECT DISTINCT {} FROM {} WHERE {} NOT IN (SELECT {} FROM {})",
                        foreign_key,
                        cube.table.name,
                        foreign_key,
                        last_level.key_column,
                        dimension_table.name,
                    );

                    let res_df = req.state().backend
                        .exec_sql(sql_str)
                        .wait()
                        .and_then(move |df| {
                            Ok(df)
                        });

                    match res_df {
                        Ok(res_df) => {
                            match res_df.columns.get(0) {
                                Some(column) => {
                                    // This is used here as a hack to extract the data inside
                                    // the enum as a vector instead of using a match on every
                                    // single data type supported.
                                    let column_data = column.stringify_column_data();

                                    if column_data.len() > 0 {
                                        error_types.push("MissingDimensionIDs".to_string());
                                        error_messages.push(
                                            format!(
                                                "The following IDs for [{}].[{}].[{}] are not present in its dimension table: {}.",
                                                dimension.name,
                                                hierarchy.name,
                                                last_level.name,
                                                column_data.join(", ")
                                            )
                                        );
                                    }
                                },
                                None => ()
                            }
                        },
                        Err(err) => ()
                    }
                }
            }
        }
    }

    if error_messages.len() == 0 {
        Ok(HttpResponse::Ok().json("Success.".to_string()))
    } else {
        let df = DataFrame { columns: vec![
            Column {
                name: "type".to_string(),
                column_data: ColumnData::Text(error_types)
            },
            Column {
                name: "message".to_string(),
                column_data: ColumnData::Text(error_messages)
            }
        ] };

        let headers = vec!["Error Type".to_string(), "Error Message".to_string()];
        let content_type = format_to_content_type(&format);

        match format_records(&headers, df, format, None) {
            Ok(res) => {
                Ok(HttpResponse::ExpectationFailed()
                    .set(content_type)
                    .body(res))
            },
            Err(err) => Ok(HttpResponse::NotFound().json(err.to_string())),
        }
    }
}

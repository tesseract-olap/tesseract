use std::str;

use actix_web::{
    client,
    HttpMessage,
};
use failure::{Error, format_err};
use futures::future::Future;
use serde_derive::Deserialize;
use url::Url;


#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct GeoServiceResponseJson {
    pub geoid: String,
    pub level: String,
}


pub enum GeoserviceQuery {
    Neighbors,
    Children,
    Parents,
    Intersects,
    Distance,
}


/// Queries geoservice for geo cuts resolution.
pub fn query_geoservice(
    geoservice_url: &str,
    geoservice_query: &GeoserviceQuery,
    geo_id: &str
) -> Result<Vec<GeoServiceResponseJson>, Error> {
    let base_url = Url::parse(geoservice_url.clone()).unwrap();

    let join_str = match geoservice_query {
        GeoserviceQuery::Neighbors => format!("{}neighbors/{}", geoservice_url, geo_id),
        GeoserviceQuery::Children => format!("{}relations/children/{}", geoservice_url, geo_id),
        GeoserviceQuery::Parents => format!("{}relations/parents/{}", geoservice_url, geo_id),
        _ => return Err(format_err!("This type of geoservice query is not yet supported"))
    };

    let query_url = base_url.join(&join_str).unwrap().as_str().to_string();

    let result: Result<Vec<GeoServiceResponseJson>, Result<(), Error>> = client::get(query_url)
        .header("User-Agent", "Actix-web")
        .finish()
        .unwrap()
        .send()
        .map_err(|err| {
            Err(format_err!("{}", err.to_string()))
        })
        .and_then(|response| {
            response.body()
                .and_then(|body| {
                    let body = str::from_utf8(&body).unwrap();
                    let data: Vec<GeoServiceResponseJson> = serde_json::from_str(body).unwrap();
                    Ok(data)
                })
                .map_err(|err| {
                    Err(format_err!("{}", err.to_string()))
                })
        })
        .wait();

    match result {
        Ok(data) => Ok(data),
        Err(err) => {
            match err {
                Ok(_) => Err(format_err!("No data returned from geoservice")),
                Err(err) => Err(format_err!("{}", err.to_string()))
            }
        }
    }
}

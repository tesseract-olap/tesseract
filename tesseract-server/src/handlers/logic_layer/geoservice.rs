use std::str;

use anyhow::{anyhow, Result};
use actix_web::client::Client;
use serde_derive::Deserialize;
use url::Url;


#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct GeoServiceResponseJson {
    pub geoid: String,
    pub level: String,
}


pub enum GeoserviceQuery {
    Neighbors,
    // Not constructed yet
    //Children,
    //Parents,
    //Intersects,
    //Distance,
}


/// Queries geoservice for geo cuts resolution.
pub async fn query_geoservice(
    base_url: &Url,
    geoservice_query: &GeoserviceQuery,
    geo_id: &str
) -> Result<Vec<GeoServiceResponseJson>> {
    let join_str = match geoservice_query {
        GeoserviceQuery::Neighbors => format!("neighbors/{}", geo_id),
        //GeoserviceQuery::Children => format!("relations/children/{}", geo_id),
        //GeoserviceQuery::Parents => format!("relations/parents/{}", geo_id),
        //_ => bail!("This type of geoservice query is not yet supported")
    };

    let query_url = base_url.join(&join_str).unwrap();

    // TODO put client in AppData
    let client = Client::default();
    let mut resp = client.get(query_url.as_str())
        .insert_header(("User-Agent", "Actix-web"))
        .send()
        .await
        .map_err(|e| anyhow!(e.to_string()))?;

    let data: Vec<GeoServiceResponseJson> = resp.json().await?;

    Ok(data)
}

use std::sync::{Arc, RwLock};
use actix_web::server;
use failure::{Error, format_err};
use log::*;
use std::env;
use tesseract_olap::app::{EnvVars, SchemaSource, create_app};
use tesseract_olap::logic_layer;
use tesseract_olap::{schema_config, db_config};
use std::path::Path;

use actix_web::*;
use std::thread;

static ll_config_str: &str = r##"
{
    "aliases": {
        "cubes": [
            {
                "name": "Sales",
                "alternatives": ["sales", "sale"],
                "levels": [
                    {
                        "current_name": "Category.Category.Category",
                        "unique_name": "Unique Category"
                    }
                ],
                "properties": [
                {
                        "current_name": "Geography.Geography.Continent.Continent PT",
                        "unique_name": "Continent PT"
                }
                ]
            }
        ]
    },
    "named_sets": [
        {
            "level_name": "Category.Category.Category",
            "sets": [
                {
                    "set_name": "Set 1",
                    "values": ["1", "4"]
                },
                {
                    "set_name": "Set 2",
                    "values": ["2", "3"]
                }
            ]
        }
    ]
}
"##;

static schema_str: &str = r##"
<Schema name="Webshop">
    <SharedDimension name="Geography" type="geo">
        <Hierarchy name="Geography">
            <Table name="tesseract_webshop_geographies" />
            <Level name="Continent" key_column="continent_id" name_column="continent_name"
                    key_type="text">
                <Property name="Continent PT" column="continent_name_pt" caption_set="pt" />
                <Property name="Continent ES" column="continent_name_es" caption_set="es" />
            </Level>
            <Level name="Country" key_column="country_id" name_column="country_name"
                    key_type="nontext">
                <Property name="Country PT" column="country_name_pt" caption_set="pt" />
                <Property name="Country ES" column="country_name_es" caption_set="es" />
            </Level>
        </Hierarchy>
    </SharedDimension>
    <Cube name="Sales">
        <Table name="tesseract_webshop_sales" />
        <DimensionUsage foreign_key="country_id" name="Geography" source="Geography" />
        <Dimension name="Year" foreign_key="year">
            <Hierarchy name="Year">
                <Level name="Year" key_column="year" />
            </Hierarchy>
        </Dimension>

        <Dimension name="Month" foreign_key="month_id">
            <Hierarchy name="Month">
                <Table name="tesseract_webshop_time" />

                <Level name="Month" key_column="month_id" name_column="month_name">
                    <Property name="Month PT" column="month_name_pt" caption_set="pt" />
                </Level>
            </Hierarchy>
        </Dimension>

        <Dimension name="Category" foreign_key="category_id">
            <Hierarchy name="Category">
                <InlineTable alias="tesseract_webshop_categories">
                    <ColumnDef name="category_name" key_type="text" />
                    <ColumnDef name="category_name_pt" key_type="text" caption_set="pt" />
                    <ColumnDef name="category_name_es" key_type="text" caption_set="es" />
                    <ColumnDef name="category_idx" key_type="nontext" key_column_type="Int32" />
                    <Row>
                        <Value column="category_name">Books</Value>
                        <Value column="category_name_pt">Livros</Value>
                        <Value column="category_name_es">Libros</Value>
                        <Value column="category_idx">1</Value>
                    </Row>
                    <Row>
                        <Value column="category_name">Sports</Value>
                        <Value column="category_name_pt">Esportes</Value>
                        <Value column="category_name_es">Deportes</Value>
                        <Value column="category_idx">2</Value>
                    </Row>
                    <Row>
                        <Value column="category_name">Various</Value>
                        <Value column="category_name_pt">Vários</Value>
                        <Value column="category_name_es">Varios</Value>
                        <Value column="category_idx">3</Value>
                    </Row>
                    <Row>
                        <Value column="category_name">Videos</Value>
                        <Value column="category_name_pt">Vídeos</Value>
                        <Value column="category_name_es">Videos</Value>
                        <Value column="category_idx">4</Value>
                    </Row>
                </InlineTable>

                <!-- <Level name="Category" key_column="category_id" name_column="category_name" key_type="nontext" /> -->
                <Level name="Category" key_column="category_idx" name_column="category_name" key_type="nontext" />
            </Hierarchy>
        </Dimension>
        <Measure name="Price Total" column="price_total" aggregator="sum" />
        <Measure name="Quantity" column="quantity" aggregator="sum" />
    </Cube>
</Schema>
    "##;

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{test, App};
    use tesseract_core::schema::Schema;
    use std::time;

    #[test]
    fn test_end_to_end() {
        let db_url_full = env::var("TESSERACT_DATABASE_URL").unwrap_or_else(|_| "clickhouse://localhost:9000".into());

        let (db, db_url, db_type) = db_config::get_db(&db_url_full).unwrap();
        let schema_source = SchemaSource::LocalSchema { filepath: "blah".to_string() };

        let env_vars = EnvVars {
            database_url: db_url.clone(),
            geoservice_url: None,
            schema_source,
            api_key: None,
            jwt_secret: None,
            flush_secret: None,
        };

        let mut schema = Schema::from_xml(&schema_str).unwrap();
        schema.validate().expect("failed to validate schema");
        let mut has_unique_levels_properties = schema.has_unique_levels_properties();
        let logic_layer_config = match logic_layer::read_config_str(&ll_config_str) {
            Ok(config_obj) => {
                has_unique_levels_properties = config_obj.has_unique_levels_properties(&schema).unwrap();
                Some(config_obj)
            },
            Err(err) => panic!("ERROR in logic layer")
        };
 
        thread::spawn(move || {
            let mut sys = actix::System::new("tesseract");

            let cache = logic_layer::populate_cache(
                schema.clone(), &logic_layer_config, db.clone(), &mut sys
            ).map_err(|err| format_err!("Cache population error: {}", err)).unwrap();
            let logic_layer_config = match logic_layer_config {
                Some(ll_config) => Some(Arc::new(RwLock::new(ll_config))),
                None => None
            };
            let cache_arc = Arc::new(RwLock::new(cache));
            let schema_arc = Arc::new(RwLock::new(schema.clone()));
    
            server::new(
                move|| create_app(
                    false,
                    db.clone(),
                    db_type.clone(),
                    env_vars.clone(),
                    schema_arc.clone(),
                    cache_arc.clone(),
                    logic_layer_config.clone(),
                    false,
                    has_unique_levels_properties.clone(),
                )
            )
            .bind("127.0.0.1:7777")
            .expect(&format!("cannot bind to {}", 7777))
            .start();
            sys.run();
        });

        // Sleep for 1 second to wait for server boot
        thread::sleep(time::Duration::from_secs(1));
        
        use actix_web::{actix, client};
        use futures::Future;
        actix::run(
            || client::get("http://127.0.0.1:7777/data?cube=Sales&drilldowns=Year&measures=Quantity&Year=2017")
                .header("User-Agent", "Actix-web")
                .finish().unwrap()
                .send()
                .map_err(|_| ())
                .and_then(|response| {
                    assert_eq!(response.status(), 200);
                    let res = response.body().wait().expect("Failed to parse test API response body");
                    let expected = "{\"data\":[{\"Year\":2017,\"Quantity\":266.0}],\n\"source\": [\n{\"name\":\"Sales\",\"measures\":[\"Price Total\",\"Quantity\"],\"annotations\":null}\n]}";
                    assert_eq!(res, expected);
                    actix::System::current().stop();
                    Ok(())
                })
        );
    
    }
}
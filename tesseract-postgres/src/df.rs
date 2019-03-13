use tesseract_core::Column as TesseractColumn;
use tesseract_core::DataFrame;
use tesseract_core::ColumnData;
use tokio_postgres::{Column , Row};

// TODO: numeric type not supported!
// TODO: boolean support

pub fn rows_to_df(rows: Vec<Row>, columns: &[Column]) -> DataFrame {
    let mut tcolumn_list = vec![];
    // For each column in the dataframe, setup the appropriate column vector
    // based on the underlying postgres types so that we will be able to add the values
    for col in columns.iter() {
        let col_type_name = col.type_().name();
        let col_name = col.name();
        match col_type_name {
            "int4" => { // 4 bytes
                tcolumn_list.push(TesseractColumn::new(
                    col_name.to_string(),
                    ColumnData::Int32(vec![]),
                ))
            },
            "int8" => { // 8 bytes
                tcolumn_list.push(TesseractColumn::new(
                    col_name.to_string(),
                    ColumnData::Int64(vec![]),
                ))
            },
            "float4" | "real" => {
                tcolumn_list.push(TesseractColumn::new(
                    col_name.to_string(),
                    ColumnData::Float32(vec![]),
                ));
            },
            "float8" => {
                tcolumn_list.push(TesseractColumn::new(
                    col_name.to_string(),
                    ColumnData::Float64(vec![]),
                ));
            },
            "numeric" => {
                tcolumn_list.push(TesseractColumn::new(
                    col_name.to_string(),
                    ColumnData::Float64(vec![]),
                ));
            },
            "text" => {
                tcolumn_list.push(TesseractColumn::new(
                    col_name.to_string(),
                    ColumnData::Text(vec![]),
                ));
            },
            _ => {
                println!("UNKNOWN TYPE {}", col_type_name);
            }
        }
    }

    let mut df = DataFrame::from_vec(tcolumn_list);

    for row in &rows {
        for col_idx in 0..df.columns.len() {
            let column_data = df.columns
                .get_mut(col_idx)
                .expect("logic checked?")
                .column_data();
            match column_data {
                ColumnData::Int32(col_data) => {
                    let value = row.get::<_, i32>(col_idx);
                    col_data.push(value);
                },
                ColumnData::Int64(col_data) => {
                    let value = row.get::<_, i64>(col_idx);
                    col_data.push(value);
                },
                ColumnData::Float32(col_data) => {
                    let value = row.get::<_, f32>(col_idx);
                    col_data.push(value);
                },
                ColumnData::Float64(col_data) => {
                    let value = row.get::<_, f64>(col_idx);
                    col_data.push(value);
                },
                ColumnData::Text(col_data) => {
                    let value = row.get::<_, String>(col_idx);
                    col_data.push(value);
                },
                _ => {
                    println!("NO MATCH!");
                }
            }
        }
    }

    df
}

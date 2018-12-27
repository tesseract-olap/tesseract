use failure::Error;
use futures::future::Future;
use tesseract_core::{Backend, DataFrame};

extern crate futures;
extern crate mysql_async as my;
// extern crate tokio;

// extern crate mysql;
// use mysql as my;
// use futures::done;
mod df;
use self::df::{build_column_vec, push_data_to_vec};

use my::prelude::*;
use failure::err_msg;
#[derive(Clone)]
pub struct MySql {
    pool: my::Pool
}

impl MySql {
    pub fn new(address: &str) -> MySql {
        MySql { pool: my::Pool::new(address.to_string()) }
    }

    pub fn from_addr(address: &str) -> Result<Self, Error> {
        Ok(MySql::new(address))
    }
}

impl Backend for MySql {
    fn exec_sql(&self, sql: String) -> Box<Future<Item=DataFrame, Error=Error>> {
        println!("TRYING {:?}", sql);
        let future = self.pool.get_conn()
            .and_then(move |conn| {
                conn.prep_exec(sql.to_string(), ())
            })
            .and_then(|result| {
                let tmp_vec = build_column_vec(&result).unwrap();
                Ok(push_data_to_vec(tmp_vec, result))
            })
            .and_then(|x| {
                // TODO once I can get the the push_data_to_vec fn to chain properly
                // ...should be straightforward to build the df
                Ok(DataFrame::new())
            })
            .map_err(|e| {
                err_msg(e.description().to_string())
            });
        Box::new(future)
    }

    fn box_clone(&self) -> Box<dyn Backend + Send + Sync> {
        Box::new((*self).clone())
    }
}


#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use std::env;

    #[test]
    fn test_add1() {
        let mysql_db = env::var("MYSQL_DATABASE_URL").unwrap();
        let sql = r"SELECT gh_project_id from valid_projects limit 5;";
        let mysql = MySql::new(&mysql_db);
        let r = mysql.exec_sql(sql.to_string()).wait().unwrap();
        println!("{:?}", r);
    }

    // #[test]
    // fn test2() {
    //     let mysql_db = env::var("MYSQL_DATABASE_URL").unwrap();
    //     let pool = my::Pool::new(mysql_db);
    //     println!("HEYO!");
    //     let future = pool.get_conn().and_then(move |conn| {
    //             // Create temporary table
    //             conn.prep_exec("SELECT 1 as test", ())
    //         })
    //         .and_then(|result| {
    //             let y = result.columns_ref();
    //             result.for_each(|row| {
    //                 let val = row.get(0).unwrap();
    //                 let z = match val {
    //                     Value::Int(y) => y,
    //                     s => 1111
    //                 };
    //                 println!("booo GOT IT!!! {:?}", z);
    //             })
    //         });

    //      future.wait().unwrap();
    // }
}

use failure::Error;
use futures::future::Future;
use tesseract_core::{Backend, DataFrame};

extern crate mysql;
use mysql as my;
use futures::done;
mod df;
use self::df::queryresult_to_df;

#[derive(Clone)]
pub struct MySql {
    options: String
}

impl MySql {
    pub fn from_addr(address: &str) -> Result<Self, Error> {
        Ok(MySql {
            options: address.to_string()
        })
    }
}

impl Backend for MySql {
    fn exec_sql(&self, sql: String) -> Box<Future<Item=DataFrame, Error=Error>> {
        
        println!("TRYING {:?}", sql);

        // TODO in reality we should setup the pool in the constructor and not for each query!
        let pool = my::Pool::new(self.options.to_string()).unwrap();
        let query_result = pool.prep_exec(sql.to_string(), ()).unwrap();
        
        // done() let's us convert a regular function into a future
        Box::new(done(queryresult_to_df(query_result)))
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
        // let dburl = "";
        let sql = r"SELECT project_id, commits from project_facts LIMIT 10";
        let mysql = MySql {
            options: mysql_db.to_string()
        };

        mysql.exec_sql(sql.to_string());
    }
}

use actix_web::{
    HttpResponse,
};
use failure::Fail;

#[derive(Debug, Fail)]
pub enum ServerError {
    // the display is shown in the logs as an explanation of the error
    #[fail(display="db internal error")]
    Db {
        cause: String,
    },
}

impl actix_web::error::ResponseError for ServerError {
    fn error_response(&self) -> HttpResponse {
        match self {
            ServerError::Db { cause } => HttpResponse::InternalServerError().body(cause.clone()),
        }
    }
}



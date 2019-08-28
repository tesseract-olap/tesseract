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
    #[fail(display="Logic Layer duplicate name: {:?} in cube {:?}. Level/property name must be unique.", name, cube)]
    LogicLayerDuplicateNames {
        cube: String,
        name: String,
    },

    #[fail(display="Internal Server Error {}", code)]
    ErrorCode {
        code: String,
    }
}

impl actix_web::error::ResponseError for ServerError {
    fn error_response(&self) -> HttpResponse {
        match self {
            ServerError::Db { cause } => HttpResponse::InternalServerError().body(cause.clone()),
            ServerError::LogicLayerDuplicateNames { .. } => HttpResponse::InternalServerError().body(self.to_string()),
            ServerError::ErrorCode { .. } => HttpResponse::InternalServerError().body(self.to_string()),
        }
    }
}



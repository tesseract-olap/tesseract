use actix_web::{
    FutureResponse,
    HttpResponse,
};
use futures::future::{self};


/// Helper method to return errors (FutureResponse<HttpResponse>).
pub fn boxed_error(message: String) -> FutureResponse<HttpResponse> {
    Box::new(
        future::result(
            Ok(HttpResponse::NotFound().json(message))
        )
    )
}

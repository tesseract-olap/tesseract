use futures::Future;

use actix_web::{
    AsyncResponder,
    FutureResponse,
    HttpRequest,
    HttpResponse,
};

use crate::app::{AppState};

/// Healthcheck endpoint
/// The response's status code indicates if the app is healthy or not.
/// There's no default format, as the response is always empty.
pub fn healthcheck_handler(req: HttpRequest<AppState>) -> FutureResponse<HttpResponse> {
    req.state().backend
        .ping()
        .then(|result| match result {
            Ok(_) => Ok(HttpResponse::Ok().finish()),
            Err(_) => Ok(HttpResponse::InternalServerError().finish()),
        })
        .responder()
}

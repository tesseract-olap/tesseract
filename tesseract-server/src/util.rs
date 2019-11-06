use actix_web::{
    FutureResponse,
    HttpResponse,
};
use futures::future::{self};

use actix_web::HttpRequest;
use tesseract_core::schema::Cube;

use crate::app::AppState;


/// Helper method to return errors (FutureResponse<HttpResponse>) from String.
pub fn boxed_error_string(message: String) -> FutureResponse<HttpResponse> {
    Box::new(
        future::result(
            Ok(HttpResponse::NotFound().json(message))
        )
    )
}

/// Helper method to return errors (FutureResponse<HttpResponse>) from HttpResponse.
pub fn boxed_error_http_response(response: HttpResponse) -> FutureResponse<HttpResponse> {
    Box::new(future::result(Ok(response)))
}


pub const X_TESSERACT_API_KEY: &str = "x-tesseract-api-key";


pub fn verify_api_key(req: &HttpRequest<AppState>, cube: &Cube) -> Result<(), HttpResponse> {
    if cube.public == false {
        match &req.state().env_vars.api_key {
            Some(tesseract_api_key) => {
                // Check query parameters
                let qp_secret_is_valid = {
                    let qry = req.query();
                    let qp_secret = qry.get(X_TESSERACT_API_KEY);
                    qp_secret.map(|val| val == tesseract_api_key)
                        .unwrap_or(false)
                };

                // Check headers
                let header_api_key = req.headers().get(X_TESSERACT_API_KEY);
                let header_secret_is_valid = header_api_key.map(|result_val| {
                    result_val.to_str().map(|val| val == tesseract_api_key).unwrap_or(false)
                }).unwrap_or(false);

                if qp_secret_is_valid || header_secret_is_valid {
                    return Ok(())
                } else {
                    return Err(HttpResponse::Unauthorized().json("This cube is not public".to_string()));
                }
            },
            None => {
                // TODO: Move somewhere else
                return Err(HttpResponse::InternalServerError().json("Internal Server Error 700".to_string()));
            }
        }
    }

    Ok(())
}

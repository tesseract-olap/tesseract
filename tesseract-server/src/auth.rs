use jsonwebtoken::{decode, Validation};
use serde_derive::{Serialize, Deserialize};

use actix_web::middleware::{Middleware, Started};
use actix_web::{HttpRequest, HttpResponse, Result};
pub const X_TESSERACT_JWT_TOKEN: &str = "x-tesseract-jwt-token";
use crate::app::AppState;

pub struct ValidateAccess;

impl Middleware<AppState> for ValidateAccess {
    // We only need to hook into the `start` for this middleware.
    fn start(&self, req: &HttpRequest<AppState>) -> Result<Started> {
        let app_state: &AppState = req.state();
        let jwt_secret = &app_state.env_vars.jwt_secret;

        // TODO grab from HTTP request
        let qry = req.query();

        let token = {
            let qp_token = qry.get(X_TESSERACT_JWT_TOKEN);
            match qp_token {
                None => {
                    // If we don't match in query params, try headers
                    // The next lines below are little ugly. Basically,
                    // we need to catch for two potential errors:
                    // 1. the key might not be present (phase1)
                    // 2. the key might not parse to a string properly (phase2)
                    let phase1 = req.headers().get(X_TESSERACT_JWT_TOKEN);
                    match phase1 {
                        Some(val) => {
                            let phase2 = val.to_str();
                            match phase2 {
                                Ok(v) => v,
                                _ => ""
                            }
                        },
                        _ => "",
                    }
                },
                Some(token) => token,
            }
        };
        match validate_web_token(jwt_secret, &token) {
            true => Ok(Started::Done),
            false => Ok(Started::Response(
                        HttpResponse::Unauthorized()
                            .json("Unauthorized")
                    ))
            }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,
    status: String,
    exp: usize,
}


pub fn validate_web_token(jwt_secret: &Option<String>, raw_token: &str) -> bool {
    match jwt_secret {
        Some(key) => {
            let validation = Validation {
                ..Validation::default()
            };
            match decode::<Claims>(&raw_token, key.as_ref(), &validation) {
                Ok(c) => {
                    let claims: Claims = c.claims;
                    claims.status == "valid" // TODO allow this value to be configurable
                },
                Err(_) => false, // If any error occurs, do not validate
            }
        },
        None => true
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_jwt_auth_good() {
        let jwt_secret = Some("hello-secret-123".to_string());
        let result = validate_web_token(&jwt_secret, "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJPbmxpbmUgSldUIEJ1aWxkZXIiLCJpYXQiOjE1Nzk4ODI4NDcsImV4cCI6MjUyNjU2NzY3MywiYXVkIjoid3d3LmV4YW1wbGUuY29tIiwic3ViIjoianJvY2tldEBleGFtcGxlLmNvbSIsInN0YXR1cyI6InZhbGlkIn0.GSMVTKG3RrWOCfoDpGmJcYakspKsmjkZw7Le9lPJwtw");
        assert_eq!(result, true);
    }

    #[test]
    fn test_jwt_auth_bad1() {
        let jwt_secret = Some("hello-secret-123".to_string());
        let result = validate_web_token(&jwt_secret, "eyJpc3MiOiJPbmxpbmUgSldUIEJ1aWxkZXIiLCJpYXQiOjE1Nzk4ODI4NDcsImV4cCI6MjUyNjU2NzY3MywiYXVkIjoid3d3LmV4YW1wbGUuY29tIiwic3ViIjoianJvY2tldEBleGFtcGxlLmNvbSIsInN0YXR1cyI6InZhbGlkIn0.GSMVTKG3RrWOCfoDpGmJcYakspKsmjkZw7Le9lPJwtw");
        assert_eq!(result, false);
    }

    #[test]
    fn test_jwt_auth_bad2() {
        let jwt_secret = Some("hello-secret-123".to_string());
        let result = validate_web_token(&jwt_secret, "");
        assert_eq!(result, false);
    }

    #[test]
    fn test_jwt_auth_good2() {
        // if token is none, all requests are OK
        let jwt_secret = None;
        let result = validate_web_token(&jwt_secret, "");
        assert_eq!(result, true);
    }
}
use jsonwebtoken::{decode, Validation};
use serde_derive::{Serialize, Deserialize};
use log;
use actix_web::middleware::{Middleware, Started};
use actix_web::{HttpRequest, HttpResponse, Result};
pub const X_TESSERACT_JWT_TOKEN: &str = "x-tesseract-jwt-token";
use crate::app::AppState;
use tesseract_core::{DEFAULT_ALLOWED_ACCESS, INVALID_ACCESS};
pub struct ValidateAccess;

impl Middleware<AppState> for ValidateAccess {
    // We only need to hook into the `start` for this middleware.
    fn start(&self, req: &HttpRequest<AppState>) -> Result<Started> {
        let app_state: &AppState = req.state();
        let jwt_secret = &app_state.env_vars.jwt_secret;

        let token = extract_token(&req);
        if validate_web_token(jwt_secret, &token, DEFAULT_ALLOWED_ACCESS) {
            Ok(Started::Done)
        } else {
            Ok(Started::Response(
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
    auth_level: Option<i32>,
}

pub fn extract_token(req: &HttpRequest<AppState>) -> String {
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
    token.to_string()
}

// None = auth not set on server, -1 = bad auth level
pub fn user_auth_level(jwt_secret: &Option<String>, raw_token: &str) -> Option<i32> {
    match jwt_secret {
        Some(key) => {
            let validation = Validation::default();
            match decode::<Claims>(&raw_token, key.as_ref(), &validation) {
                Ok(c) => {
                    let claims: Claims = c.claims;
                    if claims.auth_level.is_some() && claims.status == "valid" {
                        claims.auth_level
                    } else {
                        Some(INVALID_ACCESS)
                    }
                },
                Err(_err) => Some(INVALID_ACCESS), // If any error occurs, do not validate
            }
        },
        None => None
    }
}

pub fn validate_web_token(jwt_secret: &Option<String>, raw_token: &str, min_auth_level: i32) -> bool {
    match jwt_secret {
        Some(key) => {
            let validation = Validation::default();
            match decode::<Claims>(&raw_token, key.as_ref(), &validation) {
                Ok(c) => {
                    let claims: Claims = c.claims;
                    let part1 = claims.status == "valid"; // TODO allow this value to be configurable
                    let part2 = match claims.auth_level {
                        Some(lvl) => lvl >= min_auth_level,
                        None => false // if no auth_level is set, do not validate
                    };
                    part1 && part2
                },
                Err(_err) => false, // If any error occurs, do not validate
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
        let result = validate_web_token(&jwt_secret, "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJPbmxpbmUgSldUIEJ1aWxkZXIiLCJpYXQiOjE1ODA0MjA2NzQsImV4cCI6MTczODE4NzA4OSwiYXVkIjoid3d3LmV4YW1wbGUuY29tIiwic3ViIjoianJvY2tldEBleGFtcGxlLmNvbSIsInN0YXR1cyI6InZhbGlkIiwiYXV0aF9sZXZlbCI6Mn0.IUkh8-y9LIcNcxpmCJyLK09SY9LDm8P0ekJcL4OZKNI", DEFAULT_ALLOWED_ACCESS);
        assert_eq!(result, true);
    }

    #[test]
    fn test_jwt_auth_bad1() {
        let jwt_secret = Some("hello-secret-123".to_string());
        let result = validate_web_token(&jwt_secret, "eyJpc3MiOiJPbmxpbmUgSldUIEJ1aWxkZXIiLCJpYXQiOjE1Nzk4ODI4NDcsImV4cCI6MjUyNjU2NzY3MywiYXVkIjoid3d3LmV4YW1wbGUuY29tIiwic3ViIjoianJvY2tldEBleGFtcGxlLmNvbSIsInN0YXR1cyI6InZhbGlkIn0.GSMVTKG3RrWOCfoDpGmJcYakspKsmjkZw7Le9lPJwtw", DEFAULT_ALLOWED_ACCESS);
        assert_eq!(result, false);
    }

    #[test]
    fn test_jwt_auth_bad2() {
        let jwt_secret = Some("hello-secret-123".to_string());
        let result = validate_web_token(&jwt_secret, "", DEFAULT_ALLOWED_ACCESS);
        assert_eq!(result, false);
    }

    #[test]
    fn test_jwt_auth_good2() {
        // if token is none, all requests are OK
        let jwt_secret = None;
        let result = validate_web_token(&jwt_secret, "", DEFAULT_ALLOWED_ACCESS);
        assert_eq!(result, true);
    }

    #[test]
    fn test_jwt_auth_level_overflow_bad() {
        let jwt_secret = Some("hello-secret-123".to_string());
        let result = validate_web_token(&jwt_secret, "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJPbmxpbmUgSldUIEJ1aWxkZXIiLCJpYXQiOjE1ODAyNDg2OTAsImV4cCI6MTU4MDI1MjgyNiwiYXVkIjoid3d3LmV4YW1wbGUuY29tIiwic3ViIjoianJvY2tldEBleGFtcGxlLmNvbSIsInN0YXR1cyI6InZhbGlkIiwiYXV0aF9sZXZlbCI6NWUrNDUsImp0aSI6IjIzYmIwMDg5LWQ2NDctNDNlOC04YjdiLWIxOGU4N2ViMjljZCJ9.-d7k5i6oGopyBzSbiD9rl9FyYQUR_hwy4tvYzgfMb1M", DEFAULT_ALLOWED_ACCESS);
        assert_eq!(result, false);
    }

    #[test]
    fn test_auth_level_too_low_bad() {
        let jwt_secret = Some("hello-secret-123".to_string());
        let result = validate_web_token(&jwt_secret, "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJPbmxpbmUgSldUIEJ1aWxkZXIiLCJpYXQiOjE1ODA0MjA2NzQsImV4cCI6MTczODE4NzA4OSwiYXVkIjoid3d3LmV4YW1wbGUuY29tIiwic3ViIjoianJvY2tldEBleGFtcGxlLmNvbSIsInN0YXR1cyI6InZhbGlkIiwiYXV0aF9sZXZlbCI6Mn0.IUkh8-y9LIcNcxpmCJyLK09SY9LDm8P0ekJcL4OZKNI", 100);
        assert_eq!(result, false);
    }

    #[test]
    fn test_no_auth_level_in_jwt_is_bad() {
        let jwt_secret = Some("hello-secret-123".to_string());
        let result = validate_web_token(&jwt_secret, "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwiaWF0IjoxNTE2MjM5MDIyLCJleHAiOjE5MTYyMzkwMjIsInN0YXR1cyI6InZhbGlkIn0.8kc8kYiPe2PSzGuEvDQJNw0eJicHloPhJK6FYJL95pI", 0);
        assert_eq!(result, false);
    }
}
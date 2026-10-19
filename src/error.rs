use crate::fairings::{request_id_for, request_span_for};
use rocket::http::{Header, Status};
use rocket::response::Responder;
use rocket::serde::json::Json;
use rocket::{Request, Response};
use serde::{Deserialize, Serialize};
use std::fmt;
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ApiErrorCode {
    BadRequest,
    UnprocessableEntity,
    Unauthorized,
    Forbidden,
    NotFound,
    InternalError,
    RateLimited,
    NotYetIndexed,
    SwapUnsupportedToken,
    SwapSameToken,
    SwapNoLiquidity,
    SwapOracleUnavailable,
    SwapMarketClosed,
    SwapQuoteFailed,
    SwapTimeout,
    SwapPreflightFailed,
    SwapCalldataFailed,
    OrdersQueryFailed,
    TradesQueryFailed,
    UpstreamUnavailable,
}

impl ApiErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BadRequest => "BAD_REQUEST",
            Self::UnprocessableEntity => "UNPROCESSABLE_ENTITY",
            Self::Unauthorized => "UNAUTHORIZED",
            Self::Forbidden => "FORBIDDEN",
            Self::NotFound => "NOT_FOUND",
            Self::InternalError => "INTERNAL_ERROR",
            Self::RateLimited => "RATE_LIMITED",
            Self::NotYetIndexed => "NOT_YET_INDEXED",
            Self::SwapUnsupportedToken => "SWAP_UNSUPPORTED_TOKEN",
            Self::SwapSameToken => "SWAP_SAME_TOKEN",
            Self::SwapNoLiquidity => "SWAP_NO_LIQUIDITY",
            Self::SwapOracleUnavailable => "SWAP_ORACLE_UNAVAILABLE",
            Self::SwapMarketClosed => "SWAP_MARKET_CLOSED",
            Self::SwapQuoteFailed => "SWAP_QUOTE_FAILED",
            Self::SwapTimeout => "SWAP_TIMEOUT",
            Self::SwapPreflightFailed => "SWAP_PREFLIGHT_FAILED",
            Self::SwapCalldataFailed => "SWAP_CALLDATA_FAILED",
            Self::OrdersQueryFailed => "ORDERS_QUERY_FAILED",
            Self::TradesQueryFailed => "TRADES_QUERY_FAILED",
            Self::UpstreamUnavailable => "UPSTREAM_UNAVAILABLE",
        }
    }

    pub const fn status(self) -> Status {
        match self {
            Self::BadRequest
            | Self::SwapUnsupportedToken
            | Self::SwapSameToken
            | Self::SwapPreflightFailed => Status::BadRequest,
            Self::UnprocessableEntity => Status::UnprocessableEntity,
            Self::Unauthorized => Status::Unauthorized,
            Self::Forbidden => Status::Forbidden,
            // An expected state (the stock oracle has no price outside US
            // trading hours), not an outage: the request conflicts with the
            // market's current state and succeeds unchanged once it reopens.
            Self::SwapMarketClosed => Status::Conflict,
            Self::NotFound | Self::SwapNoLiquidity => Status::NotFound,
            Self::RateLimited => Status::TooManyRequests,
            Self::NotYetIndexed => Status::Accepted,
            Self::OrdersQueryFailed | Self::TradesQueryFailed => Status::BadGateway,
            Self::SwapOracleUnavailable | Self::UpstreamUnavailable => Status::ServiceUnavailable,
            Self::SwapTimeout => Status::GatewayTimeout,
            Self::InternalError | Self::SwapQuoteFailed | Self::SwapCalldataFailed => {
                Status::InternalServerError
            }
        }
    }
}

impl fmt::Display for ApiErrorCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiErrorDetail {
    #[schema(example = "BAD_REQUEST")]
    pub code: ApiErrorCode,
    #[schema(example = "Something went wrong")]
    pub message: String,
    /// For SWAP_MARKET_CLOSED: when the stock oracle's session next opens
    /// (RFC 3339, UTC). Omitted for every other error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(example = "2026-10-19T08:00:00Z")]
    pub next_open: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({"request_id": "550e8400-e29b-41d4-a716-446655440000", "error": {"code": "BAD_REQUEST", "message": "Something went wrong"}}))]
pub struct ApiErrorResponse {
    pub request_id: String,
    pub error: ApiErrorDetail,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum ApiError {
    #[error("Bad request: {0}")]
    BadRequest(String),
    #[error("Unauthorized: {0}")]
    Unauthorized(String),
    #[error("Forbidden: {0}")]
    Forbidden(String),
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Internal error: {0}")]
    Internal(String),
    #[error("Rate limited: {0}")]
    RateLimited(String),
    #[error("Not yet indexed: {0}")]
    NotYetIndexed(String),
    /// The stock oracle answered "no live quote" while the US session is shut.
    #[error("market closed (next open {next_open:?})")]
    MarketClosed { next_open: Option<String> },
    #[error("{code}: {public_message}")]
    Coded {
        code: ApiErrorCode,
        public_message: &'static str,
    },
}

impl ApiError {
    pub fn coded(code: ApiErrorCode, public_message: &'static str) -> Self {
        Self::Coded {
            code,
            public_message,
        }
    }

    /// The stable error code this error is reported as.
    ///
    /// Shared by the HTTP responder and by analytics so a failure carries the same
    /// code in PostHog as the caller saw on the wire — otherwise the two drift and
    /// a dashboard can disagree with the client about what actually happened.
    pub fn code(&self) -> ApiErrorCode {
        match self {
            ApiError::BadRequest(_) => ApiErrorCode::BadRequest,
            ApiError::Unauthorized(_) => ApiErrorCode::Unauthorized,
            ApiError::Forbidden(_) => ApiErrorCode::Forbidden,
            ApiError::NotFound(_) => ApiErrorCode::NotFound,
            ApiError::Internal(_) => ApiErrorCode::InternalError,
            ApiError::RateLimited(_) => ApiErrorCode::RateLimited,
            ApiError::NotYetIndexed(_) => ApiErrorCode::NotYetIndexed,
            ApiError::MarketClosed { .. } => ApiErrorCode::SwapMarketClosed,
            ApiError::Coded { code, .. } => *code,
        }
    }

    /// The message returned to the caller. Public by construction — every variant
    /// holds text already destined for the response body, so this never leaks
    /// internal detail that `respond_to` would have withheld.
    pub fn public_message(&self) -> String {
        match self {
            ApiError::BadRequest(msg)
            | ApiError::Unauthorized(msg)
            | ApiError::Forbidden(msg)
            | ApiError::NotFound(msg)
            | ApiError::Internal(msg)
            | ApiError::RateLimited(msg)
            | ApiError::NotYetIndexed(msg) => msg.clone(),
            ApiError::MarketClosed { .. } => MARKET_CLOSED_MESSAGE.to_string(),
            ApiError::Coded { public_message, .. } => (*public_message).to_string(),
        }
    }

    /// When the market next opens, for a market-closed answer.
    pub fn next_open(&self) -> Option<String> {
        match self {
            ApiError::MarketClosed { next_open } => next_open.clone(),
            _ => None,
        }
    }
}

pub(crate) const MARKET_CLOSED_MESSAGE: &str =
    "the US stock market is closed, so this swap has no live price until it reopens";

impl ApiErrorResponse {
    pub(crate) fn from_error(request_id: String, error: &ApiError) -> Self {
        Self {
            request_id,
            error: ApiErrorDetail {
                code: error.code(),
                message: error.public_message(),
                next_open: error.next_open(),
            },
        }
    }
}

impl<'r> Responder<'r, 'static> for ApiError {
    fn respond_to(self, req: &'r Request<'_>) -> rocket::response::Result<'static> {
        let (code, message) = (self.code(), self.public_message());
        let status = code.status();
        let span = request_span_for(req);
        span.in_scope(|| {
            if status.code >= 500 {
                tracing::error!(
                    status = status.code,
                    code = %code,
                    error_message = %message,
                    "request failed"
                );
            } else if code == ApiErrorCode::NotYetIndexed {
                tracing::info!(
                    status = status.code,
                    code = %code,
                    error_message = %message,
                    "transaction not yet indexed"
                );
            } else {
                tracing::warn!(
                    status = status.code,
                    code = %code,
                    error_message = %message,
                    "request failed"
                );
            }
        });

        let request_id = request_id_for(req);
        let body = ApiErrorResponse::from_error(request_id, &self);
        let json_response = match Json(body).respond_to(req) {
            Ok(r) => r,
            Err(s) => {
                tracing::error!(status = %s.code, "failed to serialize error response");
                return Err(s);
            }
        };
        let mut response = Response::build_from(json_response)
            .status(status)
            .finalize();
        if code == ApiErrorCode::RateLimited {
            response.set_header(Header::new("Retry-After", "60"));
        }
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fairings::RequestLogger;
    use rocket::local::blocking::Client;
    use tracing_test::traced_test;

    #[get("/bad-request")]
    fn bad_request() -> Result<(), ApiError> {
        Err(ApiError::BadRequest("invalid input".into()))
    }
    #[get("/unauthorized")]
    fn unauthorized() -> Result<(), ApiError> {
        Err(ApiError::Unauthorized("no token".into()))
    }
    #[get("/not-found")]
    fn not_found() -> Result<(), ApiError> {
        Err(ApiError::NotFound("order not found".into()))
    }
    #[get("/internal")]
    fn internal() -> Result<(), ApiError> {
        Err(ApiError::Internal("something broke".into()))
    }
    #[get("/coded")]
    fn coded() -> Result<(), ApiError> {
        Err(ApiError::coded(
            ApiErrorCode::SwapNoLiquidity,
            "no executable liquidity",
        ))
    }

    #[get("/market-closed")]
    fn market_closed() -> Result<(), ApiError> {
        Err(ApiError::MarketClosed {
            next_open: Some("2026-10-19T08:00:00Z".to_string()),
        })
    }
    #[get("/oracle-unavailable")]
    fn oracle_unavailable() -> Result<(), ApiError> {
        Err(ApiError::coded(
            ApiErrorCode::SwapOracleUnavailable,
            "the oracle required to evaluate this swap is temporarily unavailable",
        ))
    }

    fn error_client() -> Client {
        let rocket = rocket::build()
            .mount(
                "/",
                rocket::routes![
                    bad_request,
                    unauthorized,
                    not_found,
                    internal,
                    coded,
                    market_closed,
                    oracle_unavailable
                ],
            )
            .attach(RequestLogger);
        Client::tracked(rocket).expect("valid rocket instance")
    }

    fn assert_error_response(
        client: &Client,
        path: &str,
        expected_status: u16,
        expected_code: &str,
        expected_message: &str,
    ) {
        let response = client.get(path).dispatch();
        assert_eq!(response.status().code, expected_status);
        let request_id = response
            .headers()
            .get_one("X-Request-Id")
            .expect("request id response header")
            .to_string();
        let body: serde_json::Value =
            serde_json::from_str(&response.into_string().unwrap()).unwrap();
        assert_eq!(body["request_id"], request_id);
        assert_eq!(body["error"]["code"], expected_code);
        assert_eq!(body["error"]["message"], expected_message);
    }

    #[test]
    fn test_market_closed_returns_409_with_next_open() {
        let client = error_client();
        assert_error_response(
            &client,
            "/market-closed",
            409,
            "SWAP_MARKET_CLOSED",
            MARKET_CLOSED_MESSAGE,
        );
        let body: serde_json::Value = serde_json::from_str(
            &client
                .get("/market-closed")
                .dispatch()
                .into_string()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(body["error"]["next_open"], "2026-10-19T08:00:00Z");
    }

    #[test]
    fn test_oracle_unavailable_body_is_unchanged() {
        let client = error_client();
        let response = client.get("/oracle-unavailable").dispatch();
        assert_eq!(response.status().code, 503);
        let body: serde_json::Value =
            serde_json::from_str(&response.into_string().unwrap()).unwrap();
        let error = body["error"].as_object().expect("error object");
        assert_eq!(error["code"], "SWAP_ORACLE_UNAVAILABLE");
        let mut keys: Vec<&str> = error.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(keys, ["code", "message"]);
    }

    #[test]
    fn test_bad_request_returns_400() {
        let client = error_client();
        assert_error_response(&client, "/bad-request", 400, "BAD_REQUEST", "invalid input");
    }

    #[test]
    fn test_unauthorized_returns_401() {
        let client = error_client();
        assert_error_response(&client, "/unauthorized", 401, "UNAUTHORIZED", "no token");
    }

    #[test]
    fn test_not_found_returns_404() {
        let client = error_client();
        assert_error_response(&client, "/not-found", 404, "NOT_FOUND", "order not found");
    }

    #[test]
    fn test_internal_returns_500() {
        let client = error_client();
        assert_error_response(
            &client,
            "/internal",
            500,
            "INTERNAL_ERROR",
            "something broke",
        );
    }

    #[traced_test]
    #[test]
    fn test_coded_error_uses_catalog_status_and_code() {
        let client = error_client();
        assert_error_response(
            &client,
            "/coded",
            404,
            "SWAP_NO_LIQUIDITY",
            "no executable liquidity",
        );
        assert!(logs_contain("SWAP_NO_LIQUIDITY"));
    }

    #[test]
    fn test_code_and_message_cover_coded_and_representative_errors() {
        let cases: [(ApiError, ApiErrorCode, &str); 2] = [
            (
                ApiError::BadRequest("bad".into()),
                ApiErrorCode::BadRequest,
                "bad",
            ),
            (
                ApiError::coded(ApiErrorCode::SwapSameToken, "same token"),
                ApiErrorCode::SwapSameToken,
                "same token",
            ),
        ];

        for (error, expected_code, expected_message) in cases {
            assert_eq!(error.code(), expected_code);
            assert_eq!(error.public_message(), expected_message);
        }
    }

    #[test]
    fn test_catalog_serializes_stable_codes() {
        let cases = [
            (ApiErrorCode::BadRequest, "\"BAD_REQUEST\""),
            (ApiErrorCode::SwapMarketClosed, "\"SWAP_MARKET_CLOSED\""),
            (ApiErrorCode::SwapQuoteFailed, "\"SWAP_QUOTE_FAILED\""),
            (ApiErrorCode::SwapTimeout, "\"SWAP_TIMEOUT\""),
            (ApiErrorCode::SwapSameToken, "\"SWAP_SAME_TOKEN\""),
            (
                ApiErrorCode::UpstreamUnavailable,
                "\"UPSTREAM_UNAVAILABLE\"",
            ),
        ];

        for (code, expected) in cases {
            assert_eq!(serde_json::to_string(&code).unwrap(), expected);
        }
    }

    #[test]
    fn test_trade_catalog_uses_expected_http_statuses() {
        let cases = [
            (ApiErrorCode::SwapUnsupportedToken, Status::BadRequest),
            (ApiErrorCode::SwapSameToken, Status::BadRequest),
            (ApiErrorCode::SwapPreflightFailed, Status::BadRequest),
            (ApiErrorCode::SwapNoLiquidity, Status::NotFound),
            (
                ApiErrorCode::SwapOracleUnavailable,
                Status::ServiceUnavailable,
            ),
            (ApiErrorCode::SwapMarketClosed, Status::Conflict),
            (ApiErrorCode::SwapQuoteFailed, Status::InternalServerError),
            (ApiErrorCode::SwapTimeout, Status::GatewayTimeout),
            (
                ApiErrorCode::SwapCalldataFailed,
                Status::InternalServerError,
            ),
            (ApiErrorCode::OrdersQueryFailed, Status::BadGateway),
            (ApiErrorCode::TradesQueryFailed, Status::BadGateway),
            (
                ApiErrorCode::UpstreamUnavailable,
                Status::ServiceUnavailable,
            ),
        ];

        for (code, expected_status) in cases {
            assert_eq!(code.status(), expected_status);
        }
    }
}

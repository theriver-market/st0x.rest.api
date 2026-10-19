use crate::error::{ApiErrorCode, ApiErrorDetail, ApiErrorResponse};
use crate::fairings::{request_id_for, request_span_for};
use rocket::http::Header;
use rocket::response::Responder;
use rocket::serde::json::Json;
use rocket::Catcher;
use rocket::Request;

#[catch(400)]
pub fn bad_request(req: &Request<'_>) -> Json<ApiErrorResponse> {
    let span = request_span_for(req);
    span.in_scope(|| {
        tracing::warn!("bad request (invalid content type, missing headers, or malformed input)")
    });

    Json(ApiErrorResponse {
        request_id: request_id_for(req),
        error: ApiErrorDetail {
            code: ApiErrorCode::BadRequest,
            message: "The request was invalid or malformed".to_string(),
            next_open: None,
        },
    })
}

#[catch(401)]
pub fn unauthorized(req: &Request<'_>) -> Json<ApiErrorResponse> {
    let span = request_span_for(req);
    span.in_scope(|| tracing::warn!("unauthorized (missing or invalid credentials)"));

    Json(ApiErrorResponse {
        request_id: request_id_for(req),
        error: ApiErrorDetail {
            code: ApiErrorCode::Unauthorized,
            message: "Missing or invalid credentials".to_string(),
            next_open: None,
        },
    })
}

#[catch(403)]
pub fn forbidden(req: &Request<'_>) -> Json<ApiErrorResponse> {
    let span = request_span_for(req);
    span.in_scope(|| tracing::warn!("forbidden (insufficient permissions)"));

    Json(ApiErrorResponse {
        request_id: request_id_for(req),
        error: ApiErrorDetail {
            code: ApiErrorCode::Forbidden,
            message: "Insufficient permissions".to_string(),
            next_open: None,
        },
    })
}

#[catch(404)]
pub fn not_found(req: &Request<'_>) -> Json<ApiErrorResponse> {
    let span = request_span_for(req);
    span.in_scope(|| tracing::warn!("route not found"));

    Json(ApiErrorResponse {
        request_id: request_id_for(req),
        error: ApiErrorDetail {
            code: ApiErrorCode::NotFound,
            message: "The requested resource was not found".to_string(),
            next_open: None,
        },
    })
}

#[catch(422)]
pub fn unprocessable_entity(req: &Request<'_>) -> Json<ApiErrorResponse> {
    let span = request_span_for(req);
    span.in_scope(|| tracing::warn!("unprocessable entity (likely malformed request body)"));

    Json(ApiErrorResponse {
        request_id: request_id_for(req),
        error: ApiErrorDetail {
            code: ApiErrorCode::UnprocessableEntity,
            message: "Request body could not be parsed".to_string(),
            next_open: None,
        },
    })
}

pub(crate) struct RateLimitedResponse(Json<ApiErrorResponse>);

impl<'r, 'o: 'r> Responder<'r, 'o> for RateLimitedResponse {
    fn respond_to(self, req: &'r Request<'_>) -> rocket::response::Result<'o> {
        let mut res = self.0.respond_to(req)?;
        res.set_header(Header::new("Retry-After", "60"));
        Ok(res)
    }
}

#[catch(429)]
pub fn too_many_requests(req: &Request<'_>) -> RateLimitedResponse {
    let span = request_span_for(req);
    span.in_scope(|| tracing::warn!("rate limit exceeded"));

    RateLimitedResponse(Json(ApiErrorResponse {
        request_id: request_id_for(req),
        error: ApiErrorDetail {
            code: ApiErrorCode::RateLimited,
            message: "Too many requests, please try again later".to_string(),
            next_open: None,
        },
    }))
}

#[catch(500)]
pub fn internal_server_error(req: &Request<'_>) -> Json<ApiErrorResponse> {
    let span = request_span_for(req);
    span.in_scope(|| tracing::error!("unhandled internal server error"));

    Json(ApiErrorResponse {
        request_id: request_id_for(req),
        error: ApiErrorDetail {
            code: ApiErrorCode::InternalError,
            message: "Internal server error".to_string(),
            next_open: None,
        },
    })
}

pub fn catchers() -> Vec<Catcher> {
    rocket::catchers![
        bad_request,
        unauthorized,
        forbidden,
        not_found,
        too_many_requests,
        unprocessable_entity,
        internal_server_error
    ]
}

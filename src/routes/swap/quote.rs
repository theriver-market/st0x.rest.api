use super::{
    capture_swap_outcome, ensure_distinct_tokens, exchange_log::SwapExchangeLog,
    no_liquidity_error, request_chain_id, snapshot_swap_context, RaindexSwapDataSource,
    SwapAnalyticsContext, SwapCandidateBuild, SwapDataSource,
};
use crate::analytics::{
    swap_quote_failed_event, swap_quoted_event, swap_quoted_v2_event, Analytics, ApiVersion,
};
use crate::app_state::ApplicationState;
use crate::auth::AuthenticatedKey;
use crate::db::DbPool;
use crate::error::{ApiError, ApiErrorCode, ApiErrorResponse};
use crate::fairings::{GlobalRateLimit, TracingSpan};
use crate::routes::swap::denomination::{
    denormalize_calldata_price_cap, normalize_calldata_price_cap,
    normalize_calldata_request_amount, normalize_quote_amounts, CalldataAmountNormalization,
};
use crate::swap_capacity::SwapCapacity;
use crate::types::swap::{
    SwapQuoteRequest, SwapQuoteResponse, SwapQuoteV2Request, SwapQuoteV2RequestBody,
    SwapQuoteV2Response,
};
use alloy::primitives::{fixed_bytes, Address};
use rain_math_float::Float;
use rain_orderbook_common::take_orders::{
    simulate_buy_over_candidates, ParsedTakeOrdersMode, TakeOrdersMode,
};
use rocket::serde::json::Json;
use rocket::State;
use std::ops::{Div, Sub};
use tracing::Instrument;

// Up-to modes can be microscopically under target because DecimalFloat arithmetic
// rounds. One part per billion is presentation dust; exact modes remain strict.
const FULL_FILL_RELATIVE_TOLERANCE: Float = Float::from_raw(fixed_bytes!(
    "fffffff700000000000000000000000000000000000000000000000000000001"
));

#[utoipa::path(
    post,
    path = "/v1/swap/quote",
    tag = "Swap",
    security(("basicAuth" = [])),
    request_body = SwapQuoteRequest,
    responses(
        (status = 200, description = "Swap quote", body = SwapQuoteResponse),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "No liquidity found", body = ApiErrorResponse),
        (status = 422, description = "Request body could not be parsed", body = ApiErrorResponse),
        (status = 429, description = "Rate limited", body = ApiErrorResponse),
        (status = 500, description = "Internal server error", body = ApiErrorResponse),
        (status = 502, description = "Order source unavailable", body = ApiErrorResponse),
        (status = 503, description = "Required swap oracle unavailable", body = ApiErrorResponse),
        (status = 504, description = "Swap request timed out", body = ApiErrorResponse),
    )
)]
#[post("/quote", data = "<request>")]
#[allow(clippy::too_many_arguments)] // Rocket handler: args are guards + managed state + body.
pub async fn post_swap_quote(
    _global: GlobalRateLimit,
    key: AuthenticatedKey,
    shared_raindex: &State<crate::raindex::SharedRaindexProvider>,
    app_state: &State<ApplicationState>,
    pool: &State<DbPool>,
    analytics: &State<Analytics>,
    capacity: &State<SwapCapacity>,
    span: TracingSpan,
    request: Json<SwapQuoteRequest>,
) -> Result<Json<SwapQuoteResponse>, ApiError> {
    let request_id = span.request_id().to_string();
    let api_version = span.api_version();
    async move {
        let mut req = request.into_inner();
        req.chain_id = crate::routes::compatibility_swap_chain_id(api_version, req.chain_id)?;
        let chain_id_result = {
            let raindex = shared_raindex.read().await;
            crate::routes::resolve_required_raindex_chain_id(raindex.client(), req.chain_id)
        };
        if let Err(error) = &chain_id_result {
            SwapExchangeLog::new(
                &request_id,
                "/v1/swap/quote",
                "v1",
                "quote",
                &key,
                req.input_token,
                req.output_token,
                &req,
            )
            .record_error(error);
        }
        let chain_id = chain_id_result?;
        tracing::info!(chain_id, "resolved required Raindex chain");
        req.chain_id = Some(chain_id);
        let exchange = SwapExchangeLog::new(
            &request_id,
            "/v1/swap/quote",
            "v1",
            "quote",
            &key,
            req.input_token,
            req.output_token,
            &req,
        );
        let result = capacity
            .run(key.id, key.swap_max_concurrent, async {
                let raindex = shared_raindex.read().await;
                let ds = RaindexSwapDataSource::new(
                    raindex.client(),
                    &app_state.response_caches,
                    pool.inner(),
                );
                handle_swap_quote(&ds, &key, analytics.inner(), req).await
            })
            .await;
        exchange.record(&result);
        result.map(Json)
    }
    .instrument(span.0)
    .await
}

#[utoipa::path(
    post,
    path = "/v2/swap/quote",
    tag = "Swap",
    summary = "Simulate a mode-based swap",
    description = "Returns the SDK-backed executable-route simulation used by V2 calldata. Provide exactly one of priceCap or slippageBps. referenceIoRatio is an optional input-token-per-output-token guard that is valid only with slippageBps.",
    security(("basicAuth" = [])),
    request_body = SwapQuoteV2RequestBody,
    responses(
        (status = 200, description = "Mode-based swap quote", body = SwapQuoteV2Response),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "No liquidity found", body = ApiErrorResponse),
        (status = 422, description = "Request body could not be parsed", body = ApiErrorResponse),
        (status = 429, description = "Rate limited", body = ApiErrorResponse),
        (status = 500, description = "Internal server error", body = ApiErrorResponse),
        (status = 502, description = "Order source unavailable", body = ApiErrorResponse),
        (status = 503, description = "Required swap oracle unavailable", body = ApiErrorResponse),
        (status = 504, description = "Swap request timed out", body = ApiErrorResponse),
    )
)]
#[post("/quote", data = "<request>")]
#[allow(clippy::too_many_arguments)] // Rocket handler: args are guards + managed state + body.
pub async fn post_swap_quote_v2(
    _global: GlobalRateLimit,
    key: AuthenticatedKey,
    shared_raindex: &State<crate::raindex::SharedRaindexProvider>,
    app_state: &State<ApplicationState>,
    pool: &State<DbPool>,
    analytics: &State<Analytics>,
    capacity: &State<SwapCapacity>,
    span: TracingSpan,
    request: Json<SwapQuoteV2Request>,
) -> Result<Json<SwapQuoteV2Response>, ApiError> {
    let request_id = span.request_id().to_string();
    let api_version = span.api_version();
    let (route_path, version_label, analytics_api_version) = if api_version == Some(3) {
        ("/v3/swap/quote", "v3", ApiVersion::V3)
    } else {
        ("/v2/swap/quote", "v2", ApiVersion::V2)
    };
    async move {
        let mut req = request.into_inner();
        req.chain_id = crate::routes::compatibility_swap_chain_id(api_version, req.chain_id)?;
        let chain_id_result = {
            let raindex = shared_raindex.read().await;
            crate::routes::resolve_required_raindex_chain_id(raindex.client(), req.chain_id)
        };
        if let Err(error) = &chain_id_result {
            SwapExchangeLog::new(
                &request_id,
                route_path,
                version_label,
                "quote",
                &key,
                req.input_token,
                req.output_token,
                &req,
            )
            .record_error(error);
        }
        let chain_id = chain_id_result?;
        tracing::info!(chain_id, "resolved required Raindex chain");
        req.chain_id = Some(chain_id);
        let exchange = SwapExchangeLog::new(
            &request_id,
            route_path,
            version_label,
            "quote",
            &key,
            req.input_token,
            req.output_token,
            &req,
        );
        let result = capacity
            .run(key.id, key.swap_max_concurrent, async {
                let raindex = shared_raindex.read().await;
                let ds = RaindexSwapDataSource::new(
                    raindex.client(),
                    &app_state.response_caches,
                    pool.inner(),
                );
                handle_swap_quote_v2(&ds, &key, analytics.inner(), analytics_api_version, req).await
            })
            .await;
        exchange.record(&result);
        result.map(Json)
    }
    .instrument(span.0)
    .await
}

async fn handle_swap_quote(
    ds: &dyn SwapDataSource,
    key: &AuthenticatedKey,
    analytics: &Analytics,
    req: SwapQuoteRequest,
) -> Result<SwapQuoteResponse, ApiError> {
    let analytics_context = snapshot_swap_context(analytics, || SwapAnalyticsContext {
        chain_id: req.chain_id,
        input_token: req.input_token,
        output_token: req.output_token,
        requested_amount: req.output_amount.clone(),
        denomination: serde_json::to_value(req.denomination).unwrap_or(serde_json::Value::Null),
        api_version: ApiVersion::V1,
        mode: None,
        taker: None,
    });

    capture_swap_outcome(
        analytics,
        analytics_context,
        process_swap_quote(ds, req),
        |context, error| swap_quote_failed_event(key, context.failure(), error),
        |_context, response| {
            swap_quoted_event(
                key,
                response.input_token,
                response.output_token,
                serde_json::to_value(response.denomination).unwrap_or(serde_json::Value::Null),
                response,
            )
        },
    )
    .await
}

async fn handle_swap_quote_v2(
    ds: &dyn SwapDataSource,
    key: &AuthenticatedKey,
    analytics: &Analytics,
    api_version: ApiVersion,
    req: SwapQuoteV2Request,
) -> Result<SwapQuoteV2Response, ApiError> {
    let analytics_context = snapshot_swap_context(analytics, || SwapAnalyticsContext {
        chain_id: req.chain_id,
        input_token: req.input_token,
        output_token: req.output_token,
        requested_amount: req.amount.clone(),
        denomination: serde_json::to_value(req.denomination).unwrap_or(serde_json::Value::Null),
        api_version,
        mode: serde_json::to_value(req.mode).ok(),
        taker: None,
    });

    capture_swap_outcome(
        analytics,
        analytics_context,
        process_swap_quote_v2(ds, req),
        |context, error| swap_quote_failed_event(key, context.failure(), error),
        |_context, response| swap_quoted_v2_event(key, response),
    )
    .await
}

async fn process_swap_quote(
    ds: &dyn SwapDataSource,
    req: SwapQuoteRequest,
) -> Result<SwapQuoteResponse, ApiError> {
    let chain_id = request_chain_id(req.chain_id)?;
    ensure_distinct_tokens(req.input_token, req.output_token)?;

    ds.validate_supported_tokens_on_chain(chain_id, req.input_token, req.output_token)
        .await
        .map_err(|error| map_quote_boundary_error(error, ApiErrorCode::SwapQuoteFailed))?;

    let orders = ds
        .get_orders_for_pair_on_chain(chain_id, req.input_token, req.output_token)
        .await
        .map_err(|error| map_quote_boundary_error(error, ApiErrorCode::OrdersQueryFailed))?;

    if orders.is_empty() {
        return Err(no_liquidity_error());
    }

    let SwapCandidateBuild {
        candidates,
        failures,
    } = ds
        .build_candidates_for_pair(&orders, req.input_token, req.output_token, Address::ZERO)
        .await
        .map_err(|error| map_quote_boundary_error(error, ApiErrorCode::SwapQuoteFailed))?;

    if candidates.is_empty() {
        return Err(failures
            .oracle_unavailable_error()
            .unwrap_or_else(no_liquidity_error));
    }

    let buy_target = Float::parse(req.output_amount.clone()).map_err(|e| {
        tracing::error!(error = %e, "failed to parse output_amount");
        ApiError::BadRequest("invalid output_amount".into())
    })?;

    let price_cap = Float::max_positive_value().map_err(|e| {
        tracing::error!(error = %e, "failed to create price cap");
        quote_failed_error()
    })?;

    let sim = simulate_buy_over_candidates(candidates, buy_target, price_cap).map_err(|e| {
        tracing::error!(error = %e, "failed to simulate swap");
        quote_failed_error()
    })?;

    if sim.legs.is_empty() {
        return Err(failures
            .oracle_unavailable_error()
            .unwrap_or_else(no_liquidity_error));
    }

    if !is_quote_fully_filled(sim.total_output, buy_target, false)? {
        if let Some(error) = failures.oracle_unavailable_error() {
            return Err(error);
        }
    }

    let (estimated_input, estimated_output) = normalize_quote_amounts(
        ds,
        chain_id,
        req.denomination,
        req.input_token,
        req.output_token,
        sim.total_input,
        sim.total_output,
    )
    .await
    .map_err(|error| map_quote_boundary_error(error, ApiErrorCode::SwapQuoteFailed))?;

    let blended_ratio = estimated_input.div(estimated_output).map_err(|e| {
        tracing::error!(error = %e, "failed to compute blended ratio");
        quote_failed_error()
    })?;

    let formatted_output = estimated_output.format().map_err(|e| {
        tracing::error!(error = %e, "failed to format estimated output");
        quote_failed_error()
    })?;

    let formatted_input = estimated_input.format().map_err(|e| {
        tracing::error!(error = %e, "failed to format estimated input");
        quote_failed_error()
    })?;

    let formatted_ratio = blended_ratio.format().map_err(|e| {
        tracing::error!(error = %e, "failed to format ratio");
        quote_failed_error()
    })?;

    Ok(SwapQuoteResponse {
        chain_id,
        input_token: req.input_token,
        output_token: req.output_token,
        output_amount: req.output_amount,
        denomination: req.denomination,
        estimated_output: formatted_output,
        estimated_input: formatted_input,
        estimated_io_ratio: formatted_ratio,
    })
}

async fn process_swap_quote_v2(
    ds: &dyn SwapDataSource,
    req: SwapQuoteV2Request,
) -> Result<SwapQuoteV2Response, ApiError> {
    let chain_id = request_chain_id(req.chain_id)?;
    ensure_distinct_tokens(req.input_token, req.output_token)?;
    let price_limit = validate_quote_v2_price_limit(&req)?;

    ds.validate_supported_tokens_on_chain(chain_id, req.input_token, req.output_token)
        .await
        .map_err(|error| map_quote_boundary_error(error, ApiErrorCode::SwapQuoteFailed))?;

    let mode: TakeOrdersMode = req.mode.into();
    let (amount, wrap_ratios) = normalize_calldata_request_amount(
        ds,
        CalldataAmountNormalization {
            chain_id,
            denomination: req.denomination,
            input_token: req.input_token,
            output_token: req.output_token,
            mode,
            amount: req.amount.clone(),
            amount_field: "amount",
        },
    )
    .await
    .map_err(|error| map_quote_boundary_error(error, ApiErrorCode::SwapQuoteFailed))?;
    let parsed_mode =
        ParsedTakeOrdersMode::parse(mode, &amount).map_err(super::map_raindex_error)?;

    let orders = ds
        .get_orders_for_pair_on_chain(chain_id, req.input_token, req.output_token)
        .await
        .map_err(|error| map_quote_boundary_error(error, ApiErrorCode::OrdersQueryFailed))?;
    if orders.is_empty() {
        return Err(no_liquidity_error());
    }

    let SwapCandidateBuild {
        candidates,
        failures,
    } = ds
        .build_candidates_for_pair(
            &orders,
            req.input_token,
            req.output_token,
            req.taker.unwrap_or(Address::ZERO),
        )
        .await
        .map_err(|error| map_quote_boundary_error(error, ApiErrorCode::SwapQuoteFailed))?;
    if candidates.is_empty() {
        return Err(failures
            .oracle_unavailable_error()
            .unwrap_or_else(no_liquidity_error));
    }

    let price_cap = match price_limit {
        QuoteV2PriceLimit::Fixed(price_cap) => {
            let normalized = normalize_calldata_price_cap(
                price_cap.to_string(),
                "price_cap",
                req.denomination,
                req.input_token,
                req.output_token,
                &wrap_ratios,
            )?;
            Float::parse(normalized).map_err(|error| {
                tracing::warn!(%error, "swap quote rejected for invalid price_cap");
                ApiError::BadRequest("invalid price_cap".into())
            })?
        }
        QuoteV2PriceLimit::Slippage {
            slippage_bps,
            reference_io_ratio,
        } => {
            // An order whose oracle can't be fetched can't be taken either, so the
            // executable candidates are the whole executable book: price from them.
            // (River 7 Oct: one NVDA order with a retired oracle URL 503'd every NVDA
            // quote while a live order could fill.) Shortfalls still report the oracle.
            let reference_io_ratio = reference_io_ratio
                .map(|reference_io_ratio| {
                    normalize_calldata_price_cap(
                        reference_io_ratio.to_string(),
                        "reference_io_ratio",
                        req.denomination,
                        req.input_token,
                        req.output_token,
                        &wrap_ratios,
                    )
                    .and_then(|normalized| {
                        Float::parse(normalized).map_err(|error| {
                            tracing::warn!(
                                %error,
                                "swap quote rejected for invalid reference_io_ratio"
                            );
                            ApiError::BadRequest("invalid reference_io_ratio".into())
                        })
                    })
                })
                .transpose()?;
            super::slippage::resolve_slippage_price_cap(
                candidates.clone(),
                mode,
                &amount,
                slippage_bps,
                reference_io_ratio,
            )?
        }
    };

    let is_buy_mode = parsed_mode.is_buy_mode();
    let is_exact_mode = parsed_mode.is_exact_mode();
    let target_amount = parsed_mode.target_amount();
    let simulation =
        super::slippage::select_best_raindex_simulation(candidates, parsed_mode, price_cap)
            .map_err(|error| {
                if matches!(
                    &error,
                    ApiError::Coded {
                        code: ApiErrorCode::SwapNoLiquidity,
                        ..
                    }
                ) {
                    failures.oracle_unavailable_error().unwrap_or(error)
                } else {
                    error
                }
            })?;
    let achieved_amount = if is_buy_mode {
        simulation.total_output
    } else {
        simulation.total_input
    };
    let fully_filled = is_quote_fully_filled(achieved_amount, target_amount, is_exact_mode)?;
    if !fully_filled {
        let requested = target_amount.format().map_err(|error| {
            tracing::error!(%error, "failed to format requested quote amount");
            quote_failed_error()
        })?;
        let available = achieved_amount.format().map_err(|error| {
            tracing::error!(%error, "failed to format available quote amount");
            quote_failed_error()
        })?;
        tracing::warn!(%requested, %available, "insufficient executable liquidity");
        if let Some(error) = failures.oracle_unavailable_error() {
            return Err(error);
        }
        if is_exact_mode {
            return Err(no_liquidity_error());
        }
    }

    let (estimated_input, estimated_output) = normalize_quote_amounts(
        ds,
        chain_id,
        req.denomination,
        req.input_token,
        req.output_token,
        simulation.total_input,
        simulation.total_output,
    )
    .await
    .map_err(|error| map_quote_boundary_error(error, ApiErrorCode::SwapQuoteFailed))?;
    let estimated_io_ratio = estimated_input.div(estimated_output).map_err(|error| {
        tracing::error!(%error, "failed to compute v2 swap quote ratio");
        quote_failed_error()
    })?;
    let resolved_price_cap = denormalize_calldata_price_cap(
        price_cap,
        req.denomination,
        req.input_token,
        req.output_token,
        &wrap_ratios,
    )
    .map_err(|error| map_quote_boundary_error(error, ApiErrorCode::SwapQuoteFailed))?;

    let format_estimate = |value: Float, label: &str| {
        value.format().map_err(|error| {
            tracing::error!(%error, label, "failed to format v2 swap quote");
            quote_failed_error()
        })
    };

    Ok(SwapQuoteV2Response {
        chain_id,
        input_token: req.input_token,
        output_token: req.output_token,
        mode: req.mode,
        amount: req.amount,
        denomination: req.denomination,
        estimated_input: format_estimate(estimated_input, "estimated input")?,
        estimated_output: format_estimate(estimated_output, "estimated output")?,
        estimated_io_ratio: format_estimate(estimated_io_ratio, "estimated IO ratio")?,
        fully_filled,
        resolved_price_cap,
    })
}

fn is_quote_fully_filled(
    achieved: Float,
    target: Float,
    is_exact_mode: bool,
) -> Result<bool, ApiError> {
    if is_exact_mode {
        return achieved.eq(target).map_err(|error| {
            tracing::error!(%error, "failed to compare exact swap quote fill amount");
            quote_failed_error()
        });
    }

    let relative_shortfall = target
        .sub(achieved)
        .and_then(|shortfall| shortfall.div(target))
        .map_err(|error| {
            tracing::error!(%error, "failed to compute relative swap quote fill shortfall");
            quote_failed_error()
        })?;
    relative_shortfall
        .lte(FULL_FILL_RELATIVE_TOLERANCE)
        .map_err(|error| {
            tracing::error!(%error, "failed to compare relative swap quote fill shortfall");
            quote_failed_error()
        })
}

enum QuoteV2PriceLimit<'a> {
    Fixed(&'a str),
    Slippage {
        slippage_bps: u16,
        reference_io_ratio: Option<&'a str>,
    },
}

fn validate_quote_v2_price_limit(
    req: &SwapQuoteV2Request,
) -> Result<QuoteV2PriceLimit<'_>, ApiError> {
    match (
        req.price_cap.as_deref(),
        req.slippage_bps,
        req.reference_io_ratio.as_deref(),
    ) {
        (Some(price_cap), None, None) => Ok(QuoteV2PriceLimit::Fixed(price_cap)),
        (Some(_), None, Some(_)) => {
            tracing::warn!(
                "swap quote rejected because reference_io_ratio was provided without slippage_bps"
            );
            Err(ApiError::BadRequest(
                "reference_io_ratio requires slippage_bps".into(),
            ))
        }
        (None, Some(slippage_bps @ 1..=5000), reference_io_ratio) => {
            Ok(QuoteV2PriceLimit::Slippage {
                slippage_bps,
                reference_io_ratio,
            })
        }
        (None, Some(_), _) => {
            tracing::warn!("swap quote rejected for out-of-range slippage_bps");
            Err(ApiError::BadRequest(
                "slippage_bps must be between 1 and 5000".into(),
            ))
        }
        _ => {
            tracing::warn!("swap quote rejected without exactly one price limit");
            Err(ApiError::BadRequest(
                "provide exactly one of price_cap or slippage_bps".into(),
            ))
        }
    }
}

fn quote_failed_error() -> ApiError {
    ApiError::coded(
        ApiErrorCode::SwapQuoteFailed,
        "the swap quote could not be generated",
    )
}

fn map_quote_boundary_error(error: ApiError, fallback_code: ApiErrorCode) -> ApiError {
    if matches!(error, ApiError::Coded { .. } | ApiError::BadRequest(_)) {
        return error;
    }
    tracing::error!(%error, code = %fallback_code, "swap quote boundary failed");
    match fallback_code {
        ApiErrorCode::OrdersQueryFailed => ApiError::coded(
            fallback_code,
            "the order source could not serve this request",
        ),
        _ => quote_failed_error(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analytics::{Analytics, RecordingSink};
    use crate::auth::AuthenticatedKey;
    use crate::routes::swap::test_fixtures::MockSwapDataSource;
    use crate::test_helpers::{mock_candidate, mock_order, TestClientBuilder};
    use crate::types::swap::{SwapCalldataMode, SwapDenomination};
    use crate::wrap_ratio::WrapRatioValue;
    use alloy::primitives::address;
    use async_trait::async_trait;
    use rocket::http::{ContentType, Status};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    const USDC: alloy::primitives::Address = address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");
    const WETH: alloy::primitives::Address = address!("4200000000000000000000000000000000000006");
    const TAKER: alloy::primitives::Address = address!("1111111111111111111111111111111111111111");

    fn test_key() -> AuthenticatedKey {
        AuthenticatedKey {
            id: 1,
            key_id: "test-client".to_string(),
            label: "Test client".to_string(),
            owner: "test-owner".to_string(),
            is_admin: false,
            swap_max_concurrent: None,
        }
    }

    fn quote_request(output_amount: &str) -> SwapQuoteRequest {
        SwapQuoteRequest {
            chain_id: Some(8453),
            input_token: USDC,
            output_token: WETH,
            output_amount: output_amount.to_string(),
            denomination: SwapDenomination::Wrapped,
        }
    }

    fn quote_v2_request(mode: SwapCalldataMode, amount: &str) -> SwapQuoteV2Request {
        SwapQuoteV2Request {
            chain_id: Some(8453),
            taker: None,
            input_token: USDC,
            output_token: WETH,
            mode,
            amount: amount.to_string(),
            price_cap: Some("2".to_string()),
            slippage_bps: None,
            reference_io_ratio: None,
            denomination: SwapDenomination::Wrapped,
        }
    }

    #[test]
    fn test_full_fill_tolerance_accepts_incident_rounding_dust() {
        let achieved = Float::parse(
            "0.04999999999999999999999999999999999999999999999999999999999999999999".to_string(),
        )
        .unwrap();
        let target = Float::parse("0.05".to_string()).unwrap();

        assert!(is_quote_fully_filled(achieved, target, false).unwrap());
        assert!(!is_quote_fully_filled(achieved, target, true).unwrap());
    }

    #[test]
    fn test_full_fill_tolerance_rejects_incident_executable_shortfall() {
        let achieved = Float::parse(
            "0.04310434222334697689407343024988324343123847171843280166595003948319".to_string(),
        )
        .unwrap();
        let target = Float::parse("0.05".to_string()).unwrap();

        assert!(!is_quote_fully_filled(achieved, target, false).unwrap());
    }

    #[test]
    fn test_full_fill_tolerance_accepts_boundary_and_overfill() {
        let target = Float::parse("1".to_string()).unwrap();
        let boundary = Float::parse("0.999999999".to_string()).unwrap();
        let beyond = Float::parse("0.9999999989".to_string()).unwrap();
        let overfill = Float::parse("1.01".to_string()).unwrap();

        assert!(is_quote_fully_filled(boundary, target, false).unwrap());
        assert!(!is_quote_fully_filled(beyond, target, false).unwrap());
        assert!(is_quote_fully_filled(overfill, target, false).unwrap());
    }

    fn candidate_outcome_data_source(
        candidates: Vec<rain_orderbook_common::take_orders::TakeOrderCandidate>,
        failures: Vec<super::super::SwapQuoteFailure>,
    ) -> CandidateOutcomeDataSource {
        CandidateOutcomeDataSource {
            base: MockSwapDataSource {
                supported_tokens: Ok(()),
                orders: Ok(vec![mock_order()]),
                candidates,
                calldata_result: Err(ApiError::Internal("unused".into())),
            },
            failures: failures.into_iter().collect(),
        }
    }

    fn assert_error_code<T>(result: Result<T, ApiError>, expected: ApiErrorCode) {
        assert!(matches!(
            result,
            Err(ApiError::Coded { code, .. }) if code == expected
        ));
    }

    fn unwrapped_quote_request(
        input_token: alloy::primitives::Address,
        output_token: alloy::primitives::Address,
        output_amount: &str,
    ) -> SwapQuoteRequest {
        SwapQuoteRequest {
            chain_id: Some(8453),
            input_token,
            output_token,
            output_amount: output_amount.to_string(),
            denomination: SwapDenomination::Unwrapped,
        }
    }

    fn wrap_ratio(
        share_address: alloy::primitives::Address,
        assets_per_share: &str,
    ) -> WrapRatioValue {
        WrapRatioValue {
            share_address,
            assets_per_share: assets_per_share.to_string(),
        }
    }

    struct MockQuoteDataSource {
        base: MockSwapDataSource,
        wrap_ratios: HashMap<alloy::primitives::Address, WrapRatioValue>,
    }

    struct RecordingCounterpartyDataSource {
        base: MockSwapDataSource,
        counterparties: Mutex<Vec<alloy::primitives::Address>>,
    }

    struct CandidateOutcomeDataSource {
        base: MockSwapDataSource,
        failures: super::super::SwapQuoteFailures,
    }

    #[async_trait]
    impl SwapDataSource for CandidateOutcomeDataSource {
        async fn validate_supported_tokens(
            &self,
            input_token: alloy::primitives::Address,
            output_token: alloy::primitives::Address,
        ) -> Result<(), ApiError> {
            self.base
                .validate_supported_tokens(input_token, output_token)
                .await
        }

        async fn get_orders_for_pair(
            &self,
            input_token: alloy::primitives::Address,
            output_token: alloy::primitives::Address,
        ) -> Result<Vec<rain_orderbook_common::raindex_client::orders::RaindexOrder>, ApiError>
        {
            self.base
                .get_orders_for_pair(input_token, output_token)
                .await
        }

        async fn build_candidates_for_pair(
            &self,
            _orders: &[rain_orderbook_common::raindex_client::orders::RaindexOrder],
            _input_token: alloy::primitives::Address,
            _output_token: alloy::primitives::Address,
            _counterparty: alloy::primitives::Address,
        ) -> Result<super::super::SwapCandidateBuild, ApiError> {
            Ok(super::super::SwapCandidateBuild {
                candidates: self.base.candidates.clone(),
                failures: self.failures.clone(),
            })
        }

        async fn get_calldata(
            &self,
            request: rain_orderbook_common::raindex_client::take_orders::TakeOrdersRequest,
        ) -> Result<crate::types::swap::SwapCalldataResponse, ApiError> {
            self.base.get_calldata(request).await
        }
    }

    #[async_trait]
    impl SwapDataSource for RecordingCounterpartyDataSource {
        async fn validate_supported_tokens(
            &self,
            input_token: alloy::primitives::Address,
            output_token: alloy::primitives::Address,
        ) -> Result<(), ApiError> {
            self.base
                .validate_supported_tokens(input_token, output_token)
                .await
        }

        async fn get_orders_for_pair(
            &self,
            input_token: alloy::primitives::Address,
            output_token: alloy::primitives::Address,
        ) -> Result<Vec<rain_orderbook_common::raindex_client::orders::RaindexOrder>, ApiError>
        {
            self.base
                .get_orders_for_pair(input_token, output_token)
                .await
        }

        async fn build_candidates_for_pair(
            &self,
            orders: &[rain_orderbook_common::raindex_client::orders::RaindexOrder],
            input_token: alloy::primitives::Address,
            output_token: alloy::primitives::Address,
            counterparty: alloy::primitives::Address,
        ) -> Result<super::super::SwapCandidateBuild, ApiError> {
            self.counterparties
                .lock()
                .map_err(|_| ApiError::Internal("counterparty recorder poisoned".into()))?
                .push(counterparty);
            self.base
                .build_candidates_for_pair(orders, input_token, output_token, counterparty)
                .await
        }

        async fn get_calldata(
            &self,
            request: rain_orderbook_common::raindex_client::take_orders::TakeOrdersRequest,
        ) -> Result<crate::types::swap::SwapCalldataResponse, ApiError> {
            self.base.get_calldata(request).await
        }
    }

    #[async_trait]
    impl SwapDataSource for MockQuoteDataSource {
        async fn validate_supported_tokens(
            &self,
            input_token: alloy::primitives::Address,
            output_token: alloy::primitives::Address,
        ) -> Result<(), ApiError> {
            self.base
                .validate_supported_tokens(input_token, output_token)
                .await
        }

        async fn get_orders_for_pair(
            &self,
            input_token: alloy::primitives::Address,
            output_token: alloy::primitives::Address,
        ) -> Result<Vec<rain_orderbook_common::raindex_client::orders::RaindexOrder>, ApiError>
        {
            self.base
                .get_orders_for_pair(input_token, output_token)
                .await
        }

        async fn build_candidates_for_pair(
            &self,
            orders: &[rain_orderbook_common::raindex_client::orders::RaindexOrder],
            input_token: alloy::primitives::Address,
            output_token: alloy::primitives::Address,
            counterparty: alloy::primitives::Address,
        ) -> Result<super::super::SwapCandidateBuild, ApiError> {
            self.base
                .build_candidates_for_pair(orders, input_token, output_token, counterparty)
                .await
        }

        async fn get_calldata(
            &self,
            request: rain_orderbook_common::raindex_client::take_orders::TakeOrdersRequest,
        ) -> Result<crate::types::swap::SwapCalldataResponse, ApiError> {
            self.base.get_calldata(request).await
        }

        async fn get_wrap_ratios_for_tokens(
            &self,
            token_addresses: &[alloy::primitives::Address],
        ) -> Result<HashMap<alloy::primitives::Address, WrapRatioValue>, ApiError> {
            Ok(token_addresses
                .iter()
                .filter_map(|address| {
                    self.wrap_ratios
                        .get(address)
                        .map(|ratio| (*address, ratio.clone()))
                })
                .collect())
        }
    }

    #[rocket::async_test]
    async fn test_process_swap_quote_success() {
        let ds = MockSwapDataSource {
            supported_tokens: Ok(()),
            orders: Ok(vec![mock_order()]),
            candidates: vec![mock_candidate("1000", "1.5")],
            calldata_result: Err(ApiError::Internal("unused".into())),
        };
        let result = process_swap_quote(&ds, quote_request("100")).await.unwrap();

        assert_eq!(result.input_token, USDC);
        assert_eq!(result.output_token, WETH);
        assert_eq!(result.output_amount, "100");
        assert_eq!(result.denomination, SwapDenomination::Wrapped);
        assert_eq!(result.estimated_output, "100");
        assert_eq!(result.estimated_input, "150");
        assert_eq!(result.estimated_io_ratio, "1.5");
    }

    #[rocket::async_test]
    async fn test_process_swap_quote_v2_buy_up_to() {
        let ds = MockSwapDataSource {
            supported_tokens: Ok(()),
            orders: Ok(vec![mock_order()]),
            candidates: vec![mock_candidate("5", "1"), mock_candidate("5", "2")],
            calldata_result: Err(ApiError::Internal("unused".into())),
        };

        let result = process_swap_quote_v2(&ds, quote_v2_request(SwapCalldataMode::BuyUpTo, "8"))
            .await
            .unwrap();

        assert_eq!(result.estimated_input, "11");
        assert_eq!(result.estimated_output, "8");
        assert_eq!(result.estimated_io_ratio, "1.375");
        assert!(result.fully_filled);
        assert_eq!(result.resolved_price_cap, "2");
    }

    #[rocket::async_test]
    async fn test_process_swap_quote_v2_spend_up_to() {
        let ds = MockSwapDataSource {
            supported_tokens: Ok(()),
            orders: Ok(vec![mock_order()]),
            candidates: vec![mock_candidate("5", "1"), mock_candidate("5", "2")],
            calldata_result: Err(ApiError::Internal("unused".into())),
        };

        let result = process_swap_quote_v2(&ds, quote_v2_request(SwapCalldataMode::SpendUpTo, "8"))
            .await
            .unwrap();

        assert_eq!(result.estimated_input, "8");
        assert_eq!(result.estimated_output, "6.5");
        assert_eq!(
            result.estimated_io_ratio,
            "1.2307692307692307692307692307692307692307692307692307692307692307692"
        );
        assert!(result.fully_filled);
    }

    #[rocket::async_test]
    async fn test_process_swap_quote_v2_reports_partial_up_to_fill() {
        let ds = MockSwapDataSource {
            supported_tokens: Ok(()),
            orders: Ok(vec![mock_order()]),
            candidates: vec![mock_candidate("3", "2")],
            calldata_result: Err(ApiError::Internal("unused".into())),
        };

        let result = process_swap_quote_v2(&ds, quote_v2_request(SwapCalldataMode::BuyUpTo, "5"))
            .await
            .unwrap();

        assert_eq!(result.estimated_input, "6");
        assert_eq!(result.estimated_output, "3");
        assert!(!result.fully_filled);
    }

    #[rocket::async_test]
    async fn test_process_swap_quote_v2_rejects_partial_exact_fill() {
        let ds = MockSwapDataSource {
            supported_tokens: Ok(()),
            orders: Ok(vec![mock_order()]),
            candidates: vec![mock_candidate("3", "2")],
            calldata_result: Err(ApiError::Internal("unused".into())),
        };

        let result =
            process_swap_quote_v2(&ds, quote_v2_request(SwapCalldataMode::SpendExact, "8")).await;

        assert_error_code(result, ApiErrorCode::SwapNoLiquidity);
    }

    #[rocket::async_test]
    async fn test_process_swap_quote_v2_resolves_same_slippage_cap_as_calldata() {
        let ds = MockSwapDataSource {
            supported_tokens: Ok(()),
            orders: Ok(vec![mock_order()]),
            candidates: vec![mock_candidate("5", "1"), mock_candidate("5", "2")],
            calldata_result: Err(ApiError::Internal("unused".into())),
        };
        let mut request = quote_v2_request(SwapCalldataMode::BuyUpTo, "8");
        request.price_cap = None;
        request.slippage_bps = Some(100);

        let result = process_swap_quote_v2(&ds, request).await.unwrap();

        assert_eq!(result.resolved_price_cap, "2.02");
        assert_eq!(result.estimated_input, "11");
        assert_eq!(result.estimated_output, "8");
    }

    #[rocket::async_test]
    async fn test_process_swap_quote_v2_uses_taker_for_oracle_candidates() {
        let taker = address!("1234567890AbcdEF1234567890aBcdef12345678");
        let ds = RecordingCounterpartyDataSource {
            base: MockSwapDataSource {
                supported_tokens: Ok(()),
                orders: Ok(vec![mock_order()]),
                candidates: vec![mock_candidate("5", "1")],
                calldata_result: Err(ApiError::Internal("unused".into())),
            },
            counterparties: Mutex::new(Vec::new()),
        };
        let mut request = quote_v2_request(SwapCalldataMode::BuyUpTo, "5");
        request.taker = Some(taker);

        process_swap_quote_v2(&ds, request).await.unwrap();

        assert_eq!(
            *ds.counterparties.lock().unwrap(),
            vec![taker],
            "quote candidates must use the wallet context passed by the website"
        );
    }

    #[rocket::async_test]
    async fn test_process_swap_quote_v2_validates_price_limit_options() {
        let ds = MockSwapDataSource {
            supported_tokens: Err(ApiError::Internal("data source must not be reached".into())),
            orders: Err(ApiError::Internal("data source must not be reached".into())),
            candidates: vec![],
            calldata_result: Err(ApiError::Internal("data source must not be reached".into())),
        };

        let mut both = quote_v2_request(SwapCalldataMode::BuyUpTo, "5");
        both.slippage_bps = Some(100);
        assert!(matches!(
            process_swap_quote_v2(&ds, both).await,
            Err(ApiError::BadRequest(message))
                if message.contains("exactly one")
        ));

        let mut neither = quote_v2_request(SwapCalldataMode::BuyUpTo, "5");
        neither.price_cap = None;
        assert!(matches!(
            process_swap_quote_v2(&ds, neither).await,
            Err(ApiError::BadRequest(message))
                if message.contains("exactly one")
        ));

        let mut out_of_range = quote_v2_request(SwapCalldataMode::BuyUpTo, "5");
        out_of_range.price_cap = None;
        out_of_range.slippage_bps = Some(5001);
        assert!(matches!(
            process_swap_quote_v2(&ds, out_of_range).await,
            Err(ApiError::BadRequest(message))
                if message.contains("between 1 and 5000")
        ));

        let mut reference_without_slippage = quote_v2_request(SwapCalldataMode::BuyUpTo, "5");
        reference_without_slippage.reference_io_ratio = Some("1".to_string());
        assert!(matches!(
            process_swap_quote_v2(&ds, reference_without_slippage).await,
            Err(ApiError::BadRequest(message))
                if message.contains("requires slippage_bps")
        ));
    }

    #[rocket::async_test]
    async fn test_handle_swap_quote_v2_captures_analytics() {
        let ds = MockSwapDataSource {
            supported_tokens: Ok(()),
            orders: Ok(vec![mock_order()]),
            candidates: vec![mock_candidate("5", "1")],
            calldata_result: Err(ApiError::Internal("unused".into())),
        };
        let recording = RecordingSink::new();
        let analytics = Analytics::new(Arc::new(recording.clone()));

        let response = handle_swap_quote_v2(
            &ds,
            &test_key(),
            &analytics,
            ApiVersion::V2,
            quote_v2_request(SwapCalldataMode::BuyUpTo, "5"),
        )
        .await
        .expect("successful quote");

        assert!(response.fully_filled);
        let event = recording
            .events()
            .into_iter()
            .find(|event| event.event == "swap_quoted")
            .expect("swap_quoted event");
        assert_eq!(event.distinct_id, "client:test-client");
        assert_eq!(event.properties["estimated_output"], "5");
        assert_eq!(event.properties["fully_filled"], true);
        assert_eq!(event.properties["api_version"], "v2");
    }

    #[rocket::async_test]
    async fn test_handle_swap_quote_captures_analytics() {
        let ds = MockSwapDataSource {
            supported_tokens: Ok(()),
            orders: Ok(vec![mock_order()]),
            candidates: vec![mock_candidate("1000", "1.5")],
            calldata_result: Err(ApiError::Internal("unused".into())),
        };
        let recording = RecordingSink::new();
        let analytics = Analytics::new(Arc::new(recording.clone()));

        let response = handle_swap_quote(&ds, &test_key(), &analytics, quote_request("100"))
            .await
            .expect("successful quote");

        assert_eq!(response.estimated_input, "150");
        let event = recording
            .events()
            .into_iter()
            .find(|event| event.event == "swap_quoted")
            .expect("swap_quoted event");
        assert_eq!(event.distinct_id, "client:test-client");
        assert_eq!(event.properties["estimated_input"], "150");
        assert_eq!(event.properties["api_client_owner"], "test-owner");
        assert_eq!(event.properties["api_version"], "v1");
    }

    /// A failing quote must still report the pair that failed. Without this the only
    /// signal is an `api_request` 404 with no tokens on it, which is what made a
    /// 100%-failing integrator indistinguishable from one sending no traffic.
    #[rocket::async_test]
    async fn test_handle_swap_quote_captures_failure_analytics() {
        let ds = MockSwapDataSource {
            supported_tokens: Ok(()),
            orders: Ok(vec![]), // no orders for the pair -> SWAP_NO_LIQUIDITY
            candidates: vec![],
            calldata_result: Err(ApiError::Internal("unused".into())),
        };
        let recording = RecordingSink::new();
        let analytics = Analytics::new(Arc::new(recording.clone()));

        let result = handle_swap_quote(&ds, &test_key(), &analytics, quote_request("100")).await;
        assert_error_code(result, ApiErrorCode::SwapNoLiquidity);

        let event = recording
            .events()
            .into_iter()
            .find(|event| event.event == "swap_quote_failed")
            .expect("swap_quote_failed event");
        assert_eq!(event.distinct_id, "client:test-client");
        assert_eq!(event.properties["api_client_owner"], "test-owner");
        assert_eq!(
            event.properties["input_token"],
            USDC.to_string().to_lowercase()
        );
        assert_eq!(
            event.properties["output_token"],
            WETH.to_string().to_lowercase()
        );
        assert_eq!(event.properties["requested_amount"], "100");
        assert_eq!(event.properties["error_code"], "SWAP_NO_LIQUIDITY");
        assert_eq!(event.properties["status_code"], 404);
        assert_eq!(event.properties["same_token"], false);
        assert_eq!(event.properties["api_version"], "v1");
        // No swap_quoted on the failure path: success and failure stay disjoint so a
        // success rate computed from these two events is meaningful.
        assert!(!recording
            .events()
            .iter()
            .any(|event| event.event == "swap_quoted"));
    }

    /// The guard must run before any data-source call: the whole point is to avoid the
    /// full-book RPC sweep a same-token pair would otherwise trigger. A data source
    /// that errors on first use proves it was never reached.
    #[rocket::async_test]
    async fn test_same_token_pair_is_rejected_before_touching_the_data_source() {
        let ds = MockSwapDataSource {
            supported_tokens: Err(ApiError::Internal("data source must not be reached".into())),
            orders: Err(ApiError::Internal("data source must not be reached".into())),
            candidates: vec![],
            calldata_result: Err(ApiError::Internal("unused".into())),
        };
        let mut req = quote_request("100");
        req.output_token = req.input_token;

        assert_error_code(
            process_swap_quote(&ds, req).await,
            ApiErrorCode::SwapSameToken,
        );
    }

    #[rocket::async_test]
    async fn test_same_token_failure_is_flagged_in_analytics() {
        let ds = MockSwapDataSource {
            supported_tokens: Ok(()),
            orders: Ok(vec![mock_order()]),
            candidates: vec![mock_candidate("1000", "1.5")],
            calldata_result: Err(ApiError::Internal("unused".into())),
        };
        let recording = RecordingSink::new();
        let analytics = Analytics::new(Arc::new(recording.clone()));
        let mut req = quote_request("100");
        req.output_token = req.input_token;

        let result = handle_swap_quote(&ds, &test_key(), &analytics, req).await;
        assert_error_code(result, ApiErrorCode::SwapSameToken);

        let event = recording
            .events()
            .into_iter()
            .find(|event| event.event == "swap_quote_failed")
            .expect("swap_quote_failed event");
        assert_eq!(event.properties["same_token"], true);
        assert_eq!(event.properties["error_code"], "SWAP_SAME_TOKEN");
        assert_eq!(event.properties["status_code"], 400);
    }

    #[rocket::async_test]
    async fn test_handle_swap_quote_v2_captures_failure_analytics() {
        let ds = MockSwapDataSource {
            supported_tokens: Ok(()),
            orders: Ok(vec![]),
            candidates: vec![],
            calldata_result: Err(ApiError::Internal("unused".into())),
        };
        let recording = RecordingSink::new();
        let analytics = Analytics::new(Arc::new(recording.clone()));

        let mut request = quote_v2_request(SwapCalldataMode::BuyUpTo, "5");
        request.taker = Some(TAKER);
        let result =
            handle_swap_quote_v2(&ds, &test_key(), &analytics, ApiVersion::V2, request).await;
        assert_error_code(result, ApiErrorCode::SwapNoLiquidity);

        let event = recording
            .events()
            .into_iter()
            .find(|event| event.event == "swap_quote_failed")
            .expect("swap_quote_failed event");
        assert_eq!(event.properties["api_version"], "v2");
        assert_eq!(event.properties["requested_amount"], "5");
        assert_eq!(event.properties["error_code"], "SWAP_NO_LIQUIDITY");
        assert!(event.properties.get("taker").is_none());
    }

    #[rocket::async_test]
    async fn test_process_swap_quote_multi_leg() {
        let ds = MockSwapDataSource {
            supported_tokens: Ok(()),
            orders: Ok(vec![mock_order()]),
            candidates: vec![mock_candidate("50", "2"), mock_candidate("50", "3")],
            calldata_result: Err(ApiError::Internal("unused".into())),
        };
        let result = process_swap_quote(&ds, quote_request("100")).await.unwrap();

        assert_eq!(result.output_amount, "100");
        assert_eq!(result.estimated_output, "100");
        assert_eq!(result.estimated_input, "250");
        assert_eq!(result.estimated_io_ratio, "2.5");
    }

    #[rocket::async_test]
    async fn test_process_swap_quote_partial_fill() {
        let ds = MockSwapDataSource {
            supported_tokens: Ok(()),
            orders: Ok(vec![mock_order()]),
            candidates: vec![mock_candidate("30", "2")],
            calldata_result: Err(ApiError::Internal("unused".into())),
        };
        let result = process_swap_quote(&ds, quote_request("100")).await.unwrap();

        assert_eq!(result.output_amount, "100");
        assert_eq!(result.estimated_output, "30");
        assert_eq!(result.estimated_input, "60");
    }

    #[rocket::async_test]
    async fn test_process_swap_quote_picks_best_ratio() {
        let ds = MockSwapDataSource {
            supported_tokens: Ok(()),
            orders: Ok(vec![mock_order()]),
            candidates: vec![
                mock_candidate("1000", "3"),
                mock_candidate("1000", "1.5"),
                mock_candidate("1000", "2"),
            ],
            calldata_result: Err(ApiError::Internal("unused".into())),
        };
        let result = process_swap_quote(&ds, quote_request("10")).await.unwrap();

        assert_eq!(result.estimated_io_ratio, "1.5");
        assert_eq!(result.estimated_input, "15");
    }

    #[rocket::async_test]
    async fn test_process_swap_quote_unwrapped_converts_input_amount_and_ratio() {
        let wt_mstr = address!("Ff05e1BD696900DC6A52cA35cA61bB1024eDA8e2");
        let ds = MockQuoteDataSource {
            base: MockSwapDataSource {
                supported_tokens: Ok(()),
                orders: Ok(vec![mock_order()]),
                candidates: vec![mock_candidate("1000", "1.5")],
                calldata_result: Err(ApiError::Internal("unused".into())),
            },
            wrap_ratios: HashMap::from([(wt_mstr, wrap_ratio(wt_mstr, "2"))]),
        };

        let result = process_swap_quote(&ds, unwrapped_quote_request(wt_mstr, WETH, "100"))
            .await
            .unwrap();

        assert_eq!(result.denomination, SwapDenomination::Unwrapped);
        assert_eq!(result.output_amount, "100");
        assert_eq!(result.estimated_output, "100");
        assert_eq!(result.estimated_input, "300");
        assert_eq!(result.estimated_io_ratio, "3");
    }

    #[rocket::async_test]
    async fn test_process_swap_quote_unwrapped_converts_output_amount_and_ratio() {
        let wt_mstr = address!("Ff05e1BD696900DC6A52cA35cA61bB1024eDA8e2");
        let ds = MockQuoteDataSource {
            base: MockSwapDataSource {
                supported_tokens: Ok(()),
                orders: Ok(vec![mock_order()]),
                candidates: vec![mock_candidate("1000", "1.5")],
                calldata_result: Err(ApiError::Internal("unused".into())),
            },
            wrap_ratios: HashMap::from([(wt_mstr, wrap_ratio(wt_mstr, "2"))]),
        };

        let result = process_swap_quote(&ds, unwrapped_quote_request(USDC, wt_mstr, "100"))
            .await
            .unwrap();

        assert_eq!(result.denomination, SwapDenomination::Unwrapped);
        assert_eq!(result.output_amount, "100");
        assert_eq!(result.estimated_output, "200");
        assert_eq!(result.estimated_input, "150");
        assert_eq!(result.estimated_io_ratio, "0.75");
    }

    #[rocket::async_test]
    async fn test_process_swap_quote_unwrapped_converts_both_sides() {
        let wt_mstr = address!("Ff05e1BD696900DC6A52cA35cA61bB1024eDA8e2");
        let wt_coin = address!("EeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE");
        let ds = MockQuoteDataSource {
            base: MockSwapDataSource {
                supported_tokens: Ok(()),
                orders: Ok(vec![mock_order()]),
                candidates: vec![mock_candidate("1000", "1.5")],
                calldata_result: Err(ApiError::Internal("unused".into())),
            },
            wrap_ratios: HashMap::from([
                (wt_mstr, wrap_ratio(wt_mstr, "2")),
                (wt_coin, wrap_ratio(wt_coin, "3")),
            ]),
        };

        let result = process_swap_quote(&ds, unwrapped_quote_request(wt_mstr, wt_coin, "100"))
            .await
            .unwrap();

        assert_eq!(result.denomination, SwapDenomination::Unwrapped);
        assert_eq!(result.output_amount, "100");
        assert_eq!(result.estimated_output, "300");
        assert_eq!(result.estimated_input, "300");
        assert_eq!(result.estimated_io_ratio, "1");
    }

    #[rocket::async_test]
    async fn test_process_swap_quote_unwrapped_noop_for_non_wrapped_tokens() {
        let ds = MockQuoteDataSource {
            base: MockSwapDataSource {
                supported_tokens: Ok(()),
                orders: Ok(vec![mock_order()]),
                candidates: vec![mock_candidate("1000", "1.5")],
                calldata_result: Err(ApiError::Internal("unused".into())),
            },
            wrap_ratios: HashMap::new(),
        };

        let result = process_swap_quote(&ds, unwrapped_quote_request(USDC, WETH, "100"))
            .await
            .unwrap();

        assert_eq!(result.denomination, SwapDenomination::Unwrapped);
        assert_eq!(result.output_amount, "100");
        assert_eq!(result.estimated_output, "100");
        assert_eq!(result.estimated_input, "150");
        assert_eq!(result.estimated_io_ratio, "1.5");
    }

    #[rocket::async_test]
    async fn test_process_swap_quote_no_liquidity() {
        let ds = MockSwapDataSource {
            supported_tokens: Ok(()),
            orders: Ok(vec![]),
            candidates: vec![],
            calldata_result: Err(ApiError::Internal("unused".into())),
        };
        let result = process_swap_quote(&ds, quote_request("100")).await;
        assert_error_code(result, ApiErrorCode::SwapNoLiquidity);
    }

    #[rocket::async_test]
    async fn test_process_swap_quote_no_candidates() {
        let ds = MockSwapDataSource {
            supported_tokens: Ok(()),
            orders: Ok(vec![mock_order()]),
            candidates: vec![],
            calldata_result: Err(ApiError::Internal("unused".into())),
        };
        let result = process_swap_quote(&ds, quote_request("100")).await;
        assert_error_code(result, ApiErrorCode::SwapNoLiquidity);
    }

    #[rocket::async_test]
    async fn test_process_swap_quote_reports_oracle_unavailable() {
        let ds = candidate_outcome_data_source(
            Vec::new(),
            vec![super::super::SwapQuoteFailure::OracleUnavailable],
        );

        let result = process_swap_quote(&ds, quote_request("100")).await;

        assert_error_code(result, ApiErrorCode::SwapOracleUnavailable);
    }

    #[rocket::async_test]
    async fn test_process_swap_quote_treats_order_reverts_as_no_liquidity() {
        let ds = candidate_outcome_data_source(
            Vec::new(),
            vec![super::super::SwapQuoteFailure::QuoteFailed],
        );

        let result = process_swap_quote(&ds, quote_request("100")).await;

        assert_error_code(result, ApiErrorCode::SwapNoLiquidity);
    }

    #[rocket::async_test]
    async fn test_process_swap_quote_succeeds_with_executable_and_unavailable_candidates() {
        let ds = candidate_outcome_data_source(
            vec![mock_candidate("1000", "1.5")],
            vec![super::super::SwapQuoteFailure::OracleUnavailable],
        );

        let result = process_swap_quote(&ds, quote_request("100")).await.unwrap();

        assert_eq!(result.estimated_input, "150");
        assert_eq!(result.estimated_output, "100");
    }

    #[rocket::async_test]
    async fn test_process_swap_quote_partial_fill_reports_oracle_unavailable() {
        let ds = candidate_outcome_data_source(
            vec![mock_candidate("3", "2")],
            vec![super::super::SwapQuoteFailure::OracleUnavailable],
        );

        let result = process_swap_quote(&ds, quote_request("8")).await;

        assert_error_code(result, ApiErrorCode::SwapOracleUnavailable);
    }

    #[rocket::async_test]
    async fn test_process_swap_quote_v2_exact_shortfall_reports_oracle_unavailable() {
        let ds = candidate_outcome_data_source(
            vec![mock_candidate("3", "2")],
            vec![super::super::SwapQuoteFailure::OracleUnavailable],
        );

        let result =
            process_swap_quote_v2(&ds, quote_v2_request(SwapCalldataMode::SpendExact, "8")).await;

        assert_error_code(result, ApiErrorCode::SwapOracleUnavailable);
    }

    #[rocket::async_test]
    async fn test_process_swap_quote_v2_incomplete_price_cap_filter_reports_oracle_unavailable() {
        let ds = candidate_outcome_data_source(
            vec![mock_candidate("100", "3")],
            vec![super::super::SwapQuoteFailure::OracleUnavailable],
        );

        let result =
            process_swap_quote_v2(&ds, quote_v2_request(SwapCalldataMode::BuyUpTo, "5")).await;

        assert_error_code(result, ApiErrorCode::SwapOracleUnavailable);
    }

    #[rocket::async_test]
    async fn test_process_swap_quote_v2_up_to_shortfall_reports_oracle_unavailable() {
        let ds = candidate_outcome_data_source(
            vec![mock_candidate("3", "2")],
            vec![super::super::SwapQuoteFailure::OracleUnavailable],
        );

        let result =
            process_swap_quote_v2(&ds, quote_v2_request(SwapCalldataMode::SpendUpTo, "8")).await;

        assert_error_code(result, ApiErrorCode::SwapOracleUnavailable);
    }

    #[rocket::async_test]
    async fn test_process_swap_quote_invalid_output_amount() {
        let ds = MockSwapDataSource {
            supported_tokens: Ok(()),
            orders: Ok(vec![mock_order()]),
            candidates: vec![mock_candidate("1000", "1.5")],
            calldata_result: Err(ApiError::Internal("unused".into())),
        };
        let result = process_swap_quote(&ds, quote_request("not-a-number")).await;
        assert!(matches!(result, Err(ApiError::BadRequest(_))));
    }

    #[rocket::async_test]
    async fn test_process_swap_quote_query_failure() {
        let ds = MockSwapDataSource {
            supported_tokens: Ok(()),
            orders: Err(ApiError::Internal("failed".into())),
            candidates: vec![],
            calldata_result: Err(ApiError::Internal("unused".into())),
        };
        let result = process_swap_quote(&ds, quote_request("100")).await;
        assert_error_code(result, ApiErrorCode::OrdersQueryFailed);
    }

    #[rocket::async_test]
    async fn test_process_swap_quote_rejects_unsupported_tokens() {
        let ds = MockSwapDataSource {
            supported_tokens: Err(ApiError::coded(
                ApiErrorCode::SwapUnsupportedToken,
                "one or both swap tokens are unsupported",
            )),
            orders: Ok(vec![mock_order()]),
            candidates: vec![mock_candidate("1000", "1.5")],
            calldata_result: Err(ApiError::Internal("unused".into())),
        };
        let result = process_swap_quote(&ds, quote_request("100")).await;
        assert_error_code(result, ApiErrorCode::SwapUnsupportedToken);
    }

    #[rocket::async_test]
    async fn test_process_swap_quote_registry_failure_is_not_retryable() {
        let ds = MockSwapDataSource {
            supported_tokens: Err(ApiError::Internal("invalid local registry".into())),
            orders: Ok(vec![mock_order()]),
            candidates: vec![mock_candidate("1000", "1.5")],
            calldata_result: Err(ApiError::Internal("unused".into())),
        };
        let result = process_swap_quote(&ds, quote_request("100")).await;
        assert_error_code(result, ApiErrorCode::SwapQuoteFailed);
    }

    #[rocket::async_test]
    async fn test_swap_quote_401_without_auth() {
        let client = TestClientBuilder::new().build().await;
        let response = client
            .post("/v1/swap/quote")
            .header(ContentType::JSON)
            .body(r#"{"inputToken":"0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913","outputToken":"0x4200000000000000000000000000000000000006","outputAmount":"100"}"#)
            .dispatch()
            .await;
        assert_eq!(response.status(), Status::Unauthorized);
    }

    #[rocket::async_test]
    async fn test_swap_quote_400_for_unsupported_tokens() {
        let client = TestClientBuilder::new().build().await;
        let (key_id, secret) = crate::test_helpers::seed_api_key(&client).await;
        let header = crate::test_helpers::basic_auth_header(&key_id, &secret);
        let response = client
            .post("/v1/swap/quote")
            .header(ContentType::JSON)
            .header(rocket::http::Header::new("Authorization", header))
            .body(r#"{"inputToken":"0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913","outputToken":"0x4200000000000000000000000000000000000006","outputAmount":"100"}"#)
            .dispatch()
            .await;
        assert_eq!(response.status(), Status::BadRequest);
        let body = response.into_json::<ApiErrorResponse>().await.unwrap();
        assert_eq!(body.error.code, ApiErrorCode::SwapUnsupportedToken);
        assert!(!body.request_id.is_empty());
    }

    #[rocket::async_test]
    async fn test_swap_quote_422_for_invalid_denomination() {
        let client = TestClientBuilder::new().build().await;
        let (key_id, secret) = crate::test_helpers::seed_api_key(&client).await;
        let header = crate::test_helpers::basic_auth_header(&key_id, &secret);
        let response = client
            .post("/v1/swap/quote")
            .header(ContentType::JSON)
            .header(rocket::http::Header::new("Authorization", header))
            .body(r#"{"inputToken":"0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913","outputToken":"0x4200000000000000000000000000000000000006","outputAmount":"100","denomination":"invalid"}"#)
            .dispatch()
            .await;
        assert_eq!(response.status(), Status::UnprocessableEntity);
    }

    #[rocket::async_test]
    async fn test_v3_swap_quote_requires_chain_id() {
        let client = TestClientBuilder::new().build().await;
        let (key_id, secret) = crate::test_helpers::seed_api_key(&client).await;
        let header = crate::test_helpers::basic_auth_header(&key_id, &secret);
        let response = client
            .post("/v3/swap/quote")
            .header(ContentType::JSON)
            .header(rocket::http::Header::new("Authorization", header))
            .body(r#"{"inputToken":"0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913","outputToken":"0x4200000000000000000000000000000000000006","mode":"spendExact","amount":"100","priceCap":"2500"}"#)
            .dispatch()
            .await;
        assert_eq!(response.status(), Status::BadRequest);
        let body = response.into_json::<ApiErrorResponse>().await.unwrap();
        assert_eq!(body.error.message, "chainId is required");
    }
}

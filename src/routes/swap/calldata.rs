use super::{
    capture_swap_outcome, ensure_distinct_tokens, exchange_log::SwapExchangeLog,
    no_liquidity_error, request_chain_id, snapshot_swap_context, RaindexSwapDataSource,
    SwapAnalyticsContext, SwapCandidateBuild, SwapDataSource,
};
use crate::analytics::{
    swap_calldata_failed_event, swap_calldata_generated_event, Analytics, ApiVersion,
};
use crate::app_state::ApplicationState;
use crate::attribution::{attribution_message_hash, Attribution, AttributionSigner};
use crate::auth::AuthenticatedKey;
use crate::db::DbPool;
use crate::error::{ApiError, ApiErrorCode, ApiErrorResponse};
use crate::fairings::{GlobalRateLimit, TracingSpan};
use crate::routes::swap::denomination::{
    denormalize_calldata_price_cap, normalize_calldata_price_cap,
    normalize_calldata_request_amount, normalize_calldata_request_values,
    normalize_calldata_response, CalldataAmountNormalization, CalldataRequestNormalization,
};
use crate::swap_capacity::SwapCapacity;
use crate::types::swap::{
    SwapCalldataRequest, SwapCalldataResponse, SwapCalldataV2Request, SwapCalldataV2RequestBody,
    SwapCalldataV2Response,
};
use alloy::primitives::{keccak256, Address, Bytes, Signature, B256};
use alloy::sol_types::{SolCall, SolValue};
use futures::{stream, StreamExt, TryStreamExt};
use rain_math_float::Float;
use rain_orderbook_bindings::IRaindexV6::{takeOrders4Call, OrderV4};
use rain_orderbook_common::oracle::{encode_oracle_body_batch, fetch_signed_context_batch};
use rain_orderbook_common::raindex_client::take_orders::TakeOrdersRequest;
use rain_orderbook_common::take_orders::TakeOrdersMode;
use rocket::serde::json::Json;
use rocket::State;
use std::collections::BTreeMap;
use tracing::Instrument;

const ORACLE_REFRESH_CONCURRENCY_LIMIT: usize = 8;

#[utoipa::path(
    post,
    path = "/v1/swap/calldata",
    tag = "Swap",
    security(("basicAuth" = [])),
    request_body = SwapCalldataRequest,
    responses(
        (status = 200, description = "Swap calldata", body = SwapCalldataResponse),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "No liquidity found", body = ApiErrorResponse),
        (status = 422, description = "Request body could not be parsed", body = ApiErrorResponse),
        (status = 429, description = "Rate limited", body = ApiErrorResponse),
        (status = 500, description = "Internal server error", body = ApiErrorResponse),
        (status = 502, description = "Order source unavailable", body = ApiErrorResponse),
        (status = 503, description = "Required upstream or swap oracle unavailable", body = ApiErrorResponse),
        (status = 504, description = "Swap request timed out", body = ApiErrorResponse),
    )
)]
#[post("/calldata", data = "<request>")]
#[allow(clippy::too_many_arguments)] // Rocket handler: args are guards + managed state + body.
pub async fn post_swap_calldata(
    _global: GlobalRateLimit,
    key: AuthenticatedKey,
    shared_raindex: &State<crate::raindex::SharedRaindexProvider>,
    app_state: &State<ApplicationState>,
    pool: &State<DbPool>,
    analytics: &State<Analytics>,
    capacity: &State<SwapCapacity>,
    span: TracingSpan,
    request: Json<SwapCalldataRequest>,
) -> Result<Json<SwapCalldataResponse>, ApiError> {
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
                "/v1/swap/calldata",
                "v1",
                "calldata",
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
            "/v1/swap/calldata",
            "v1",
            "calldata",
            &key,
            req.input_token,
            req.output_token,
            &req,
        );
        let result = capacity
            .run(key.id, key.swap_max_concurrent, async {
                let attribution = app_state.attribution.for_api_key(&key.key_id, req.taker);
                let raindex = shared_raindex.read().await;
                let ds = RaindexSwapDataSource::new(
                    raindex.client(),
                    &app_state.response_caches,
                    pool.inner(),
                );
                handle_swap_calldata(
                    &ds,
                    &key,
                    analytics.inner(),
                    &app_state.attribution.signer,
                    &attribution,
                    req,
                )
                .await
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
    path = "/v2/swap/calldata",
    tag = "Swap",
    summary = "Build ready-to-send swap calldata",
    description = "Builds SDK calldata for one executable route. Provide exactly one of priceCap or slippageBps. When approvals are returned, submit them and retry with the response's resolvedPriceCap as priceCap so the original limit remains fixed.",
    security(("basicAuth" = [])),
    request_body = SwapCalldataV2RequestBody,
    responses(
        (status = 200, description = "Swap calldata", body = SwapCalldataV2Response),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "No liquidity found", body = ApiErrorResponse),
        (status = 422, description = "Request body could not be parsed", body = ApiErrorResponse),
        (status = 429, description = "Rate limited", body = ApiErrorResponse),
        (status = 500, description = "Internal server error", body = ApiErrorResponse),
        (status = 502, description = "Order source unavailable", body = ApiErrorResponse),
        (status = 503, description = "Required upstream or swap oracle unavailable", body = ApiErrorResponse),
        (status = 504, description = "Swap request timed out", body = ApiErrorResponse),
    )
)]
#[post("/calldata", data = "<request>")]
#[allow(clippy::too_many_arguments)] // Rocket handler: args are guards + managed state + body.
pub async fn post_swap_calldata_v2(
    _global: GlobalRateLimit,
    key: AuthenticatedKey,
    shared_raindex: &State<crate::raindex::SharedRaindexProvider>,
    app_state: &State<ApplicationState>,
    pool: &State<DbPool>,
    analytics: &State<Analytics>,
    capacity: &State<SwapCapacity>,
    span: TracingSpan,
    request: Json<SwapCalldataV2Request>,
) -> Result<Json<SwapCalldataV2Response>, ApiError> {
    let request_id = span.request_id().to_string();
    let api_version = span.api_version();
    let (route_path, version_label, analytics_api_version) = if api_version == Some(3) {
        ("/v3/swap/calldata", "v3", ApiVersion::V3)
    } else {
        ("/v2/swap/calldata", "v2", ApiVersion::V2)
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
                "calldata",
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
            "calldata",
            &key,
            req.input_token,
            req.output_token,
            &req,
        );
        let result = capacity
            .run(key.id, key.swap_max_concurrent, async {
                let attribution = app_state.attribution.for_api_key(&key.key_id, req.taker);
                let raindex = shared_raindex.read().await;
                let ds = RaindexSwapDataSource::new(
                    raindex.client(),
                    &app_state.response_caches,
                    pool.inner(),
                );
                handle_swap_calldata_v2(
                    &ds,
                    &key,
                    analytics.inner(),
                    analytics_api_version,
                    &app_state.attribution.signer,
                    &attribution,
                    req,
                )
                .await
            })
            .await;
        exchange.record(&result);
        result.map(Json)
    }
    .instrument(span.0)
    .await
}

async fn handle_swap_calldata(
    ds: &dyn SwapDataSource,
    key: &AuthenticatedKey,
    analytics: &Analytics,
    signer: &AttributionSigner,
    attribution: &Attribution,
    req: SwapCalldataRequest,
) -> Result<SwapCalldataResponse, ApiError> {
    let taker = req.taker;
    let analytics_context = snapshot_swap_context(analytics, || SwapAnalyticsContext {
        chain_id: req.chain_id,
        taker: Some(taker),
        input_token: req.input_token,
        output_token: req.output_token,
        requested_amount: req.output_amount.clone(),
        denomination: serde_json::to_value(req.denomination).unwrap_or(serde_json::Value::Null),
        api_version: ApiVersion::V1,
        mode: None,
    });

    // Attribution failures are calldata failures too — the caller ends up with no
    // calldata either way — so both fallible steps feed the same failure event.
    capture_swap_outcome(
        analytics,
        analytics_context,
        async {
            let mut response = process_swap_calldata(ds, req).await?;
            embed_and_validate_attribution(&mut response, signer, attribution).await?;
            Ok(response)
        },
        |context, error| swap_calldata_failed_event(key, context.failure(), error),
        |context, response| {
            swap_calldata_generated_event(
                key,
                taker,
                context.input_token,
                context.output_token,
                context.denomination.clone(),
                context.api_version,
                context.mode.clone(),
                response,
            )
        },
    )
    .await
}

async fn handle_swap_calldata_v2(
    ds: &dyn SwapDataSource,
    key: &AuthenticatedKey,
    analytics: &Analytics,
    api_version: ApiVersion,
    signer: &AttributionSigner,
    attribution: &Attribution,
    req: SwapCalldataV2Request,
) -> Result<SwapCalldataV2Response, ApiError> {
    let taker = req.taker;
    let analytics_context = snapshot_swap_context(analytics, || SwapAnalyticsContext {
        chain_id: req.chain_id,
        taker: Some(taker),
        input_token: req.input_token,
        output_token: req.output_token,
        requested_amount: req.amount.clone(),
        denomination: serde_json::to_value(req.denomination).unwrap_or(serde_json::Value::Null),
        api_version,
        mode: serde_json::to_value(req.mode).ok(),
    });

    capture_swap_outcome(
        analytics,
        analytics_context,
        async {
            let mut response = process_swap_calldata_v2(ds, req).await?;
            embed_and_validate_attribution(&mut response.calldata, signer, attribution).await?;
            Ok(response)
        },
        |context, error| swap_calldata_failed_event(key, context.failure(), error),
        |context, response| {
            swap_calldata_generated_event(
                key,
                taker,
                context.input_token,
                context.output_token,
                context.denomination.clone(),
                context.api_version,
                context.mode.clone(),
                &response.calldata,
            )
        },
    )
    .await
}

async fn embed_and_validate_attribution(
    response: &mut SwapCalldataResponse,
    signer: &AttributionSigner,
    attribution: &Attribution,
) -> Result<(), ApiError> {
    if !response.approvals.is_empty() || response.data.is_empty() {
        return validate_attribution_calldata(response, signer.address(), attribution);
    }

    let mut decoded = takeOrders4Call::abi_decode(&response.data).map_err(|error| {
        tracing::error!(
            %error,
            api_key_hash = %attribution.api_key_hash,
            "failed to decode generated takeOrders4 calldata for attribution"
        );
        ApiError::Internal("generated swap calldata is missing attribution".into())
    })?;
    for order_config in &mut decoded.config.orders {
        let order_hash = keccak256(order_config.order.abi_encode());
        let context = signer
            .sign_context(attribution, order_hash)
            .await
            .map_err(|error| {
                tracing::error!(
                    %error,
                    %order_hash,
                    api_key_hash = %attribution.api_key_hash,
                    "failed to sign REST attribution context"
                );
                ApiError::Internal("failed to sign swap calldata attribution".into())
            })?;
        order_config.signedContext.push(context);
    }
    response.data = Bytes::from(decoded.abi_encode());

    validate_attribution_calldata(response, signer.address(), attribution)
}

fn validate_attribution_calldata(
    response: &SwapCalldataResponse,
    signer_address: Address,
    attribution: &Attribution,
) -> Result<(), ApiError> {
    if !response.approvals.is_empty() {
        if !response.data.is_empty() {
            tracing::error!(
                api_key_hash = %attribution.api_key_hash,
                "swap response unexpectedly contains approvals and executable calldata"
            );
            return Err(ApiError::Internal(
                "generated swap calldata has an invalid state".into(),
            ));
        }
        tracing::info!(
            api_key_hash = %attribution.api_key_hash,
            "swap calldata requires approval; executable attribution is not expected"
        );
        return Ok(());
    }

    if response.data.is_empty() {
        tracing::error!(
            api_key_hash = %attribution.api_key_hash,
            "swap response contains neither approvals nor executable calldata"
        );
        return Err(ApiError::Internal(
            "generated swap calldata is missing attribution".into(),
        ));
    }
    let decoded = takeOrders4Call::abi_decode(&response.data).map_err(|error| {
        tracing::error!(
            %error,
            api_key_hash = %attribution.api_key_hash,
            "failed to decode generated takeOrders4 calldata for attribution validation"
        );
        ApiError::Internal("generated swap calldata is missing attribution".into())
    })?;
    if decoded.config.orders.is_empty() {
        tracing::error!(
            api_key_hash = %attribution.api_key_hash,
            "generated takeOrders4 calldata contains no orders"
        );
        return Err(ApiError::Internal(
            "generated swap calldata is missing attribution".into(),
        ));
    }

    for order_config in &decoded.config.orders {
        let order_hash = keccak256(order_config.order.abi_encode());
        let expected_context = attribution.context_for_order(order_hash);
        let mut matching_attribution = order_config.signedContext.iter().filter(|context| {
            context.signer == signer_address && context.context.as_slice() == expected_context
        });
        let first_match = matching_attribution.next();
        let has_duplicate = matching_attribution.next().is_some();
        let signature_is_valid = if let Some(context) = first_match {
            let context_hash = attribution_message_hash(&expected_context);
            Signature::try_from(context.signature.as_ref())
                .and_then(|signature| signature.recover_address_from_msg(context_hash.as_slice()))
                .is_ok_and(|recovered| recovered == signer_address)
        } else {
            false
        };
        if first_match.is_none() || has_duplicate || !signature_is_valid {
            tracing::error!(
                %order_hash,
                api_key_hash = %attribution.api_key_hash,
                signer = %signer_address,
                duplicate_attribution_context = has_duplicate,
                "generated order is missing its exact REST attribution context"
            );
            return Err(ApiError::Internal(
                "generated swap calldata is missing attribution".into(),
            ));
        }
    }

    Ok(())
}

#[derive(Debug)]
struct SwapCalldataBuildRequest {
    chain_id: u32,
    taker: Address,
    input_token: Address,
    output_token: Address,
    mode: TakeOrdersMode,
    amount: String,
    amount_field: &'static str,
    price_limit: SwapCalldataPriceLimit,
    denomination: crate::types::swap::SwapDenomination,
}

#[derive(Debug)]
enum SwapCalldataPriceLimit {
    Explicit {
        value: String,
        field: &'static str,
    },
    SlippageBps {
        slippage_bps: u16,
        reference_io_ratio: Option<String>,
    },
}

struct SwapCalldataBuildResult {
    calldata: SwapCalldataResponse,
    resolved_price_cap: String,
}

impl TryFrom<SwapCalldataRequest> for SwapCalldataBuildRequest {
    type Error = ApiError;

    fn try_from(req: SwapCalldataRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            chain_id: request_chain_id(req.chain_id)?,
            taker: req.taker,
            input_token: req.input_token,
            output_token: req.output_token,
            mode: TakeOrdersMode::BuyUpTo,
            amount: req.output_amount,
            amount_field: "output_amount",
            price_limit: SwapCalldataPriceLimit::Explicit {
                value: req.maximum_io_ratio,
                field: "maximum_io_ratio",
            },
            denomination: req.denomination,
        })
    }
}

impl TryFrom<SwapCalldataV2Request> for SwapCalldataBuildRequest {
    type Error = ApiError;

    fn try_from(req: SwapCalldataV2Request) -> Result<Self, Self::Error> {
        let price_limit = match (req.price_cap, req.slippage_bps, req.reference_io_ratio) {
            (Some(value), None, None) => SwapCalldataPriceLimit::Explicit {
                value,
                field: "price_cap",
            },
            (Some(_), None, Some(_)) => {
                tracing::warn!(
                    "swap calldata rejected because reference_io_ratio was provided without slippage_bps"
                );
                return Err(ApiError::BadRequest(
                    "reference_io_ratio requires slippage_bps".into(),
                ));
            }
            (None, Some(slippage_bps @ 1..=5000), reference_io_ratio) => {
                SwapCalldataPriceLimit::SlippageBps {
                    slippage_bps,
                    reference_io_ratio,
                }
            }
            (None, Some(_), _) => {
                tracing::warn!("swap calldata rejected for out-of-range slippage_bps");
                return Err(ApiError::BadRequest(
                    "slippage_bps must be between 1 and 5000".into(),
                ));
            }
            _ => {
                tracing::warn!("swap calldata rejected without exactly one price limit");
                return Err(ApiError::BadRequest(
                    "provide exactly one of price_cap or slippage_bps".into(),
                ));
            }
        };

        Ok(Self {
            chain_id: request_chain_id(req.chain_id)?,
            taker: req.taker,
            input_token: req.input_token,
            output_token: req.output_token,
            mode: req.mode.into(),
            amount: req.amount,
            amount_field: "amount",
            price_limit,
            denomination: req.denomination,
        })
    }
}

async fn process_swap_calldata(
    ds: &dyn SwapDataSource,
    req: SwapCalldataRequest,
) -> Result<SwapCalldataResponse, ApiError> {
    Ok(process_swap_calldata_build(ds, req.try_into()?)
        .await?
        .calldata)
}

async fn process_swap_calldata_v2(
    ds: &dyn SwapDataSource,
    req: SwapCalldataV2Request,
) -> Result<SwapCalldataV2Response, ApiError> {
    ensure_distinct_tokens(req.input_token, req.output_token)?;
    let result = process_swap_calldata_build(ds, req.try_into()?).await?;
    Ok(SwapCalldataV2Response {
        calldata: result.calldata,
        resolved_price_cap: result.resolved_price_cap,
    })
}

async fn build_calldata_candidates(
    ds: &dyn SwapDataSource,
    chain_id: u32,
    input_token: Address,
    output_token: Address,
    taker: Address,
) -> Result<SwapCandidateBuild, ApiError> {
    let orders = ds
        .get_orders_for_pair_on_chain(chain_id, input_token, output_token)
        .await
        .map_err(map_calldata_boundary_error)?;
    if orders.is_empty() {
        return Err(no_liquidity_error());
    }

    let candidate_build = ds
        .build_candidates_for_pair(&orders, input_token, output_token, taker)
        .await
        .map_err(map_calldata_boundary_error)?;
    // Executable candidates win in a mixed book: an order whose oracle can't be
    // fetched can't be taken, so it can't make the user's price worse.
    if candidate_build.candidates.is_empty() {
        return Err(candidate_build
            .failures
            .oracle_unavailable_error()
            .unwrap_or_else(no_liquidity_error));
    }
    Ok(candidate_build)
}

async fn process_swap_calldata_build(
    ds: &dyn SwapDataSource,
    req: SwapCalldataBuildRequest,
) -> Result<SwapCalldataBuildResult, ApiError> {
    ensure_distinct_tokens(req.input_token, req.output_token)?;

    ds.validate_supported_tokens_on_chain(req.chain_id, req.input_token, req.output_token)
        .await
        .map_err(map_calldata_boundary_error)?;

    let (amount, price_cap, resolved_price_cap, wrap_ratios) = match req.price_limit {
        SwapCalldataPriceLimit::Explicit { value, field } => {
            let resolved_price_cap = value.clone();
            let (amount, price_cap, wrap_ratios) = normalize_calldata_request_values(
                ds,
                CalldataRequestNormalization {
                    chain_id: req.chain_id,
                    denomination: req.denomination,
                    input_token: req.input_token,
                    output_token: req.output_token,
                    mode: req.mode,
                    amount: req.amount,
                    amount_field: req.amount_field,
                    price_cap: value,
                    price_cap_field: field,
                },
            )
            .await
            .map_err(map_calldata_boundary_error)?;
            build_calldata_candidates(
                ds,
                req.chain_id,
                req.input_token,
                req.output_token,
                req.taker,
            )
            .await?;
            (amount, price_cap, resolved_price_cap, wrap_ratios)
        }
        SwapCalldataPriceLimit::SlippageBps {
            slippage_bps,
            reference_io_ratio,
        } => {
            let (amount, wrap_ratios) = normalize_calldata_request_amount(
                ds,
                CalldataAmountNormalization {
                    chain_id: req.chain_id,
                    denomination: req.denomination,
                    input_token: req.input_token,
                    output_token: req.output_token,
                    mode: req.mode,
                    amount: req.amount,
                    amount_field: req.amount_field,
                },
            )
            .await
            .map_err(map_calldata_boundary_error)?;
            let reference_io_ratio = reference_io_ratio
                .map(|reference_io_ratio| {
                    normalize_calldata_price_cap(
                        reference_io_ratio,
                        "reference_io_ratio",
                        req.denomination,
                        req.input_token,
                        req.output_token,
                        &wrap_ratios,
                    )
                    .and_then(|reference_io_ratio| {
                        Float::parse(reference_io_ratio).map_err(|error| {
                            tracing::warn!(
                                %error,
                                "swap calldata rejected for invalid reference_io_ratio"
                            );
                            ApiError::BadRequest("invalid reference_io_ratio".into())
                        })
                    })
                })
                .transpose()?;
            let candidate_build = build_calldata_candidates(
                ds,
                req.chain_id,
                req.input_token,
                req.output_token,
                req.taker,
            )
            .await?;
            let price_cap = super::slippage::resolve_slippage_price_cap(
                candidate_build.candidates,
                req.mode,
                &amount,
                slippage_bps,
                reference_io_ratio,
            )
            .map_err(map_calldata_boundary_error)?;
            let resolved_price_cap = denormalize_calldata_price_cap(
                price_cap,
                req.denomination,
                req.input_token,
                req.output_token,
                &wrap_ratios,
            )
            .map_err(map_calldata_boundary_error)?;
            tracing::info!(
                slippage_bps,
                resolved_price_cap = %resolved_price_cap,
                denomination = ?req.denomination,
                "resolved swap slippage price cap"
            );
            let price_cap = price_cap.format().map_err(|error| {
                tracing::error!(%error, "failed to format resolved slippage price cap");
                ApiError::coded(
                    ApiErrorCode::SwapCalldataFailed,
                    "swap calldata could not be generated",
                )
            })?;
            (amount, price_cap, resolved_price_cap, wrap_ratios)
        }
    };

    let take_req = TakeOrdersRequest {
        taker: req.taker.to_string(),
        chain_id: req.chain_id,
        sell_token: req.input_token.to_string(),
        buy_token: req.output_token.to_string(),
        mode: req.mode,
        amount,
        price_cap,
    };

    let response = ds
        .get_calldata_on_chain(req.chain_id, take_req)
        .await
        .map_err(map_calldata_boundary_error)?;
    let mut calldata =
        normalize_calldata_response(&wrap_ratios, req.denomination, req.input_token, response)
            .map_err(map_calldata_boundary_error)?;
    refresh_oracle_signed_context(
        ds,
        &mut calldata,
        req.chain_id,
        req.input_token,
        req.output_token,
        req.taker,
    )
    .await
    .map_err(map_calldata_boundary_error)?;
    Ok(SwapCalldataBuildResult {
        calldata,
        resolved_price_cap,
    })
}

/// Replace oracle signed context in executable takeOrders calldata with a
/// freshly fetched frame. Quote/preflight reuse a frame that can already be
/// near expiry (~20s TTL) by the time the response is returned.
async fn refresh_oracle_signed_context(
    ds: &dyn SwapDataSource,
    calldata: &mut SwapCalldataResponse,
    chain_id: u32,
    input_token: Address,
    output_token: Address,
    taker: Address,
) -> Result<(), ApiError> {
    if !calldata.approvals.is_empty() || calldata.data.is_empty() {
        return Ok(());
    }
    let Ok(mut decoded) = takeOrders4Call::abi_decode(&calldata.data) else {
        return Ok(());
    };
    if decoded
        .config
        .orders
        .iter()
        .all(|order| order.signedContext.is_empty())
    {
        return Ok(());
    }

    struct RefreshItem {
        order_index: usize,
        order_hash: B256,
        order: OrderV4,
        input_io_index: u32,
        output_io_index: u32,
    }

    let orders = ds
        .get_orders_for_pair_on_chain(chain_id, input_token, output_token)
        .await?;
    let mut batches = BTreeMap::<String, Vec<RefreshItem>>::new();
    for (order_index, order_config) in decoded.config.orders.iter().enumerate() {
        if order_config.signedContext.is_empty() {
            continue;
        }
        let encoded_order = Bytes::from(order_config.order.abi_encode());
        let order_hash = B256::from(keccak256(&encoded_order));
        let Some(order) = orders
            .iter()
            .find(|order| order.order_hash() == order_hash || order.order_bytes() == encoded_order)
        else {
            tracing::error!(
                %order_hash,
                pair_orders = orders.len(),
                "cannot refresh oracle context; order missing from pair query"
            );
            return Err(ApiError::coded(
                ApiErrorCode::SwapCalldataFailed,
                "swap calldata could not be generated",
            ));
        };
        let Some(oracle_url) = order.oracle_url() else {
            tracing::error!(
                %order_hash,
                "cannot refresh oracle context; order has no oracle URL"
            );
            return Err(ApiError::coded(
                ApiErrorCode::SwapCalldataFailed,
                "swap calldata could not be generated",
            ));
        };
        let input_io_index = u32::try_from(order_config.inputIOIndex).map_err(|error| {
            tracing::error!(%error, %order_hash, "takeOrders inputIOIndex does not fit u32");
            ApiError::coded(
                ApiErrorCode::SwapCalldataFailed,
                "swap calldata could not be generated",
            )
        })?;
        let output_io_index = u32::try_from(order_config.outputIOIndex).map_err(|error| {
            tracing::error!(%error, %order_hash, "takeOrders outputIOIndex does not fit u32");
            ApiError::coded(
                ApiErrorCode::SwapCalldataFailed,
                "swap calldata could not be generated",
            )
        })?;
        batches.entry(oracle_url).or_default().push(RefreshItem {
            order_index,
            order_hash,
            order: order_config.order.clone(),
            input_io_index,
            output_io_index,
        });
    }

    let oracle_endpoint_count = batches.len();
    let refreshed_batches = stream::iter(batches)
        .map(|(oracle_url, items)| async move {
            let body = encode_oracle_body_batch(
                items
                    .iter()
                    .map(|item| {
                        (
                            &item.order,
                            item.input_io_index,
                            item.output_io_index,
                            taker,
                        )
                    })
                    .collect(),
            );
            let expected_count = items.len();
            let fresh = fetch_signed_context_batch(&oracle_url, body, expected_count)
                .await
                .map_err(|error| {
                    tracing::error!(
                        %error,
                        oracle_request_count = expected_count,
                        "failed to refresh oracle signed context before returning calldata"
                    );
                    ApiError::coded(
                        ApiErrorCode::SwapCalldataFailed,
                        "swap calldata could not be generated",
                    )
                })?;

            Ok::<_, ApiError>(items.into_iter().zip(fresh).collect::<Vec<_>>())
        })
        .buffer_unordered(ORACLE_REFRESH_CONCURRENCY_LIMIT)
        .try_collect::<Vec<_>>()
        .await?;

    let mut refreshed = 0usize;
    for (item, fresh) in refreshed_batches.into_iter().flatten() {
        let Some(order_config) = decoded.config.orders.get_mut(item.order_index) else {
            tracing::error!(
                order_index = item.order_index,
                order_hash = %item.order_hash,
                "oracle refresh target is missing from decoded calldata"
            );
            return Err(ApiError::coded(
                ApiErrorCode::SwapCalldataFailed,
                "swap calldata could not be generated",
            ));
        };
        let Some(oracle_context) = order_config.signedContext.first_mut() else {
            tracing::error!(
                order_index = item.order_index,
                order_hash = %item.order_hash,
                "oracle refresh target has no signed context"
            );
            return Err(ApiError::coded(
                ApiErrorCode::SwapCalldataFailed,
                "swap calldata could not be generated",
            ));
        };
        *oracle_context = fresh;
        refreshed += 1;
    }
    if refreshed > 0 {
        tracing::info!(
            refreshed,
            oracle_endpoint_count,
            "refreshed oracle signed context in swap calldata"
        );
        calldata.data = Bytes::from(decoded.abi_encode());
    }
    Ok(())
}

fn map_calldata_boundary_error(error: ApiError) -> ApiError {
    if matches!(error, ApiError::Coded { .. } | ApiError::BadRequest(_)) {
        return error;
    }
    if matches!(error, ApiError::NotFound(_)) {
        tracing::warn!(%error, code = %ApiErrorCode::SwapNoLiquidity, "swap calldata found no executable liquidity");
        return no_liquidity_error();
    }
    tracing::error!(%error, code = %ApiErrorCode::SwapCalldataFailed, "swap calldata boundary failed");
    ApiError::coded(
        ApiErrorCode::SwapCalldataFailed,
        "swap calldata could not be generated",
    )
}

#[cfg(all(test, feature = "oracle-integration-tests"))]
mod oracle_integration_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analytics::{Analytics, RecordingSink};
    use crate::auth::AuthenticatedKey;
    use crate::routes::swap::test_fixtures::MockSwapDataSource;
    use crate::test_helpers::{mock_candidate, mock_order, order_json, TestClientBuilder};
    use crate::types::common::Approval;
    use crate::types::swap::{SwapCalldataMode, SwapDenomination};
    use crate::wrap_ratio::WrapRatioValue;
    use alloy::primitives::{address, Address, Bytes, U256};
    use alloy::sol_types::SolValue;
    use async_trait::async_trait;
    use rain_orderbook_bindings::IRaindexV6::{
        SignedContextV1, TakeOrderConfigV4, TakeOrdersConfigV5,
    };
    use rain_orderbook_common::raindex_client::orders::RaindexOrder;
    use rocket::http::{ContentType, Status};
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const USDC: Address = address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");
    const WETH: Address = address!("4200000000000000000000000000000000000006");
    const TAKER: Address = address!("1111111111111111111111111111111111111111");
    const ORDERBOOK: Address = address!("d2938e7c9fe3597f78832ce780feb61945c377d7");
    const WT_MSTR: Address = address!("Ff05e1BD696900DC6A52cA35cA61bB1024eDA8e2");
    const WT_COIN: Address = address!("EeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE");
    type CapturedTakeOrdersRequest = Arc<Mutex<Option<TakeOrdersRequest>>>;
    type CapturedCounterparty = Arc<Mutex<Option<Address>>>;

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

    fn test_attribution_state() -> crate::attribution::AttributionState {
        crate::attribution::AttributionState::new(
            crate::attribution::AttributionSigner::from_hex_key(
                "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
            )
            .expect("test attribution signer"),
        )
    }

    fn calldata_request(output_amount: &str, max_ratio: &str) -> SwapCalldataRequest {
        SwapCalldataRequest {
            chain_id: Some(8453),
            taker: TAKER,
            input_token: USDC,
            output_token: WETH,
            output_amount: output_amount.to_string(),
            maximum_io_ratio: max_ratio.to_string(),
            denomination: SwapDenomination::Wrapped,
        }
    }

    fn assert_error_code<T>(result: Result<T, ApiError>, expected: ApiErrorCode) {
        assert!(matches!(
            result,
            Err(ApiError::Coded { code, .. }) if code == expected
        ));
    }

    fn calldata_v2_request(
        mode: SwapCalldataMode,
        amount: &str,
        price_cap: &str,
    ) -> SwapCalldataV2Request {
        SwapCalldataV2Request {
            chain_id: Some(8453),
            taker: TAKER,
            input_token: USDC,
            output_token: WETH,
            mode,
            amount: amount.to_string(),
            price_cap: Some(price_cap.to_string()),
            slippage_bps: None,
            reference_io_ratio: None,
            denomination: SwapDenomination::Wrapped,
        }
    }

    fn unwrapped_calldata_request(
        input_token: Address,
        output_token: Address,
        output_amount: &str,
        max_ratio: &str,
    ) -> SwapCalldataRequest {
        SwapCalldataRequest {
            chain_id: Some(8453),
            taker: TAKER,
            input_token,
            output_token,
            output_amount: output_amount.to_string(),
            maximum_io_ratio: max_ratio.to_string(),
            denomination: SwapDenomination::Unwrapped,
        }
    }

    fn unwrapped_calldata_v2_request(
        input_token: Address,
        output_token: Address,
        mode: SwapCalldataMode,
        amount: &str,
        price_cap: &str,
    ) -> SwapCalldataV2Request {
        SwapCalldataV2Request {
            chain_id: Some(8453),
            taker: TAKER,
            input_token,
            output_token,
            mode,
            amount: amount.to_string(),
            price_cap: Some(price_cap.to_string()),
            slippage_bps: None,
            reference_io_ratio: None,
            denomination: SwapDenomination::Unwrapped,
        }
    }

    fn slippage_v2_request(
        mode: SwapCalldataMode,
        amount: &str,
        slippage_bps: u16,
    ) -> SwapCalldataV2Request {
        SwapCalldataV2Request {
            chain_id: Some(8453),
            taker: TAKER,
            input_token: USDC,
            output_token: WETH,
            mode,
            amount: amount.to_string(),
            price_cap: None,
            slippage_bps: Some(slippage_bps),
            reference_io_ratio: None,
            denomination: SwapDenomination::Wrapped,
        }
    }

    fn ready_response() -> SwapCalldataResponse {
        SwapCalldataResponse {
            chain_id: 8453,
            to: ORDERBOOK,
            data: Bytes::from(vec![0xab, 0xcd, 0xef]),
            value: U256::ZERO,
            estimated_input: "150".to_string(),
            denomination: SwapDenomination::Wrapped,
            approvals: vec![],
        }
    }

    fn approval_response() -> SwapCalldataResponse {
        SwapCalldataResponse {
            chain_id: 8453,
            to: ORDERBOOK,
            data: Bytes::new(),
            value: U256::ZERO,
            estimated_input: "1000".to_string(),
            denomination: SwapDenomination::Wrapped,
            approvals: vec![Approval {
                token: USDC,
                spender: ORDERBOOK,
                amount: "1000".to_string(),
                symbol: String::new(),
                approval_data: Bytes::from(vec![0x09, 0x5e, 0xa7, 0xb3]),
            }],
        }
    }

    fn test_signed_context(value: u64) -> SignedContextV1 {
        SignedContextV1 {
            signer: TAKER,
            context: vec![B256::from(U256::from(value))],
            signature: Bytes::from(vec![value as u8; 65]),
        }
    }

    fn oracle_calldata_response(orders: Vec<OrderV4>) -> SwapCalldataResponse {
        let orders = orders
            .into_iter()
            .map(|order| TakeOrderConfigV4 {
                order,
                inputIOIndex: U256::ZERO,
                outputIOIndex: U256::ZERO,
                signedContext: vec![test_signed_context(1)],
            })
            .collect();
        let data = takeOrders4Call {
            config: TakeOrdersConfigV5 {
                minimumIO: U256::ZERO.into(),
                maximumIO: U256::from(1).into(),
                maximumIORatio: U256::from(1).into(),
                orders,
                IOIsInput: true,
                data: Bytes::new(),
            },
        }
        .abi_encode();
        SwapCalldataResponse {
            chain_id: 8453,
            to: ORDERBOOK,
            data: Bytes::from(data),
            value: U256::ZERO,
            estimated_input: "1".to_string(),
            denomination: SwapDenomination::Wrapped,
            approvals: vec![],
        }
    }

    fn raindex_order_for(order: &OrderV4, oracle_url: Option<&str>) -> RaindexOrder {
        let encoded_order = Bytes::from(order.abi_encode());
        let mut value = order_json();
        value["orderBytes"] = json!(encoded_order);
        value["orderHash"] = json!(keccak256(&encoded_order));
        value["parsedMeta"] = oracle_url.map_or_else(
            || json!([]),
            |url| json!([{"RaindexSignedContextOracleV1": url}]),
        );
        serde_json::from_value(value).expect("deserialize oracle test order")
    }

    struct BatchOracleServer {
        base_url: String,
        calls: Arc<AtomicUsize>,
        max_in_flight: Arc<AtomicUsize>,
        batch_sizes: Arc<Mutex<Vec<usize>>>,
    }

    impl BatchOracleServer {
        async fn start() -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind batch oracle");
            let address = listener.local_addr().expect("batch oracle address");
            let calls = Arc::new(AtomicUsize::new(0));
            let in_flight = Arc::new(AtomicUsize::new(0));
            let max_in_flight = Arc::new(AtomicUsize::new(0));
            let batch_sizes = Arc::new(Mutex::new(Vec::new()));

            let server_calls = Arc::clone(&calls);
            let server_in_flight = Arc::clone(&in_flight);
            let server_max_in_flight = Arc::clone(&max_in_flight);
            let server_batch_sizes = Arc::clone(&batch_sizes);
            tokio::spawn(async move {
                loop {
                    let Ok((mut socket, _)) = listener.accept().await else {
                        break;
                    };
                    let calls = Arc::clone(&server_calls);
                    let in_flight = Arc::clone(&server_in_flight);
                    let max_in_flight = Arc::clone(&server_max_in_flight);
                    let batch_sizes = Arc::clone(&server_batch_sizes);
                    tokio::spawn(async move {
                        let Some(body) = read_test_request_body(&mut socket).await else {
                            return;
                        };
                        calls.fetch_add(1, Ordering::SeqCst);
                        let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                        max_in_flight.fetch_max(current, Ordering::SeqCst);
                        let requests = <Vec<(OrderV4, U256, U256, Address)>>::abi_decode(&body)
                            .expect("decode batch oracle request");
                        batch_sizes
                            .lock()
                            .expect("batch sizes lock")
                            .push(requests.len());
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        let response = json!(requests
                            .iter()
                            .map(|_| json!({
                                "signer": TAKER,
                                "context": [B256::from(U256::from(99))],
                                "signature": Bytes::from(vec![99u8; 65]),
                            }))
                            .collect::<Vec<_>>())
                        .to_string();
                        in_flight.fetch_sub(1, Ordering::SeqCst);
                        write_test_response(&mut socket, &response).await;
                    });
                }
            });

            Self {
                base_url: format!("http://{address}"),
                calls,
                max_in_flight,
                batch_sizes,
            }
        }

        fn url(&self, path: &str) -> String {
            format!("{}{path}", self.base_url)
        }
    }

    async fn read_test_request_body(socket: &mut tokio::net::TcpStream) -> Option<Vec<u8>> {
        let mut request = Vec::new();
        let mut chunk = [0u8; 8192];
        let (header_end, content_length) = loop {
            let read = socket.read(&mut chunk).await.ok()?;
            if read == 0 {
                return None;
            }
            request.extend_from_slice(&chunk[..read]);
            if let Some(header_end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                let header_end = header_end + 4;
                let headers = String::from_utf8_lossy(&request[..header_end]);
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())
                            .flatten()
                    })
                    .unwrap_or_default();
                break (header_end, content_length);
            }
        };
        while request.len() < header_end + content_length {
            let read = socket.read(&mut chunk).await.ok()?;
            if read == 0 {
                return None;
            }
            request.extend_from_slice(&chunk[..read]);
        }
        Some(request[header_end..header_end + content_length].to_vec())
    }

    async fn write_test_response(socket: &mut tokio::net::TcpStream, body: &str) {
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        socket
            .write_all(response.as_bytes())
            .await
            .expect("write batch oracle response");
    }

    #[rocket::async_test]
    async fn test_refresh_oracle_context_batches_by_url_and_runs_endpoints_concurrently() {
        let server = BatchOracleServer::start().await;
        let mut first_order = mock_candidate("1", "1").order;
        first_order.nonce = U256::from(1).into();
        let mut second_order = first_order.clone();
        second_order.nonce = U256::from(2).into();
        let mut third_order = first_order.clone();
        third_order.nonce = U256::from(3).into();
        let first_url = server.url("/first");
        let second_url = server.url("/second");
        let ds = MockSwapDataSource {
            supported_tokens: Ok(()),
            orders: Ok(vec![
                raindex_order_for(&first_order, Some(&first_url)),
                raindex_order_for(&second_order, Some(&first_url)),
                raindex_order_for(&third_order, Some(&second_url)),
            ]),
            candidates: vec![],
            calldata_result: Ok(ready_response()),
        };
        let mut calldata = oracle_calldata_response(vec![first_order, second_order, third_order]);

        refresh_oracle_signed_context(&ds, &mut calldata, 8453, USDC, WETH, TAKER)
            .await
            .expect("refresh batched oracle contexts");

        assert_eq!(server.calls.load(Ordering::SeqCst), 2);
        assert_eq!(server.max_in_flight.load(Ordering::SeqCst), 2);
        let mut batch_sizes = server.batch_sizes.lock().expect("batch sizes lock").clone();
        batch_sizes.sort_unstable();
        assert_eq!(batch_sizes, vec![1, 2]);
        let decoded =
            takeOrders4Call::abi_decode(&calldata.data).expect("decode refreshed calldata");
        for order in decoded.config.orders {
            assert_eq!(
                order.signedContext[0].context,
                test_signed_context(99).context
            );
        }
    }

    #[rocket::async_test]
    async fn test_refresh_oracle_context_fails_when_order_cannot_be_resolved() {
        let order = mock_candidate("1", "1").order;
        let mut calldata = oracle_calldata_response(vec![order]);
        let original_data = calldata.data.clone();
        let ds = MockSwapDataSource {
            supported_tokens: Ok(()),
            orders: Ok(vec![]),
            candidates: vec![],
            calldata_result: Ok(ready_response()),
        };

        let result =
            refresh_oracle_signed_context(&ds, &mut calldata, 8453, USDC, WETH, TAKER).await;

        assert_error_code(result, ApiErrorCode::SwapCalldataFailed);
        assert_eq!(calldata.data, original_data);
    }

    #[rocket::async_test]
    async fn test_refresh_oracle_context_fails_when_order_has_no_oracle_url() {
        let order = mock_candidate("1", "1").order;
        let mut calldata = oracle_calldata_response(vec![order.clone()]);
        let original_data = calldata.data.clone();
        let ds = MockSwapDataSource {
            supported_tokens: Ok(()),
            orders: Ok(vec![raindex_order_for(&order, None)]),
            candidates: vec![],
            calldata_result: Ok(ready_response()),
        };

        let result =
            refresh_oracle_signed_context(&ds, &mut calldata, 8453, USDC, WETH, TAKER).await;

        assert_error_code(result, ApiErrorCode::SwapCalldataFailed);
        assert_eq!(calldata.data, original_data);
    }

    #[rocket::async_test]
    #[tracing_test::traced_test]
    async fn test_refresh_oracle_context_does_not_log_raw_oracle_url() {
        const SECRET: &str = "unique-refresh-secret-do-not-log";
        let order = mock_candidate("1", "1").order;
        let oracle_url =
            format!("ftp://user:password@example.com/oracle?api_key={SECRET}#fragment");
        let ds = MockSwapDataSource {
            supported_tokens: Ok(()),
            orders: Ok(vec![raindex_order_for(&order, Some(&oracle_url))]),
            candidates: vec![],
            calldata_result: Ok(ready_response()),
        };
        let mut calldata = oracle_calldata_response(vec![order]);

        let result =
            refresh_oracle_signed_context(&ds, &mut calldata, 8453, USDC, WETH, TAKER).await;

        assert_error_code(result, ApiErrorCode::SwapCalldataFailed);
        assert!(!logs_contain(SECRET));
    }

    fn wrap_ratio(share_address: Address, assets_per_share: &str) -> WrapRatioValue {
        WrapRatioValue {
            share_address,
            assets_per_share: assets_per_share.to_string(),
        }
    }

    fn capture_ds(
        response: SwapCalldataResponse,
        wrap_ratios: HashMap<Address, WrapRatioValue>,
    ) -> (MockCalldataDataSource, CapturedTakeOrdersRequest) {
        capture_ds_with_wrap_result(response, Ok(wrap_ratios))
    }

    fn capture_ds_with_wrap_result(
        response: SwapCalldataResponse,
        wrap_ratios: Result<HashMap<Address, WrapRatioValue>, ApiError>,
    ) -> (MockCalldataDataSource, CapturedTakeOrdersRequest) {
        let captured_request = Arc::new(Mutex::new(None));
        (
            MockCalldataDataSource {
                base: MockSwapDataSource {
                    supported_tokens: Ok(()),
                    orders: Ok(vec![mock_order()]),
                    candidates: vec![mock_candidate("1000", "1.5")],
                    calldata_result: Ok(response),
                },
                failures: super::super::SwapQuoteFailures::default(),
                wrap_ratios,
                captured_request: Arc::clone(&captured_request),
                captured_counterparty: Arc::new(Mutex::new(None)),
            },
            captured_request,
        )
    }

    fn capture_slippage_ds(
        wrap_ratios: HashMap<Address, WrapRatioValue>,
    ) -> (
        MockCalldataDataSource,
        CapturedTakeOrdersRequest,
        CapturedCounterparty,
    ) {
        let captured_request = Arc::new(Mutex::new(None));
        let captured_counterparty = Arc::new(Mutex::new(None));
        (
            MockCalldataDataSource {
                base: MockSwapDataSource {
                    supported_tokens: Ok(()),
                    orders: Ok(vec![mock_order()]),
                    candidates: vec![mock_candidate("100", "2")],
                    calldata_result: Ok(ready_response()),
                },
                failures: super::super::SwapQuoteFailures::default(),
                wrap_ratios: Ok(wrap_ratios),
                captured_request: Arc::clone(&captured_request),
                captured_counterparty: Arc::clone(&captured_counterparty),
            },
            captured_request,
            captured_counterparty,
        )
    }

    fn capture_candidate_outcome_ds(
        candidates: Vec<rain_orderbook_common::take_orders::TakeOrderCandidate>,
        failures: Vec<super::super::SwapQuoteFailure>,
    ) -> (MockCalldataDataSource, CapturedTakeOrdersRequest) {
        let (mut ds, captured_request, _) = capture_slippage_ds(HashMap::new());
        ds.base.candidates = candidates;
        ds.failures = failures.into_iter().collect();
        (ds, captured_request)
    }

    struct MockCalldataDataSource {
        base: MockSwapDataSource,
        failures: super::super::SwapQuoteFailures,
        wrap_ratios: Result<HashMap<Address, WrapRatioValue>, ApiError>,
        captured_request: CapturedTakeOrdersRequest,
        captured_counterparty: CapturedCounterparty,
    }

    #[async_trait]
    impl SwapDataSource for MockCalldataDataSource {
        async fn validate_supported_tokens(
            &self,
            input_token: Address,
            output_token: Address,
        ) -> Result<(), ApiError> {
            self.base
                .validate_supported_tokens(input_token, output_token)
                .await
        }

        async fn get_orders_for_pair(
            &self,
            input_token: Address,
            output_token: Address,
        ) -> Result<Vec<rain_orderbook_common::raindex_client::orders::RaindexOrder>, ApiError>
        {
            self.base
                .get_orders_for_pair(input_token, output_token)
                .await
        }

        async fn build_candidates_for_pair(
            &self,
            orders: &[rain_orderbook_common::raindex_client::orders::RaindexOrder],
            input_token: Address,
            output_token: Address,
            counterparty: Address,
        ) -> Result<super::super::SwapCandidateBuild, ApiError> {
            *self.captured_counterparty.lock().unwrap() = Some(counterparty);
            let mut candidate_build = self
                .base
                .build_candidates_for_pair(orders, input_token, output_token, counterparty)
                .await?;
            candidate_build.failures = self.failures.clone();
            Ok(candidate_build)
        }

        async fn get_calldata(
            &self,
            request: TakeOrdersRequest,
        ) -> Result<SwapCalldataResponse, ApiError> {
            *self.captured_request.lock().unwrap() = Some(request);
            self.base.calldata_result.clone()
        }

        async fn get_wrap_ratios_for_tokens(
            &self,
            token_addresses: &[Address],
        ) -> Result<HashMap<Address, WrapRatioValue>, ApiError> {
            let wrap_ratios = self.wrap_ratios.clone()?;
            Ok(token_addresses
                .iter()
                .filter_map(|address| {
                    wrap_ratios
                        .get(address)
                        .map(|ratio| (*address, ratio.clone()))
                })
                .collect())
        }
    }

    fn captured_take_orders_request(
        captured_request: &CapturedTakeOrdersRequest,
    ) -> TakeOrdersRequest {
        captured_request.lock().unwrap().clone().unwrap()
    }

    fn no_take_orders_request_was_made(captured_request: &CapturedTakeOrdersRequest) {
        assert!(captured_request.lock().unwrap().is_none());
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_ready() {
        let ds = MockSwapDataSource {
            supported_tokens: Ok(()),
            orders: Ok(vec![mock_order()]),
            candidates: vec![mock_candidate("1000", "1.5")],
            calldata_result: Ok(ready_response()),
        };
        let result = process_swap_calldata(&ds, calldata_request("100", "2.5"))
            .await
            .unwrap();

        assert_eq!(result.to, ORDERBOOK);
        assert!(!result.data.is_empty());
        assert_eq!(result.value, U256::ZERO);
        assert_eq!(result.estimated_input, "150");
        assert_eq!(result.denomination, SwapDenomination::Wrapped);
        assert!(result.approvals.is_empty());
    }

    #[rocket::async_test]
    async fn test_handle_swap_calldata_captures_v1_analytics() {
        let ds = MockSwapDataSource {
            supported_tokens: Ok(()),
            orders: Ok(vec![mock_order()]),
            candidates: vec![mock_candidate("1000", "1.5")],
            calldata_result: Ok(approval_response()),
        };
        let recording = RecordingSink::new();
        let analytics = Analytics::new(Arc::new(recording.clone()));
        let key = test_key();
        let attribution_state = test_attribution_state();
        let attribution = attribution_state.for_api_key(&key.key_id, TAKER);

        handle_swap_calldata(
            &ds,
            &key,
            &analytics,
            &attribution_state.signer,
            &attribution,
            calldata_request("100", "2.5"),
        )
        .await
        .expect("successful calldata");

        let event = recording
            .events()
            .into_iter()
            .find(|event| event.event == "swap_calldata_generated")
            .expect("swap_calldata_generated event");
        assert_eq!(event.distinct_id, TAKER.to_string().to_lowercase());
        assert_eq!(event.properties["api_version"], "v1");
        assert!(event.properties.get("mode").is_none());
    }

    #[rocket::async_test]
    async fn test_handle_swap_calldata_captures_v2_analytics() {
        let ds = MockSwapDataSource {
            supported_tokens: Ok(()),
            orders: Ok(vec![mock_order()]),
            candidates: vec![mock_candidate("1000", "1.5")],
            calldata_result: Ok(approval_response()),
        };
        let recording = RecordingSink::new();
        let analytics = Analytics::new(Arc::new(recording.clone()));
        let key = test_key();
        let attribution_state = test_attribution_state();
        let attribution = attribution_state.for_api_key(&key.key_id, TAKER);

        handle_swap_calldata_v2(
            &ds,
            &key,
            &analytics,
            ApiVersion::V2,
            &attribution_state.signer,
            &attribution,
            calldata_v2_request(SwapCalldataMode::SpendExact, "100", "2.5"),
        )
        .await
        .expect("successful calldata");

        let event = recording
            .events()
            .into_iter()
            .find(|event| event.event == "swap_calldata_generated")
            .expect("swap_calldata_generated event");
        assert_eq!(event.properties["api_version"], "v2");
        assert_eq!(event.properties["mode"], "spendExact");
    }

    #[rocket::async_test]
    async fn test_handle_swap_calldata_captures_v1_failure_analytics() {
        let ds = MockSwapDataSource {
            supported_tokens: Ok(()),
            orders: Ok(vec![mock_order()]),
            candidates: vec![mock_candidate("1000", "1.5")],
            calldata_result: Err(ApiError::Internal("failed to generate calldata".into())),
        };
        let recording = RecordingSink::new();
        let analytics = Analytics::new(Arc::new(recording.clone()));
        let key = test_key();
        let attribution_state = test_attribution_state();
        let attribution = attribution_state.for_api_key(&key.key_id, TAKER);

        let result = handle_swap_calldata(
            &ds,
            &key,
            &analytics,
            &attribution_state.signer,
            &attribution,
            calldata_request("100", "2.5"),
        )
        .await;
        assert_error_code(result, ApiErrorCode::SwapCalldataFailed);

        let events = recording.events();
        let event = events
            .iter()
            .find(|event| event.event == "swap_calldata_failed")
            .expect("swap_calldata_failed event");
        assert_eq!(event.distinct_id, "client:test-client");
        assert_eq!(event.properties["requested_amount"], "100");
        assert_eq!(event.properties["api_version"], "v1");
        assert_eq!(event.properties["taker"], TAKER.to_string().to_lowercase());
        assert_eq!(event.properties["error_code"], "SWAP_CALLDATA_FAILED");
        assert_eq!(event.properties["status_code"], 500);
        assert!(!events
            .iter()
            .any(|event| event.event == "swap_calldata_generated"));
    }

    #[rocket::async_test]
    async fn test_handle_swap_calldata_v2_captures_attribution_failure_analytics() {
        let ds = MockSwapDataSource {
            supported_tokens: Ok(()),
            orders: Ok(vec![mock_order()]),
            candidates: vec![mock_candidate("1000", "1.5")],
            // Deliberately invalid ABI: processing succeeds, attribution embedding fails.
            calldata_result: Ok(ready_response()),
        };
        let recording = RecordingSink::new();
        let analytics = Analytics::new(Arc::new(recording.clone()));
        let key = test_key();
        let attribution_state = test_attribution_state();
        let attribution = attribution_state.for_api_key(&key.key_id, TAKER);

        let result = handle_swap_calldata_v2(
            &ds,
            &key,
            &analytics,
            ApiVersion::V2,
            &attribution_state.signer,
            &attribution,
            calldata_v2_request(SwapCalldataMode::SpendExact, "100", "2.5"),
        )
        .await;
        assert!(matches!(result, Err(ApiError::Internal(_))));

        let events = recording.events();
        let event = events
            .iter()
            .find(|event| event.event == "swap_calldata_failed")
            .expect("swap_calldata_failed event");
        assert_eq!(event.properties["requested_amount"], "100");
        assert_eq!(event.properties["api_version"], "v2");
        assert_eq!(event.properties["mode"], "spendExact");
        assert_eq!(event.properties["taker"], TAKER.to_string().to_lowercase());
        assert_eq!(event.properties["error_code"], "INTERNAL_ERROR");
        assert_eq!(event.properties["status_code"], 500);
        assert!(!events
            .iter()
            .any(|event| event.event == "swap_calldata_generated"));
    }

    #[rocket::async_test]
    async fn test_handle_swap_calldata_rejects_same_token_before_data_source_and_captures_failure()
    {
        let ds = MockSwapDataSource {
            supported_tokens: Err(ApiError::Internal("data source must not be reached".into())),
            orders: Err(ApiError::Internal("data source must not be reached".into())),
            candidates: vec![],
            calldata_result: Err(ApiError::Internal("data source must not be reached".into())),
        };
        let recording = RecordingSink::new();
        let analytics = Analytics::new(Arc::new(recording.clone()));
        let key = test_key();
        let attribution_state = test_attribution_state();
        let attribution = attribution_state.for_api_key(&key.key_id, TAKER);
        let mut request = calldata_request("100", "2.5");
        request.output_token = request.input_token;

        let result = handle_swap_calldata(
            &ds,
            &key,
            &analytics,
            &attribution_state.signer,
            &attribution,
            request,
        )
        .await;

        assert_error_code(result, ApiErrorCode::SwapSameToken);
        let event = recording
            .events()
            .into_iter()
            .find(|event| event.event == "swap_calldata_failed")
            .expect("swap_calldata_failed event");
        assert_eq!(event.properties["api_version"], "v1");
        assert_eq!(event.properties["same_token"], true);
        assert_eq!(event.properties["error_code"], "SWAP_SAME_TOKEN");
    }

    #[rocket::async_test]
    async fn test_handle_swap_calldata_v2_same_token_precedes_price_limit_validation() {
        let ds = MockSwapDataSource {
            supported_tokens: Err(ApiError::Internal("data source must not be reached".into())),
            orders: Err(ApiError::Internal("data source must not be reached".into())),
            candidates: vec![],
            calldata_result: Err(ApiError::Internal("data source must not be reached".into())),
        };
        let recording = RecordingSink::new();
        let analytics = Analytics::new(Arc::new(recording.clone()));
        let key = test_key();
        let attribution_state = test_attribution_state();
        let attribution = attribution_state.for_api_key(&key.key_id, TAKER);
        let mut request = calldata_v2_request(SwapCalldataMode::SpendExact, "100", "2.5");
        request.output_token = request.input_token;
        request.price_cap = None;

        let result = handle_swap_calldata_v2(
            &ds,
            &key,
            &analytics,
            ApiVersion::V2,
            &attribution_state.signer,
            &attribution,
            request,
        )
        .await;

        assert_error_code(result, ApiErrorCode::SwapSameToken);
        let event = recording
            .events()
            .into_iter()
            .find(|event| event.event == "swap_calldata_failed")
            .expect("swap_calldata_failed event");
        assert_eq!(event.properties["api_version"], "v2");
        assert_eq!(event.properties["same_token"], true);
        assert_eq!(event.properties["error_code"], "SWAP_SAME_TOKEN");
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_needs_approval() {
        let ds = MockSwapDataSource {
            supported_tokens: Ok(()),
            orders: Ok(vec![mock_order()]),
            candidates: vec![mock_candidate("1000", "1.5")],
            calldata_result: Ok(approval_response()),
        };
        let result = process_swap_calldata(&ds, calldata_request("100", "2.5"))
            .await
            .unwrap();

        assert_eq!(result.to, ORDERBOOK);
        assert!(result.data.is_empty());
        assert_eq!(result.denomination, SwapDenomination::Wrapped);
        assert_eq!(result.approvals.len(), 1);
        assert_eq!(result.approvals[0].token, USDC);
        assert_eq!(result.approvals[0].spender, ORDERBOOK);
    }

    #[rocket::async_test]
    async fn test_approval_response_does_not_require_executable_attribution() {
        let state = test_attribution_state();
        let attribution = state.for_api_key("customer-key", TAKER);
        let mut response = approval_response();

        embed_and_validate_attribution(&mut response, &state.signer, &attribution)
            .await
            .unwrap();
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_default_denomination_preserves_request() {
        let (ds, captured_request) = capture_ds(ready_response(), HashMap::new());
        let result = process_swap_calldata(&ds, calldata_request("100", "2.5"))
            .await
            .unwrap();
        let request = captured_take_orders_request(&captured_request);

        assert_eq!(request.sell_token, USDC.to_string());
        assert_eq!(request.buy_token, WETH.to_string());
        assert_eq!(request.mode, TakeOrdersMode::BuyUpTo);
        assert_eq!(request.amount, "100");
        assert_eq!(request.price_cap, "2.5");
        assert_eq!(result.estimated_input, "150");
        assert_eq!(result.denomination, SwapDenomination::Wrapped);
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_v2_spend_exact_preserves_request() {
        let (ds, captured_request) = capture_ds(ready_response(), HashMap::new());
        let result = process_swap_calldata_v2(
            &ds,
            calldata_v2_request(SwapCalldataMode::SpendExact, "100", "2.5"),
        )
        .await
        .unwrap();
        let request = captured_take_orders_request(&captured_request);

        assert_eq!(request.sell_token, USDC.to_string());
        assert_eq!(request.buy_token, WETH.to_string());
        assert_eq!(request.mode, TakeOrdersMode::SpendExact);
        assert_eq!(request.amount, "100");
        assert_eq!(request.price_cap, "2.5");
        assert_eq!(result.calldata.estimated_input, "150");
        assert_eq!(result.calldata.denomination, SwapDenomination::Wrapped);
        assert_eq!(result.resolved_price_cap, "2.5");
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_v2_spend_up_to_preserves_request() {
        // Exact request and surviving-leg input from production block 49,797,519.
        let executable_input =
            "0.04310434222334697689407343024988324343123847171843280166595003948319";
        let response = SwapCalldataResponse {
            estimated_input: executable_input.to_string(),
            ..ready_response()
        };
        let (ds, captured_request) = capture_ds(response, HashMap::new());
        let mut incident_request = calldata_v2_request(
            SwapCalldataMode::SpendUpTo,
            "0.05",
            "0.010507140312878644839541586318708372026337414326508889434455784294694",
        );
        incident_request.taker = address!("D2843D9E7738d46D90CB6Dff8D6C83db58B9c165");
        incident_request.input_token = WT_MSTR;
        incident_request.output_token = USDC;
        let result = process_swap_calldata_v2(&ds, incident_request)
            .await
            .unwrap();
        let request = captured_take_orders_request(&captured_request);

        assert_eq!(request.taker, "0xD2843D9E7738d46D90CB6Dff8D6C83db58B9c165");
        assert_eq!(request.sell_token, WT_MSTR.to_string());
        assert_eq!(request.buy_token, USDC.to_string());
        assert_eq!(request.mode, TakeOrdersMode::SpendUpTo);
        assert_eq!(request.amount, "0.05");
        assert_eq!(
            request.price_cap,
            "0.010507140312878644839541586318708372026337414326508889434455784294694"
        );
        assert_eq!(result.calldata.estimated_input, executable_input);
        assert_eq!(result.calldata.denomination, SwapDenomination::Wrapped);
        assert_eq!(
            result.resolved_price_cap,
            "0.010507140312878644839541586318708372026337414326508889434455784294694"
        );
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_v2_buy_up_to_preserves_request() {
        let (ds, captured_request) = capture_ds(ready_response(), HashMap::new());
        let result = process_swap_calldata_v2(
            &ds,
            calldata_v2_request(SwapCalldataMode::BuyUpTo, "50", "2"),
        )
        .await
        .unwrap();
        let request = captured_take_orders_request(&captured_request);

        assert_eq!(request.mode, TakeOrdersMode::BuyUpTo);
        assert_eq!(request.amount, "50");
        assert_eq!(request.price_cap, "2");
        assert_eq!(result.calldata.denomination, SwapDenomination::Wrapped);
        assert_eq!(result.resolved_price_cap, "2");
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_v2_resolves_optional_slippage() {
        let (ds, captured_request, captured_counterparty) = capture_slippage_ds(HashMap::new());
        let result = process_swap_calldata_v2(
            &ds,
            slippage_v2_request(SwapCalldataMode::SpendExact, "100", 50),
        )
        .await
        .unwrap();
        let request = captured_take_orders_request(&captured_request);

        assert_eq!(request.price_cap, "2.01");
        assert_eq!(result.resolved_price_cap, "2.01");
        assert_eq!(*captured_counterparty.lock().unwrap(), Some(TAKER));
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_v2_slippage_reports_oracle_unavailable() {
        let (ds, captured_request) = capture_candidate_outcome_ds(
            Vec::new(),
            vec![super::super::SwapQuoteFailure::OracleUnavailable],
        );

        let result = process_swap_calldata_v2(
            &ds,
            slippage_v2_request(SwapCalldataMode::SpendExact, "100", 50),
        )
        .await;

        assert_error_code(result, ApiErrorCode::SwapOracleUnavailable);
        no_take_orders_request_was_made(&captured_request);
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_v2_slippage_treats_order_reverts_as_no_liquidity() {
        let (ds, captured_request) = capture_candidate_outcome_ds(
            Vec::new(),
            vec![super::super::SwapQuoteFailure::QuoteFailed],
        );

        let result = process_swap_calldata_v2(
            &ds,
            slippage_v2_request(SwapCalldataMode::SpendExact, "100", 50),
        )
        .await;

        assert_error_code(result, ApiErrorCode::SwapNoLiquidity);
        no_take_orders_request_was_made(&captured_request);
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_v2_slippage_prices_from_executable_candidates() {
        // An order whose oracle can't be fetched can't be taken: the cap comes from the
        // executable candidates (2 + 50 bps) instead of failing the whole pair.
        let (ds, captured_request) = capture_candidate_outcome_ds(
            vec![mock_candidate("100", "2")],
            vec![super::super::SwapQuoteFailure::OracleUnavailable],
        );

        let result = process_swap_calldata_v2(
            &ds,
            slippage_v2_request(SwapCalldataMode::SpendExact, "100", 50),
        )
        .await
        .unwrap();

        let request = captured_take_orders_request(&captured_request);
        assert_eq!(request.price_cap, "2.01");
        assert_eq!(result.resolved_price_cap, "2.01");
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_v2_explicit_reports_oracle_unavailable() {
        let (ds, captured_request) = capture_candidate_outcome_ds(
            Vec::new(),
            vec![super::super::SwapQuoteFailure::OracleUnavailable],
        );

        let result = process_swap_calldata_v2(
            &ds,
            calldata_v2_request(SwapCalldataMode::SpendExact, "100", "2"),
        )
        .await;

        assert_error_code(result, ApiErrorCode::SwapOracleUnavailable);
        no_take_orders_request_was_made(&captured_request);
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_v2_applies_reference_price_guard() {
        let (ds, captured_request, _) = capture_slippage_ds(HashMap::new());
        let mut request = slippage_v2_request(SwapCalldataMode::SpendExact, "100", 50);
        request.reference_io_ratio = Some("1".to_string());

        let result = process_swap_calldata_v2(&ds, request).await;

        assert_error_code(result, ApiErrorCode::SwapNoLiquidity);
        no_take_orders_request_was_made(&captured_request);
    }

    #[test]
    fn test_swap_calldata_v2_response_flattens_calldata_fields() {
        let response = serde_json::to_value(SwapCalldataV2Response {
            calldata: ready_response(),
            resolved_price_cap: "2.01".to_string(),
        })
        .unwrap();

        assert_eq!(response["to"], ORDERBOOK.to_string().to_lowercase());
        assert_eq!(response["estimatedInput"], "150");
        assert_eq!(response["resolvedPriceCap"], "2.01");
        assert!(response.get("calldata").is_none());
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_v2_denormalizes_resolved_slippage_cap() {
        let (ds, captured_request, _) =
            capture_slippage_ds(HashMap::from([(WT_COIN, wrap_ratio(WT_COIN, "4"))]));
        let mut request = slippage_v2_request(SwapCalldataMode::BuyUpTo, "100", 50);
        request.output_token = WT_COIN;
        request.denomination = SwapDenomination::Unwrapped;

        let result = process_swap_calldata_v2(&ds, request).await.unwrap();
        let request = captured_take_orders_request(&captured_request);

        assert_eq!(request.amount, "25");
        assert_eq!(request.price_cap, "2.01");
        assert_eq!(result.resolved_price_cap, "0.5025");
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_v2_requires_exactly_one_price_limit() {
        let (ds, captured_request) = capture_ds(ready_response(), HashMap::new());
        let mut request = calldata_v2_request(SwapCalldataMode::SpendExact, "100", "2.5");
        request.slippage_bps = Some(50);
        let result = process_swap_calldata_v2(&ds, request).await;

        assert!(
            matches!(result, Err(ApiError::BadRequest(message)) if message == "provide exactly one of price_cap or slippage_bps")
        );
        no_take_orders_request_was_made(&captured_request);

        let mut request = slippage_v2_request(SwapCalldataMode::SpendExact, "100", 50);
        request.slippage_bps = None;
        let result = process_swap_calldata_v2(&ds, request).await;

        assert!(
            matches!(result, Err(ApiError::BadRequest(message)) if message == "provide exactly one of price_cap or slippage_bps")
        );
        no_take_orders_request_was_made(&captured_request);
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_v2_rejects_reference_ratio_without_slippage() {
        let (ds, captured_request) = capture_ds(ready_response(), HashMap::new());
        let mut request = calldata_v2_request(SwapCalldataMode::SpendExact, "100", "2.5");
        request.reference_io_ratio = Some("2".to_string());

        let result = process_swap_calldata_v2(&ds, request).await;

        assert!(
            matches!(result, Err(ApiError::BadRequest(message)) if message == "reference_io_ratio requires slippage_bps")
        );
        no_take_orders_request_was_made(&captured_request);
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_v2_rejects_slippage_out_of_range() {
        let (ds, captured_request) = capture_ds(ready_response(), HashMap::new());
        let result = process_swap_calldata_v2(
            &ds,
            slippage_v2_request(SwapCalldataMode::SpendExact, "100", 5001),
        )
        .await;

        assert!(
            matches!(result, Err(ApiError::BadRequest(message)) if message == "slippage_bps must be between 1 and 5000")
        );
        no_take_orders_request_was_made(&captured_request);
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_explicit_wrapped_preserves_request() {
        let (ds, captured_request) = capture_ds(ready_response(), HashMap::new());
        let mut request = calldata_request("100", "2.5");
        request.denomination = SwapDenomination::Wrapped;
        let result = process_swap_calldata(&ds, request).await.unwrap();
        let request = captured_take_orders_request(&captured_request);

        assert_eq!(request.amount, "100");
        assert_eq!(request.price_cap, "2.5");
        assert_eq!(result.denomination, SwapDenomination::Wrapped);
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_unwrapped_converts_wrapped_output_amount() {
        let (ds, captured_request) = capture_ds(
            ready_response(),
            HashMap::from([(WT_MSTR, wrap_ratio(WT_MSTR, "2"))]),
        );
        let result =
            process_swap_calldata(&ds, unwrapped_calldata_request(USDC, WT_MSTR, "100", "2.5"))
                .await
                .unwrap();
        let request = captured_take_orders_request(&captured_request);

        assert_eq!(request.sell_token, USDC.to_string());
        assert_eq!(request.buy_token, WT_MSTR.to_string());
        assert_eq!(request.amount, "50");
        assert_eq!(request.price_cap, "5");
        assert_eq!(result.estimated_input, "150");
        assert_eq!(result.denomination, SwapDenomination::Unwrapped);
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_unwrapped_converts_wrapped_input_ratio_and_response() {
        let (ds, captured_request) = capture_ds(
            ready_response(),
            HashMap::from([(WT_MSTR, wrap_ratio(WT_MSTR, "2"))]),
        );
        let result =
            process_swap_calldata(&ds, unwrapped_calldata_request(WT_MSTR, WETH, "100", "2.5"))
                .await
                .unwrap();
        let request = captured_take_orders_request(&captured_request);

        assert_eq!(request.sell_token, WT_MSTR.to_string());
        assert_eq!(request.buy_token, WETH.to_string());
        assert_eq!(request.amount, "100");
        assert_eq!(request.price_cap, "1.25");
        assert_eq!(result.estimated_input, "300");
        assert_eq!(result.denomination, SwapDenomination::Unwrapped);
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_v2_unwrapped_spend_converts_wrapped_input_amount() {
        let (ds, captured_request) = capture_ds(
            ready_response(),
            HashMap::from([(WT_MSTR, wrap_ratio(WT_MSTR, "2"))]),
        );
        let result = process_swap_calldata_v2(
            &ds,
            unwrapped_calldata_v2_request(
                WT_MSTR,
                WETH,
                SwapCalldataMode::SpendExact,
                "100",
                "2.5",
            ),
        )
        .await
        .unwrap();
        let request = captured_take_orders_request(&captured_request);

        assert_eq!(request.sell_token, WT_MSTR.to_string());
        assert_eq!(request.buy_token, WETH.to_string());
        assert_eq!(request.mode, TakeOrdersMode::SpendExact);
        assert_eq!(request.amount, "50");
        assert_eq!(request.price_cap, "1.25");
        assert_eq!(result.calldata.estimated_input, "300");
        assert_eq!(result.calldata.denomination, SwapDenomination::Unwrapped);
        assert_eq!(result.resolved_price_cap, "2.5");
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_v2_unwrapped_buy_converts_wrapped_output_amount() {
        let (ds, captured_request) = capture_ds(
            ready_response(),
            HashMap::from([(WT_COIN, wrap_ratio(WT_COIN, "4"))]),
        );
        let result = process_swap_calldata_v2(
            &ds,
            unwrapped_calldata_v2_request(USDC, WT_COIN, SwapCalldataMode::BuyUpTo, "100", "2.5"),
        )
        .await
        .unwrap();
        let request = captured_take_orders_request(&captured_request);

        assert_eq!(request.mode, TakeOrdersMode::BuyUpTo);
        assert_eq!(request.amount, "25");
        assert_eq!(request.price_cap, "10");
        assert_eq!(result.calldata.estimated_input, "150");
        assert_eq!(result.calldata.denomination, SwapDenomination::Unwrapped);
        assert_eq!(result.resolved_price_cap, "2.5");
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_unwrapped_converts_both_wrapped_sides() {
        let (ds, captured_request) = capture_ds(
            ready_response(),
            HashMap::from([
                (WT_MSTR, wrap_ratio(WT_MSTR, "2")),
                (WT_COIN, wrap_ratio(WT_COIN, "4")),
            ]),
        );
        let result = process_swap_calldata(
            &ds,
            unwrapped_calldata_request(WT_MSTR, WT_COIN, "100", "2.5"),
        )
        .await
        .unwrap();
        let request = captured_take_orders_request(&captured_request);

        assert_eq!(request.amount, "25");
        assert_eq!(request.price_cap, "5");
        assert_eq!(result.estimated_input, "300");
        assert_eq!(result.denomination, SwapDenomination::Unwrapped);
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_unwrapped_noop_for_non_wrapped_tokens() {
        let (ds, captured_request) = capture_ds(ready_response(), HashMap::new());
        let result =
            process_swap_calldata(&ds, unwrapped_calldata_request(USDC, WETH, "100.0", "2.50"))
                .await
                .unwrap();
        let request = captured_take_orders_request(&captured_request);

        assert_eq!(request.amount, "100.0");
        assert_eq!(request.price_cap, "2.50");
        assert_eq!(result.estimated_input, "150");
        assert_eq!(result.denomination, SwapDenomination::Unwrapped);
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_unwrapped_keeps_approval_amount_wrapped() {
        let (ds, captured_request) = capture_ds(
            SwapCalldataResponse {
                estimated_input: "1000".to_string(),
                approvals: vec![Approval {
                    token: WT_MSTR,
                    spender: ORDERBOOK,
                    amount: "1000".to_string(),
                    symbol: "wtMSTR".to_string(),
                    approval_data: Bytes::from(vec![0x09, 0x5e, 0xa7, 0xb3]),
                }],
                ..approval_response()
            },
            HashMap::from([(WT_MSTR, wrap_ratio(WT_MSTR, "2"))]),
        );
        let result =
            process_swap_calldata(&ds, unwrapped_calldata_request(WT_MSTR, WETH, "100", "2.5"))
                .await
                .unwrap();
        let request = captured_take_orders_request(&captured_request);

        assert_eq!(request.price_cap, "1.25");
        assert_eq!(result.estimated_input, "2000");
        assert_eq!(result.denomination, SwapDenomination::Unwrapped);
        assert_eq!(result.approvals.len(), 1);
        assert_eq!(result.approvals[0].token, WT_MSTR);
        assert_eq!(result.approvals[0].amount, "1000");
        assert_eq!(result.approvals[0].symbol, "wtMSTR");
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_unwrapped_invalid_output_amount_is_bad_request() {
        let (ds, captured_request) = capture_ds(
            ready_response(),
            HashMap::from([(WT_MSTR, wrap_ratio(WT_MSTR, "2"))]),
        );
        let result = process_swap_calldata(
            &ds,
            unwrapped_calldata_request(USDC, WT_MSTR, "not-a-number", "2.5"),
        )
        .await;

        assert!(matches!(result, Err(ApiError::BadRequest(msg)) if msg == "invalid output_amount"));
        no_take_orders_request_was_made(&captured_request);
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_unwrapped_invalid_maximum_io_ratio_is_bad_request() {
        let (ds, captured_request) = capture_ds(
            ready_response(),
            HashMap::from([(WT_MSTR, wrap_ratio(WT_MSTR, "2"))]),
        );
        let result = process_swap_calldata(
            &ds,
            unwrapped_calldata_request(USDC, WT_MSTR, "100", "not-a-number"),
        )
        .await;

        assert!(
            matches!(result, Err(ApiError::BadRequest(msg)) if msg == "invalid maximum_io_ratio")
        );
        no_take_orders_request_was_made(&captured_request);
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_v2_unwrapped_invalid_amount_is_bad_request() {
        let (ds, captured_request) = capture_ds(
            ready_response(),
            HashMap::from([(WT_MSTR, wrap_ratio(WT_MSTR, "2"))]),
        );
        let result = process_swap_calldata_v2(
            &ds,
            unwrapped_calldata_v2_request(
                WT_MSTR,
                WETH,
                SwapCalldataMode::SpendUpTo,
                "not-a-number",
                "2.5",
            ),
        )
        .await;

        assert!(matches!(result, Err(ApiError::BadRequest(msg)) if msg == "invalid amount"));
        no_take_orders_request_was_made(&captured_request);
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_v2_unwrapped_invalid_price_cap_is_bad_request() {
        let (ds, captured_request) = capture_ds(
            ready_response(),
            HashMap::from([(WT_MSTR, wrap_ratio(WT_MSTR, "2"))]),
        );
        let result = process_swap_calldata_v2(
            &ds,
            unwrapped_calldata_v2_request(
                WT_MSTR,
                WETH,
                SwapCalldataMode::SpendUpTo,
                "100",
                "not-a-number",
            ),
        )
        .await;

        assert!(matches!(result, Err(ApiError::BadRequest(msg)) if msg == "invalid price_cap"));
        no_take_orders_request_was_made(&captured_request);
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_unwrapped_wrap_ratio_lookup_failure() {
        let (ds, captured_request) = capture_ds_with_wrap_result(
            ready_response(),
            Err(ApiError::Internal("failed to read wrap ratios".into())),
        );
        let result =
            process_swap_calldata(&ds, unwrapped_calldata_request(WT_MSTR, WETH, "100", "2.5"))
                .await;

        assert_error_code(result, ApiErrorCode::SwapCalldataFailed);
        no_take_orders_request_was_made(&captured_request);
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_unwrapped_malformed_wrap_ratio_is_internal_error() {
        let (ds, captured_request) = capture_ds(
            ready_response(),
            HashMap::from([(WT_MSTR, wrap_ratio(WT_MSTR, "not-a-number"))]),
        );
        let result =
            process_swap_calldata(&ds, unwrapped_calldata_request(USDC, WT_MSTR, "100", "2.5"))
                .await;

        assert_error_code(result, ApiErrorCode::SwapCalldataFailed);
        no_take_orders_request_was_made(&captured_request);
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_unwrapped_invalid_estimated_input_is_internal_error() {
        let (ds, captured_request) = capture_ds(
            SwapCalldataResponse {
                estimated_input: "not-a-number".to_string(),
                ..ready_response()
            },
            HashMap::from([(WT_MSTR, wrap_ratio(WT_MSTR, "2"))]),
        );
        let result =
            process_swap_calldata(&ds, unwrapped_calldata_request(WT_MSTR, WETH, "100", "2.5"))
                .await;

        let request = captured_take_orders_request(&captured_request);
        assert_eq!(request.price_cap, "1.25");
        assert_error_code(result, ApiErrorCode::SwapCalldataFailed);
    }

    #[test]
    fn test_swap_calldata_request_defaults_to_wrapped_denomination() {
        let request: SwapCalldataRequest = serde_json::from_str(
            r#"{"taker":"0x1111111111111111111111111111111111111111","inputToken":"0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913","outputToken":"0x4200000000000000000000000000000000000006","outputAmount":"100","maximumIoRatio":"2.5"}"#,
        )
        .unwrap();

        assert_eq!(request.denomination, SwapDenomination::Wrapped);
    }

    #[test]
    fn test_swap_calldata_v2_request_defaults_to_wrapped_denomination() {
        let request: SwapCalldataV2Request = serde_json::from_str(
            r#"{"taker":"0x1111111111111111111111111111111111111111","inputToken":"0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913","outputToken":"0x4200000000000000000000000000000000000006","mode":"spendExact","amount":"100","priceCap":"2.5"}"#,
        )
        .unwrap();

        assert_eq!(request.mode, SwapCalldataMode::SpendExact);
        assert_eq!(request.denomination, SwapDenomination::Wrapped);
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_not_found() {
        let ds = MockSwapDataSource {
            supported_tokens: Ok(()),
            orders: Ok(vec![]),
            candidates: vec![],
            calldata_result: Err(ApiError::coded(
                ApiErrorCode::SwapNoLiquidity,
                "no executable liquidity is available for this pair",
            )),
        };
        let result = process_swap_calldata(&ds, calldata_request("100", "2.5")).await;
        assert_error_code(result, ApiErrorCode::SwapNoLiquidity);
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_bad_request() {
        let ds = MockSwapDataSource {
            supported_tokens: Ok(()),
            orders: Ok(vec![mock_order()]),
            candidates: vec![mock_candidate("1000", "1.5")],
            calldata_result: Err(ApiError::BadRequest("invalid parameters".into())),
        };
        let result = process_swap_calldata(&ds, calldata_request("not-a-number", "2.5")).await;
        assert!(matches!(result, Err(ApiError::BadRequest(_))));
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_internal_error() {
        let ds = MockSwapDataSource {
            supported_tokens: Ok(()),
            orders: Ok(vec![mock_order()]),
            candidates: vec![mock_candidate("1000", "1.5")],
            calldata_result: Err(ApiError::Internal("failed to generate calldata".into())),
        };
        let result = process_swap_calldata(&ds, calldata_request("100", "2.5")).await;
        assert_error_code(result, ApiErrorCode::SwapCalldataFailed);
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_rejects_unsupported_tokens() {
        let ds = MockSwapDataSource {
            supported_tokens: Err(ApiError::coded(
                ApiErrorCode::SwapUnsupportedToken,
                "one or both swap tokens are unsupported",
            )),
            orders: Ok(vec![]),
            candidates: vec![],
            calldata_result: Ok(ready_response()),
        };
        let result = process_swap_calldata(&ds, calldata_request("100", "2.5")).await;
        assert_error_code(result, ApiErrorCode::SwapUnsupportedToken);
    }

    #[rocket::async_test]
    async fn test_process_swap_calldata_registry_failure_is_not_retryable() {
        let ds = MockSwapDataSource {
            supported_tokens: Err(ApiError::Internal("invalid local registry".into())),
            orders: Ok(vec![]),
            candidates: vec![],
            calldata_result: Ok(ready_response()),
        };
        let result = process_swap_calldata(&ds, calldata_request("100", "2.5")).await;
        assert_error_code(result, ApiErrorCode::SwapCalldataFailed);
    }

    #[rocket::async_test]
    async fn test_swap_calldata_401_without_auth() {
        let client = TestClientBuilder::new().build().await;
        let response = client
            .post("/v1/swap/calldata")
            .header(ContentType::JSON)
            .body(r#"{"taker":"0x1111111111111111111111111111111111111111","inputToken":"0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913","outputToken":"0x4200000000000000000000000000000000000006","outputAmount":"100","maximumIoRatio":"2.5"}"#)
            .dispatch()
            .await;
        assert_eq!(response.status(), Status::Unauthorized);
    }

    #[rocket::async_test]
    async fn test_swap_calldata_v2_401_without_auth() {
        let client = TestClientBuilder::new().build().await;
        let response = client
            .post("/v2/swap/calldata")
            .header(ContentType::JSON)
            .body(r#"{"taker":"0x1111111111111111111111111111111111111111","inputToken":"0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913","outputToken":"0x4200000000000000000000000000000000000006","mode":"spendExact","amount":"100","priceCap":"2.5"}"#)
            .dispatch()
            .await;
        assert_eq!(response.status(), Status::Unauthorized);
    }

    #[rocket::async_test]
    async fn test_v3_swap_calldata_requires_chain_id() {
        let client = TestClientBuilder::new().build().await;
        let (key_id, secret) = crate::test_helpers::seed_api_key(&client).await;
        let header = crate::test_helpers::basic_auth_header(&key_id, &secret);
        let response = client
            .post("/v3/swap/calldata")
            .header(ContentType::JSON)
            .header(rocket::http::Header::new("Authorization", header))
            .body(r#"{"taker":"0x1111111111111111111111111111111111111111","inputToken":"0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913","outputToken":"0x4200000000000000000000000000000000000006","mode":"spendExact","amount":"100","priceCap":"2.5"}"#)
            .dispatch()
            .await;
        assert_eq!(response.status(), Status::BadRequest);
        let body = response.into_json::<ApiErrorResponse>().await.unwrap();
        assert_eq!(body.error.message, "chainId is required");
    }

    #[rocket::async_test]
    async fn test_swap_calldata_400_for_unsupported_tokens() {
        let client = TestClientBuilder::new().build().await;
        let (key_id, secret) = crate::test_helpers::seed_api_key(&client).await;
        let header = crate::test_helpers::basic_auth_header(&key_id, &secret);
        let response = client
            .post("/v1/swap/calldata")
            .header(ContentType::JSON)
            .header(rocket::http::Header::new("Authorization", header))
            .body(r#"{"taker":"0x1111111111111111111111111111111111111111","inputToken":"0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913","outputToken":"0x4200000000000000000000000000000000000006","outputAmount":"100","maximumIoRatio":"2.5"}"#)
            .dispatch()
            .await;
        assert_eq!(response.status(), Status::BadRequest);
        let body = response.into_json::<ApiErrorResponse>().await.unwrap();
        assert_eq!(body.error.code, ApiErrorCode::SwapUnsupportedToken);
        assert!(!body.request_id.is_empty());
    }

    #[rocket::async_test]
    async fn test_swap_calldata_v2_400_for_unsupported_tokens() {
        let client = TestClientBuilder::new().build().await;
        let (key_id, secret) = crate::test_helpers::seed_api_key(&client).await;
        let header = crate::test_helpers::basic_auth_header(&key_id, &secret);
        let response = client
            .post("/v2/swap/calldata")
            .header(ContentType::JSON)
            .header(rocket::http::Header::new("Authorization", header))
            .body(r#"{"taker":"0x1111111111111111111111111111111111111111","inputToken":"0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913","outputToken":"0x4200000000000000000000000000000000000006","mode":"spendExact","amount":"100","priceCap":"2.5"}"#)
            .dispatch()
            .await;
        assert_eq!(response.status(), Status::BadRequest);
    }

    #[rocket::async_test]
    async fn test_swap_calldata_422_for_invalid_denomination() {
        let client = TestClientBuilder::new().build().await;
        let (key_id, secret) = crate::test_helpers::seed_api_key(&client).await;
        let header = crate::test_helpers::basic_auth_header(&key_id, &secret);
        let response = client
            .post("/v1/swap/calldata")
            .header(ContentType::JSON)
            .header(rocket::http::Header::new("Authorization", header))
            .body(r#"{"taker":"0x1111111111111111111111111111111111111111","inputToken":"0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913","outputToken":"0x4200000000000000000000000000000000000006","outputAmount":"100","maximumIoRatio":"2.5","denomination":"invalid"}"#)
            .dispatch()
            .await;
        assert_eq!(response.status(), Status::UnprocessableEntity);
    }

    #[rocket::async_test]
    async fn test_swap_calldata_v2_422_for_buy_exact_mode() {
        let client = TestClientBuilder::new().build().await;
        let (key_id, secret) = crate::test_helpers::seed_api_key(&client).await;
        let header = crate::test_helpers::basic_auth_header(&key_id, &secret);
        let response = client
            .post("/v2/swap/calldata")
            .header(ContentType::JSON)
            .header(rocket::http::Header::new("Authorization", header))
            .body(r#"{"taker":"0x1111111111111111111111111111111111111111","inputToken":"0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913","outputToken":"0x4200000000000000000000000000000000000006","mode":"buyExact","amount":"100","priceCap":"2.5"}"#)
            .dispatch()
            .await;
        assert_eq!(response.status(), Status::UnprocessableEntity);
    }
}

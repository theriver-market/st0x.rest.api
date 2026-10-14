use super::{
    build_trades_list_response, get_by_token::trades_cache_key, trades_pagination_params,
    RaindexTradesDataSource, TradesDataSource,
};
use crate::app_state::ApplicationState;
use crate::auth::AuthenticatedKey;
use crate::db::DbPool;
use crate::error::{ApiError, ApiErrorResponse};
use crate::fairings::{GlobalRateLimit, TracingSpan};
use crate::types::trades::{TradesByAddressResponse, TradesPaginationParams};
use alloy::primitives::Address;
use rocket::serde::json::Json;
use rocket::State;
use tracing::Instrument;

/// Latest Raindex trades across every token (for the site's live trades ticker).
#[utoipa::path(
    get,
    path = "/v2/trades/recent",
    tag = "Trades",
    security(("basicAuth" = [])),
    params(TradesPaginationParams),
    responses(
        (status = 200, description = "Most recent trades, newest first", body = TradesByAddressResponse),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 429, description = "Rate limited", body = ApiErrorResponse),
        (status = 500, description = "Internal server error", body = ApiErrorResponse),
    )
)]
#[get("/recent?<params..>")]
pub async fn get_recent_trades(
    _global: GlobalRateLimit,
    _key: AuthenticatedKey,
    shared_raindex: &State<crate::raindex::SharedRaindexProvider>,
    app_state: &State<ApplicationState>,
    pool: &State<DbPool>,
    span: TracingSpan,
    params: TradesPaginationParams,
) -> Result<Json<TradesByAddressResponse>, ApiError> {
    async move {
        tracing::info!(params = ?params, "request received");
        let (client, chain_ids) = {
            let raindex = shared_raindex.read().await;
            let chain_ids =
                crate::routes::optional_chain_ids_filter(raindex.raindex_yaml(), params.chain_id)?;
            (raindex.client().clone(), chain_ids)
        };
        let cache_key = trades_cache_key("trades/recent", Address::ZERO, &params);
        let run = |params: TradesPaginationParams| {
            let client = client.clone();
            let chain_ids = chain_ids.clone();
            async move {
                let ds = RaindexTradesDataSource {
                    client: &client,
                    pool: pool.inner(),
                };
                process_get_recent_trades(&ds, chain_ids, params).await
            }
        };
        if !app_state.response_caches.is_enabled() {
            return run(params).await;
        }
        let response = app_state
            .response_caches
            .trades_by_token
            .get_or_try_insert(cache_key, || async move {
                run(params).await.map(Json::into_inner)
            })
            .await
            .map_err(|e| (*e).clone())?;
        Ok(Json(response))
    }
    .instrument(span.0)
    .await
}

pub(super) async fn process_get_recent_trades(
    ds: &dyn TradesDataSource,
    chain_ids: Option<Vec<u32>>,
    params: TradesPaginationParams,
) -> Result<Json<TradesByAddressResponse>, ApiError> {
    let denomination = params.denomination.unwrap_or_default();
    let (page, page_size, sdk_page, sdk_page_size, time_filter) = trades_pagination_params(params)?;
    if page_size > 50 {
        return Err(ApiError::BadRequest("pageSize must be at most 50".into()));
    }
    let result = ds
        .get_recent_trades_on_chains(chain_ids, sdk_page, sdk_page_size, time_filter)
        .await?;
    build_trades_list_response(ds, result, page, page_size, denomination).await
}

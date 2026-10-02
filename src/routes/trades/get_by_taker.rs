use super::{
    build_trades_list_response, build_trades_list_response_from_parts, trades_pagination_params,
    RaindexTradesDataSource, TradesDataSource,
};
use crate::app_state::ApplicationState;
use crate::auth::AuthenticatedKey;
use crate::db::DbPool;
use crate::error::{ApiError, ApiErrorResponse};
use crate::fairings::{GlobalRateLimit, TracingSpan};
use crate::types::common::ValidatedAddress;
use crate::types::trades::{TradesByAddressResponse, TradesPaginationParams};
use alloy::primitives::{Address, Bytes, B256, U256};
use rain_orderbook_common::raindex_client::trades::RaindexTrade;
use rocket::serde::json::Json;
use rocket::State;
use std::collections::HashSet;
use tracing::Instrument;

#[utoipa::path(
    get,
    path = "/v2/trades/taker/{address}",
    tag = "Trades",
    security(("basicAuth" = [])),
    params(
        ("address" = String, Path, description = "Taker address"),
        TradesPaginationParams,
    ),
    responses(
        (status = 200, description = "Paginated list of trades for taker", body = TradesByAddressResponse),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 429, description = "Rate limited", body = ApiErrorResponse),
        (status = 500, description = "Internal server error", body = ApiErrorResponse),
    )
)]
#[allow(clippy::too_many_arguments)]
#[get("/taker/<address>?<params..>")]
pub async fn get_trades_by_taker(
    _global: GlobalRateLimit,
    _key: AuthenticatedKey,
    shared_raindex: &State<crate::raindex::SharedRaindexProvider>,
    app_state: &State<ApplicationState>,
    pool: &State<DbPool>,
    span: TracingSpan,
    address: ValidatedAddress,
    params: TradesPaginationParams,
) -> Result<Json<TradesByAddressResponse>, ApiError> {
    let mut params = params;
    params.chain_id = crate::routes::compatibility_chain_id(span.api_version(), params.chain_id);
    async move {
        tracing::info!(address = ?address, params = ?params, "request received");
        let addr = address.0;
        let (client, chain_ids) = {
            let raindex = shared_raindex.read().await;
            let chain_ids =
                crate::routes::optional_chain_ids_filter(raindex.raindex_yaml(), params.chain_id)?;
            (raindex.client().clone(), chain_ids)
        };
        if !app_state.response_caches.is_enabled() {
            let ds = RaindexTradesDataSource {
                client: &client,
                pool: pool.inner(),
            };
            return process_get_trades_by_taker_for_chains(&ds, chain_ids, addr, params).await;
        }

        let cache_key = super::get_by_token::trades_cache_key("trades/taker", addr, &params);
        let response = app_state
            .response_caches
            .trades_by_taker
            .get_or_try_insert(cache_key, || async move {
                let ds = RaindexTradesDataSource {
                    client: &client,
                    pool: pool.inner(),
                };
                process_get_trades_by_taker_for_chains(&ds, chain_ids, addr, params)
                    .await
                    .map(Json::into_inner)
            })
            .await
            .map_err(|e| (*e).clone())?;
        Ok(Json(response))
    }
    .instrument(span.0)
    .await
}

#[cfg(test)]
pub(super) async fn process_get_trades_by_taker(
    ds: &dyn TradesDataSource,
    taker: Address,
    params: TradesPaginationParams,
) -> Result<Json<TradesByAddressResponse>, ApiError> {
    process_get_trades_by_taker_for_chains(ds, None, taker, params).await
}

async fn process_get_trades_by_taker_for_chains(
    ds: &dyn TradesDataSource,
    chain_ids: Option<Vec<u32>>,
    taker: Address,
    params: TradesPaginationParams,
) -> Result<Json<TradesByAddressResponse>, ApiError> {
    let denomination = params.denomination.unwrap_or_default();
    let (page, page_size, sdk_page, sdk_page_size, time_filter) = trades_pagination_params(params)?;

    tracing::info!(taker = ?taker, page, page_size, "querying trades by taker");
    let river_hashes = ds.river_take_tx_hashes(chain_ids.as_deref(), taker).await?;
    if river_hashes.is_empty() {
        let result = ds
            .get_trades_for_taker_on_chains(chain_ids, taker, sdk_page, sdk_page_size, time_filter)
            .await?;
        return build_trades_list_response(ds, result, page, page_size, denomination).await;
    }

    // Merge the user's direct fills with the fills RiverTaker took for them.
    // Fetch every direct fill up to the end of the requested page, merge,
    // order newest first, then cut the page.
    let end = u64::from(page) * u64::from(page_size);
    let direct_size = u16::try_from(end)
        .map_err(|_| ApiError::BadRequest("page * page_size too large".into()))?;
    let direct = ds
        .get_trades_for_taker_on_chains(
            chain_ids.clone(),
            taker,
            1,
            direct_size,
            time_filter.clone(),
        )
        .await?;
    let mut trades: Vec<RaindexTrade> = direct.trades().to_vec();
    let mut seen: HashSet<(B256, Bytes)> = trades
        .iter()
        .map(|t| (t.transaction().id(), t.id()))
        .collect();
    let mut river_count: u64 = 0;
    for hash in river_hashes {
        let in_tx = match ds
            .get_trades_by_tx_for_chains(chain_ids.clone(), hash)
            .await
        {
            Ok(result) => result,
            Err(error) => {
                // The local orderbook DB can lag the RiverTake index by a sync tick.
                tracing::warn!(tx = %hash, ?error, "RiverTaker fill not yet in trades DB");
                continue;
            }
        };
        for trade in in_tx.trades() {
            let ts = trade.timestamp();
            let after_start = time_filter.start.is_none_or(|s| ts >= U256::from(s));
            let before_end = time_filter.end.is_none_or(|e| ts <= U256::from(e));
            if !(after_start && before_end) {
                continue;
            }
            if seen.insert((trade.transaction().id(), trade.id())) {
                river_count += 1;
                trades.push(trade.clone());
            }
        }
    }
    trades.sort_by(|a, b| {
        b.timestamp()
            .cmp(&a.timestamp())
            .then_with(|| b.id().cmp(&a.id()))
    });
    let start = usize::try_from(end - u64::from(page_size)).unwrap_or(usize::MAX);
    let page_trades: Vec<RaindexTrade> = trades
        .into_iter()
        .skip(start)
        .take(page_size as usize)
        .collect();
    let total = direct.total_count() + river_count;
    tracing::info!(
        river_count,
        total,
        "merged RiverTaker fills into taker trades"
    );

    build_trades_list_response_from_parts(ds, &page_trades, total, page, page_size, denomination)
        .await
        .map(Json)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ApiError;
    use crate::routes::order::test_fixtures::{
        mock_empty_trades_list_result, mock_trades_list_result,
    };
    use crate::test_helpers::{basic_auth_header, seed_api_key, TestClientBuilder};
    use alloy::primitives::{address, B256};
    use async_trait::async_trait;
    use rain_orderbook_common::raindex_client::trades::RaindexTradesListResult;
    use rain_orderbook_common::raindex_client::types::{PaginationParams, TimeFilter};
    use rocket::http::{Header, Status};
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Clone)]
    struct CapturedTakerQuery {
        taker: Address,
        page: u16,
        page_size: u16,
        time_filter: TimeFilter,
    }

    struct MockTradesDataSource {
        taker_result: Result<RaindexTradesListResult, ApiError>,
        captured: Arc<Mutex<Option<CapturedTakerQuery>>>,
        river_hashes: Vec<B256>,
        by_tx: Option<RaindexTradesListResult>,
    }

    #[async_trait]
    impl TradesDataSource for MockTradesDataSource {
        async fn get_trades_by_tx(
            &self,
            _tx_hash: B256,
        ) -> Result<RaindexTradesListResult, ApiError> {
            self.by_tx
                .clone()
                .ok_or_else(|| ApiError::Internal("no trades for tx".into()))
        }

        async fn river_take_tx_hashes(
            &self,
            _chain_ids: Option<&[u32]>,
            _taker: Address,
        ) -> Result<Vec<B256>, ApiError> {
            Ok(self.river_hashes.clone())
        }

        async fn get_trades_for_owner(
            &self,
            _owner: Address,
            _pagination: PaginationParams,
            _time_filter: TimeFilter,
        ) -> Result<RaindexTradesListResult, ApiError> {
            unimplemented!()
        }

        async fn get_trades_for_token(
            &self,
            _token: Address,
            _page: u16,
            _page_size: u16,
            _time_filter: TimeFilter,
        ) -> Result<RaindexTradesListResult, ApiError> {
            unimplemented!()
        }

        async fn get_trades_for_taker(
            &self,
            taker: Address,
            page: u16,
            page_size: u16,
            time_filter: TimeFilter,
        ) -> Result<RaindexTradesListResult, ApiError> {
            *self.captured.lock().unwrap() = Some(CapturedTakerQuery {
                taker,
                page,
                page_size,
                time_filter,
            });
            match &self.taker_result {
                Ok(r) => Ok(r.clone()),
                Err(e) => Err(e.clone()),
            }
        }
    }

    #[rocket::async_test]
    async fn test_process_success() {
        let captured = Arc::new(Mutex::new(None));
        let ds = MockTradesDataSource {
            taker_result: Ok(mock_trades_list_result()),
            captured: Arc::clone(&captured),
            river_hashes: Vec::new(),
            by_tx: None,
        };
        let taker = address!("cccccccccccccccccccccccccccccccccccccccc");
        let params = TradesPaginationParams {
            chain_id: None,
            page: Some(2),
            page_size: Some(10),
            start_time: Some(1700000000),
            end_time: Some(1700002000),
            denomination: None,
        };
        let result = process_get_trades_by_taker(&ds, taker, params)
            .await
            .unwrap();

        let response = result.into_inner();
        assert_eq!(response.trades.len(), 1);
        assert_eq!(response.pagination.page, 2);
        assert_eq!(response.pagination.page_size, 10);
        assert_eq!(response.pagination.total_trades, 1);
        assert_eq!(response.pagination.total_pages, 1);
        assert!(!response.pagination.has_more);

        let t = &response.trades[0];
        assert_eq!(t.timestamp, 1700001000);
        assert_eq!(t.block_number, 100);
        assert_eq!(t.input_amount, "0.500000");
        assert_eq!(t.output_amount, "-0.250000000000000000");
        assert_eq!(t.input_token.symbol, "USDC");
        assert_eq!(t.output_token.symbol, "WETH");

        let captured = captured.lock().unwrap().clone().unwrap();
        assert_eq!(captured.taker, taker);
        assert_eq!(captured.page, 2);
        assert_eq!(captured.page_size, 10);
        assert_eq!(captured.time_filter.start, Some(1700000000));
        assert_eq!(captured.time_filter.end, Some(1700002000));
    }

    #[rocket::async_test]
    async fn test_process_no_trades() {
        let ds = MockTradesDataSource {
            taker_result: Ok(mock_empty_trades_list_result()),
            captured: Arc::new(Mutex::new(None)),
            river_hashes: Vec::new(),
            by_tx: None,
        };
        let params = TradesPaginationParams {
            chain_id: None,
            page: Some(1),
            page_size: Some(20),
            start_time: None,
            end_time: None,
            denomination: None,
        };
        let result = process_get_trades_by_taker(
            &ds,
            address!("cccccccccccccccccccccccccccccccccccccccc"),
            params,
        )
        .await
        .unwrap();

        let response = result.into_inner();
        assert!(response.trades.is_empty());
        assert_eq!(response.pagination.total_trades, 0);
        assert_eq!(response.pagination.total_pages, 0);
        assert!(!response.pagination.has_more);
    }

    #[rocket::async_test]
    async fn test_process_query_failure() {
        let ds = MockTradesDataSource {
            taker_result: Err(ApiError::Internal("subgraph error".into())),
            captured: Arc::new(Mutex::new(None)),
            river_hashes: Vec::new(),
            by_tx: None,
        };
        let params = TradesPaginationParams {
            chain_id: None,
            page: Some(1),
            page_size: Some(20),
            start_time: None,
            end_time: None,
            denomination: None,
        };
        let result = process_get_trades_by_taker(
            &ds,
            address!("cccccccccccccccccccccccccccccccccccccccc"),
            params,
        )
        .await;
        assert!(matches!(result, Err(ApiError::Internal(_))));
    }

    #[rocket::async_test]
    async fn test_401_without_auth() {
        let client = TestClientBuilder::new().build().await;
        let response = client
            .get("/v1/trades/taker/0xcccccccccccccccccccccccccccccccccccccccc")
            .dispatch()
            .await;
        assert_eq!(response.status(), Status::Unauthorized);
    }

    #[rocket::async_test]
    async fn test_invalid_address_returns_422() {
        let client = TestClientBuilder::new().build().await;
        let (key_id, secret) = seed_api_key(&client).await;
        let header = basic_auth_header(&key_id, &secret);
        let response = client
            .get("/v1/trades/taker/not-an-address")
            .header(Header::new("Authorization", header))
            .dispatch()
            .await;
        assert_eq!(response.status(), Status::UnprocessableEntity);
    }

    #[test]
    fn test_route_is_registered() {
        let routes = crate::routes::trades::routes();
        assert!(routes
            .iter()
            .any(|route| route.uri.path() == "/taker/<address>"));
    }

    fn river_trade_list(timestamp_hex: &str) -> RaindexTradesListResult {
        fn retime(v: &mut serde_json::Value, ts: &str) {
            match v {
                serde_json::Value::Object(map) => {
                    for (k, val) in map.iter_mut() {
                        if k == "timestamp" {
                            *val = serde_json::Value::String(ts.to_string());
                        } else {
                            retime(val, ts);
                        }
                    }
                }
                serde_json::Value::Array(items) => items.iter_mut().for_each(|i| retime(i, ts)),
                _ => {}
            }
        }
        let mut trade = crate::routes::order::test_fixtures::trade_json();
        retime(&mut trade, timestamp_hex);
        trade["id"] =
            serde_json::json!("0x0000000000000000000000000000000000000000000000000000000000000043");
        trade["transaction"]["id"] =
            serde_json::json!("0x00000000000000000000000000000000000000000000000000000000000000a1");
        serde_json::from_value(serde_json::json!({
            "trades": [trade],
            "totalCount": 1,
            "summary": null
        }))
        .unwrap()
    }

    // 1700001100 = 0x6553f54c; the direct fixture trade is at 1700001000 (0x6553f4e8).
    const RIVER_TS: &str = "0x000000000000000000000000000000000000000000000000000000006553f54c";

    #[rocket::async_test]
    async fn merges_rivertaker_fills_newest_first() {
        let ds = MockTradesDataSource {
            taker_result: Ok(mock_trades_list_result()),
            captured: Arc::new(Mutex::new(None)),
            river_hashes: vec![B256::repeat_byte(0xa1)],
            by_tx: Some(river_trade_list(RIVER_TS)),
        };
        let taker = address!("cccccccccccccccccccccccccccccccccccccccc");
        let params = TradesPaginationParams {
            chain_id: None,
            page: Some(1),
            page_size: Some(10),
            start_time: None,
            end_time: None,
            denomination: None,
        };
        let Json(resp) = process_get_trades_by_taker(&ds, taker, params)
            .await
            .unwrap();
        assert_eq!(resp.pagination.total_trades, 2);
        assert_eq!(resp.trades.len(), 2);
        assert_eq!(resp.trades[0].timestamp, 1_700_001_100);
        assert_eq!(resp.trades[1].timestamp, 1_700_001_000);
        // The direct query was asked for everything up to the end of the page.
        let captured = ds.captured.lock().unwrap().clone().unwrap();
        assert_eq!((captured.page, captured.page_size), (1, 10));
    }

    #[rocket::async_test]
    async fn rivertaker_fills_respect_the_time_filter_and_paging() {
        let make = || MockTradesDataSource {
            taker_result: Ok(mock_trades_list_result()),
            captured: Arc::new(Mutex::new(None)),
            river_hashes: vec![B256::repeat_byte(0xa1)],
            by_tx: Some(river_trade_list(RIVER_TS)),
        };
        let taker = address!("cccccccccccccccccccccccccccccccccccccccc");
        let filtered = TradesPaginationParams {
            chain_id: None,
            page: Some(1),
            page_size: Some(10),
            start_time: None,
            end_time: Some(1_700_001_050),
            denomination: None,
        };
        let Json(resp) = process_get_trades_by_taker(&make(), taker, filtered)
            .await
            .unwrap();
        assert_eq!(resp.pagination.total_trades, 1);
        assert_eq!(resp.trades[0].timestamp, 1_700_001_000);

        // Page 2 of size 1 is the older (direct) trade.
        let page2 = TradesPaginationParams {
            chain_id: None,
            page: Some(2),
            page_size: Some(1),
            start_time: None,
            end_time: None,
            denomination: None,
        };
        let ds = make();
        let Json(resp) = process_get_trades_by_taker(&ds, taker, page2)
            .await
            .unwrap();
        assert_eq!(resp.trades.len(), 1);
        assert_eq!(resp.trades[0].timestamp, 1_700_001_000);
        assert_eq!(resp.pagination.total_trades, 2);
        assert!(!resp.pagination.has_more);
        let captured = ds.captured.lock().unwrap().clone().unwrap();
        assert_eq!((captured.page, captured.page_size), (1, 2));
    }
}

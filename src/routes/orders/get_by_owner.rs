use super::{
    active_filter_for_state, build_orders_list_response, current_wrap_ratios_for_orders,
    get_order_quotes_for_summaries, OrdersListDataSource, RaindexOrdersListDataSource,
    DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE,
};
use crate::app_state::ApplicationState;
use crate::auth::AuthenticatedKey;
use crate::db::DbPool;
use crate::error::{ApiError, ApiErrorResponse};
use crate::fairings::{GlobalRateLimit, TracingSpan};
use crate::types::common::{Denomination, ValidatedAddress};
use crate::types::orders::{OrderState, OrdersListResponse, OrdersPaginationParams};
use alloy::primitives::Address;
use rain_orderbook_common::raindex_client::orders::GetOrdersFilters;
use rocket::serde::json::Json;
use rocket::State;
use tracing::Instrument;

pub(crate) async fn process_get_orders_by_owner(
    ds: &dyn OrdersListDataSource,
    chain_ids: Option<Vec<u32>>,
    address: Address,
    state: Option<OrderState>,
    page: Option<u16>,
    page_size: Option<u16>,
    denomination: Denomination,
) -> Result<OrdersListResponse, ApiError> {
    let active_filter = active_filter_for_state(state);
    let filters = GetOrdersFilters {
        owners: vec![address],
        active: active_filter,
        has_positive_output_vault_balance: (active_filter == Some(true)).then_some(true),
        ..Default::default()
    };

    let page_num = page.unwrap_or(1);
    let effective_page_size = page_size
        .unwrap_or(DEFAULT_PAGE_SIZE as u16)
        .min(MAX_PAGE_SIZE);
    let (orders, total_count) = ds
        .get_orders_list(
            chain_ids,
            filters,
            Some(page_num),
            Some(effective_page_size),
        )
        .await?;

    tracing::info!(
        quoted_orders = orders.len(),
        "fetching batched quotes for orders by owner"
    );
    let quote_results = get_order_quotes_for_summaries(ds, &orders).await;
    let wrap_ratios = current_wrap_ratios_for_orders(ds, denomination, &orders).await?;

    let mut response = build_orders_list_response(
        &orders,
        total_count,
        page_num.into(),
        effective_page_size.into(),
        quote_results,
        denomination,
        &wrap_ratios,
    )?;
    // Owner lists carry each order's Rainlang so the site can group strategies.
    for (summary, order) in response.orders.iter_mut().zip(&orders) {
        summary.rainlang = super::order_source(order);
    }
    Ok(response)
}

#[utoipa::path(
    get,
    path = "/v2/orders/owner/{address}",
    tag = "Orders",
    security(("basicAuth" = [])),
    params(
        ("address" = String, Path, description = "Owner address"),
        OrdersPaginationParams,
    ),
    responses(
        (status = 200, description = "Paginated list of orders", body = OrdersListResponse),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 429, description = "Rate limited", body = ApiErrorResponse),
        (status = 500, description = "Internal server error", body = ApiErrorResponse),
    )
)]
#[allow(clippy::too_many_arguments)]
#[get("/owner/<address>?<params..>")]
pub async fn get_orders_by_address(
    _global: GlobalRateLimit,
    _key: AuthenticatedKey,
    shared_raindex: &State<crate::raindex::SharedRaindexProvider>,
    app_state: &State<ApplicationState>,
    pool: &State<DbPool>,
    span: TracingSpan,
    address: ValidatedAddress,
    params: OrdersPaginationParams,
) -> Result<Json<OrdersListResponse>, ApiError> {
    let requested_chain_id =
        crate::routes::compatibility_chain_id(span.api_version(), params.chain_id);
    async move {
        tracing::info!(address = ?address, params = ?params, "request received");
        let addr = address.0;
        let state = params.state;
        let (client, chain_ids) = {
            let raindex = shared_raindex.read().await;
            let chain_ids = crate::routes::optional_chain_ids_filter(
                raindex.raindex_yaml(),
                requested_chain_id,
            )?;
            (raindex.client().clone(), chain_ids)
        };
        let page = params.page;
        let page_size = params.page_size;
        let denomination = params.denomination.unwrap_or_default();
        let ds = RaindexOrdersListDataSource {
            client: &client,
            caches: &app_state.response_caches,
            pool: pool.inner(),
        };
        let response =
            process_get_orders_by_owner(&ds, chain_ids, addr, state, page, page_size, denomination)
                .await?;
        Ok(Json(response))
    }
    .instrument(span.0)
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::order::test_fixtures::{
        mock_order, mock_order_with_rainlang, mock_order_with_shared_vaults, mock_quote,
        RIVER_PEG_RAINLANG,
    };
    use crate::routes::orders::test_fixtures::{
        MockOrdersListDataSource, RecordingOrdersListDataSource,
    };
    use crate::test_helpers::{basic_auth_header, seed_api_key, TestClientBuilder};
    use crate::types::orders::OrderSummaryOrderType;
    use alloy::primitives::address;
    use rocket::http::{Header, Status};

    #[rocket::async_test]
    async fn test_process_get_orders_by_owner_success() {
        let ds = MockOrdersListDataSource {
            orders: Ok(vec![mock_order()]),
            total_count: 1,
            quotes: Ok(vec![mock_quote("1.5")]),
        };
        let addr: Address = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
            .parse()
            .unwrap();
        let result =
            process_get_orders_by_owner(&ds, None, addr, None, None, None, Denomination::Wrapped)
                .await
                .unwrap();

        assert_eq!(result.orders.len(), 1);
        assert_eq!(result.orders[0].input_token.symbol, "USDC");
        assert_eq!(result.orders[0].output_token.symbol, "WETH");
        assert_eq!(result.orders[0].chain_id, 8453);
        assert_eq!(result.orders[0].order_bytes.as_ref(), &[1]);
        assert!(result.orders[0].active);
        assert_eq!(result.orders[0].removed_at, None);
        assert_eq!(result.orders[0].order_type, OrderSummaryOrderType::Custom);
        assert_eq!(result.orders[0].io_ratio, "1.5");
        assert_eq!(result.orders[0].max_output.as_deref(), Some("1"));
        assert_eq!(result.pagination.total_orders, 1);
        assert_eq!(result.pagination.page, 1);
        assert!(!result.pagination.has_more);
    }

    #[rocket::async_test]
    async fn test_process_get_orders_by_owner_empty() {
        let ds = MockOrdersListDataSource {
            orders: Ok(vec![]),
            total_count: 0,
            quotes: Ok(vec![]),
        };
        let addr: Address = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
            .parse()
            .unwrap();
        let result =
            process_get_orders_by_owner(&ds, None, addr, None, None, None, Denomination::Wrapped)
                .await
                .unwrap();

        assert!(result.orders.is_empty());
        assert_eq!(result.pagination.total_orders, 0);
        assert_eq!(result.pagination.total_pages, 0);
    }

    #[rocket::async_test]
    async fn test_process_get_orders_by_owner_quote_failure_shows_dash() {
        let ds = MockOrdersListDataSource {
            orders: Ok(vec![mock_order()]),
            total_count: 1,
            quotes: Err(ApiError::Internal("quote error".into())),
        };
        let addr: Address = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
            .parse()
            .unwrap();
        let result =
            process_get_orders_by_owner(&ds, None, addr, None, None, None, Denomination::Wrapped)
                .await
                .unwrap();

        assert_eq!(result.orders[0].io_ratio, "-");
        assert_eq!(result.orders[0].max_output, None);
    }

    #[rocket::async_test]
    async fn test_process_get_orders_by_owner_query_failure() {
        let ds = MockOrdersListDataSource {
            orders: Err(ApiError::Internal("failed".into())),
            total_count: 0,
            quotes: Ok(vec![]),
        };
        let addr: Address = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
            .parse()
            .unwrap();
        let result =
            process_get_orders_by_owner(&ds, None, addr, None, None, None, Denomination::Wrapped)
                .await;
        assert!(matches!(result, Err(ApiError::Internal(_))));
    }

    #[rocket::async_test]
    async fn test_process_get_orders_by_owner_shared_vaults() {
        let ds = MockOrdersListDataSource {
            orders: Ok(vec![mock_order_with_shared_vaults()]),
            total_count: 1,
            quotes: Ok(vec![mock_quote("200.0")]),
        };
        let addr: Address = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
            .parse()
            .unwrap();
        let result =
            process_get_orders_by_owner(&ds, None, addr, None, None, None, Denomination::Wrapped)
                .await
                .unwrap();

        assert_eq!(result.orders.len(), 1);
        assert_eq!(result.orders[0].input_token.symbol, "wtMSTR");
        assert_eq!(result.orders[0].output_token.symbol, "wtMSTR");
        assert_eq!(result.orders[0].chain_id, 8453);
        assert_eq!(result.orders[0].io_ratio, "200.0");
    }

    #[rocket::async_test]
    async fn test_process_get_orders_by_owner_inactive_state_sets_active_false_filter() {
        let ds = RecordingOrdersListDataSource::default();
        let addr: Address = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
            .parse()
            .unwrap();

        let result = process_get_orders_by_owner(
            &ds,
            None,
            addr,
            Some(OrderState::Inactive),
            None,
            None,
            Denomination::Wrapped,
        )
        .await;

        assert!(result.is_ok());
        let filters = ds.filters.lock().expect("lock filters");
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].active, Some(false));
        assert_eq!(filters[0].has_positive_output_vault_balance, None);
    }

    #[rocket::async_test]
    async fn test_process_get_orders_by_owner_all_state_omits_active_filter() {
        let ds = RecordingOrdersListDataSource::default();
        let addr: Address = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
            .parse()
            .unwrap();

        let result = process_get_orders_by_owner(
            &ds,
            None,
            addr,
            Some(OrderState::All),
            None,
            None,
            Denomination::Wrapped,
        )
        .await;

        assert!(result.is_ok());
        let filters = ds.filters.lock().expect("lock filters");
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].active, None);
        assert_eq!(filters[0].has_positive_output_vault_balance, None);
    }

    fn owner() -> Address {
        address!("0000000000000000000000000000000000000001")
    }

    #[rocket::async_test]
    async fn test_process_get_orders_by_owner_includes_rainlang() {
        let ds = MockOrdersListDataSource {
            orders: Ok(vec![
                mock_order_with_rainlang(RIVER_PEG_RAINLANG),
                mock_order(),
            ]),
            total_count: 2,
            quotes: Ok(vec![mock_quote("1.5")]),
        };
        let result = process_get_orders_by_owner(
            &ds,
            None,
            owner(),
            None,
            None,
            None,
            Denomination::Wrapped,
        )
        .await
        .unwrap();

        assert_eq!(
            result.orders[0].rainlang.as_deref(),
            Some(RIVER_PEG_RAINLANG)
        );
        assert_eq!(result.orders[0].order_type, OrderSummaryOrderType::Peg);
        assert_eq!(result.orders[1].rainlang, None);

        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["orders"][0]["rainlang"], RIVER_PEG_RAINLANG);
        assert_eq!(json["orders"][0]["orderType"], "peg");
        // No source -> field omitted; existing fields keep their shape.
        assert!(json["orders"][1].get("rainlang").is_none());
        let mut keys: Vec<&str> = json["orders"][1]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                "active",
                "chainId",
                "createdAt",
                "inputToken",
                "ioRatio",
                "maxOutput",
                "orderBytes",
                "orderHash",
                "orderType",
                "orderbookId",
                "outputToken",
                "outputVaultBalance",
                "owner",
                "removedAt",
            ]
        );
    }

    #[rocket::async_test]
    async fn test_get_orders_by_owner_401_without_auth() {
        let client = TestClientBuilder::new().build().await;
        let response = client
            .get("/v1/orders/owner/0x833589fcd6edb6e08f4c7c32d4f71b54bda02913")
            .dispatch()
            .await;
        assert_eq!(response.status(), Status::Unauthorized);
    }

    #[rocket::async_test]
    async fn test_get_orders_by_owner_invalid_address_returns_422() {
        let client = TestClientBuilder::new().build().await;
        let (key_id, secret) = seed_api_key(&client).await;
        let header = basic_auth_header(&key_id, &secret);
        let response = client
            .get("/v1/orders/owner/not-an-address")
            .header(Header::new("Authorization", header))
            .dispatch()
            .await;
        assert_eq!(response.status(), Status::UnprocessableEntity);
    }
}

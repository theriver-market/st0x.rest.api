use super::{OrderDataSource, RaindexOrderDataSource};
use crate::app_state::ApplicationState;
use crate::auth::AuthenticatedKey;
use crate::db::DbPool;
use crate::error::{ApiError, ApiErrorResponse};
use crate::fairings::{GlobalRateLimit, TracingSpan};
use crate::types::common::{Denomination, TokenRef, ValidatedFixedBytes};
use crate::types::order::{
    OrderDetail, OrderDetailParams, OrderDetailsInfo, OrderTradeEntry, OrderType,
};
use crate::wrap_ratio::WrapRatioValue;
use alloy::primitives::{Address, B256};
use rain_orderbook_common::parsed_meta::ParsedMeta;
use rain_orderbook_common::raindex_client::orders::RaindexOrder;
use rain_orderbook_common::raindex_client::trades::RaindexTrade;
use rocket::serde::json::Json;
use rocket::State;
use std::collections::HashMap;
use tracing::Instrument;

#[utoipa::path(
    get,
    path = "/v2/order/{order_hash}",
    tag = "Order",
    security(("basicAuth" = [])),
    params(
        ("order_hash" = String, Path, description = "The order hash"),
        OrderDetailParams,
    ),
    responses(
        (status = 200, description = "Order details", body = OrderDetail),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 429, description = "Rate limited", body = ApiErrorResponse),
        (status = 404, description = "Order not found", body = ApiErrorResponse),
        (status = 500, description = "Internal server error", body = ApiErrorResponse),
    )
)]
#[allow(clippy::too_many_arguments)]
#[get("/<order_hash>?<params..>")]
pub async fn get_order(
    _global: GlobalRateLimit,
    _key: AuthenticatedKey,
    shared_raindex: &State<crate::raindex::SharedRaindexProvider>,
    app_state: &State<ApplicationState>,
    pool: &State<DbPool>,
    span: TracingSpan,
    order_hash: ValidatedFixedBytes,
    params: OrderDetailParams,
) -> Result<Json<OrderDetail>, ApiError> {
    let requested_chain_id =
        crate::routes::compatibility_chain_id(span.api_version(), params.chain_id);
    async move {
        tracing::info!(order_hash = ?order_hash, params = ?params, "request received");
        let hash = order_hash.0;
        let denomination = params.denomination.unwrap_or_default();
        let raindex = shared_raindex.read().await;
        let chain_id =
            crate::routes::resolve_required_raindex_chain_id(raindex.client(), requested_chain_id)?;
        tracing::info!(chain_id, "resolved required Raindex chain");
        let ds = RaindexOrderDataSource {
            client: raindex.client(),
            caches: &app_state.response_caches,
            pool: Some(pool.inner()),
        };
        let detail = process_get_order_for_chain(&ds, chain_id, hash, denomination).await?;
        Ok(Json(detail))
    }
    .instrument(span.0)
    .await
}

#[cfg(test)]
async fn process_get_order(
    ds: &dyn OrderDataSource,
    hash: B256,
    denomination: Denomination,
) -> Result<OrderDetail, ApiError> {
    process_get_order_for_chain(ds, 8453, hash, denomination).await
}

async fn process_get_order_for_chain(
    ds: &dyn OrderDataSource,
    chain_id: u32,
    hash: B256,
    denomination: Denomination,
) -> Result<OrderDetail, ApiError> {
    let orders = ds.get_orders_by_hash_on_chain(chain_id, hash).await?;
    let order = orders
        .into_iter()
        .next()
        .ok_or_else(|| ApiError::NotFound("order not found".into()))?;
    let quotes = ds.get_order_quotes(&order).await?;
    let io_ratio = quotes
        .first()
        .and_then(|q| q.data.as_ref())
        .map(|d| d.formatted_ratio.clone())
        .unwrap_or_else(|| "-".into());
    let trades = ds.get_order_trades(&order).await?;
    let wrap_ratios =
        current_wrap_ratios_for_order_detail(ds, denomination, &order, &trades).await?;
    let order_type = determine_order_type(&order);
    build_order_detail(
        &order,
        order_type,
        &io_ratio,
        &trades,
        denomination,
        &wrap_ratios,
    )
}

fn determine_order_type(order: &RaindexOrder) -> OrderType {
    for meta in order.parsed_meta() {
        if let ParsedMeta::OrderBuilderStateV1(builder_state) = meta {
            if builder_state
                .selected_deployment
                .to_lowercase()
                .contains("dca")
            {
                return OrderType::Dca;
            }
        }
    }
    OrderType::Solver
}

fn build_order_detail(
    order: &RaindexOrder,
    order_type: OrderType,
    io_ratio: &str,
    trades: &[RaindexTrade],
    denomination: Denomination,
    wrap_ratios: &HashMap<Address, WrapRatioValue>,
) -> Result<OrderDetail, ApiError> {
    let (input, output) = crate::routes::resolve_io_vaults(order)?;

    let input_token_info = input.token();
    let output_token_info = output.token();

    let trade_entries: Vec<OrderTradeEntry> = trades
        .iter()
        .map(|trade| map_trade(trade, denomination, wrap_ratios))
        .collect::<Result<Vec<_>, ApiError>>()?;

    let created_at: u64 = order.timestamp_added().try_into().unwrap_or(0);
    let input_vault_balance = if denomination == Denomination::Unwrapped {
        crate::denomination::convert_wrapped_amount_for_token(
            input.formatted_balance(),
            input_token_info.address(),
            wrap_ratios,
        )?
    } else {
        input.formatted_balance()
    };
    let output_vault_balance = if denomination == Denomination::Unwrapped {
        crate::denomination::convert_wrapped_amount_for_token(
            output.formatted_balance(),
            output_token_info.address(),
            wrap_ratios,
        )?
    } else {
        output.formatted_balance()
    };
    let converted_io_ratio = if denomination == Denomination::Unwrapped {
        crate::denomination::convert_wrapped_io_ratio(
            io_ratio.to_string(),
            input_token_info.address(),
            output_token_info.address(),
            wrap_ratios,
        )?
    } else {
        io_ratio.to_string()
    };

    Ok(OrderDetail {
        chain_id: order.chain_id(),
        order_hash: order.order_hash(),
        owner: order.owner(),
        order_details: OrderDetailsInfo {
            type_: order_type,
            io_ratio: converted_io_ratio.clone(),
        },
        input_token: TokenRef {
            address: input_token_info.address(),
            symbol: input_token_info.symbol().unwrap_or_default(),
            decimals: input_token_info.decimals(),
        },
        output_token: TokenRef {
            address: output_token_info.address(),
            symbol: output_token_info.symbol().unwrap_or_default(),
            decimals: output_token_info.decimals(),
        },
        input_vault_id: input.vault_id(),
        output_vault_id: output.vault_id(),
        input_vault_balance,
        output_vault_balance,
        io_ratio: converted_io_ratio,
        created_at,
        orderbook_id: order.raindex(),
        trades: trade_entries,
        rainlang: crate::routes::orders::order_source(order),
    })
}

fn map_trade(
    trade: &RaindexTrade,
    denomination: Denomination,
    wrap_ratios: &HashMap<Address, WrapRatioValue>,
) -> Result<OrderTradeEntry, ApiError> {
    let timestamp: u64 = trade.timestamp().try_into().unwrap_or(0);
    let tx = trade.transaction();
    let input_vc = trade.input_vault_balance_change();
    let output_vc = trade.output_vault_balance_change();
    let input_token = input_vc.token().address();
    let output_token = output_vc.token().address();
    let input_amount = if denomination == Denomination::Unwrapped {
        crate::denomination::convert_wrapped_amount_for_token(
            input_vc.formatted_amount(),
            input_token,
            wrap_ratios,
        )?
    } else {
        input_vc.formatted_amount()
    };
    let output_amount = if denomination == Denomination::Unwrapped {
        crate::denomination::convert_wrapped_amount_for_token(
            output_vc.formatted_amount(),
            output_token,
            wrap_ratios,
        )?
    } else {
        output_vc.formatted_amount()
    };

    Ok(OrderTradeEntry {
        chain_id: trade.chain_id(),
        id: trade.id().to_string(),
        tx_hash: tx.id(),
        input_amount,
        output_amount,
        timestamp,
        sender: tx.from(),
    })
}

async fn current_wrap_ratios_for_order_detail(
    ds: &dyn OrderDataSource,
    denomination: Denomination,
    order: &RaindexOrder,
    trades: &[RaindexTrade],
) -> Result<HashMap<Address, WrapRatioValue>, ApiError> {
    if denomination == Denomination::Wrapped {
        return Ok(HashMap::new());
    }

    let (input, output) = crate::routes::resolve_io_vaults(order)?;
    let mut token_addresses = vec![input.token().address(), output.token().address()];
    for trade in trades {
        token_addresses.push(trade.input_vault_balance_change().token().address());
        token_addresses.push(trade.output_vault_balance_change().token().address());
    }
    token_addresses.sort_unstable();
    token_addresses.dedup();

    ds.get_wrap_ratios_for_tokens_on_chain(order.chain_id(), &token_addresses)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ApiError;
    use crate::routes::order::test_fixtures::*;
    use crate::test_helpers::TestClientBuilder;
    use crate::wrap_ratio::WrapRatioValue;
    use alloy::primitives::address;
    use alloy::primitives::{Address, Bytes};
    use rocket::http::Status;
    use std::collections::HashMap;

    #[rocket::async_test]
    async fn test_process_get_order_success() {
        let ds = MockOrderDataSource {
            orders: Ok(vec![mock_order()]),
            trades: Ok(vec![mock_trade()]),
            quotes: Ok(vec![mock_quote("1.5")]),
            calldata: Ok(Bytes::new()),
        };
        let detail = process_get_order(&ds, test_hash(), Denomination::Wrapped)
            .await
            .unwrap();

        assert_eq!(detail.order_hash, test_hash());
        assert_eq!(
            detail.owner,
            "0x0000000000000000000000000000000000000001"
                .parse::<Address>()
                .unwrap()
        );
        assert_eq!(detail.input_token.symbol, "USDC");
        assert_eq!(detail.output_token.symbol, "WETH");
        assert_eq!(detail.input_vault_balance, "1.000000");
        assert_eq!(detail.output_vault_balance, "0.500000000000000000");
        assert_eq!(detail.io_ratio, "1.5");
        assert_eq!(detail.order_details.type_, OrderType::Solver);
        assert_eq!(detail.order_details.io_ratio, "1.5");
        assert_eq!(detail.created_at, 1700000000);
        assert_eq!(detail.trades.len(), 1);
        assert_eq!(detail.trades[0].input_amount, "0.500000");
        assert_eq!(detail.trades[0].output_amount, "-0.250000000000000000");
        assert_eq!(detail.trades[0].timestamp, 1700001000);
    }

    #[rocket::async_test]
    async fn test_process_get_order_includes_rainlang() {
        let ds = MockOrderDataSource {
            orders: Ok(vec![mock_order_with_rainlang(RIVER_PEG_RAINLANG)]),
            trades: Ok(vec![]),
            quotes: Ok(vec![mock_quote("1.5")]),
            calldata: Ok(Bytes::new()),
        };
        let detail = process_get_order(&ds, test_hash(), Denomination::Wrapped)
            .await
            .unwrap();

        assert_eq!(detail.rainlang.as_deref(), Some(RIVER_PEG_RAINLANG));
        let json = serde_json::to_value(&detail).unwrap();
        assert_eq!(json["rainlang"], RIVER_PEG_RAINLANG);
        // Existing fields unchanged.
        assert_eq!(json["ioRatio"], "1.5");
        assert_eq!(json["inputVaultBalance"], "1.000000");
    }

    #[rocket::async_test]
    async fn test_process_get_order_without_rainlang_omits_field() {
        let ds = MockOrderDataSource {
            orders: Ok(vec![mock_order()]),
            trades: Ok(vec![]),
            quotes: Ok(vec![mock_quote("1.5")]),
            calldata: Ok(Bytes::new()),
        };
        let detail = process_get_order(&ds, test_hash(), Denomination::Wrapped)
            .await
            .unwrap();

        assert_eq!(detail.rainlang, None);
        let json = serde_json::to_value(&detail).unwrap();
        assert!(json.get("rainlang").is_none());
    }

    #[rocket::async_test]
    async fn test_process_get_order_not_found() {
        let ds = MockOrderDataSource {
            orders: Ok(vec![]),
            trades: Ok(vec![]),
            quotes: Ok(vec![]),
            calldata: Ok(Bytes::new()),
        };
        let result = process_get_order(&ds, test_hash(), Denomination::Wrapped).await;
        assert!(matches!(result, Err(ApiError::NotFound(_))));
    }

    #[rocket::async_test]
    async fn test_process_get_order_empty_trades() {
        let ds = MockOrderDataSource {
            orders: Ok(vec![mock_order()]),
            trades: Ok(vec![]),
            quotes: Ok(vec![mock_quote("2.0")]),
            calldata: Ok(Bytes::new()),
        };
        let detail = process_get_order(&ds, test_hash(), Denomination::Wrapped)
            .await
            .unwrap();
        assert!(detail.trades.is_empty());
        assert_eq!(detail.io_ratio, "2.0");
    }

    #[rocket::async_test]
    async fn test_process_get_order_failed_quote() {
        let ds = MockOrderDataSource {
            orders: Ok(vec![mock_order()]),
            trades: Ok(vec![]),
            quotes: Ok(vec![mock_failed_quote()]),
            calldata: Ok(Bytes::new()),
        };
        let detail = process_get_order(&ds, test_hash(), Denomination::Wrapped)
            .await
            .unwrap();
        assert_eq!(detail.io_ratio, "-");
        assert_eq!(detail.order_details.io_ratio, "-");
    }

    #[rocket::async_test]
    async fn test_process_get_order_query_failure() {
        let ds = MockOrderDataSource {
            orders: Err(ApiError::Internal("failed to query orders".into())),
            trades: Ok(vec![]),
            quotes: Ok(vec![]),
            calldata: Ok(Bytes::new()),
        };
        let result = process_get_order(&ds, test_hash(), Denomination::Wrapped).await;
        assert!(matches!(result, Err(ApiError::Internal(_))));
    }

    #[rocket::async_test]
    async fn test_process_get_order_quotes_failure() {
        let ds = MockOrderDataSource {
            orders: Ok(vec![mock_order()]),
            trades: Ok(vec![]),
            quotes: Err(ApiError::Internal("failed to query order quotes".into())),
            calldata: Ok(Bytes::new()),
        };
        let result = process_get_order(&ds, test_hash(), Denomination::Wrapped).await;
        assert!(matches!(result, Err(ApiError::Internal(_))));
    }

    #[rocket::async_test]
    async fn test_process_get_order_trades_failure() {
        let ds = MockOrderDataSource {
            orders: Ok(vec![mock_order()]),
            trades: Err(ApiError::Internal("failed to query order trades".into())),
            quotes: Ok(vec![mock_quote("1.5")]),
            calldata: Ok(Bytes::new()),
        };
        let result = process_get_order(&ds, test_hash(), Denomination::Wrapped).await;
        assert!(matches!(result, Err(ApiError::Internal(_))));
    }

    #[rocket::async_test]
    async fn test_process_get_order_shared_vaults() {
        let ds = MockOrderDataSource {
            orders: Ok(vec![mock_order_with_shared_vaults()]),
            trades: Ok(vec![]),
            quotes: Ok(vec![mock_quote("200.0")]),
            calldata: Ok(Bytes::new()),
        };
        let hash = "0x000000000000000000000000000000000000000000000000000000000000beef"
            .parse()
            .unwrap();
        let detail = process_get_order(&ds, hash, Denomination::Wrapped)
            .await
            .unwrap();

        assert_eq!(detail.input_token.symbol, "wtMSTR");
        assert_eq!(detail.output_token.symbol, "wtMSTR");
        assert_eq!(detail.input_vault_balance, "0");
        assert_eq!(detail.output_vault_balance, "0");
    }

    #[test]
    fn test_map_trade_converts_unwrapped_amounts() {
        let wrapped_output = address!("ff05e1bd696900dc6a52ca35ca61bb1024eda8e2");
        let mut value = trade_json();
        value["outputVaultBalanceChange"]["token"]["address"] =
            serde_json::json!(format!("{wrapped_output:#x}"));
        value["outputVaultBalanceChange"]["token"]["id"] =
            serde_json::json!(format!("{wrapped_output:#x}"));
        value["outputVaultBalanceChange"]["token"]["symbol"] = serde_json::json!("wtMSTR");
        let trade: RaindexTrade =
            serde_json::from_value(value).expect("deserialize wrapped-output trade");
        let ratios = HashMap::from([(
            wrapped_output,
            WrapRatioValue {
                share_address: wrapped_output,
                assets_per_share: "2".to_string(),
            },
        )]);

        let entry = map_trade(&trade, Denomination::Unwrapped, &ratios).expect("map trade");

        assert_eq!(entry.input_amount, "0.500000");
        assert_eq!(entry.output_amount, "-0.5");
    }

    #[rocket::async_test]
    async fn test_determine_order_type_solver_default() {
        let order = mock_order();
        assert_eq!(determine_order_type(&order), OrderType::Solver);
    }

    #[rocket::async_test]
    async fn test_get_order_401_without_auth() {
        let client = TestClientBuilder::new().build().await;
        let response = client
            .get("/v1/order/0x000000000000000000000000000000000000000000000000000000000000abcd")
            .dispatch()
            .await;
        assert_eq!(response.status(), Status::Unauthorized);
    }
}

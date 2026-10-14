mod get_by_owner;
mod get_by_token;
mod get_by_tx;
mod query;

use crate::cache::RouteResponseCaches;
use crate::error::ApiError;
use crate::routes::{raindex_backed_tokens, resolve_raindex_chain_ids};
use crate::types::common::{Denomination, TokenRef};
use crate::types::orders::{
    OrderState, OrderSummary, OrderSummaryOrderType, OrdersListResponse, OrdersPagination,
};
use crate::wrap_ratio::{
    persist_wrap_ratio_snapshots_best_effort, read_wrap_ratio_responses_for_addresses,
    wrap_ratio_values_from_responses, WrapRatioValue,
};
use alloy::primitives::Address;
use async_trait::async_trait;
use futures::{stream, StreamExt};
use rain_orderbook_common::raindex_client::order_quotes::{
    get_order_quotes_batch as fetch_order_quotes_batch, RaindexOrderQuote,
};
use rain_orderbook_common::raindex_client::orders::{GetOrdersFilters, RaindexOrder};
use rain_orderbook_common::raindex_client::types::ChainIds;
use rain_orderbook_common::raindex_client::RaindexClient;
use rocket::Route;
use std::collections::BTreeMap;
use std::collections::HashMap;

pub(crate) const DEFAULT_PAGE_SIZE: u32 = 20;
pub(crate) const MAX_PAGE_SIZE: u16 = 50;
const MAX_CHAIN_BATCH_CONCURRENCY: usize = 4;
const MAX_INDIVIDUAL_QUOTE_CONCURRENCY: usize = 8;

type OrderQuoteResult = Result<Vec<RaindexOrderQuote>, ApiError>;
type OrderQuoteBatchResult = Result<Vec<Vec<RaindexOrderQuote>>, ApiError>;
type IndexedOrder = (usize, RaindexOrder);
type GroupedOrders = BTreeMap<u32, Vec<IndexedOrder>>;
pub(crate) type OrderWrapRatioMap = HashMap<(u32, Address), WrapRatioValue>;

enum OrderQuoteCacheResult {
    ApiError(ApiError),
    NotCacheable(Vec<RaindexOrderQuote>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OrderQuoteSummary {
    pub io_ratio: String,
    pub max_output: Option<String>,
}

#[async_trait]
pub(crate) trait OrdersListDataSource: Send + Sync {
    async fn get_orders_list(
        &self,
        chain_ids: Option<Vec<u32>>,
        filters: GetOrdersFilters,
        page: Option<u16>,
        page_size: Option<u16>,
    ) -> Result<(Vec<RaindexOrder>, u32), ApiError>;

    async fn get_order_quotes(
        &self,
        order: &RaindexOrder,
    ) -> Result<Vec<RaindexOrderQuote>, ApiError>;

    async fn get_order_quotes_batch_for_chain(
        &self,
        _orders: &[RaindexOrder],
    ) -> OrderQuoteBatchResult {
        Err(ApiError::Internal(
            "batched order quote fetch unavailable".into(),
        ))
    }

    async fn get_order_quotes_batch(&self, orders: &[RaindexOrder]) -> Vec<OrderQuoteResult> {
        fetch_order_quotes_grouped(self, orders).await
    }

    async fn get_wrap_ratios_for_tokens(
        &self,
        _token_addresses: &[Address],
    ) -> Result<HashMap<Address, WrapRatioValue>, ApiError> {
        Ok(HashMap::new())
    }

    async fn get_wrap_ratios_for_tokens_on_chain(
        &self,
        _chain_id: u32,
        token_addresses: &[Address],
    ) -> Result<HashMap<Address, WrapRatioValue>, ApiError> {
        self.get_wrap_ratios_for_tokens(token_addresses).await
    }
}

#[async_trait]
pub(crate) trait BatchOrdersDataSource: OrdersListDataSource {
    async fn get_orders_query(
        &self,
        chain_id: u32,
        filters: GetOrdersFilters,
        page: u16,
        page_size: u16,
    ) -> Result<(Vec<RaindexOrder>, u32), ApiError>;
}

pub(crate) struct RaindexOrdersListDataSource<'a> {
    pub client: &'a RaindexClient,
    pub caches: &'a RouteResponseCaches,
    pub pool: &'a crate::db::DbPool,
}

pub(crate) fn order_quote_cache_key(order: &RaindexOrder) -> String {
    format!(
        "order-quotes/latest/default/{}/{}/{}",
        order.chain_id(),
        order.raindex(),
        order.order_hash()
    )
}

fn quote_set_is_complete(quotes: &[RaindexOrderQuote]) -> bool {
    !quotes.is_empty()
        && quotes
            .iter()
            .all(|quote| quote.success && quote.error.is_none())
}

pub(crate) fn active_filter_for_state(state: Option<OrderState>) -> Option<bool> {
    match state.unwrap_or(OrderState::Active) {
        OrderState::Active => Some(true),
        OrderState::Inactive => Some(false),
        OrderState::All => None,
    }
}

/// The order's Rainlang from its onchain meta, falling back to the dotrain source.
pub(crate) fn order_source(order: &RaindexOrder) -> Option<String> {
    order.rainlang().or_else(|| order.dotrain_source())
}

pub(crate) fn classify_order_type(order: &RaindexOrder) -> OrderSummaryOrderType {
    let source = order_source(order);
    let Some(source) = source else {
        return OrderSummaryOrderType::Custom;
    };

    if source.contains("other-vwaio") {
        return OrderSummaryOrderType::DynamicSpread;
    }

    // The River's strategies, by fingerprints unique to each Rainlang. Checked
    // before DCA: river-ladder uses linear-growth and was labelled DCA (2 Oct).
    if source.contains("Stop not triggered") {
        return OrderSummaryOrderType::Stop;
    }
    if source.contains("Stop-buy not triggered") {
        return OrderSummaryOrderType::StopBuy;
    }
    if source.contains("Trade causes dust") {
        return OrderSummaryOrderType::Ladder;
    }
    if is_cycle_source(&source) {
        return OrderSummaryOrderType::Cycle;
    }
    // river-spread: `ask-per-share: max(mul(price ...)) min-sell` (its bid line
    // is peg-like, so it is checked first).
    if source.lines().any(|line| {
        let compact: String = line.chars().filter(|c| !c.is_whitespace()).collect();
        compact.starts_with("ask-per-share:max(mul(price")
    }) {
        return OrderSummaryOrderType::Spread;
    }
    if source.lines().any(|line| {
        let compact: String = line.chars().filter(|c| !c.is_whitespace()).collect();
        compact.starts_with("per-share:min(mul(price")
    }) {
        return OrderSummaryOrderType::Peg;
    }
    if source
        .lines()
        .any(|line| line.trim_start().starts_with("rebalance-usd:"))
    {
        return OrderSummaryOrderType::Basket;
    }

    let handle_io = handle_io_section(&source);
    let has_dca_markers = handle_io.as_deref().is_some_and(|section| {
        section.contains("min-amount:")
            || section.contains("linear-growth")
            || section.contains("amount-epochs")
            || section.contains("halflife")
    });
    if has_dca_markers {
        return OrderSummaryOrderType::Dca;
    }

    if handle_io.as_deref().is_some_and(is_empty_handle_io_section) {
        return OrderSummaryOrderType::Limit;
    }

    OrderSummaryOrderType::Custom
}

/// river-cycle: `max-output: if(equal-to(get(<32-byte key>) <phase>) ...`.
fn is_cycle_source(source: &str) -> bool {
    source.lines().any(|line| {
        let compact: String = line.chars().filter(|c| !c.is_whitespace()).collect();
        compact
            .find("max-output:if(equal-to(get(0x")
            .map(|i| {
                let rest = &compact[i + "max-output:if(equal-to(get(0x".len()..];
                rest.len() > 64
                    && rest[..64].chars().all(|c| c.is_ascii_hexdigit())
                    && rest[64..].starts_with(')')
            })
            .unwrap_or(false)
    })
}

fn handle_io_section(source: &str) -> Option<String> {
    let mut section = Vec::new();
    let mut in_handle_io = false;

    for line in source.lines() {
        let trimmed = line.trim();
        let is_section_header =
            trimmed.starts_with("/*") || trimmed.starts_with('#') || trimmed.starts_with("//");

        if in_handle_io && is_section_header && !trimmed.contains("handle-io") {
            break;
        }

        if in_handle_io {
            section.push(line);
            continue;
        }

        if trimmed.contains("handle-io") {
            in_handle_io = true;
            let after_marker = trimmed
                .split_once("*/")
                .map(|(_, after)| after.trim())
                .filter(|after| !after.is_empty());
            if let Some(after_marker) = after_marker {
                section.push(after_marker);
            }
        }
    }

    in_handle_io.then(|| section.join("\n"))
}

fn is_empty_handle_io_section(section: &str) -> bool {
    matches!(section.trim(), ":" | ":;")
}

fn group_orders_by_chain(orders: &[RaindexOrder]) -> GroupedOrders {
    let mut grouped_orders: GroupedOrders = BTreeMap::new();
    for (index, order) in orders.iter().cloned().enumerate() {
        grouped_orders
            .entry(order.chain_id())
            .or_default()
            .push((index, order));
    }
    grouped_orders
}

async fn fetch_order_quotes_individually<T>(
    ds: &T,
    chain_id: u32,
    indexed_orders: Vec<IndexedOrder>,
) -> Vec<(usize, OrderQuoteResult)>
where
    T: OrdersListDataSource + ?Sized,
{
    tracing::info!(
        chain_id,
        order_count = indexed_orders.len(),
        "falling back to per-order quotes"
    );

    stream::iter(indexed_orders.into_iter().map(|(index, order)| async move {
        let result = ds.get_order_quotes(&order).await;
        (index, result)
    }))
    .buffer_unordered(MAX_INDIVIDUAL_QUOTE_CONCURRENCY)
    .collect()
    .await
}

async fn fetch_quotes_for_chain_group<T>(
    ds: &T,
    chain_id: u32,
    indexed_orders: Vec<IndexedOrder>,
) -> Vec<(usize, OrderQuoteResult)>
where
    T: OrdersListDataSource + ?Sized,
{
    let group_orders: Vec<RaindexOrder> = indexed_orders
        .iter()
        .map(|(_, order)| order.clone())
        .collect();

    match ds.get_order_quotes_batch_for_chain(&group_orders).await {
        Ok(group_quotes) if group_quotes.len() == group_orders.len() => {
            tracing::info!(
                chain_id,
                order_count = group_orders.len(),
                "queried order quotes in batch"
            );
            indexed_orders
                .into_iter()
                .zip(group_quotes)
                .map(|((index, _), quotes)| (index, Ok(quotes)))
                .collect()
        }
        Ok(group_quotes) => {
            tracing::warn!(
                chain_id,
                order_count = group_orders.len(),
                received_quote_sets = group_quotes.len(),
                "batch quote fetch returned unexpected result count; falling back"
            );
            fetch_order_quotes_individually(ds, chain_id, indexed_orders).await
        }
        Err(error) => {
            tracing::warn!(
                chain_id,
                order_count = group_orders.len(),
                error = ?error,
                "batch quote fetch failed; falling back"
            );
            fetch_order_quotes_individually(ds, chain_id, indexed_orders).await
        }
    }
}

async fn fetch_order_quotes_grouped<T>(ds: &T, orders: &[RaindexOrder]) -> Vec<OrderQuoteResult>
where
    T: OrdersListDataSource + ?Sized,
{
    if orders.is_empty() {
        return vec![];
    }

    let grouped_orders = group_orders_by_chain(orders);
    let grouped_results = stream::iter(grouped_orders.into_iter().map(
        |(chain_id, indexed_orders)| async move {
            fetch_quotes_for_chain_group(ds, chain_id, indexed_orders).await
        },
    ))
    .buffer_unordered(MAX_CHAIN_BATCH_CONCURRENCY)
    .collect::<Vec<_>>()
    .await;

    let mut ordered_results = Vec::with_capacity(orders.len());
    ordered_results.resize_with(orders.len(), || None);

    for group_results in grouped_results {
        for (index, quotes_result) in group_results {
            ordered_results[index] = Some(quotes_result);
        }
    }

    ordered_results
        .into_iter()
        .map(|entry| {
            entry.unwrap_or_else(|| Err(ApiError::Internal("failed to query order quotes".into())))
        })
        .collect()
}

#[async_trait]
impl<'a> OrdersListDataSource for RaindexOrdersListDataSource<'a> {
    async fn get_orders_list(
        &self,
        chain_ids: Option<Vec<u32>>,
        filters: GetOrdersFilters,
        page: Option<u16>,
        page_size: Option<u16>,
    ) -> Result<(Vec<RaindexOrder>, u32), ApiError> {
        let chain_ids = resolve_raindex_chain_ids(self.client, chain_ids)?;
        let result = self
            .client
            .get_orders(Some(ChainIds(chain_ids)), Some(filters), page, page_size)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "failed to query orders");
                ApiError::Internal("failed to query orders".into())
            })?;
        Ok((result.orders().to_vec(), result.total_count()))
    }

    async fn get_order_quotes(
        &self,
        order: &RaindexOrder,
    ) -> Result<Vec<RaindexOrderQuote>, ApiError> {
        let fetch = || async {
            order.get_quotes(None, None).await.map_err(|e| {
                tracing::error!(error = %e, "failed to query order quotes");
                ApiError::Internal("failed to query order quotes".into())
            })
        };

        if !self.caches.is_enabled() {
            return fetch().await;
        }

        match self
            .caches
            .order_quotes
            .get_or_try_insert(order_quote_cache_key(order), || async {
                let quotes = fetch().await.map_err(OrderQuoteCacheResult::ApiError)?;
                if quote_set_is_complete(&quotes) {
                    Ok(quotes)
                } else {
                    Err(OrderQuoteCacheResult::NotCacheable(quotes))
                }
            })
            .await
        {
            Ok(quotes) => Ok(quotes),
            Err(error) => match error.as_ref() {
                OrderQuoteCacheResult::ApiError(error) => Err(error.clone()),
                OrderQuoteCacheResult::NotCacheable(quotes) => Ok(quotes.clone()),
            },
        }
    }

    async fn get_order_quotes_batch_for_chain(
        &self,
        orders: &[RaindexOrder],
    ) -> OrderQuoteBatchResult {
        if !self.caches.is_enabled() {
            return fetch_order_quotes_batch(orders, None, None)
                .await
                .map_err(|error| {
                    let chain_id = orders
                        .first()
                        .map(RaindexOrder::chain_id)
                        .unwrap_or_default();
                    tracing::error!(
                        chain_id,
                        error = %error,
                        "failed to batch query order quotes"
                    );
                    ApiError::Internal("failed to query order quotes".into())
                });
        }

        let mut ordered_quotes: Vec<Option<Vec<RaindexOrderQuote>>> =
            Vec::with_capacity(orders.len());
        ordered_quotes.resize_with(orders.len(), || None);
        let mut missed_orders = Vec::new();
        let mut missed_keys = Vec::new();

        for (index, order) in orders.iter().enumerate() {
            let key = order_quote_cache_key(order);
            if let Some(quotes) = self.caches.order_quotes.get(&key).await {
                ordered_quotes[index] = Some(quotes);
            } else {
                missed_orders.push(order.clone());
                missed_keys.push((index, key));
            }
        }

        if missed_orders.is_empty() {
            return Ok(ordered_quotes
                .into_iter()
                .map(|quotes| quotes.unwrap_or_default())
                .collect());
        }

        let chain_id = orders
            .first()
            .map(RaindexOrder::chain_id)
            .unwrap_or_default();
        let missed_quotes = fetch_order_quotes_batch(&missed_orders, None, None)
            .await
            .map_err(|error| {
                tracing::error!(
                    chain_id,
                    error = %error,
                    "failed to batch query order quotes"
                );
                ApiError::Internal("failed to query order quotes".into())
            })?;

        if missed_quotes.len() != missed_keys.len() {
            tracing::error!(
                expected_results = missed_keys.len(),
                actual_results = missed_quotes.len(),
                "order quote cache miss batch returned unexpected result count"
            );
            return Err(ApiError::Internal("failed to query order quotes".into()));
        }

        for ((index, key), quotes) in missed_keys.into_iter().zip(missed_quotes) {
            if quote_set_is_complete(&quotes) {
                self.caches.order_quotes.insert(key, quotes.clone()).await;
            } else {
                tracing::warn!(chain_id, "incomplete order quote result will not be cached");
            }
            ordered_quotes[index] = Some(quotes);
        }

        Ok(ordered_quotes
            .into_iter()
            .map(|quotes| quotes.unwrap_or_default())
            .collect())
    }

    async fn get_wrap_ratios_for_tokens(
        &self,
        token_addresses: &[Address],
    ) -> Result<HashMap<Address, WrapRatioValue>, ApiError> {
        let tokens = raindex_backed_tokens(self.client)?;

        let responses = read_wrap_ratio_responses_for_addresses(&tokens, token_addresses).await?;
        persist_wrap_ratio_snapshots_best_effort(self.pool, &responses).await;
        Ok(wrap_ratio_values_from_responses(responses))
    }

    async fn get_wrap_ratios_for_tokens_on_chain(
        &self,
        chain_id: u32,
        token_addresses: &[Address],
    ) -> Result<HashMap<Address, WrapRatioValue>, ApiError> {
        let tokens: Vec<_> = self
            .client
            .get_all_tokens()
            .map_err(|error| {
                tracing::error!(%error, "failed to retrieve curated tokens");
                ApiError::Internal("failed to retrieve curated tokens".into())
            })?
            .into_values()
            .filter(|token| token.network.chain_id == chain_id)
            .collect();

        let responses = read_wrap_ratio_responses_for_addresses(&tokens, token_addresses).await?;
        persist_wrap_ratio_snapshots_best_effort(self.pool, &responses).await;
        Ok(wrap_ratio_values_from_responses(responses))
    }
}

#[async_trait]
impl BatchOrdersDataSource for RaindexOrdersListDataSource<'_> {
    async fn get_orders_query(
        &self,
        chain_id: u32,
        filters: GetOrdersFilters,
        page: u16,
        page_size: u16,
    ) -> Result<(Vec<RaindexOrder>, u32), ApiError> {
        let result = self
            .client
            .get_orders(
                Some(ChainIds(vec![chain_id])),
                Some(filters),
                Some(page),
                Some(page_size),
            )
            .await
            .map_err(|error| {
                tracing::error!(chain_id, %error, "failed to batch query orders");
                ApiError::coded(
                    crate::error::ApiErrorCode::OrdersQueryFailed,
                    "the order source could not serve this request",
                )
            })?;
        Ok((result.orders().to_vec(), result.total_count()))
    }
}

pub(crate) fn build_order_summary(
    order: &RaindexOrder,
    io_ratio: &str,
    max_output: Option<String>,
    denomination: Denomination,
    wrap_ratios: &HashMap<Address, WrapRatioValue>,
) -> Result<OrderSummary, ApiError> {
    let (input, output) = super::resolve_io_vaults(order)?;

    let input_token_info = input.token();
    let output_token_info = output.token();
    let created_at: u64 = order.timestamp_added().try_into().unwrap_or(0);

    let output_vault_balance = if denomination == Denomination::Unwrapped {
        crate::denomination::convert_wrapped_amount_for_token(
            output.formatted_balance(),
            output_token_info.address(),
            wrap_ratios,
        )?
    } else {
        output.formatted_balance()
    };
    let io_ratio = if denomination == Denomination::Unwrapped {
        crate::denomination::convert_wrapped_io_ratio(
            io_ratio.to_string(),
            input_token_info.address(),
            output_token_info.address(),
            wrap_ratios,
        )?
    } else {
        io_ratio.to_string()
    };
    let max_output = match (denomination, max_output) {
        (Denomination::Unwrapped, Some(max_output)) => {
            Some(crate::denomination::convert_wrapped_amount_for_token(
                max_output,
                output_token_info.address(),
                wrap_ratios,
            )?)
        }
        (_, max_output) => max_output,
    };

    Ok(OrderSummary {
        order_hash: order.order_hash(),
        owner: order.owner(),
        chain_id: order.chain_id(),
        order_bytes: order.order_bytes(),
        active: order.active(),
        removed_at: order
            .timestamp_removed()
            .and_then(|timestamp| timestamp.try_into().ok()),
        order_type: classify_order_type(order),
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
        output_vault_balance: if order.active() {
            output_vault_balance
        } else {
            "0".into()
        },
        max_output,
        io_ratio,
        created_at,
        orderbook_id: order.raindex(),
        rainlang: None,
    })
}

pub(crate) fn quote_result_to_summary(
    order: &RaindexOrder,
    quotes_result: OrderQuoteResult,
) -> OrderQuoteSummary {
    match quotes_result {
        Ok(quotes) => {
            let quote_data = quotes.first().and_then(|quote| quote.data.as_ref());
            OrderQuoteSummary {
                io_ratio: quote_data
                    .map(|quote| quote.formatted_ratio.clone())
                    .unwrap_or_else(|| "-".into()),
                max_output: quote_data.map(|quote| quote.formatted_max_output.clone()),
            }
        }
        Err(err) => {
            tracing::warn!(
                order_hash = ?order.order_hash(),
                error = ?err,
                "quote fetch failed; using fallback io_ratio and null max_output"
            );
            OrderQuoteSummary {
                io_ratio: "-".into(),
                max_output: None,
            }
        }
    }
}

pub(crate) async fn get_order_quotes_for_summaries(
    ds: &dyn OrdersListDataSource,
    orders: &[RaindexOrder],
) -> Vec<OrderQuoteResult> {
    let mut quote_results: Vec<Option<OrderQuoteResult>> =
        (0..orders.len()).map(|_| None).collect();
    let active_orders: Vec<(usize, RaindexOrder)> = orders
        .iter()
        .enumerate()
        .filter_map(|(index, order)| {
            if order.active() {
                Some((index, order.clone()))
            } else {
                quote_results[index] = Some(Ok(Vec::new()));
                None
            }
        })
        .collect();

    if !active_orders.is_empty() {
        let active_order_values: Vec<RaindexOrder> = active_orders
            .iter()
            .map(|(_, order)| order.clone())
            .collect();
        let active_quote_results = ds.get_order_quotes_batch(&active_order_values).await;
        for ((index, _), quote_result) in active_orders.into_iter().zip(active_quote_results) {
            quote_results[index] = Some(quote_result);
        }
    }

    quote_results
        .into_iter()
        .map(|quote_result| quote_result.unwrap_or_else(|| Ok(Vec::new())))
        .collect()
}

pub(crate) fn build_pagination(total_count: u32, page: u32, page_size: u32) -> OrdersPagination {
    let total_orders = total_count as u64;
    let total_pages = if page_size == 0 {
        0
    } else {
        total_orders.div_ceil(page_size as u64)
    };
    OrdersPagination {
        page,
        page_size,
        total_orders,
        total_pages,
        has_more: (page as u64) < total_pages,
    }
}

pub(crate) fn build_orders_list_response(
    orders: &[RaindexOrder],
    total_count: u32,
    page: u32,
    page_size: u32,
    quote_results: Vec<OrderQuoteResult>,
    denomination: Denomination,
    wrap_ratios: &OrderWrapRatioMap,
) -> Result<OrdersListResponse, ApiError> {
    if quote_results.len() != orders.len() {
        tracing::error!(
            expected_results = orders.len(),
            actual_results = quote_results.len(),
            "order quote results length mismatch"
        );
        return Err(ApiError::Internal("failed to query order quotes".into()));
    }

    let mut summaries = Vec::with_capacity(orders.len());
    for (order, quotes_result) in orders.iter().zip(quote_results) {
        let quote_summary = quote_result_to_summary(order, quotes_result);
        let order_wrap_ratios = wrap_ratios
            .iter()
            .filter_map(|((chain_id, address), ratio)| {
                (*chain_id == order.chain_id()).then_some((*address, ratio.clone()))
            })
            .collect();
        summaries.push(build_order_summary(
            order,
            &quote_summary.io_ratio,
            quote_summary.max_output,
            denomination,
            &order_wrap_ratios,
        )?);
    }

    Ok(OrdersListResponse {
        orders: summaries,
        pagination: build_pagination(total_count, page, page_size),
    })
}

pub(crate) async fn current_wrap_ratios_for_orders(
    ds: &dyn OrdersListDataSource,
    denomination: Denomination,
    orders: &[RaindexOrder],
) -> Result<OrderWrapRatioMap, ApiError> {
    if denomination == Denomination::Wrapped || orders.is_empty() {
        return Ok(HashMap::new());
    }

    let mut token_addresses_by_chain: BTreeMap<u32, Vec<Address>> = BTreeMap::new();
    for order in orders {
        let (input, output) = super::resolve_io_vaults(order)?;
        let token_addresses = token_addresses_by_chain
            .entry(order.chain_id())
            .or_default();
        token_addresses.push(input.token().address());
        token_addresses.push(output.token().address());
    }
    let mut ratios = HashMap::new();
    for (chain_id, mut token_addresses) in token_addresses_by_chain {
        token_addresses.sort_unstable();
        token_addresses.dedup();
        ratios.extend(
            ds.get_wrap_ratios_for_tokens_on_chain(chain_id, &token_addresses)
                .await?
                .into_iter()
                .map(|(address, ratio)| ((chain_id, address), ratio)),
        );
    }
    Ok(ratios)
}

pub use get_by_owner::*;
pub use get_by_token::*;
pub use get_by_tx::*;
pub use query::*;

pub fn routes() -> Vec<Route> {
    rocket::routes![
        get_by_tx::get_orders_by_tx,
        get_by_owner::get_orders_by_address,
        get_by_token::get_orders_by_token,
        query::post_orders_query
    ]
}

pub fn routes_v2() -> Vec<Route> {
    rocket::routes![
        get_by_owner::get_orders_by_address,
        get_by_token::get_orders_by_token,
        query::post_orders_query
    ]
}

#[cfg(test)]
pub(crate) mod test_fixtures {
    use super::OrdersListDataSource;
    use crate::error::ApiError;
    use async_trait::async_trait;
    use rain_orderbook_common::raindex_client::order_quotes::RaindexOrderQuote;
    use rain_orderbook_common::raindex_client::orders::{GetOrdersFilters, RaindexOrder};
    use std::sync::Mutex;

    pub struct MockOrdersListDataSource {
        pub orders: Result<Vec<RaindexOrder>, ApiError>,
        pub total_count: u32,
        pub quotes: Result<Vec<RaindexOrderQuote>, ApiError>,
    }

    #[derive(Default)]
    pub struct RecordingOrdersListDataSource {
        pub filters: Mutex<Vec<GetOrdersFilters>>,
    }

    #[async_trait]
    impl OrdersListDataSource for MockOrdersListDataSource {
        async fn get_orders_list(
            &self,
            _chain_ids: Option<Vec<u32>>,
            _filters: GetOrdersFilters,
            _page: Option<u16>,
            _page_size: Option<u16>,
        ) -> Result<(Vec<RaindexOrder>, u32), ApiError> {
            match &self.orders {
                Ok(orders) => Ok((orders.clone(), self.total_count)),
                Err(_) => Err(ApiError::Internal("failed to query orders".into())),
            }
        }

        async fn get_order_quotes(
            &self,
            _order: &RaindexOrder,
        ) -> Result<Vec<RaindexOrderQuote>, ApiError> {
            match &self.quotes {
                Ok(quotes) => Ok(quotes.clone()),
                Err(_) => Err(ApiError::Internal("failed to query order quotes".into())),
            }
        }
    }

    #[async_trait]
    impl OrdersListDataSource for RecordingOrdersListDataSource {
        async fn get_orders_list(
            &self,
            _chain_ids: Option<Vec<u32>>,
            filters: GetOrdersFilters,
            _page: Option<u16>,
            _page_size: Option<u16>,
        ) -> Result<(Vec<RaindexOrder>, u32), ApiError> {
            self.filters.lock().expect("lock filters").push(filters);
            Ok((vec![], 0))
        }

        async fn get_order_quotes(
            &self,
            _order: &RaindexOrder,
        ) -> Result<Vec<RaindexOrderQuote>, ApiError> {
            unreachable!("recording datasource returns no orders")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::order::test_fixtures::{mock_failed_quote, mock_quote, order_json};
    use crate::wrap_ratio::WrapRatioValue;
    use alloy::primitives::address;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    struct BatchingTestDataSource {
        per_order_quotes: HashMap<String, OrderQuoteResult>,
        batched_quotes: HashMap<u32, OrderQuoteBatchResult>,
        batch_calls: Arc<Mutex<Vec<(u32, usize)>>>,
        single_calls: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl OrdersListDataSource for BatchingTestDataSource {
        async fn get_orders_list(
            &self,
            _chain_ids: Option<Vec<u32>>,
            _filters: GetOrdersFilters,
            _page: Option<u16>,
            _page_size: Option<u16>,
        ) -> Result<(Vec<RaindexOrder>, u32), ApiError> {
            unreachable!("not used in batching tests")
        }

        async fn get_order_quotes(
            &self,
            order: &RaindexOrder,
        ) -> Result<Vec<RaindexOrderQuote>, ApiError> {
            let order_hash = format!("{:?}", order.order_hash());
            self.single_calls
                .lock()
                .expect("lock single calls")
                .push(order_hash.clone());
            self.per_order_quotes
                .get(&order_hash)
                .cloned()
                .expect("per-order quote should exist")
        }

        async fn get_order_quotes_batch_for_chain(
            &self,
            orders: &[RaindexOrder],
        ) -> OrderQuoteBatchResult {
            let chain_id = orders
                .first()
                .map(RaindexOrder::chain_id)
                .unwrap_or_default();
            self.batch_calls
                .lock()
                .expect("lock batch calls")
                .push((chain_id, orders.len()));
            self.batched_quotes
                .get(&chain_id)
                .cloned()
                .expect("batch quote result should exist")
        }
    }

    fn mock_order_for_chain(chain_id: u32, order_hash: &str) -> RaindexOrder {
        let mut value = order_json();
        value["chainId"] = json!(chain_id);
        value["orderHash"] = json!(order_hash);
        serde_json::from_value(value).expect("deserialize chain-specific mock order")
    }

    fn mock_order_with_source(source: Option<&str>) -> RaindexOrder {
        let mut value = order_json();
        value["rainlang"] = source.map_or(serde_json::Value::Null, |source| json!(source));
        serde_json::from_value(value).expect("deserialize mock order with source")
    }

    fn mock_inactive_order(order_hash: &str, removed_at: u64) -> RaindexOrder {
        let mut value = order_json();
        value["active"] = json!(false);
        value["orderHash"] = json!(order_hash);
        value["timestampRemoved"] = json!(format!("0x{removed_at:x}"));
        serde_json::from_value(value).expect("deserialize inactive mock order")
    }

    fn quote_ratio(result: &OrderQuoteResult) -> String {
        result
            .as_ref()
            .expect("quote result should succeed")
            .first()
            .and_then(|quote| quote.data.as_ref())
            .map(|quote| quote.formatted_ratio.clone())
            .expect("quote should contain ratio")
    }

    fn order_hash_key(order: &RaindexOrder) -> String {
        format!("{:?}", order.order_hash())
    }

    fn wrap_ratio(share_address: Address, assets_per_share: &str) -> WrapRatioValue {
        WrapRatioValue {
            share_address,
            assets_per_share: assets_per_share.to_string(),
        }
    }

    #[test]
    fn test_active_filter_for_state_defaults_to_active() {
        assert_eq!(active_filter_for_state(None), Some(true));
        assert_eq!(
            active_filter_for_state(Some(OrderState::Active)),
            Some(true)
        );
        assert_eq!(
            active_filter_for_state(Some(OrderState::Inactive)),
            Some(false)
        );
        assert_eq!(active_filter_for_state(Some(OrderState::All)), None);
    }

    #[test]
    fn test_classify_order_type_no_source_is_custom() {
        let order = mock_order_with_source(None);
        assert_eq!(classify_order_type(&order), OrderSummaryOrderType::Custom);
    }

    #[test]
    fn test_classify_order_type_dynamic_spread() {
        let order = mock_order_with_source(Some("using-words-from 0xabc\n_: other-vwaio();"));
        assert_eq!(
            classify_order_type(&order),
            OrderSummaryOrderType::DynamicSpread
        );
    }

    #[test]
    fn test_classify_order_type_river_strategies() {
        let stop = mock_order_with_source(Some(
            "/* 0. calculate-io */\nprice _: dia-price(\"SGOV\" 10800),\n:ensure(less-than-or-equal-to(price 50) \"Stop not triggered\"),\nmax-output: max-positive-value(),\nio: mul(price 1 0.98);\n\n/* 1. handle-io */\n:;",
        ));
        assert_eq!(classify_order_type(&stop), OrderSummaryOrderType::Stop);
        let peg = mock_order_with_source(Some(
            "/* 0. calculate-io */\nprice _: dia-price(\"NVDA\" 10800),\nshares-per-token: call<2>(),\nper-share: min(mul(price 0.997) 300),\nmax-output: max-positive-value(),\nio: inv(mul(per-share shares-per-token));\n\n/* 1. handle-io */\n:;",
        ));
        assert_eq!(classify_order_type(&peg), OrderSummaryOrderType::Peg);
        let basket = mock_order_with_source(Some(
            "/* 0. calculate-io */\nrebalance-usd: if(greater-than(v-out mul(v-in add(1 0.02))) gap-usd 0),\nmax-output: 0,\nio: 1;\n\n/* 1. handle-io */\n:;",
        ));
        assert_eq!(classify_order_type(&basket), OrderSummaryOrderType::Basket);
        let stop_buy = mock_order_with_source(Some(
            "/* 0. calculate-io */\nprice _: dia-price(\"NVDA\" 10800),\n:ensure(greater-than-or-equal-to(price 250) \"Stop-buy not triggered\"),\n:ensure(less-than-or-equal-to(price 260) \"Above max price\"),\nmax-output: max-positive-value(),\nio: div(1 mul(price 1 1.02));\n\n/* 1. handle-io */\n:;",
        ));
        assert_eq!(
            classify_order_type(&stop_buy),
            OrderSummaryOrderType::StopBuy
        );
        let spread = mock_order_with_source(Some(
            "/* 0. calculate-io */\nprice _: dia-price(0x01 10800),\nask-per-share: max(mul(price add(1 s)) 90),\nbid-per-share: min(mul(price sub(1 s)) 110),\nmax-output: max-positive-value(),\nio: 1;\n\n/* 1. handle-io */\n:;",
        ));
        assert_eq!(classify_order_type(&spread), OrderSummaryOrderType::Spread);
        assert_eq!(
            serde_json::to_string(&OrderSummaryOrderType::Spread).unwrap(),
            "\"spread\""
        );
        assert_eq!(
            serde_json::to_string(&OrderSummaryOrderType::StopBuy).unwrap(),
            "\"stop-buy\""
        );
        // Ladder uses linear-growth (a DCA marker) and must not be labelled DCA.
        let ladder = mock_order_with_source(Some(
            "/* 0. calculate-io */\ntranche-io-ratio: linear-growth(0.0111 0.0005 0),\n\n/* 1. handle-io */\n:ensure(any(is-zero(frac(x)) 1) \"Trade causes dust.\"),\n:;",
        ));
        assert_eq!(classify_order_type(&ladder), OrderSummaryOrderType::Ladder);
        let cycle = mock_order_with_source(Some(
            "/* 0. calculate-io */\nmax-output io: call<2>();\n\n/* 1. handle-io */\n:call<3>();\n\n/* 2. sell-calculate-io */\nmax-output: if(equal-to(get(0x091156eed579139da9eba7cace36f041b0d573886e8c646976e79e27e3b91b25) 1) 0.03333333 0),\nio: 91.8;",
        ));
        assert_eq!(classify_order_type(&cycle), OrderSummaryOrderType::Cycle);
    }

    #[test]
    fn test_classify_order_type_dca_from_handle_io_min_amount() {
        let order = mock_order_with_source(Some(
            r#"/* 0. calculate-io */
_: 1;

/* 1. handle-io */
min-amount: 1;
:;"#,
        ));
        assert_eq!(classify_order_type(&order), OrderSummaryOrderType::Dca);
    }

    #[test]
    fn test_classify_order_type_dca_from_source_markers() {
        for marker in ["linear-growth", "amount-epochs", "halflife"] {
            let order = mock_order_with_source(Some(&format!(
                r#"/* 0. calculate-io */
_: 1;

/* 1. handle-io */
_: {marker}();
:;"#
            )));
            assert_eq!(classify_order_type(&order), OrderSummaryOrderType::Dca);
        }
    }

    #[test]
    fn test_classify_order_type_ignores_dca_markers_outside_handle_io() {
        let order = mock_order_with_source(Some(
            r#"/* 0. calculate-io */
// linear-growth appears in an unrelated comment.
_: 1;

/* 1. handle-io */
:;"#,
        ));

        assert_eq!(classify_order_type(&order), OrderSummaryOrderType::Limit);
    }

    #[test]
    fn test_classify_order_type_limit_from_simple_handle_io() {
        for handle_io in [":;", ":"] {
            let order = mock_order_with_source(Some(&format!(
                r#"/* 0. calculate-io */
_: 1;

/* 1. handle-io */
{handle_io}"#
            )));
            assert_eq!(classify_order_type(&order), OrderSummaryOrderType::Limit);
        }
    }

    #[test]
    fn test_classify_order_type_custom_fallback() {
        let order = mock_order_with_source(Some(
            r#"/* 0. calculate-io */
_: 1;

/* 1. handle-io */
_: custom-handle-io();"#,
        ));
        assert_eq!(classify_order_type(&order), OrderSummaryOrderType::Custom);
    }

    #[rocket::async_test]
    async fn test_get_order_quotes_for_summaries_skips_inactive_orders() {
        let active_order = mock_order_for_chain(
            1,
            "0x0000000000000000000000000000000000000000000000000000000000000101",
        );
        let inactive_order = mock_inactive_order(
            "0x0000000000000000000000000000000000000000000000000000000000000102",
            1_718_452_900,
        );
        let batch_calls = Arc::new(Mutex::new(Vec::new()));
        let single_calls = Arc::new(Mutex::new(Vec::new()));
        let ds = BatchingTestDataSource {
            per_order_quotes: HashMap::new(),
            batched_quotes: HashMap::from([(1, Ok(vec![vec![mock_quote("1.7")]]))]),
            batch_calls: Arc::clone(&batch_calls),
            single_calls: Arc::clone(&single_calls),
        };

        let results =
            get_order_quotes_for_summaries(&ds, &[active_order.clone(), inactive_order]).await;

        assert_eq!(results.len(), 2);
        assert_eq!(quote_ratio(&results[0]), "1.7");
        assert!(results[1].as_ref().expect("inactive result").is_empty());
        assert_eq!(
            batch_calls.lock().expect("lock batch calls").as_slice(),
            &[(1, 1)]
        );
        assert!(single_calls.lock().expect("lock single calls").is_empty());
    }

    #[test]
    fn test_build_orders_list_response_uses_non_live_fields_for_inactive_order() {
        let order = mock_inactive_order(
            "0x0000000000000000000000000000000000000000000000000000000000000201",
            1_718_452_900,
        );
        let response = build_orders_list_response(
            &[order],
            1,
            1,
            20,
            vec![Ok(Vec::new())],
            Denomination::Wrapped,
            &HashMap::new(),
        )
        .expect("build inactive response");

        let summary = &response.orders[0];
        assert!(!summary.active);
        assert_eq!(summary.removed_at, Some(1_718_452_900));
        assert_eq!(summary.io_ratio, "-");
        assert_eq!(summary.max_output, None);
        assert_eq!(summary.output_vault_balance, "0");
        assert_eq!(summary.order_type, OrderSummaryOrderType::Custom);
        assert_eq!(summary.order_bytes.as_ref(), &[1]);
    }

    #[rocket::async_test]
    async fn test_fetch_order_quotes_grouped_preserves_input_order() {
        let orders = vec![
            mock_order_for_chain(
                1,
                "0x0000000000000000000000000000000000000000000000000000000000000001",
            ),
            mock_order_for_chain(
                10,
                "0x0000000000000000000000000000000000000000000000000000000000000002",
            ),
            mock_order_for_chain(
                1,
                "0x0000000000000000000000000000000000000000000000000000000000000003",
            ),
        ];
        let batch_calls = Arc::new(Mutex::new(Vec::new()));
        let single_calls = Arc::new(Mutex::new(Vec::new()));
        let ds = BatchingTestDataSource {
            per_order_quotes: HashMap::new(),
            batched_quotes: HashMap::from([
                (
                    1,
                    Ok(vec![vec![mock_quote("1.1")], vec![mock_quote("1.3")]]),
                ),
                (10, Ok(vec![vec![mock_quote("1.2")]])),
            ]),
            batch_calls: Arc::clone(&batch_calls),
            single_calls: Arc::clone(&single_calls),
        };

        let results = ds.get_order_quotes_batch(&orders).await;

        assert_eq!(results.len(), 3);
        assert_eq!(quote_ratio(&results[0]), "1.1");
        assert_eq!(quote_ratio(&results[1]), "1.2");
        assert_eq!(quote_ratio(&results[2]), "1.3");

        let batch_calls = batch_calls.lock().expect("lock batch calls");
        assert_eq!(batch_calls.len(), 2);
        assert!(batch_calls.contains(&(1, 2)));
        assert!(batch_calls.contains(&(10, 1)));
        assert!(single_calls.lock().expect("lock single calls").is_empty());
    }

    #[rocket::async_test]
    async fn test_fetch_order_quotes_grouped_falls_back_when_batch_fails() {
        let orders = vec![
            mock_order_for_chain(
                137,
                "0x0000000000000000000000000000000000000000000000000000000000000011",
            ),
            mock_order_for_chain(
                137,
                "0x0000000000000000000000000000000000000000000000000000000000000012",
            ),
        ];
        let batch_calls = Arc::new(Mutex::new(Vec::new()));
        let single_calls = Arc::new(Mutex::new(Vec::new()));
        let ds = BatchingTestDataSource {
            per_order_quotes: HashMap::from([
                (order_hash_key(&orders[0]), Ok(vec![mock_quote("2.5")])),
                (order_hash_key(&orders[1]), Ok(vec![mock_quote("2.5")])),
            ]),
            batched_quotes: HashMap::from([(137, Err(ApiError::Internal("batch failed".into())))]),
            batch_calls: Arc::clone(&batch_calls),
            single_calls: Arc::clone(&single_calls),
        };

        let results = ds.get_order_quotes_batch(&orders).await;

        assert_eq!(results.len(), 2);
        assert_eq!(quote_ratio(&results[0]), "2.5");
        assert_eq!(quote_ratio(&results[1]), "2.5");
        assert_eq!(
            batch_calls.lock().expect("lock batch calls").as_slice(),
            &[(137, 2)]
        );
        assert_eq!(single_calls.lock().expect("lock single calls").len(), 2);
    }

    #[rocket::async_test]
    async fn test_fetch_order_quotes_grouped_falls_back_on_result_count_mismatch() {
        let orders = vec![
            mock_order_for_chain(
                42161,
                "0x0000000000000000000000000000000000000000000000000000000000000021",
            ),
            mock_order_for_chain(
                42161,
                "0x0000000000000000000000000000000000000000000000000000000000000022",
            ),
        ];
        let batch_calls = Arc::new(Mutex::new(Vec::new()));
        let single_calls = Arc::new(Mutex::new(Vec::new()));
        let ds = BatchingTestDataSource {
            per_order_quotes: HashMap::from([
                (order_hash_key(&orders[0]), Ok(vec![mock_quote("3.5")])),
                (order_hash_key(&orders[1]), Ok(vec![mock_quote("3.5")])),
            ]),
            batched_quotes: HashMap::from([(42161, Ok(vec![vec![mock_quote("9.9")]]))]),
            batch_calls: Arc::clone(&batch_calls),
            single_calls: Arc::clone(&single_calls),
        };

        let results = ds.get_order_quotes_batch(&orders).await;

        assert_eq!(results.len(), 2);
        assert_eq!(quote_ratio(&results[0]), "3.5");
        assert_eq!(quote_ratio(&results[1]), "3.5");
        assert_eq!(
            batch_calls.lock().expect("lock batch calls").as_slice(),
            &[(42161, 2)]
        );
        assert_eq!(single_calls.lock().expect("lock single calls").len(), 2);
    }

    #[test]
    fn test_build_orders_list_response_fails_on_length_mismatch() {
        let orders = vec![mock_order_for_chain(
            1,
            "0x0000000000000000000000000000000000000000000000000000000000000101",
        )];

        let result = build_orders_list_response(
            &orders,
            1,
            1,
            20,
            vec![],
            Denomination::Wrapped,
            &HashMap::new(),
        );

        assert!(matches!(result, Err(ApiError::Internal(_))));
    }

    #[test]
    fn test_build_order_summary_converts_unwrapped_output_values() {
        let wrapped_output = address!("ff05e1bd696900dc6a52ca35ca61bb1024eda8e2");
        let mut value = order_json();
        value["outputs"][0]["formattedBalance"] = json!("2");
        value["outputs"][0]["token"]["address"] = json!(format!("{wrapped_output:#x}"));
        value["outputs"][0]["token"]["id"] = json!(format!("{wrapped_output:#x}"));
        value["outputs"][0]["token"]["symbol"] = json!("wtMSTR");
        let order: RaindexOrder =
            serde_json::from_value(value).expect("deserialize wrapped-output order");
        let ratios = HashMap::from([(wrapped_output, wrap_ratio(wrapped_output, "3"))]);

        let summary = build_order_summary(
            &order,
            "9",
            Some("4".into()),
            Denomination::Unwrapped,
            &ratios,
        )
        .expect("summary");

        assert_eq!(summary.output_vault_balance, "6");
        assert_eq!(summary.max_output, Some("12".into()));
        assert_eq!(summary.io_ratio, "3");
    }

    #[test]
    fn test_quote_result_to_summary_extracts_ratio_and_max_output() {
        let order = mock_order_for_chain(
            1,
            "0x0000000000000000000000000000000000000000000000000000000000000102",
        );

        let summary = quote_result_to_summary(&order, Ok(vec![mock_quote("1.25")]));

        assert_eq!(
            summary,
            OrderQuoteSummary {
                io_ratio: "1.25".into(),
                max_output: Some("1".into()),
            }
        );
    }

    #[test]
    fn quote_cache_accepts_only_complete_successful_results() {
        assert!(quote_set_is_complete(&[mock_quote("1.25")]));
        assert!(!quote_set_is_complete(&[]));
        assert!(!quote_set_is_complete(&[mock_failed_quote()]));
    }

    #[test]
    fn test_quote_result_to_summary_uses_null_max_output_for_missing_data() {
        let order = mock_order_for_chain(
            1,
            "0x0000000000000000000000000000000000000000000000000000000000000103",
        );

        let empty_summary = quote_result_to_summary(&order, Ok(vec![]));
        assert_eq!(
            empty_summary,
            OrderQuoteSummary {
                io_ratio: "-".into(),
                max_output: None,
            }
        );

        let failed_summary = quote_result_to_summary(&order, Ok(vec![mock_failed_quote()]));
        assert_eq!(
            failed_summary,
            OrderQuoteSummary {
                io_ratio: "-".into(),
                max_output: None,
            }
        );
    }
}

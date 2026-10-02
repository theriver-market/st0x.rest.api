pub(crate) mod get_by_address;
pub(crate) mod get_by_taker;
pub(crate) mod get_by_token;
pub(crate) mod get_by_tx;
pub(crate) mod query;

use crate::error::{ApiError, ApiErrorCode};
use crate::routes::{raindex_backed_tokens, required_raindex_chain_ids, resolve_raindex_chain_ids};
use crate::types::common::{Denomination, TokenRef};
use crate::types::trades::{
    TradeByAddress, TradesByAddressResponse, TradesPagination, TradesPaginationParams,
};
use crate::wrap_ratio::{
    persist_wrap_ratio_snapshots_best_effort, read_wrap_ratio_responses_for_addresses,
    wrap_ratio_values_from_responses, WrapRatioValue,
};
use alloy::primitives::{Address, B256};
use async_trait::async_trait;
use rain_orderbook_common::raindex_client::trades::{
    GetTradesByOrderHashesFilters, GetTradesFilters, GetTradesTokenFilter, OrderHashes,
    RaindexTrade, RaindexTradesByOrderHashResult, RaindexTradesListResult,
};
use rain_orderbook_common::raindex_client::types::{ChainIds, PaginationParams, TimeFilter};
use rain_orderbook_common::raindex_client::{RaindexClient, RaindexError};
use rocket::serde::json::Json;
use rocket::Route;
use std::collections::{BTreeMap, HashMap};

pub(crate) type TradeWrapRatioMap = HashMap<(u32, Address, u64), WrapRatioValue>;

#[async_trait]
pub(crate) trait TradesDataSource: Send + Sync {
    async fn get_trades_by_tx(&self, tx_hash: B256) -> Result<RaindexTradesListResult, ApiError>;

    async fn get_trades_by_tx_for_chains(
        &self,
        _chain_ids: Option<Vec<u32>>,
        tx_hash: B256,
    ) -> Result<RaindexTradesListResult, ApiError> {
        self.get_trades_by_tx(tx_hash).await
    }

    async fn get_trades_for_owner(
        &self,
        owner: Address,
        pagination: PaginationParams,
        time_filter: TimeFilter,
    ) -> Result<RaindexTradesListResult, ApiError>;

    async fn get_trades_for_owner_on_chains(
        &self,
        _chain_ids: Option<Vec<u32>>,
        owner: Address,
        pagination: PaginationParams,
        time_filter: TimeFilter,
    ) -> Result<RaindexTradesListResult, ApiError> {
        self.get_trades_for_owner(owner, pagination, time_filter)
            .await
    }

    async fn get_trades_for_token(
        &self,
        token: Address,
        page: u16,
        page_size: u16,
        time_filter: TimeFilter,
    ) -> Result<RaindexTradesListResult, ApiError>;

    async fn get_trades_for_token_on_chains(
        &self,
        _chain_ids: Option<Vec<u32>>,
        token: Address,
        page: u16,
        page_size: u16,
        time_filter: TimeFilter,
    ) -> Result<RaindexTradesListResult, ApiError> {
        self.get_trades_for_token(token, page, page_size, time_filter)
            .await
    }

    async fn get_trades_for_taker(
        &self,
        taker: Address,
        page: u16,
        page_size: u16,
        time_filter: TimeFilter,
    ) -> Result<RaindexTradesListResult, ApiError>;

    async fn get_trades_for_taker_on_chains(
        &self,
        _chain_ids: Option<Vec<u32>>,
        taker: Address,
        page: u16,
        page_size: u16,
        time_filter: TimeFilter,
    ) -> Result<RaindexTradesListResult, ApiError> {
        self.get_trades_for_taker(taker, page, page_size, time_filter)
            .await
    }

    async fn get_current_wrap_ratios_for_tokens(
        &self,
        _token_addresses: &[Address],
    ) -> Result<HashMap<Address, WrapRatioValue>, ApiError> {
        Ok(HashMap::new())
    }

    /// Transactions in which RiverTaker took orders on `taker`'s behalf
    /// (Raindex records RiverTaker, not the user, as the taker of those fills).
    async fn river_take_tx_hashes(
        &self,
        _chain_ids: Option<&[u32]>,
        _taker: Address,
    ) -> Result<Vec<B256>, ApiError> {
        Ok(Vec::new())
    }

    async fn get_current_wrap_ratios_for_tokens_on_chain(
        &self,
        _chain_id: u32,
        token_addresses: &[Address],
    ) -> Result<HashMap<Address, WrapRatioValue>, ApiError> {
        self.get_current_wrap_ratios_for_tokens(token_addresses)
            .await
    }
}

#[async_trait]
pub(crate) trait BatchTradesDataSource: TradesDataSource {
    async fn get_trades_query(
        &self,
        chain_id: u32,
        filters: GetTradesFilters,
        page: u16,
        page_size: u16,
    ) -> Result<RaindexTradesListResult, ApiError>;

    async fn get_trades_by_order_hashes_query(
        &self,
        chain_id: Option<u32>,
        order_hashes: Vec<B256>,
        filters: GetTradesByOrderHashesFilters,
    ) -> Result<RaindexTradesByOrderHashResult, ApiError>;
}

pub(crate) struct RaindexTradesDataSource<'a> {
    pub client: &'a RaindexClient,
    pub pool: &'a crate::db::DbPool,
}

#[async_trait]
impl TradesDataSource for RaindexTradesDataSource<'_> {
    async fn river_take_tx_hashes(
        &self,
        chain_ids: Option<&[u32]>,
        taker: Address,
    ) -> Result<Vec<B256>, ApiError> {
        let chain = crate::river_takes::RIVER_TAKER_CHAIN_ID;
        if chain_ids.is_some_and(|ids| !ids.contains(&chain)) {
            return Ok(Vec::new());
        }
        crate::river_takes::tx_hashes_for_user(self.pool, chain, taker)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "failed to read RiverTaker fills for taker");
                ApiError::Internal("failed to query trades".into())
            })
    }

    async fn get_trades_by_tx(&self, tx_hash: B256) -> Result<RaindexTradesListResult, ApiError> {
        self.get_trades_by_tx_for_chains(None, tx_hash).await
    }

    async fn get_trades_by_tx_for_chains(
        &self,
        chain_ids: Option<Vec<u32>>,
        tx_hash: B256,
    ) -> Result<RaindexTradesListResult, ApiError> {
        let chain_ids = resolve_raindex_chain_ids(self.client, chain_ids)?;
        self.client
            .get_trades_for_transaction(Some(ChainIds(chain_ids)), None, tx_hash)
            .await
            .map_err(|e| match e {
                RaindexError::TransactionIndexingTimeout { tx_hash, attempts } => {
                    ApiError::NotYetIndexed(format!(
                        "transaction {tx_hash:#x} not yet indexed after {attempts} attempts"
                    ))
                }
                other => {
                    tracing::error!(error = %other, "failed to query trades for transaction");
                    ApiError::Internal("failed to query trades".into())
                }
            })
    }

    async fn get_trades_for_owner(
        &self,
        owner: Address,
        pagination: PaginationParams,
        time_filter: TimeFilter,
    ) -> Result<RaindexTradesListResult, ApiError> {
        self.get_trades_for_owner_on_chains(None, owner, pagination, time_filter)
            .await
    }

    async fn get_trades_for_owner_on_chains(
        &self,
        chain_ids: Option<Vec<u32>>,
        owner: Address,
        pagination: PaginationParams,
        time_filter: TimeFilter,
    ) -> Result<RaindexTradesListResult, ApiError> {
        let chain_ids = resolve_raindex_chain_ids(self.client, chain_ids)?;
        let filters = GetTradesFilters {
            owners: vec![owner],
            time_filter: Some(time_filter),
            ..Default::default()
        };

        self.client
            .get_trades(
                Some(ChainIds(chain_ids)),
                Some(filters),
                pagination.page,
                pagination.page_size,
            )
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "failed to query trades for owner");
                ApiError::Internal("failed to query trades".into())
            })
    }

    async fn get_trades_for_token(
        &self,
        token: Address,
        page: u16,
        page_size: u16,
        time_filter: TimeFilter,
    ) -> Result<RaindexTradesListResult, ApiError> {
        self.get_trades_for_token_on_chains(None, token, page, page_size, time_filter)
            .await
    }

    async fn get_trades_for_token_on_chains(
        &self,
        chain_ids: Option<Vec<u32>>,
        token: Address,
        page: u16,
        page_size: u16,
        time_filter: TimeFilter,
    ) -> Result<RaindexTradesListResult, ApiError> {
        let chain_ids = resolve_raindex_chain_ids(self.client, chain_ids)?;
        let filters = GetTradesFilters {
            tokens: Some(GetTradesTokenFilter {
                inputs: Some(vec![token]),
                outputs: Some(vec![token]),
            }),
            time_filter: Some(time_filter),
            ..Default::default()
        };

        self.client
            .get_trades(
                Some(ChainIds(chain_ids)),
                Some(filters),
                Some(page),
                Some(page_size),
            )
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "failed to query trades for token");
                ApiError::Internal("failed to query trades".into())
            })
    }

    async fn get_trades_for_taker(
        &self,
        taker: Address,
        page: u16,
        page_size: u16,
        time_filter: TimeFilter,
    ) -> Result<RaindexTradesListResult, ApiError> {
        self.get_trades_for_taker_on_chains(None, taker, page, page_size, time_filter)
            .await
    }

    async fn get_trades_for_taker_on_chains(
        &self,
        chain_ids: Option<Vec<u32>>,
        taker: Address,
        page: u16,
        page_size: u16,
        time_filter: TimeFilter,
    ) -> Result<RaindexTradesListResult, ApiError> {
        let chain_ids = resolve_raindex_chain_ids(self.client, chain_ids)?;
        let filters = GetTradesFilters {
            takers: vec![taker],
            time_filter: Some(time_filter),
            ..Default::default()
        };

        self.client
            .get_trades(
                Some(ChainIds(chain_ids)),
                Some(filters),
                Some(page),
                Some(page_size),
            )
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "failed to query trades for taker");
                ApiError::Internal("failed to query trades".into())
            })
    }

    async fn get_current_wrap_ratios_for_tokens(
        &self,
        token_addresses: &[Address],
    ) -> Result<HashMap<Address, WrapRatioValue>, ApiError> {
        let tokens = raindex_backed_tokens(self.client)?;

        let responses = read_wrap_ratio_responses_for_addresses(&tokens, token_addresses).await?;
        persist_wrap_ratio_snapshots_best_effort(self.pool, &responses).await;
        Ok(wrap_ratio_values_from_responses(responses))
    }

    async fn get_current_wrap_ratios_for_tokens_on_chain(
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
impl BatchTradesDataSource for RaindexTradesDataSource<'_> {
    async fn get_trades_query(
        &self,
        chain_id: u32,
        filters: GetTradesFilters,
        page: u16,
        page_size: u16,
    ) -> Result<RaindexTradesListResult, ApiError> {
        self.client
            .get_trades(
                Some(ChainIds(vec![chain_id])),
                Some(filters),
                Some(page),
                Some(page_size),
            )
            .await
            .map_err(|error| {
                tracing::error!(chain_id, %error, "failed to batch query trades");
                ApiError::coded(
                    ApiErrorCode::TradesQueryFailed,
                    "the trade source could not serve this request",
                )
            })
    }

    async fn get_trades_by_order_hashes_query(
        &self,
        chain_id: Option<u32>,
        order_hashes: Vec<B256>,
        filters: GetTradesByOrderHashesFilters,
    ) -> Result<RaindexTradesByOrderHashResult, ApiError> {
        let chain_ids = match chain_id {
            Some(chain_id) => vec![chain_id],
            None => required_raindex_chain_ids(self.client)?,
        };
        self.client
            .get_trades_by_order_hashes(
                Some(ChainIds(chain_ids)),
                OrderHashes(order_hashes),
                Some(filters),
            )
            .await
            .map_err(|error| {
                tracing::error!(chain_id, %error, "failed to batch query trades by order hashes");
                ApiError::coded(
                    ApiErrorCode::TradesQueryFailed,
                    "the trade source could not serve this request",
                )
            })
    }
}

pub(super) fn map_trade_for_list(
    trade: &RaindexTrade,
    denomination: Denomination,
    trade_wrap_ratios: &TradeWrapRatioMap,
) -> Result<TradeByAddress, ApiError> {
    let tx_hash = trade.transaction().id();
    let input_vc = trade.input_vault_balance_change();
    let output_vc = trade.output_vault_balance_change();

    let input_token_data = input_vc.token();
    let output_token_data = output_vc.token();

    let timestamp: u64 = trade.timestamp().try_into().map_err(|_| {
        tracing::error!("timestamp does not fit in u64");
        ApiError::Internal("timestamp overflow".into())
    })?;
    let block_number = trade_block_number(trade)?;
    let wrap_ratios = if denomination == Denomination::Unwrapped {
        wrap_ratio_map_for_trade(
            trade.chain_id(),
            input_token_data.address(),
            output_token_data.address(),
            block_number,
            trade_wrap_ratios,
        )
    } else {
        HashMap::new()
    };
    let input_amount = if denomination == Denomination::Unwrapped {
        crate::denomination::convert_wrapped_amount_for_token(
            input_vc.formatted_amount(),
            input_token_data.address(),
            &wrap_ratios,
        )?
    } else {
        input_vc.formatted_amount()
    };
    let output_amount = if denomination == Denomination::Unwrapped {
        crate::denomination::convert_wrapped_amount_for_token(
            output_vc.formatted_amount(),
            output_token_data.address(),
            &wrap_ratios,
        )?
    } else {
        output_vc.formatted_amount()
    };

    Ok(TradeByAddress {
        chain_id: trade.chain_id(),
        tx_hash,
        input_amount,
        output_amount,
        input_token: TokenRef {
            address: input_token_data.address(),
            symbol: input_token_data.symbol().unwrap_or_default(),
            decimals: input_token_data.decimals(),
        },
        output_token: TokenRef {
            address: output_token_data.address(),
            symbol: output_token_data.symbol().unwrap_or_default(),
            decimals: output_token_data.decimals(),
        },
        order_hash: Some(trade.order_hash()),
        timestamp,
        block_number,
    })
}

pub(super) async fn build_trades_list_response(
    ds: &dyn TradesDataSource,
    result: RaindexTradesListResult,
    page: u32,
    page_size: u32,
    denomination: Denomination,
) -> Result<Json<TradesByAddressResponse>, ApiError> {
    build_trades_list_response_from_parts(
        ds,
        result.trades(),
        result.total_count(),
        page,
        page_size,
        denomination,
    )
    .await
    .map(Json)
}

pub(super) async fn build_trades_list_response_from_parts(
    ds: &dyn TradesDataSource,
    indexed_trades: &[RaindexTrade],
    total_trades: u64,
    page: u32,
    page_size: u32,
    denomination: Denomination,
) -> Result<TradesByAddressResponse, ApiError> {
    let trade_wrap_ratios =
        current_wrap_ratios_for_trades(ds, denomination, indexed_trades).await?;
    let trades = indexed_trades
        .iter()
        .map(|trade| map_trade_for_list(trade, denomination, &trade_wrap_ratios))
        .collect::<Result<Vec<_>, ApiError>>()?;

    let total_pages = if page_size > 0 {
        total_trades.div_ceil(u64::from(page_size))
    } else {
        0
    };
    let has_more = u64::from(page) < total_pages;

    Ok(TradesByAddressResponse {
        trades,
        pagination: TradesPagination {
            page,
            page_size,
            total_trades,
            total_pages,
            has_more,
        },
    })
}

pub(super) async fn current_wrap_ratios_for_trades<'a>(
    ds: &dyn TradesDataSource,
    denomination: Denomination,
    trades: impl IntoIterator<Item = &'a RaindexTrade>,
) -> Result<TradeWrapRatioMap, ApiError> {
    if denomination == Denomination::Wrapped {
        return Ok(HashMap::new());
    }

    let trades = trades.into_iter().collect::<Vec<_>>();
    if trades.is_empty() {
        return Ok(HashMap::new());
    }

    let mut token_addresses_by_chain: BTreeMap<u32, Vec<Address>> = BTreeMap::new();
    for trade in &trades {
        let token_addresses = token_addresses_by_chain
            .entry(trade.chain_id())
            .or_default();
        token_addresses.push(trade.input_vault_balance_change().token().address());
        token_addresses.push(trade.output_vault_balance_change().token().address());
    }
    let mut current_ratios = HashMap::new();
    for (chain_id, mut token_addresses) in token_addresses_by_chain {
        token_addresses.sort_unstable();
        token_addresses.dedup();
        let chain_ratios = ds
            .get_current_wrap_ratios_for_tokens_on_chain(chain_id, &token_addresses)
            .await?;
        current_ratios.extend(
            chain_ratios
                .into_iter()
                .map(|(address, ratio)| ((chain_id, address), ratio)),
        );
    }
    let mut ratios = HashMap::new();

    for trade in trades {
        let block_number = trade_block_number(trade)?;
        for token in [
            trade.input_vault_balance_change().token().address(),
            trade.output_vault_balance_change().token().address(),
        ] {
            if let Some(ratio) = current_ratios.get(&(trade.chain_id(), token)) {
                ratios.insert((trade.chain_id(), token, block_number), ratio.clone());
            }
        }
    }

    Ok(ratios)
}

pub(super) fn wrap_ratio_map_for_trade(
    chain_id: u32,
    input_token: Address,
    output_token: Address,
    block_number: u64,
    trade_wrap_ratios: &TradeWrapRatioMap,
) -> crate::denomination::WrapRatioMap {
    let mut ratios = HashMap::new();
    if let Some(ratio) = trade_wrap_ratios.get(&(chain_id, input_token, block_number)) {
        ratios.insert(input_token, ratio.clone());
    }
    if let Some(ratio) = trade_wrap_ratios.get(&(chain_id, output_token, block_number)) {
        ratios.insert(output_token, ratio.clone());
    }
    ratios
}

pub(super) fn trade_block_number(trade: &RaindexTrade) -> Result<u64, ApiError> {
    trade.transaction().block_number().try_into().map_err(|_| {
        tracing::error!("block number does not fit in u64");
        ApiError::Internal("block number overflow".into())
    })
}

pub(super) fn trades_pagination_params(
    params: TradesPaginationParams,
) -> Result<(u32, u32, u16, u16, TimeFilter), ApiError> {
    let page = params.page.unwrap_or(1);
    let page_size = params.page_size.unwrap_or(20);

    let sdk_page = page
        .try_into()
        .map_err(|_| ApiError::BadRequest("page value too large".into()))?;
    let sdk_page_size = page_size
        .try_into()
        .map_err(|_| ApiError::BadRequest("page_size value too large".into()))?;
    let time_filter = TimeFilter {
        start: params.start_time,
        end: params.end_time,
    };

    Ok((page, page_size, sdk_page, sdk_page_size, time_filter))
}

pub fn routes() -> Vec<Route> {
    rocket::routes![
        get_by_tx::get_trades_by_tx,
        query::post_trades_query,
        get_by_token::get_trades_by_token,
        get_by_taker::get_trades_by_taker,
        get_by_address::get_trades_by_address
    ]
}

pub fn routes_v2() -> Vec<Route> {
    routes()
}

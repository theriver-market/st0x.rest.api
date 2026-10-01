mod calldata;
mod denomination;
mod exchange_log;
mod quote;
mod slippage;

use crate::analytics::{Analytics, AnalyticsEvent, ApiVersion, SwapFailure};
use crate::atomic_liquidity::has_executable_atomic_amounts;
use crate::cache::RouteResponseCaches;
use crate::db::DbPool;
use crate::error::{ApiError, ApiErrorCode};
use crate::routes::raindex_backed_tokens;
use crate::types::swap::{SwapCalldataMode, SwapCalldataResponse, SwapDenomination};
use crate::wrap_ratio::{
    persist_wrap_ratio_snapshots_best_effort, read_wrap_ratio_responses_for_addresses,
    wrap_ratio_values_from_responses, WrapRatioValue,
};
use alloy::primitives::Address;
use async_trait::async_trait;
use rain_orderbook_common::raindex_client::order_quotes::{
    get_order_quotes_batch_with_injector, RaindexOrderQuote,
};
use rain_orderbook_common::raindex_client::orders::{
    GetOrdersFilters, GetOrdersTokenFilter, RaindexOrder,
};
use rain_orderbook_common::raindex_client::take_orders::{
    build_candidate_from_quote, TakeOrdersInfo, TakeOrdersRequest,
};
use rain_orderbook_common::raindex_client::types::ChainIds;
use rain_orderbook_common::raindex_client::RaindexClient;
use rain_orderbook_common::raindex_client::RaindexError;
use rain_orderbook_common::rpc_client::RpcClientError;
use rain_orderbook_common::take_orders::{NoopInjector, TakeOrderCandidate, TakeOrdersMode};
use rocket::Route;
use std::collections::HashMap;
use std::future::Future;
use std::sync::OnceLock;

/// RiverTaker on Base (theriver-market/river-contracts 740bd8a, verified on
/// Blockscout): immutable orderbook/USDC/fee receiver/10 bps, no owner.
const RIVER_TAKER_BASE: &str = "0xd0daDF272d6c07129c058B937B867428A46c4fDb";

struct SwapAnalyticsContext {
    chain_id: Option<u32>,
    input_token: Address,
    output_token: Address,
    requested_amount: String,
    denomination: serde_json::Value,
    api_version: ApiVersion,
    mode: Option<serde_json::Value>,
    taker: Option<Address>,
}

impl SwapAnalyticsContext {
    fn failure(&self) -> SwapFailure<'_> {
        SwapFailure {
            chain_id: self.chain_id,
            input_token: self.input_token,
            output_token: self.output_token,
            requested_amount: &self.requested_amount,
            denomination: self.denomination.clone(),
            api_version: self.api_version,
            mode: self.mode.clone(),
            taker: self.taker,
        }
    }
}

fn snapshot_swap_context(
    analytics: &Analytics,
    build: impl FnOnce() -> SwapAnalyticsContext,
) -> Option<SwapAnalyticsContext> {
    analytics.is_enabled().then(build)
}

async fn capture_swap_outcome<T>(
    analytics: &Analytics,
    context: Option<SwapAnalyticsContext>,
    operation: impl Future<Output = Result<T, ApiError>>,
    build_failure: impl FnOnce(&SwapAnalyticsContext, &ApiError) -> AnalyticsEvent,
    build_success: impl FnOnce(&SwapAnalyticsContext, &T) -> AnalyticsEvent,
) -> Result<T, ApiError> {
    match operation.await {
        Ok(response) => {
            if let Some(context) = context.as_ref() {
                analytics.capture(|| build_success(context, &response));
            }
            Ok(response)
        }
        Err(error) => {
            if let Some(context) = context.as_ref() {
                analytics.capture(|| build_failure(context, &error));
            }
            Err(error)
        }
    }
}

const ORACLE_FETCH_FAILURE_PREFIX: &str = "Oracle fetch failed for pair (";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SwapQuoteFailure {
    OracleUnavailable,
    QuoteFailed,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SwapQuoteFailures {
    oracle_unavailable: usize,
    quote_failed: usize,
}

impl SwapQuoteFailures {
    fn record(&mut self, failure: SwapQuoteFailure) {
        match failure {
            SwapQuoteFailure::OracleUnavailable => self.oracle_unavailable += 1,
            SwapQuoteFailure::QuoteFailed => self.quote_failed += 1,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.oracle_unavailable == 0 && self.quote_failed == 0
    }

    pub(crate) fn oracle_unavailable_error(&self) -> Option<ApiError> {
        (self.oracle_unavailable > 0).then(|| {
            ApiError::coded(
                ApiErrorCode::SwapOracleUnavailable,
                "the oracle required to evaluate this swap is temporarily unavailable",
            )
        })
    }
}

impl FromIterator<SwapQuoteFailure> for SwapQuoteFailures {
    fn from_iter<T: IntoIterator<Item = SwapQuoteFailure>>(failures: T) -> Self {
        let mut result = Self::default();
        for failure in failures {
            result.record(failure);
        }
        result
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SwapCandidateBuild {
    pub candidates: Vec<TakeOrderCandidate>,
    pub failures: SwapQuoteFailures,
}

fn classify_quote_failure(error: Option<&str>) -> SwapQuoteFailure {
    match error {
        Some(error) if error.starts_with(ORACLE_FETCH_FAILURE_PREFIX) => {
            SwapQuoteFailure::OracleUnavailable
        }
        _ => SwapQuoteFailure::QuoteFailed,
    }
}

fn quote_matches_pair(
    order: &rain_orderbook_bindings::IRaindexV6::OrderV4,
    quote: &RaindexOrderQuote,
    input_token: Address,
    output_token: Address,
) -> bool {
    let Some(input) = order.validInputs.get(quote.pair.input_index as usize) else {
        return false;
    };
    let Some(output) = order.validOutputs.get(quote.pair.output_index as usize) else {
        return false;
    };
    input.token == input_token && output.token == output_token
}

fn configured_token_decimals(
    tokens: &HashMap<(u32, Address), Option<u8>>,
    chain_id: u32,
    token: Address,
) -> Result<u8, ApiError> {
    match tokens.get(&(chain_id, token)) {
        Some(Some(decimals)) => Ok(*decimals),
        Some(None) => {
            tracing::error!(%token, "configured swap token is missing decimals");
            Err(ApiError::Internal(
                "the token registry is missing required decimals".into(),
            ))
        }
        None => Err(ApiError::coded(
            ApiErrorCode::SwapUnsupportedToken,
            "one or both swap tokens are unsupported",
        )),
    }
}

fn build_swap_candidates_from_quotes(
    orders: &[RaindexOrder],
    quotes: Vec<Vec<RaindexOrderQuote>>,
    input_token: Address,
    output_token: Address,
    input_decimals: u8,
    output_decimals: u8,
) -> Result<SwapCandidateBuild, ApiError> {
    if quotes.len() != orders.len() {
        tracing::error!(
            order_count = orders.len(),
            quote_set_count = quotes.len(),
            "order candidate quote count mismatch"
        );
        return Err(ApiError::coded(
            ApiErrorCode::SwapQuoteFailed,
            "the swap quote could not be generated",
        ));
    }

    let mut result = SwapCandidateBuild::default();
    let mut non_executable_atomic_count = 0usize;
    for (order, order_quotes) in orders.iter().zip(quotes) {
        let order_v4: rain_orderbook_bindings::IRaindexV6::OrderV4 =
            order.try_into().map_err(|e| {
                tracing::error!(error = %e, "failed to inspect order quotes");
                ApiError::coded(
                    ApiErrorCode::SwapQuoteFailed,
                    "the swap quote could not be generated",
                )
            })?;
        for quote in &order_quotes {
            if !quote_matches_pair(&order_v4, quote, input_token, output_token) {
                continue;
            }

            if !quote.success || quote.data.is_none() {
                result
                    .failures
                    .record(classify_quote_failure(quote.error.as_deref()));
                continue;
            }

            if let Some(candidate) = build_candidate_from_quote(order, quote).map_err(|e| {
                tracing::error!(error = %e, "failed to build order candidate");
                ApiError::coded(
                    ApiErrorCode::SwapQuoteFailed,
                    "the swap quote could not be generated",
                )
            })? {
                let executable = match has_executable_atomic_amounts(
                    candidate.max_output,
                    candidate.ratio,
                    input_decimals,
                    output_decimals,
                ) {
                    Ok(executable) => executable,
                    Err(error) => {
                        non_executable_atomic_count += 1;
                        tracing::warn!(
                            %error,
                            order_hash = %order.order_hash(),
                            input_token = %input_token,
                            output_token = %output_token,
                            "excluding swap candidate with invalid atomic amounts"
                        );
                        continue;
                    }
                };
                if !executable {
                    non_executable_atomic_count += 1;
                    tracing::debug!(
                        order_hash = %order.order_hash(),
                        input_token = %input_token,
                        output_token = %output_token,
                        "excluding swap candidate that truncates to zero atomic units"
                    );
                    continue;
                }
                result.candidates.push(candidate);
            }
        }
    }

    tracing::info!(
        order_count = orders.len(),
        candidate_count = result.candidates.len(),
        oracle_unavailable_count = result.failures.oracle_unavailable,
        quote_failure_count = result.failures.quote_failed,
        non_executable_atomic_count,
        input_token = %input_token,
        output_token = %output_token,
        "built REST swap candidates"
    );

    Ok(result)
}

#[async_trait]
pub(crate) trait SwapDataSource: Send + Sync {
    async fn validate_supported_tokens(
        &self,
        input_token: Address,
        output_token: Address,
    ) -> Result<(), ApiError>;

    async fn validate_supported_tokens_on_chain(
        &self,
        _chain_id: u32,
        input_token: Address,
        output_token: Address,
    ) -> Result<(), ApiError> {
        self.validate_supported_tokens(input_token, output_token)
            .await
    }

    async fn get_orders_for_pair(
        &self,
        input_token: Address,
        output_token: Address,
    ) -> Result<Vec<RaindexOrder>, ApiError>;

    async fn get_orders_for_pair_on_chain(
        &self,
        _chain_id: u32,
        input_token: Address,
        output_token: Address,
    ) -> Result<Vec<RaindexOrder>, ApiError> {
        self.get_orders_for_pair(input_token, output_token).await
    }

    async fn build_candidates_for_pair(
        &self,
        orders: &[RaindexOrder],
        input_token: Address,
        output_token: Address,
        counterparty: Address,
    ) -> Result<SwapCandidateBuild, ApiError>;

    async fn get_calldata(
        &self,
        request: TakeOrdersRequest,
    ) -> Result<SwapCalldataResponse, ApiError>;

    async fn get_calldata_on_chain(
        &self,
        chain_id: u32,
        request: TakeOrdersRequest,
    ) -> Result<SwapCalldataResponse, ApiError> {
        let mut response = self.get_calldata(request).await?;
        response.chain_id = chain_id;
        Ok(response)
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

pub(crate) struct RaindexSwapDataSource<'a> {
    pub client: &'a RaindexClient,
    pub caches: &'a RouteResponseCaches,
    pub pool: &'a DbPool,
    configured_tokens: OnceLock<HashMap<(u32, Address), Option<u8>>>,
}

impl<'a> RaindexSwapDataSource<'a> {
    pub(crate) fn new(
        client: &'a RaindexClient,
        caches: &'a RouteResponseCaches,
        pool: &'a DbPool,
    ) -> Self {
        Self {
            client,
            caches,
            pool,
            configured_tokens: OnceLock::new(),
        }
    }

    fn configured_tokens(&self) -> Result<&HashMap<(u32, Address), Option<u8>>, ApiError> {
        if let Some(tokens) = self.configured_tokens.get() {
            return Ok(tokens);
        }

        let tokens = raindex_backed_tokens(self.client)?
            .into_iter()
            .map(|token| ((token.network.chain_id, token.address), token.decimals))
            .collect();
        let _ = self.configured_tokens.set(tokens);
        self.configured_tokens.get().ok_or_else(|| {
            tracing::error!("failed to cache configured swap tokens");
            ApiError::Internal("failed to read the token registry".into())
        })
    }
}

fn swap_candidates_cache_key(
    orders: &[RaindexOrder],
    input_token: Address,
    output_token: Address,
) -> String {
    let mut order_keys = orders
        .iter()
        .map(|order| {
            format!(
                "{}:{}:{}",
                order.chain_id(),
                order.raindex(),
                order.order_hash()
            )
        })
        .collect::<Vec<_>>();
    order_keys.sort_unstable();
    let order_keys = order_keys.join(",");
    format!("swap-candidates/latest/default/{input_token}/{output_token}/{order_keys}")
}

#[async_trait]
impl<'a> SwapDataSource for RaindexSwapDataSource<'a> {
    async fn validate_supported_tokens(
        &self,
        input_token: Address,
        output_token: Address,
    ) -> Result<(), ApiError> {
        let tokens = self.configured_tokens()?;
        let chain_ids = tokens
            .keys()
            .filter(|(_, token)| *token == input_token)
            .map(|(chain_id, _)| *chain_id)
            .filter(|chain_id| tokens.contains_key(&(*chain_id, output_token)))
            .collect::<std::collections::HashSet<_>>();
        if chain_ids.len() != 1 {
            tracing::warn!(%input_token, %output_token, "swap token pair is missing or ambiguous across configured chains");
            return Err(ApiError::coded(
                ApiErrorCode::SwapUnsupportedToken,
                "one or both swap tokens are unsupported",
            ));
        }
        let chain_id = chain_ids.into_iter().next().ok_or_else(|| {
            ApiError::coded(
                ApiErrorCode::SwapUnsupportedToken,
                "one or both swap tokens are unsupported",
            )
        })?;
        self.validate_supported_tokens_on_chain(chain_id, input_token, output_token)
            .await
    }

    async fn validate_supported_tokens_on_chain(
        &self,
        chain_id: u32,
        input_token: Address,
        output_token: Address,
    ) -> Result<(), ApiError> {
        let tokens = self.configured_tokens()?;
        let input_supported = tokens.contains_key(&(chain_id, input_token));
        let output_supported = tokens.contains_key(&(chain_id, output_token));

        if input_supported && output_supported {
            configured_token_decimals(tokens, chain_id, input_token)?;
            configured_token_decimals(tokens, chain_id, output_token)?;
            tracing::info!(input_token = %input_token, output_token = %output_token, "validated supported swap tokens");
            return Ok(());
        }

        tracing::warn!(
            input_token = %input_token,
            output_token = %output_token,
            input_supported,
            output_supported,
            "swap request rejected for unsupported curated tokens"
        );
        Err(ApiError::coded(
            ApiErrorCode::SwapUnsupportedToken,
            "one or both swap tokens are unsupported",
        ))
    }

    async fn get_orders_for_pair(
        &self,
        input_token: Address,
        output_token: Address,
    ) -> Result<Vec<RaindexOrder>, ApiError> {
        let chain_ids = self.client.get_unique_chain_ids().map_err(|error| {
            tracing::error!(%error, "failed to read configured swap chains");
            ApiError::Internal("failed to read configured networks".into())
        })?;
        let chain_id = match chain_ids.as_slice() {
            [chain_id] => *chain_id,
            [] => return Err(ApiError::Internal("no configured networks".into())),
            _ => {
                return Err(ApiError::BadRequest(
                    "chainId is required when multiple networks are configured".into(),
                ));
            }
        };
        self.get_orders_for_pair_on_chain(chain_id, input_token, output_token)
            .await
    }

    async fn get_orders_for_pair_on_chain(
        &self,
        chain_id: u32,
        input_token: Address,
        output_token: Address,
    ) -> Result<Vec<RaindexOrder>, ApiError> {
        let filters = GetOrdersFilters {
            active: Some(true),
            tokens: Some(GetOrdersTokenFilter {
                inputs: Some(vec![input_token]),
                outputs: Some(vec![output_token]),
            }),
            has_positive_output_vault_balance: Some(true),
            ..Default::default()
        };
        self.client
            .get_orders(Some(ChainIds(vec![chain_id])), Some(filters), None, None)
            .await
            .map(|r| r.orders().to_vec())
            .map_err(|e| {
                tracing::error!(error = %e, "failed to query orders for pair");
                ApiError::coded(
                    ApiErrorCode::OrdersQueryFailed,
                    "the order source could not serve this request",
                )
            })
    }

    async fn build_candidates_for_pair(
        &self,
        orders: &[RaindexOrder],
        input_token: Address,
        output_token: Address,
        counterparty: Address,
    ) -> Result<SwapCandidateBuild, ApiError> {
        let configured_tokens = self.configured_tokens()?;
        let chain_id = orders.first().map(RaindexOrder::chain_id).ok_or_else(|| {
            tracing::error!("cannot build swap candidates without orders");
            no_liquidity_error()
        })?;
        if orders.iter().any(|order| order.chain_id() != chain_id) {
            tracing::error!("swap candidate orders span multiple chains");
            return Err(ApiError::Internal(
                "swap candidate orders span multiple chains".into(),
            ));
        }
        let input_decimals = configured_token_decimals(configured_tokens, chain_id, input_token)?;
        let output_decimals = configured_token_decimals(configured_tokens, chain_id, output_token)?;
        let fetch = || async {
            let quotes = get_order_quotes_batch_with_injector(
                orders,
                None,
                None,
                counterparty,
                &NoopInjector,
            )
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "failed to fetch quotes for order candidates");
                ApiError::coded(
                    ApiErrorCode::SwapQuoteFailed,
                    "the swap quote could not be generated",
                )
            })?;
            build_swap_candidates_from_quotes(
                orders,
                quotes,
                input_token,
                output_token,
                input_decimals,
                output_decimals,
            )
        };

        if !self.caches.is_enabled() || counterparty != Address::ZERO {
            return fetch().await;
        }

        self.caches
            .swap_candidates
            .get_or_try_insert_if(
                swap_candidates_cache_key(orders, input_token, output_token),
                fetch,
                |build| build.failures.is_empty(),
            )
            .await
            .map_err(|e| (*e).clone())
    }

    async fn get_calldata(
        &self,
        request: TakeOrdersRequest,
    ) -> Result<SwapCalldataResponse, ApiError> {
        let chain_id = request.chain_id;
        self.get_calldata_on_chain(chain_id, request).await
    }

    async fn get_calldata_on_chain(
        &self,
        chain_id: u32,
        request: TakeOrdersRequest,
    ) -> Result<SwapCalldataResponse, ApiError> {
        // The River: when the taker is our pinned RiverTaker contract (it pulls
        // funds from the user and charges the interface fee in the same tx),
        // build the calldata without the taker approval check / preflight
        // (the contract approves just in time and holds no balance). The site
        // simulates the real RiverTaker.take call before the wallet opens.
        let unchecked = request.taker.eq_ignore_ascii_case(RIVER_TAKER_BASE) && chain_id == 8453;
        let result = if unchecked {
            self.client.get_take_orders_calldata_unchecked(request).await
        } else {
            self.client.get_take_orders_calldata(request).await
        }
        .map_err(map_raindex_error)?;

        if let Some(approval_info) = result.approval_info() {
            let formatted_amount = approval_info.formatted_amount().to_string();
            Ok(SwapCalldataResponse {
                chain_id,
                to: approval_info.spender(),
                data: alloy::primitives::Bytes::new(),
                value: alloy::primitives::U256::ZERO,
                estimated_input: formatted_amount.clone(),
                denomination: SwapDenomination::Wrapped,
                approvals: vec![crate::types::common::Approval {
                    token: approval_info.token(),
                    spender: approval_info.spender(),
                    amount: formatted_amount,
                    symbol: String::new(),
                    approval_data: approval_info.calldata().clone(),
                }],
            })
        } else if let Some(take_orders_info) = result.take_orders_info() {
            swap_calldata_response_from_take_orders_info(chain_id, &take_orders_info)
        } else {
            tracing::error!("calldata provider returned an unexpected result state");
            Err(ApiError::coded(
                ApiErrorCode::SwapCalldataFailed,
                "swap calldata could not be generated",
            ))
        }
    }

    async fn get_wrap_ratios_for_tokens(
        &self,
        token_addresses: &[Address],
    ) -> Result<HashMap<Address, WrapRatioValue>, ApiError> {
        let tokens = raindex_backed_tokens(self.client)?;
        let chain_ids = tokens
            .iter()
            .filter(|token| token_addresses.contains(&token.address))
            .map(|token| token.network.chain_id)
            .collect::<std::collections::HashSet<_>>();
        let chain_id = match chain_ids.len() {
            0 => {
                return Err(ApiError::BadRequest(
                    "unable to resolve token network".into(),
                ))
            }
            1 => chain_ids
                .into_iter()
                .next()
                .ok_or_else(|| ApiError::BadRequest("unable to resolve token network".into()))?,
            _ => {
                return Err(ApiError::BadRequest(
                    "chainId is required when multiple networks are configured".into(),
                ));
            }
        };
        self.get_wrap_ratios_for_tokens_on_chain(chain_id, token_addresses)
            .await
    }

    async fn get_wrap_ratios_for_tokens_on_chain(
        &self,
        chain_id: u32,
        token_addresses: &[Address],
    ) -> Result<HashMap<Address, WrapRatioValue>, ApiError> {
        crate::routes::validate_raindex_chain_id(self.client, chain_id)?;
        let tokens: Vec<_> = raindex_backed_tokens(self.client)?
            .into_iter()
            .filter(|token| token.network.chain_id == chain_id)
            .collect();

        let responses = read_wrap_ratio_responses_for_addresses(&tokens, token_addresses).await?;
        persist_wrap_ratio_snapshots_best_effort(self.pool, &responses).await;
        Ok(wrap_ratio_values_from_responses(responses))
    }
}

fn swap_calldata_response_from_take_orders_info(
    chain_id: u32,
    take_orders_info: &TakeOrdersInfo,
) -> Result<SwapCalldataResponse, ApiError> {
    let expected_sell = take_orders_info.expected_sell().format().map_err(|e| {
        tracing::error!(error = %e, "failed to format expected sell");
        ApiError::coded(
            ApiErrorCode::SwapCalldataFailed,
            "swap calldata could not be generated",
        )
    })?;

    Ok(SwapCalldataResponse {
        chain_id,
        to: take_orders_info.raindex(),
        data: take_orders_info.calldata().clone(),
        value: alloy::primitives::U256::ZERO,
        estimated_input: expected_sell,
        denomination: SwapDenomination::Wrapped,
        approvals: vec![],
    })
}

/// Reject a swap whose input and output token are the same, before any order lookup.
///
/// Such a request is always a caller bug, but it is not a *cheap* one: the token is
/// individually supported, so `validate_supported_tokens` passes, and the pair matches
/// every order holding that token. For a stablecoin leg that is the entire book, which
/// then gets quoted over RPC before the simulation finds no executable leg and returns
/// "no liquidity" — a ~15s round trip to deliver a misleading answer to a malformed
/// question. Failing fast makes the error self-explanatory and stops one request from
/// costing a full-book RPC sweep.
pub(crate) fn ensure_distinct_tokens(
    input_token: Address,
    output_token: Address,
) -> Result<(), ApiError> {
    if input_token != output_token {
        return Ok(());
    }
    tracing::warn!(
        token = %input_token,
        "swap request rejected: input and output token are identical"
    );
    Err(same_token_error())
}

pub(crate) fn request_chain_id(chain_id: Option<u32>) -> Result<u32, ApiError> {
    chain_id.ok_or_else(|| ApiError::BadRequest("chainId is required".into()))
}

fn same_token_error() -> ApiError {
    ApiError::coded(
        ApiErrorCode::SwapSameToken,
        "inputToken and outputToken must be different tokens",
    )
}

pub(crate) fn no_liquidity_error() -> ApiError {
    ApiError::coded(
        ApiErrorCode::SwapNoLiquidity,
        "no executable liquidity is available for this pair",
    )
}

fn map_raindex_error(e: RaindexError) -> ApiError {
    match &e {
        RaindexError::NoLiquidity | RaindexError::InsufficientLiquidity { .. } => {
            tracing::warn!(error = %e, "no liquidity found");
            no_liquidity_error()
        }
        // Kept distinct from the generic bucket below: `ensure_distinct_tokens` should
        // catch this at the edge, so reaching it here means a same-token pair slipped
        // past the guard and the caller still deserves to be told which mistake it was.
        RaindexError::SameTokenPair => {
            tracing::warn!(error = %e, "same-token pair reached the raindex layer");
            same_token_error()
        }
        RaindexError::NonPositiveAmount
        | RaindexError::NegativePriceCap
        | RaindexError::FromHexError(_)
        | RaindexError::Float(_) => {
            tracing::warn!(error = %e, "invalid request parameters");
            ApiError::BadRequest("invalid swap parameters".into())
        }
        RaindexError::RaindexSubgraphClientError(_) => {
            tracing::error!(error = %e, "order source failed during calldata generation");
            ApiError::coded(
                ApiErrorCode::OrdersQueryFailed,
                "the order source could not serve this request",
            )
        }
        RaindexError::RpcClientError(
            RpcClientError::Transport(_)
            | RpcClientError::RpcError { .. }
            | RpcClientError::RateLimited { .. },
        ) => {
            tracing::error!(error = %e, "RPC unavailable during calldata generation");
            ApiError::coded(
                ApiErrorCode::UpstreamUnavailable,
                "a required chain data provider is temporarily unavailable",
            )
        }
        RaindexError::PreflightError(_) => {
            tracing::error!(error = %e, "preflight failed without a typed failure category");
            ApiError::coded(
                ApiErrorCode::SwapCalldataFailed,
                "swap calldata could not be generated",
            )
        }
        _ => {
            tracing::error!(error = %e, "calldata generation failed");
            ApiError::coded(
                ApiErrorCode::SwapCalldataFailed,
                "swap calldata could not be generated",
            )
        }
    }
}

impl From<SwapCalldataMode> for TakeOrdersMode {
    fn from(mode: SwapCalldataMode) -> Self {
        match mode {
            SwapCalldataMode::BuyUpTo => TakeOrdersMode::BuyUpTo,
            SwapCalldataMode::SpendExact => TakeOrdersMode::SpendExact,
            SwapCalldataMode::SpendUpTo => TakeOrdersMode::SpendUpTo,
        }
    }
}

pub use calldata::*;
pub use quote::*;

pub fn routes() -> Vec<Route> {
    rocket::routes![quote::post_swap_quote, calldata::post_swap_calldata]
}

pub fn routes_v2() -> Vec<Route> {
    rocket::routes![quote::post_swap_quote_v2, calldata::post_swap_calldata_v2]
}

pub fn routes_v3() -> Vec<Route> {
    routes_v2()
}

#[cfg(test)]
mod tests {
    use super::{
        build_swap_candidates_from_quotes, classify_quote_failure, configured_token_decimals,
        ensure_distinct_tokens, map_raindex_error, snapshot_swap_context,
        swap_calldata_response_from_take_orders_info, swap_candidates_cache_key, SwapQuoteFailure,
    };
    use crate::analytics::Analytics;
    use crate::error::{ApiError, ApiErrorCode};
    use alloy::primitives::{address, Address, Bytes, U256};
    use alloy::sol_types::SolValue;
    use rain_orderbook_bindings::IRaindexV6::{EvaluableV4, OrderV4, IOV2};
    use rain_orderbook_common::raindex_client::orders::RaindexOrder;
    use rain_orderbook_common::raindex_client::take_orders::TakeOrdersInfo;
    use rain_orderbook_common::raindex_client::RaindexError;
    use rain_orderbook_common::rpc_client::RpcClientError;
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn mock_order(chain_id: u32, order_hash: &str) -> RaindexOrder {
        let mut value = crate::test_helpers::order_json();
        value["chainId"] = json!(chain_id);
        value["orderHash"] = json!(order_hash);
        serde_json::from_value(value).expect("deserialize mock order")
    }

    fn single_pair_order(
        input_token: Address,
        output_token: Address,
        order_hash: &str,
    ) -> RaindexOrder {
        let decoded_order = OrderV4 {
            owner: Address::ZERO,
            nonce: U256::ZERO.into(),
            evaluable: EvaluableV4 {
                interpreter: Address::ZERO,
                store: Address::ZERO,
                bytecode: Bytes::new(),
            },
            validInputs: vec![IOV2 {
                token: input_token,
                vaultId: U256::ZERO.into(),
            }],
            validOutputs: vec![IOV2 {
                token: output_token,
                vaultId: U256::ZERO.into(),
            }],
        };
        let mut value = crate::test_helpers::order_json();
        value["orderBytes"] = json!(alloy::hex::encode_prefixed(decoded_order.abi_encode()));
        value["orderHash"] = json!(order_hash);
        serde_json::from_value(value).expect("deserialize single-pair order")
    }

    #[test]
    fn test_ensure_distinct_tokens_accepts_distinct_and_rejects_equal_tokens() {
        let usdc = address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");
        let weth = address!("4200000000000000000000000000000000000006");
        assert!(ensure_distinct_tokens(usdc, weth).is_ok());
        assert!(matches!(
            ensure_distinct_tokens(usdc, usdc),
            Err(ApiError::Coded { code, .. }) if code == ApiErrorCode::SwapSameToken
        ));
    }

    #[test]
    fn test_disabled_analytics_skips_swap_context_snapshot() {
        let built = AtomicBool::new(false);

        let context = snapshot_swap_context(&Analytics::disabled(), || {
            built.store(true, Ordering::Relaxed);
            unreachable!("disabled analytics must not build a swap context")
        });

        assert!(context.is_none());
        assert!(!built.load(Ordering::Relaxed));
    }

    /// A same-token pair that somehow reaches the raindex layer must still surface as
    /// `SWAP_SAME_TOKEN`, not be flattened into the generic invalid-parameters bucket.
    #[test]
    fn test_same_token_pair_from_raindex_keeps_its_code() {
        assert!(matches!(
            map_raindex_error(RaindexError::SameTokenPair),
            ApiError::Coded { code, .. } if code == ApiErrorCode::SwapSameToken
        ));
    }

    #[test]
    fn test_swap_candidates_cache_key_is_order_insensitive() {
        let input_token = address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");
        let output_token = address!("4200000000000000000000000000000000000006");
        let order_a = mock_order(
            8453,
            "0x0000000000000000000000000000000000000000000000000000000000000001",
        );
        let order_b = mock_order(
            8453,
            "0x0000000000000000000000000000000000000000000000000000000000000002",
        );

        assert_eq!(
            swap_candidates_cache_key(
                &[order_a.clone(), order_b.clone()],
                input_token,
                output_token,
            ),
            swap_candidates_cache_key(&[order_b, order_a], input_token, output_token)
        );
    }

    #[test]
    fn test_missing_configured_token_decimals_fail_closed() {
        let token = address!("ff05e1bd696900dc6a52ca35ca61bb1024eda8e2");
        let tokens = std::collections::HashMap::from([((8453, token), None)]);

        assert!(matches!(
            configured_token_decimals(&tokens, 8453, token),
            Err(ApiError::Internal(message))
                if message == "the token registry is missing required decimals"
        ));
    }

    #[test]
    fn test_rest_candidate_builder_excludes_production_wtmstr_dust_order() {
        let order = single_pair_order(
            crate::test_helpers::WT_MSTR,
            crate::test_helpers::USDC,
            &crate::test_helpers::WT_MSTR_DUST_ORDER_HASH.to_string(),
        );
        let quote = crate::test_helpers::production_wtmstr_dust_quote();

        let result = build_swap_candidates_from_quotes(
            &[order],
            vec![vec![quote]],
            crate::test_helpers::WT_MSTR,
            crate::test_helpers::USDC,
            18,
            6,
        )
        .expect("candidate build succeeds");

        assert!(result.candidates.is_empty());
        assert!(result.failures.is_empty());
    }

    #[test]
    fn test_invalid_atomic_candidate_does_not_hide_valid_liquidity() {
        let invalid_order = single_pair_order(
            crate::test_helpers::WT_MSTR,
            crate::test_helpers::USDC,
            "0x1000000000000000000000000000000000000000000000000000000000000000",
        );
        let valid_order = single_pair_order(
            crate::test_helpers::WT_MSTR,
            crate::test_helpers::USDC,
            "0x2000000000000000000000000000000000000000000000000000000000000000",
        );
        let invalid_quote = crate::test_helpers::wtmstr_quote("1e100", "1", "1", "1");
        let valid_quote = crate::test_helpers::wtmstr_quote("1", "1", "1", "1");

        let result = build_swap_candidates_from_quotes(
            &[invalid_order, valid_order],
            vec![vec![invalid_quote], vec![valid_quote]],
            crate::test_helpers::WT_MSTR,
            crate::test_helpers::USDC,
            18,
            6,
        )
        .expect("invalid candidate is excluded locally");

        assert_eq!(result.candidates.len(), 1);
        assert_eq!(
            result.candidates[0]
                .max_output
                .format()
                .expect("format valid output"),
            "1"
        );
    }

    #[test]
    fn test_take_orders_info_maps_executable_incident_input() {
        // After preflight removed the reverting leg, this was the only executable input.
        let expected_sell =
            "0.04310434222334697689407343024988324343123847171843280166595003948319";
        let expected_sell_float = rain_math_float::Float::parse(expected_sell.to_string())
            .expect("parse incident executable input");
        let effective_price = rain_math_float::Float::parse(
            "0.010405262850569590820343337005442828611770549820812206944645896242997".to_string(),
        )
        .expect("parse incident effective price");
        let max_sell_cap =
            rain_math_float::Float::parse("0.05".to_string()).expect("parse incident maximum sell");
        let take_orders_info: TakeOrdersInfo = serde_json::from_value(json!({
            "raindex": "0xe522cB4a5fCb2eb31a52Ff41a4653d85A4fd7C9D",
            "calldata": "0xabcdef",
            "effectivePrice": effective_price,
            "prices": [effective_price],
            "expectedSell": expected_sell_float,
            "maxSellCap": max_sell_cap
        }))
        .expect("deserialize SDK take-orders result");

        let response = swap_calldata_response_from_take_orders_info(8453, &take_orders_info)
            .expect("map ready SDK result");

        assert_eq!(response.estimated_input, expected_sell);
        assert_eq!(
            response.to,
            address!("e522cB4a5fCb2eb31a52Ff41a4653d85A4fd7C9D")
        );
        assert_eq!(response.data.as_ref(), [0xab, 0xcd, 0xef]);
        assert!(response.approvals.is_empty());
    }

    #[test]
    fn test_only_upstream_oracle_fetch_failures_are_classified_as_oracle_unavailable() {
        assert_eq!(
            classify_quote_failure(Some(
                "Oracle fetch failed for pair (0, 1): HTTP request failed: 404 Not Found"
            )),
            SwapQuoteFailure::OracleUnavailable
        );

        for unrelated in [
            "Unknown oracle session",
            "StalePrice",
            "HTTP request failed: 404 Not Found",
            "quote reverted: Oracle fetch failed for pair (0, 1)",
        ] {
            assert_eq!(
                classify_quote_failure(Some(unrelated)),
                SwapQuoteFailure::QuoteFailed
            );
        }
    }

    #[test]
    fn test_raindex_no_liquidity_maps_to_stable_code() {
        assert!(matches!(
            map_raindex_error(RaindexError::NoLiquidity),
            ApiError::Coded {
                code: ApiErrorCode::SwapNoLiquidity,
                ..
            }
        ));
    }

    #[test]
    fn test_ambiguous_raindex_preflight_maps_to_server_failure() {
        let error = map_raindex_error(RaindexError::PreflightError(
            "sensitive rpc detail".to_string(),
        ));
        assert!(matches!(
            error,
            ApiError::Coded {
                code: ApiErrorCode::SwapCalldataFailed,
                public_message: "swap calldata could not be generated",
            }
        ));
    }

    #[test]
    fn test_typed_rpc_failure_maps_to_upstream_unavailable() {
        let error = map_raindex_error(RaindexError::RpcClientError(RpcClientError::RpcError {
            message: "sensitive rpc detail".to_string(),
        }));
        assert!(matches!(
            error,
            ApiError::Coded {
                code: ApiErrorCode::UpstreamUnavailable,
                public_message: "a required chain data provider is temporarily unavailable",
            }
        ));
    }

    #[test]
    fn test_rpc_configuration_failure_is_not_retryable() {
        let error = map_raindex_error(RaindexError::RpcClientError(RpcClientError::Config {
            message: "bad local config".to_string(),
        }));
        assert!(matches!(
            error,
            ApiError::Coded {
                code: ApiErrorCode::SwapCalldataFailed,
                ..
            }
        ));
    }
}

#[cfg(test)]
pub(crate) mod test_fixtures {
    use super::{SwapCandidateBuild, SwapDataSource, SwapQuoteFailures};
    use crate::error::ApiError;
    use crate::types::swap::SwapCalldataResponse;
    use alloy::primitives::Address;
    use async_trait::async_trait;
    use rain_orderbook_common::raindex_client::orders::RaindexOrder;
    use rain_orderbook_common::raindex_client::take_orders::TakeOrdersRequest;
    use rain_orderbook_common::take_orders::TakeOrderCandidate;

    pub struct MockSwapDataSource {
        pub supported_tokens: Result<(), ApiError>,
        pub orders: Result<Vec<RaindexOrder>, ApiError>,
        pub candidates: Vec<TakeOrderCandidate>,
        pub calldata_result: Result<SwapCalldataResponse, ApiError>,
    }

    #[async_trait]
    impl SwapDataSource for MockSwapDataSource {
        async fn validate_supported_tokens(
            &self,
            _input_token: Address,
            _output_token: Address,
        ) -> Result<(), ApiError> {
            self.supported_tokens.clone()
        }

        async fn get_orders_for_pair(
            &self,
            _input_token: Address,
            _output_token: Address,
        ) -> Result<Vec<RaindexOrder>, ApiError> {
            match &self.orders {
                Ok(orders) => Ok(orders.clone()),
                Err(_) => Err(ApiError::Internal("failed to query orders".into())),
            }
        }

        async fn build_candidates_for_pair(
            &self,
            _orders: &[RaindexOrder],
            _input_token: Address,
            _output_token: Address,
            _counterparty: Address,
        ) -> Result<SwapCandidateBuild, ApiError> {
            Ok(SwapCandidateBuild {
                candidates: self.candidates.clone(),
                failures: SwapQuoteFailures::default(),
            })
        }

        async fn get_calldata(
            &self,
            _request: TakeOrdersRequest,
        ) -> Result<SwapCalldataResponse, ApiError> {
            self.calldata_result.clone()
        }
    }
}

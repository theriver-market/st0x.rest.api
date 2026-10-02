use crate::atomic_liquidity::has_executable_atomic_amounts;
use crate::cache::RouteResponseCaches;
use crate::db::market_price_history::{
    delete_market_price_snapshots_before, replace_market_price_sample, NewMarketPriceSnapshot,
};
use crate::db::DbPool;
use crate::error::ApiError;
use crate::raindex::SharedRaindexProvider;
use crate::routes::orders::{
    get_order_quotes_for_summaries, RaindexOrdersListDataSource, MAX_PAGE_SIZE,
};
use crate::routes::{
    configured_chain_ids, optional_chain_ids_filter, required_raindex_chain_ids,
    resolve_required_chain_id,
};
use crate::types::health::MarketPriceHealthStatus;
use crate::wrap_ratio::{
    legacy_address, persist_wrap_ratio_snapshots_best_effort,
    read_wrap_ratio_responses_for_addresses, token_address_variants, unwrapped_address,
    wrap_ratio_values_from_responses, WrapRatioValue,
};
use alloy::primitives::Address;
use alloy::sol_types::SolValue;
use futures::{stream, StreamExt};
use rain_math_float::Float;
use rain_orderbook_app_settings::token::TokenCfg;
use rain_orderbook_bindings::IRaindexV6::OrderV4;
use rain_orderbook_common::raindex_client::orders::{
    GetOrdersFilters, GetOrdersTokenFilter, RaindexOrder,
};
use rain_orderbook_common::raindex_client::{ChainIds, RaindexClient};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::{Add, Div, Mul};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const MAX_ORDER_PAGES: u16 = 1_000;
const MAX_PRICE_MARKET_CONCURRENCY: usize = 4;
const USDC_SYMBOL: &str = "USDC";
const MARKET_QUOTE_EXTENSION: &str = "marketQuote";

/// Minimum tradable size (in the quote token, USDC) for a quote to count toward
/// the displayed midpoint.
const MIN_DISPLAY_NOTIONAL_QUOTE: &str = "10";

#[derive(Debug, Clone)]
pub(crate) struct MarketPriceConfig {
    pub enabled: bool,
    pub sample_interval: Duration,
    pub retention: Duration,
}

impl TryFrom<&crate::config::Config> for MarketPriceConfig {
    type Error = String;

    fn try_from(config: &crate::config::Config) -> Result<Self, Self::Error> {
        if config.price_sample_interval_seconds == 0 {
            return Err("price_sample_interval_seconds must be greater than 0".to_string());
        }
        if config.price_history_retention_seconds < config.price_sample_interval_seconds {
            return Err(
                "price_history_retention_seconds must be at least one sample interval".to_string(),
            );
        }
        Ok(Self {
            enabled: config.price_sampler_enabled,
            sample_interval: Duration::from_secs(config.price_sample_interval_seconds),
            retention: Duration::from_secs(config.price_history_retention_seconds),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MarketToken {
    pub canonical_address: Address,
    pub symbol: String,
    pub variants: Vec<Address>,
    pub unwrapped_address: Option<Address>,
    pub legacy_address: Option<Address>,
}

#[derive(Debug, Clone)]
pub(crate) struct PriceMarket {
    pub chain_id: u32,
    pub quote_token_address: Address,
    pub tokens: Vec<MarketToken>,
    pub(crate) registry_tokens: Vec<TokenCfg>,
}

#[derive(Clone)]
pub(crate) struct MarketPriceState {
    pub pool: DbPool,
    pub shared_raindex: SharedRaindexProvider,
    pub config: MarketPriceConfig,
    sampler_status: std::sync::Arc<tokio::sync::RwLock<MarketPriceSamplerStatus>>,
}

impl MarketPriceState {
    pub(crate) fn new(
        pool: DbPool,
        shared_raindex: SharedRaindexProvider,
        config: MarketPriceConfig,
    ) -> Self {
        Self {
            pool,
            shared_raindex,
            config,
            sampler_status: std::sync::Arc::new(tokio::sync::RwLock::new(
                MarketPriceSamplerStatus::default(),
            )),
        }
    }

    async fn record_running(&self, running: bool) {
        self.sampler_status.write().await.running = running;
    }

    async fn record_attempt(&self, observed_at: i64) {
        self.sampler_status.write().await.last_attempt_at = Some(observed_at);
    }

    async fn record_success(&self, observed_at: i64) {
        let mut status = self.sampler_status.write().await;
        status.last_success_at = Some(observed_at);
        status.consecutive_failures = 0;
        status.error = None;
    }

    async fn record_failure(&self, message: &'static str) {
        let mut status = self.sampler_status.write().await;
        status.consecutive_failures = status.consecutive_failures.saturating_add(1);
        status.error = Some(message.to_string());
    }

    pub(crate) async fn health_status(&self, now: i64) -> MarketPriceHealthStatus {
        let status = self.sampler_status.read().await;
        let freshness_window = self.config.sample_interval.as_secs().saturating_mul(2);
        let fresh = status.last_success_at.is_some_and(|last_success| {
            u64::try_from(now.saturating_sub(last_success)).is_ok_and(|age| age <= freshness_window)
        });
        let healthy =
            !self.config.enabled || (status.running && fresh && status.consecutive_failures == 0);

        MarketPriceHealthStatus {
            enabled: self.config.enabled,
            healthy,
            running: status.running,
            last_attempt_at: status.last_attempt_at,
            last_success_at: status.last_success_at,
            consecutive_failures: status.consecutive_failures,
            error: status.error.clone(),
        }
    }
}

#[derive(Debug, Default)]
struct MarketPriceSamplerStatus {
    running: bool,
    last_attempt_at: Option<i64>,
    last_success_at: Option<i64>,
    consecutive_failures: u64,
    error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MarketSide {
    Bid,
    Ask,
}

#[derive(Debug, Clone, Copy)]
struct ObservedQuote {
    asset_address: Address,
    side: MarketSide,
    price: Float,
}

#[derive(Debug, Clone, Copy)]
struct MarketVariant {
    canonical_address: Address,
    price_multiplier: Float,
}

#[derive(Debug, Default)]
struct Book {
    best_bid: Option<Float>,
    best_ask: Option<Float>,
}

#[derive(Clone)]
pub(crate) struct MarketPriceSampler {
    state: MarketPriceState,
    caches: std::sync::Arc<RouteResponseCaches>,
}

impl MarketPriceSampler {
    pub(crate) fn new(state: MarketPriceState) -> Self {
        Self {
            state,
            caches: std::sync::Arc::new(RouteResponseCaches::new(0, Duration::ZERO)),
        }
    }

    pub(crate) async fn run(self) {
        let mut interval = tokio::time::interval(self.state.config.sample_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            if let Ok(attempted_at) = unix_now() {
                self.state.record_attempt(attempted_at).await;
            }
            match tokio::time::timeout(self.state.config.sample_interval, self.sample_once()).await
            {
                Ok(Ok(_)) => {
                    if let Ok(succeeded_at) = unix_now() {
                        self.state.record_success(succeeded_at).await;
                    }
                }
                Ok(Err(error)) => {
                    self.state
                        .record_failure("market price sampling failed")
                        .await;
                    tracing::error!(error = ?error, "market price sampling failed");
                }
                Err(_) => {
                    self.state
                        .record_failure("market price sampling timed out")
                        .await;
                    tracing::error!(
                        timeout_seconds = self.state.config.sample_interval.as_secs(),
                        "market price sampling timed out"
                    );
                }
            }
        }
    }

    pub(crate) async fn sample_once(&self) -> Result<u64, ApiError> {
        let observed_at = sample_bucket(unix_now()?, self.state.config.sample_interval)?;
        tracing::info!(observed_at, "sampling market prices");

        let retention_seconds =
            i64::try_from(self.state.config.retention.as_secs()).map_err(|_| {
                ApiError::Internal("market price retention interval is too large".into())
            })?;
        let cutoff = observed_at.saturating_sub(retention_seconds);
        let (client, discovery) = {
            let raindex = self.state.shared_raindex.read().await;
            let client = raindex.client().clone();
            let chain_ids = required_raindex_chain_ids(&client)?;
            let tokens = client.get_all_tokens().map_err(token_registry_error)?;
            let discovery = discover_price_markets(tokens.into_values(), &chain_ids);
            (client, discovery)
        };
        let markets = discovery.markets;
        let mut first_error = discovery.errors.into_iter().next().map(|(_, error)| error);
        if markets.is_empty() {
            if let Some(error) = first_error {
                return Err(error);
            }
            tracing::error!("registry has no ST0x price markets");
            return Err(ApiError::Internal(
                "registry has no ST0x price markets".into(),
            ));
        }

        let market_count = markets.len();
        let results = stream::iter(markets.into_iter().map(|market| {
            let sampler = self.clone();
            let client = client.clone();
            async move {
                let result = sampler.sample_market(&client, &market, observed_at).await;
                (market, result)
            }
        }))
        .buffer_unordered(MAX_PRICE_MARKET_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;

        let mut snapshots = Vec::new();
        for (market, result) in results {
            match result {
                Ok(mut market_snapshots) => snapshots.append(&mut market_snapshots),
                Err(error) => {
                    tracing::error!(
                        chain_id = market.chain_id,
                        quote_token = %market.quote_token_address,
                        error = ?error,
                        "failed to sample price market"
                    );
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }

        if let Some(error) = first_error {
            tracing::error!(
                snapshot_count = snapshots.len(),
                "discarding partial multi-chain market price sample"
            );
            return Err(error);
        }

        let deleted = delete_market_price_snapshots_before(&self.state.pool, cutoff)
            .await
            .map_err(|error| {
                tracing::error!(error = %error, "failed to prune market price snapshots");
                ApiError::Internal("failed to prune market price snapshots".into())
            })?;
        let (replaced, inserted) =
            replace_market_price_sample(&self.state.pool, observed_at, &snapshots)
                .await
                .map_err(|error| {
                    tracing::error!(error = %error, "failed to persist market price snapshots");
                    ApiError::Internal("failed to persist market price snapshots".into())
                })?;

        tracing::info!(
            market_count,
            snapshot_count = snapshots.len(),
            inserted,
            replaced,
            deleted,
            "market price sampling complete"
        );
        Ok(inserted)
    }

    async fn sample_market(
        &self,
        client: &RaindexClient,
        market: &PriceMarket,
        observed_at: i64,
    ) -> Result<Vec<NewMarketPriceSnapshot>, ApiError> {
        tracing::info!(
            chain_id = market.chain_id,
            quote_token = %market.quote_token_address,
            token_count = market.tokens.len(),
            "sampling price market"
        );
        let ds = RaindexOrdersListDataSource {
            client,
            caches: self.caches.as_ref(),
            pool: &self.state.pool,
        };
        let share_addresses = market
            .tokens
            .iter()
            .flat_map(|token| [Some(token.canonical_address), token.legacy_address])
            .flatten()
            .collect::<Vec<_>>();
        let wrap_ratio_responses =
            read_wrap_ratio_responses_for_addresses(&market.registry_tokens, &share_addresses)
                .await?;
        persist_wrap_ratio_snapshots_best_effort(&self.state.pool, &wrap_ratio_responses).await;
        let wrap_ratios = wrap_ratio_values_from_responses(wrap_ratio_responses);
        let variant_map = token_variant_map(&market.tokens, &wrap_ratios)?;
        let token_decimals = market_token_decimals(market);
        let orders = fetch_active_market_orders(
            client,
            market.chain_id,
            market.quote_token_address,
            &variant_map,
        )
        .await?;
        let quote_results = complete_quote_results(
            get_order_quotes_for_summaries(&ds, &orders).await,
            market.chain_id,
        )?;

        let mut observations = Vec::new();
        for (order, quotes) in orders.iter().zip(quote_results) {
            if order.chain_id() != market.chain_id {
                continue;
            }
            observations.extend(observed_quotes_from_order(
                order,
                &quotes,
                market.quote_token_address,
                &variant_map,
                &token_decimals,
            )?);
        }
        let snapshots = aggregate_observations(
            &market.tokens,
            &observations,
            market.chain_id,
            market.quote_token_address,
            observed_at,
            &wrap_ratios,
        )?;
        tracing::info!(
            chain_id = market.chain_id,
            order_count = orders.len(),
            observation_count = observations.len(),
            snapshot_count = snapshots.len(),
            "price market sampling complete"
        );
        Ok(snapshots)
    }
}

fn complete_quote_results<T>(
    results: Vec<Result<T, ApiError>>,
    chain_id: u32,
) -> Result<Vec<T>, ApiError> {
    results
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            tracing::error!(
                chain_id,
                error = ?error,
                "market price sample has incomplete order quotes"
            );
            ApiError::Internal("failed to quote complete market price book".into())
        })
}

pub(crate) async fn supervise_market_price_sampler(state: MarketPriceState) {
    loop {
        state.record_running(true).await;
        let sampler = MarketPriceSampler::new(state.clone());
        let result = tokio::spawn(sampler.run()).await;
        state.record_running(false).await;
        state
            .record_failure("market price sampler task stopped")
            .await;
        match result {
            Ok(()) => tracing::error!("market price sampler stopped unexpectedly"),
            Err(error) => {
                tracing::error!(error = %error, "market price sampler task failed unexpectedly")
            }
        }
        tokio::time::sleep(state.config.sample_interval).await;
    }
}

pub(crate) async fn configured_price_markets(
    shared_raindex: &SharedRaindexProvider,
    requested_chain_id: Option<u32>,
) -> Result<Vec<PriceMarket>, ApiError> {
    let raindex = shared_raindex.read().await;
    let chain_ids = optional_chain_ids_filter(raindex.raindex_yaml(), requested_chain_id)?
        .map_or_else(|| configured_chain_ids(raindex.raindex_yaml()), Ok)?;
    let tokens = raindex
        .client()
        .get_all_tokens()
        .map_err(token_registry_error)?;
    let discovery = discover_price_markets(tokens.into_values(), &chain_ids);
    if requested_chain_id.is_some() || discovery.markets.is_empty() {
        if let Some((_, error)) = discovery.errors.into_iter().next() {
            return Err(error);
        }
    }
    Ok(discovery.markets)
}

pub(crate) async fn resolve_required_price_market(
    shared_raindex: &SharedRaindexProvider,
    requested_chain_id: Option<u32>,
) -> Result<Option<PriceMarket>, ApiError> {
    let raindex = shared_raindex.read().await;
    let chain_id = resolve_required_chain_id(raindex.raindex_yaml(), requested_chain_id)?;
    let tokens = raindex
        .client()
        .get_all_tokens()
        .map_err(token_registry_error)?;
    let discovery = discover_price_markets(tokens.into_values(), &[chain_id]);
    if let Some((_, error)) = discovery.errors.into_iter().next() {
        return Err(error);
    }
    Ok(discovery.markets.into_iter().next())
}

pub(crate) fn find_market_token(tokens: &[MarketToken], address: Address) -> Option<&MarketToken> {
    tokens
        .iter()
        .find(|token| token.variants.contains(&address))
}

struct PriceMarketDiscovery {
    markets: Vec<PriceMarket>,
    errors: Vec<(u32, ApiError)>,
}

fn discover_price_markets(
    tokens: impl IntoIterator<Item = TokenCfg>,
    chain_ids: &[u32],
) -> PriceMarketDiscovery {
    let tokens = tokens.into_iter().collect::<Vec<_>>();
    let mut markets = Vec::new();
    let mut errors = Vec::new();

    for &chain_id in chain_ids {
        let market_tokens = market_tokens_from_registry(tokens.iter().cloned(), chain_id);
        if market_tokens.is_empty() {
            continue;
        }
        let registry_tokens = tokens
            .iter()
            .filter(|token| token.network.chain_id == chain_id)
            .cloned()
            .collect::<Vec<_>>();
        let configured_quote_tokens = tokens
            .iter()
            .filter(|token| {
                token.network.chain_id == chain_id
                    && token
                        .extensions
                        .as_ref()
                        .and_then(|extensions| extensions.get(MARKET_QUOTE_EXTENSION))
                        .and_then(serde_json::Value::as_bool)
                        == Some(true)
            })
            .collect::<Vec<_>>();
        let quote_tokens = if configured_quote_tokens.is_empty() {
            tokens
                .iter()
                .filter(|token| {
                    token.network.chain_id == chain_id
                        && token
                            .symbol
                            .as_deref()
                            .is_some_and(|symbol| symbol.eq_ignore_ascii_case(USDC_SYMBOL))
                })
                .collect::<Vec<_>>()
        } else {
            configured_quote_tokens
        };
        let quote_token_address = match quote_tokens.as_slice() {
            [token] => token.address,
            [] => {
                tracing::error!(chain_id, "registry has no market quote token");
                errors.push((
                    chain_id,
                    ApiError::Internal(format!(
                        "market quote token is not configured for chain {chain_id}"
                    )),
                ));
                continue;
            }
            _ => {
                tracing::error!(
                    chain_id,
                    quote_token_count = quote_tokens.len(),
                    "registry has ambiguous market quote tokens"
                );
                errors.push((
                    chain_id,
                    ApiError::Internal(format!(
                        "market quote token is ambiguous for chain {chain_id}"
                    )),
                ));
                continue;
            }
        };
        markets.push(PriceMarket {
            chain_id,
            quote_token_address,
            tokens: market_tokens,
            registry_tokens,
        });
    }

    PriceMarketDiscovery { markets, errors }
}

fn market_tokens_from_registry(
    tokens: impl IntoIterator<Item = TokenCfg>,
    chain_id: u32,
) -> Vec<MarketToken> {
    let mut result = tokens
        .into_iter()
        .filter(|token| {
            token.network.chain_id == chain_id && crate::wrap_ratio::is_st0x_token(token)
        })
        .map(|token| {
            let variants = token_address_variants(&token);
            let unwrapped_address = unwrapped_address(&token).ok();
            let legacy_address = legacy_address(&token);
            let symbol = token.symbol.unwrap_or(token.key);
            MarketToken {
                canonical_address: token.address,
                symbol,
                variants,
                unwrapped_address,
                legacy_address,
            }
        })
        .collect::<Vec<_>>();
    result.sort_by_key(|token| token.canonical_address);
    result
}

fn token_registry_error(error: impl std::fmt::Display) -> ApiError {
    tracing::error!(error = %error, "failed to retrieve market price tokens");
    ApiError::Internal("failed to retrieve market price tokens".into())
}

fn token_variant_map(
    tokens: &[MarketToken],
    wrap_ratios: &HashMap<Address, WrapRatioValue>,
) -> Result<HashMap<Address, MarketVariant>, ApiError> {
    let identity = Float::parse("1".to_string()).map_err(float_error)?;
    let mut variants = HashMap::new();

    for token in tokens {
        let ratio = wrap_ratios
            .get(&token.canonical_address)
            .ok_or_else(|| {
                tracing::error!(
                    share_address = %token.canonical_address,
                    "missing market price wrap ratio"
                );
                ApiError::Internal("failed to read wrapped token ratio".into())
            })
            .and_then(|ratio| {
                Float::parse(ratio.assets_per_share.clone()).map_err(|error| {
                    tracing::error!(
                        error = %error,
                        share_address = %token.canonical_address,
                        assets_per_share = %ratio.assets_per_share,
                        "failed to parse market price wrap ratio"
                    );
                    ApiError::Internal("failed to read wrapped token ratio".into())
                })
            })?;

        variants.insert(
            token.canonical_address,
            MarketVariant {
                canonical_address: token.canonical_address,
                price_multiplier: identity,
            },
        );
        if let Some(unwrapped_address) = token
            .unwrapped_address
            .filter(|address| *address != token.canonical_address)
        {
            variants.insert(
                unwrapped_address,
                MarketVariant {
                    canonical_address: token.canonical_address,
                    price_multiplier: ratio,
                },
            );
        }
        if let Some(legacy_address) = token
            .legacy_address
            .filter(|address| *address != token.canonical_address)
        {
            let Some(legacy_ratio) = wrap_ratios.get(&legacy_address) else {
                tracing::warn!(
                    share_address = %legacy_address,
                    canonical_address = %token.canonical_address,
                    "excluding legacy market price variant without a wrap ratio"
                );
                continue;
            };
            let legacy_ratio = match Float::parse(legacy_ratio.assets_per_share.clone()) {
                Ok(legacy_ratio) => legacy_ratio,
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        share_address = %legacy_address,
                        canonical_address = %token.canonical_address,
                        assets_per_share = %legacy_ratio.assets_per_share,
                        "excluding legacy market price variant with an invalid wrap ratio"
                    );
                    continue;
                }
            };
            let price_multiplier = match ratio.div(legacy_ratio) {
                Ok(price_multiplier) => price_multiplier,
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        share_address = %legacy_address,
                        canonical_address = %token.canonical_address,
                        "excluding legacy market price variant with an unusable wrap ratio"
                    );
                    continue;
                }
            };
            variants.insert(
                legacy_address,
                MarketVariant {
                    canonical_address: token.canonical_address,
                    price_multiplier,
                },
            );
        }
    }

    Ok(variants)
}

fn market_token_decimals(market: &PriceMarket) -> HashMap<Address, u8> {
    let mut decimals = market
        .registry_tokens
        .iter()
        .filter(|token| token.network.chain_id == market.chain_id)
        .filter_map(|token| token.decimals.map(|decimals| (token.address, decimals)))
        .collect::<HashMap<_, _>>();
    for token in &market.tokens {
        if let Some(canonical_decimals) = decimals.get(&token.canonical_address).copied() {
            for variant in &token.variants {
                decimals.entry(*variant).or_insert(canonical_decimals);
            }
        }
    }
    decimals
}

async fn fetch_active_market_orders(
    client: &RaindexClient,
    chain_id: u32,
    quote_token: Address,
    variant_map: &HashMap<Address, MarketVariant>,
) -> Result<Vec<RaindexOrder>, ApiError> {
    let mut market_tokens = variant_map.keys().copied().collect::<Vec<_>>();
    market_tokens.push(quote_token);
    market_tokens.sort_unstable();
    market_tokens.dedup();
    let filters = GetOrdersFilters {
        active: Some(true),
        tokens: Some(GetOrdersTokenFilter {
            inputs: Some(market_tokens.clone()),
            outputs: Some(market_tokens),
        }),
        has_positive_output_vault_balance: Some(true),
        ..Default::default()
    };
    let mut orders = Vec::new();
    let mut seen = HashSet::new();
    let mut fetched_count = 0usize;

    for page in 1..=MAX_ORDER_PAGES {
        let result = client
            .get_orders(
                Some(ChainIds(vec![chain_id])),
                Some(filters.clone()),
                Some(page),
                Some(MAX_PAGE_SIZE),
            )
            .await
            .map_err(|error| {
                tracing::error!(
                    chain_id,
                    page,
                    error = %error,
                    "failed to query market price orders"
                );
                ApiError::Internal("failed to query market price orders".into())
            })?;
        let page_orders = result.orders();
        if page_orders.is_empty() {
            break;
        }
        fetched_count = fetched_count.saturating_add(page_orders.len());
        for order in page_orders {
            if order_has_market_pair(order, quote_token, variant_map)
                && seen.insert(order.order_hash())
            {
                orders.push(order.clone());
            }
        }
        if fetched_count >= result.total_count() as usize {
            break;
        }
    }

    Ok(orders)
}

fn order_has_market_pair(
    order: &RaindexOrder,
    quote_token: Address,
    variant_map: &HashMap<Address, MarketVariant>,
) -> bool {
    let Ok(decoded_order) = OrderV4::abi_decode(order.order_bytes().as_ref()) else {
        return false;
    };
    decoded_order.validInputs.iter().any(|input| {
        decoded_order.validOutputs.iter().any(|output| {
            (input.token == quote_token && variant_map.contains_key(&output.token))
                || (output.token == quote_token && variant_map.contains_key(&input.token))
        })
    })
}

fn observed_quotes_from_order(
    order: &RaindexOrder,
    quotes: &[rain_orderbook_common::raindex_client::order_quotes::RaindexOrderQuote],
    quote_token: Address,
    variant_map: &HashMap<Address, MarketVariant>,
    token_decimals: &HashMap<Address, u8>,
) -> Result<Vec<ObservedQuote>, ApiError> {
    let decoded_order = OrderV4::abi_decode(order.order_bytes().as_ref()).map_err(|error| {
        tracing::error!(
            order_hash = %order.order_hash(),
            error = %error,
            "failed to decode order while sampling market prices"
        );
        ApiError::Internal("failed to decode market price order".into())
    })?;
    observed_quotes_from_decoded_order(
        &decoded_order,
        quotes,
        quote_token,
        variant_map,
        token_decimals,
        order.order_hash(),
    )
}

fn observed_quotes_from_decoded_order(
    decoded_order: &OrderV4,
    quotes: &[rain_orderbook_common::raindex_client::order_quotes::RaindexOrderQuote],
    quote_token: Address,
    variant_map: &HashMap<Address, MarketVariant>,
    token_decimals: &HashMap<Address, u8>,
    order_hash: alloy::primitives::B256,
) -> Result<Vec<ObservedQuote>, ApiError> {
    let zero = Float::zero().map_err(float_error)?;
    let relevant_pairs = decoded_order
        .validInputs
        .iter()
        .enumerate()
        .flat_map(|(input_index, input)| {
            decoded_order.validOutputs.iter().enumerate().filter_map(
                move |(output_index, output)| {
                    let is_market_pair = (input.token == quote_token
                        && variant_map.contains_key(&output.token))
                        || (output.token == quote_token && variant_map.contains_key(&input.token));
                    is_market_pair.then_some((input_index, output_index))
                },
            )
        })
        .collect::<HashSet<_>>();
    let mut remaining_pairs = relevant_pairs.clone();
    let mut observations = Vec::new();

    for quote in quotes {
        let pair = (
            quote.pair.input_index as usize,
            quote.pair.output_index as usize,
        );
        if !relevant_pairs.contains(&pair) {
            continue;
        }
        if !remaining_pairs.remove(&pair) {
            tracing::error!(
                order_hash = %order_hash,
                input_index = quote.pair.input_index,
                output_index = quote.pair.output_index,
                "market price sample contains a duplicate relevant quote"
            );
            return Err(ApiError::Internal(
                "failed to quote complete market price book".into(),
            ));
        }
        if !quote.success {
            tracing::warn!(
                order_hash = %order_hash,
                input_index = quote.pair.input_index,
                output_index = quote.pair.output_index,
                error = ?quote.error,
                "excluding unavailable order quote from market price book"
            );
            continue;
        }
        let Some(data) = quote.data.as_ref() else {
            tracing::error!(
                order_hash = %order_hash,
                input_index = quote.pair.input_index,
                output_index = quote.pair.output_index,
                "successful market price quote is missing data"
            );
            return Err(ApiError::Internal(
                "failed to quote complete market price book".into(),
            ));
        };
        if !matches!(data.max_output.gt(zero), Ok(true)) || !matches!(data.ratio.gt(zero), Ok(true))
        {
            continue;
        }
        let Some(input) = decoded_order
            .validInputs
            .get(quote.pair.input_index as usize)
        else {
            continue;
        };
        let Some(output) = decoded_order
            .validOutputs
            .get(quote.pair.output_index as usize)
        else {
            continue;
        };
        let input_address = input.token;
        let output_address = output.token;
        let executable = match (
            token_decimals.get(&input_address),
            token_decimals.get(&output_address),
        ) {
            (Some(input_decimals), Some(output_decimals)) => {
                match has_executable_atomic_amounts(
                    data.max_output,
                    data.ratio,
                    *input_decimals,
                    *output_decimals,
                ) {
                    Ok(executable) => executable,
                    Err(error) => {
                        tracing::warn!(
                            %error,
                            order_hash = %order_hash,
                            input_token = %input_address,
                            output_token = %output_address,
                            "excluding market quote with invalid atomic amounts"
                        );
                        false
                    }
                }
            }
            _ => {
                tracing::warn!(
                    order_hash = %order_hash,
                    input_token = %input_address,
                    output_token = %output_address,
                    "excluding market quote because token decimals are unavailable"
                );
                false
            }
        };
        if !executable {
            tracing::debug!(
                order_hash = %order_hash,
                input_token = %input_address,
                output_token = %output_address,
                "excluding market quote that truncates to zero atomic units"
            );
            continue;
        }

        // A quote only counts toward the displayed price if it can trade a
        // meaningful size: one small far bid/ask must not move the midpoint
        // everyone sees (2 Oct: a $1.50 canary bid dragged wtSGOV ~6%).
        if input_address == quote_token || output_address == quote_token {
            let notional = if output_address == quote_token {
                Ok(data.max_output)
            } else {
                data.max_output.mul(data.ratio)
            };
            let min = Float::parse(MIN_DISPLAY_NOTIONAL_QUOTE.to_string()).map_err(float_error)?;
            match notional.map(|n| n.lt(min)) {
                Ok(Ok(true)) => {
                    tracing::debug!(
                        order_hash = %order_hash,
                        "excluding market quote below the display notional minimum"
                    );
                    continue;
                }
                Ok(Ok(false)) => {}
                _ => continue,
            }
        }

        let observation = if input_address == quote_token {
            match variant_map.get(&output_address) {
                Some(variant) => Some(ObservedQuote {
                    asset_address: variant.canonical_address,
                    side: MarketSide::Ask,
                    price: normalize_variant_price(data.ratio, *variant)?,
                }),
                None => None,
            }
        } else if output_address == quote_token {
            match variant_map.get(&input_address) {
                Some(variant) => Some(ObservedQuote {
                    asset_address: variant.canonical_address,
                    side: MarketSide::Bid,
                    price: normalize_variant_price(data.inverse_ratio, *variant)?,
                }),
                None => None,
            }
        } else {
            None
        };
        if let Some(observation) = observation {
            observations.push(observation);
        }
    }

    if !remaining_pairs.is_empty() {
        tracing::error!(
            order_hash = %order_hash,
            missing_pair_count = remaining_pairs.len(),
            "market price sample is missing relevant quotes"
        );
        return Err(ApiError::Internal(
            "failed to quote complete market price book".into(),
        ));
    }

    Ok(observations)
}

fn normalize_variant_price(price: Float, variant: MarketVariant) -> Result<Float, ApiError> {
    price.mul(variant.price_multiplier).map_err(|error| {
        tracing::error!(
            error = %error,
            asset_token = %variant.canonical_address,
            "failed to normalize market price denomination"
        );
        ApiError::Internal("failed to normalize market price denomination".into())
    })
}

fn aggregate_observations(
    tokens: &[MarketToken],
    observations: &[ObservedQuote],
    chain_id: u32,
    quote_token: Address,
    observed_at: i64,
    wrap_ratios: &HashMap<Address, WrapRatioValue>,
) -> Result<Vec<NewMarketPriceSnapshot>, ApiError> {
    let mut books = BTreeMap::<Address, Book>::new();
    for observation in observations {
        let book = books.entry(observation.asset_address).or_default();
        match observation.side {
            MarketSide::Bid => {
                book.best_bid = Some(match book.best_bid {
                    Some(current) => current.max(observation.price).map_err(float_error)?,
                    None => observation.price,
                });
            }
            MarketSide::Ask => {
                book.best_ask = Some(match book.best_ask {
                    Some(current) => current.min(observation.price).map_err(float_error)?,
                    None => observation.price,
                });
            }
        }
    }

    let two = Float::parse("2".to_string()).map_err(float_error)?;
    let quote_token_address = normalize_address(quote_token);
    let mut snapshots = Vec::new();
    for token in tokens {
        let Some(book) = books.get(&token.canonical_address) else {
            continue;
        };
        let (Some(best_bid), Some(best_ask)) = (book.best_bid, book.best_ask) else {
            continue;
        };
        if best_bid.gt(best_ask).map_err(float_error)? {
            tracing::warn!(
                asset_token = %token.canonical_address,
                "ignoring crossed market price book"
            );
            continue;
        }
        let midpoint = best_bid
            .add(best_ask)
            .and_then(|sum| sum.div(two))
            .map_err(float_error)?;
        let assets_per_share = wrap_ratios
            .get(&token.canonical_address)
            .ok_or_else(|| {
                tracing::error!(
                    share_address = %token.canonical_address,
                    "missing market price snapshot denomination"
                );
                ApiError::Internal("failed to read wrapped token ratio".into())
            })?
            .assets_per_share
            .clone();
        snapshots.push(NewMarketPriceSnapshot {
            chain_id: i64::from(chain_id),
            asset_token_address: normalize_address(token.canonical_address),
            quote_token_address: quote_token_address.clone(),
            best_bid: best_bid.format().map_err(float_error)?,
            best_ask: best_ask.format().map_err(float_error)?,
            midpoint: midpoint.format().map_err(float_error)?,
            assets_per_share,
            observed_at,
        });
    }
    Ok(snapshots)
}

fn float_error(error: rain_math_float::FloatError) -> ApiError {
    tracing::error!(error = %error, "failed to calculate market price");
    ApiError::Internal("failed to calculate market price".into())
}

pub(crate) fn normalize_address(address: Address) -> String {
    format!("{address:#x}").to_ascii_lowercase()
}

pub(crate) fn unix_now() -> Result<i64, ApiError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            tracing::error!(error = %error, "system clock is before Unix epoch");
            ApiError::Internal("failed to read current time".into())
        })?;
    i64::try_from(duration.as_secs())
        .map_err(|_| ApiError::Internal("current timestamp is too large".into()))
}

fn sample_bucket(now: i64, interval: Duration) -> Result<i64, ApiError> {
    let seconds = i64::try_from(interval.as_secs())
        .map_err(|_| ApiError::Internal("market price sample interval is too large".into()))?;
    if seconds == 0 {
        return Err(ApiError::Internal(
            "market price sample interval cannot be zero".into(),
        ));
    }
    Ok(now - now.rem_euclid(seconds))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{address, Bytes, U256};
    use rain_orderbook_app_settings::network::NetworkCfg;
    use rain_orderbook_bindings::IRaindexV6::{EvaluableV4, IOV2};
    use rain_orderbook_common::raindex_client::order_quotes::{
        RaindexOrderQuote, RaindexOrderQuoteValue,
    };
    use serde_json::json;
    use std::sync::Arc;

    const ASSET: Address = address!("1111111111111111111111111111111111111111");
    const QUOTE: Address = address!("2222222222222222222222222222222222222222");
    const ASSET_TWO: Address = address!("3333333333333333333333333333333333333333");

    fn token() -> MarketToken {
        MarketToken {
            canonical_address: ASSET,
            symbol: "wtTEST".to_string(),
            variants: vec![ASSET],
            unwrapped_address: None,
            legacy_address: None,
        }
    }

    fn wrap_ratios() -> HashMap<Address, WrapRatioValue> {
        HashMap::from([(
            ASSET,
            WrapRatioValue {
                share_address: ASSET,
                assets_per_share: "1".to_string(),
            },
        )])
    }

    fn token_decimals() -> HashMap<Address, u8> {
        HashMap::from([(ASSET, 18), (QUOTE, 6)])
    }

    fn single_pair_order(input_token: Address, output_token: Address) -> OrderV4 {
        OrderV4 {
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
        }
    }

    fn observation(side: MarketSide, price: &str) -> ObservedQuote {
        ObservedQuote {
            asset_address: ASSET,
            side,
            price: Float::parse(price.to_string()).expect("valid test price"),
        }
    }

    fn registry_token(chain_id: u32, address: Address, symbol: &str, st0x: bool) -> TokenCfg {
        let mut network = NetworkCfg::dummy();
        network.key = format!("chain-{chain_id}");
        network.chain_id = chain_id;
        let extensions = st0x.then(|| {
            serde_json::from_value(json!({ "category": "ST0x" }))
                .expect("valid registry extensions")
        });
        TokenCfg {
            document: rain_orderbook_app_settings::yaml::default_document(),
            key: format!("{symbol}-{chain_id}"),
            network: Arc::new(network),
            address,
            decimals: Some(18),
            label: None,
            symbol: Some(symbol.to_string()),
            logo_uri: None,
            extensions,
        }
    }

    #[test]
    fn aggregates_best_bid_ask_and_exact_midpoint() {
        let snapshots = aggregate_observations(
            &[token()],
            &[
                observation(MarketSide::Bid, "10"),
                observation(MarketSide::Bid, "11"),
                observation(MarketSide::Ask, "13"),
                observation(MarketSide::Ask, "12"),
            ],
            8453,
            QUOTE,
            120,
            &wrap_ratios(),
        )
        .expect("aggregate observations");

        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].best_bid, "11");
        assert_eq!(snapshots[0].best_ask, "12");
        assert_eq!(snapshots[0].midpoint, "11.5");
    }

    #[test]
    fn requires_two_sides_and_rejects_crossed_books() {
        let one_sided = aggregate_observations(
            &[token()],
            &[observation(MarketSide::Bid, "10")],
            8453,
            QUOTE,
            120,
            &wrap_ratios(),
        )
        .expect("aggregate one-sided book");
        assert!(one_sided.is_empty());

        let crossed = aggregate_observations(
            &[token()],
            &[
                observation(MarketSide::Bid, "12"),
                observation(MarketSide::Ask, "11"),
            ],
            8453,
            QUOTE,
            120,
            &wrap_ratios(),
        )
        .expect("aggregate crossed book");
        assert!(crossed.is_empty());
    }

    #[test]
    fn normalizes_underlying_and_legacy_variants_to_canonical_wrapped_units() {
        let unwrapped = address!("4444444444444444444444444444444444444444");
        let legacy = address!("3333333333333333333333333333333333333333");
        let token = MarketToken {
            canonical_address: ASSET,
            symbol: "wtTEST".to_string(),
            variants: vec![ASSET, unwrapped, legacy],
            unwrapped_address: Some(unwrapped),
            legacy_address: Some(legacy),
        };
        let ratios = HashMap::from([
            (
                ASSET,
                WrapRatioValue {
                    share_address: ASSET,
                    assets_per_share: "2".to_string(),
                },
            ),
            (
                legacy,
                WrapRatioValue {
                    share_address: legacy,
                    assets_per_share: "4".to_string(),
                },
            ),
        ]);
        let variant_map = token_variant_map(&[token], &ratios).expect("variant map");
        let canonical = variant_map.get(&ASSET).expect("canonical variant");
        let unwrapped = variant_map.get(&unwrapped).expect("unwrapped variant");
        let legacy = variant_map.get(&legacy).expect("legacy variant");

        assert_eq!(canonical.canonical_address, ASSET);
        assert_eq!(
            normalize_variant_price(Float::parse("100".to_string()).expect("price"), *canonical)
                .expect("canonical price")
                .format()
                .expect("format"),
            "100"
        );
        assert_eq!(unwrapped.canonical_address, ASSET);
        assert_eq!(
            normalize_variant_price(Float::parse("100".to_string()).expect("price"), *unwrapped)
                .expect("unwrapped price")
                .format()
                .expect("format"),
            "200"
        );
        assert_eq!(legacy.canonical_address, ASSET);
        assert_eq!(
            normalize_variant_price(Float::parse("100".to_string()).expect("price"), *legacy)
                .expect("legacy price")
                .format()
                .expect("format"),
            "50"
        );
    }

    #[test]
    fn excludes_legacy_variant_when_its_wrap_ratio_is_unavailable() {
        let unwrapped = address!("4444444444444444444444444444444444444444");
        let legacy = address!("3333333333333333333333333333333333333333");
        let token = MarketToken {
            canonical_address: ASSET,
            symbol: "wtTEST".to_string(),
            variants: vec![ASSET, unwrapped, legacy],
            unwrapped_address: Some(unwrapped),
            legacy_address: Some(legacy),
        };

        let variant_map =
            token_variant_map(&[token], &wrap_ratios()).expect("canonical ratio is available");

        assert!(variant_map.contains_key(&ASSET));
        assert!(variant_map.contains_key(&unwrapped));
        assert!(!variant_map.contains_key(&legacy));
    }

    #[test]
    fn sample_bucket_is_stable_within_interval() {
        assert_eq!(
            sample_bucket(179, Duration::from_secs(60)).expect("sample bucket"),
            120
        );
    }

    #[test]
    fn incomplete_quote_results_fail_the_market_sample() {
        let result = complete_quote_results(
            vec![
                Ok(1_u8),
                Err(ApiError::Internal("quote transport failed".into())),
            ],
            8453,
        );
        assert!(matches!(
            result,
            Err(ApiError::Internal(message))
                if message == "failed to quote complete market price book"
        ));
    }

    #[test]
    fn failed_relevant_order_quote_is_excluded_from_the_market_book() {
        let order = single_pair_order(QUOTE, ASSET);
        let failed_quote: RaindexOrderQuote = serde_json::from_value(json!({
            "pair": {
                "pairName": "USDC/wtTEST",
                "inputIndex": 0,
                "outputIndex": 0
            },
            "blockNumber": 1,
            "data": null,
            "success": false,
            "error": "quote reverted"
        }))
        .expect("valid failed quote");
        let variant_map = HashMap::from([(
            ASSET,
            MarketVariant {
                canonical_address: ASSET,
                price_multiplier: Float::parse("1".to_string()).expect("valid multiplier"),
            },
        )]);

        let result = observed_quotes_from_decoded_order(
            &order,
            &[failed_quote],
            QUOTE,
            &variant_map,
            &token_decimals(),
            alloy::primitives::B256::ZERO,
        )
        .expect("failed quote is excluded");

        assert!(result.is_empty());
    }

    #[test]
    fn successful_relevant_order_quote_without_data_fails_the_market_sample() {
        let order = single_pair_order(QUOTE, ASSET);
        let incomplete_quote: RaindexOrderQuote = serde_json::from_value(json!({
            "pair": {
                "pairName": "USDC/wtTEST",
                "inputIndex": 0,
                "outputIndex": 0
            },
            "blockNumber": 1,
            "data": null,
            "success": true,
            "error": null
        }))
        .expect("valid incomplete quote");
        let variant_map = HashMap::from([(
            ASSET,
            MarketVariant {
                canonical_address: ASSET,
                price_multiplier: Float::parse("1".to_string()).expect("valid multiplier"),
            },
        )]);

        let result = observed_quotes_from_decoded_order(
            &order,
            &[incomplete_quote],
            QUOTE,
            &variant_map,
            &token_decimals(),
            alloy::primitives::B256::ZERO,
        );

        assert!(matches!(
            result,
            Err(ApiError::Internal(message))
                if message == "failed to quote complete market price book"
        ));
    }

    #[test]
    fn excludes_production_wtmstr_dust_quote_from_market_book() {
        let order = single_pair_order(crate::test_helpers::WT_MSTR, crate::test_helpers::USDC);
        let quote = crate::test_helpers::production_wtmstr_dust_quote();
        let variant_map = HashMap::from([(
            crate::test_helpers::WT_MSTR,
            MarketVariant {
                canonical_address: crate::test_helpers::WT_MSTR,
                price_multiplier: Float::parse("1".to_string()).expect("valid multiplier"),
            },
        )]);
        let mut usdc = registry_token(8453, crate::test_helpers::USDC, "USDC", false);
        usdc.decimals = Some(6);
        let discovery = discover_price_markets(
            vec![
                registry_token(8453, crate::test_helpers::WT_MSTR, "wtMSTR", true),
                usdc,
            ],
            &[8453],
        );
        let token_decimals = market_token_decimals(&discovery.markets[0]);

        let observations = observed_quotes_from_decoded_order(
            &order,
            &[quote],
            crate::test_helpers::USDC,
            &variant_map,
            &token_decimals,
            crate::test_helpers::WT_MSTR_DUST_ORDER_HASH,
        )
        .expect("dust quote is excluded");

        assert!(observations.is_empty());
    }

    #[test]
    fn invalid_atomic_quote_does_not_hide_valid_market_observation() {
        let order = OrderV4 {
            owner: Address::ZERO,
            nonce: U256::ZERO.into(),
            evaluable: EvaluableV4 {
                interpreter: Address::ZERO,
                store: Address::ZERO,
                bytecode: Bytes::new(),
            },
            validInputs: vec![IOV2 {
                token: crate::test_helpers::USDC,
                vaultId: U256::ZERO.into(),
            }],
            validOutputs: vec![
                IOV2 {
                    token: crate::test_helpers::WT_MSTR,
                    vaultId: U256::ZERO.into(),
                },
                IOV2 {
                    token: ASSET_TWO,
                    vaultId: U256::ZERO.into(),
                },
            ],
        };
        let invalid_quote = crate::test_helpers::wtmstr_quote("1e100", "1", "1", "1");
        // 1 token at 20 USDC: above the display notional minimum.
        let mut valid_quote = crate::test_helpers::wtmstr_quote("1", "20", "20", "0.05");
        valid_quote.pair.output_index = 1;
        let one = Float::parse("1".to_string()).expect("valid multiplier");
        let variant_map = HashMap::from([
            (
                crate::test_helpers::WT_MSTR,
                MarketVariant {
                    canonical_address: crate::test_helpers::WT_MSTR,
                    price_multiplier: one,
                },
            ),
            (
                ASSET_TWO,
                MarketVariant {
                    canonical_address: ASSET_TWO,
                    price_multiplier: one,
                },
            ),
        ]);
        let token_decimals = HashMap::from([
            (crate::test_helpers::USDC, 6),
            (crate::test_helpers::WT_MSTR, 18),
            (ASSET_TWO, 18),
        ]);

        let observations = observed_quotes_from_decoded_order(
            &order,
            &[invalid_quote, valid_quote],
            crate::test_helpers::USDC,
            &variant_map,
            &token_decimals,
            alloy::primitives::B256::ZERO,
        )
        .expect("invalid quote is excluded locally");

        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].asset_address, ASSET_TWO);
    }

    fn notional_observations(max_output: &str, ratio: &str, inverse: &str, ask: bool) -> usize {
        let order = if ask {
            single_pair_order(crate::test_helpers::USDC, crate::test_helpers::WT_MSTR)
        } else {
            single_pair_order(crate::test_helpers::WT_MSTR, crate::test_helpers::USDC)
        };
        let quote = crate::test_helpers::wtmstr_quote(max_output, "1", ratio, inverse);
        let variant_map = HashMap::from([(
            crate::test_helpers::WT_MSTR,
            MarketVariant {
                canonical_address: crate::test_helpers::WT_MSTR,
                price_multiplier: Float::parse("1".to_string()).expect("valid multiplier"),
            },
        )]);
        let token_decimals = HashMap::from([
            (crate::test_helpers::USDC, 6),
            (crate::test_helpers::WT_MSTR, 18),
        ]);
        observed_quotes_from_decoded_order(
            &order,
            &[quote],
            crate::test_helpers::USDC,
            &variant_map,
            &token_decimals,
            alloy::primitives::B256::ZERO,
        )
        .expect("observations")
        .len()
    }

    #[test]
    fn small_quotes_do_not_move_the_displayed_price() {
        // Ask: 0.01 token at 100 USDC = 1 USDC -> excluded; 1 token at 100 = 100 USDC -> kept.
        assert_eq!(notional_observations("0.01", "100", "0.01", true), 0);
        assert_eq!(notional_observations("1", "100", "0.01", true), 1);
        // Bid offering 1.5 USDC (the 2 Oct canary rung) -> excluded; 50 USDC -> kept.
        assert_eq!(notional_observations("1.5", "0.0111", "90", false), 0);
        assert_eq!(notional_observations("50", "0.0111", "90", false), 1);
    }

    #[test]
    fn missing_market_token_decimals_exclude_quote() {
        let order = single_pair_order(QUOTE, ASSET);
        let one = Float::parse("1".to_string()).expect("valid quote value");
        let quote = RaindexOrderQuote {
            pair: serde_json::from_value(json!({
                "pairName": "USDC/wtTEST",
                "inputIndex": 0,
                "outputIndex": 0
            }))
            .expect("valid pair"),
            block_number: 0,
            data: Some(RaindexOrderQuoteValue {
                max_output: one,
                formatted_max_output: "1".to_string(),
                max_input: one,
                formatted_max_input: "1".to_string(),
                ratio: one,
                formatted_ratio: "1".to_string(),
                inverse_ratio: one,
                formatted_inverse_ratio: "1".to_string(),
                formatted_max_output_as_percent_of_vault: None,
            }),
            success: true,
            error: None,
            signed_context: Vec::new(),
        };
        let variant_map = HashMap::from([(
            ASSET,
            MarketVariant {
                canonical_address: ASSET,
                price_multiplier: one,
            },
        )]);

        let observations = observed_quotes_from_decoded_order(
            &order,
            &[quote],
            QUOTE,
            &variant_map,
            &HashMap::from([(ASSET, 18)]),
            alloy::primitives::B256::ZERO,
        )
        .expect("missing decimals exclude the quote");

        assert!(observations.is_empty());
    }

    #[test]
    fn malformed_chain_does_not_hide_valid_registry_market() {
        let discovery = discover_price_markets(
            vec![
                registry_token(8453, ASSET, "wtTEST", true),
                registry_token(8453, QUOTE, "USDC", false),
                registry_token(10, ASSET_TWO, "wtOTHER", true),
            ],
            &[10, 8453],
        );

        assert_eq!(discovery.markets.len(), 1);
        assert_eq!(discovery.markets[0].chain_id, 8453);
        assert_eq!(discovery.errors.len(), 1);
        assert_eq!(discovery.errors[0].0, 10);
    }

    #[test]
    fn uses_registry_market_quote_token_when_it_is_not_usdc() {
        let mut quote = registry_token(4663, QUOTE, "USDG", false);
        quote.extensions = Some(HashMap::from([(
            MARKET_QUOTE_EXTENSION.to_string(),
            json!(true),
        )]));

        let discovery = discover_price_markets(
            vec![registry_token(4663, ASSET, "wtTEST", true), quote],
            &[4663],
        );

        assert!(discovery.errors.is_empty());
        assert_eq!(discovery.markets.len(), 1);
        assert_eq!(discovery.markets[0].quote_token_address, QUOTE);
    }

    #[test]
    fn configured_market_quote_takes_precedence_over_usdc_fallback() {
        let mut configured_quote = registry_token(4663, QUOTE, "USDG", false);
        configured_quote.extensions = Some(HashMap::from([(
            MARKET_QUOTE_EXTENSION.to_string(),
            json!(true),
        )]));

        let discovery = discover_price_markets(
            vec![
                registry_token(4663, ASSET, "wtTEST", true),
                configured_quote,
                registry_token(4663, ASSET_TWO, "USDC", false),
            ],
            &[4663],
        );

        assert!(discovery.errors.is_empty());
        assert_eq!(discovery.markets[0].quote_token_address, QUOTE);
    }
}

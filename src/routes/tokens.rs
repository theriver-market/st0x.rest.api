use crate::auth::AuthenticatedKey;
use crate::db::wrapped_exchange_rate_history::{
    count_wrapped_exchange_rate_snapshots_for_share,
    find_latest_wrapped_exchange_rate_snapshot_at_or_before,
    list_wrapped_exchange_rate_snapshots_for_share, WrappedExchangeRateSnapshot,
};
use crate::db::DbPool;
use crate::error::{ApiError, ApiErrorResponse};
use crate::fairings::{GlobalRateLimit, TracingSpan};
use crate::raindex::SharedRaindexProvider;
use crate::types::common::ValidatedAddress;
use crate::wrap_ratio::{
    build_wrap_ratio_response, find_wrap_ratio_item, is_st0x_token,
    persist_wrap_ratio_snapshots_best_effort, read_wrap_ratios_batch, unwrapped_address,
    WrapRatioBatchInput, WrapRatioMetadata, WrapRatioResponse,
};
use alloy::primitives::Address;
use futures::StreamExt;
use rain_orderbook_app_settings::token::TokenCfg;
use rain_orderbook_common::raindex_client::RaindexError;
use rocket::form::FromForm;
use rocket::serde::json::Json;
use rocket::{Route, State};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use time::{macros::format_description, Date};
use tracing::Instrument;
use utoipa::{IntoParams, ToSchema};

#[path = "token_details.rs"]
mod token_details;

pub use token_details::*;

pub(super) const SFT_PAGE_SIZE: usize = 1000;
const SUBGRAPH_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const SUBGRAPH_MAX_ATTEMPTS: usize = 3;
const SUBGRAPH_RETRY_BACKOFF: Duration = Duration::from_millis(250);
const SUBGRAPH_ERROR_BODY_LOG_LIMIT: u64 = 2_048;

#[derive(Debug, Serialize)]
pub struct TokenResponse {
    #[serde(flatten)]
    token: TokenCfg,
    name: Option<String>,
    isin: Option<String>,
}

#[derive(Debug, Clone, Default, FromForm, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
#[serde(rename_all = "camelCase")]
pub struct TokenChainParams {
    #[field(name = "chainId")]
    #[param(example = 8453)]
    chain_id: Option<u32>,
}

#[derive(Debug, Clone, Default, FromForm, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
#[serde(rename_all = "camelCase")]
pub struct TokenListParams {
    #[field(name = "chainId")]
    #[param(example = 8453)]
    chain_id: Option<u32>,
}

#[derive(Debug, Clone, Default, FromForm, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
#[serde(rename_all = "camelCase")]
pub struct WrapRatioListParams {
    #[field(name = "chainId")]
    #[param(example = 8453)]
    chain_id: Option<u32>,
    /// UTC calendar date. Returns the latest stored ratio at or before 23:59:59 UTC.
    #[param(example = "2026-09-24")]
    date: Option<String>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct WrapRatioErrorResponse {
    #[schema(example = 8453)]
    chain_id: u32,
    #[schema(value_type = String, example = "0xff05e1bd696900dc6a52ca35ca61bb1024eda8e2")]
    share_address: Address,
    #[schema(example = "failed to read ERC4626 ratio")]
    message: String,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct WrapRatioBatchResponse {
    data: Vec<WrapRatioResponse>,
    errors: Vec<WrapRatioErrorResponse>,
}

#[derive(Debug, Clone, FromForm, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
#[serde(rename_all = "camelCase")]
pub struct WrapRatioHistoryParams {
    #[field(name = "chainId")]
    #[param(example = 8453)]
    chain_id: Option<u32>,
    #[field(name = "page")]
    #[param(example = 1)]
    page: Option<u32>,
    #[field(name = "pageSize")]
    #[param(example = 20)]
    page_size: Option<u32>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct WrapRatioHistoryResponse {
    #[schema(example = 8453)]
    chain_id: u32,
    #[schema(value_type = String, example = "0xff05e1bd696900dc6a52ca35ca61bb1024eda8e2")]
    share_address: Address,
    #[schema(value_type = String, example = "0x013b782f402d61aa1004cca95b9f5bb402c9d5fe")]
    asset_address: Address,
    events: Vec<WrapRatioHistorySnapshotEvent>,
    pagination: WrapRatioHistoryPagination,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct WrapRatioHistorySnapshotEvent {
    #[serde(rename = "type")]
    #[schema(example = "snapshot")]
    event_type: String,
    #[schema(example = 123)]
    block_number: u64,
    #[schema(nullable = true, example = 456)]
    block_timestamp: Option<u64>,
    #[schema(example = "1.0027")]
    assets_per_share: String,
    #[schema(example = "1781506371")]
    captured_at: String,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct WrapRatioHistoryPagination {
    #[schema(example = 1)]
    page: u32,
    #[schema(example = 20)]
    page_size: u32,
    #[schema(example = 42)]
    total_events: u64,
    #[schema(example = 3)]
    total_pages: u64,
    #[schema(example = true)]
    has_more: bool,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TokenProofsResponse {
    #[schema(example = 8453)]
    chain_id: u32,
    #[schema(value_type = String, example = "0xff05e1bd696900dc6a52ca35ca61bb1024eda8e2")]
    address: Address,
    metadata: Vec<TokenProofMetadata>,
    schemas: Vec<TokenProofSchema>,
    receipts: Vec<TokenProofReceipt>,
}

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TokenProofMetadata {
    id: String,
    meta: String,
    sender: String,
    subject: String,
    #[schema(example = "0x1234")]
    meta_hash: String,
}

#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct TokenProofSchema {
    id: String,
    information: String,
    timestamp: u64,
}

#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct TokenProofReceipt {
    id: String,
    #[serde(rename = "receiptId")]
    receipt_id: String,
    #[serde(rename = "txHash")]
    tx_hash: String,
    #[serde(rename = "type")]
    receipt_type: TokenProofReceiptType,
    information: String,
    timestamp: u64,
}

#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum TokenProofReceiptType {
    Deposit,
    Withdraw,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct TokenNetworkResponseSchema {
    /// Network identifier from the registry settings.
    key: String,
    /// RPC URLs are intentionally stripped from public token responses.
    rpcs: Vec<String>,
    /// EVM chain ID.
    chain_id: u32,
    /// Human-readable network label, when configured.
    label: Option<String>,
    /// Optional network ID from the registry settings.
    network_id: Option<u32>,
    /// Native currency symbol.
    currency: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
struct TokenResponseSchema {
    /// Token key from the registry settings.
    key: String,
    /// Sanitized network metadata for the token.
    network: TokenNetworkResponseSchema,
    /// Token contract address.
    address: String,
    /// Token decimals, when known.
    decimals: Option<u8>,
    /// Human-readable token label.
    label: Option<String>,
    /// Token symbol.
    symbol: Option<String>,
    /// Token logo URI, when available.
    logo_uri: Option<String>,
    /// Token list metadata extensions, when available.
    #[schema(value_type = Object)]
    extensions: Option<Value>,
    /// Convenience display name derived from the token label.
    name: Option<String>,
    /// ISIN identifier from token extensions, when available.
    isin: Option<String>,
}

impl From<TokenCfg> for TokenResponse {
    fn from(mut token: TokenCfg) -> Self {
        let name = token.label.clone();
        let isin = token
            .extensions
            .as_ref()
            .and_then(|extensions| extract_extension_string(extensions, "isin"))
            .or_else(|| {
                token
                    .extensions
                    .as_ref()
                    .and_then(|extensions| extract_extension_string(extensions, "ISIN"))
            });

        token.network = Arc::new(sanitize_network(token.network.as_ref()));

        Self { token, name, isin }
    }
}

fn sanitize_network(
    network: &rain_orderbook_app_settings::network::NetworkCfg,
) -> rain_orderbook_app_settings::network::NetworkCfg {
    let mut network = network.clone();
    network.rpcs.clear();
    network
}

fn extract_extension_string(
    extensions: &std::collections::HashMap<String, Value>,
    key: &str,
) -> Option<String> {
    match extensions.get(key) {
        Some(Value::String(value)) => Some(value.clone()),
        Some(Value::Null) | None => None,
        Some(value) => Some(value.to_string()),
    }
}

fn token_lookup_error(error: RaindexError) -> ApiError {
    tracing::error!(error = %error, "failed to get tokens from raindex");
    ApiError::Internal("failed to retrieve token list".into())
}

pub(super) async fn registry_tokens(
    shared_raindex: &State<SharedRaindexProvider>,
) -> Result<Vec<TokenCfg>, ApiError> {
    let raindex = shared_raindex.read().await;
    let tokens = raindex
        .client()
        .get_all_tokens()
        .map_err(token_lookup_error)?
        .into_values()
        .collect();
    Ok(tokens)
}

fn normalize_address(address: Address) -> String {
    format!("{address:#x}").to_ascii_lowercase()
}

fn wrap_ratio_date_cutoff(date: &str) -> Result<i64, ApiError> {
    let date = Date::parse(date, format_description!("[year]-[month]-[day]"))
        .map_err(|_| ApiError::BadRequest("date must use YYYY-MM-DD format".into()))?;
    let end_of_day = date
        .with_hms(23, 59, 59)
        .map_err(|_| ApiError::BadRequest("date must use YYYY-MM-DD format".into()))?;
    Ok(end_of_day.assume_utc().unix_timestamp())
}

fn wrap_ratio_response_from_snapshot(
    token: &TokenCfg,
    snapshot: WrappedExchangeRateSnapshot,
) -> Result<WrapRatioResponse, String> {
    let chain_id = u32::try_from(snapshot.chain_id)
        .map_err(|_| "stored wrapped token ratio has an invalid chain ID".to_string())?;
    if chain_id != token.network.chain_id {
        return Err("stored wrapped token ratio has an unexpected chain ID".to_string());
    }

    let share_address = snapshot
        .share_token_address
        .parse::<Address>()
        .map_err(|_| "stored wrapped token ratio has an invalid share address".to_string())?;
    if share_address != token.address {
        return Err("stored wrapped token ratio has an unexpected share address".to_string());
    }

    let asset_address = snapshot
        .asset_token_address
        .parse::<Address>()
        .map_err(|_| "stored wrapped token ratio has an invalid asset address".to_string())?;
    let expected_asset_address = unwrapped_address(token).map_err(|error| error.batch_message())?;
    if asset_address != expected_asset_address {
        return Err("stored wrapped token ratio does not match registry asset".to_string());
    }

    Ok(WrapRatioResponse {
        chain_id,
        share_address,
        asset_address,
        assets_per_share: snapshot.assets_per_share,
        block_number: snapshot
            .block_number
            .try_into()
            .map_err(|_| "stored wrapped token ratio has an invalid block number".to_string())?,
        block_timestamp: snapshot
            .block_timestamp
            .map(u64::try_from)
            .transpose()
            .map_err(|_| "stored wrapped token ratio has an invalid block timestamp".to_string())?,
        captured_at: snapshot.captured_at,
    })
}

async fn historical_wrap_ratios(
    pool: &DbPool,
    tokens: &[&TokenCfg],
    cutoff: i64,
) -> Result<WrapRatioBatchResponse, ApiError> {
    let mut data = Vec::new();
    let mut errors = Vec::new();

    for token in tokens {
        let share_token_address = normalize_address(token.address);
        let snapshot = find_latest_wrapped_exchange_rate_snapshot_at_or_before(
            pool,
            token.network.chain_id,
            &share_token_address,
            cutoff,
        )
        .await
        .map_err(|error| {
            tracing::error!(
                error = %error,
                cutoff,
                chain_id = token.network.chain_id,
                share_address = %token.address,
                "failed to query dated wrapped token ratio"
            );
            ApiError::Internal("failed to query wrapped token ratios".into())
        })?;
        let Some(snapshot) = snapshot else {
            errors.push(WrapRatioErrorResponse {
                chain_id: token.network.chain_id,
                share_address: token.address,
                message: "no wrapped token ratio snapshot available at or before requested date"
                    .into(),
            });
            continue;
        };

        match wrap_ratio_response_from_snapshot(token, snapshot) {
            Ok(response) => data.push(response),
            Err(message) => {
                tracing::error!(
                    chain_id = token.network.chain_id,
                    share_address = %token.address,
                    reason = %message,
                    "invalid stored wrapped token ratio"
                );
                errors.push(WrapRatioErrorResponse {
                    chain_id: token.network.chain_id,
                    share_address: token.address,
                    message: "invalid stored wrapped token ratio".into(),
                });
            }
        }
    }

    Ok(WrapRatioBatchResponse { data, errors })
}

fn wrap_ratio_history_pagination_params(
    params: WrapRatioHistoryParams,
) -> Result<(u32, u32, u32), ApiError> {
    let page = params.page.unwrap_or(1);
    if page == 0 {
        return Err(ApiError::BadRequest("page must be greater than 0".into()));
    }

    let page_size = params.page_size.unwrap_or(20).min(100);
    if page_size == 0 {
        return Err(ApiError::BadRequest(
            "pageSize must be greater than 0".into(),
        ));
    }

    let offset = page
        .checked_sub(1)
        .and_then(|page_index| page_index.checked_mul(page_size))
        .ok_or_else(|| ApiError::BadRequest("pagination offset value too large".into()))?;

    Ok((page, page_size, offset))
}

fn build_wrap_ratio_history_pagination(
    total_events: u64,
    page: u32,
    page_size: u32,
) -> WrapRatioHistoryPagination {
    let total_pages = total_events.div_ceil(page_size as u64);
    WrapRatioHistoryPagination {
        page,
        page_size,
        total_events,
        total_pages,
        has_more: (page as u64) < total_pages,
    }
}

fn wrap_ratio_history_event_from_snapshot(
    snapshot: WrappedExchangeRateSnapshot,
) -> Result<WrapRatioHistorySnapshotEvent, ApiError> {
    Ok(WrapRatioHistorySnapshotEvent {
        event_type: "snapshot".to_string(),
        block_number: snapshot
            .block_number
            .try_into()
            .map_err(|_| ApiError::Internal("block number overflow".into()))?,
        block_timestamp: snapshot
            .block_timestamp
            .map(u64::try_from)
            .transpose()
            .map_err(|_| ApiError::Internal("block timestamp overflow".into()))?,
        assets_per_share: snapshot.assets_per_share,
        captured_at: snapshot.captured_at,
    })
}

pub(super) fn matches_token_proof_address(token: &TokenCfg, address: Address) -> bool {
    crate::wrap_ratio::token_address_variants(token).contains(&address)
}

fn select_st0x_token<'a>(
    tokens: &'a [TokenCfg],
    requested_address: Address,
    requested_chain_id: Option<u32>,
    matches_address: impl Fn(&TokenCfg) -> bool,
    not_found_message: &'static str,
) -> Result<&'a TokenCfg, ApiError> {
    let mut matches = tokens.iter().filter(|token| {
        is_st0x_token(token)
            && requested_chain_id.is_none_or(|chain_id| token.network.chain_id == chain_id)
            && matches_address(token)
    });
    let Some(token) = matches.next() else {
        tracing::warn!(address = %requested_address, ?requested_chain_id, "ST0x token not found");
        return Err(ApiError::NotFound(not_found_message.into()));
    };

    if requested_chain_id.is_none() && matches.next().is_some() {
        tracing::warn!(address = %requested_address, "token address is ambiguous across networks");
        return Err(ApiError::BadRequest(
            "chainId is required when an address exists on multiple networks".into(),
        ));
    }

    Ok(token)
}

async fn validated_requested_chain_id(
    shared_raindex: &State<SharedRaindexProvider>,
    requested_chain_id: Option<u32>,
) -> Result<Option<u32>, ApiError> {
    let raindex = shared_raindex.read().await;
    requested_chain_id
        .map(|chain_id| crate::routes::validate_chain_id(raindex.raindex_yaml(), chain_id))
        .transpose()
}

fn resolve_proof_subgraph_urls(
    yaml: &rain_orderbook_app_settings::yaml::raindex::RaindexYaml,
    network_key: &str,
) -> Result<(String, String), ApiError> {
    let sft_subgraph = resolve_sft_subgraph_url(yaml, network_key)?;
    let metaboard = yaml.get_metaboard(network_key).map_err(|error| {
        tracing::warn!(
            network_key,
            error = %error,
            "token proofs are not supported on this network"
        );
        ApiError::BadRequest("token proofs are not supported on this chain".into())
    })?;
    tracing::info!(
        network_key,
        metaboard_key = network_key,
        "resolved metadata subgraph"
    );

    Ok((sft_subgraph, metaboard.url.to_string()))
}

pub(super) fn resolve_sft_subgraph_url(
    yaml: &rain_orderbook_app_settings::yaml::raindex::RaindexYaml,
    network_key: &str,
) -> Result<String, ApiError> {
    // Convention: the SFT subgraph is configured separately from the raindex
    // subgraph as `subgraphs.sft-{network}` (for example, `sft-base`).
    let key = format!("sft-{network_key}");
    if let Ok(subgraph) = yaml.get_subgraph(&key) {
        tracing::info!(network_key, sft_subgraph_key = %key, "resolved SFT subgraph");
        return Ok(subgraph.url.to_string());
    }

    tracing::error!(network_key, sft_subgraph_key = %key, "failed to resolve SFT subgraph");
    Err(ApiError::Internal("SFT subgraph is not configured".into()))
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub(super) enum TimestampValue {
    Number(u64),
    String(String),
}

impl TimestampValue {
    pub(super) fn parse(&self) -> Result<u64, ApiError> {
        match self {
            TimestampValue::Number(value) => Ok(*value),
            TimestampValue::String(value) => value.parse::<u64>().map_err(|error| {
                tracing::error!(value, error = %error, "invalid timestamp from subgraph");
                ApiError::Internal("invalid timestamp from subgraph".into())
            }),
        }
    }
}

#[derive(Debug, Deserialize)]
struct GraphqlResponse<T> {
    data: Option<T>,
    errors: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SftProofsData {
    #[serde(default)]
    offchain_asset_receipt_vaults: Vec<SftVault>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SftVault {
    #[serde(default)]
    receipt_vault_informations: Vec<SftReceiptVaultInformation>,
    #[serde(default)]
    deposits: Vec<SftReceiptEvent>,
    #[serde(default)]
    withdraws: Vec<SftReceiptEvent>,
}

#[derive(Debug, Deserialize)]
struct SftReceiptVaultInformation {
    id: String,
    information: String,
    timestamp: TimestampValue,
}

#[derive(Debug, Deserialize)]
struct SftReceiptEvent {
    timestamp: TimestampValue,
    transaction: Option<SftTransaction>,
    receipt: Option<SftReceipt>,
}

#[derive(Debug, Clone, Deserialize)]
struct SftTransaction {
    id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SftReceipt {
    receipt_id: String,
    #[serde(default)]
    receipt_informations: Vec<SftReceiptInformation>,
}

#[derive(Debug, Deserialize)]
struct SftReceiptInformation {
    id: String,
    information: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MetadataProofsData {
    #[serde(default)]
    meta_v1_s: Vec<TokenProofMetadata>,
}

pub(super) async fn post_graphql<T: for<'de> Deserialize<'de>>(
    url: &str,
    query: &str,
    variables: Value,
) -> Result<T, ApiError> {
    let client = reqwest::Client::new();
    let payload = json!({
        "query": query,
        "variables": variables,
    });
    let mut last_error = None;

    for attempt in 1..=SUBGRAPH_MAX_ATTEMPTS {
        let response = match client
            .post(url)
            .timeout(SUBGRAPH_REQUEST_TIMEOUT)
            .json(&payload)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                tracing::warn!(
                    url,
                    attempt,
                    max_attempts = SUBGRAPH_MAX_ATTEMPTS,
                    error = %error,
                    "failed to query subgraph"
                );
                last_error = Some(ApiError::Internal("failed to query subgraph".into()));
                if attempt < SUBGRAPH_MAX_ATTEMPTS {
                    tokio::time::sleep(SUBGRAPH_RETRY_BACKOFF * attempt as u32).await;
                    continue;
                }
                break;
            }
        };

        let status = response.status();
        if status.is_success() {
            let body = response
                .json::<GraphqlResponse<T>>()
                .await
                .map_err(|error| {
                    tracing::error!(url, error = %error, "failed to decode subgraph response");
                    ApiError::Internal("failed to decode subgraph response".into())
                })?;

            if let Some(errors) = body.errors {
                tracing::error!(url, errors = %errors, "subgraph returned GraphQL errors");
                return Err(ApiError::Internal(
                    "subgraph returned GraphQL errors".into(),
                ));
            }

            return body.data.ok_or_else(|| {
                tracing::error!(url, "subgraph response missing data");
                ApiError::Internal("subgraph response missing data".into())
            });
        }

        let status_code = status.as_u16();
        let should_retry = status == reqwest::StatusCode::REQUEST_TIMEOUT
            || status == reqwest::StatusCode::TOO_MANY_REQUESTS
            || status.is_server_error();
        let body = read_limited_response_body(response, SUBGRAPH_ERROR_BODY_LOG_LIMIT).await;
        tracing::warn!(
            url,
            status = status_code,
            attempt,
            max_attempts = SUBGRAPH_MAX_ATTEMPTS,
            retryable = should_retry,
            body_snippet = %body,
            "subgraph returned error status"
        );
        last_error = Some(ApiError::Internal("subgraph returned error status".into()));
        if should_retry && attempt < SUBGRAPH_MAX_ATTEMPTS {
            tokio::time::sleep(SUBGRAPH_RETRY_BACKOFF * attempt as u32).await;
        } else {
            break;
        }
    }

    Err(last_error.unwrap_or_else(|| ApiError::Internal("failed to query subgraph".into())))
}

async fn read_limited_response_body(response: reqwest::Response, limit: u64) -> String {
    let max_bytes = limit.saturating_add(1) as usize;
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();

    while bytes.len() < max_bytes {
        let Some(chunk) = stream.next().await else {
            break;
        };
        match chunk {
            Ok(chunk) => {
                let remaining = max_bytes - bytes.len();
                bytes.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
            }
            Err(error) => {
                tracing::warn!(error = %error, "failed to read subgraph error body");
                break;
            }
        }
    }

    let was_truncated = bytes.len() as u64 > limit;
    bytes.truncate(limit as usize);
    let mut body = String::from_utf8_lossy(&bytes).into_owned();
    if was_truncated {
        body.push_str("...[truncated]");
    }
    body
}

fn build_token_proofs_response(
    chain_id: u32,
    address: Address,
    sft: SftProofsData,
    metadata: MetadataProofsData,
) -> Result<TokenProofsResponse, ApiError> {
    let Some(vault) = sft.offchain_asset_receipt_vaults.first() else {
        tracing::warn!(address = %address, "SFT vault not found for token");
        return Err(ApiError::NotFound("SFT vault not found for token".into()));
    };

    let schemas = vault
        .receipt_vault_informations
        .iter()
        .map(|schema| {
            Ok(TokenProofSchema {
                id: schema.id.clone(),
                information: schema.information.clone(),
                timestamp: schema.timestamp.parse()?,
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;

    let mut receipts = flatten_receipt_events(&vault.deposits, TokenProofReceiptType::Deposit)?;
    receipts.extend(flatten_receipt_events(
        &vault.withdraws,
        TokenProofReceiptType::Withdraw,
    )?);

    Ok(TokenProofsResponse {
        chain_id,
        address,
        metadata: metadata.meta_v1_s,
        schemas,
        receipts,
    })
}

fn flatten_receipt_events(
    events: &[SftReceiptEvent],
    receipt_type: TokenProofReceiptType,
) -> Result<Vec<TokenProofReceipt>, ApiError> {
    let mut rows = Vec::new();

    for event in events {
        let timestamp = event.timestamp.parse()?;
        let Some(transaction) = event.transaction.as_ref() else {
            tracing::error!("receipt event missing transaction");
            return Err(ApiError::Internal(
                "receipt event missing transaction".into(),
            ));
        };
        let Some(receipt) = event.receipt.as_ref() else {
            tracing::error!("receipt event missing receipt");
            return Err(ApiError::Internal("receipt event missing receipt".into()));
        };

        for information in &receipt.receipt_informations {
            rows.push(TokenProofReceipt {
                id: information.id.clone(),
                receipt_id: receipt.receipt_id.clone(),
                tx_hash: transaction.id.clone(),
                receipt_type: receipt_type.clone(),
                information: information.information.clone(),
                timestamp,
            });
        }
    }

    Ok(rows)
}

pub(super) fn api_error_message(error: &ApiError) -> String {
    match error {
        ApiError::BadRequest(message)
        | ApiError::Unauthorized(message)
        | ApiError::Forbidden(message)
        | ApiError::NotFound(message)
        | ApiError::Internal(message)
        | ApiError::RateLimited(message)
        | ApiError::NotYetIndexed(message) => message.clone(),
        ApiError::MarketClosed { .. } => crate::error::MARKET_CLOSED_MESSAGE.to_string(),
        ApiError::Coded { public_message, .. } => (*public_message).to_string(),
    }
}

async fn read_token_proofs(
    chain_id: u32,
    address: Address,
    sft_subgraph_url: &str,
    metadata_subgraph_url: &str,
) -> Result<TokenProofsResponse, ApiError> {
    const SFT_QUERY: &str = r#"
query TokenProofs($address: String!) {
  offchainAssetReceiptVaults(where: { wrappedTokenContractAddress: $address }) {
    id
    address
    receiptVaultInformations(orderBy: timestamp, orderDirection: desc) {
      id
      information
      timestamp
    }
    deposits {
      id
      timestamp
      transaction { id }
      receipt {
        id
        receiptId
        receiptInformations { information id }
      }
    }
    withdraws {
      id
      timestamp
      transaction { id }
      receipt {
        id
        receiptId
        receiptInformations { information id }
      }
    }
  }
}
"#;
    const METADATA_QUERY: &str = r#"
query TokenMetadata($subject: String!) {
  metaV1S(
    where: { subject: $subject },
    orderBy: transaction__timestamp,
    orderDirection: desc
  ) {
    id
    meta
    sender
    subject
    metaHash
  }
}
"#;

    let address_lower = format!("{address:#x}");
    let subject = format!(
        "0x000000000000000000000000{}",
        address_lower.trim_start_matches("0x")
    );

    let (sft, metadata) = tokio::try_join!(
        post_graphql::<SftProofsData>(
            sft_subgraph_url,
            SFT_QUERY,
            json!({ "address": address_lower }),
        ),
        post_graphql::<MetadataProofsData>(
            metadata_subgraph_url,
            METADATA_QUERY,
            json!({ "subject": subject }),
        ),
    )?;

    build_token_proofs_response(chain_id, address, sft, metadata)
}

#[utoipa::path(
    get,
    path = "/v2/tokens",
    tag = "Tokens",
    security(("basicAuth" = [])),
    params(TokenListParams),
    responses(
        (
            status = 200,
            description = "List of supported tokens",
            body = Vec<TokenResponseSchema>,
            example = json!([
                {
                    "key": "usdc",
                    "network": {
                        "key": "base",
                        "rpcs": [],
                        "chainId": 8453,
                        "label": "Base",
                        "networkId": null,
                        "currency": "ETH"
                    },
                    "address": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
                    "decimals": 6,
                    "label": "USD Coin",
                    "symbol": "USDC",
                    "logo-uri": null,
                    "extensions": {
                        "isin": "US0000000001"
                    },
                    "name": "USD Coin",
                    "isin": "US0000000001"
                }
            ])
        ),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 429, description = "Rate limited", body = ApiErrorResponse),
        (status = 500, description = "Internal server error", body = ApiErrorResponse),
    )
)]
#[get("/?<params..>")]
pub async fn get_tokens(
    _global: GlobalRateLimit,
    _key: AuthenticatedKey,
    span: TracingSpan,
    shared_raindex: &State<SharedRaindexProvider>,
    params: TokenListParams,
) -> Result<Json<Vec<TokenResponse>>, ApiError> {
    let chain_id = crate::routes::compatibility_chain_id(span.api_version(), params.chain_id);
    async move {
        tracing::info!(params = ?params, "request received");

        let raindex = shared_raindex.read().await;
        let chain_ids = crate::routes::optional_chain_ids_filter(raindex.raindex_yaml(), chain_id)?;
        let tokens = raindex
            .client()
            .get_all_tokens()
            .map_err(|e: RaindexError| {
                tracing::error!(error = %e, "failed to get tokens from raindex");
                ApiError::Internal("failed to retrieve token list".into())
            })?;

        let result: Vec<TokenResponse> = tokens
            .into_values()
            .filter(|token| {
                chain_ids
                    .as_ref()
                    .is_none_or(|chain_ids| chain_ids.contains(&token.network.chain_id))
            })
            .map(TokenResponse::from)
            .collect();
        tracing::info!(count = result.len(), "returning tokens");
        Ok(Json(result))
    }
    .instrument(span.0)
    .await
}

#[utoipa::path(
    get,
    path = "/v2/tokens/wrap-ratio",
    tag = "Tokens",
    security(("basicAuth" = [])),
    params(WrapRatioListParams),
    responses(
        (status = 200, description = "Wrapped ST0x token ratios", body = WrapRatioBatchResponse),
        (status = 400, description = "Unsupported chainId or invalid date", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 429, description = "Rate limited", body = ApiErrorResponse),
        (status = 500, description = "Internal server error", body = ApiErrorResponse),
    )
)]
#[get("/wrap-ratio?<params..>")]
pub async fn get_wrap_ratios(
    _global: GlobalRateLimit,
    _key: AuthenticatedKey,
    span: TracingSpan,
    shared_raindex: &State<SharedRaindexProvider>,
    pool: &State<DbPool>,
    params: WrapRatioListParams,
) -> Result<Json<WrapRatioBatchResponse>, ApiError> {
    let chain_id = crate::routes::compatibility_chain_id(span.api_version(), params.chain_id);
    let cutoff = params
        .date
        .as_deref()
        .map(wrap_ratio_date_cutoff)
        .transpose()?;
    async move {
        tracing::info!(params = ?params, "request received");

        let tokens = registry_tokens(shared_raindex).await?;
        let chain_ids = {
            let raindex = shared_raindex.read().await;
            crate::routes::optional_chain_ids_filter(raindex.raindex_yaml(), chain_id)?
        };
        let st0x_tokens: Vec<&TokenCfg> = tokens
            .iter()
            .filter(|token| {
                is_st0x_token(token)
                    && chain_ids
                        .as_ref()
                        .is_none_or(|chain_ids| chain_ids.contains(&token.network.chain_id))
            })
            .collect();

        if let Some(cutoff) = cutoff {
            tracing::info!(
                count = st0x_tokens.len(),
                cutoff,
                "reading dated wrapped token ratios"
            );
            let response = historical_wrap_ratios(pool.inner(), &st0x_tokens, cutoff).await?;
            tracing::info!(
                data_count = response.data.len(),
                error_count = response.errors.len(),
                "returning dated wrapped token ratios"
            );
            return Ok(Json(response));
        }

        tracing::info!(count = st0x_tokens.len(), "reading wrapped token ratios");

        let mut data = Vec::new();
        let mut errors = Vec::new();
        let mut inputs = Vec::new();

        for token in st0x_tokens {
            match unwrapped_address(token) {
                Ok(expected_asset_address) => inputs.push(WrapRatioBatchInput {
                    token,
                    share_address: token.address,
                    expected_asset_address,
                }),
                Err(error) => {
                    tracing::error!(
                        share_address = %token.address,
                        error = %error,
                        "failed to read wrapped token ratio"
                    );
                    errors.push(WrapRatioErrorResponse {
                        chain_id: token.network.chain_id,
                        share_address: token.address,
                        message: error.batch_message(),
                    });
                }
            }
        }

        for group in read_wrap_ratios_batch(&inputs).await {
            let response = match group.response {
                Ok(response) => response,
                Err(_) => {
                    for index in group.input_indices {
                        let Some(input) = inputs.get(index) else {
                            continue;
                        };
                        errors.push(WrapRatioErrorResponse {
                            chain_id: input.token.network.chain_id,
                            share_address: input.share_address,
                            message: "failed to read ERC4626 ratio".into(),
                        });
                    }
                    continue;
                }
            };
            let metadata = WrapRatioMetadata::from_batch_response(&response);

            if response.items.len() != group.input_indices.len() {
                tracing::error!(
                    expected = group.input_indices.len(),
                    actual = response.items.len(),
                    "ERC4626 batch response item count mismatch"
                );
            }

            for index in group.input_indices {
                let Some(input) = inputs.get(index) else {
                    continue;
                };

                let row =
                    find_wrap_ratio_item(&response.items, input.share_address).and_then(|item| {
                        build_wrap_ratio_response(
                            input.token.network.chain_id,
                            item,
                            input.expected_asset_address,
                            &metadata,
                        )
                    });

                match row {
                    Ok(row) => data.push(row),
                    Err(error) => {
                        tracing::error!(
                            share_address = %input.share_address,
                            error = %error,
                            "failed to read wrapped token ratio"
                        );
                        errors.push(WrapRatioErrorResponse {
                            chain_id: input.token.network.chain_id,
                            share_address: input.share_address,
                            message: error.batch_message(),
                        });
                    }
                }
            }
        }

        persist_wrap_ratio_snapshots_best_effort(pool.inner(), &data).await;

        tracing::info!(
            data_count = data.len(),
            error_count = errors.len(),
            "returning wrapped token ratios"
        );
        Ok(Json(WrapRatioBatchResponse { data, errors }))
    }
    .instrument(span.0)
    .await
}

#[utoipa::path(
    get,
    path = "/v2/tokens/wrap-ratio/{address}",
    tag = "Tokens",
    security(("basicAuth" = [])),
    params(
        ("address" = String, Path, description = "Wrapped token / ERC4626 vault address"),
        TokenChainParams
    ),
    responses(
        (status = 200, description = "Wrapped token ratio", body = WrapRatioResponse),
        (status = 400, description = "Ambiguous token address or unsupported chainId", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Wrapped ST0x token not found", body = ApiErrorResponse),
        (status = 422, description = "Invalid wrapped token address", body = ApiErrorResponse),
        (status = 429, description = "Rate limited", body = ApiErrorResponse),
        (status = 500, description = "Internal server error", body = ApiErrorResponse),
    )
)]
#[get("/wrap-ratio/<address>?<params..>")]
pub async fn get_wrap_ratio_by_address(
    _global: GlobalRateLimit,
    _key: AuthenticatedKey,
    span: TracingSpan,
    shared_raindex: &State<SharedRaindexProvider>,
    pool: &State<DbPool>,
    address: ValidatedAddress,
    params: TokenChainParams,
) -> Result<Json<WrapRatioResponse>, ApiError> {
    let requested_chain_id =
        crate::routes::compatibility_chain_id(span.api_version(), params.chain_id);
    async move {
        tracing::info!(share_address = %address.0, "request received");

        let chain_id = validated_requested_chain_id(shared_raindex, requested_chain_id).await?;
        let tokens = registry_tokens(shared_raindex).await?;
        let token = select_st0x_token(
            &tokens,
            address.0,
            chain_id,
            |token| token.address == address.0,
            "wrapped ST0x token not found",
        )?;

        let expected_asset_address = unwrapped_address(token).map_err(|error| {
            tracing::error!(
                share_address = %token.address,
                error = %error,
                "failed to read wrapped token ratio"
            );
            error.into_api_error()
        })?;

        let inputs = vec![WrapRatioBatchInput {
            token,
            share_address: token.address,
            expected_asset_address,
        }];
        let mut groups = read_wrap_ratios_batch(&inputs).await;
        let group = groups.pop().ok_or_else(|| {
            tracing::error!(
                share_address = %token.address,
                "ERC4626 batch response did not include requested token"
            );
            ApiError::Internal("failed to read ERC4626 ratio".into())
        })?;
        let response = group.response?;

        if response.items.len() != 1 {
            tracing::error!(
                expected = 1,
                actual = response.items.len(),
                "ERC4626 batch response item count mismatch"
            );
            return Err(ApiError::Internal("failed to read ERC4626 ratio".into()));
        }

        let metadata = WrapRatioMetadata::from_batch_response(&response);
        let item = find_wrap_ratio_item(&response.items, token.address).map_err(|error| {
            tracing::error!(
                share_address = %token.address,
                error = %error,
                "failed to read wrapped token ratio"
            );
            error.into_api_error()
        })?;

        let response = build_wrap_ratio_response(
            token.network.chain_id,
            item,
            expected_asset_address,
            &metadata,
        )
        .map_err(|error| {
            tracing::error!(
                share_address = %token.address,
                error = %error,
                "failed to read wrapped token ratio"
            );
            error.into_api_error()
        })?;

        persist_wrap_ratio_snapshots_best_effort(pool.inner(), std::slice::from_ref(&response))
            .await;

        tracing::info!(share_address = %token.address, "returning wrapped token ratio");
        Ok(Json(response))
    }
    .instrument(span.0)
    .await
}

#[utoipa::path(
    get,
    path = "/v2/tokens/wrap-ratio/{address}/history",
    tag = "Tokens",
    security(("basicAuth" = [])),
    params(
        ("address" = String, Path, description = "Wrapped token / ERC4626 vault address"),
        WrapRatioHistoryParams
    ),
    responses(
        (status = 200, description = "Wrapped token ratio snapshot history", body = WrapRatioHistoryResponse),
        (status = 400, description = "Invalid pagination, ambiguous token address, or unsupported chainId", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Wrapped ST0x token not found", body = ApiErrorResponse),
        (status = 422, description = "Invalid wrapped token address", body = ApiErrorResponse),
        (status = 429, description = "Rate limited", body = ApiErrorResponse),
        (status = 500, description = "Internal server error", body = ApiErrorResponse),
    )
)]
#[get("/wrap-ratio/<address>/history?<params..>")]
pub async fn get_wrap_ratio_history_by_address(
    _global: GlobalRateLimit,
    _key: AuthenticatedKey,
    span: TracingSpan,
    shared_raindex: &State<SharedRaindexProvider>,
    pool: &State<DbPool>,
    address: ValidatedAddress,
    params: WrapRatioHistoryParams,
) -> Result<Json<WrapRatioHistoryResponse>, ApiError> {
    let requested_chain_id =
        crate::routes::compatibility_chain_id(span.api_version(), params.chain_id);
    async move {
        tracing::info!(share_address = %address.0, "request received");

        let chain_id = validated_requested_chain_id(shared_raindex, requested_chain_id).await?;
        let tokens = registry_tokens(shared_raindex).await?;
        let token = select_st0x_token(
            &tokens,
            address.0,
            chain_id,
            |token| token.address == address.0,
            "wrapped ST0x token not found",
        )?;

        let asset_address = unwrapped_address(token).map_err(|error| {
            tracing::error!(
                share_address = %token.address,
                error = %error,
                "failed to read wrapped token ratio history"
            );
            error.into_api_error()
        })?;

        let (page, page_size, offset) = wrap_ratio_history_pagination_params(params)?;
        let share_token_address = normalize_address(token.address);
        let total_events = count_wrapped_exchange_rate_snapshots_for_share(
            pool.inner(),
            token.network.chain_id,
            &share_token_address,
        )
        .await
        .map_err(|error| {
            tracing::error!(
                share_address = %token.address,
                error = %error,
                "failed to count wrapped token ratio history"
            );
            ApiError::Internal("failed to query wrapped token ratio history".into())
        })?;
        let snapshots = list_wrapped_exchange_rate_snapshots_for_share(
            pool.inner(),
            token.network.chain_id,
            &share_token_address,
            page_size,
            offset,
        )
        .await
        .map_err(|error| {
            tracing::error!(
                share_address = %token.address,
                error = %error,
                "failed to list wrapped token ratio history"
            );
            ApiError::Internal("failed to query wrapped token ratio history".into())
        })?;

        let events = snapshots
            .into_iter()
            .map(wrap_ratio_history_event_from_snapshot)
            .collect::<Result<Vec<_>, ApiError>>()?;
        let pagination = build_wrap_ratio_history_pagination(total_events, page, page_size);

        tracing::info!(
            share_address = %token.address,
            event_count = events.len(),
            total_events,
            page,
            page_size,
            "returning wrapped token ratio history"
        );
        Ok(Json(WrapRatioHistoryResponse {
            chain_id: token.network.chain_id,
            share_address: token.address,
            asset_address,
            events,
            pagination,
        }))
    }
    .instrument(span.0)
    .await
}

#[utoipa::path(
    get,
    path = "/v2/tokens/{address}/proofs",
    tag = "Tokens",
    security(("basicAuth" = [])),
    params(
        ("address" = String, Path, description = "Wrapped, unwrapped, or legacy ST0x token address"),
        TokenChainParams
    ),
    responses(
        (status = 200, description = "Raw ST0x proof metadata, schemas, and receipts", body = TokenProofsResponse),
        (status = 400, description = "Ambiguous token address or proofs unsupported on the selected chain", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Wrapped ST0x token or SFT vault not found", body = ApiErrorResponse),
        (status = 422, description = "Invalid token address", body = ApiErrorResponse),
        (status = 429, description = "Rate limited", body = ApiErrorResponse),
        (status = 500, description = "Internal server error", body = ApiErrorResponse),
    )
)]
#[get("/<address>/proofs?<params..>", rank = 10)]
pub async fn get_token_proofs(
    _global: GlobalRateLimit,
    _key: AuthenticatedKey,
    span: TracingSpan,
    shared_raindex: &State<SharedRaindexProvider>,
    address: ValidatedAddress,
    params: TokenChainParams,
) -> Result<Json<TokenProofsResponse>, ApiError> {
    let requested_chain_id =
        crate::routes::compatibility_chain_id(span.api_version(), params.chain_id);
    async move {
        tracing::info!(address = %address.0, "request received");

        let chain_id = validated_requested_chain_id(shared_raindex, requested_chain_id).await?;
        let tokens = registry_tokens(shared_raindex).await?;
        let token = select_st0x_token(
            &tokens,
            address.0,
            chain_id,
            |token| matches_token_proof_address(token, address.0),
            "wrapped ST0x token not found",
        )?;

        let (sft_subgraph_url, metadata_subgraph_url) = {
            let raindex = shared_raindex.read().await;
            resolve_proof_subgraph_urls(raindex.raindex_yaml(), &token.network.key)?
        };

        tracing::info!(
            requested_address = %address.0,
            wrapped_address = %token.address,
            network_key = %token.network.key,
            "querying token proofs"
        );

        let response = read_token_proofs(
            token.network.chain_id,
            token.address,
            &sft_subgraph_url,
            &metadata_subgraph_url,
        )
        .await?;

        tracing::info!(
            wrapped_address = %token.address,
            metadata_count = response.metadata.len(),
            schema_count = response.schemas.len(),
            receipt_count = response.receipts.len(),
            "returning token proofs"
        );
        Ok(Json(response))
    }
    .instrument(span.0)
    .await
}

pub fn routes() -> Vec<Route> {
    rocket::routes![
        get_tokens,
        get_wrap_ratios,
        get_wrap_ratio_by_address,
        get_wrap_ratio_history_by_address,
        get_token_details,
        get_token_details_by_address,
        get_token_proofs
    ]
}

pub fn routes_v2() -> Vec<Route> {
    routes()
}

#[cfg(test)]
mod tests {
    use super::token_details::clear_token_details_aggregate_cache;
    use super::{
        api_error_message, post_graphql, read_limited_response_body, SFT_PAGE_SIZE,
        SUBGRAPH_ERROR_BODY_LOG_LIMIT,
    };
    use crate::db::wrapped_exchange_rate_history::{
        insert_wrapped_exchange_rate_snapshots, NewWrappedExchangeRateSnapshot,
    };
    use crate::test_helpers::{
        basic_auth_header, mock_raindex_registry_url_with_settings_and_tokens, seed_api_key,
        TestClientBuilder,
    };
    use alloy::primitives::{address, Address, U256};
    use alloy::providers::bindings::IMulticall3::{
        aggregate3Call, aggregateCall, aggregateReturn, getCurrentBlockTimestampCall,
        Result as Multicall3Result,
    };
    use alloy::{hex::encode_prefixed, sol_types::SolCall};
    use rain_erc::erc4626::{
        IERC20Metadata::decimalsCall as erc20DecimalsCall,
        IERC4626::{assetCall, convertToAssetsCall, decimalsCall as erc4626DecimalsCall},
    };
    use rocket::http::{Header, Status};
    use serde_json::json;
    use std::collections::VecDeque;
    use std::sync::{Arc as StdArc, Mutex};
    use tokio::sync::Mutex as AsyncMutex;

    const WT_MSTR: Address = address!("ff05e1bd696900dc6a52ca35ca61bb1024eda8e2");
    const T_MSTR: Address = address!("013b782f402d61aa1004cca95b9f5bb402c9d5fe");
    const WT_SECOND: Address = address!("3333333333333333333333333333333333333333");
    const T_SECOND: Address = address!("4444444444444444444444444444444444444444");
    const WT_BAD: Address = address!("1111111111111111111111111111111111111111");
    const T_BAD: Address = address!("2222222222222222222222222222222222222222");
    const WT_LEGACY: Address = address!("5555555555555555555555555555555555555555");
    static TOKEN_DETAILS_CACHE_TEST_LOCK: AsyncMutex<()> = AsyncMutex::const_new(());

    #[derive(Debug, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct TestTokenHolderPageData {
        #[serde(default)]
        token_holders: Vec<serde_json::Value>,
    }

    fn success_result<C: SolCall>(value: &C::Return) -> Multicall3Result {
        Multicall3Result {
            success: true,
            returnData: C::abi_encode_returns(value).into(),
        }
    }

    fn failed_result() -> Multicall3Result {
        Multicall3Result {
            success: false,
            returnData: Vec::<u8>::new().into(),
        }
    }

    fn timestamp_success(block_number: u64, timestamp: u64) -> String {
        let ret = aggregateReturn {
            blockNumber: U256::from(block_number),
            returnData: vec![
                getCurrentBlockTimestampCall::abi_encode_returns(&U256::from(timestamp)).into(),
            ],
        };
        encode_prefixed(aggregateCall::abi_encode_returns(&ret))
    }

    fn history_snapshot(
        share_token_address: Address,
        asset_token_address: Address,
        assets_per_share: &str,
        block_number: i64,
        block_timestamp: Option<i64>,
        captured_at: &str,
    ) -> NewWrappedExchangeRateSnapshot {
        NewWrappedExchangeRateSnapshot {
            chain_id: 8453,
            share_token_address: format!("{share_token_address:#x}"),
            asset_token_address: format!("{asset_token_address:#x}"),
            assets_per_share: assets_per_share.to_string(),
            block_number,
            block_timestamp,
            captured_at: captured_at.to_string(),
        }
    }

    async fn seed_history_snapshots(
        client: &rocket::local::asynchronous::Client,
        snapshots: &[NewWrappedExchangeRateSnapshot],
    ) {
        let pool = client
            .rocket()
            .state::<crate::db::DbPool>()
            .expect("pool in state");
        insert_wrapped_exchange_rate_snapshots(pool, snapshots)
            .await
            .expect("insert history snapshots");
    }

    async fn mock_erc4626_batch_rpc(
        vault: Address,
        asset: Address,
        share_decimals: u8,
        asset_decimals: u8,
        converted_assets: U256,
    ) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock rpc");
        let addr = listener.local_addr().expect("mock rpc address");

        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };

                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    let n = tokio::io::AsyncReadExt::read(&mut socket, &mut buf)
                        .await
                        .unwrap_or(0);
                    let request = String::from_utf8_lossy(&buf[..n]);

                    let request_lower = request.to_ascii_lowercase();
                    let result = if request.contains("eth_blockNumber") {
                        json!("0x7b")
                    } else if request.contains(
                        encode_prefixed(getCurrentBlockTimestampCall::SELECTOR)
                            .trim_start_matches("0x"),
                    ) {
                        json!(timestamp_success(123, 456))
                    } else if request.contains(
                        encode_prefixed(erc4626DecimalsCall::SELECTOR).trim_start_matches("0x"),
                    ) && (request_lower.contains(
                        format!("{vault:#x}")
                            .trim_start_matches("0x")
                            .to_ascii_lowercase()
                            .as_str(),
                    ) || request_lower.contains(
                        format!("{WT_BAD:#x}")
                            .trim_start_matches("0x")
                            .to_ascii_lowercase()
                            .as_str(),
                    )) {
                        let vault_hex = format!("{vault:#x}")
                            .trim_start_matches("0x")
                            .to_ascii_lowercase();
                        let bad_hex = format!("{WT_BAD:#x}")
                            .trim_start_matches("0x")
                            .to_ascii_lowercase();
                        let mut targets = Vec::new();
                        if let Some(index) = request_lower.find(&vault_hex) {
                            targets.push(index);
                        }
                        if let Some(index) = request_lower.find(&bad_hex) {
                            targets.push(index);
                        }
                        targets.sort();

                        let results = targets
                            .into_iter()
                            .map(|_| success_result::<erc4626DecimalsCall>(&share_decimals))
                            .collect::<Vec<_>>();

                        json!(encode_prefixed(aggregate3Call::abi_encode_returns(
                            &results
                        )))
                    } else if request
                        .contains(encode_prefixed(assetCall::SELECTOR).trim_start_matches("0x"))
                    {
                        let vault_hex = format!("{vault:#x}")
                            .trim_start_matches("0x")
                            .to_ascii_lowercase();
                        let bad_hex = format!("{WT_BAD:#x}")
                            .trim_start_matches("0x")
                            .to_ascii_lowercase();
                        let mut targets = Vec::new();
                        if let Some(index) = request_lower.find(&vault_hex) {
                            targets.push((index, false));
                        }
                        if let Some(index) = request_lower.find(&bad_hex) {
                            targets.push((index, true));
                        }
                        targets.sort_by_key(|(index, _)| *index);

                        let mut results = Vec::with_capacity(targets.len());
                        for (_, is_bad) in targets {
                            if is_bad {
                                results.push(failed_result());
                            } else {
                                results.push(success_result::<assetCall>(&asset));
                            }
                        }

                        json!(encode_prefixed(aggregate3Call::abi_encode_returns(
                            &results
                        )))
                    } else if request.contains(
                        encode_prefixed(erc20DecimalsCall::SELECTOR).trim_start_matches("0x"),
                    ) && request_lower.contains(
                        format!("{asset:#x}")
                            .trim_start_matches("0x")
                            .to_ascii_lowercase()
                            .as_str(),
                    ) {
                        let results = vec![success_result::<erc20DecimalsCall>(&asset_decimals)];
                        json!(encode_prefixed(aggregate3Call::abi_encode_returns(
                            &results
                        )))
                    } else if request.contains(
                        encode_prefixed(convertToAssetsCall::SELECTOR).trim_start_matches("0x"),
                    ) {
                        let results =
                            vec![success_result::<convertToAssetsCall>(&converted_assets)];
                        json!(encode_prefixed(aggregate3Call::abi_encode_returns(
                            &results
                        )))
                    } else {
                        json!("0x")
                    };

                    let body = json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "result": result,
                    })
                    .to_string();
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                        body.len()
                    );
                    let _ =
                        tokio::io::AsyncWriteExt::write_all(&mut socket, response.as_bytes()).await;
                });
            }
        });

        format!("http://{addr}/rpc")
    }

    async fn wrap_ratio_client(rpc_url: &str) -> rocket::local::asynchronous::Client {
        let settings = format!(
            r#"version: 6
networks:
  base:
    rpcs:
      - {rpc_url}
    chain-id: 8453
    currency: ETH
subgraphs:
  base: https://api.goldsky.com/api/public/project_clv14x04y9kzi01saerx7bxpg/subgraphs/ob4-base/0.9/gn
raindexes:
  base:
    address: 0xd2938e7c9fe3597f78832ce780feb61945c377d7
    network: base
    subgraph: base
    deployment-block: 0
deployers:
  base:
    address: 0xC1A14cE2fd58A3A2f99deCb8eDd866204eE07f8D
    network: base
using-tokens-from:
  - __TOKENS_URL__
"#
        );
        let remote_tokens = format!(
            r#"{{
  "name": "ST0x Base Token List",
  "timestamp": "2026-06-02T00:00:00.000Z",
  "version": {{
    "major": 1,
    "minor": 0,
    "patch": 0
  }},
  "tokens": [
    {{
      "chainId": 8453,
      "address": "{WT_MSTR:#x}",
      "decimals": 18,
      "name": "Wrapped MicroStrategy ST0x",
      "symbol": "wtMSTR",
      "extensions": {{
        "category": "ST0x",
        "unwrappedAddress": "{T_MSTR:#x}"
      }}
    }},
    {{
      "chainId": 8453,
      "address": "{T_MSTR:#x}",
      "decimals": 18,
      "name": "MicroStrategy ST0x",
      "symbol": "tMSTR"
    }},
    {{
      "chainId": 8453,
      "address": "{WT_BAD:#x}",
      "decimals": 18,
      "name": "Bad Wrapped ST0x",
      "symbol": "wtBAD",
      "extensions": {{
        "category": "ST0x",
        "unwrappedAddress": "{T_BAD:#x}"
      }}
    }},
    {{
      "chainId": 8453,
      "address": "0x4200000000000000000000000000000000000006",
      "decimals": 18,
      "name": "Wrapped Ether",
      "symbol": "WETH"
    }}
  ]
}}"#
        );
        let registry_url =
            mock_raindex_registry_url_with_settings_and_tokens(&settings, &remote_tokens).await;
        let config = crate::raindex::RaindexProvider::load(&registry_url, None)
            .await
            .expect("load raindex config");
        TestClientBuilder::new()
            .raindex_config(config)
            .build()
            .await
    }

    async fn wrap_ratio_multi_network_client(
        base_rpc_url: &str,
        polygon_rpc_url: &str,
    ) -> rocket::local::asynchronous::Client {
        let settings = format!(
            r#"version: 6
networks:
  base:
    rpcs:
      - {base_rpc_url}
    chain-id: 8453
    currency: ETH
  polygon:
    rpcs:
      - {polygon_rpc_url}
    chain-id: 137
    currency: POL
subgraphs:
  base: https://api.goldsky.com/api/public/project_clv14x04y9kzi01saerx7bxpg/subgraphs/ob4-base/0.9/gn
raindexes:
  base:
    address: 0xd2938e7c9fe3597f78832ce780feb61945c377d7
    network: base
    subgraph: base
    deployment-block: 0
deployers:
  base:
    address: 0xC1A14cE2fd58A3A2f99deCb8eDd866204eE07f8D
    network: base
using-tokens-from:
  - __TOKENS_URL__
"#
        );
        let remote_tokens = format!(
            r#"{{
  "name": "ST0x Multi Network Token List",
  "timestamp": "2026-06-02T00:00:00.000Z",
  "version": {{
    "major": 1,
    "minor": 0,
    "patch": 0
  }},
  "tokens": [
    {{
      "chainId": 8453,
      "address": "{WT_MSTR:#x}",
      "decimals": 18,
      "name": "Wrapped MicroStrategy ST0x",
      "symbol": "wtMSTR",
      "extensions": {{
        "category": "ST0x",
        "unwrappedAddress": "{T_MSTR:#x}"
      }}
    }},
    {{
      "chainId": 137,
      "address": "{WT_SECOND:#x}",
      "decimals": 18,
      "name": "Wrapped Second ST0x",
      "symbol": "wtSECOND",
      "extensions": {{
        "category": "ST0x",
        "unwrappedAddress": "{T_SECOND:#x}"
      }}
    }}
  ]
}}"#
        );
        let registry_url =
            mock_raindex_registry_url_with_settings_and_tokens(&settings, &remote_tokens).await;
        let config = crate::raindex::RaindexProvider::load(&registry_url, None)
            .await
            .expect("load raindex config");
        TestClientBuilder::new()
            .raindex_config(config)
            .build()
            .await
    }

    #[rocket::async_test]
    async fn token_lookup_requires_chain_id_only_for_ambiguous_addresses() {
        let client = wrap_ratio_multi_network_client(
            "http://127.0.0.1:1/base",
            "http://127.0.0.1:1/polygon",
        )
        .await;
        let shared_raindex = client
            .rocket()
            .state::<crate::raindex::SharedRaindexProvider>()
            .expect("shared raindex state");
        let mut tokens = shared_raindex
            .read()
            .await
            .client()
            .get_all_tokens()
            .expect("registry tokens")
            .into_values()
            .collect::<Vec<_>>();
        tokens
            .iter_mut()
            .find(|token| token.network.chain_id == 137)
            .expect("second-network token")
            .address = WT_MSTR;

        let error = super::select_st0x_token(
            &tokens,
            WT_MSTR,
            None,
            |token| token.address == WT_MSTR,
            "token not found",
        )
        .expect_err("duplicate address should require chainId");
        assert!(matches!(error, crate::error::ApiError::BadRequest(_)));

        let base = super::select_st0x_token(
            &tokens,
            WT_MSTR,
            Some(8453),
            |token| token.address == WT_MSTR,
            "token not found",
        )
        .expect("Base token should resolve");
        assert_eq!(base.network.chain_id, 8453);
    }

    async fn mock_proofs_subgraphs(
        sft_body: serde_json::Value,
        metadata_body: serde_json::Value,
    ) -> (String, String) {
        let (sft_url, metadata_url, _) =
            mock_proofs_subgraphs_with_requests(sft_body, metadata_body).await;
        (sft_url, metadata_url)
    }

    async fn mock_proofs_subgraphs_with_requests(
        sft_body: serde_json::Value,
        metadata_body: serde_json::Value,
    ) -> (String, String, StdArc<Mutex<Vec<String>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock proofs subgraph");
        let addr = listener.local_addr().expect("mock proofs subgraph address");
        let requests = StdArc::new(Mutex::new(Vec::new()));
        let recorded_requests = requests.clone();

        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };

                let sft_body = sft_body.clone();
                let metadata_body = metadata_body.clone();
                let requests = recorded_requests.clone();

                tokio::spawn(async move {
                    let mut buf = [0u8; 8192];
                    let n = tokio::io::AsyncReadExt::read(&mut socket, &mut buf)
                        .await
                        .unwrap_or(0);
                    let request = String::from_utf8_lossy(&buf[..n]).to_string();
                    requests
                        .lock()
                        .expect("mock proofs request lock")
                        .push(request.clone());
                    let body = if request.contains("/metadata") {
                        metadata_body
                    } else {
                        sft_body
                    }
                    .to_string();
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                        body.len()
                    );
                    let _ =
                        tokio::io::AsyncWriteExt::write_all(&mut socket, response.as_bytes()).await;
                });
            }
        });

        (
            format!("http://{addr}/sft"),
            format!("http://{addr}/metadata"),
            requests,
        )
    }

    async fn proofs_client(
        sft_url: &str,
        metadata_url: &str,
    ) -> rocket::local::asynchronous::Client {
        let settings = format!(
            r#"version: 6
networks:
  base:
    rpcs:
      - https://mainnet.base.org
    chain-id: 8453
    currency: ETH
subgraphs:
  base: https://example.com/raindex-subgraph
  sft-base: {sft_url}
metaboards:
  base: {metadata_url}
raindexes:
  base:
    address: 0xd2938e7c9fe3597f78832ce780feb61945c377d7
    network: base
    subgraph: base
    deployment-block: 0
deployers:
  base:
    address: 0xC1A14cE2fd58A3A2f99deCb8eDd866204eE07f8D
    network: base
using-tokens-from:
  - __TOKENS_URL__
"#
        );
        let remote_tokens = format!(
            r#"{{
  "name": "ST0x Proof Token List",
  "timestamp": "2026-06-02T00:00:00.000Z",
  "version": {{
    "major": 1,
    "minor": 0,
    "patch": 0
  }},
  "tokens": [
    {{
      "chainId": 8453,
      "address": "{WT_MSTR:#x}",
      "decimals": 18,
      "name": "Wrapped MicroStrategy ST0x",
      "symbol": "wtMSTR",
      "extensions": {{
        "category": "ST0x",
        "unwrappedAddress": "{T_MSTR:#x}",
        "legacyAddress": "{WT_LEGACY:#x}"
      }}
    }},
    {{
      "chainId": 8453,
      "address": "0x4200000000000000000000000000000000000006",
      "decimals": 18,
      "name": "Wrapped Ether",
      "symbol": "WETH"
    }}
  ]
}}"#
        );
        let registry_url =
            mock_raindex_registry_url_with_settings_and_tokens(&settings, &remote_tokens).await;
        let config = crate::raindex::RaindexProvider::load(&registry_url, None)
            .await
            .expect("load raindex config");
        TestClientBuilder::new()
            .raindex_config(config)
            .build()
            .await
    }

    async fn authorized_get<'a>(
        client: &'a rocket::local::asynchronous::Client,
        path: String,
    ) -> rocket::local::asynchronous::LocalResponse<'a> {
        let (key_id, secret) = seed_api_key(client).await;
        let header = basic_auth_header(&key_id, &secret);
        client
            .get(path)
            .header(Header::new("Authorization", header))
            .dispatch()
            .await
    }

    fn proofs_sft_body() -> serde_json::Value {
        json!({
            "data": {
                "offchainAssetReceiptVaults": [{
                    "id": "vault-1",
                    "address": "0xvault",
                    "receiptVaultInformations": [{
                        "id": "schema-1",
                        "information": "0xRAWSCHEMA",
                        "timestamp": "101"
                    }],
                    "deposits": [{
                        "id": "deposit-1",
                        "timestamp": "201",
                        "transaction": { "id": "0xdeposit" },
                        "receipt": {
                            "id": "receipt-1",
                            "receiptId": "7",
                            "receiptInformations": [
                                { "id": "deposit-info-1", "information": "0xRAWDEPOSIT1" },
                                { "id": "deposit-info-2", "information": "0xRAWDEPOSIT2" }
                            ]
                        }
                    }],
                    "withdraws": [{
                        "id": "withdraw-1",
                        "timestamp": 202,
                        "transaction": { "id": "0xwithdraw" },
                        "receipt": {
                            "id": "receipt-2",
                            "receiptId": "8",
                            "receiptInformations": [
                                { "id": "withdraw-info-1", "information": "0xRAWWITHDRAW" }
                            ]
                        }
                    }]
                }]
            }
        })
    }

    fn proofs_metadata_body() -> serde_json::Value {
        json!({
            "data": {
                "metaV1S": [{
                    "id": "meta-1",
                    "meta": "0xRAWMETA",
                    "sender": "0xsender",
                    "subject": format!(
                        "0x000000000000000000000000{}",
                        format!("{WT_MSTR:#x}").trim_start_matches("0x")
                    ),
                    "metaHash": "0xmetahash"
                }]
            }
        })
    }

    fn graphql_request_for_path(requests: &[String], path: &str) -> serde_json::Value {
        let request = requests
            .iter()
            .find(|request| request.contains(path))
            .expect("mock GraphQL request for path");
        let body = request
            .split("\r\n\r\n")
            .nth(1)
            .expect("mock GraphQL request body");
        serde_json::from_str(body).expect("mock GraphQL request body is json")
    }

    fn request_json(request: &str) -> serde_json::Value {
        let body = request
            .split("\r\n\r\n")
            .nth(1)
            .expect("mock GraphQL request body");
        serde_json::from_str(body).expect("mock GraphQL request body is json")
    }

    fn variables_last_id(body: &serde_json::Value) -> &str {
        body["variables"]["lastId"].as_str().unwrap_or_default()
    }

    fn variables_activity_limit(body: &serde_json::Value) -> usize {
        body["variables"]["activityLimit"].as_u64().unwrap_or(5) as usize
    }

    fn count_rows(count: usize) -> Vec<serde_json::Value> {
        (0..count)
            .map(|index| json!({ "id": format!("row-{index}") }))
            .collect()
    }

    fn amount_rows(amounts: &[&str]) -> Vec<serde_json::Value> {
        amounts
            .iter()
            .enumerate()
            .map(|(index, amount)| json!({ "id": format!("amount-{index}"), "amount": amount }))
            .collect()
    }

    fn detail_activity(kind: &str, count: usize) -> Vec<serde_json::Value> {
        (0..count)
            .map(|index| {
                json!({
                    "id": format!("{kind}-{index}"),
                    "amount": if kind == "deposit" { "100" } else { "40" },
                    "timestamp": format!("{}", 200 + index),
                    "transaction": { "id": format!("0x{kind}{index}") },
                    "caller": { "address": format!("{WT_MSTR:#x}") },
                    "receipt": { "receiptId": format!("{index}") }
                })
            })
            .collect()
    }

    fn token_details_body(activity_limit: usize) -> serde_json::Value {
        json!({
            "data": {
                "offchainAssetReceiptVaults": [{
                    "id": "vault-1",
                    "address": format!("{T_MSTR:#x}"),
                    "receiptContractAddress": format!("{T_SECOND:#x}"),
                    "totalShares": "1000",
                    "deployTimestamp": "101",
                    "deployer": format!("{WT_MSTR:#x}"),
                    "admin": format!("{WT_SECOND:#x}"),
                    "name": "Subgraph MicroStrategy",
                    "symbol": "sgMSTR",
                    "deposits": detail_activity("deposit", activity_limit),
                    "withdraws": detail_activity("withdraw", activity_limit)
                }]
            }
        })
    }

    fn token_details_list_body(body: &serde_json::Value) -> serde_json::Value {
        let missing_address = format!("{WT_SECOND:#x}");
        let vaults = body["variables"]["addresses"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|address| address.as_str())
            .filter(|address| *address != missing_address)
            .map(|address| {
                json!({
                    "receiptContractAddress": format!("{T_SECOND:#x}"),
                    "wrappedTokenContractAddress": address,
                    "totalShares": "1000",
                    "name": "Subgraph MicroStrategy",
                    "symbol": "sgMSTR",
                    "tokenHolderCount": "1001",
                    "shareTransferCount": "1002",
                    "depositVolume": "300",
                    "withdrawVolume": "40"
                })
            })
            .collect::<Vec<_>>();

        json!({
            "data": {
                "offchainAssetReceiptVaults": vaults
            }
        })
    }

    fn token_details_response_for_request(request: &str) -> serde_json::Value {
        let body = request_json(request);
        let query = body["query"].as_str().unwrap_or_default();
        let last_id = variables_last_id(&body);

        if query.contains("query TokenDetailsList") {
            return token_details_list_body(&body);
        }

        if query.contains("query TokenDetails") {
            if body["variables"]["address"] == format!("{WT_SECOND:#x}") {
                return json!({ "data": { "offchainAssetReceiptVaults": [] } });
            }
            return token_details_body(variables_activity_limit(&body));
        }

        if query.contains("query TokenHolderPage") {
            return json!({
                "data": {
                    "tokenHolders": if last_id.is_empty() { count_rows(SFT_PAGE_SIZE) } else { count_rows(1) }
                }
            });
        }

        if query.contains("query ShareTransferPage") {
            return json!({
                "data": {
                    "sharesTransfers": if last_id.is_empty() { count_rows(SFT_PAGE_SIZE) } else { count_rows(2) }
                }
            });
        }

        if query.contains("query DepositPage") {
            return json!({
                "data": {
                    "depositWithReceipts": if last_id.is_empty() { amount_rows(&["100", "200"]) } else { vec![] }
                }
            });
        }

        if query.contains("query WithdrawPage") {
            return json!({
                "data": {
                    "withdrawWithReceipts": if last_id.is_empty() { amount_rows(&["40"]) } else { vec![] }
                }
            });
        }

        json!({ "data": { "offchainAssetReceiptVaults": [] } })
    }

    async fn read_mock_http_request(socket: &mut tokio::net::TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut chunk = [0u8; 4096];

        loop {
            let n = tokio::io::AsyncReadExt::read(socket, &mut chunk)
                .await
                .unwrap_or(0);
            if n == 0 {
                break;
            }
            bytes.extend_from_slice(&chunk[..n]);

            let Some(header_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") else {
                continue;
            };
            let headers = String::from_utf8_lossy(&bytes[..header_end]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    line.strip_prefix("content-length:")
                        .or_else(|| line.strip_prefix("Content-Length:"))
                        .and_then(|value| value.trim().parse::<usize>().ok())
                })
                .unwrap_or(0);

            if bytes.len() >= header_end + 4 + content_length {
                break;
            }
        }

        String::from_utf8_lossy(&bytes).to_string()
    }

    async fn mock_token_details_subgraph() -> (String, StdArc<Mutex<Vec<String>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock token details subgraph");
        let addr = listener
            .local_addr()
            .expect("mock token details subgraph address");
        let requests = StdArc::new(Mutex::new(Vec::new()));
        let recorded_requests = requests.clone();

        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let requests = recorded_requests.clone();

                tokio::spawn(async move {
                    let request = read_mock_http_request(&mut socket).await;
                    requests
                        .lock()
                        .expect("mock token details request lock")
                        .push(request.clone());
                    let body = token_details_response_for_request(&request).to_string();
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                        body.len()
                    );
                    let _ =
                        tokio::io::AsyncWriteExt::write_all(&mut socket, response.as_bytes()).await;
                });
            }
        });

        (format!("http://{addr}/sft"), requests)
    }

    async fn mock_scripted_subgraph(
        responses: Vec<(u16, String)>,
    ) -> (String, StdArc<Mutex<Vec<String>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock scripted subgraph");
        let addr = listener
            .local_addr()
            .expect("mock scripted subgraph address");
        let requests = StdArc::new(Mutex::new(Vec::new()));
        let recorded_requests = requests.clone();
        let scripted_responses = StdArc::new(Mutex::new(VecDeque::from(responses)));

        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let requests = recorded_requests.clone();
                let scripted_responses = scripted_responses.clone();

                tokio::spawn(async move {
                    let request = read_mock_http_request(&mut socket).await;
                    requests
                        .lock()
                        .expect("mock scripted subgraph request lock")
                        .push(request);
                    let (status, body) = scripted_responses
                        .lock()
                        .expect("mock scripted subgraph responses lock")
                        .pop_front()
                        .unwrap_or_else(|| (500, "unexpected request".to_string()));
                    let reason = if status == 200 { "OK" } else { "ERROR" };
                    let response = format!(
                        "HTTP/1.1 {status} {reason}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                        body.len()
                    );
                    let _ =
                        tokio::io::AsyncWriteExt::write_all(&mut socket, response.as_bytes()).await;
                });
            }
        });

        (format!("http://{addr}/sft"), requests)
    }

    async fn token_details_client(sft_url: &str) -> rocket::local::asynchronous::Client {
        proofs_client(sft_url, "https://example.com/metadata").await
    }

    async fn token_details_multi_client(sft_url: &str) -> rocket::local::asynchronous::Client {
        let settings = format!(
            r#"version: 6
networks:
  base:
    rpcs:
      - https://mainnet.base.org
    chain-id: 8453
    currency: ETH
subgraphs:
  base: https://example.com/raindex-subgraph
  sft-base: {sft_url}
metaboards:
  base: https://example.com/metadata
raindexes:
  base:
    address: 0xd2938e7c9fe3597f78832ce780feb61945c377d7
    network: base
    subgraph: base
    deployment-block: 0
deployers:
  base:
    address: 0xC1A14cE2fd58A3A2f99deCb8eDd866204eE07f8D
    network: base
using-tokens-from:
  - __TOKENS_URL__
"#
        );
        let remote_tokens = format!(
            r#"{{
  "name": "ST0x Token Details List",
  "timestamp": "2026-06-02T00:00:00.000Z",
  "version": {{ "major": 1, "minor": 0, "patch": 0 }},
  "tokens": [
    {{
      "chainId": 8453,
      "address": "{WT_MSTR:#x}",
      "decimals": 18,
      "name": "Wrapped MicroStrategy ST0x",
      "symbol": "wtMSTR",
      "extensions": {{
        "category": "ST0x",
        "unwrappedAddress": "{T_MSTR:#x}",
        "legacyAddress": "{WT_LEGACY:#x}"
      }}
    }},
    {{
      "chainId": 8453,
      "address": "{WT_SECOND:#x}",
      "decimals": 18,
      "name": "Wrapped Second ST0x",
      "symbol": "wtSECOND",
      "extensions": {{
        "category": "ST0x",
        "unwrappedAddress": "{T_SECOND:#x}"
      }}
    }}
  ]
}}"#
        );
        let registry_url =
            mock_raindex_registry_url_with_settings_and_tokens(&settings, &remote_tokens).await;
        let config = crate::raindex::RaindexProvider::load(&registry_url, None)
            .await
            .expect("load raindex config");
        TestClientBuilder::new()
            .raindex_config(config)
            .build()
            .await
    }

    fn token_details_test_address(index: usize) -> String {
        format!("0x{:040x}", index + 1)
    }

    async fn token_details_many_client(
        sft_url: &str,
        token_count: usize,
    ) -> rocket::local::asynchronous::Client {
        let settings = format!(
            r#"version: 6
networks:
  base:
    rpcs:
      - https://mainnet.base.org
    chain-id: 8453
    currency: ETH
subgraphs:
  base: https://example.com/raindex-subgraph
  sft-base: {sft_url}
metaboards:
  base: https://example.com/metadata
raindexes:
  base:
    address: 0xd2938e7c9fe3597f78832ce780feb61945c377d7
    network: base
    subgraph: base
    deployment-block: 0
deployers:
  base:
    address: 0xC1A14cE2fd58A3A2f99deCb8eDd866204eE07f8D
    network: base
using-tokens-from:
  - __TOKENS_URL__
"#
        );
        let tokens = (0..token_count)
            .map(|index| {
                json!({
                    "chainId": 8453,
                    "address": token_details_test_address(index),
                    "decimals": 18,
                    "name": format!("Wrapped Test {index} ST0x"),
                    "symbol": format!("wtTEST{index}"),
                    "extensions": {
                        "category": "ST0x"
                    }
                })
            })
            .collect::<Vec<_>>();
        let remote_tokens = json!({
            "name": "ST0x Token Details Large List",
            "timestamp": "2026-06-02T00:00:00.000Z",
            "version": { "major": 1, "minor": 0, "patch": 0 },
            "tokens": tokens
        })
        .to_string();
        let registry_url =
            mock_raindex_registry_url_with_settings_and_tokens(&settings, &remote_tokens).await;
        let config = crate::raindex::RaindexProvider::load(&registry_url, None)
            .await
            .expect("load raindex config");
        TestClientBuilder::new()
            .raindex_config(config)
            .build()
            .await
    }

    #[rocket::async_test]
    async fn test_get_tokens_returns_token_list() {
        let client = TestClientBuilder::new().build().await;
        let (key_id, secret) = seed_api_key(&client).await;
        let header = basic_auth_header(&key_id, &secret);
        let response = client
            .get("/v1/tokens")
            .header(Header::new("Authorization", header))
            .dispatch()
            .await;
        assert_eq!(response.status(), Status::Ok);
        let body: serde_json::Value =
            serde_json::from_str(&response.into_string().await.unwrap()).unwrap();
        let tokens = body.as_array().expect("tokens is an array");
        assert_eq!(tokens.len(), 1);
        let first = &tokens[0];
        assert_eq!(
            first["address"],
            "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
        );
    }

    #[rocket::async_test]
    async fn test_get_tokens_returns_multiple_tokens() {
        let settings = r#"version: 6
networks:
  base:
    rpcs:
      - https://mainnet.base.org
    chain-id: 8453
    currency: ETH
subgraphs:
  base: https://api.goldsky.com/api/public/project_clv14x04y9kzi01saerx7bxpg/subgraphs/ob4-base/0.9/gn
raindexes:
  base:
    address: 0xd2938e7c9fe3597f78832ce780feb61945c377d7
    network: base
    subgraph: base
    deployment-block: 0
deployers:
  base:
    address: 0xC1A14cE2fd58A3A2f99deCb8eDd866204eE07f8D
    network: base
tokens:
  usdc:
    address: 0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913
    network: base
    decimals: 6
    label: USD Coin
    symbol: USDC
  weth:
    address: 0x4200000000000000000000000000000000000006
    network: base
    decimals: 18
    label: Wrapped Ether
    symbol: WETH
"#;
        let registry_url =
            crate::test_helpers::mock_raindex_registry_url_with_settings(settings).await;
        let config = crate::raindex::RaindexProvider::load(&registry_url, None)
            .await
            .expect("load raindex config");
        let client = TestClientBuilder::new()
            .raindex_config(config)
            .build()
            .await;
        let (key_id, secret) = seed_api_key(&client).await;
        let header = basic_auth_header(&key_id, &secret);
        let response = client
            .get("/v1/tokens")
            .header(Header::new("Authorization", header))
            .dispatch()
            .await;
        assert_eq!(response.status(), Status::Ok);
        let body: serde_json::Value =
            serde_json::from_str(&response.into_string().await.unwrap()).unwrap();
        let tokens = body.as_array().expect("tokens is an array");
        assert_eq!(tokens.len(), 2);
    }

    #[rocket::async_test]
    async fn test_get_tokens_filters_by_chain_id() {
        let settings = r#"version: 6
networks:
  base:
    rpcs:
      - https://mainnet.base.org
    chain-id: 8453
    currency: ETH
  polygon:
    rpcs:
      - https://polygon-rpc.com
    chain-id: 137
    currency: POL
subgraphs:
  base: https://example.com/base-subgraph
  polygon: https://example.com/polygon-subgraph
raindexes:
  base:
    address: 0xd2938e7c9fe3597f78832ce780feb61945c377d7
    network: base
    subgraph: base
    deployment-block: 0
  polygon:
    address: 0xd2938e7c9fe3597f78832ce780feb61945c377d7
    network: polygon
    subgraph: polygon
    deployment-block: 0
deployers:
  base:
    address: 0xC1A14cE2fd58A3A2f99deCb8eDd866204eE07f8D
    network: base
  polygon:
    address: 0xC1A14cE2fd58A3A2f99deCb8eDd866204eE07f8D
    network: polygon
tokens:
  usdc-base:
    address: 0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913
    network: base
    decimals: 6
    label: USD Coin
    symbol: USDC
  usdc-polygon:
    address: 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174
    network: polygon
    decimals: 6
    label: USD Coin (PoS)
    symbol: USDC
"#;
        let registry_url =
            crate::test_helpers::mock_raindex_registry_url_with_settings(settings).await;
        let config = crate::raindex::RaindexProvider::load(&registry_url, None)
            .await
            .expect("load raindex config");
        let client = TestClientBuilder::new()
            .raindex_config(config)
            .build()
            .await;
        let (key_id, secret) = seed_api_key(&client).await;
        let header = basic_auth_header(&key_id, &secret);

        let response = client
            .get("/v1/tokens")
            .header(Header::new("Authorization", header.clone()))
            .dispatch()
            .await;
        assert_eq!(response.status(), Status::Ok);
        let body: serde_json::Value = response.into_json().await.expect("v1 token response");
        let tokens = body.as_array().expect("tokens is an array");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0]["network"]["chainId"], 8453);

        let response = client
            .get("/v2/tokens")
            .header(Header::new("Authorization", header.clone()))
            .dispatch()
            .await;
        assert_eq!(response.status(), Status::Ok);
        let body: serde_json::Value = response.into_json().await.expect("v2 token response");
        assert_eq!(body.as_array().expect("tokens is an array").len(), 2);

        let response = client
            .get("/v2/tokens?chainId=137")
            .header(Header::new("Authorization", header.clone()))
            .dispatch()
            .await;
        assert_eq!(response.status(), Status::Ok);
        let body: serde_json::Value =
            serde_json::from_str(&response.into_string().await.unwrap()).unwrap();
        let tokens = body.as_array().expect("tokens is an array");
        assert_eq!(tokens.len(), 1);
        assert_eq!(
            tokens[0]["address"],
            "0x2791bca1f2de4661ed88a30c99a7a9449aa84174"
        );
        assert_eq!(tokens[0]["network"]["chainId"], 137);

        let response = client
            .get("/v2/tokens?chainId=999")
            .header(Header::new("Authorization", header))
            .dispatch()
            .await;
        assert_eq!(response.status(), Status::BadRequest);
    }

    #[rocket::async_test]
    async fn test_get_tokens_clears_network_rpcs() {
        let private_rpc = "https://private-rpc.example.com/secret-token";
        let settings = format!(
            r#"version: 6
networks:
  base:
    rpcs:
      - {private_rpc}
    chain-id: 8453
    currency: ETH
subgraphs:
  base: https://api.goldsky.com/api/public/project_clv14x04y9kzi01saerx7bxpg/subgraphs/ob4-base/0.9/gn
raindexes:
  base:
    address: 0xd2938e7c9fe3597f78832ce780feb61945c377d7
    network: base
    subgraph: base
    deployment-block: 0
deployers:
  base:
    address: 0xC1A14cE2fd58A3A2f99deCb8eDd866204eE07f8D
    network: base
tokens:
  usdc:
    address: 0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913
    network: base
    decimals: 6
    label: USD Coin
    symbol: USDC
"#
        );
        let registry_url =
            crate::test_helpers::mock_raindex_registry_url_with_settings(&settings).await;
        let config = crate::raindex::RaindexProvider::load(&registry_url, None)
            .await
            .expect("load raindex config");
        let client = TestClientBuilder::new()
            .raindex_config(config)
            .build()
            .await;
        let (key_id, secret) = seed_api_key(&client).await;
        let header = basic_auth_header(&key_id, &secret);
        let response = client
            .get("/v1/tokens")
            .header(Header::new("Authorization", header))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);
        let response_body = response.into_string().await.expect("response body");
        assert!(!response_body.contains(private_rpc));

        let body: serde_json::Value =
            serde_json::from_str(&response_body).expect("response is json");
        let tokens = body.as_array().expect("tokens is an array");
        let rpcs = tokens[0]["network"]["rpcs"]
            .as_array()
            .expect("network rpcs is an array");
        assert!(rpcs.is_empty());
    }

    #[rocket::async_test]
    async fn test_get_tokens_adds_name_and_isin_from_remote_tokens() {
        let settings = r#"version: 6
networks:
  base:
    rpcs:
      - https://mainnet.base.org
    chain-id: 8453
    currency: ETH
subgraphs:
  base: https://api.goldsky.com/api/public/project_clv14x04y9kzi01saerx7bxpg/subgraphs/ob4-base/0.9/gn
raindexes:
  base:
    address: 0xd2938e7c9fe3597f78832ce780feb61945c377d7
    network: base
    subgraph: base
    deployment-block: 0
deployers:
  base:
    address: 0xC1A14cE2fd58A3A2f99deCb8eDd866204eE07f8D
    network: base
using-tokens-from:
  - __TOKENS_URL__
"#;
        let remote_tokens = r#"{
  "name": "ST0x Base Token List",
  "timestamp": "2026-03-20T00:00:00.000Z",
  "version": {
    "major": 1,
    "minor": 0,
    "patch": 0
  },
  "tokens": [
    {
      "chainId": 8453,
      "address": "0x8AFba81DEc38DE0A18E2Df5E1967a7493651eebf",
      "decimals": 18,
      "name": "Wrapped Circle Internet Group Inc ST0x",
      "symbol": "wtCRCL",
      "logoURI": "https://tokens.st0x.com/images/CRCL.png",
      "extensions": {
        "category": "ST0x",
        "isin": "US1725731079"
      }
    }
  ]
}"#;
        let registry_url =
            mock_raindex_registry_url_with_settings_and_tokens(settings, remote_tokens).await;
        let config = crate::raindex::RaindexProvider::load(&registry_url, None)
            .await
            .expect("load raindex config");
        let client = TestClientBuilder::new()
            .raindex_config(config)
            .build()
            .await;
        let (key_id, secret) = seed_api_key(&client).await;
        let header = basic_auth_header(&key_id, &secret);
        let response = client
            .get("/v1/tokens")
            .header(Header::new("Authorization", header))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);
        let body: serde_json::Value =
            serde_json::from_str(&response.into_string().await.unwrap()).unwrap();
        let tokens = body.as_array().expect("tokens is an array");
        assert_eq!(tokens.len(), 1);

        let first = &tokens[0];
        assert_eq!(
            first["label"],
            serde_json::Value::String("Wrapped Circle Internet Group Inc ST0x".to_string())
        );
        assert_eq!(
            first["name"],
            serde_json::Value::String("Wrapped Circle Internet Group Inc ST0x".to_string())
        );
        assert_eq!(
            first["isin"],
            serde_json::Value::String("US1725731079".to_string())
        );
    }

    #[rocket::async_test]
    async fn test_get_token_proofs_rejects_invalid_address() {
        let (sft_url, metadata_url) =
            mock_proofs_subgraphs(proofs_sft_body(), proofs_metadata_body()).await;
        let client = proofs_client(&sft_url, &metadata_url).await;

        let response =
            authorized_get(&client, "/v1/tokens/not-an-address/proofs".to_string()).await;

        assert_eq!(response.status(), Status::UnprocessableEntity);
    }

    #[rocket::async_test]
    async fn test_get_token_details_by_address_returns_aggregates_and_recent_activity() {
        let _cache_guard = TOKEN_DETAILS_CACHE_TEST_LOCK.lock().await;
        clear_token_details_aggregate_cache();
        let (sft_url, _) = mock_token_details_subgraph().await;
        let client = token_details_client(&sft_url).await;

        let response = authorized_get(
            &client,
            format!("/v1/tokens/{WT_MSTR:#x}/details?activityLimit=2"),
        )
        .await;

        assert_eq!(response.status(), Status::Ok);
        let body: serde_json::Value =
            serde_json::from_str(&response.into_string().await.unwrap()).unwrap();
        assert_eq!(body["chainId"], 8453);
        assert_eq!(body["address"], format!("{WT_MSTR:#x}"));
        assert_eq!(body["receiptContractAddress"], format!("{T_SECOND:#x}"));
        assert_eq!(body["sftVaultAddress"], format!("{T_MSTR:#x}"));
        assert_eq!(body["name"], "Wrapped MicroStrategy ST0x");
        assert_eq!(body["symbol"], "wtMSTR");
        assert_eq!(body["decimals"], 18);
        assert_eq!(body["totalSupply"], "1000");
        assert_eq!(body["holderCount"], 1001);
        assert_eq!(body["transferCount"], 1002);
        assert_eq!(body["depositVolume"], "300");
        assert_eq!(body["withdrawVolume"], "40");
        assert_eq!(body["bridgedSupply"], "260");
        assert_eq!(body["activityVolume"], "340");
        assert_eq!(body["deployTimestamp"], 101);
        assert_eq!(body["deployer"], format!("{WT_MSTR:#x}"));
        assert_eq!(body["admin"], format!("{WT_SECOND:#x}"));
        assert_eq!(body["activity"]["deposits"].as_array().unwrap().len(), 2);
        assert_eq!(body["activity"]["withdraws"].as_array().unwrap().len(), 2);
        assert_eq!(body["activity"]["deposits"][0]["txHash"], "0xdeposit0");
        assert!(body["activity"].get("transfers").is_none());
    }

    #[rocket::async_test]
    async fn test_get_token_details_by_address_resolves_unwrapped_and_legacy_addresses() {
        let _cache_guard = TOKEN_DETAILS_CACHE_TEST_LOCK.lock().await;
        clear_token_details_aggregate_cache();
        let (sft_url, requests) = mock_token_details_subgraph().await;
        let client = token_details_client(&sft_url).await;

        for address in [T_MSTR, WT_LEGACY] {
            let response =
                authorized_get(&client, format!("/v1/tokens/{address:#x}/details")).await;
            assert_eq!(response.status(), Status::Ok);

            let body: serde_json::Value =
                serde_json::from_str(&response.into_string().await.unwrap()).unwrap();
            assert_eq!(body["address"], format!("{WT_MSTR:#x}"));
        }

        let recorded = requests.lock().expect("mock requests").clone();
        let detail_requests = recorded
            .iter()
            .filter(|request| request.contains("query TokenDetails"))
            .map(|request| request_json(request))
            .collect::<Vec<_>>();
        assert!(detail_requests
            .iter()
            .all(|request| request["variables"]["address"] == format!("{WT_MSTR:#x}")));
    }

    #[rocket::async_test]
    async fn test_get_token_details_uses_cached_aggregates() {
        let _cache_guard = TOKEN_DETAILS_CACHE_TEST_LOCK.lock().await;
        clear_token_details_aggregate_cache();
        let (sft_url, requests) = mock_token_details_subgraph().await;
        let client = token_details_client(&sft_url).await;

        for _ in 0..2 {
            let response =
                authorized_get(&client, format!("/v1/tokens/{WT_MSTR:#x}/details")).await;
            assert_eq!(response.status(), Status::Ok);
        }

        let recorded = requests.lock().expect("mock requests").clone();
        let holder_page_requests = recorded
            .iter()
            .filter(|request| request.contains("query TokenHolderPage"))
            .map(|request| request_json(request))
            .collect::<Vec<_>>();
        let transfer_page_requests = recorded
            .iter()
            .filter(|request| request.contains("query ShareTransferPage"))
            .map(|request| request_json(request))
            .collect::<Vec<_>>();
        assert_eq!(holder_page_requests.len(), 2);
        assert_eq!(transfer_page_requests.len(), 2);
        assert_eq!(holder_page_requests[0]["variables"]["lastId"], "");
        assert_eq!(holder_page_requests[1]["variables"]["lastId"], "row-999");
        assert_eq!(transfer_page_requests[0]["variables"]["lastId"], "");
        assert_eq!(transfer_page_requests[1]["variables"]["lastId"], "row-999");
        assert!(holder_page_requests
            .iter()
            .all(|request| request["variables"].get("skip").is_none()));
        assert!(transfer_page_requests
            .iter()
            .all(|request| request["variables"].get("skip").is_none()));
        let holder_page_request_count = recorded
            .iter()
            .filter(|request| request.contains("query TokenHolderPage"))
            .count();
        let transfer_page_request_count = recorded
            .iter()
            .filter(|request| request.contains("query ShareTransferPage"))
            .count();
        assert_eq!(holder_page_request_count, 2);
        assert_eq!(transfer_page_request_count, 2);
    }

    #[rocket::async_test]
    async fn test_post_graphql_retries_retryable_status_then_succeeds() {
        let success_body = json!({
            "data": {
                "tokenHolders": []
            }
        })
        .to_string();
        let (sft_url, requests) = mock_scripted_subgraph(vec![
            (500, "temporary upstream failure".into()),
            (200, success_body),
        ])
        .await;

        let data = post_graphql::<TestTokenHolderPageData>(
            &sft_url,
            "query TokenHolderPage { tokenHolders { id } }",
            json!({}),
        )
        .await
        .expect("retryable status eventually succeeds");

        assert!(data.token_holders.is_empty());
        let recorded = requests.lock().expect("mock requests").clone();
        assert_eq!(recorded.len(), 2);
    }

    #[rocket::async_test]
    async fn test_post_graphql_does_not_retry_permanent_client_status() {
        let (sft_url, requests) = mock_scripted_subgraph(vec![(
            400,
            "permanent bad request".repeat(SUBGRAPH_ERROR_BODY_LOG_LIMIT as usize),
        )])
        .await;

        let error = post_graphql::<TestTokenHolderPageData>(
            &sft_url,
            "query TokenHolderPage { tokenHolders { id } }",
            json!({}),
        )
        .await
        .expect_err("permanent client status fails");

        assert_eq!(api_error_message(&error), "subgraph returned error status");
        let recorded = requests.lock().expect("mock requests").clone();
        assert_eq!(recorded.len(), 1);
    }

    #[rocket::async_test]
    async fn test_read_limited_response_body_truncates_oversized_body() {
        let (sft_url, _) =
            mock_scripted_subgraph(vec![(500, "0123456789abcdefghijklmnopqrstuvwxyz".into())])
                .await;
        let response = reqwest::Client::new()
            .post(&sft_url)
            .send()
            .await
            .expect("mock response");

        let body = read_limited_response_body(response, 10).await;

        assert_eq!(body, "0123456789...[truncated]");
    }

    #[rocket::async_test]
    async fn test_get_token_details_by_address_returns_not_found_for_missing_vault() {
        let _cache_guard = TOKEN_DETAILS_CACHE_TEST_LOCK.lock().await;
        clear_token_details_aggregate_cache();
        let (sft_url, _) = mock_token_details_subgraph().await;
        let client = token_details_multi_client(&sft_url).await;

        let response = authorized_get(&client, format!("/v1/tokens/{WT_SECOND:#x}/details")).await;

        assert_eq!(response.status(), Status::NotFound);
    }

    #[rocket::async_test]
    async fn test_get_token_details_returns_data_and_per_token_errors() {
        let _cache_guard = TOKEN_DETAILS_CACHE_TEST_LOCK.lock().await;
        clear_token_details_aggregate_cache();
        let (sft_url, _) = mock_token_details_subgraph().await;
        let client = token_details_multi_client(&sft_url).await;

        let response = authorized_get(&client, "/v1/tokens/details".to_string()).await;

        assert_eq!(response.status(), Status::Ok);
        let body: serde_json::Value =
            serde_json::from_str(&response.into_string().await.unwrap()).unwrap();
        let data = body["data"].as_array().expect("data is an array");
        let errors = body["errors"].as_array().expect("errors is an array");
        assert_eq!(data.len(), 1);
        assert_eq!(errors.len(), 1);
        assert_eq!(data[0]["chainId"], 8453);
        assert_eq!(data[0]["address"], format!("{WT_MSTR:#x}"));
        assert_eq!(data[0]["holderCount"], 1001);
        assert_eq!(data[0]["transferCount"], 1002);
        assert_eq!(data[0]["bridgedSupply"], "260");
        assert_eq!(errors[0]["chainId"], 8453);
        assert_eq!(errors[0]["address"], format!("{WT_SECOND:#x}"));
        assert_eq!(errors[0]["message"], "SFT vault not found for token");
    }

    #[rocket::async_test]
    async fn test_get_token_details_uses_cached_list_response() {
        let _cache_guard = TOKEN_DETAILS_CACHE_TEST_LOCK.lock().await;
        clear_token_details_aggregate_cache();
        let (sft_url, requests) = mock_token_details_subgraph().await;
        let client = token_details_client(&sft_url).await;

        for _ in 0..2 {
            let response = authorized_get(&client, "/v1/tokens/details".to_string()).await;
            assert_eq!(response.status(), Status::Ok);
        }

        let recorded = requests.lock().expect("mock requests").clone();
        let token_details_requests = recorded
            .iter()
            .filter(|request| request.contains("query TokenDetailsList"))
            .count();
        let holder_page_requests = recorded
            .iter()
            .filter(|request| request.contains("query TokenHolderPage"))
            .count();

        assert_eq!(token_details_requests, 1);
        assert_eq!(holder_page_requests, 0);
    }

    #[rocket::async_test]
    async fn test_get_token_details_chunks_batched_list_queries() {
        let _cache_guard = TOKEN_DETAILS_CACHE_TEST_LOCK.lock().await;
        clear_token_details_aggregate_cache();
        let (sft_url, requests) = mock_token_details_subgraph().await;
        let token_count = SFT_PAGE_SIZE + 1;
        let client = token_details_many_client(&sft_url, token_count).await;

        let response = authorized_get(&client, "/v1/tokens/details".to_string()).await;

        assert_eq!(response.status(), Status::Ok);
        let body: serde_json::Value =
            serde_json::from_str(&response.into_string().await.unwrap()).unwrap();
        assert_eq!(
            body["data"].as_array().expect("data is an array").len(),
            token_count
        );
        assert!(body["errors"]
            .as_array()
            .expect("errors is an array")
            .is_empty());

        let recorded = requests.lock().expect("mock requests").clone();
        let token_details_requests = recorded
            .iter()
            .filter(|request| request.contains("query TokenDetailsList"))
            .map(|request| request_json(request))
            .collect::<Vec<_>>();
        let chunk_sizes = token_details_requests
            .iter()
            .map(|request| {
                request["variables"]["addresses"]
                    .as_array()
                    .expect("addresses is an array")
                    .len()
            })
            .collect::<Vec<_>>();
        let holder_page_requests = recorded
            .iter()
            .filter(|request| request.contains("query TokenHolderPage"))
            .count();

        assert_eq!(chunk_sizes, vec![SFT_PAGE_SIZE, 1]);
        assert_eq!(holder_page_requests, 0);
    }

    #[rocket::async_test]
    async fn test_get_token_details_does_not_cache_partial_list_response() {
        let _cache_guard = TOKEN_DETAILS_CACHE_TEST_LOCK.lock().await;
        clear_token_details_aggregate_cache();
        let (sft_url, requests) = mock_token_details_subgraph().await;
        let client = token_details_multi_client(&sft_url).await;

        for _ in 0..2 {
            let response = authorized_get(&client, "/v1/tokens/details".to_string()).await;
            assert_eq!(response.status(), Status::Ok);
        }

        let recorded = requests.lock().expect("mock requests").clone();
        let token_details_requests = recorded
            .iter()
            .filter(|request| request.contains("query TokenDetailsList"))
            .count();
        let holder_page_requests = recorded
            .iter()
            .filter(|request| request.contains("query TokenHolderPage"))
            .count();

        assert_eq!(token_details_requests, 2);
        assert_eq!(holder_page_requests, 0);
    }

    #[rocket::async_test]
    async fn test_get_token_proofs_returns_not_found_for_unsupported_token() {
        let (sft_url, metadata_url) =
            mock_proofs_subgraphs(proofs_sft_body(), proofs_metadata_body()).await;
        let client = proofs_client(&sft_url, &metadata_url).await;

        let response = authorized_get(
            &client,
            "/v1/tokens/0x4200000000000000000000000000000000000006/proofs".to_string(),
        )
        .await;

        assert_eq!(response.status(), Status::NotFound);
    }

    #[rocket::async_test]
    async fn test_get_token_proofs_resolves_wrapped_unwrapped_and_legacy_addresses() {
        let (sft_url, metadata_url) =
            mock_proofs_subgraphs(proofs_sft_body(), proofs_metadata_body()).await;
        let client = proofs_client(&sft_url, &metadata_url).await;

        for address in [WT_MSTR, T_MSTR, WT_LEGACY] {
            let response = authorized_get(&client, format!("/v1/tokens/{address:#x}/proofs")).await;
            assert_eq!(response.status(), Status::Ok);

            let body: serde_json::Value =
                serde_json::from_str(&response.into_string().await.unwrap()).unwrap();
            assert_eq!(body["address"], format!("{WT_MSTR:#x}"));
        }
    }

    #[rocket::async_test]
    async fn test_get_token_proofs_queries_subgraphs_with_normalized_wrapped_address() {
        let (sft_url, metadata_url, requests) =
            mock_proofs_subgraphs_with_requests(proofs_sft_body(), proofs_metadata_body()).await;
        let client = proofs_client(&sft_url, &metadata_url).await;

        let response = authorized_get(&client, format!("/v1/tokens/{T_MSTR:#x}/proofs")).await;

        assert_eq!(response.status(), Status::Ok);
        let recorded = requests.lock().expect("mock requests").clone();
        let sft_request = graphql_request_for_path(&recorded, "/sft");
        let metadata_request = graphql_request_for_path(&recorded, "/metadata");
        let wrapped_address = format!("{WT_MSTR:#x}");
        let metadata_subject = format!(
            "0x000000000000000000000000{}",
            wrapped_address.trim_start_matches("0x")
        );

        assert_eq!(sft_request["variables"]["address"], wrapped_address);
        assert_eq!(metadata_request["variables"]["subject"], metadata_subject);
        assert!(sft_request["query"]
            .as_str()
            .expect("SFT query string")
            .contains("wrappedTokenContractAddress: $address"));
        assert!(metadata_request["query"]
            .as_str()
            .expect("metadata query string")
            .contains("subject: $subject"));
    }

    #[rocket::async_test]
    async fn test_get_token_proofs_returns_raw_metadata_schemas_and_flattened_receipts() {
        let (sft_url, metadata_url) =
            mock_proofs_subgraphs(proofs_sft_body(), proofs_metadata_body()).await;
        let client = proofs_client(&sft_url, &metadata_url).await;

        let response = authorized_get(&client, format!("/v1/tokens/{WT_MSTR:#x}/proofs")).await;

        assert_eq!(response.status(), Status::Ok);
        let body: serde_json::Value =
            serde_json::from_str(&response.into_string().await.unwrap()).unwrap();
        assert_eq!(body["chainId"], 8453);
        assert_eq!(body["address"], format!("{WT_MSTR:#x}"));
        assert_eq!(body["metadata"][0]["meta"], "0xRAWMETA");
        assert_eq!(body["metadata"][0]["metaHash"], "0xmetahash");
        assert_eq!(body["schemas"][0]["id"], "schema-1");
        assert_eq!(body["schemas"][0]["information"], "0xRAWSCHEMA");
        assert_eq!(body["schemas"][0]["timestamp"], 101);

        let receipts = body["receipts"].as_array().expect("receipts is an array");
        assert_eq!(receipts.len(), 3);
        assert!(receipts.iter().any(|row| row["id"] == "deposit-info-1"
            && row["receiptId"] == "7"
            && row["txHash"] == "0xdeposit"
            && row["type"] == "deposit"
            && row["information"] == "0xRAWDEPOSIT1"
            && row["timestamp"] == 201));
        assert!(receipts.iter().any(|row| row["id"] == "deposit-info-2"
            && row["type"] == "deposit"
            && row["information"] == "0xRAWDEPOSIT2"));
        assert!(receipts.iter().any(|row| row["id"] == "withdraw-info-1"
            && row["receiptId"] == "8"
            && row["txHash"] == "0xwithdraw"
            && row["type"] == "withdraw"
            && row["information"] == "0xRAWWITHDRAW"
            && row["timestamp"] == 202));
    }

    #[rocket::async_test]
    async fn test_get_token_proofs_returns_empty_collections_when_vault_has_no_rows() {
        let sft_body = json!({
            "data": {
                "offchainAssetReceiptVaults": [{
                    "id": "vault-1",
                    "address": "0xvault",
                    "receiptVaultInformations": [],
                    "deposits": [],
                    "withdraws": []
                }]
            }
        });
        let metadata_body = json!({ "data": { "metaV1S": [] } });
        let (sft_url, metadata_url) = mock_proofs_subgraphs(sft_body, metadata_body).await;
        let client = proofs_client(&sft_url, &metadata_url).await;

        let response = authorized_get(&client, format!("/v1/tokens/{WT_MSTR:#x}/proofs")).await;

        assert_eq!(response.status(), Status::Ok);
        let body: serde_json::Value =
            serde_json::from_str(&response.into_string().await.unwrap()).unwrap();
        assert_eq!(body["metadata"].as_array().unwrap().len(), 0);
        assert_eq!(body["schemas"].as_array().unwrap().len(), 0);
        assert_eq!(body["receipts"].as_array().unwrap().len(), 0);
    }

    #[rocket::async_test]
    async fn test_get_token_proofs_returns_not_found_when_sft_vault_is_missing() {
        let sft_body = json!({ "data": { "offchainAssetReceiptVaults": [] } });
        let (sft_url, metadata_url) = mock_proofs_subgraphs(sft_body, proofs_metadata_body()).await;
        let client = proofs_client(&sft_url, &metadata_url).await;

        let response = authorized_get(&client, format!("/v1/tokens/{WT_MSTR:#x}/proofs")).await;

        assert_eq!(response.status(), Status::NotFound);
    }

    #[rocket::async_test]
    async fn test_get_wrap_ratios_returns_data_and_per_token_errors() {
        let rpc_url =
            mock_erc4626_batch_rpc(WT_MSTR, T_MSTR, 18, 18, U256::from(10).pow(U256::from(18)))
                .await;
        let client = wrap_ratio_client(&rpc_url).await;
        let (key_id, secret) = seed_api_key(&client).await;
        let header = basic_auth_header(&key_id, &secret);

        let response = client
            .get("/v1/tokens/wrap-ratio")
            .header(Header::new("Authorization", header))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);
        let body: serde_json::Value =
            serde_json::from_str(&response.into_string().await.unwrap()).unwrap();
        let data = body["data"].as_array().expect("data is an array");
        let errors = body["errors"].as_array().expect("errors is an array");
        assert_eq!(data.len(), 1);
        assert_eq!(errors.len(), 1);

        assert_eq!(data[0]["chainId"], 8453);
        assert_eq!(data[0]["shareAddress"], format!("{WT_MSTR:#x}"));
        assert_eq!(data[0]["assetAddress"], format!("{T_MSTR:#x}"));
        assert_eq!(data[0]["assetsPerShare"], "1.0");
        assert_eq!(data[0]["blockNumber"], 123);
        assert_eq!(data[0]["blockTimestamp"], 456);
        assert!(data[0]["capturedAt"].as_str().is_some());

        let pool = client
            .rocket()
            .state::<crate::db::DbPool>()
            .expect("pool in state");
        let persisted = sqlx::query_as::<_, (i64, String, String, String, i64, Option<i64>)>(
            "SELECT chain_id, share_token_address, asset_token_address, assets_per_share, block_number, block_timestamp \
             FROM wrapped_exchange_rate_snapshots \
             WHERE chain_id = ? AND share_token_address = ?",
        )
        .bind(8453)
        .bind(format!("{WT_MSTR:#x}"))
        .fetch_one(pool)
        .await
        .expect("read persisted snapshot");
        assert_eq!(persisted.0, 8453);
        assert_eq!(persisted.1, format!("{WT_MSTR:#x}"));
        assert_eq!(persisted.2, format!("{T_MSTR:#x}"));
        assert_eq!(persisted.3, "1.0");
        assert_eq!(persisted.4, 123);
        assert_eq!(persisted.5, Some(456));

        assert_eq!(errors[0]["chainId"], 8453);
        assert_eq!(errors[0]["shareAddress"], format!("{WT_BAD:#x}"));
        assert_eq!(errors[0]["message"], "failed to read ERC4626 ratio");
    }

    #[rocket::async_test]
    async fn test_get_wrap_ratios_batches_tokens_by_network() {
        let base_rpc_url =
            mock_erc4626_batch_rpc(WT_MSTR, T_MSTR, 18, 18, U256::from(10).pow(U256::from(18)))
                .await;
        let polygon_rpc_url = mock_erc4626_batch_rpc(
            WT_SECOND,
            T_SECOND,
            18,
            18,
            U256::from(3_000_000_000_000_000_000u128),
        )
        .await;
        let client = wrap_ratio_multi_network_client(&base_rpc_url, &polygon_rpc_url).await;
        let (key_id, secret) = seed_api_key(&client).await;
        let header = basic_auth_header(&key_id, &secret);

        let response = client
            .get("/v2/tokens/wrap-ratio")
            .header(Header::new("Authorization", header))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);
        let body: serde_json::Value =
            serde_json::from_str(&response.into_string().await.unwrap()).unwrap();
        let data = body["data"].as_array().expect("data is an array");
        let errors = body["errors"].as_array().expect("errors is an array");
        assert_eq!(data.len(), 2);
        assert!(errors.is_empty());

        assert!(data.iter().any(|row| row["chainId"] == 8453
            && row["shareAddress"] == format!("{WT_MSTR:#x}")
            && row["assetAddress"] == format!("{T_MSTR:#x}")
            && row["assetsPerShare"] == "1.0"));
        assert!(data.iter().any(|row| row["chainId"] == 137
            && row["shareAddress"] == format!("{WT_SECOND:#x}")
            && row["assetAddress"] == format!("{T_SECOND:#x}")
            && row["assetsPerShare"] == "3"));
    }

    #[rocket::async_test]
    async fn test_get_wrap_ratios_keeps_other_networks_when_one_rpc_fails() {
        let base_rpc_url =
            mock_erc4626_batch_rpc(WT_MSTR, T_MSTR, 18, 18, U256::from(10).pow(U256::from(18)))
                .await;
        let client =
            wrap_ratio_multi_network_client(&base_rpc_url, "http://127.0.0.1:1/unavailable").await;
        let (key_id, secret) = seed_api_key(&client).await;
        let header = basic_auth_header(&key_id, &secret);

        let response = client
            .get("/v2/tokens/wrap-ratio")
            .header(Header::new("Authorization", header))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);
        let body: serde_json::Value =
            serde_json::from_str(&response.into_string().await.unwrap()).unwrap();
        let data = body["data"].as_array().expect("data is an array");
        let errors = body["errors"].as_array().expect("errors is an array");
        assert_eq!(data.len(), 1);
        assert_eq!(errors.len(), 1);
        assert_eq!(data[0]["chainId"], 8453);
        assert_eq!(data[0]["shareAddress"], format!("{WT_MSTR:#x}"));
        assert_eq!(errors[0]["chainId"], 137);
        assert_eq!(errors[0]["shareAddress"], format!("{WT_SECOND:#x}"));
        assert_eq!(errors[0]["message"], "failed to read ERC4626 ratio");
    }

    #[rocket::async_test]
    async fn test_get_wrap_ratios_by_date_returns_latest_snapshot_before_utc_cutoff() {
        let client = wrap_ratio_client("http://127.0.0.1:1/unavailable").await;
        seed_history_snapshots(
            &client,
            &[
                history_snapshot(
                    WT_MSTR,
                    T_MSTR,
                    "1.1",
                    100,
                    Some(1_790_208_000),
                    "1790208001",
                ),
                history_snapshot(
                    WT_MSTR,
                    T_MSTR,
                    "1.2",
                    101,
                    Some(1_790_294_400),
                    "1790294401",
                ),
            ],
        )
        .await;
        let (key_id, secret) = seed_api_key(&client).await;
        let header = basic_auth_header(&key_id, &secret);

        let response = client
            .get("/v2/tokens/wrap-ratio?date=2026-09-24")
            .header(Header::new("Authorization", header))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);
        let body: serde_json::Value =
            serde_json::from_str(&response.into_string().await.unwrap()).unwrap();
        let data = body["data"].as_array().expect("data is an array");
        let errors = body["errors"].as_array().expect("errors is an array");
        assert_eq!(data.len(), 1);
        assert_eq!(data[0]["shareAddress"], format!("{WT_MSTR:#x}"));
        assert_eq!(data[0]["assetsPerShare"], "1.1");
        assert_eq!(data[0]["blockNumber"], 100);
        assert_eq!(data[0]["blockTimestamp"], 1_790_208_000u64);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0]["shareAddress"], format!("{WT_BAD:#x}"));
        assert_eq!(
            errors[0]["message"],
            "no wrapped token ratio snapshot available at or before requested date"
        );
    }

    #[rocket::async_test]
    async fn test_get_wrap_ratios_rejects_invalid_date() {
        let client = wrap_ratio_client("http://127.0.0.1:1/unavailable").await;
        let (key_id, secret) = seed_api_key(&client).await;
        let header = basic_auth_header(&key_id, &secret);

        let response = client
            .get("/v2/tokens/wrap-ratio?date=09-24-2026")
            .header(Header::new("Authorization", header))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::BadRequest);
        let body: serde_json::Value =
            serde_json::from_str(&response.into_string().await.unwrap()).unwrap();
        assert_eq!(body["error"]["message"], "date must use YYYY-MM-DD format");
    }

    #[rocket::async_test]
    async fn test_get_wrap_ratio_by_address_returns_row() {
        let rpc_url = mock_erc4626_batch_rpc(
            WT_MSTR,
            T_MSTR,
            18,
            18,
            U256::from(2_000_000_000_000_000_000u128),
        )
        .await;
        let client = wrap_ratio_client(&rpc_url).await;
        let (key_id, secret) = seed_api_key(&client).await;
        let header = basic_auth_header(&key_id, &secret);

        let response = client
            .get(format!("/v1/tokens/wrap-ratio/{WT_MSTR:#x}"))
            .header(Header::new("Authorization", header))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);
        let body: serde_json::Value =
            serde_json::from_str(&response.into_string().await.unwrap()).unwrap();
        assert_eq!(body["chainId"], 8453);
        assert_eq!(body["shareAddress"], format!("{WT_MSTR:#x}"));
        assert_eq!(body["assetAddress"], format!("{T_MSTR:#x}"));
        assert_eq!(body["assetsPerShare"], "2");
        assert_eq!(body["blockNumber"], 123);
        assert_eq!(body["blockTimestamp"], 456);
        assert!(body["capturedAt"].as_str().is_some());
    }

    #[rocket::async_test]
    async fn test_get_wrap_ratio_history_by_address_returns_snapshot_events() {
        let rpc_url =
            mock_erc4626_batch_rpc(WT_MSTR, T_MSTR, 18, 18, U256::from(10).pow(U256::from(18)))
                .await;
        let client = wrap_ratio_client(&rpc_url).await;
        seed_history_snapshots(
            &client,
            &[
                history_snapshot(WT_MSTR, T_MSTR, "1.0", 100, Some(1100), "1781506300"),
                history_snapshot(WT_MSTR, T_MSTR, "1.1", 101, Some(1101), "1781506301"),
                history_snapshot(WT_MSTR, T_MSTR, "1.2", 102, Some(1102), "1781506302"),
                history_snapshot(WT_SECOND, T_SECOND, "9.9", 103, Some(1103), "1781506303"),
            ],
        )
        .await;
        let (key_id, secret) = seed_api_key(&client).await;
        let header = basic_auth_header(&key_id, &secret);

        let response = client
            .get(format!(
                "/v1/tokens/wrap-ratio/{WT_MSTR:#x}/history?page=1&pageSize=2"
            ))
            .header(Header::new("Authorization", header))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);
        let body: serde_json::Value =
            serde_json::from_str(&response.into_string().await.unwrap()).unwrap();
        assert_eq!(body["chainId"], 8453);
        assert_eq!(body["shareAddress"], format!("{WT_MSTR:#x}"));
        assert_eq!(body["assetAddress"], format!("{T_MSTR:#x}"));

        let events = body["events"].as_array().expect("events is an array");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["type"], "snapshot");
        assert_eq!(events[0]["blockNumber"], 102);
        assert_eq!(events[0]["blockTimestamp"], 1102);
        assert_eq!(events[0]["assetsPerShare"], "1.2");
        assert_eq!(events[0]["capturedAt"], "1781506302");
        assert_eq!(events[1]["type"], "snapshot");
        assert_eq!(events[1]["blockNumber"], 101);

        assert_eq!(body["pagination"]["page"], 1);
        assert_eq!(body["pagination"]["pageSize"], 2);
        assert_eq!(body["pagination"]["totalEvents"], 3);
        assert_eq!(body["pagination"]["totalPages"], 2);
        assert_eq!(body["pagination"]["hasMore"], true);
    }

    #[rocket::async_test]
    async fn test_get_wrap_ratio_history_by_address_returns_empty_history_for_valid_token() {
        let rpc_url =
            mock_erc4626_batch_rpc(WT_MSTR, T_MSTR, 18, 18, U256::from(10).pow(U256::from(18)))
                .await;
        let client = wrap_ratio_client(&rpc_url).await;
        let (key_id, secret) = seed_api_key(&client).await;
        let header = basic_auth_header(&key_id, &secret);

        let response = client
            .get(format!("/v1/tokens/wrap-ratio/{WT_MSTR:#x}/history"))
            .header(Header::new("Authorization", header))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);
        let body: serde_json::Value =
            serde_json::from_str(&response.into_string().await.unwrap()).unwrap();
        assert_eq!(body["shareAddress"], format!("{WT_MSTR:#x}"));
        assert_eq!(body["assetAddress"], format!("{T_MSTR:#x}"));
        assert!(body["events"]
            .as_array()
            .expect("events is an array")
            .is_empty());
        assert_eq!(body["pagination"]["page"], 1);
        assert_eq!(body["pagination"]["pageSize"], 20);
        assert_eq!(body["pagination"]["totalEvents"], 0);
        assert_eq!(body["pagination"]["totalPages"], 0);
        assert_eq!(body["pagination"]["hasMore"], false);
    }

    #[rocket::async_test]
    async fn test_get_wrap_ratio_history_by_address_paginates_and_caps_page_size() {
        let rpc_url =
            mock_erc4626_batch_rpc(WT_MSTR, T_MSTR, 18, 18, U256::from(10).pow(U256::from(18)))
                .await;
        let client = wrap_ratio_client(&rpc_url).await;
        seed_history_snapshots(
            &client,
            &[
                history_snapshot(WT_MSTR, T_MSTR, "1.0", 100, Some(1100), "1781506300"),
                history_snapshot(WT_MSTR, T_MSTR, "1.1", 101, Some(1101), "1781506301"),
                history_snapshot(WT_MSTR, T_MSTR, "1.2", 102, Some(1102), "1781506302"),
            ],
        )
        .await;
        let (key_id, secret) = seed_api_key(&client).await;
        let header = basic_auth_header(&key_id, &secret);

        let response = client
            .get(format!(
                "/v1/tokens/wrap-ratio/{WT_MSTR:#x}/history?page=2&pageSize=2"
            ))
            .header(Header::new("Authorization", header.clone()))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);
        let body: serde_json::Value =
            serde_json::from_str(&response.into_string().await.unwrap()).unwrap();
        let events = body["events"].as_array().expect("events is an array");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["blockNumber"], 100);
        assert_eq!(body["pagination"]["page"], 2);
        assert_eq!(body["pagination"]["pageSize"], 2);
        assert_eq!(body["pagination"]["totalEvents"], 3);
        assert_eq!(body["pagination"]["totalPages"], 2);
        assert_eq!(body["pagination"]["hasMore"], false);

        let capped_response = client
            .get(format!(
                "/v1/tokens/wrap-ratio/{WT_MSTR:#x}/history?pageSize=500"
            ))
            .header(Header::new("Authorization", header))
            .dispatch()
            .await;

        assert_eq!(capped_response.status(), Status::Ok);
        let capped_body: serde_json::Value =
            serde_json::from_str(&capped_response.into_string().await.unwrap()).unwrap();
        assert_eq!(capped_body["pagination"]["pageSize"], 100);
    }

    #[rocket::async_test]
    async fn test_get_wrap_ratio_history_by_address_rejects_symbol_lookup() {
        let rpc_url =
            mock_erc4626_batch_rpc(WT_MSTR, T_MSTR, 18, 18, U256::from(10).pow(U256::from(18)))
                .await;
        let client = wrap_ratio_client(&rpc_url).await;
        let (key_id, secret) = seed_api_key(&client).await;
        let header = basic_auth_header(&key_id, &secret);

        let response = client
            .get("/v1/tokens/wrap-ratio/wtMSTR/history")
            .header(Header::new("Authorization", header))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::UnprocessableEntity);
    }

    #[rocket::async_test]
    async fn test_get_wrap_ratio_history_by_address_returns_not_found_for_unsupported_address() {
        let rpc_url =
            mock_erc4626_batch_rpc(WT_MSTR, T_MSTR, 18, 18, U256::from(10).pow(U256::from(18)))
                .await;
        let client = wrap_ratio_client(&rpc_url).await;
        let (key_id, secret) = seed_api_key(&client).await;
        let header = basic_auth_header(&key_id, &secret);

        for path in [
            "/v1/tokens/wrap-ratio/0x4200000000000000000000000000000000000006/history".to_string(),
            "/v1/tokens/wrap-ratio/0x9999999999999999999999999999999999999999/history".to_string(),
        ] {
            let response = client
                .get(path)
                .header(Header::new("Authorization", header.clone()))
                .dispatch()
                .await;

            assert_eq!(response.status(), Status::NotFound);
        }
    }

    #[rocket::async_test]
    async fn test_get_wrap_ratio_by_address_rejects_symbol_lookup() {
        let rpc_url =
            mock_erc4626_batch_rpc(WT_MSTR, T_MSTR, 18, 18, U256::from(10).pow(U256::from(18)))
                .await;
        let client = wrap_ratio_client(&rpc_url).await;
        let (key_id, secret) = seed_api_key(&client).await;
        let header = basic_auth_header(&key_id, &secret);

        let response = client
            .get("/v1/tokens/wrap-ratio/wtMSTR")
            .header(Header::new("Authorization", header))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::UnprocessableEntity);
    }

    #[rocket::async_test]
    async fn test_get_wrap_ratio_by_address_returns_not_found_for_non_st0x_token() {
        let rpc_url =
            mock_erc4626_batch_rpc(WT_MSTR, T_MSTR, 18, 18, U256::from(10).pow(U256::from(18)))
                .await;
        let client = wrap_ratio_client(&rpc_url).await;
        let (key_id, secret) = seed_api_key(&client).await;
        let header = basic_auth_header(&key_id, &secret);

        let response = client
            .get("/v1/tokens/wrap-ratio/0x4200000000000000000000000000000000000006")
            .header(Header::new("Authorization", header))
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::NotFound);
    }
}

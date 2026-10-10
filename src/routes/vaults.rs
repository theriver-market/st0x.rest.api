use crate::auth::AuthenticatedKey;
use crate::error::{ApiError, ApiErrorResponse};
use crate::fairings::{GlobalRateLimit, TracingSpan};
use crate::routes::resolve_raindex_chain_ids;
use crate::types::vaults::{
    VaultChangeResponse, VaultChangesResponse, VaultOrderRef, VaultPositionResponse,
    VaultTokenResponse, VaultTotalResponse, VaultTotalTokenResponse, VaultTotalsQueryParams,
    VaultTotalsResponse, VaultsPagination, VaultsQueryParams, VaultsResponse,
};
use alloy::primitives::{Address, Bytes, FixedBytes, B256, U256};
use async_trait::async_trait;
use rain_math_float::Float;
use rain_orderbook_common::raindex_client::{
    types::ChainIds,
    vaults::{GetVaultsFilters, RaindexVault, RaindexVaultBalanceChangeType},
    RaindexClient,
};
use rocket::serde::json::Json;
use rocket::{Route, State};
use std::collections::HashMap;
use tracing::Instrument;

const DEFAULT_PAGE: u16 = 1;
const DEFAULT_PAGE_SIZE: u16 = 100;
const MAX_PAGE_SIZE: u16 = 100;
const TOTALS_PAGE_SIZE: u16 = 1000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VaultRecord {
    pub chain_id: u32,
    pub id: String,
    pub vault_id: String,
    pub owner: Address,
    pub token: VaultTokenResponse,
    pub balance: U256,
    pub orderbook: Address,
    pub orders_as_input: Vec<FixedBytes<32>>,
    pub orders_as_output: Vec<FixedBytes<32>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VaultsPage {
    pub vaults: Vec<VaultRecord>,
    pub page: u32,
    pub page_size: u32,
    pub total_items: u64,
    pub has_more: bool,
}

#[async_trait]
pub(crate) trait VaultsDataSource: Send + Sync {
    async fn get_vaults(
        &self,
        chain_ids: Option<Vec<u32>>,
        filters: GetVaultsFilters,
        page: u16,
        page_size: u16,
    ) -> Result<VaultsPage, ApiError>;
}

pub(crate) struct RaindexVaultsDataSource<'a> {
    pub client: &'a RaindexClient,
}

#[async_trait]
impl VaultsDataSource for RaindexVaultsDataSource<'_> {
    async fn get_vaults(
        &self,
        chain_ids: Option<Vec<u32>>,
        filters: GetVaultsFilters,
        page: u16,
        page_size: u16,
    ) -> Result<VaultsPage, ApiError> {
        let chain_ids = resolve_raindex_chain_ids(self.client, chain_ids)?;
        let response = self
            .client
            .get_vaults(
                Some(ChainIds(chain_ids)),
                Some(filters),
                Some(page),
                Some(page_size),
            )
            .await
            .map_err(|error| {
                tracing::error!(
                    error = %error,
                    page,
                    page_size,
                    "failed to retrieve vaults from raindex"
                );
                ApiError::Internal("failed to retrieve vaults".into())
            })?;

        let vaults = response
            .items()
            .into_iter()
            .map(vault_record_from_sdk)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(VaultsPage {
            vaults,
            page: response.page().into(),
            page_size: response.page_size().into(),
            total_items: response.total_items().into(),
            has_more: response.has_more(),
        })
    }
}

pub(crate) fn vault_record_from_sdk(vault: RaindexVault) -> Result<VaultRecord, ApiError> {
    let token = vault.token();
    let decimals = token.decimals();
    let balance = vault
        .balance()
        .to_fixed_decimal_lossy(decimals)
        .map_err(|error| {
            tracing::error!(
                error = %error,
                vault_id = %vault.vault_id(),
                token = %token.address(),
                "failed to convert vault balance to raw token units"
            );
            ApiError::Internal("failed to convert vault balance".into())
        })?
        .0;

    Ok(VaultRecord {
        id: vault.id().to_string(),
        chain_id: vault.chain_id(),
        vault_id: vault.vault_id().to_string(),
        owner: vault.owner(),
        token: VaultTokenResponse {
            address: token.address(),
            name: token.name(),
            symbol: token.symbol(),
            decimals,
        },
        balance,
        orderbook: vault.raindex(),
        orders_as_input: vault
            .orders_as_inputs()
            .into_iter()
            .map(|order| order.order_hash)
            .collect(),
        orders_as_output: vault
            .orders_as_outputs()
            .into_iter()
            .map(|order| order.order_hash)
            .collect(),
    })
}

pub(crate) fn parse_address(value: &str, field: &str) -> Result<Address, ApiError> {
    value.parse::<Address>().map_err(|error| {
        tracing::warn!(field, value, error = %error, "invalid address query parameter");
        ApiError::BadRequest(format!("{field} must be a valid address"))
    })
}

fn pagination(params: &VaultsQueryParams) -> Result<(u16, u16), ApiError> {
    let page = params.page.unwrap_or(DEFAULT_PAGE);
    if page == 0 {
        return Err(ApiError::BadRequest(
            "page must be greater than zero".into(),
        ));
    }

    let page_size = params.page_size.unwrap_or(DEFAULT_PAGE_SIZE);
    if page_size == 0 {
        return Err(ApiError::BadRequest(
            "pageSize must be greater than zero".into(),
        ));
    }

    Ok((page, page_size.min(MAX_PAGE_SIZE)))
}

fn order_refs(order_hashes: Vec<FixedBytes<32>>) -> Vec<VaultOrderRef> {
    order_hashes
        .into_iter()
        .map(|order_hash| VaultOrderRef { order_hash })
        .collect()
}

fn position_response(vault: VaultRecord) -> VaultPositionResponse {
    VaultPositionResponse {
        chain_id: vault.chain_id,
        id: vault.id,
        vault_id: vault.vault_id,
        owner: vault.owner,
        token: vault.token,
        balance: vault.balance.to_string(),
        orderbook: vault.orderbook,
        orders_as_input: order_refs(vault.orders_as_input),
        orders_as_output: order_refs(vault.orders_as_output),
    }
}

pub(crate) async fn process_get_vaults(
    ds: &dyn VaultsDataSource,
    chain_ids: Option<Vec<u32>>,
    params: VaultsQueryParams,
) -> Result<VaultsResponse, ApiError> {
    let owner = params
        .owner
        .as_deref()
        .ok_or_else(|| ApiError::BadRequest("owner is required".into()))
        .and_then(|owner| parse_address(owner, "owner"))?;
    let token = params
        .token
        .as_deref()
        .map(|token| parse_address(token, "token"))
        .transpose()?;
    let (page, page_size) = pagination(&params)?;

    let filters = GetVaultsFilters {
        owners: vec![owner],
        hide_zero_balance: params.hide_zero_balance.unwrap_or(false),
        tokens: token.map(|token| vec![token]),
        ..Default::default()
    };

    let page = ds.get_vaults(chain_ids, filters, page, page_size).await?;

    Ok(VaultsResponse {
        vaults: page.vaults.into_iter().map(position_response).collect(),
        pagination: VaultsPagination {
            page: page.page,
            page_size: page.page_size,
            total_items: page.total_items,
            has_more: page.has_more,
        },
    })
}

#[derive(Debug, Clone)]
struct VaultTotalAccumulator {
    chain_id: u32,
    token: VaultTokenResponse,
    total_balance: U256,
    vault_count: u64,
}

pub(crate) async fn process_get_vault_totals(
    ds: &dyn VaultsDataSource,
    chain_ids: Option<Vec<u32>>,
) -> Result<VaultTotalsResponse, ApiError> {
    let filters = GetVaultsFilters {
        owners: Vec::new(),
        hide_zero_balance: true,
        ..Default::default()
    };
    let mut totals: HashMap<(u32, Address), VaultTotalAccumulator> = HashMap::new();
    let mut page = DEFAULT_PAGE;

    loop {
        let response = ds
            .get_vaults(chain_ids.clone(), filters.clone(), page, TOTALS_PAGE_SIZE)
            .await?;

        for vault in response
            .vaults
            .into_iter()
            .filter(|vault| vault.balance > U256::ZERO)
        {
            let entry = totals
                .entry((vault.chain_id, vault.token.address))
                .or_insert_with(|| VaultTotalAccumulator {
                    chain_id: vault.chain_id,
                    token: vault.token.clone(),
                    total_balance: U256::ZERO,
                    vault_count: 0,
                });
            entry.total_balance += vault.balance;
            entry.vault_count += 1;
        }

        if !response.has_more {
            break;
        }
        page = page.checked_add(1).ok_or_else(|| {
            tracing::error!("vault totals pagination exhausted u16 page range");
            ApiError::Internal("vault totals pagination exceeded maximum".into())
        })?;
    }

    let mut totals: Vec<VaultTotalResponse> = totals
        .into_values()
        .map(|total| VaultTotalResponse {
            chain_id: total.chain_id,
            token: VaultTotalTokenResponse {
                address: total.token.address,
                symbol: total.token.symbol,
                decimals: total.token.decimals,
            },
            total_balance: total.total_balance.to_string(),
            vault_count: total.vault_count,
        })
        .collect();
    totals.sort_by(|a, b| {
        a.chain_id
            .cmp(&b.chain_id)
            .then_with(|| a.token.address.cmp(&b.token.address))
    });

    Ok(VaultTotalsResponse { totals })
}

#[utoipa::path(
    get,
    path = "/v2/vaults",
    tag = "Vaults",
    security(("basicAuth" = [])),
    params(VaultsQueryParams),
    responses(
        (status = 200, description = "Paginated list of deposit vault positions", body = VaultsResponse),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 429, description = "Rate limited", body = ApiErrorResponse),
        (status = 500, description = "Internal server error", body = ApiErrorResponse),
    )
)]
#[get("/?<params..>")]
pub async fn get_vaults(
    _global: GlobalRateLimit,
    _key: AuthenticatedKey,
    shared_raindex: &State<crate::raindex::SharedRaindexProvider>,
    span: TracingSpan,
    params: VaultsQueryParams,
) -> Result<Json<VaultsResponse>, ApiError> {
    let chain_id = crate::routes::compatibility_chain_id(span.api_version(), params.chain_id);
    async move {
        tracing::info!(params = ?params, "request received");
        let raindex = shared_raindex.read().await;
        let chain_ids = crate::routes::optional_chain_ids_filter(raindex.raindex_yaml(), chain_id)?;
        let ds = RaindexVaultsDataSource {
            client: raindex.client(),
        };
        let response = process_get_vaults(&ds, chain_ids, params.clone())
            .await
            .map_err(|error| {
                tracing::warn!(params = ?params, error = %error, "get_vaults failed");
                error
            })?;
        tracing::info!(
            vault_count = response.vaults.len(),
            total_items = response.pagination.total_items,
            "returning vault positions"
        );
        Ok(Json(response))
    }
    .instrument(span.0)
    .await
}

#[utoipa::path(
    get,
    path = "/v2/vaults/totals",
    tag = "Vaults",
    security(("basicAuth" = [])),
    params(VaultTotalsQueryParams),
    responses(
        (status = 200, description = "Aggregated non-zero vault balances by token", body = VaultTotalsResponse),
        (status = 400, description = "Unsupported chainId", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 429, description = "Rate limited", body = ApiErrorResponse),
        (status = 500, description = "Internal server error", body = ApiErrorResponse),
    )
)]
#[get("/totals?<params..>")]
pub async fn get_vault_totals(
    _global: GlobalRateLimit,
    _key: AuthenticatedKey,
    shared_raindex: &State<crate::raindex::SharedRaindexProvider>,
    span: TracingSpan,
    params: VaultTotalsQueryParams,
) -> Result<Json<VaultTotalsResponse>, ApiError> {
    let chain_id = crate::routes::compatibility_chain_id(span.api_version(), params.chain_id);
    async move {
        tracing::info!("request received");
        let raindex = shared_raindex.read().await;
        let chain_ids = crate::routes::optional_chain_ids_filter(raindex.raindex_yaml(), chain_id)?;
        let ds = RaindexVaultsDataSource {
            client: raindex.client(),
        };
        let response = process_get_vault_totals(&ds, chain_ids)
            .await
            .map_err(|error| {
                tracing::warn!(error = %error, "get_vault_totals failed");
                error
            })?;
        tracing::info!(
            token_count = response.totals.len(),
            "returning vault totals"
        );
        Ok(Json(response))
    }
    .instrument(span.0)
    .await
}

/// One deposit, withdrawal or fill on a vault, as read from the SDK (own index; for
/// The River's strategy P&L, research/OWN-INDEXER.md). Amounts stay as SDK floats here and
/// are converted to raw signed token units by `process_get_vault_changes`.
#[derive(Debug, Clone)]
pub(crate) struct VaultChangeRecord {
    pub change_type: RaindexVaultBalanceChangeType,
    pub amount: Float,
    pub old_balance: Float,
    pub new_balance: Float,
    pub token: Address,
    pub decimals: u8,
    pub timestamp: u64,
    pub tx_hash: B256,
    pub sender: Address,
}

#[async_trait]
pub(crate) trait VaultChangesDataSource: Send + Sync {
    /// All balance changes for one vault. `Ok(None)` when the vault is unknown.
    async fn get_vault_changes(
        &self,
        chain_id: u32,
        raindex: Address,
        vault: Bytes,
        page: u16,
    ) -> Result<Option<Vec<VaultChangeRecord>>, ApiError>;
}

#[async_trait]
impl VaultChangesDataSource for RaindexVaultsDataSource<'_> {
    async fn get_vault_changes(
        &self,
        chain_id: u32,
        raindex: Address,
        vault: Bytes,
        page: u16,
    ) -> Result<Option<Vec<VaultChangeRecord>>, ApiError> {
        let ident = rain_orderbook_common::local_db::RaindexIdentifier::new(chain_id, raindex);
        let vault = match self.client.get_vault(&ident, vault.clone()).await {
            Ok(vault) => vault,
            Err(error) => {
                tracing::warn!(error = %error, chain_id, vault = %vault, "vault not found");
                return Ok(None);
            }
        };
        Ok(Some(sdk_vault_change_records(&vault, Some(page)).await?))
    }
}

/// Reads one vault's balance changes through the SDK (local DB or subgraph, as the
/// SDK routes the chain) and maps them to `VaultChangeRecord`s.
pub(crate) async fn sdk_vault_change_records(
    vault: &RaindexVault,
    page: Option<u16>,
) -> Result<Vec<VaultChangeRecord>, ApiError> {
    let decimals = vault.token().decimals();
    let changes = vault
        .get_balance_changes(page, None)
        .await
        .map_err(|error| {
            tracing::error!(error = %error, chain_id = vault.chain_id(), "failed to read vault changes");
            ApiError::Internal("failed to read vault changes".into())
        })?;
    Ok(changes
        .into_iter()
        .map(|change| {
            let tx = change.transaction();
            VaultChangeRecord {
                change_type: change.r#type(),
                amount: change.amount(),
                old_balance: change.old_balance(),
                new_balance: change.new_balance(),
                token: change.token().address(),
                decimals,
                timestamp: change.timestamp().try_into().unwrap_or(u64::MAX),
                tx_hash: tx.id(),
                sender: tx.from(),
            }
        })
        .collect())
}

fn change_type_key(change_type: &RaindexVaultBalanceChangeType) -> &'static str {
    match change_type {
        RaindexVaultBalanceChangeType::Deposit => "deposit",
        RaindexVaultBalanceChangeType::Withdrawal => "withdrawal",
        RaindexVaultBalanceChangeType::TakeOrder => "takeOrder",
        RaindexVaultBalanceChangeType::Clear => "clear",
        RaindexVaultBalanceChangeType::ClearBounty => "clearBounty",
        RaindexVaultBalanceChangeType::Unknown => "unknown",
    }
}

/// Converts an SDK float to raw token units, keeping the sign (negative = out of the vault).
fn raw_units(v: Float, decimals: u8, what: &str) -> Result<String, ApiError> {
    let float_error = |error: rain_math_float::FloatError| {
        tracing::error!(error = %error, what, "float comparison failed for vault change");
        ApiError::Internal("failed to convert vault change".into())
    };
    // to_fixed_decimal_lossy refuses negatives, so convert the magnitude and re-sign.
    let zero = Float::parse("0".to_string()).map_err(float_error)?;
    let negative = v.lt(zero).map_err(float_error)?;
    let magnitude = if negative {
        v.abs().map_err(float_error)?
    } else {
        v
    };
    let (units, _) = magnitude.to_fixed_decimal_lossy(decimals).map_err(|error| {
        tracing::error!(error = %error, what, "failed to convert vault change to raw token units");
        ApiError::Internal("failed to convert vault change".into())
    })?;
    Ok(if negative && !units.is_zero() {
        format!("-{units}")
    } else {
        units.to_string()
    })
}

/// One change row in the public shape: raw signed token units, machine + display type.
pub(crate) fn vault_change_response(
    record: VaultChangeRecord,
) -> Result<VaultChangeResponse, ApiError> {
    Ok(VaultChangeResponse {
        change_type: change_type_key(&record.change_type).to_string(),
        kind: record.change_type.display_name().to_string(),
        amount: raw_units(record.amount, record.decimals, "amount")?,
        old_balance: raw_units(record.old_balance, record.decimals, "old_balance")?,
        new_balance: raw_units(record.new_balance, record.decimals, "new_balance")?,
        token: record.token.to_string(),
        decimals: record.decimals,
        timestamp: record.timestamp,
        tx_hash: record.tx_hash.to_string(),
        sender: record.sender.to_string(),
    })
}

pub(crate) async fn process_get_vault_changes(
    ds: &dyn VaultChangesDataSource,
    chain_id: u32,
    raindex: &str,
    id: &str,
    page: Option<u16>,
) -> Result<VaultChangesResponse, ApiError> {
    let raindex_address = parse_address(raindex, "raindex")?;
    let vault: Bytes = id.parse().map_err(|_| {
        tracing::warn!(id, "invalid vault id");
        ApiError::BadRequest("id must be hex".into())
    })?;
    let page = page.unwrap_or(DEFAULT_PAGE).max(1);
    let mut records = ds
        .get_vault_changes(chain_id, raindex_address, vault, page)
        .await?
        .ok_or_else(|| ApiError::NotFound("vault not found".into()))?;
    // Newest first. Stable, so the source's block/log order holds within one timestamp.
    records.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
    let changes = records
        .into_iter()
        .map(vault_change_response)
        .collect::<Result<Vec<_>, ApiError>>()?;
    Ok(VaultChangesResponse {
        chain_id,
        vault: id.to_string(),
        page,
        changes,
    })
}

#[utoipa::path(
    get,
    path = "/v2/vaults/{chain_id}/{raindex}/{id}/changes",
    tag = "Vaults",
    security(("basicAuth" = [])),
    params(
        ("chain_id" = u32, Path, description = "Chain id"),
        ("raindex" = String, Path, description = "Raindex (orderbook) contract address"),
        ("id" = String, Path, description = "Vault id as returned by /v2/vaults (`id`, hex)"),
        ("page" = Option<u16>, Query, description = "Page (subgraph-backed chains only; the local index returns every change)"),
    ),
    responses(
        (status = 200, description = "Deposits, withdrawals and fills on the vault, newest first, in raw signed token units", body = VaultChangesResponse),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Vault not found", body = ApiErrorResponse),
        (status = 429, description = "Rate limited", body = ApiErrorResponse),
        (status = 500, description = "Internal server error", body = ApiErrorResponse),
    )
)]
#[allow(clippy::too_many_arguments)]
#[get("/<chain_id>/<raindex>/<id>/changes?<page>")]
pub async fn get_vault_changes(
    _global: GlobalRateLimit,
    _key: AuthenticatedKey,
    shared_raindex: &State<crate::raindex::SharedRaindexProvider>,
    span: TracingSpan,
    chain_id: u32,
    raindex: &str,
    id: &str,
    page: Option<u16>,
) -> Result<Json<VaultChangesResponse>, ApiError> {
    async move {
        tracing::info!(chain_id, raindex, id, ?page, "request received");
        let provider = shared_raindex.read().await;
        let ds = RaindexVaultsDataSource {
            client: provider.client(),
        };
        let response = process_get_vault_changes(&ds, chain_id, raindex, id, page)
            .await
            .map_err(|error| {
                tracing::warn!(chain_id, id, error = %error, "get_vault_changes failed");
                error
            })?;
        tracing::info!(
            change_count = response.changes.len(),
            "returning vault changes"
        );
        Ok(Json(response))
    }
    .instrument(span.0)
    .await
}

pub fn routes() -> Vec<Route> {
    rocket::routes![get_vault_totals, get_vaults, get_vault_changes]
}

pub fn routes_v2() -> Vec<Route> {
    routes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{address, fixed_bytes};
    use std::sync::{Arc, Mutex};

    const OWNER: Address = address!("1111111111111111111111111111111111111111");
    const OTHER_OWNER: Address = address!("2222222222222222222222222222222222222222");
    const TOKEN_A: Address = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    const TOKEN_B: Address = address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    const ORDERBOOK: Address = address!("d2938e7c9fe3597f78832ce780feb61945c377d7");

    type VaultsCall = (Option<Vec<u32>>, GetVaultsFilters, u16, u16);

    #[derive(Clone, Default)]
    struct MockVaultsDataSource {
        vaults: Vec<VaultRecord>,
        error: Option<ApiError>,
        calls: Arc<Mutex<Vec<VaultsCall>>>,
    }

    #[async_trait]
    impl VaultsDataSource for MockVaultsDataSource {
        async fn get_vaults(
            &self,
            chain_ids: Option<Vec<u32>>,
            filters: GetVaultsFilters,
            page: u16,
            page_size: u16,
        ) -> Result<VaultsPage, ApiError> {
            self.calls
                .lock()
                .unwrap()
                .push((chain_ids.clone(), filters.clone(), page, page_size));
            if let Some(error) = &self.error {
                return Err(error.clone());
            }

            let mut vaults: Vec<VaultRecord> = self
                .vaults
                .iter()
                .filter(|vault| {
                    chain_ids
                        .as_ref()
                        .is_none_or(|chain_ids| chain_ids.contains(&vault.chain_id))
                        && (filters.owners.is_empty() || filters.owners.contains(&vault.owner))
                        && filters
                            .tokens
                            .as_ref()
                            .is_none_or(|tokens| tokens.contains(&vault.token.address))
                        && (!filters.hide_zero_balance || vault.balance > U256::ZERO)
                })
                .cloned()
                .collect();
            vaults.sort_by(|a, b| a.id.cmp(&b.id));

            let total_items = vaults.len() as u64;
            let start = ((page as usize) - 1) * page_size as usize;
            let end = start.saturating_add(page_size as usize).min(vaults.len());
            let page_vaults = if start < vaults.len() {
                vaults[start..end].to_vec()
            } else {
                Vec::new()
            };

            Ok(VaultsPage {
                vaults: page_vaults,
                page: page.into(),
                page_size: page_size.into(),
                total_items,
                has_more: end < vaults.len(),
            })
        }
    }

    fn token(address: Address, symbol: &str, decimals: u8) -> VaultTokenResponse {
        VaultTokenResponse {
            address,
            name: Some(format!("{symbol} Token")),
            symbol: Some(symbol.to_string()),
            decimals,
        }
    }

    fn vault(
        id: &str,
        owner: Address,
        token: VaultTokenResponse,
        balance: u64,
        order_hash_seed: u8,
    ) -> VaultRecord {
        VaultRecord {
            chain_id: 8453,
            id: id.to_string(),
            vault_id: balance.to_string(),
            owner,
            token,
            balance: U256::from(balance),
            orderbook: ORDERBOOK,
            orders_as_input: vec![FixedBytes::from([order_hash_seed; 32])],
            orders_as_output: vec![FixedBytes::from([order_hash_seed + 1; 32])],
        }
    }

    fn params(owner: &str) -> VaultsQueryParams {
        VaultsQueryParams {
            chain_id: None,
            owner: Some(owner.to_string()),
            token: None,
            hide_zero_balance: None,
            page: None,
            page_size: None,
        }
    }

    #[rocket::async_test]
    async fn get_vaults_happy_path() {
        let ds = MockVaultsDataSource {
            vaults: vec![vault("1", OWNER, token(TOKEN_A, "USDC", 6), 1_000_000, 1)],
            ..Default::default()
        };

        let response = process_get_vaults(&ds, None, params(&OWNER.to_string()))
            .await
            .unwrap();

        assert_eq!(response.vaults.len(), 1);
        let vault = &response.vaults[0];
        assert_eq!(vault.owner, OWNER);
        assert_eq!(vault.token.address, TOKEN_A);
        assert_eq!(vault.balance, "1000000");
        assert_eq!(vault.orderbook, ORDERBOOK);
        assert_eq!(
            vault.orders_as_input[0].order_hash,
            fixed_bytes!("0101010101010101010101010101010101010101010101010101010101010101")
        );
        assert_eq!(
            vault.orders_as_output[0].order_hash,
            fixed_bytes!("0202020202020202020202020202020202020202020202020202020202020202")
        );
        assert_eq!(response.pagination.total_items, 1);
        assert!(!response.pagination.has_more);
    }

    #[rocket::async_test]
    async fn get_vaults_applies_token_filter() {
        let ds = MockVaultsDataSource {
            vaults: vec![
                vault("1", OWNER, token(TOKEN_A, "USDC", 6), 1, 1),
                vault("2", OWNER, token(TOKEN_B, "WETH", 18), 2, 3),
            ],
            ..Default::default()
        };
        let mut params = params(&OWNER.to_string());
        params.token = Some(TOKEN_B.to_string());

        let response = process_get_vaults(&ds, None, params).await.unwrap();

        assert_eq!(response.vaults.len(), 1);
        assert_eq!(response.vaults[0].token.address, TOKEN_B);
    }

    #[rocket::async_test]
    async fn get_vaults_applies_chain_filter() {
        let base_vault = vault("1", OWNER, token(TOKEN_A, "USDC", 6), 1, 1);
        let mut polygon_vault = vault("2", OWNER, token(TOKEN_A, "USDC", 6), 2, 3);
        polygon_vault.chain_id = 137;
        let ds = MockVaultsDataSource {
            vaults: vec![base_vault, polygon_vault],
            ..Default::default()
        };

        let response = process_get_vaults(&ds, Some(vec![137]), params(&OWNER.to_string()))
            .await
            .unwrap();

        assert_eq!(response.vaults.len(), 1);
        assert_eq!(response.vaults[0].chain_id, 137);
        assert_eq!(response.vaults[0].balance, "2");
    }

    #[rocket::async_test]
    async fn get_vaults_hide_zero_balance() {
        let ds = MockVaultsDataSource {
            vaults: vec![
                vault("1", OWNER, token(TOKEN_A, "USDC", 6), 0, 1),
                vault("2", OWNER, token(TOKEN_A, "USDC", 6), 5, 3),
            ],
            ..Default::default()
        };
        let mut params = params(&OWNER.to_string());
        params.hide_zero_balance = Some(true);

        let response = process_get_vaults(&ds, None, params).await.unwrap();

        assert_eq!(response.vaults.len(), 1);
        assert_eq!(response.vaults[0].balance, "5");
    }

    #[rocket::async_test]
    async fn get_vaults_paginates_and_sets_has_more() {
        let ds = MockVaultsDataSource {
            vaults: (0..3)
                .map(|i| {
                    vault(
                        &i.to_string(),
                        OWNER,
                        token(TOKEN_A, "USDC", 6),
                        i + 1,
                        i as u8,
                    )
                })
                .collect(),
            ..Default::default()
        };
        let mut params = params(&OWNER.to_string());
        params.page = Some(2);
        params.page_size = Some(1);

        let response = process_get_vaults(&ds, None, params).await.unwrap();

        assert_eq!(response.vaults.len(), 1);
        assert_eq!(response.vaults[0].id, "1");
        assert_eq!(response.pagination.page, 2);
        assert_eq!(response.pagination.page_size, 1);
        assert_eq!(response.pagination.total_items, 3);
        assert!(response.pagination.has_more);
    }

    #[rocket::async_test]
    async fn get_vaults_rejects_invalid_owner_and_token() {
        let ds = MockVaultsDataSource::default();

        let err = process_get_vaults(&ds, None, params("not-an-address"))
            .await
            .unwrap_err();
        assert!(matches!(err, ApiError::BadRequest(_)));

        let mut params = params(&OWNER.to_string());
        params.token = Some("not-a-token".to_string());
        let err = process_get_vaults(&ds, None, params).await.unwrap_err();
        assert!(matches!(err, ApiError::BadRequest(_)));
    }

    #[rocket::async_test]
    async fn get_vaults_requires_owner() {
        let ds = MockVaultsDataSource::default();
        let err = process_get_vaults(&ds, None, VaultsQueryParams::default())
            .await
            .unwrap_err();
        assert!(matches!(err, ApiError::BadRequest(_)));
    }

    #[rocket::async_test]
    async fn get_vaults_maps_data_source_error() {
        let ds = MockVaultsDataSource {
            error: Some(ApiError::Internal("subgraph error".into())),
            ..Default::default()
        };
        let err = process_get_vaults(&ds, None, params(&OWNER.to_string()))
            .await
            .unwrap_err();
        assert!(matches!(err, ApiError::Internal(_)));
    }

    #[rocket::async_test]
    async fn get_vault_totals_aggregates_non_zero_by_token() {
        let mut polygon_vault = vault("4", OTHER_OWNER, token(TOKEN_A, "USDC", 6), 13, 7);
        polygon_vault.chain_id = 137;
        let ds = MockVaultsDataSource {
            vaults: vec![
                vault("1", OWNER, token(TOKEN_A, "USDC", 6), 7, 1),
                vault("2", OTHER_OWNER, token(TOKEN_A, "USDC", 6), 5, 3),
                vault("3", OWNER, token(TOKEN_B, "WETH", 18), 11, 5),
                polygon_vault,
            ],
            ..Default::default()
        };

        let response = process_get_vault_totals(&ds, None).await.unwrap();

        assert_eq!(response.totals.len(), 3);
        assert_eq!(response.totals[0].chain_id, 137);
        assert_eq!(response.totals[0].token.address, TOKEN_A);
        assert_eq!(response.totals[0].total_balance, "13");
        assert_eq!(response.totals[0].vault_count, 1);
        assert_eq!(response.totals[1].chain_id, 8453);
        assert_eq!(response.totals[1].token.address, TOKEN_A);
        assert_eq!(response.totals[1].total_balance, "12");
        assert_eq!(response.totals[1].vault_count, 2);
        assert_eq!(response.totals[2].chain_id, 8453);
        assert_eq!(response.totals[2].token.address, TOKEN_B);
        assert_eq!(response.totals[2].total_balance, "11");
        assert_eq!(response.totals[2].vault_count, 1);
    }

    #[rocket::async_test]
    async fn get_vault_totals_skips_zero_balances() {
        let ds = MockVaultsDataSource {
            vaults: vec![
                vault("1", OWNER, token(TOKEN_A, "USDC", 6), 0, 1),
                vault("2", OWNER, token(TOKEN_A, "USDC", 6), 9, 3),
            ],
            ..Default::default()
        };

        let response = process_get_vault_totals(&ds, None).await.unwrap();

        assert_eq!(response.totals.len(), 1);
        assert_eq!(response.totals[0].total_balance, "9");
        assert_eq!(response.totals[0].vault_count, 1);
    }

    type VaultChangesCall = (u32, Address, Bytes, u16);

    #[derive(Clone, Default)]
    struct MockVaultChangesDataSource {
        /// None = vault unknown.
        changes: Option<Vec<VaultChangeRecord>>,
        error: Option<ApiError>,
        calls: Arc<Mutex<Vec<VaultChangesCall>>>,
    }

    #[async_trait]
    impl VaultChangesDataSource for MockVaultChangesDataSource {
        async fn get_vault_changes(
            &self,
            chain_id: u32,
            raindex: Address,
            vault: Bytes,
            page: u16,
        ) -> Result<Option<Vec<VaultChangeRecord>>, ApiError> {
            self.calls
                .lock()
                .unwrap()
                .push((chain_id, raindex, vault, page));
            if let Some(error) = &self.error {
                return Err(error.clone());
            }
            Ok(self.changes.clone())
        }
    }

    const VAULT: &str = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    fn float(value: &str) -> Float {
        Float::parse(value.to_string()).unwrap()
    }

    fn change(
        change_type: RaindexVaultBalanceChangeType,
        amount: &str,
        old_balance: &str,
        new_balance: &str,
        timestamp: u64,
        tx_seed: u8,
    ) -> VaultChangeRecord {
        VaultChangeRecord {
            change_type,
            amount: float(amount),
            old_balance: float(old_balance),
            new_balance: float(new_balance),
            token: TOKEN_A,
            decimals: 6,
            timestamp,
            tx_hash: B256::from([tx_seed; 32]),
            sender: OWNER,
        }
    }

    /// Deposit 10 USDC, fill takes 1.5 out, withdraw 2.25, fill brings 0.000001 in.
    /// Given oldest first, as a source might.
    fn usdc_history() -> Vec<VaultChangeRecord> {
        vec![
            change(
                RaindexVaultBalanceChangeType::Deposit,
                "10",
                "0",
                "10",
                100,
                1,
            ),
            change(
                RaindexVaultBalanceChangeType::TakeOrder,
                "-1.5",
                "10",
                "8.5",
                200,
                2,
            ),
            change(
                RaindexVaultBalanceChangeType::Withdrawal,
                "-2.25",
                "8.5",
                "6.25",
                300,
                3,
            ),
            change(
                RaindexVaultBalanceChangeType::Clear,
                "0.000001",
                "6.25",
                "6.250001",
                400,
                4,
            ),
        ]
    }

    #[rocket::async_test]
    async fn vault_changes_raw_signed_units_newest_first() {
        let ds = MockVaultChangesDataSource {
            changes: Some(usdc_history()),
            ..Default::default()
        };

        let response = process_get_vault_changes(&ds, 8453, &ORDERBOOK.to_string(), VAULT, None)
            .await
            .unwrap();

        assert_eq!(response.chain_id, 8453);
        assert_eq!(response.vault, VAULT);
        assert_eq!(response.page, 1);
        let rows: Vec<(&str, &str, &str, &str, &str, u64)> = response
            .changes
            .iter()
            .map(|c| {
                (
                    c.change_type.as_str(),
                    c.kind.as_str(),
                    c.amount.as_str(),
                    c.old_balance.as_str(),
                    c.new_balance.as_str(),
                    c.timestamp,
                )
            })
            .collect();
        assert_eq!(
            rows,
            vec![
                ("clear", "Clear", "1", "6250000", "6250001", 400),
                (
                    "withdrawal",
                    "Withdrawal",
                    "-2250000",
                    "8500000",
                    "6250000",
                    300
                ),
                (
                    "takeOrder",
                    "Take order",
                    "-1500000",
                    "10000000",
                    "8500000",
                    200
                ),
                ("deposit", "Deposit", "10000000", "0", "10000000", 100),
            ]
        );
        let deposit = &response.changes[3];
        assert_eq!(deposit.token, TOKEN_A.to_string());
        assert_eq!(deposit.decimals, 6);
        assert_eq!(deposit.sender, OWNER.to_string());
        assert_eq!(deposit.tx_hash, B256::from([1u8; 32]).to_string());

        // Balances chain: each row's oldBalance + amount == newBalance (raw units).
        for c in &response.changes {
            let old: i128 = c.old_balance.parse().unwrap();
            let amount: i128 = c.amount.parse().unwrap();
            let new: i128 = c.new_balance.parse().unwrap();
            assert_eq!(old + amount, new, "{c:?}");
        }
    }

    #[rocket::async_test]
    async fn vault_changes_serialises_camel_case() {
        let ds = MockVaultChangesDataSource {
            changes: Some(vec![change(
                RaindexVaultBalanceChangeType::TakeOrder,
                "-1.5",
                "10",
                "8.5",
                200,
                2,
            )]),
            ..Default::default()
        };
        let response = process_get_vault_changes(&ds, 8453, &ORDERBOOK.to_string(), VAULT, Some(2))
            .await
            .unwrap();
        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(json["chainId"], 8453);
        assert_eq!(json["page"], 2);
        let row = &json["changes"][0];
        assert_eq!(row["changeType"], "takeOrder");
        assert_eq!(row["kind"], "Take order");
        assert_eq!(row["amount"], "-1500000");
        assert_eq!(row["oldBalance"], "10000000");
        assert_eq!(row["newBalance"], "8500000");
        assert_eq!(row["decimals"], 6);
        assert_eq!(row["timestamp"], 200);
        assert!(row["txHash"].as_str().unwrap().starts_with("0x0202"));
    }

    #[rocket::async_test]
    async fn vault_changes_keeps_source_order_within_one_timestamp() {
        // Two fills in one block: the source orders them (log index desc); keep that.
        let ds = MockVaultChangesDataSource {
            changes: Some(vec![
                change(
                    RaindexVaultBalanceChangeType::TakeOrder,
                    "-1",
                    "9",
                    "8",
                    500,
                    7,
                ),
                change(
                    RaindexVaultBalanceChangeType::TakeOrder,
                    "-1",
                    "10",
                    "9",
                    500,
                    6,
                ),
                change(
                    RaindexVaultBalanceChangeType::Deposit,
                    "10",
                    "0",
                    "10",
                    100,
                    5,
                ),
            ]),
            ..Default::default()
        };
        let response = process_get_vault_changes(&ds, 8453, &ORDERBOOK.to_string(), VAULT, None)
            .await
            .unwrap();
        let new_balances: Vec<&str> = response
            .changes
            .iter()
            .map(|c| c.new_balance.as_str())
            .collect();
        assert_eq!(new_balances, vec!["8000000", "9000000", "10000000"]);
    }

    #[rocket::async_test]
    async fn vault_changes_18_decimals() {
        let mut record = change(
            RaindexVaultBalanceChangeType::Withdrawal,
            "-0.123456789012345678",
            "1",
            "0.876543210987654322",
            1,
            1,
        );
        record.decimals = 18;
        let ds = MockVaultChangesDataSource {
            changes: Some(vec![record]),
            ..Default::default()
        };
        let response = process_get_vault_changes(&ds, 8453, &ORDERBOOK.to_string(), VAULT, None)
            .await
            .unwrap();
        assert_eq!(response.changes[0].amount, "-123456789012345678");
        assert_eq!(response.changes[0].old_balance, "1000000000000000000");
        assert_eq!(response.changes[0].new_balance, "876543210987654322");
    }

    #[rocket::async_test]
    async fn vault_changes_empty_vault() {
        let ds = MockVaultChangesDataSource {
            changes: Some(vec![]),
            ..Default::default()
        };
        let response = process_get_vault_changes(&ds, 8453, &ORDERBOOK.to_string(), VAULT, None)
            .await
            .unwrap();
        assert!(response.changes.is_empty());
        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(json["changes"], serde_json::json!([]));
    }

    #[rocket::async_test]
    async fn vault_changes_passes_params_to_source() {
        let ds = MockVaultChangesDataSource {
            changes: Some(vec![]),
            ..Default::default()
        };
        process_get_vault_changes(&ds, 46630, &ORDERBOOK.to_string(), VAULT, Some(0))
            .await
            .unwrap();
        let calls = ds.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, 46630);
        assert_eq!(calls[0].1, ORDERBOOK);
        assert_eq!(calls[0].2, VAULT.parse::<Bytes>().unwrap());
        assert_eq!(calls[0].3, 1, "page 0 clamps to 1");
    }

    #[rocket::async_test]
    async fn vault_changes_unknown_vault_is_404() {
        let ds = MockVaultChangesDataSource::default();
        let err = process_get_vault_changes(&ds, 8453, &ORDERBOOK.to_string(), VAULT, None)
            .await
            .unwrap_err();
        assert!(matches!(err, ApiError::NotFound(_)));
    }

    #[rocket::async_test]
    async fn vault_changes_rejects_bad_input_without_calling_source() {
        let ds = MockVaultChangesDataSource {
            changes: Some(vec![]),
            ..Default::default()
        };
        let err = process_get_vault_changes(&ds, 8453, "not-an-address", VAULT, None)
            .await
            .unwrap_err();
        assert!(matches!(err, ApiError::BadRequest(_)));
        let err = process_get_vault_changes(&ds, 8453, &ORDERBOOK.to_string(), "zz", None)
            .await
            .unwrap_err();
        assert!(matches!(err, ApiError::BadRequest(_)));
        assert!(ds.calls.lock().unwrap().is_empty());
    }

    #[rocket::async_test]
    async fn vault_changes_maps_source_error() {
        let ds = MockVaultChangesDataSource {
            error: Some(ApiError::Internal("db".into())),
            ..Default::default()
        };
        let err = process_get_vault_changes(&ds, 8453, &ORDERBOOK.to_string(), VAULT, None)
            .await
            .unwrap_err();
        assert!(matches!(err, ApiError::Internal(_)));
    }

    #[rocket::async_test]
    async fn vault_changes_route_is_mounted_and_requires_auth() {
        let client = crate::test_helpers::TestClientBuilder::new().build().await;
        for version in ["v1", "v2"] {
            let response = client
                .get(format!(
                    "/{version}/vaults/8453/{ORDERBOOK}/{VAULT}/changes"
                ))
                .dispatch()
                .await;
            assert_eq!(response.status(), rocket::http::Status::Unauthorized);
        }
    }

    #[rocket::async_test]
    async fn totals_uses_empty_owner_filter_and_hide_zero_balance() {
        let ds = MockVaultsDataSource {
            vaults: vec![vault("1", OWNER, token(TOKEN_A, "USDC", 6), 1, 1)],
            ..Default::default()
        };

        process_get_vault_totals(&ds, None).await.unwrap();

        let calls = ds.calls.lock().unwrap();
        assert_eq!(calls[0].1.owners, Vec::<Address>::new());
        assert!(calls[0].1.hide_zero_balance);
    }
}

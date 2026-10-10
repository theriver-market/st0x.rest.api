//! Owner-wide reads. `/vault-changes` returns every balance change of every vault an
//! owner holds on one chain in one response, so The River's strategy P&L makes one
//! call per owner instead of one per vault (research/OWN-INDEXER.md).

use crate::auth::AuthenticatedKey;
use crate::error::{ApiError, ApiErrorResponse};
use crate::fairings::{GlobalRateLimit, TracingSpan};
use crate::routes::vaults::{
    parse_address, sdk_vault_change_records, vault_change_response, VaultChangeRecord,
};
use crate::types::vaults::{OwnerVaultChangesEntry, OwnerVaultChangesResponse};
use alloy::primitives::{Address, Bytes, B256, U256};
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use rain_math_float::Float;
use rain_orderbook_common::raindex_client::{
    types::ChainIds,
    vaults::{GetVaultsFilters, RaindexVault, RaindexVaultBalanceChangeType},
    RaindexClient,
};
use rocket::serde::json::Json;
use rocket::{Route, State};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{FromRow, SqlitePool};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;
use tracing::Instrument;

const OWNER_VAULT_CHANGES_SQL: &str = include_str!("owner_vault_changes.sql");
/// Vault list page size for the SDK fallback.
const FALLBACK_VAULTS_PAGE_SIZE: u16 = 1000;
/// Concurrent per-vault reads in the SDK fallback.
const FALLBACK_CONCURRENCY: usize = 8;

/// One vault of the owner with its changes, before conversion to raw units.
#[derive(Debug, Clone)]
pub(crate) struct OwnerVaultRecord {
    /// /v2/vaults `id` (hex).
    pub id: String,
    /// On-chain vault id.
    pub vault_id: U256,
    pub orderbook: Address,
    pub token: Address,
    pub decimals: u8,
    pub changes: Vec<VaultChangeRecord>,
}

#[async_trait]
pub(crate) trait OwnerVaultChangesDataSource: Send + Sync {
    /// Every vault `owner` holds on `chain_id`, each with all its balance changes.
    async fn get_owner_vault_changes(
        &self,
        chain_id: u32,
        owner: Address,
    ) -> Result<Vec<OwnerVaultRecord>, ApiError>;
}

pub(crate) async fn process_get_owner_vault_changes(
    ds: &dyn OwnerVaultChangesDataSource,
    chain_id: u32,
    owner: &str,
) -> Result<OwnerVaultChangesResponse, ApiError> {
    let owner_address = parse_address(owner, "owner")?;
    let records = ds.get_owner_vault_changes(chain_id, owner_address).await?;
    let vaults = records
        .into_iter()
        .map(|mut vault| {
            // Newest first. Stable, so the source's block/log order holds within one timestamp.
            vault.changes.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
            Ok(OwnerVaultChangesEntry {
                id: vault.id,
                vault_id: vault.vault_id.to_string(),
                orderbook: vault.orderbook.to_string(),
                token: vault.token.to_string(),
                decimals: vault.decimals,
                changes: vault
                    .changes
                    .into_iter()
                    .map(vault_change_response)
                    .collect::<Result<Vec<_>, ApiError>>()?,
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    Ok(OwnerVaultChangesResponse {
        chain_id,
        owner: owner_address.to_string(),
        vaults,
    })
}

/// The SDK's vault `id`: raindex ++ owner ++ token ++ vault id (32 bytes, little endian),
/// as `RaindexVault::try_from_local_db` builds it.
pub(crate) fn local_db_vault_id(
    raindex: Address,
    owner: Address,
    token: Address,
    vault_id: U256,
) -> String {
    let mut id = Vec::with_capacity(20 * 3 + 32);
    id.extend_from_slice(raindex.as_slice());
    id.extend_from_slice(owner.as_slice());
    id.extend_from_slice(token.as_slice());
    id.extend_from_slice(&vault_id.to_le_bytes::<32>());
    Bytes::from(id).to_string()
}

#[derive(Debug, FromRow)]
#[sqlx(rename_all = "camelCase")]
struct OwnerVaultChangeRow {
    raindex_address: String,
    owner: String,
    token: String,
    vault_id: String,
    token_decimals: i64,
    transaction_hash: Option<String>,
    block_timestamp: Option<i64>,
    transaction_sender: Option<String>,
    change_type: Option<String>,
    delta: Option<String>,
    running_balance: Option<String>,
}

fn row_error(what: &'static str) -> impl Fn(String) -> ApiError {
    move |error| {
        tracing::error!(error = %error, what, "invalid owner vault change row");
        ApiError::Internal("failed to read vault changes".into())
    }
}

fn parse_row_address(value: &str, what: &'static str) -> Result<Address, ApiError> {
    Address::from_str(value).map_err(|e| row_error(what)(e.to_string()))
}

fn parse_row_float(value: &str, what: &'static str) -> Result<Float, ApiError> {
    Float::from_hex(value).map_err(|e| row_error(what)(e.to_string()))
}

/// Groups the SQL rows (ordered by vault, then newest change first) into vaults.
fn group_rows(rows: Vec<OwnerVaultChangeRow>) -> Result<Vec<OwnerVaultRecord>, ApiError> {
    let mut vaults: Vec<OwnerVaultRecord> = Vec::new();
    for row in rows {
        let raindex = parse_row_address(&row.raindex_address, "raindex_address")?;
        let owner = parse_row_address(&row.owner, "owner")?;
        let token = parse_row_address(&row.token, "token")?;
        let vault_id =
            U256::from_str(&row.vault_id).map_err(|e| row_error("vault_id")(e.to_string()))?;
        let decimals = u8::try_from(row.token_decimals)
            .map_err(|e| row_error("token_decimals")(e.to_string()))?;
        let id = local_db_vault_id(raindex, owner, token, vault_id);

        if vaults.last().is_none_or(|last| last.id != id) {
            vaults.push(OwnerVaultRecord {
                id,
                vault_id,
                orderbook: raindex,
                token,
                decimals,
                changes: Vec::new(),
            });
        }

        // NULL change columns: a vault with no changes yet (LEFT JOIN).
        let Some(tx_hash) = row.transaction_hash else {
            continue;
        };
        let change_type = row
            .change_type
            .ok_or_else(|| row_error("change_type")("missing".into()))?;
        let delta = row
            .delta
            .ok_or_else(|| row_error("delta")("missing".into()))?;
        let running_balance = row
            .running_balance
            .ok_or_else(|| row_error("running_balance")("missing".into()))?;
        let amount = parse_row_float(&delta, "delta")?;
        let new_balance = parse_row_float(&running_balance, "running_balance")?;
        let old_balance =
            (new_balance - amount).map_err(|e| row_error("old_balance")(e.to_string()))?;
        let sender = match row.transaction_sender {
            Some(sender) => parse_row_address(&sender, "transaction_sender")?,
            None => owner,
        };
        let change = VaultChangeRecord {
            change_type: RaindexVaultBalanceChangeType::try_from(change_type)
                .map_err(|e| row_error("change_type")(e.to_string()))?,
            amount,
            old_balance,
            new_balance,
            token,
            decimals,
            timestamp: row
                .block_timestamp
                .and_then(|t| u64::try_from(t).ok())
                .unwrap_or(0),
            tx_hash: B256::from_str(&tx_hash)
                .map_err(|e| row_error("transaction_hash")(e.to_string()))?,
            sender,
        };
        if let Some(vault) = vaults.last_mut() {
            vault.changes.push(change);
        }
    }
    Ok(vaults)
}

/// One query against the Raindex local DB for all of an owner's vaults and changes.
pub(crate) async fn fetch_owner_vault_changes_local(
    pool: &SqlitePool,
    chain_id: u32,
    owner: Address,
) -> Result<Vec<OwnerVaultRecord>, ApiError> {
    let rows = sqlx::query_as::<_, OwnerVaultChangeRow>(OWNER_VAULT_CHANGES_SQL)
        .bind(i64::from(chain_id))
        .bind(alloy::hex::encode_prefixed(owner))
        .fetch_all(pool)
        .await
        .map_err(|error| {
            tracing::error!(error = %error, chain_id, "owner vault changes query failed");
            ApiError::Internal("failed to read vault changes".into())
        })?;
    group_rows(rows)
}

async fn open_local_db(path: &Path) -> Result<SqlitePool, sqlx::Error> {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .read_only(true)
        .busy_timeout(Duration::from_secs(5));
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
}

pub(crate) struct RaindexOwnerVaultChangesDataSource<'a> {
    pub client: &'a RaindexClient,
    pub db_path: Option<PathBuf>,
}

impl RaindexOwnerVaultChangesDataSource<'_> {
    /// True when the SDK itself would answer this chain from the local DB.
    async fn local_db_ready(&self, chain_id: u32) -> bool {
        if self.db_path.is_none() {
            return false;
        }
        match self.client.get_local_db_sync_snapshot().await {
            Ok(snapshot) => snapshot
                .networks
                .iter()
                .any(|network| network.chain_id == chain_id && network.ready),
            Err(error) => {
                tracing::warn!(error = %error, chain_id, "local db snapshot unavailable");
                false
            }
        }
    }

    /// Chains not served by the local DB: vault list through the SDK, then each vault's
    /// changes with bounded concurrency (subgraph-backed chains return the first page).
    async fn via_sdk(
        &self,
        chain_id: u32,
        owner: Address,
    ) -> Result<Vec<OwnerVaultRecord>, ApiError> {
        let filters = GetVaultsFilters {
            owners: vec![owner],
            hide_zero_balance: false,
            ..Default::default()
        };
        let mut vaults: Vec<RaindexVault> = Vec::new();
        let mut page: u16 = 1;
        loop {
            let response = self
                .client
                .get_vaults(
                    Some(ChainIds(vec![chain_id])),
                    Some(filters.clone()),
                    Some(page),
                    Some(FALLBACK_VAULTS_PAGE_SIZE),
                )
                .await
                .map_err(|error| {
                    tracing::error!(error = %error, chain_id, page, "failed to list owner vaults");
                    ApiError::Internal("failed to retrieve vaults".into())
                })?;
            let has_more = response.has_more();
            vaults.extend(response.items());
            if !has_more {
                break;
            }
            page = page.checked_add(1).ok_or_else(|| {
                tracing::error!(chain_id, "owner vault pagination exhausted u16 page range");
                ApiError::Internal("owner vault pagination exceeded maximum".into())
            })?;
        }
        futures::stream::iter(vaults)
            .map(|vault| async move {
                let changes = sdk_vault_change_records(&vault, None).await?;
                Ok::<_, ApiError>(OwnerVaultRecord {
                    id: vault.id().to_string(),
                    vault_id: vault.vault_id(),
                    orderbook: vault.raindex(),
                    token: vault.token().address(),
                    decimals: vault.token().decimals(),
                    changes,
                })
            })
            .buffered(FALLBACK_CONCURRENCY)
            .try_collect()
            .await
    }
}

#[async_trait]
impl OwnerVaultChangesDataSource for RaindexOwnerVaultChangesDataSource<'_> {
    async fn get_owner_vault_changes(
        &self,
        chain_id: u32,
        owner: Address,
    ) -> Result<Vec<OwnerVaultRecord>, ApiError> {
        if let (true, Some(path)) = (self.local_db_ready(chain_id).await, &self.db_path) {
            match open_local_db(path).await {
                Ok(pool) => {
                    tracing::info!(chain_id, "owner vault changes from local db (one query)");
                    let result = fetch_owner_vault_changes_local(&pool, chain_id, owner).await;
                    pool.close().await;
                    return result;
                }
                Err(error) => {
                    tracing::warn!(error = %error, chain_id, "local db open failed; using sdk");
                }
            }
        }
        tracing::info!(chain_id, "owner vault changes through the sdk (per vault)");
        self.via_sdk(chain_id, owner).await
    }
}

#[utoipa::path(
    get,
    path = "/v1/owners/{chain_id}/{owner}/vault-changes",
    tag = "Vaults",
    security(("basicAuth" = [])),
    params(
        ("chain_id" = u32, Path, description = "Chain id"),
        ("owner" = String, Path, description = "Vault owner address"),
    ),
    responses(
        (status = 200, description = "Every vault the owner holds on the chain, each with its deposits, withdrawals and fills newest first, in raw signed token units", body = OwnerVaultChangesResponse),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 429, description = "Rate limited", body = ApiErrorResponse),
        (status = 500, description = "Internal server error", body = ApiErrorResponse),
    )
)]
#[get("/<chain_id>/<owner>/vault-changes")]
pub async fn get_owner_vault_changes(
    _global: GlobalRateLimit,
    _key: AuthenticatedKey,
    shared_raindex: &State<crate::raindex::SharedRaindexProvider>,
    span: TracingSpan,
    chain_id: u32,
    owner: &str,
) -> Result<Json<OwnerVaultChangesResponse>, ApiError> {
    async move {
        tracing::info!(chain_id, owner, "request received");
        let provider = shared_raindex.read().await;
        let ds = RaindexOwnerVaultChangesDataSource {
            client: provider.client(),
            db_path: provider.db_path(),
        };
        let response = process_get_owner_vault_changes(&ds, chain_id, owner)
            .await
            .map_err(|error| {
                tracing::warn!(chain_id, owner, error = %error, "get_owner_vault_changes failed");
                error
            })?;
        tracing::info!(
            vault_count = response.vaults.len(),
            change_count = response
                .vaults
                .iter()
                .map(|vault| vault.changes.len())
                .sum::<usize>(),
            "returning owner vault changes"
        );
        Ok(Json(response))
    }
    .instrument(span.0)
    .await
}

pub fn routes() -> Vec<Route> {
    rocket::routes![get_owner_vault_changes]
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::address;
    use std::sync::{Arc, Mutex};

    const OWNER: Address = address!("1111111111111111111111111111111111111111");
    const OTHER: Address = address!("2222222222222222222222222222222222222222");
    const TAKER: Address = address!("3333333333333333333333333333333333333333");
    const USDC: Address = address!("833589fcd6edb6e08f4c7c32d4f71b54bda02913");
    const WETH: Address = address!("4200000000000000000000000000000000000006");
    const RAINDEX: Address = address!("e522cb4a5fcb2eb31a52ff41a4653d85a4fd7c9d");

    fn float(value: &str) -> Float {
        Float::parse(value.to_string()).unwrap()
    }

    fn change(
        change_type: RaindexVaultBalanceChangeType,
        amount: &str,
        old_balance: &str,
        new_balance: &str,
        decimals: u8,
        timestamp: u64,
        tx_seed: u8,
    ) -> VaultChangeRecord {
        VaultChangeRecord {
            change_type,
            amount: float(amount),
            old_balance: float(old_balance),
            new_balance: float(new_balance),
            token: USDC,
            decimals,
            timestamp,
            tx_hash: B256::from([tx_seed; 32]),
            sender: OWNER,
        }
    }

    #[derive(Clone, Default)]
    struct MockOwnerSource {
        vaults: Vec<OwnerVaultRecord>,
        error: Option<ApiError>,
        calls: Arc<Mutex<Vec<(u32, Address)>>>,
    }

    #[async_trait]
    impl OwnerVaultChangesDataSource for MockOwnerSource {
        async fn get_owner_vault_changes(
            &self,
            chain_id: u32,
            owner: Address,
        ) -> Result<Vec<OwnerVaultRecord>, ApiError> {
            self.calls.lock().unwrap().push((chain_id, owner));
            if let Some(error) = &self.error {
                return Err(error.clone());
            }
            Ok(self.vaults.clone())
        }
    }

    fn two_vaults() -> Vec<OwnerVaultRecord> {
        vec![
            OwnerVaultRecord {
                id: local_db_vault_id(RAINDEX, OWNER, USDC, U256::from(1)),
                vault_id: U256::from(1),
                orderbook: RAINDEX,
                token: USDC,
                decimals: 6,
                // Oldest first, as a source might give them.
                changes: vec![
                    change(
                        RaindexVaultBalanceChangeType::Deposit,
                        "10",
                        "0",
                        "10",
                        6,
                        100,
                        1,
                    ),
                    change(
                        RaindexVaultBalanceChangeType::TakeOrder,
                        "-1.5",
                        "10",
                        "8.5",
                        6,
                        200,
                        2,
                    ),
                ],
            },
            OwnerVaultRecord {
                id: local_db_vault_id(RAINDEX, OWNER, WETH, U256::from(2)),
                vault_id: U256::from(2),
                orderbook: RAINDEX,
                token: WETH,
                decimals: 18,
                changes: vec![],
            },
        ]
    }

    #[rocket::async_test]
    async fn owner_changes_shape_and_raw_units() {
        let ds = MockOwnerSource {
            vaults: two_vaults(),
            ..Default::default()
        };
        let response =
            process_get_owner_vault_changes(&ds, 8453, &OWNER.to_string().to_lowercase())
                .await
                .unwrap();
        assert_eq!(response.chain_id, 8453);
        assert_eq!(response.owner, OWNER.to_string());
        assert_eq!(response.vaults.len(), 2);

        let usdc = &response.vaults[0];
        assert_eq!(usdc.vault_id, "1");
        assert_eq!(usdc.orderbook, RAINDEX.to_string());
        assert_eq!(usdc.token, USDC.to_string());
        assert_eq!(usdc.decimals, 6);
        let rows: Vec<(&str, &str, &str, &str, u64)> = usdc
            .changes
            .iter()
            .map(|c| {
                (
                    c.change_type.as_str(),
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
                ("takeOrder", "-1500000", "10000000", "8500000", 200),
                ("deposit", "10000000", "0", "10000000", 100),
            ]
        );
        assert!(response.vaults[1].changes.is_empty());

        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(json["chainId"], 8453);
        assert_eq!(json["vaults"][0]["vaultId"], "1");
        assert_eq!(json["vaults"][0]["changes"][0]["changeType"], "takeOrder");
        assert_eq!(json["vaults"][0]["changes"][0]["oldBalance"], "10000000");
        assert_eq!(json["vaults"][1]["changes"], serde_json::json!([]));
    }

    #[rocket::async_test]
    async fn owner_changes_passes_chain_and_owner() {
        let ds = MockOwnerSource::default();
        let response = process_get_owner_vault_changes(&ds, 4663, &OWNER.to_string())
            .await
            .unwrap();
        assert!(response.vaults.is_empty());
        assert_eq!(*ds.calls.lock().unwrap(), vec![(4663, OWNER)]);
    }

    #[rocket::async_test]
    async fn owner_changes_rejects_bad_owner_without_calling_source() {
        let ds = MockOwnerSource::default();
        let err = process_get_owner_vault_changes(&ds, 8453, "not-an-address")
            .await
            .unwrap_err();
        assert!(matches!(err, ApiError::BadRequest(_)));
        assert!(ds.calls.lock().unwrap().is_empty());
    }

    #[rocket::async_test]
    async fn owner_changes_maps_source_error() {
        let ds = MockOwnerSource {
            error: Some(ApiError::Internal("db".into())),
            ..Default::default()
        };
        let err = process_get_owner_vault_changes(&ds, 8453, &OWNER.to_string())
            .await
            .unwrap_err();
        assert!(matches!(err, ApiError::Internal(_)));
    }

    #[test]
    fn vault_id_matches_sdk_layout() {
        let id = local_db_vault_id(RAINDEX, OWNER, USDC, U256::from(0xfab));
        let expected = format!(
            "0x{}{}{}ab0f{}",
            "e522cb4a5fcb2eb31a52ff41a4653d85a4fd7c9d",
            "1111111111111111111111111111111111111111",
            "833589fcd6edb6e08f4c7c32d4f71b54bda02913",
            "00".repeat(30)
        );
        assert_eq!(id, expected);
    }

    #[rocket::async_test]
    async fn owner_changes_route_is_mounted_and_requires_auth() {
        let client = crate::test_helpers::TestClientBuilder::new().build().await;
        for version in ["v1", "v2"] {
            let response = client
                .get(format!("/{version}/owners/8453/{OWNER}/vault-changes"))
                .dispatch()
                .await;
            assert_eq!(response.status(), rocket::http::Status::Unauthorized);
        }
    }

    // ---- local DB query against the real Raindex schema ----

    async fn local_pool() -> SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::from_str(&format!(
                    "sqlite:file:{}?mode=memory&cache=shared",
                    uuid::Uuid::new_v4()
                ))
                .unwrap(),
            )
            .await
            .unwrap();
        sqlx::raw_sql(rain_orderbook_common::local_db::query::create_tables::create_tables_sql())
            .execute(&pool)
            .await
            .unwrap();
        pool
    }

    fn hex_addr(a: Address) -> String {
        alloy::hex::encode_prefixed(a)
    }

    fn hex_u256(v: u64) -> String {
        alloy::hex::encode_prefixed(B256::from(U256::from(v)))
    }

    fn hex_float(v: &str) -> String {
        float(v).as_hex()
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_change(
        pool: &SqlitePool,
        owner: Address,
        token: Address,
        vault: u64,
        block: i64,
        log_index: i64,
        change_type: &str,
        delta: &str,
        running: &str,
        tx_seed: u8,
    ) {
        sqlx::query(
            "INSERT INTO vault_balance_changes (chain_id, raindex_address, transaction_hash, owner, token, vault_id, block_number, block_timestamp, log_index, change_type, delta, running_balance) VALUES (8453, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(hex_addr(RAINDEX))
        .bind(alloy::hex::encode_prefixed(B256::from([tx_seed; 32])))
        .bind(hex_addr(owner))
        .bind(hex_addr(token))
        .bind(hex_u256(vault))
        .bind(block)
        .bind(block * 2)
        .bind(log_index)
        .bind(change_type)
        .bind(hex_float(delta))
        .bind(hex_float(running))
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_rvb(pool: &SqlitePool, owner: Address, token: Address, vault: u64, bal: &str) {
        sqlx::query(
            "INSERT INTO running_vault_balances (chain_id, raindex_address, owner, token, vault_id, balance, last_block, last_log_index) VALUES (8453, ?, ?, ?, ?, ?, 1, 1)",
        )
        .bind(hex_addr(RAINDEX))
        .bind(hex_addr(owner))
        .bind(hex_addr(token))
        .bind(hex_u256(vault))
        .bind(hex_float(bal))
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_token(pool: &SqlitePool, token: Address, symbol: &str, decimals: i64) {
        sqlx::query(
            "INSERT INTO erc20_tokens (chain_id, raindex_address, token_address, name, symbol, decimals) VALUES (8453, ?, ?, ?, ?, ?)",
        )
        .bind(hex_addr(RAINDEX))
        .bind(hex_addr(token))
        .bind(symbol)
        .bind(symbol)
        .bind(decimals)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn seed(pool: &SqlitePool) {
        insert_token(pool, USDC, "USDC", 6).await;
        insert_token(pool, WETH, "WETH", 18).await;
        // OWNER, USDC vault 1: deposit 10, a fill takes 1.5 out.
        insert_rvb(pool, OWNER, USDC, 1, "8.5").await;
        insert_change(pool, OWNER, USDC, 1, 10, 0, "DEPOSIT", "10", "10", 1).await;
        insert_change(pool, OWNER, USDC, 1, 20, 3, "TAKE_OUTPUT", "-1.5", "8.5", 2).await;
        sqlx::query(
            "INSERT INTO take_orders (chain_id, raindex_address, transaction_hash, log_index, block_number, block_timestamp, sender, order_owner, order_nonce, input_io_index, output_io_index, taker_input, taker_output) VALUES (8453, ?, ?, 3, 20, 40, ?, ?, ?, 0, 0, ?, ?)",
        )
        .bind(hex_addr(RAINDEX))
        .bind(alloy::hex::encode_prefixed(B256::from([2u8; 32])))
        .bind(hex_addr(TAKER))
        .bind(hex_addr(OWNER))
        .bind(hex_u256(1))
        .bind(hex_float("1"))
        .bind(hex_float("1.5"))
        .execute(pool)
        .await
        .unwrap();
        // OWNER, WETH vault 2: withdrawal only.
        insert_rvb(pool, OWNER, WETH, 2, "0").await;
        insert_change(
            pool,
            OWNER,
            WETH,
            2,
            30,
            1,
            "WITHDRAW",
            "-0.123456789012345678",
            "0",
            3,
        )
        .await;
        // Someone else's vault: must not appear.
        insert_rvb(pool, OTHER, USDC, 1, "5").await;
        insert_change(pool, OTHER, USDC, 1, 11, 0, "DEPOSIT", "5", "5", 4).await;
    }

    #[rocket::async_test]
    async fn local_query_groups_vaults_and_changes_in_one_query() {
        let pool = local_pool().await;
        seed(&pool).await;

        let vaults = fetch_owner_vault_changes_local(&pool, 8453, OWNER)
            .await
            .unwrap();
        assert_eq!(vaults.len(), 2, "{vaults:?}");

        let usdc = vaults.iter().find(|v| v.token == USDC).unwrap();
        assert_eq!(
            usdc.id,
            local_db_vault_id(RAINDEX, OWNER, USDC, U256::from(1))
        );
        assert_eq!(usdc.vault_id, U256::from(1));
        assert_eq!(usdc.orderbook, RAINDEX);
        assert_eq!(usdc.decimals, 6);
        assert_eq!(usdc.changes.len(), 2);
        // Newest first from SQL.
        assert_eq!(
            usdc.changes[0].change_type,
            RaindexVaultBalanceChangeType::TakeOrder
        );
        assert_eq!(usdc.changes[0].sender, TAKER, "sender from take_orders");
        assert_eq!(
            usdc.changes[1].change_type,
            RaindexVaultBalanceChangeType::Deposit
        );
        assert_eq!(usdc.changes[1].sender, OWNER, "falls back to vault owner");
        assert_eq!(usdc.changes[0].timestamp, 40);

        let response = process_get_owner_vault_changes(
            &StaticSource(vaults.clone()),
            8453,
            &OWNER.to_string(),
        )
        .await
        .unwrap();
        let usdc = response.vaults.iter().find(|v| v.decimals == 6).unwrap();
        let rows: Vec<(&str, &str, &str)> = usdc
            .changes
            .iter()
            .map(|c| {
                (
                    c.amount.as_str(),
                    c.old_balance.as_str(),
                    c.new_balance.as_str(),
                )
            })
            .collect();
        assert_eq!(
            rows,
            vec![
                ("-1500000", "10000000", "8500000"),
                ("10000000", "0", "10000000")
            ]
        );
        let weth = response.vaults.iter().find(|v| v.decimals == 18).unwrap();
        assert_eq!(weth.changes[0].change_type, "withdrawal");
        assert_eq!(weth.changes[0].amount, "-123456789012345678");
        assert_eq!(weth.changes[0].old_balance, "123456789012345678");
        assert_eq!(weth.changes[0].new_balance, "0");
    }

    #[rocket::async_test]
    async fn local_query_includes_order_vaults_without_changes() {
        let pool = local_pool().await;
        seed(&pool).await;
        // An order by OWNER referencing WETH vault 9, never deposited into.
        let tx = alloy::hex::encode_prefixed(B256::from([9u8; 32]));
        sqlx::query(
            "INSERT INTO order_events (chain_id, raindex_address, transaction_hash, log_index, block_number, block_timestamp, sender, interpreter_address, store_address, event_type, order_owner, order_nonce, order_bytes, order_hash) VALUES (8453, ?, ?, 5, 50, 100, ?, ?, ?, 'AddOrderV3', ?, ?, '0x', ?)",
        )
        .bind(hex_addr(RAINDEX))
        .bind(&tx)
        .bind(hex_addr(OWNER))
        .bind(hex_addr(Address::ZERO))
        .bind(hex_addr(Address::ZERO))
        .bind(hex_addr(OWNER))
        .bind(hex_u256(1))
        .bind(alloy::hex::encode_prefixed(B256::from([7u8; 32])))
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO order_ios (chain_id, raindex_address, transaction_hash, log_index, io_index, io_type, token, vault_id) VALUES (8453, ?, ?, 5, 0, 'input', ?, ?)",
        )
        .bind(hex_addr(RAINDEX))
        .bind(&tx)
        .bind(hex_addr(WETH))
        .bind(hex_u256(9))
        .execute(&pool)
        .await
        .unwrap();

        let vaults = fetch_owner_vault_changes_local(&pool, 8453, OWNER)
            .await
            .unwrap();
        assert_eq!(vaults.len(), 3);
        let empty = vaults.iter().find(|v| v.vault_id == U256::from(9)).unwrap();
        assert!(empty.changes.is_empty());
        assert_eq!(empty.decimals, 18);
    }

    #[rocket::async_test]
    async fn local_query_unknown_owner_and_other_chain_are_empty() {
        let pool = local_pool().await;
        seed(&pool).await;
        assert!(fetch_owner_vault_changes_local(&pool, 8453, TAKER)
            .await
            .unwrap()
            .is_empty());
        assert!(fetch_owner_vault_changes_local(&pool, 4663, OWNER)
            .await
            .unwrap()
            .is_empty());
    }

    struct StaticSource(Vec<OwnerVaultRecord>);

    #[async_trait]
    impl OwnerVaultChangesDataSource for StaticSource {
        async fn get_owner_vault_changes(
            &self,
            _chain_id: u32,
            _owner: Address,
        ) -> Result<Vec<OwnerVaultRecord>, ApiError> {
            Ok(self.0.clone())
        }
    }
}

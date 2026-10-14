use rain_orderbook_common::raindex_client::local_db::LocalDbStatus;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct HealthResponse {
    #[schema(example = "ok")]
    pub status: HealthStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    Ok,
    Degraded,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DetailedHealthResponse {
    /// Overall API status: "ok", "degraded", or "error"
    #[schema(example = "ok")]
    pub status: HealthStatus,

    /// st0x application database connectivity
    pub app_db: DbStatus,

    /// raindex local database sync status
    pub raindex: RaindexSyncStatus,

    /// Background ST0x market price sampler status
    pub market_prices: MarketPriceHealthStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DbStatus {
    /// Component status: "ok" or "error"
    #[schema(example = "ok")]
    pub status: DbHealthStatus,

    /// Whether the database is reachable
    #[schema(example = true)]
    pub connected: bool,

    /// Error message if not connected
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DbHealthStatus {
    Ok,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MarketPriceHealthStatus {
    #[schema(example = true)]
    pub enabled: bool,

    #[schema(example = true)]
    pub healthy: bool,

    #[schema(example = true)]
    pub running: bool,

    #[schema(nullable = true, example = 1784800000)]
    pub last_attempt_at: Option<i64>,

    #[schema(nullable = true, example = 1784800000)]
    pub last_success_at: Option<i64>,

    #[schema(example = 0)]
    pub consecutive_failures: u64,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RaindexSyncStatus {
    /// Local DB sync status: "active", "syncing", "failure", or "not_configured"
    #[schema(example = "active")]
    pub status: RaindexSyncStatusKind,

    /// Whether local DB sync is configured in raindex settings.
    #[schema(example = true)]
    pub configured: bool,

    /// Whether raindex reports the local DB sync as healthy.
    #[schema(example = true)]
    pub healthy: bool,

    /// Error message if raindex sync status could not be read or is failing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    /// Per-network sync status from raindex.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub networks: Vec<NetworkSyncInfo>,

    /// Per-orderbook sync status from raindex.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub orderbooks: Vec<OrderbookSyncInfo>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RaindexSyncStatusKind {
    Active,
    Syncing,
    Failure,
    NotConfigured,
}

impl From<LocalDbStatus> for RaindexSyncStatusKind {
    fn from(status: LocalDbStatus) -> Self {
        match status {
            LocalDbStatus::Active => Self::Active,
            LocalDbStatus::Syncing => Self::Syncing,
            LocalDbStatus::Failure => Self::Failure,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct NetworkSyncInfo {
    /// Chain ID (e.g. 8453 for Base)
    #[schema(example = 8453)]
    pub chain_id: u32,

    /// Network key from raindex settings.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network_key: Option<String>,

    /// Network sync status: "active", "syncing", or "failure"
    #[schema(example = "active")]
    pub status: RaindexSyncStatusKind,

    /// Number of configured orderbooks on this network.
    #[schema(example = 1)]
    pub orderbook_count: usize,

    /// Whether the network is ready for local DB reads.
    #[schema(example = true)]
    pub ready: bool,

    /// Error message if this network failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct OrderbookSyncInfo {
    /// Chain ID (e.g. 8453 for Base)
    #[schema(example = 8453)]
    pub chain_id: u32,

    /// Orderbook contract address
    #[schema(example = "0xd2938e7c9fe3597f78832ce780feb61945c377d7")]
    pub orderbook_address: String,

    /// Orderbook key from raindex settings.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub orderbook_key: Option<String>,

    /// Network key from raindex settings.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network_key: Option<String>,

    /// Orderbook sync status: "active", "syncing", or "failure"
    #[schema(example = "active")]
    pub status: RaindexSyncStatusKind,

    /// Whether this orderbook is ready for local DB reads.
    #[schema(example = true)]
    pub ready: bool,

    /// Current sync phase message, when syncing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase_message: Option<String>,

    /// Last block number persisted by raindex for this orderbook. Left out when unknown;
    /// `sync_position_error` then says why.
    #[schema(example = 12345678)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_synced_block: Option<u64>,

    /// UTC time (`YYYY-MM-DD HH:MM:SS`) raindex last wrote the persisted sync position.
    /// Left out when unknown; `sync_position_error` then says why.
    #[schema(example = "2026-10-13 11:15:02")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,

    /// Why `last_synced_block` / `updated_at` are missing. Left out when they are present.
    #[schema(example = "no sync watermark row for this orderbook")]
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub sync_position_error: Option<String>,

    /// Error message if this orderbook failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

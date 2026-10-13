use crate::db::DbPool;
use crate::error::ApiError;
use crate::fairings::TracingSpan;
use crate::market_price::{unix_now, MarketPriceState};
use crate::raindex::SharedRaindexProvider;
use crate::routes::owners::open_local_db;
use crate::types::health::{
    DbHealthStatus, DbStatus, DetailedHealthResponse, HealthResponse, HealthStatus,
    NetworkSyncInfo, OrderbookSyncInfo, RaindexSyncStatus, RaindexSyncStatusKind,
};
use rain_orderbook_common::raindex_client::local_db::{
    LocalDbSyncSnapshot, NetworkSyncStatusSnapshot, RaindexSyncStatusSnapshot,
};
use rocket::serde::json::Json;
use rocket::{Route, State};
use sqlx::FromRow;
use std::path::Path;
use std::time::Duration;
use tracing::Instrument;

/// Sync position per orderbook as the Raindex sync pipeline persists it. The pipeline's apply
/// step upserts `target_watermarks` in the same transaction as the synced data; the SDK
/// snapshot (`get_local_db_sync_snapshot`) reads `sync_status`, which the pipeline never
/// writes, so its `last_synced_block` / `updated_at` stay empty on a synced orderbook.
/// `updated_at` is stored as epoch milliseconds and returned in the `sync_status` text format
/// (UTC, `YYYY-MM-DD HH:MM:SS`).
const SYNC_WATERMARKS_SQL: &str = "SELECT chain_id, raindex_address, last_block, \
     strftime('%Y-%m-%d %H:%M:%S', updated_at / 1000, 'unixepoch') AS updated_at \
     FROM target_watermarks";

/// Upper bound on the sync position read, so a locked or slow local DB cannot hold up health.
const SYNC_POSITION_TIMEOUT: Duration = Duration::from_secs(2);

const NO_DB_PATH: &str = "raindex local DB path is not configured";
const DB_OPEN_FAILED: &str = "raindex local DB could not be opened";
const QUERY_FAILED: &str = "sync watermark query failed";
const READ_TIMED_OUT: &str = "sync watermark read timed out";
const NO_ROW: &str = "no sync watermark row for this orderbook";
const INVALID_ROW: &str = "sync watermark row has an invalid block";

#[derive(Debug, FromRow)]
struct SyncWatermarkRow {
    chain_id: i64,
    raindex_address: String,
    last_block: i64,
    updated_at: Option<String>,
}

#[utoipa::path(
    get,
    path = "/health",
    tag = "Health",
    responses(
        (status = 200, description = "Service is healthy", body = HealthResponse),
    )
)]
#[get("/health")]
pub async fn get_health(span: TracingSpan) -> Result<Json<HealthResponse>, ApiError> {
    async move {
        tracing::info!("request received");
        Ok(Json(HealthResponse {
            status: HealthStatus::Ok,
        }))
    }
    .instrument(span.0)
    .await
}

#[utoipa::path(
    get,
    path = "/health/detailed",
    tag = "Health",
    responses(
        (status = 200, description = "Detailed service health including sync status", body = DetailedHealthResponse),
    )
)]
#[get("/health/detailed")]
pub async fn get_health_detailed(
    span: TracingSpan,
    pool: &State<DbPool>,
    shared_raindex: &State<SharedRaindexProvider>,
    market_price_state: &State<MarketPriceState>,
) -> Result<Json<DetailedHealthResponse>, ApiError> {
    async move {
        tracing::info!("detailed health check request received");

        tracing::info!("checking application database and raindex local database");
        let (app_db, raindex) = tokio::join!(check_app_db(pool), check_raindex_db(shared_raindex));
        let market_prices = market_price_state.health_status(unix_now()?).await;

        let status = detailed_status(&app_db, &raindex, &market_prices);
        tracing::info!(status = ?status, "detailed health check completed");

        Ok(Json(DetailedHealthResponse {
            status,
            app_db,
            raindex,
            market_prices,
        }))
    }
    .instrument(span.0)
    .await
}

async fn check_app_db(pool: &DbPool) -> DbStatus {
    match sqlx::query("SELECT 1").execute(pool).await {
        Ok(_) => DbStatus {
            status: DbHealthStatus::Ok,
            connected: true,
            error: None,
        },
        Err(e) => {
            tracing::warn!(error = %e, "app database health check failed");
            DbStatus {
                status: DbHealthStatus::Error,
                connected: false,
                error: Some("application database unavailable".to_string()),
            }
        }
    }
}

async fn check_raindex_db(shared_raindex: &SharedRaindexProvider) -> RaindexSyncStatus {
    let (client, db_path) = {
        let raindex = shared_raindex.read().await;
        (raindex.client().clone(), raindex.db_path())
    };

    match client.get_local_db_sync_snapshot().await {
        Ok(snapshot) => {
            let mut status = map_raindex_snapshot(snapshot);
            fill_sync_watermarks(db_path.as_deref(), &mut status.orderbooks).await;
            status
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to get raindex local db sync snapshot");
            RaindexSyncStatus {
                status: RaindexSyncStatusKind::Failure,
                configured: false,
                healthy: false,
                error: Some("raindex local DB sync snapshot unavailable".to_string()),
                networks: vec![],
                orderbooks: vec![],
            }
        }
    }
}

/// Fills `last_synced_block` / `updated_at` from the local DB for every orderbook the SDK
/// snapshot left without them. Health must still answer when the read fails or hangs, so the
/// read is bounded and every failure is logged at warn and reported per orderbook in
/// `sync_position_error` instead of leaving the fields silently empty.
async fn fill_sync_watermarks(path: Option<&Path>, orderbooks: &mut [OrderbookSyncInfo]) {
    if orderbooks.iter().all(|o| o.last_synced_block.is_some()) {
        return;
    }
    let rows = match path {
        None => {
            tracing::warn!("raindex local db path not configured; no sync position");
            Err(NO_DB_PATH)
        }
        Some(path) => {
            match tokio::time::timeout(SYNC_POSITION_TIMEOUT, read_sync_watermarks_at(path)).await {
                Ok(rows) => rows,
                Err(_) => {
                    tracing::warn!(
                        timeout_ms = SYNC_POSITION_TIMEOUT.as_millis() as u64,
                        "raindex sync watermark read timed out; no sync position"
                    );
                    Err(READ_TIMED_OUT)
                }
            }
        }
    };
    match rows {
        Ok(rows) => apply_sync_watermarks(orderbooks, &rows),
        Err(reason) => {
            for orderbook in orderbooks
                .iter_mut()
                .filter(|o| o.last_synced_block.is_none())
            {
                orderbook.sync_position_error = Some(reason.to_string());
            }
        }
    }
}

async fn read_sync_watermarks_at(path: &Path) -> Result<Vec<SyncWatermarkRow>, &'static str> {
    let pool = open_local_db(path).await.map_err(|error| {
        tracing::warn!(error = %error, "raindex local db open failed; no sync position");
        DB_OPEN_FAILED
    })?;
    let rows = sqlx::query_as::<_, SyncWatermarkRow>(SYNC_WATERMARKS_SQL)
        .fetch_all(&pool)
        .await;
    pool.close().await;
    rows.map_err(|error| {
        tracing::warn!(error = %error, "raindex sync watermark query failed; no sync position");
        QUERY_FAILED
    })
}

fn apply_sync_watermarks(orderbooks: &mut [OrderbookSyncInfo], rows: &[SyncWatermarkRow]) {
    for orderbook in orderbooks
        .iter_mut()
        .filter(|o| o.last_synced_block.is_none())
    {
        let row = rows.iter().find(|row| {
            row.chain_id == i64::from(orderbook.chain_id)
                && row
                    .raindex_address
                    .eq_ignore_ascii_case(&orderbook.orderbook_address)
        });
        let Some(row) = row else {
            tracing::warn!(
                chain_id = orderbook.chain_id,
                orderbook_address = %orderbook.orderbook_address,
                "no sync watermark row for orderbook; no sync position"
            );
            orderbook.sync_position_error = Some(NO_ROW.to_string());
            continue;
        };
        let Ok(last_block) = u64::try_from(row.last_block) else {
            tracing::warn!(
                chain_id = orderbook.chain_id,
                orderbook_address = %orderbook.orderbook_address,
                last_block = row.last_block,
                "sync watermark row has an invalid block; no sync position"
            );
            orderbook.sync_position_error = Some(INVALID_ROW.to_string());
            continue;
        };
        orderbook.last_synced_block = Some(last_block);
        orderbook.updated_at = row.updated_at.clone();
    }
}

fn map_raindex_snapshot(snapshot: LocalDbSyncSnapshot) -> RaindexSyncStatus {
    let status = if snapshot.configured {
        snapshot.status.into()
    } else {
        RaindexSyncStatusKind::NotConfigured
    };

    log_raindex_snapshot_errors(&snapshot);

    RaindexSyncStatus {
        status,
        configured: snapshot.configured,
        healthy: snapshot.healthy,
        error: raindex_error(&snapshot),
        networks: snapshot
            .networks
            .into_iter()
            .map(map_network_snapshot)
            .collect(),
        orderbooks: snapshot
            .raindexes
            .into_iter()
            .map(map_raindex_status_snapshot)
            .collect(),
    }
}

fn map_network_snapshot(snapshot: NetworkSyncStatusSnapshot) -> NetworkSyncInfo {
    NetworkSyncInfo {
        chain_id: snapshot.chain_id,
        network_key: snapshot.network_key,
        status: snapshot.status.into(),
        orderbook_count: snapshot.raindex_count,
        ready: snapshot.ready,
        error: snapshot.error.map(|_| "network sync failed".to_string()),
    }
}

fn map_raindex_status_snapshot(snapshot: RaindexSyncStatusSnapshot) -> OrderbookSyncInfo {
    OrderbookSyncInfo {
        chain_id: snapshot.raindex_id.chain_id,
        orderbook_address: format!("{:#x}", snapshot.raindex_id.raindex_address),
        orderbook_key: snapshot.raindex_key,
        network_key: snapshot.network_key,
        status: snapshot.status.into(),
        ready: snapshot.ready,
        phase_message: snapshot.phase_message,
        last_synced_block: snapshot.last_synced_block,
        updated_at: snapshot.updated_at,
        sync_position_error: None,
        error: snapshot.error.map(|_| "orderbook sync failed".to_string()),
    }
}

fn log_raindex_snapshot_errors(snapshot: &LocalDbSyncSnapshot) {
    for network in &snapshot.networks {
        if let Some(error) = &network.error {
            tracing::warn!(
                chain_id = network.chain_id,
                network_key = network.network_key.as_deref(),
                error = %error,
                "raindex network sync failed"
            );
        }
    }

    for raindex in &snapshot.raindexes {
        if let Some(error) = &raindex.error {
            tracing::warn!(
                chain_id = raindex.raindex_id.chain_id,
                orderbook_address = %format!("{:#x}", raindex.raindex_id.raindex_address),
                orderbook_key = raindex.raindex_key.as_deref(),
                network_key = raindex.network_key.as_deref(),
                error = %error,
                "raindex orderbook sync failed"
            );
        }
    }
}

fn raindex_error(snapshot: &LocalDbSyncSnapshot) -> Option<String> {
    if !snapshot.healthy {
        Some("raindex local DB sync is unhealthy".to_string())
    } else if !snapshot.configured {
        Some("raindex local DB sync is not configured".to_string())
    } else {
        None
    }
}

fn detailed_status(
    app_db: &DbStatus,
    raindex: &RaindexSyncStatus,
    market_prices: &crate::types::health::MarketPriceHealthStatus,
) -> HealthStatus {
    if !app_db.connected || !raindex.healthy || raindex.status == RaindexSyncStatusKind::Failure {
        HealthStatus::Error
    } else if raindex.status == RaindexSyncStatusKind::NotConfigured
        || raindex.status == RaindexSyncStatusKind::Syncing
        || !market_prices.healthy
    {
        HealthStatus::Degraded
    } else {
        HealthStatus::Ok
    }
}

pub fn routes() -> Vec<Route> {
    rocket::routes![get_health, get_health_detailed]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::health::MarketPriceHealthStatus;
    use alloy::primitives::address;
    use rain_orderbook_common::local_db::RaindexIdentifier;
    use rain_orderbook_common::raindex_client::local_db::{LocalDbStatus, SchedulerState};

    #[test]
    fn detailed_status_is_degraded_when_raindex_has_not_started() {
        let app_db = DbStatus {
            status: DbHealthStatus::Ok,
            connected: true,
            error: None,
        };
        let raindex = RaindexSyncStatus {
            status: RaindexSyncStatusKind::Syncing,
            configured: true,
            healthy: true,
            error: None,
            networks: vec![],
            orderbooks: vec![],
        };

        assert_eq!(
            detailed_status(&app_db, &raindex, &healthy_market_prices()),
            HealthStatus::Degraded
        );
    }

    #[test]
    fn detailed_status_is_error_when_app_db_is_down() {
        let app_db = DbStatus {
            status: DbHealthStatus::Error,
            connected: false,
            error: Some("db unavailable".to_string()),
        };
        let raindex = RaindexSyncStatus {
            status: RaindexSyncStatusKind::Active,
            configured: true,
            healthy: true,
            error: None,
            networks: vec![],
            orderbooks: vec![],
        };

        assert_eq!(
            detailed_status(&app_db, &raindex, &healthy_market_prices()),
            HealthStatus::Error
        );
    }

    #[test]
    fn detailed_status_is_degraded_when_raindex_is_not_configured() {
        let app_db = DbStatus {
            status: DbHealthStatus::Ok,
            connected: true,
            error: None,
        };
        let raindex = RaindexSyncStatus {
            status: RaindexSyncStatusKind::NotConfigured,
            configured: false,
            healthy: true,
            error: Some("raindex local DB sync is not configured".to_string()),
            networks: vec![],
            orderbooks: vec![],
        };

        assert_eq!(
            detailed_status(&app_db, &raindex, &healthy_market_prices()),
            HealthStatus::Degraded
        );
    }

    #[test]
    fn map_raindex_snapshot_preserves_network_and_orderbook_status() {
        let orderbook_id =
            RaindexIdentifier::new(8453, address!("d2938e7c9fe3597f78832ce780feb61945c377d7"));
        let snapshot = LocalDbSyncSnapshot::from_parts(
            vec![NetworkSyncStatusSnapshot {
                chain_id: 8453,
                network_key: Some("base".to_string()),
                status: LocalDbStatus::Active,
                scheduler_state: SchedulerState::Leader,
                raindex_count: 1,
                ready: true,
                error: None,
            }],
            vec![RaindexSyncStatusSnapshot {
                raindex_id: orderbook_id,
                raindex_key: Some("base-orderbook".to_string()),
                network_key: Some("base".to_string()),
                status: LocalDbStatus::Active,
                scheduler_state: SchedulerState::Leader,
                ready: true,
                phase_message: None,
                last_synced_block: Some(12_345_678),
                updated_at: Some("2026-05-01 12:00:00".to_string()),
                error: None,
            }],
        );

        let raindex = map_raindex_snapshot(snapshot);

        assert_eq!(raindex.status, RaindexSyncStatusKind::Active);
        assert!(raindex.configured);
        assert!(raindex.healthy);
        assert_eq!(raindex.networks.len(), 1);
        assert_eq!(raindex.networks[0].network_key.as_deref(), Some("base"));
        assert_eq!(raindex.orderbooks.len(), 1);
        assert_eq!(
            raindex.orderbooks[0].orderbook_address,
            "0xd2938e7c9fe3597f78832ce780feb61945c377d7"
        );
        assert_eq!(raindex.orderbooks[0].last_synced_block, Some(12_345_678));
        assert_eq!(
            raindex.orderbooks[0].updated_at.as_deref(),
            Some("2026-05-01 12:00:00")
        );
    }

    #[test]
    fn map_raindex_snapshot_sanitizes_sync_errors() {
        let orderbook_id =
            RaindexIdentifier::new(8453, address!("d2938e7c9fe3597f78832ce780feb61945c377d7"));
        let snapshot = LocalDbSyncSnapshot::from_parts(
            vec![NetworkSyncStatusSnapshot {
                chain_id: 8453,
                network_key: Some("base".to_string()),
                status: LocalDbStatus::Failure,
                scheduler_state: SchedulerState::Leader,
                raindex_count: 1,
                ready: false,
                error: Some("sqlite: no such table sync_status".to_string()),
            }],
            vec![RaindexSyncStatusSnapshot {
                raindex_id: orderbook_id,
                raindex_key: Some("base-orderbook".to_string()),
                network_key: Some("base".to_string()),
                status: LocalDbStatus::Failure,
                scheduler_state: SchedulerState::Leader,
                ready: false,
                phase_message: None,
                last_synced_block: None,
                updated_at: None,
                error: Some("provider url includes internal host".to_string()),
            }],
        );

        let raindex = map_raindex_snapshot(snapshot);

        assert_eq!(
            raindex.error.as_deref(),
            Some("raindex local DB sync is unhealthy")
        );
        assert_eq!(
            raindex.networks[0].error.as_deref(),
            Some("network sync failed")
        );
        assert_eq!(
            raindex.orderbooks[0].error.as_deref(),
            Some("orderbook sync failed")
        );
    }

    #[test]
    fn detailed_response_does_not_expose_scheduler_state() {
        let response = DetailedHealthResponse {
            status: HealthStatus::Ok,
            app_db: DbStatus {
                status: DbHealthStatus::Ok,
                connected: true,
                error: None,
            },
            raindex: RaindexSyncStatus {
                status: RaindexSyncStatusKind::Active,
                configured: true,
                healthy: true,
                error: None,
                networks: vec![],
                orderbooks: vec![],
            },
            market_prices: healthy_market_prices(),
        };

        let serialized = match serde_json::to_value(response) {
            Ok(value) => value,
            Err(error) => panic!("detailed health response should serialize: {error}"),
        };

        assert!(serialized.get("scheduler_state").is_none());
        assert!(serialized["raindex"].get("scheduler_state").is_none());
        assert_eq!(serialized["status"], "ok");
        assert_eq!(serialized["app_db"]["status"], "ok");
        assert_eq!(serialized["raindex"]["status"], "active");
    }

    #[test]
    fn map_raindex_snapshot_reports_not_configured() {
        let raindex = map_raindex_snapshot(LocalDbSyncSnapshot::not_configured());

        assert_eq!(raindex.status, RaindexSyncStatusKind::NotConfigured);
        assert!(!raindex.configured);
        assert!(raindex.healthy);
        assert_eq!(
            raindex.error.as_deref(),
            Some("raindex local DB sync is not configured")
        );
    }

    const ORDERBOOK: alloy::primitives::Address =
        address!("e522cb4a5fcb2eb31a52ff41a4653d85a4fd7c9d");

    /// What the SDK snapshot gives on a synced chain: active, ready, no persisted position.
    fn active_snapshot_without_watermark() -> LocalDbSyncSnapshot {
        LocalDbSyncSnapshot::from_parts(
            vec![NetworkSyncStatusSnapshot {
                chain_id: 8453,
                network_key: Some("base".to_string()),
                status: LocalDbStatus::Active,
                scheduler_state: SchedulerState::Leader,
                raindex_count: 1,
                ready: true,
                error: None,
            }],
            vec![RaindexSyncStatusSnapshot {
                raindex_id: RaindexIdentifier::new(8453, ORDERBOOK),
                raindex_key: Some("base-orderbook".to_string()),
                network_key: Some("base".to_string()),
                status: LocalDbStatus::Active,
                scheduler_state: SchedulerState::Leader,
                ready: true,
                phase_message: None,
                last_synced_block: None,
                updated_at: None,
                error: None,
            }],
        )
    }

    /// A Raindex local DB file built and written with the SDK's own executor and statements:
    /// the real schema, and the watermark row exactly as the sync pipeline's apply step
    /// writes it. `sync_status` is left as the pipeline leaves it: empty.
    async fn local_db_with_watermark(
        dir: &tempfile::TempDir,
        last_block: Option<u64>,
    ) -> std::path::PathBuf {
        use rain_orderbook_common::local_db::executor::RusqliteExecutor;
        use rain_orderbook_common::local_db::query::create_tables::create_tables_batch;
        use rain_orderbook_common::local_db::query::upsert_target_watermark::upsert_target_watermark_stmt;
        use rain_orderbook_common::local_db::query::{LocalDbQueryExecutor, SqlStatementBatch};

        let path = dir.path().join("raindex.db");
        let executor = RusqliteExecutor::new(&path);
        executor
            .execute_batch(&create_tables_batch().ensure_transaction())
            .await
            .expect("create raindex tables");
        if let Some(last_block) = last_block {
            let mut batch = SqlStatementBatch::new();
            batch.add(upsert_target_watermark_stmt(
                &RaindexIdentifier::new(8453, ORDERBOOK),
                last_block,
                alloy::primitives::Bytes::from(vec![0xab; 32]),
            ));
            executor
                .execute_batch(&batch.ensure_transaction())
                .await
                .expect("write target watermark");
        }
        path
    }

    #[rocket::async_test]
    async fn active_orderbook_with_watermark_row_reports_block_and_time() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = local_db_with_watermark(&dir, Some(52_003_171)).await;
        let mut raindex = map_raindex_snapshot(active_snapshot_without_watermark());
        assert_eq!(raindex.orderbooks[0].last_synced_block, None);
        assert_eq!(raindex.orderbooks[0].updated_at, None);

        let before = time::OffsetDateTime::now_utc().unix_timestamp();
        fill_sync_watermarks(Some(&path), &mut raindex.orderbooks).await;
        let after = time::OffsetDateTime::now_utc().unix_timestamp();

        let serialized = serde_json::to_value(&raindex).expect("serialize raindex status");
        let orderbook = &serialized["orderbooks"][0];
        assert_eq!(orderbook["status"], "active");
        assert_eq!(orderbook["ready"], true);
        assert_eq!(orderbook["last_synced_block"], 52_003_171);
        assert!(orderbook.get("sync_position_error").is_none());
        let updated_at = orderbook["updated_at"]
            .as_str()
            .expect("updated_at is present");
        let written = time::PrimitiveDateTime::parse(
            updated_at,
            time::macros::format_description!("[year]-[month]-[day] [hour]:[minute]:[second]"),
        )
        .expect("updated_at is 'YYYY-MM-DD HH:MM:SS'")
        .assume_utc()
        .unix_timestamp();
        assert!(
            (before - 1..=after + 1).contains(&written),
            "updated_at {updated_at} is not the UTC time of the write"
        );
    }

    /// The cause of the empty prod fields: after the pipeline's own watermark write, the
    /// statement the SDK snapshot runs (`sync_status`) still finds nothing.
    #[rocket::async_test]
    async fn sdk_snapshot_query_finds_no_row_after_pipeline_watermark_write() {
        use rain_orderbook_common::local_db::executor::RusqliteExecutor;
        use rain_orderbook_common::local_db::query::fetch_last_synced_block::{
            fetch_last_synced_block_stmt, SyncStatusResponse,
        };
        use rain_orderbook_common::local_db::query::LocalDbQueryExecutor;

        let dir = tempfile::tempdir().expect("temp dir");
        let path = local_db_with_watermark(&dir, Some(52_003_171)).await;

        let rows: Vec<SyncStatusResponse> = RusqliteExecutor::new(&path)
            .query_json(&fetch_last_synced_block_stmt(&RaindexIdentifier::new(
                8453, ORDERBOOK,
            )))
            .await
            .expect("sdk sync_status query runs");
        assert!(rows.is_empty(), "sync_status has rows: {rows:?}");

        let rows = read_sync_watermarks_at(&path).await.expect("watermarks");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].last_block, 52_003_171);
    }

    #[rocket::async_test]
    async fn orderbook_without_watermark_row_reports_no_row() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = local_db_with_watermark(&dir, None).await;
        let mut raindex = map_raindex_snapshot(active_snapshot_without_watermark());

        fill_sync_watermarks(Some(&path), &mut raindex.orderbooks).await;

        let serialized = serde_json::to_value(&raindex).expect("serialize raindex status");
        let orderbook = &serialized["orderbooks"][0];
        assert!(orderbook.get("last_synced_block").is_none());
        assert!(orderbook.get("updated_at").is_none());
        assert_eq!(orderbook["sync_position_error"], NO_ROW);
        assert_eq!(orderbook["status"], "active");
    }

    #[rocket::async_test]
    async fn failing_watermark_query_reports_query_failed() {
        let dir = tempfile::tempdir().expect("temp dir");
        // A valid, empty SQLite file: it opens, but has no `target_watermarks` table.
        let path = dir.path().join("empty.db");
        std::fs::File::create(&path).expect("create empty db file");
        let mut raindex = map_raindex_snapshot(active_snapshot_without_watermark());

        fill_sync_watermarks(Some(&path), &mut raindex.orderbooks).await;

        let serialized = serde_json::to_value(&raindex).expect("serialize raindex status");
        let orderbook = &serialized["orderbooks"][0];
        assert!(orderbook.get("last_synced_block").is_none());
        assert!(orderbook.get("updated_at").is_none());
        assert_eq!(orderbook["sync_position_error"], QUERY_FAILED);
        assert_eq!(raindex.status, RaindexSyncStatusKind::Active);
    }

    #[rocket::async_test]
    async fn unreadable_local_db_reports_open_failed() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut raindex = map_raindex_snapshot(active_snapshot_without_watermark());

        fill_sync_watermarks(
            Some(&dir.path().join("missing.db")),
            &mut raindex.orderbooks,
        )
        .await;

        assert_eq!(raindex.orderbooks[0].last_synced_block, None);
        assert_eq!(raindex.orderbooks[0].updated_at, None);
        assert_eq!(
            raindex.orderbooks[0].sync_position_error.as_deref(),
            Some(DB_OPEN_FAILED)
        );
        assert_eq!(raindex.status, RaindexSyncStatusKind::Active);
    }

    #[rocket::async_test]
    async fn missing_db_path_reports_not_configured() {
        let mut raindex = map_raindex_snapshot(active_snapshot_without_watermark());

        fill_sync_watermarks(None, &mut raindex.orderbooks).await;

        assert_eq!(raindex.orderbooks[0].last_synced_block, None);
        assert_eq!(
            raindex.orderbooks[0].sync_position_error.as_deref(),
            Some(NO_DB_PATH)
        );
    }

    #[test]
    fn sdk_values_are_not_overwritten_by_watermark_rows() {
        let mut orderbooks = vec![OrderbookSyncInfo {
            chain_id: 8453,
            orderbook_address: format!("{ORDERBOOK:#x}"),
            orderbook_key: None,
            network_key: None,
            status: RaindexSyncStatusKind::Active,
            ready: true,
            phase_message: None,
            last_synced_block: Some(7),
            updated_at: Some("2026-05-01 12:00:00".to_string()),
            sync_position_error: None,
            error: None,
        }];
        let rows = vec![SyncWatermarkRow {
            chain_id: 8453,
            raindex_address: format!("{ORDERBOOK:#x}"),
            last_block: 9,
            updated_at: Some("2026-05-02 12:00:00".to_string()),
        }];

        apply_sync_watermarks(&mut orderbooks, &rows);

        assert_eq!(orderbooks[0].last_synced_block, Some(7));
        assert_eq!(
            orderbooks[0].updated_at.as_deref(),
            Some("2026-05-01 12:00:00")
        );
        assert_eq!(orderbooks[0].sync_position_error, None);
    }

    fn healthy_market_prices() -> MarketPriceHealthStatus {
        MarketPriceHealthStatus {
            enabled: true,
            healthy: true,
            running: true,
            last_attempt_at: Some(100),
            last_success_at: Some(100),
            consecutive_failures: 0,
            error: None,
        }
    }

    #[test]
    fn detailed_status_is_degraded_when_market_prices_are_stale() {
        let app_db = DbStatus {
            status: DbHealthStatus::Ok,
            connected: true,
            error: None,
        };
        let raindex = RaindexSyncStatus {
            status: RaindexSyncStatusKind::Active,
            configured: true,
            healthy: true,
            error: None,
            networks: vec![],
            orderbooks: vec![],
        };
        let market_prices = MarketPriceHealthStatus {
            healthy: false,
            ..healthy_market_prices()
        };

        assert_eq!(
            detailed_status(&app_db, &raindex, &market_prices),
            HealthStatus::Degraded
        );
    }
}
